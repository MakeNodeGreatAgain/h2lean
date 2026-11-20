// HTTP/2 client connection state machine (h2c and h2s). The mirror of lib/session.js.
//
// The server file is hard-locked to its role -- it reads the client preface
// (session.js:771), refuses even stream ids (session.js:968) and treats the
// rapid-reset budget as "server side only". A client does the exact inverse on
// every one of those: it WRITES the preface, opens ODD ids, parses `:status`
// instead of `:method`, obeys the peer's SETTINGS rather than advertising its
// own limits, and returns flow-control credit for bytes it consumed instead of
// granting it from its window. So this is a second state machine sharing the
// role-neutral primitives, not a `role` flag on the first.
//
// Reference for the JS-visible client surface is node's own bundled
// internal/http2/core.js: connect() (arg normalisation), ClientHttp2Session
// .request() (defaults + endStream = isPayloadMeaningless(method)) and
// ClientHttp2Stream (a Duplex). Measured off node v26.12.1 in the same session.
//
// The two performance decisions are the server's, ported rather than re-invented
// (AGENTS.md 1), because a client and a server move the same bytes in opposite
// directions and the shape of the work is identical:
//  * a whole frame is parsed in place out of the chunk node handed us. A frame
//    that straddles two reads is not assembled at all: its payload is kept as
//    the views of the chunks it arrived in, one view per read. A DATA payload
//    reaches the application that way, which is what node does with the same
//    frame (tools/probe-chunking.js measures both stacks delivering a cut
//    payload as the pieces it arrived in), and a header block is decoded out of
//    the views (lib/hpack.js decodePieces). A padded DATA payload is no copy
//    either: its views are not the payload (RFC 9113 6.1 -- the first octet is
//    the pad length, the last ones are the padding), so the padding is taken
//    off the views once the frame is complete (lib/frames.js trimPadding);
//  * one output staging buffer per batch: every frame produced while handling one
//    read or one tick is serialised into it and leaves through a single write (or a
//    single corked writev for a body that travels by reference above BODY_MIN), so a
//    HEADERS + WINDOW_UPDATE batch costs no per-frame allocation and no per-frame
//    syscall. The credit release has its own coalescing buffer (_windowUpdate), because
//    it is due at the end of a read batch, when the previous one is usually still in
//    flight and the queue would hold it a whole poll cycle.
// What still differs from the server, deliberately: receive credit is returned for
// bytes as they ARRIVE, not when the application reads them (so a slow reader does
// not throttle the peer -- read-side backpressure is a behaviour change that needs
// its own differential tests, not a performance tweak), and a body is never deferred
// for lack of queue room because the staging bound above already caps what one
// connection can make us hold.
// A fatal inbound frame ends the session the way node's does: GOAWAY on the wire
// -- flushed out of the staging batch before the socket is closed (_fatal; the
// server side does it the same way, lib/session.js _fatal -> sendGoaway -> _flush)
// -- and ERR_HTTP2_ERROR reported on the stream and on the session. Measured
// against node v26.12.1 on a malformed padded DATA frame
// (tools/probe-client-pad-bad.js) and on padded HEADERS frames
// (tools/probe-client-hdr-pad.js).
// The padded-frame rules are the server's, ported (lib/session.js _headersPrefix
// and _onDataFrame): a padded HEADERS whose padding does not fit is a connection
// error of type PROTOCOL_ERROR (RFC 9113 6.2: "If the length of the padding is
// the length of the frame payload or greater"), and so is a padded frame with no
// payload at all -- no pad-length octet to read (_beginHeaders refuses both). The
// DATA rule is RFC 9113 6.1, measured by tools/probe-client-pad-bad.js
// (_onDataFrame). The response pseudo-header rules are checked in
// _refuseHeaders before the application sees the object: a response without
// exactly one :status, a request-only pseudo-header on a response, and a
// pseudo-header in a trailer block (RFC 9113 8.1) are all per-stream refusals
// -- ERR_HTTP2_STREAM_ERROR, RST_STREAM(PROTOCOL_ERROR), the session stays up
// (measured, tools/probe-client-status.js; this closes the empty-block
// divergence).
//
// KNOWN DIVERGENCE: a stream leaves _streams when its response ends (_dropStream on
// END_STREAM), while node keeps it in state.streams until the Duplex is destroyed. A
// RST_STREAM that arrives after END_STREAM therefore finds nothing here and is ignored,
// so a request whose body was never ended -- or whose response body is still buffered
// unread -- never closes: node reports aborted/finish/close, h2lean waits forever.
// _wireDone already tracks these streams for the graceful close; the reset path does not
// consult it. Pinned as the excluded shape in test/client/client-rst.test.js.
//
// KNOWN DIVERGENCE: settings() submits the seven standard settings only. node also takes
// a `customSettings` object of raw id/value pairs (validateSettings caps it at
// MAX_ADDITIONAL_SETTINGS = 10, updateSettingsBuffer puts the entries on the wire and
// getSettings reflects them back, including an enableConnectProtocol it stores as id 8).
// Here those entries are neither validated nor submitted, so a caller using them gets a
// SETTINGS frame without them rather than node's. The standard-object path -- argument
// checks, id-ordered payload, the (null, localSettings, duration) callback, the
// 'localSettings' event, the 'connect' deferral and the ten-pending-ACK limit -- is
// node's, measured on a live node 26 client against a raw frame-decoding peer.
//
// destroy() terminates the connection the way node's closeSession does: the derived code
// goes out as GOAWAY(last=0, code) before the socket goes, preceded by a RST_STREAM with
// that same code for every open stream but the last. Measured on a live node client for
// destroy(), destroy(err), destroy(8) and destroy(err, 1) -- codes 0, 2, 8 and 1 -- for one
// to four open streams, after a graceful close() had already sent a goodbye, and with a
// stream destroyed beforehand. The rule and its C++ caveat are at destroy() below; the
// shapes are pinned by test/client/client-destroy-goaway.test.js.
//
// KNOWN DIVERGENCE: a non-zero inbound GOAWAY leaves the session open. node's
// onGoawayData destroys it at once with ERR_HTTP2_SESSION_ERROR(code) and a
// GOAWAY(NO_ERROR) of its own (core.js:781), which closes every open stream with that
// error and the goaway code as rstCode. Here only a NO_ERROR goodbye begins the graceful
// close, so a stream the application destroys afterwards reports the caller's error and
// frames a reset where node reports ERR_HTTP2_SESSION_ERROR and frames nothing. The
// rstCode agrees -- the derivation reads goawayCode -- but the error and the wire do not.
// Recorded in test/client/client-destroy-code.test.js.
'use strict';

const { EventEmitter } = require('node:events');
const tls = require('node:tls');
const f = require('./frames');
const { Decoder, Encoder, HPackError } = require('./hpack');
const FlatList = require('./flat-list');
// The process-wide inbound decode scratch, same shape and size as the server
// session's; the full why-sharing-is-safe argument lives with the server's
// instance (lib/session.js). A module-level instance here too, not the server's
// list: the two never meet the same fill, and coupling the client module to the
// server's scratch would buy nothing.
const FLAT = new FlatList(64);
const { nullProto, isFieldName, lowerName, CONNECTION_SPECIFIC, SINGLE_VALUE_FIELDS } = require('./headers');
const { ClientHttp2Stream, AbortError } = require('./client-stream');
const { pieceByteAt, pieceSlice, pieceConcat, pieceEquals } = require('./pieces');
// The settings tables, defaults and buffer-path constants are the server's, reused
// verbatim so a client and a server in this one library never disagree about what a
// SETTINGS id means, what node advertises, or where the copy/reference break-even sits.
const {
  OUR_DEFAULTS, PEER_DEFAULTS, SETTINGS_BY_ID, ID_BY_SETTING, BODY_MIN, MAX_OUT_BATCH,
  STAGE_MIN, MAX_SHAPES, MAX_SEND_BLOCK, PRIORITY_SPECLEN,
  strictSingleValueFields, hashBlock, hashPieces, kBoundSession,
  proxySocketHandler, urlOrigin, parseAltSvc, parseOrigins, received, extError
} = require('./session');

const {
  DATA, HEADERS, PRIORITY, RST_STREAM, SETTINGS, PUSH_PROMISE, PING, GOAWAY,
  WINDOW_UPDATE, CONTINUATION, ALTSVC, ORIGIN,
  F_END_STREAM, F_ACK, F_END_HEADERS, F_PADDED, F_PRIORITY,
  NO_ERROR, PROTOCOL_ERROR, INTERNAL_ERROR, FRAME_SIZE_ERROR, COMPRESSION_ERROR, CANCEL, REFUSED_STREAM,
  HEADER_LEN, PREFACE, PREFACE_LEN, readHeader, writeHeader, trimPadding
} = f;

const MAX_SETTINGS_ENTRIES = 32; // CVE-2020-11080; same bound as the server
const MAX_CONTINUATIONS = 8; // CVE-2024-28182; same bound as the server
// nghttp2 refuses a SETTINGS submit once ten frames are awaiting an ACK, and node
// turns that refusal into a destroyed session (core.js submitSettings: a false return
// from handle.settings() -> destroy(ERR_HTTP2_MAX_PENDING_SETTINGS_ACK)). Measured on
// a live node 26 client against a peer that never ACKs: the connection's own first
// SETTINGS plus nine settings() calls go out (ten frames), the tenth call submits
// nothing and destroys the session with that error; the call after it throws
// ERR_HTTP2_INVALID_SESSION because the session is already gone.
const MAX_PENDING_SETTINGS_ACK = 10;
// RFC 7540 6.9.2 / 6.5.2: what an endpoint that has not been told otherwise runs a
// stream's flow control with. The reason it matters here is ordering: a server writes
// its SETTINGS -- advertising its own window -- before it has read ours, so any stream it
// answers on is sized by OUR advertised value if that has landed by then and by this
// default if it has not. The client cannot tell which happened, so credit release is
// planned against the smaller of the two -- see _creditRecv().
const DEFAULT_WINDOW = 65535;

// The field-name checks are the server's, imported rather than restated: `isFieldName`
// carries the cache of names already known to be legal (lib/headers.js, and
// OPTIMIZATION.md item 7 for what it measures on the names a request really
// carries). Re-running an uncached regex here would be the same work the response
// path has already learned to skip.

// Methods RFC 9113 asks a client not to send a body with; node ends the stream by
// default for these (`isPayloadMeaningless(method)` in core.js).
function payloadMeaningless(method) {
  return method === 'GET' || method === 'HEAD' || method === 'DELETE' || method === 'CONNECT'
    || method === 'OPTIONS' || method === 'TRACE';
}

// The ranges validateSettings applies (internal/http2/core.js), whose limits are
// kMaxInt = 2**32-1, kMaxInitialWindowSize = 2**31-1, kMaxFrameSize = 2**24-1 and
// kMaxStreams = 2**32-1. maxFrameSize is the only one with a non-zero floor: RFC 9113
// 6.5.2 defines SETTINGS_MAX_FRAME_SIZE as 16384..16777215.
const SETTINGS_RANGE = {
  headerTableSize: [0, 0xffffffff],
  initialWindowSize: [0, 0x7fffffff],
  maxFrameSize: [16384, 0xffffff],
  maxConcurrentStreams: [0, 0xffffffff],
  maxHeaderListSize: [0, 0xffffffff],
  maxHeaderSize: [0, 0xffffffff]
};

// ERR_HTTP2_INVALID_SETTING_VALUE is a TypeError, not ERR_OUT_OF_RANGE, and its message
// names the setting and the value only; .actual is always set and .min/.max only for a
// range check (internal/errors.js: the factory takes min/max defaulting to undefined).
// Measured on a live node 26 client: {maxFrameSize: 100} and {headerTableSize: -1} both
// report `Invalid value for setting "<name>": <value>`, and so does {enablePush: 1},
// which is a type check rather than a range one and carries no .min.
function invalidSetting(name, value, min, max) {
  const err = new TypeError(`Invalid value for setting "${name}": ${value}`);
  err.code = 'ERR_HTTP2_INVALID_SETTING_VALUE';
  err.actual = value;
  if (min !== undefined) {
    err.min = min;
    err.max = max;
  }
  return err;
}

// assertIsObject + validateSettings, in node's order. undefined is legal -- it submits an
// empty SETTINGS frame -- while null, a string and an array are ERR_INVALID_ARG_TYPE;
// a Buffer is an object whose setting properties are all undefined, so it passes and
// submits nothing (both measured). An unknown key is ignored, not refused.
function checkSettings(settings) {
  if (settings === undefined) {
    return;
  }
  if (settings === null || typeof settings !== 'object' || Array.isArray(settings)) {
    throw extError('ERR_INVALID_ARG_TYPE',
      `The "settings" argument must be of type object. Received ${received(settings)}`);
  }
  for (const name of Object.keys(SETTINGS_RANGE)) {
    const value = settings[name];
    if (value === undefined) {
      continue;
    }
    const [min, max] = SETTINGS_RANGE[name];
    if (typeof value !== 'number' || value < min || value > max) {
      throw invalidSetting(name, value, min, max);
    }
  }
  for (const name of ['enablePush', 'enableConnectProtocol']) {
    if (settings[name] !== undefined && typeof settings[name] !== 'boolean') {
      throw invalidSetting(name, settings[name]);
    }
  }
}

// The request method request() fills ':method' from when the application did not
// supply one. Hoisted for reuse, not for speed: V8 interns string literals, so the
// measured position is unchanged (OPTIMIZATION.md item 36).
const DEFAULT_METHOD = 'GET';

// The estimate _encodeRequest just computed, read by its caller immediately after --
// the result-slot shape lib/frames.js and lib/headers.js use, for the same reason: no
// allocation per block. Only _encodeRequest writes it.
let sentBound = 0;

class ClientHttp2Session extends EventEmitter {
  constructor(socket, options, scheme, authority) {
    super();
    const opts = options || {};
    // The same ownership mark the server session takes (node's one Http2Session base
    // constructor): a socket bound to any session -- client or server -- cannot be
    // bound again, so performServerHandshake on a client's socket throws too.
    if (socket[kBoundSession] !== undefined) {
      const err = new Error('The socket is already bound to an Http2Session');
      err.code = 'ERR_HTTP2_SOCKET_BOUND';
      throw err;
    }
    socket[kBoundSession] = this;
    this._socket = socket;
    // public `socket` returns node's safe Proxy, built lazily; close drops it (see session.js)
    this._proxySocket = null;
    this.type = 1; // NGHTTP2_SESSION_CLIENT
    this.closed = false;
    this.destroyed = false;
    this.connecting = true;
    // encrypted/alpnProtocol are derived from the socket once it is connected
    // (_start), the way node's setupHandle fills them after 'secureConnect':
    // before that node's getters read undefined.
    this.pendingSettingsAck = true;
    // node seeds `originSet` lazily from the TLS socket's servername/remoteAddress
    // and adds the ORIGIN frames the server sent on top of it (encrypted
    // connections only). It starts undefined; the getter builds it on first read,
    // exactly as core.js does with initOriginSet.
    this._originSet = undefined;
    // maxOriginSetSize is client-only and validated by connect(); the server
    // session leaves it undefined so ORIGIN frames over h2s accumulate unbounded.
    this._maxOriginSetSize = opts.maxOriginSetSize;
    // enablePush defaults to true on the client (node's initializeOptions sets
    // NGHTTP2_SETTINGS_ENABLE_PUSH = 1 for clients; the server's OUR_DEFAULTS has
    // false because a server does not receive pushes). The application can override
    // via opts.settings.enablePush.
    // node's localSettings reports what nghttp2 has in effect, not what
    // getDefaultSettings() prints: a live node 26 client reports maxHeaderListSize and
    // maxHeaderSize 4294967295 where getDefaultSettings() says 65535, and getSettings()
    // (internal/http2/util.js) emits the eight keys in the order below -- maxHeaderSize
    // is an alias of maxHeaderListSize, enableConnectProtocol a boolean, whether or not
    // the application named either. The settings() callback hands this object out, so
    // both the order and the numbers are observable. Nothing here is a guard: the client
    // enforces no inbound header-list bound, and _advertised still decides what is sent.
    this.localSettings = {
      headerTableSize: OUR_DEFAULTS.headerTableSize,
      enablePush: true,
      initialWindowSize: OUR_DEFAULTS.initialWindowSize,
      maxFrameSize: OUR_DEFAULTS.maxFrameSize,
      maxConcurrentStreams: OUR_DEFAULTS.maxConcurrentStreams,
      maxHeaderListSize: 4294967295,
      maxHeaderSize: 4294967295,
      enableConnectProtocol: false,
      ...opts.settings
    };
    if (typeof opts.settings?.maxHeaderSize === 'number'
      && opts.settings.maxHeaderListSize === undefined) {
      this.localSettings.maxHeaderListSize = opts.settings.maxHeaderSize;
    }
    this.localSettings.maxHeaderSize = this.localSettings.maxHeaderListSize;
    // SETTINGS submits awaiting an ACK, and the callbacks they owe, oldest first. The
    // public pendingSettingsAck stays the boolean it has always been; node's own getter
    // is `pendingAck > 0` (core.js), so the counter is what makes it accurate when more
    // than one submit is outstanding.
    this._settingsPending = 0;
    this._settingsAcks = [];
    this.remoteSettings = { ...PEER_DEFAULTS };
    this._scheme = scheme || 'http';
    this._authority = authority || 'localhost';
    // The outbound header-block bound, the same option and the same coercion the server
    // session takes: node's updateOptionsBuffer gates it on `typeof === 'number'` alone
    // and writes it into a Uint32 slot (internal/http2/util.js:249), so a non-number is
    // ignored and 200.9 behaves as 200. connect() passes its options through whole.
    this._maxSendBlock = typeof opts.maxSendHeaderBlockLength === 'number'
      ? opts.maxSendHeaderBlockLength >>> 0 : MAX_SEND_BLOCK;

    // Only what the application asked for is advertised (see session.js _advertised:
    // a number we advertise is a number a peer plans around). Empty by default, which
    // is node's usual client SETTINGS frame too.
    this._advertised = new Set((opts.advertised !== undefined
      ? opts.advertised : Object.keys(opts.settings || {}))
      .filter((k) => ID_BY_SETTING[k] !== undefined));

    this._nextSid = 1; // client owns odd ids
    // Highest even id promised by the peer via PUSH_PROMISE. Used to silently
    // ignore late frames for a pushed stream the application already destroyed
    // (node does the same: session stays alive, measured v26.12.1).
    this._maxPushSid = 0;
    // node's maxReservedRemoteStreams connect option: the cap on pushed streams
    // still waiting for their response HEADERS (reserved(remote) in RFC 9113
    // terms). The default 200 and the refusal shape -- RST_STREAM(CANCEL), no
    // 'stream' event, no client error -- were measured on v26.12.1 with 500
    // silent pushes; nghttp2 owns the
    // counter in node, the count is rebuilt from the live streams here.
    this._maxReservedRemoteStreams = opts.maxReservedRemoteStreams === undefined
      ? 200 : opts.maxReservedRemoteStreams;
    this._streams = new Map();
    // Streams whose wire life is over (_dropStream) but whose Duplex has not
    // emitted 'close' yet -- an unread response body keeps the readable half
    // open. node's session counts a stream until it is destroyed, not until
    // END_STREAM (internal/http2/core.js kMaybeDestroy reads state.streams.size,
    // and the stream leaves that map in its destroy path), so a graceful close
    // waits for these too. Without the wait the session destroyed itself ahead
    // of the stream's 'close' and orphaned it: the event never fired at all
    // (measured on live node: it emits req end/close then session
    // close; h2lean hung with the stream alive and unreachable).
    this._wireDone = new Set();
    this._outboundQueue = []; // requests waiting for a concurrency slot
    this._decoder = new Decoder(this.localSettings.headerTableSize);
    this._encoder = new Encoder(this.remoteSettings.headerTableSize,
      this.localSettings.headerTableSize);
    this._blocks = new WeakMap();
    // node's strictSingleValueFields option (validateSessionOptions, default true,
    // internal/http2/core.js): gates the strict-single-value rule on the request list
    // (_addField below). The connect() call surfaces the validation the way
    // node's connect() does.
    this._strictSingle = strictSingleValueFields(opts);
    // scratch for the duplicate half of that rule: one array per session, cleared
    // per request. node grows a fresh one lazily per call (buildNgHeaderString);
    // the reuse is measured in OPTIMIZATION.md item 36.
    this._singlesSeen = [];

    // Send-side windows: our budget to write, governed by the peer's window. Both
    // start at 65535 and move with the peer's SETTINGS delta and its WINDOW_UPDATEs.
    this._connSendWin = PEER_DEFAULTS.initialWindowSize;
    this._prevInitialWin = PEER_DEFAULTS.initialWindowSize;
    // The receive side's mirror: octets consumed from the connection window whose
    // credit is not yet announced. Released in half-window steps -- see
    // _creditRecv() and the server's _creditConnection() for the measurement.
    this._connRecvUsed = 0;
    // The connection receive window we actually maintain: spent by every DATA octet,
    // restored by every WINDOW_UPDATE(0) we send. It starts at the smaller of what we
    // advertise and the 65535 a peer runs with until it has read our SETTINGS, which is
    // the only value we can promise it credit against -- see _creditRecv(). At the
    // default this is the same number the old `>= advertised >> 1` test produced.
    this._connRecvWin = Math.min(this.localSettings.initialWindowSize, DEFAULT_WINDOW);
    // Streams that took DATA since the last credit pass. _creditRecv() visits only
    // these instead of scanning every open stream: with the default 100 concurrent
    // streams a download that touches one of them used to pay ~100 property reads per
    // socket read (OPTIMIZATION.md item 19).
    this._debt = [];
    // The credit pass writes every WINDOW_UPDATE due from one batch into this one
    // session-owned buffer -- reused, never handed to the application -- so the batch
    // costs one small write instead of one allocation and one write call per update.
    this._wuBuf = null;
    this._wuLen = 0;

    // Inbound parsing, the server's design: frames are read straight out of the chunk
    // node gave us, and a frame that straddles two reads is kept as the views it
    // arrived in. The only octets the session copies are the nine of a frame header
    // that a read split: they wait in this scratch until the header is complete.
    this._fragHdr = Buffer.allocUnsafe(HEADER_LEN);
    this._frag = 0; // bytes of a straddling frame
    this._fragTarget = null;
    // A straddling DATA payload is NOT assembled: it is kept as the views of the
    // chunks the peer sent it in -- one per read -- and handed to the application
    // as they are. Node does the same: its payload reaches the application in the
    // pieces it arrived in, not in one piece per frame (measured with a frame whose
    // payload was cut in two: node's client gets two chunks, tools/probe-chunking.js),
    // so this is parity rather than a shortcut.
    this._fragPieces = null;
    //
    // A PADDED payload is the one case where the views are not the payload: its
    // first octet is the pad length and its last octets are padding (RFC 9113
    // 6.1). The padding is taken off the views when the frame is complete
    // (frames.js trimPadding), so even that case costs no copy.
    // The header of the frame being assembled, kept HERE and not read back from
    // frames.js's FH_TYPE/FH_FLAGS/FH_SID. Those are module-level slots shared by every
    // session in the process, and a straddling frame is dispatched in a LATER _onData()
    // call: another connection's readHeader() in between replaces them, and this session
    // delivers a frame its peer never sent. Measured with 8 sessions x 64 streams of
    // 256 kB (bench/micro/probe-client-framedrop.js): a DATA frame dispatched as WINDOW_UPDATE/0,
    // one whole 16 384-octet frame missing per affected stream, then HPACK
    // COMPRESSION_ERROR and the connection destroyed -- which is what the paired
    // benchmark had been reading as "the client's download is 2.5x slower".
    this._fragType = 0;
    this._fragFlags = 0;
    this._fragSid = 0;
    // CONTINUATION assembly for one inbound header block. A block whose frames all
    // arrived whole is kept in one buffer (_hdr); a block with a frame that straddled
    // a read is kept as the views it arrived in (_hdrPieces).
    this._hdr = null;
    this._hdrPieces = null;
    this._hdrLen = 0;
    this._hdrSid = -1;
    this._hdrEnd = false;
    this._hdrFlags = 0; // the HEADERS frame's flags, surfaced on 'response'/'push'
    this._hdrCont = 0;
    // Non-zero while a PUSH_PROMISE block is being assembled: the promised even
    // stream id. _finishHeaders routes to _processPushPromise when this is set.
    this._hdrPush = 0;
    // The first octets of a HEADERS payload -- the pad length and the priority
    // block -- are at most six, and a payload that arrived in pieces has no
    // buffer to read them from: they are copied into this scratch.
    this._hdrPrefix = Buffer.allocUnsafe(6);

    // Outbound: frames are serialised into one staging buffer per batch and queued by
    // reference only when the payload is big enough for the extra writev chunk to beat
    // the memcpy (BODY_MIN). See _frame/_need/_commit/_writePending, which mirror the
    // server's queue exactly.
    this._outQ = [];
    this._stage = null;
    this._stageLen = 0; // bytes of _stage not yet queued
    this._stageCap = STAGE_MIN; // size of the next staging buffer
    this._outLen = 0; // bytes the socket has not been given
    this._writing = false; // one batch in flight per connection
    this._flushScheduled = false;
    this._open = true;
    this._started = false;
    this._queuedReq = null; // requests made before 'connect'
    this._closeRequested = false; // close()/goaway asked to end once drained
    // The error destroy() was handed, and whether destroy() owns the pending
    // 'close' emission. A fatal inbound frame destroys with an ERR_HTTP2_ERROR
    // that must reach the session 'error' event; the deferred emissions below
    // (_onSocketClose / _trackClose) are for the graceful peer-FIN shape only and
    // must neither steal destroy()'s error-carrying nextTick nor fire ahead of it
    // (the documented teardown order is stream error, stream close, session
    // error, session close -- test/client/client-fatal.test.js).
    this._closeError = null;
    this._destroyScheduled = false;
    this._goawayCode = null;
    this._goawayLastStreamID = null;
    // node's state.destroyCode, set by closeSession from destroy(error, code)
    // (core.js:1246). A stream destroyed while the session is going away derives its
    // rstCode from `goawayCode || destroyCode` (core.js:2503), so the number has to be
    // kept separately from the error object the session reports.
    this._destroyCode = NO_ERROR;
    this._pings = new Map(); // 8-octet payload -> callback
    this._pingSeq = 0;
    // Response header shapes seen before: the cached object for a block whose bytes
    // we have already decoded under this dynamic-table state (the server's cache).
    this._shapes = opts.shapeCache === false ? null : new Map();
    this._shapeEpoch = 0;
    this._shapeHits = 0;
    this._shapeMisses = 0;

    this._flushSoon = () => {
      this._flushScheduled = false; this._flush();
    };
    this._onWrittenCb = () => this._onWritten();

    socket.on('data', (d) => this._onData(d));
    socket.on('error', (err) => this._onSocketError(err));
    socket.on('close', () => this._onSocketClose());
    socket.on('end', () => { /* peer FIN; 'close' does the teardown */ });
    // The socket setup node's Http2Session constructor does when a handle is
    // available (core.js): no Nagle, and no TLS renegotiation under TLS.
    if (typeof socket.setNoDelay === 'function') {
      socket.setNoDelay();
    }
    if (typeof socket.disableRenegotiation === 'function') {
      socket.disableRenegotiation();
    }
    // The preface and our SETTINGS may only go out once the socket is connected;
    // under TLS that is 'secureConnect' -- the handshake has to complete and pick
    // ALPN before the first octet is written (core.js defers setupHandle the same
    // way: "socket.connecting || socket.secureConnecting"). A createConnection
    // socket can already be established, so start immediately then.
    if (socket.connecting || socket.secureConnecting) {
      const ev = socket instanceof tls.TLSSocket ? 'secureConnect' : 'connect';
      socket.once(ev, () => this._start());
    } else {
      this._start();
    }
  }

  // ---- start / connect --------------------------------------------------------
  _start() {
    if (this._started) {
      return;
    }
    this._started = true;
    this.connecting = false;
    // What node's setupHandle records once the socket is connected: whether the
    // transport is a TLSSocket, and what ALPN negotiated on it -- 'h2' normally,
    // false when the peer ignored ALPN (prior knowledge over TLS), 'h2c' on a
    // plain socket. Reported as measured off the socket; node does not check the
    // client side against a required value, and neither do we.
    if (this._socket.encrypted) {
      this.encrypted = true;
      this.alpnProtocol = this._socket.alpnProtocol;
    } else {
      this.encrypted = false;
      this.alpnProtocol = 'h2c';
    }
    // Client magic: 24-octet preface, then SETTINGS. This is the frame ordering the
    // server validates on the way in (session.js:771) and the one we owe here.
    // Anything the application queued before the socket connected (a control frame,
    // say) must follow the handshake, never precede it -- so those bytes are set aside,
    // the handshake is staged in front of them, and the set-aside bytes go back into the
    // queue afterwards, which puts them behind SETTINGS on the wire.
    this._commit();
    const preQueued = this._outQ;
    this._outQ = [];
    this._need(PREFACE_LEN);
    PREFACE.copy(this._stage, this._stageLen);
    this._stageLen += PREFACE_LEN;
    this._outLen += PREFACE_LEN;
    this._sendSettings();
    this._flush();
    if (preQueued.length !== 0) {
      this._outQ = preQueued;
      for (let i = 0; i < preQueued.length; i++) {
        this._outLen += preQueued[i].length;
      }
      this._schedule();
    }
    // Node's ClientHttp2Session defers 'connect' unconditionally — even for an
    // already-established socket passed via createConnection — so that the caller
    // has a chance to attach a listener after connect() returns and before the
    // event fires.  Without the deferral, this pattern misses the event:
    //   const sess = lite.connect(url, { createConnection: () => alreadyOpenSocket });
    //   sess.on('connect', handler);   // attached after emit already ran synchronously
    process.nextTick(() => {
      if (this.destroyed || this._closeEmitted) {
        return;
      }
      this.emit('connect');
      if (this._queuedReq !== null) {
        const q = this._queuedReq;
        this._queuedReq = null;
        for (const r of q) {
          // node's requestOnConnect (internal/http2/core.js) opens with
          // `if (session === undefined || session.destroyed) return;` -- and a
          // destroyed stream has already cleared its own kSession (_destroy:2546).
          // So a request the application abandoned while the socket was still
          // connecting (req.destroy(), an aborted signal) submits nothing: the
          // server never sees a stream for it. A session closed in the meantime
          // refuses the request instead, with the same GOAWAY_SESSION error
          // request() itself would have produced.
          if (r.stream.destroyed) {
            continue;
          }
          if (this.closed) {
            r.stream.destroy(httpErr('ERR_HTTP2_GOAWAY_SESSION',
              'New streams cannot be created after receiving a GOAWAY'));
            continue;
          }
          this._enqueue(r);
        } // respects the peer's concurrency limit
      }
    });
  }

  // ---- outbound ---------------------------------------------------------------
  // A payload reaches the socket either by reference or by copy, and the difference is
  // who owns the memory. `owned` says it may be kept by reference: the buffer is either
  // one this session allocated for the frame (SETTINGS, RST_STREAM, GOAWAY, PING) or a
  // view of what the application handed to stream.write()/end(), which it may not mutate
  // before the bytes are on the wire -- the same guarantee node:http2 gives. Bodies above
  // BODY_MIN take that path and are never copied. Everything else travels by copy into the
  // staging buffer, because it is a view into memory the next call rewrites -- the HPACK
  // scratch block -- and those payloads are tens of bytes.
  _frame(type, flags, sid, payload, owned) {
    const len = payload === null || payload === undefined ? 0 : payload.length;
    if (owned && len >= BODY_MIN) {
      this._commit(); // what is staged was produced earlier, so it
      // has to go out first
      const head = Buffer.allocUnsafe(HEADER_LEN);
      writeHeader(head, 0, len, type, flags, sid);
      this._outQ.push(head, payload);
    } else {
      this._need(HEADER_LEN + len);
      const off = this._stageLen;
      writeHeader(this._stage, off, len, type, flags, sid);
      if (len) {
        payload.copy(this._stage, off + HEADER_LEN);
      }
      this._stageLen = off + HEADER_LEN + len;
    }
    this._outLen += HEADER_LEN + len;
    if (!this._inBatch) {
      this._schedule();
    }
  }

  // Room in the staging buffer for n more bytes, committing what it holds to the queue
  // if it does not fit. A buffer that goes into _outQ is never written into again --
  // those bytes may already be on their way to the peer -- so a full buffer is handed
  // over and a fresh one started, one allocation per batch instead of one per frame.
  _need(n) {
    if (this._stage !== null && this._stageLen + n <= this._stage.length) {
      return;
    }
    this._commit();
    let cap = this._stageCap;
    while (cap < n) {
      cap *= 2;
    }
    this._stageCap = Math.min(cap, MAX_OUT_BATCH);
    this._stage = Buffer.allocUnsafe(cap);
  }

  _commit() {
    if (this._stageLen === 0) {
      return;
    }
    this._outQ.push(this._stage.subarray(0, this._stageLen));
    this._stage = null; // the queued bytes are not ours to write
    this._stageLen = 0;
  }

  // Frames produced outside a read batch -- a request made from an 'open' handler or
  // after an await -- would otherwise sit in the queue until the next frame arrived.
  // Node drains the tick queue before microtasks, so one microtask per batch still
  // coalesces everything produced in the same turn of the loop into a single write.
  _schedule() {
    if (this._flushScheduled) {
      return;
    }
    this._flushScheduled = true;
    queueMicrotask(this._flushSoon);
  }

  _flush() {
    // leaving a batch, whatever the reason: from here on frames are produced outside a
    // socket read and have to schedule their own write
    this._inBatch = false;
    this._writePending();
  }

  // Hands the queued frames to the socket. The batch is written corked, so the whole of
  // it goes out through one _writev -- both transports implement it -- which is what
  // keeps a HEADERS + DATA + WINDOW_UPDATE batch in a single TCP segment even though the
  // frames were never concatenated into one buffer. Only one batch is ever in flight:
  // the buffers handed over belong to the socket until the callback fires (a TLS socket
  // encrypts at flush time, well after the next read), and an application that keeps
  // writing into a buffer it passed to stream.end() would otherwise change bytes already
  // promised. The queue fills up behind the outstanding write and leaves on _onWritten.
  _writePending() {
    if (this._outLen === 0) {
      return;
    }
    // Nothing leaves before the handshake is laid down: a frame flushed to the socket
    // ahead of PREFACE + SETTINGS makes the peer read it as a pre-preface violation
    // (nghttp2 answers GOAWAY(INTERNAL_ERROR)). _start sets _started and drives the
    // first write itself; until then queued frames simply wait in the staging buffer.
    if (!this._started) {
      return;
    }
    if (!this._open) {
      this._discardOut(); return;
    }
    if (this._writing || !this._socket.writable) {
      return;
    }
    this._writing = true;
    this._commit();
    const batch = this._outQ;
    this._outQ = [];
    this._outLen = 0;
    const s = this._socket;
    if (batch.length === 1) {
      // the common case: everything the batch holds went into one staging buffer, so
      // there is nothing to coalesce and corking would only add a round trip through the
      // stream's own buffer (OPTIMIZATION.md item 16).
      s.write(batch[0], this._onWrittenCb);
    } else {
      // one callback for the batch, put on its last chunk: writes complete in order, so
      // the last one is the point at which every buffer in it is the socket's no longer
      s.cork();
      for (let i = 0; i < batch.length - 1; i++) {
        s.write(batch[i]);
      }
      s.write(batch[batch.length - 1], this._onWrittenCb);
      s.uncork();
    }
  }

  _onWritten() {
    this._writing = false;
    // Dropping the batch is the whole release: the frames were never assembled into
    // anything shared, so there is no tail to move down and nothing to reset.
    this._writePending();
  }

  _discardOut() {
    this._outQ = [];
    this._stage = null;
    this._stageLen = 0;
    this._outLen = 0;
  }

  _sendSettings() {
    const s = this.localSettings;
    const names = [...this._advertised].filter((k) => ID_BY_SETTING[k] !== undefined
      && s[k] !== undefined);
    const body = Buffer.allocUnsafe(names.length * 6);
    for (let i = 0; i < names.length; i++) {
      const name = names[i];
      const value = typeof s[name] === 'boolean' ? (s[name] ? 1 : 0) : s[name];
      const at = i * 6;
      body.writeUInt16BE(ID_BY_SETTING[name], at);
      body.writeUInt32BE(value >>> 0, at + 2);
    }
    this._frame(SETTINGS, 0, 0, body, true);
    this.pendingSettingsAck = true;
    this._settingsPending++;
    // The connection's own first SETTINGS occupies a slot in the ACK queue with no
    // callback, exactly as it occupies one in nghttp2's pending count: its ACK emits
    // 'localSettings' and answers nothing else. Without the slot the first ACK would
    // answer the application's first settings() call instead, and the callback would
    // fire one ACK early (measured against a live node 26 client: event, callback, event).
    this._settingsAcks.push({ cb: undefined, values: [] });
  }

  // request(): the node-shaped surface. Accepts an object, an array of pairs, or
  // nothing; fills the request pseudo-headers from the session's scheme/authority
  // unless the application supplied them; returns a ClientHttp2Stream.
  request(headersParam, options) {
    // Argument validation stays synchronous, but a session-state failure is deferred
    // to the returned stream: node's request() captures it here and dispatches
    // `process.nextTick(requestOnError)` -- `this.destroy(error)` -- instead of
    // submitting (internal/http2/core.js:1836-1844, :1930-1931). The reason is in
    // node's own comment: a retry issued from a stream callback must not throw
    // before the session's lifecycle handlers have run.
    let requestError;
    if (this.destroyed) {
      requestError = httpErr('ERR_HTTP2_INVALID_SESSION', 'The session has been destroyed');
    } else if (this.closed) {
      requestError = httpErr('ERR_HTTP2_GOAWAY_SESSION',
        'New streams cannot be created after receiving a GOAWAY');
    }
    // No defensive copy of options: node spreads it because it validates and
    // then MUTATES the copy (setAndValidatePriorityOptions, options.endStream =
    // ... in internal/http2/core.js request()); h2lean has no priority options
    // and only reads endStream below, so the copy had no reader -- one object
    // allocation per request removed. A null options now throws where it used
    // to be silently accepted; node throws ERR_INVALID_ARG_TYPE there too
    // (assertIsObject, measured live), so the throw is the closer shape.
    const list = [];
    let method = DEFAULT_METHOD;
    let sawMethod = false;

    const src = headersParam === undefined ? {} : headersParam;
    if (this._strictSingle) {
      // the duplicate check rides _addField's own loop, the way node's
      // processHeader runs it inside buildNgHeaderString's one pass; the scratch
      // is dropped here -- reassigned, not length-cleared, for the same
      // right-trim reason as the module scratch in headers.js (OPTIMIZATION.md item 38)
      this._singlesSeen = [];
    }
    if (Array.isArray(src)) {
      for (let i = 0; i + 1 < src.length; i += 2) {
        this._addField(list, src[i], src[i + 1]);
      }
    } else {
      const keys = Object.keys(src);
      for (let i = 0; i < keys.length; i++) {
        this._addField(list, keys[i], src[keys[i]]);
      }
    }
    // list may already contain app pseudo-headers; extract method for the endStream
    // default and the pseudo-header completion node performs.
    for (let i = 0; i < list.length; i += 2) {
      if (list[i] === ':method') {
        method = list[i + 1]; sawMethod = true; break;
      }
    }
    if (!sawMethod) {
      list.push(':method', method);
    }

    let endStream = options === undefined ? undefined : options.endStream;
    if (endStream === undefined) {
      endStream = payloadMeaningless(method);
    }

    const stream = new ClientHttp2Stream(this, this._nextSid, {});
    // node records what the application passed at this point (kSentHeaders / kRawHeaders,
    // internal/http2/core.js:1902-1904); the getter folds it into the request map on read.
    stream._sentSrc = headersParam;
    this._trackClose(stream);
    this._nextSid += 2; // the id is reserved now, HEADERS may wait
    // RFC 8336: a 421 (Misdirected Request) answer removes the origin that request
    // was sent to from `originSet`. node records the origin on the stream at
    // request() time -- `stream[kOrigin] = ${scheme}://${authority}` (core.js), where
    // `scheme`/`authority` are the values `prepareRequestHeadersObject`/`...Array`
    // (util.js) resolved for this request: the application's own ':scheme', or
    // `session[kProtocol].slice(0, -1)` when it gave none; and `getAuthority()`'s
    // choice of ':authority', then 'host', then `session[kAuthority]`. The pseudo-
    // headers are only filled into the outbound list later, in _sendRequest, so the
    // same resolution is done here against the list as it stands.
    let originScheme, originAuthority;
    for (let i = 0; i < list.length; i += 2) {
      const key = list[i];
      if (key === ':scheme') {
        if (originScheme === undefined) {
          originScheme = list[i + 1];
        }
      } else if (key === ':authority') {
        originAuthority = list[i + 1]; break;
      } else if (key === 'host' && originAuthority === undefined) {
        originAuthority = list[i + 1];
      }
    }
    stream._origin = `${originScheme === undefined ? this._scheme : originScheme}`
      + `://${originAuthority === undefined ? this._authority : originAuthority}`;
    stream.endAfterHeaders = endStream;
    // node's ClientHttp2Session.request() reads options.waitForTrailers to set
    // STREAM_FLAGS_HAS_TRAILERS on the stream, which is what defers the END_STREAM
    // on the last DATA and emits 'wantTrailers'. The flag is only meaningful while
    // the writable half is not already finished by HEADERS (a GET has nothing to
    // trailer) -- node's C++ sets the flag unconditionally, but nghttp2 will not
    // emit wantTrailers for a stream whose writable half is already ended, so h2lean
    // mirrors that observable behaviour.
    stream._waitForTrailers = !!(options && options.waitForTrailers) && !endStream;
    if (endStream) {
      stream.end();
    }
    // node's ClientHttp2Session.request() wires options.signal via
    // internal/http2/core.js: `validateAbortSignal`, then an abort listener that
    // destroys the stream with an AbortError whose cause is signal.reason. The
    // listener is disposed on stream 'close'. An already-aborted signal destroys
    // the stream immediately, without a wire frame. The error has to be an instance
    // of the class, not merely named 'AbortError': _destroy derives CANCEL from
    // `err instanceof AbortError` (core.js:2516) and INTERNAL_ERROR from anything else.
    const signal = options && options.signal;
    if (signal !== undefined && signal !== null) {
      if (typeof signal !== 'object' || typeof signal.addEventListener !== 'function'
        || typeof signal.removeEventListener !== 'function'
        || typeof signal.aborted !== 'boolean') {
        const err = new TypeError(
          'The "options.signal" argument must be an instance of AbortSignal');
        err.code = 'ERR_INVALID_ARG_TYPE';
        throw err;
      }
      const aborter = () => {
        stream.destroy(new AbortError(signal.reason));
      };
      if (signal.aborted) {
        aborter();
      } else {
        signal.addEventListener('abort', aborter, { once: true });
        stream.once('close', () => {
          signal.removeEventListener('abort', aborter);
        });
      }
    }

    // hp: the original headersParam object. When the application reuses the same
    // object across requests, the HPACK encoder produces identical bytes every time
    // after the dynamic table stabilises, and _sendRequest can use _blocks to replay
    // the encoded block without re-encoding. (The server's shareHeaders equivalent.)
    const req = { stream, list, end: endStream, hp: headersParam };
    if (requestError !== undefined) {
      // node's requestOnError: destroy the stream with the captured error on the
      // next tick, and submit nothing. The id reserved above is never used -- the
      // session is destroyed or past its GOAWAY mark, so no later request takes it.
      process.nextTick(() => stream.destroy(requestError));
    } else if (this._started) {
      this._enqueue(req);
    } else {
      (this._queuedReq || (this._queuedReq = [])).push(req);
    }
    return stream;
  }

  // Open a request if a concurrency slot is free, otherwise park it. The peer's
  // SETTINGS_MAX_CONCURRENT_STREAMS bounds how many streams may be open at once; a
  // request beyond that limit keeps its reserved id but its HEADERS wait in the queue
  // until an open stream closes (RFC 9113 5.1.2, 6.5.2).
  //
  // The request is NOT held back until the peer's SETTINGS have been applied. Node
  // does not hold it back either: its client emits 'connect' as soon as the socket
  // is up (internal/http2/core.js setupHandle -> nextTick(emit, 'connect')) and
  // submits the request then, so the request goes out in the first flight. Measured
  // against a raw server that delays its SETTINGS by 200 ms: node's request HEADERS
  // arrive at 5 ms, while h2lean's used to arrive at 225 ms -- after the SETTINGS it
  // was waiting for -- which is a round trip on every session's first request
  // (tools/probe-client-first.js).
  //
  // What the peer's SETTINGS would have told us is applied when it is known: the
  // window a stream runs on is taken when the stream is opened (_openStream), and a
  // peer that will not take the stream answers REFUSED_STREAM -- which is what
  // node's client gets for a burst fired before the peer's SETTINGS land too.
  _enqueue(req) {
    const max = this.remoteSettings.maxConcurrentStreams;
    if (this._outboundQueue.length === 0
      && (max === undefined || this._streams.size < max)) {
      this._openStream(req);
    } else {
      this._outboundQueue.push(req);
    }
  }

  _openStream(req) {
    // RFC 9113 6.9.2: a stream runs on the peer's SETTINGS_INITIAL_WINDOW_SIZE. The
    // stream object was built when the application asked for the request, which may
    // have been before the peer's SETTINGS arrived -- and the delta a later SETTINGS
    // applies reaches only the streams that are open by then (see _applySettings).
    // Taking the window here, where the stream is actually opened, is what makes it
    // the peer's number and not the 65535 default the peer may never have chosen.
    req.stream._sendWin = this.remoteSettings.initialWindowSize;
    this._streams.set(req.stream.id, req.stream);
    this._sendRequest(req.stream, req.list, req.end, req.hp);
  }

  // A slot just freed (a stream closed): start queued requests while the peer's
  // concurrency limit and the live connection allow.
  _drainQueue() {
    if (this.destroyed || this._closeEmitted) {
      return;
    }
    const max = this.remoteSettings.maxConcurrentStreams;
    while (this._outboundQueue.length > 0 && (max === undefined || this._streams.size < max)) {
      const req = this._outboundQueue.shift();
      if (this.closed && req.stream.id > (this._goawayLastStreamID || 0)) {
        // past the peer's GOAWAY mark: this request will not be carried; surface it.
        req.stream.destroy(httpErr('ERR_HTTP2_GOAWAY_SESSION',
          'New streams cannot be created after receiving a GOAWAY'));
        continue;
      }
      this._openStream(req);
    }
  }

  // Normalises one application-supplied field into the flat [name, value, ...] list:
  // lower-cases names, coerces pseudo/regular, rejects connection-specific fields
  // (RFC 9113 8.2.2) and a non-token name (RFC 9113 8.2.1) exactly as node's
  // prepareRequestHeaders* does for a client request.
  //
  // The strict single-value rule rides this same loop, the way node's processHeader
  // runs it inside buildNgHeaderString's one pass (internal/http2/util.js): a
  // multi-element array on one of the fields throws before anything is pushed, and
  // a second copy of a field already seen throws against _singlesSeen. Both verdicts
  // were measured on v26.12.1 -- the array case before the pseudo-header is looked
  // at, which is why the check stands above the pseudo branch; an empty array is
  // skipped before any bookkeeping (node's `case 0: return`), so it neither pushes
  // a value nor spends a slot in _singlesSeen.
  _addField(list, rawKey, rawValue) {
    if (rawValue === undefined) {
      return;
    }
    const key = lowerName(rawKey);
    const isArray = Array.isArray(rawValue);
    if (isArray && rawValue.length === 0) {
      return;
    }
    if (this._strictSingle && SINGLE_VALUE_FIELDS.has(key)) {
      if (isArray && rawValue.length > 1) {
        throw httpErr('ERR_HTTP2_HEADER_SINGLE_VALUE',
          `Header field "${key}" must only have a single value`);
      }
      const seen = this._singlesSeen;
      if (seen.indexOf(key) !== -1) {
        throw httpErr('ERR_HTTP2_HEADER_SINGLE_VALUE',
          `Header field "${key}" must only have a single value`);
      }
      seen.push(key);
    }
    // The colon test is spelled as a code-point read, where lib/headers.js uses
    // `key[0] === ':'`. Not a style difference: the one-character read boxes a new
    // one-char string, and charCodeAt is the cheaper spelling on every input --
    // the fall-through case and the match case alike (OPTIMIZATION.md item 33).
    if (key.charCodeAt(0) === 58 /* : */) {
      if (key !== ':method' && key !== ':path' && key !== ':scheme' && key !== ':authority'
        && key !== ':protocol') {
        throw httpErr('ERR_HTTP2_PSEUDOHEADER_NOT_ALLOWED', `cannot send ${key}`);
      }
      list.push(key, String(isArray ? rawValue[0] : rawValue));
      return;
    }
    if (!isFieldName(key)) {
      throw httpErr('ERR_INVALID_HTTP_TOKEN', `${key} is an invalid header token`);
    }
    if (CONNECTION_SPECIFIC.has(key)) {
      throw httpErr('ERR_HTTP2_INVALID_CONNECTION_HEADERS',
        `HTTP/1 Connection specific header is forbidden: ${key}`);
    }
    if (isArray) {
      for (let j = 0; j < rawValue.length; j++) {
        list.push(key, String(rawValue[j]));
      }
    } else {
      list.push(key, String(rawValue));
    }
  }

  _sendRequest(stream, list, end, hp) {
    // Fill the pseudo-headers the application omitted, in the canonical order, then
    // hand the whole list to HPACK. node completes :path to '/', :scheme and
    // :authority from the session; we mirror that so `request({})` is a GET /.
    //
    // The outbound block cache: when the same headersParam object is passed to
    // request() again and the encoder's dynamic-table epoch has not moved since
    // we last encoded it, the HPACK block is byte-identical to the cached form.
    // Replaying the cached bytes skips _addField, orderPseudo, and _encodeRequest
    // in their entirety -- the server's shareHeaders mechanism inverted: on the
    // server the same idea caches outbound response blocks; here it caches outbound
    // request blocks. The WeakMap entry is dropped with the object itself (no
    // explicit invalidation), and epoch mismatch handles dynamic-table changes.
    // The subarray the cache holds is stable: _encodeRequest's buf is freshly
    // allocated each call, so the view stays alive as long as the entry is reachable.
    const cached = hp !== undefined && hp !== null && typeof hp === 'object'
      ? this._blocks.get(hp) : undefined;
    let block;
    let bound;
    if (cached !== undefined && cached.epoch === this._encoder.epoch) {
      block = cached.block;
      bound = cached.bound;
    } else {
      if (!list.includes(':path')) {
        list.push(':path', '/');
      }
      if (!list.includes(':scheme')) {
        list.push(':scheme', this._scheme);
      }
      if (!list.includes(':authority')) {
        list.push(':authority', this._authority);
      }
      const ordered = orderPseudo(list);
      block = this._encodeRequest(ordered);
      bound = sentBound;
      if (hp !== undefined && hp !== null && typeof hp === 'object') {
        this._blocks.set(hp, { epoch: this._encoder.epoch, block, bound });
      }
    }
    stream._sid = stream.id;
    if (this._sendHeaders(stream.id, block, end, bound, true)) {
      // The HEADERS never reached the wire, so the stream never opened: no body may
      // follow it and _headersSent stays false. The deferred close in _refuseBlock is
      // what finishes the Duplex.
      return;
    }
    // HEADERS is now queued; release any body the application wrote before the
    // socket connected (or before this queued request was framed). The DATA frames
    // land in _out behind the HEADERS, so the wire order stays HEADERS -> DATA.
    stream._headersSent = true;
    stream.headersSent = true;
    if (stream._pending !== null || stream._finalCb !== null) {
      this._pump(stream);
    }
  }

  _encodeRequest(list) {
    const enc = this._encoder;
    // The dynamic-table-size announcement has to precede the block it applies to, and its
    // length is not known before the block is written: the field lines are what change the
    // table, and the announcement reports where the table ended up. So the take happens
    // first and its octets are copied into the headroom of the same buffer the block is
    // encoded into -- never into a second one.
    const update = enc.takeSizeUpdate();
    let cap = 0;
    // nghttp2's outbound estimate, accumulated in the pass that already sizes the
    // scratch -- the same formula and the same string-length-not-octets rule
    // lib/headers.js encodeAt states. Published in a module slot rather than returned
    // alongside the buffer: the block cache below has to carry it too, and a second
    // return value would mean an object per request.
    let bound = 12;
    for (let i = 0; i < list.length; i += 2) {
      cap += Encoder.maxBytesOf(list[i], list[i + 1]);
      bound += 12 + list[i].length + list[i + 1].length;
    }
    sentBound = bound;
    // Without a size update the block IS the scratch: _sendHeaders runs before any later
    // encode can rewrite it, and _frame(owned=false) puts a stable copy in the batch.
    // With one, the old form was Buffer.concat([update, block]) -- a second allocation,
    // a second copy of the whole block and a temporary array per request. Reserving the
    // update's length up front costs one memcpy of the update itself
    // (OPTIMIZATION.md item 14).
    // The headroom exists on this branch only, and the returned view then starts past
    // the front of its buffer -- which every consumer here reads as
    // .length/.copy/write(), all of which honour byteOffset.
    const ul = update === null ? 0 : update.length;
    const buf = Buffer.allocUnsafe(ul + cap);
    enc.unsettled = false;
    let off = ul;
    for (let i = 0; i < list.length; i += 2) {
      off = enc.write(buf, off, list[i], list[i + 1], false);
    }
    if (ul === 0) {
      return buf.subarray(0, off);
    }
    update.copy(buf, 0);
    return buf.subarray(0, off);
  }

  // Returns true when the block was refused. `opening` says whether this HEADERS is the
  // one that opens the stream on the wire, which is what decides the refusal's shape:
  // nghttp2's prep-error path closes a stream-opening request HEADERS itself with
  // NGHTTP2_REFUSED_STREAM (nghttp2_session.c, session_prep_frame), while a trailer block
  // is refused on a stream that is already open and nghttp2 closes nothing -- so there
  // node's own deferred close(FRAME_SIZE_ERROR) is what lands. Both were measured on
  // v26.12.1 against a node server: the request refusal puts NOTHING on the wire (the
  // server sees no stream at all and its session closes with goawayCode 0) and reports
  // rstCode 7; the trailer refusal puts RST_STREAM(6) out and the peer reports
  // ERR_HTTP2_STREAM_ERROR / rstCode 6.
  _sendHeaders(sid, block, end, bound, opening) {
    if (bound + PRIORITY_SPECLEN > this._maxSendBlock) {
      this._refuseBlock(sid, opening ? REFUSED_STREAM : FRAME_SIZE_ERROR, !opening);
      return true;
    }
    const max = this.remoteSettings.maxFrameSize;
    // END_STREAM belongs on the HEADERS frame, not on the CONTINUATION that finishes the
    // block -- RFC 9113 6.2 / 5.1 / 8.1, the same rule lib/session.js sendHeaders states.
    const es = end ? F_END_STREAM : 0;
    if (block.length <= max) {
      // owned=false: the batch keeps a copy, because `block` may be the encoder
      // scratch; one memcpy, and the double allocation the old by-reference path cost
      // is gone.
      this._frame(HEADERS, F_END_HEADERS | es, sid, block, false);
      return false;
    }
    this._frame(HEADERS, es, sid, block.subarray(0, max), false);
    let off = max;
    for (;;) {
      const n = Math.min(block.length - off, max);
      const last = off + n >= block.length;
      this._frame(CONTINUATION, last ? F_END_HEADERS : 0,
        sid, block.subarray(off, off + n), false);
      off += n;
      if (last) {
        return false;
      }
    }
  }

  // node's onFrameError (internal/http2/core.js:677): 'frameError' goes out synchronously
  // on the stream while the session still holds it, and the close is deferred a turn so a
  // GOAWAY already in flight can arrive first. `onWire` is whether this stream's HEADERS
  // reached the peer -- see _sendHeaders for why the two refusals differ.
  _refuseBlock(sid, code, onWire) {
    const stream = this._streams.get(sid);
    (stream === undefined ? this : stream).emit('frameError', HEADERS, FRAME_SIZE_ERROR, sid);
    setImmediate(() => {
      if (stream !== undefined && !stream.destroyed) {
        // closeStream (core.js:2071) records the code and finishes the writable half the
        // application left open. Recording it is what makes the destroy below derive the
        // error from this code instead of overriding it with INTERNAL_ERROR.
        stream._reset = true;
        stream._closeStream(code);
        // The RST_STREAM is submitted here with the refusal's own code, not left to the
        // Duplex's _destroy. node submits it from closeStream (core.js:2095) before the
        // destroy derives the error from rstCode, and the peer is measured to report the
        // code this stream closed with. Both branches drop the id, so the rstStream inside
        // _destroy finds nothing and puts no second reset on the wire.
        if (onWire) {
          this.rstStream(sid, code);
        } else {
          // nghttp2 closed the stream itself at prep time, so no RST_STREAM is
          // submitted for it at all -- a reset for a stream the peer never saw would
          // be a connection error (measured: the server arm sees nothing).
          this._dropStream(sid);
        }
        // No error argument: onStreamClose destroys with none (core.js:624) and _destroy
        // synthesizes ERR_HTTP2_STREAM_ERROR from rstCode, which is null for codes 0 and 8.
        stream.destroy();
      }
      this.close();
    });
  }

  // Flow-controlled send for the writable half of a stream. Called from the Duplex's
  // _write/_final and again from WINDOW_UPDATE / SETTINGS to resume a stalled body.
  _pump(stream) {
    // The peer reset this stream. nghttp2 refuses DATA and HEADERS on a closed stream
    // ("After calling close ... no DATA and HEADERS frames may be sent",
    // internal/http2/core.js), but closeStream called end() anyway, so the Duplex still
    // owes its 'finish'. Settle the writable here rather than frame an END_STREAM for a
    // stream the peer has already closed.
    if (stream._reset) {
      stream._pending = null;
      stream._pendingOff = 0;
      const cb = stream._writeCb;
      if (cb !== null) {
        stream._writeCb = null; cb();
      }
      stream._writableDone();
      return;
    }
    // No bytes may leave before the stream's HEADERS: DATA on an unopened stream id
    // is a connection error, and nghttp2 answers it with GOAWAY(INTERNAL_ERROR). A
    // write() that arrives before 'connect' parks its payload in _pending and waits
    // for _sendRequest to flip _headersSent and call us again.
    if (!stream._headersSent) {
      return;
    }
    const max = this.remoteSettings.maxFrameSize;
    // waitForTrailers defers the END_STREAM: the last DATA goes out without the flag,
    // 'wantTrailers' fires on the next tick, and sendTrailers() puts HEADERS with
    // END_STREAM later. Mirrors node's STREAM_FLAGS_HAS_TRAILERS path in core.js
    // finishWrite -- shutdownWritable is skipped so the wire stays open, and the
    // Duplex's 'finish' is still emitted because finishWrite itself runs.
    const deferEs = stream._waitForTrailers;
    while (stream._pending !== null) {
      const room = Math.min(this._connSendWin, stream._sendWin, max);
      if (room <= 0) {
        return;
      } // stalled; WINDOW_UPDATE resumes
      const avail = stream._pending.length - stream._pendingOff;
      const take = Math.min(room, avail);
      const finishedByte = (stream._pendingOff + take) === stream._pending.length;
      const es = !deferEs && finishedByte && stream._writeCb === null && stream._finalCb !== null;
      const slice = (stream._pendingOff === 0 && take === stream._pending.length)
        ? stream._pending : stream._pending.subarray(stream._pendingOff, stream._pendingOff + take);
      this._frame(DATA, es ? F_END_STREAM : 0, stream.id, slice, true);
      this._connSendWin -= take;
      stream._sendWin -= take;
      stream._pendingOff += take;
      if (stream._pendingOff === stream._pending.length) {
        const cb = stream._writeCb;
        stream._pending = null;
        stream._pendingOff = 0;
        if (cb !== null) {
          stream._writeCb = null; cb();
        }
        if (es) {
          stream._writableDone(); return;
        }
        if (deferEs && finishedByte && stream._finalCb !== null && stream._writeCb === null) {
          // node's client order: 'wantTrailers' fires from nghttp2's shutdown
          // callback and THEN the Duplex 'finish' runs via finishWrite's cb. Do
          // the same here: emit synchronously into the stream's emit queue, then
          // hand the writable back so the Duplex's _final callback runs after.
          emitWantTrailers(stream);
          stream._writableDone();
          return;
        }
        break; // a further write may follow
      }
    }
    // Everything is framed and we are finalizing: close the message with END_STREAM,
    // unless the GET put it on HEADERS already.
    if (stream._pending === null && stream._finalCb !== null && stream._writeCb === null
      && !stream._endSent) {
      if (stream.endAfterHeaders) {
        stream._writableDone(); return;
      }
      if (deferEs) {
        emitWantTrailers(stream);
        stream._writableDone();
        return;
      }
      this._frame(DATA, F_END_STREAM, stream.id, null, false);
      stream._writableDone();
    }
  }

  // Put a trailer HEADERS+END_STREAM block on the wire for a stream whose last DATA
  // withheld its END_STREAM flag (waitForTrailers). Called by ClientHttp2Stream.
  // sendTrailers(). Mirrors what the server path does in lib/stream.js: encode the
  // field lines through the session's HPACK encoder (never a pseudo-header) and hand
  // them to _sendHeaders with end=true.
  _sendTrailers(stream, trailers) {
    if (this.destroyed || stream.destroyed) {
      return;
    }
    const list = [];
    this._singlesSeen = [];
    const src = trailers === undefined || trailers === null ? {} : trailers;
    if (Array.isArray(src)) {
      for (let i = 0; i + 1 < src.length; i += 2) {
        this._addField(list, src[i], src[i + 1]);
      }
    } else {
      const keys = Object.keys(src);
      for (let i = 0; i < keys.length; i++) {
        this._addField(list, keys[i], src[keys[i]]);
      }
    }
    // RFC 9113 8.1: a trailer block carries no pseudo-header. _addField throws
    // ERR_HTTP2_PSEUDOHEADER_NOT_ALLOWED for one, and node does the same here.
    const block = this._encodeRequest(list);
    const bound = sentBound;
    if (block.length === 0) {
      // An empty trailer map is not a HEADERS frame: the END_STREAM that waitForTrailers
      // withheld from the body rides on a zero-length DATA instead (the same rule the
      // server path applies in lib/stream.js sendTrailers, measured against node). A
      // pending table-size update makes the block non-empty, so it is never dropped here.
      this._frame(DATA, F_END_STREAM, stream.id, null, false);
    } else {
      if (this._sendHeaders(stream.id, block, true, bound, false)) {
        // The END_STREAM this block carried never went out; _refuseBlock's deferred
        // close finishes the writable half instead.
        return;
      }
    }
    stream._endSent = true;
    this._flush();
  }

  // ---- inbound ----------------------------------------------------------------
  // The server's parse loop, mirrored: a frame that fits inside the chunk is handled
  // in place with no copy at all, and a straddling frame is kept as the views it
  // arrived in -- a DATA payload is handed to the application as those views, a
  // header block is decoded out of them, and the control frames are put in a
  // buffer of their own for the parse. Node's socket chunks are never rewritten
  // after being handed out (measured, AGENTS.md 4), which is what makes the view
  // safe to keep.
  _onData(chunk) {
    const n = chunk.length;
    let i = 0;
    this._inBatch = true;

    while (i < n) {
      if (this._frag > 0) {
        // continuing a frame that straddles reads: take no more than it wants
        const want = this._fragTarget === null
          ? HEADER_LEN - this._frag
          : this._fragTarget - this._frag;
        const take = Math.min(want, n - i);
        if (take <= 0) {
          break;
        }
        if (this._fragPieces !== null) {
          this._fragPieces.push(chunk.subarray(i, i + take));
        } else {
          // the frame's header is still incomplete: its octets wait in the
          // scratch until the header is whole
          chunk.copy(this._fragHdr, this._frag, i, i + take);
        }
        this._frag += take;
        i += take;
        if (this._fragTarget === null && this._frag === HEADER_LEN) {
          readHeader(this._fragHdr, 0);
          if (!this._allowFrame(f.FH_TYPE, f.FH_LEN, f.FH_FLAGS)) {
            return;
          }
          this._fragTarget = HEADER_LEN + f.FH_LEN;
          this._fragType = f.FH_TYPE;
          this._fragFlags = f.FH_FLAGS;
          this._fragSid = f.FH_SID;
          // The payload of the frame is kept as the views it arrives in: a DATA
          // payload is handed to the application that way, and a header block is
          // decoded out of them (lib/hpack.js decodePieces).
          this._fragPieces = [];
        }
        if (this._fragTarget !== null && this._frag === this._fragTarget) {
          const pieces = this._fragPieces;
          const len = this._fragTarget - HEADER_LEN;
          const ok = this._handlePieces(this._fragType, this._fragFlags, this._fragSid, pieces, len);
          this._frag = 0;
          this._fragTarget = null;
          this._fragPieces = null;
          if (!ok) {
            return;
          }
        }
        continue;
      }

      if (n - i < HEADER_LEN) { // even the header is incomplete
        this._fragTarget = null;
        this._fragPieces = null;
        this._frag = n - i;
        chunk.copy(this._fragHdr, 0, i, n);
        break;
      }

      readHeader(chunk, i);
      if (!this._allowFrame(f.FH_TYPE, f.FH_LEN, f.FH_FLAGS)) {
        return;
      }
      const total = HEADER_LEN + f.FH_LEN;
      if (n - i < total) { // header known, payload straddles
        this._fragTarget = total;
        this._fragType = f.FH_TYPE;
        this._fragFlags = f.FH_FLAGS;
        this._fragSid = f.FH_SID;
        this._frag = n - i;
        // The payload is not contiguous: it starts in this chunk and ends in a
        // later one. It is kept as the views it is and handed over as the pieces
        // it arrived in, which is what node does with the same frame. A header
        // block is decoded out of them, so it costs no copy either.
        this._fragPieces = [chunk.subarray(i + HEADER_LEN, n)];
        break;
      }
      if (!this._handle(f.FH_TYPE, f.FH_FLAGS, f.FH_SID, chunk, i + HEADER_LEN, f.FH_LEN)) {
        return;
      }
      i += total;
      if (!this._open) {
        return;
      }
    }

    this._endBatch();
  }

  // What may arrive at all. RFC 9113 6.5.2 lets the peer send up to OUR advertised
  // SETTINGS_MAX_FRAME_SIZE and nothing more; a longer payload has no bounded home
  // in the session, so it is a connection error of type FRAME_SIZE_ERROR -- the same
  // rule the server applies to us. SETTINGS and PING are exempt because their
  // payloads are not frame-size governed (and SETTINGS is bounded by entry count in
  // _applySettings).
  // Unlike the server, no first-frame-is-SETTINGS rule is enforced here: that is a
  // protocol-strictness change, not a fast path, and it needs its own node comparison.
  _allowFrame(type, len, _flags) {
    if (type !== PING && type !== SETTINGS && len > this.localSettings.maxFrameSize) {
      this._fatal(FRAME_SIZE_ERROR, 'frame larger than our advertised max frame size');
      return false;
    }
    return true;
  }

  _endBatch() {
    this._inBatch = false;
    // One read batch is one accounting period: the frames that crossed their
    // thresholds during it leave together, the rest of the bytes stay owed until
    // the next batch pays them (the server's _endBatch does the same). The release
    // visits only the streams in _debt -- the ones that actually took DATA.
    this._creditRecv();
    this._flush();
  }

  // Return the flow-control credit (RFC 9113 6.9) for every stream that has
  // consumed past half its window, then for the connection's share, each as one
  // coalesced WINDOW_UPDATE. Not every consumed octet is worth a frame: node's
  // nghttp2 releases credit in half-window steps (measured on the server side,
  // session.js _creditConnection, tools/probe-conn-credit.js), and the eager
  // one-frame-per-DATA alternative measured far slower (OPTIMIZATION.md item 32).
  //
  // Each threshold is half of the window the level could ACTUALLY be run with, and the
  // honest answer to that question at both levels is the smaller of what we advertise
  // and the 65535 an endpoint uses until it has read our SETTINGS.
  //
  // The ordering is what forces the conservative number rather than our advertised one.
  // A server writes its SETTINGS before it has read ours, and it sizes a new stream's
  // send window -- and the connection window it starts answering on -- by whatever it
  // knows at that moment: the default if our SETTINGS have not landed yet, our value if
  // they have. The client cannot tell which happened, and bench/micro/probe-win.js measured both
  // shapes on nghttpd: with 262144 advertised the server sent 65535 octets on its
  // connection window and then stopped, because the release for sid 0 was planned
  // against half of 262144 and never came due. A release planned against half of the
  // lower bound is early enough for either case and, being one frame per 32767 octets,
  // is exactly node's measured cadence when both sides run the default -- which is every
  // benchmark and differential test here.
  //
  // The connection window is additionally tracked (`_connRecvWin`) so that a peer which
  // really does run it at the advertised size still gets it restored before it runs
  // out: the release is due when EITHER the half-window step says so or the window the
  // peer may fill without hearing from us drops under the step.
  _creditRecv() {
    const step = Math.min(this.localSettings.initialWindowSize, DEFAULT_WINDOW) >> 1;
    const debt = this._debt;
    // Compacted in place: a stream whose bytes have not yet crossed the half-window
    // step STAYS in the list (and stays marked), because the credit it owes is only
    // paid by a later pass. Clearing the mark without releasing would drop the debt on
    // the floor and leave the peer waiting for a WINDOW_UPDATE that never comes.
    let k = 0;
    for (let i = 0; i < debt.length; i++) {
      const stream = debt[i];
      if (stream._recvUsed >= step) {
        const inc = stream._recvUsed;
        stream._recvUsed = 0;
        stream._debtMarked = false;
        this._windowUpdate(stream.id, inc);
      } else {
        debt[k++] = stream;
      }
    }
    debt.length = k;
    if (this._connRecvUsed >= step
      || this._connRecvWin - this._connRecvUsed < step) {
      const inc = this._connRecvUsed;
      this._connRecvUsed = 0;
      this._connRecvWin += inc;
      this._windowUpdate(0, inc);
    }
    // Everything the pass produced leaves as one unit. Two exits, chosen by whether
    // anything of ours is still unflushed: with the queue and the staging buffer empty
    // and a batch in flight, the credit goes straight to the socket -- through _outQ it
    // would wait for _onWritten, a whole poll cycle, and the peer sits out that delay
    // (bench/client measured the 256 kB download spending it on every half-window step).
    // Otherwise the bytes are copied into the staging buffer, behind whatever is already
    // in it, so that a HEADERS produced in this same batch cannot be overtaken by the
    // WINDOW_UPDATE for its own stream.
    //
    // The direct write hands over a COPY, not a view of _wuBuf: that buffer is rewritten
    // by the next pass, and a chunk the socket still owns (a partial write, or a TLS
    // socket that encrypts at flush time) must not read bytes we changed -- the issued-
    // buffer rule of AGENTS.md 5. One allocation per credit batch replaces the old one
    // per update.
    if (this._wuLen !== 0) {
      if (this._writing && this._open && this._started && this._outLen === 0
        && this._socket.writable) {
        // a copy, not a view -- the reason is the comment above -- taken through
        // allocUnsafe rather than Buffer.from(subarray): the pooled allocation is the
        // cheaper way to get a fresh buffer (OPTIMIZATION.md item 18)
        const out = Buffer.allocUnsafe(this._wuLen);
        this._wuBuf.copy(out, 0, 0, this._wuLen);
        this._socket.write(out);
      } else {
        this._need(this._wuLen);
        this._wuBuf.copy(this._stage, this._stageLen, 0, this._wuLen);
        this._stageLen += this._wuLen;
        this._outLen += this._wuLen;
        this._schedule();
      }
      this._wuLen = 0;
    }
  }

  // Returns false when the session has been torn down mid-read (so the parse loop
  // stops), true to keep going. _fatal and stream teardown set _open false.
  _handle(type, flags, sid, buf, start, len) {
    // RFC 9113 6.10: only CONTINUATION may follow a HEADERS that lacks END_HEADERS.
    if (this._hdrSid !== -1 && type !== CONTINUATION) {
      return this._fatal(PROTOCOL_ERROR, 'frame interleaved with an unfinished header block');
    }
    switch (type) {
      case SETTINGS:
        if (sid !== 0) {
          return this._fatal(PROTOCOL_ERROR, 'SETTINGS with a non-zero stream id');
        }
        if (flags & F_ACK) {
          if (len !== 0) {
            return this._fatal(FRAME_SIZE_ERROR, 'SETTINGS ACK must be empty');
          }
          if (this._settingsPending > 0) {
            this._settingsPending--;
          }
          this.pendingSettingsAck = this._settingsPending > 0;
          // node's settingsCallback (core.js): the ACK answers the oldest outstanding
          // submit, the callback gets (null, localSettings, duration) and 'localSettings'
          // is emitted on EVERY ack -- including the one for the connection's own first
          // SETTINGS frame, which has no callback. Measured on a live node 26 client
          // against a raw peer: two ACKs produced two 'localSettings' events and one
          // callback, in the order event, callback, event.
          const entry = this._settingsAcks.shift();
          if (entry !== undefined) {
            for (let i = 0; i < entry.values.length; i += 2) {
              this.localSettings[entry.values[i]] = entry.values[i + 1];
            }
            this.localSettings.maxHeaderSize = this.localSettings.maxHeaderListSize;
            if (typeof entry.cb === 'function') {
              // duration is not an elapsed time: node reports its monotonic clock reading
              // in milliseconds. Measured -- a submit 120 ms earlier answered with
              // 524545981.423 where Number(process.hrtime.bigint())/1e6 read
              // 524545981.445, and a second call 200 ms later tracked the clock again
              // rather than the interval. Matching the reading, not inventing a delta.
              entry.cb(null, this.localSettings, Number(process.hrtime.bigint()) / 1e6);
            }
          }
          this.emit('localSettings', this.localSettings);
          return this._open;
        }
        this._applySettings(buf, start, len);
        this.emit('remoteSettings', this.remoteSettings);
        this._frame(SETTINGS, F_ACK, 0, null, false);
        return this._open;
      case PING:
        if (sid !== 0) {
          return this._fatal(PROTOCOL_ERROR, 'PING with a non-zero stream id');
        }
        if (len !== 8) {
          return this._fatal(FRAME_SIZE_ERROR, 'PING payload must be 8 octets');
        }
        if (flags & F_ACK) {
          const key = buf.toString('latin1', start, start + 8);
          const entry = this._pings.get(key);
          if (entry !== undefined) {
            this._pings.delete(key);
            // node's measured callback shape is (err, duration, pong): duration is a
            // millisecond number and pong is the echoed 8-octet buffer.
            const duration = Number(process.hrtime.bigint() - entry.at) / 1e6;
            // The pong goes to the application, which may keep it: a PING ACK that
            // arrived in pieces was put in a buffer of its own for the parse -- a
            // fresh one, not a reused one -- so the view does not move under the
            // application (AGENTS.md 5: issued buffers stay valid). Node's copy
            // comes out of the C++ layer as a fresh buffer, so it does not move
            // under the application there.
            // a fresh 8-octet buffer for the callback; allocUnsafe + copy rather
            // than Buffer.from(subarray) for the pooled allocation
            // (OPTIMIZATION.md item 18)
            const pong = Buffer.allocUnsafe(8);
            buf.copy(pong, 0, start, start + 8);
            entry.cb(null, duration, pong);
          }
        } else {
          this._frame(PING, F_ACK, 0, buf.subarray(start, start + 8), true);
        }
        return this._open;
      case HEADERS:
        this._beginHeaders(sid, flags, buf, start, len);
        return this._open;
      case CONTINUATION:
        if (this._hdrSid !== sid) {
          return this._fatal(PROTOCOL_ERROR, 'CONTINUATION without HEADERS');
        }
        if (++this._hdrCont > MAX_CONTINUATIONS) {
          return this._fatal(PROTOCOL_ERROR, 'CONTINUATION: too many frames for one block');
        }
        this._appendHdr(buf, start, len);
        if (flags & F_END_HEADERS) {
          this._finishHeaders();
        }
        return this._open;
      case DATA:
        this._onDataFrame(sid, flags, buf, start, len);
        return this._open;
      case WINDOW_UPDATE:
        if (len !== 4) {
          return this._fatal(FRAME_SIZE_ERROR, 'WINDOW_UPDATE payload');
        }
        this._onWindowUpdate(sid, buf.readUInt32BE(start) & 0x7fffffff);
        return this._open;
      case RST_STREAM:
        if (len !== 4) {
          return this._fatal(FRAME_SIZE_ERROR, 'RST_STREAM payload');
        }
        this._onRst(sid, buf.readUInt32BE(start));
        return this._open;
      case GOAWAY:
        return this._onGoaway(buf, start, len);
      case PRIORITY:
        if (len !== 5) {
          return this._fatal(FRAME_SIZE_ERROR, 'PRIORITY payload must be 5 octets');
        }
        if (sid === 0) {
          return this._fatal(PROTOCOL_ERROR, 'PRIORITY on stream 0');
        }
        return this._open; // contents deprecated (RFC 9218)
      case PUSH_PROMISE:
        return this._beginPushPromise(sid, flags, buf, start, len);
      case ALTSVC: {
        // RFC 7807: an informational frame. Malformed content is discarded, never
        // fatal, so the existing "unknown type 10 is ignored" test stays green. The
        // event carries the decoded field value, the origin (a URL origin string, ''
        // for a frame on a specific stream) and the stream id.
        const a = parseAltSvc(buf, start, len);
        if (a !== null) {
          this.emit('altsvc', a.alt, a.origin, sid);
        }
        return this._open;
      }
      case ORIGIN: {
        // RFC 8336: the server asserts an origin is authoritative for this
        // connection. Only meaningful over TLS; malformed content is discarded,
        // never fatal. core.js's onOrigin adds every origin, applies the
        // maxOriginSetSize cap and destroys the session with
        // ERR_HTTP2_TOO_MANY_ORIGINS on overflow, then emits 'origin' once with
        // the whole array of origin strings.
        const os = parseOrigins(buf, start, len);
        if (os === null) {
          return this._open;
        }
        if (!this.encrypted || this.destroyed) {
          return this._open;
        }
        const set = this._initOriginSet();
        for (let i = 0; i < os.length; i++) {
          if (set.size >= this._maxOriginSetSize) {
            const err = new Error(
              'The server sent more ORIGIN frames than the allowed number of ' +
              `${this._maxOriginSetSize}`);
            err.code = 'ERR_HTTP2_TOO_MANY_ORIGINS';
            this.destroy(err);
            return this._open;
          }
          set.add(os[i]);
        }
        this.emit('origin', os);
        return this._open;
      }
      default:
        return this._open; // RFC 9113 5.5: discard unknown
    }
  }

  // A frame whose payload straddled two reads is dispatched as the pieces it
  // arrived in. A DATA payload is handed to the application as those pieces; a
  // header block is decoded out of them; the control frames -- small and of a
  // fixed size -- are put in a buffer of their own for the parse, which is the
  // parse the whole-frame path uses.
  _handlePieces(type, flags, sid, pieces, len) {
    // RFC 9113 6.10: the same rule the whole-frame path applies (see _handle).
    // A header block that is unfinished may only be followed by CONTINUATION.
    if (this._hdrSid !== -1 && type !== CONTINUATION) {
      return this._fatal(PROTOCOL_ERROR, 'frame interleaved with an unfinished header block');
    }
    if (type === DATA) {
      // RFC 9113 6.1: a padded payload's views are not the payload -- the pad
      // length is at the front and the padding at the back -- so the padding
      // comes off the views and the flow control counts the data alone.
      let data = len;
      if (flags & F_PADDED) {
        if (len === 0) {
          return this._fatal(PROTOCOL_ERROR, 'padded DATA frame with an empty payload');
        }
        data = trimPadding(pieces);
        if (data < 0) {
          return this._fatal(PROTOCOL_ERROR, 'pad larger than the DATA payload');
        }
      }
      this._onDataPieces(sid, flags, pieces, data);
      return this._open;
    }
    if (type === HEADERS) {
      this._beginHeadersPieces(sid, flags, pieces, len);
      return this._open;
    }
    if (type === CONTINUATION) {
      // the same two rules the whole-frame path applies (see _handle)
      if (this._hdrSid !== sid) {
        return this._fatal(PROTOCOL_ERROR, 'CONTINUATION without HEADERS');
      }
      if (++this._hdrCont > MAX_CONTINUATIONS) {
        return this._fatal(PROTOCOL_ERROR, 'CONTINUATION: too many frames for one block');
      }
      this._appendHdrPieces(pieces, 0, len);
      if (flags & F_END_HEADERS) {
        this._finishHeaders();
      }
      return this._open;
    }
    // the control frames: the payload goes into a buffer of its own and the
    // parse is the one _handle does
    const buf = pieceConcat(pieces, 0, len);
    return this._handle(type, flags, sid, buf, 0, len);
  }

  _applySettings(buf, start, len) {
    if (len % 6 !== 0) {
      this._fatal(FRAME_SIZE_ERROR, 'SETTINGS payload size'); return;
    }
    if (len / 6 > MAX_SETTINGS_ENTRIES) {
      this._fatal(PROTOCOL_ERROR, 'SETTINGS: too many entries'); return;
    }
    const seen = this.remoteSettings;
    for (let i = 0; i + 6 <= len; i += 6) {
      const id = buf.readUInt16BE(start + i);
      const value = buf.readUInt32BE(start + i + 2);
      const name = SETTINGS_BY_ID[id];
      if (name === undefined) {
        continue;
      }
      seen[name] = value;
    }
    // A new INITIAL_WINDOW_SIZE shifts every open stream window by the delta (RFC 9113
    // 6.9.2), and may drive it negative -- the same rule the server applies inbound.
    const delta = seen.initialWindowSize - this._prevInitialWin;
    if (delta !== 0) {
      this._prevInitialWin = seen.initialWindowSize;
      for (const s of this._streams.values()) {
        s._sendWin += delta; this._pump(s);
      }
    }
    // The peer's table size bounds the encoder that builds blocks for ITS decoder.
    this._encoder.setPeerTableSize(seen.headerTableSize);
    // A SETTINGS_MAX_CONCURRENT_STREAMS just learned may open a slot for a parked
    // request (or a raised limit frees several at once).
    this._drainQueue();
    // SETTINGS can end our pending-ack wait and unblock queued sends.
    for (const s of this._streams.values()) {
      this._pump(s);
    }
  }

  _beginHeaders(sid, flags, buf, start, len) {
    let p = start;
    let l = len;
    // A payload with no pad length octet has no block at all: the octet the
    // pad length would be read from is not this frame's, so it is not read.
    // The split path reads the same (no octet, no pad), so the two agree.
    if (flags & F_PADDED) {
      // RFC 9113 6.2: "If the length of the padding is the length of the frame
      // payload or greater, the recipient MUST treat this as a connection error
      // of type PROTOCOL_ERROR." A padded payload with no octet at all has no
      // pad length to read, which is the same connection error -- node ends the
      // session with ERR_HTTP2_ERROR for both (measured,
      // tools/probe-client-hdr-pad.js); the server refuses them identically
      // (lib/session.js _headersPrefix).
      if (len < 1) {
        return this._fatal(PROTOCOL_ERROR, 'padded HEADERS frame with an empty payload');
      }
      const pad = buf[p];
      p += 1;
      l -= 1 + pad;
      if (l < 0) {
        return this._fatal(PROTOCOL_ERROR, 'pad larger than the HEADERS payload');
      }
    }
    if (flags & F_PRIORITY) {
      p += 5; l -= 5;
    }
    this._hdrCont = 0;
    if (flags & F_END_HEADERS) {
      this._processHeaders(sid, (flags & F_END_STREAM) !== 0, buf.subarray(p, p + l), flags);
      return;
    }
    this._hdrSid = sid;
    this._hdrEnd = (flags & F_END_STREAM) !== 0;
    this._hdrFlags = flags;
    this._hdrLen = 0;
    this._appendHdr(buf, p, l);
  }

  // A HEADERS frame whose payload straddled two reads. The pad length is the
  // payload's first octet and the priority block is the five after it; both are
  // read out of the views, and the header block stays in them.
  _beginHeadersPieces(sid, flags, pieces, len) {
    let p = 0;
    let l = len;
    if (flags & F_PADDED) {
      // The split path enforces the same rule as the whole-buffer one above:
      // no pad-length octet, or padding that claims more than the payload
      // holds, is the connection error of RFC 9113 6.2.
      if (len < 1) {
        return this._fatal(PROTOCOL_ERROR, 'padded HEADERS frame with an empty payload');
      }
      const pad = pieceByteAt(pieces, 0);
      p += 1;
      l -= 1 + pad;
      if (l < 0) {
        return this._fatal(PROTOCOL_ERROR, 'pad larger than the HEADERS payload');
      }
    }
    if (flags & F_PRIORITY) {
      p += 5; l -= 5;
    }
    this._hdrCont = 0;
    if (l < 0) {
      l = 0;
    }
    if (flags & F_END_HEADERS) {
      this._processBlockPieces(sid, (flags & F_END_STREAM) !== 0, pieces, p, p + l, flags);
      return;
    }
    this._hdrSid = sid;
    this._hdrEnd = (flags & F_END_STREAM) !== 0;
    this._hdrFlags = flags;
    this._hdrLen = 0;
    this._appendHdrPieces(pieces, p, p + l);
  }

  _appendHdr(buf, start, len) {
    if (this._hdrPieces !== null) {
      // The block is already kept as the views it arrived in, and this frame's
      // payload is a view of the chunk it came in -- so it joins them.
      this._hdrPieces.push(buf.subarray(start, start + len));
      this._hdrLen += len;
      return;
    }
    if (this._hdr === null) {
      this._hdr = Buffer.allocUnsafe(8192);
    }
    if (this._hdrLen + len > this._hdr.length) {
      let cap = this._hdr.length * 2;
      while (cap < this._hdrLen + len) {
        cap *= 2;
      }
      const bigger = Buffer.allocUnsafe(cap);
      this._hdr.copy(bigger, 0, 0, this._hdrLen);
      this._hdr = bigger;
    }
    buf.copy(this._hdr, this._hdrLen, start, start + len);
    this._hdrLen += len;
  }

  // The block is kept as the views it arrived in as soon as one frame of it
  // straddled a read. A block whose frames all arrived whole is kept in one
  // buffer, and the first split frame turns that buffer into the first view of
  // the list: the buffer is not written to again while the block is open, and
  // the block is decoded before the next one starts.
  _appendHdrPieces(pieces, from, to) {
    if (this._hdrPieces === null) {
      this._hdrPieces = this._hdrLen === 0 ? [] : [this._hdr.subarray(0, this._hdrLen)];
    }
    const parts = pieceSlice(pieces, from, to);
    for (let i = 0; i < parts.length; i++) {
      this._hdrPieces.push(parts[i]);
    }
    this._hdrLen += to - from;
  }

  _finishHeaders() {
    const sid = this._hdrSid, end = this._hdrEnd, flags = this._hdrFlags;
    const push = this._hdrPush;
    this._hdrPush = 0;
    if (this._hdrPieces !== null) {
      const pieces = this._hdrPieces;
      const len = this._hdrLen;
      this._hdrSid = -1;
      this._hdrLen = 0;
      this._hdrPieces = null;
      if (push !== 0) {
        this._processPushPromise(sid, push, pieceConcat(pieces, 0, len));
      } else {
        this._processBlockPieces(sid, end, pieces, 0, len, flags);
      }
      return;
    }
    const block = this._hdr.subarray(0, this._hdrLen);
    this._hdrSid = -1;
    this._hdrLen = 0;
    if (push !== 0) {
      this._processPushPromise(sid, push, block);
    } else {
      this._processHeaders(sid, end, block, flags);
    }
  }

  // ---- PUSH_PROMISE (RFC 9113 6.6) --------------------------------------------
  // A server promising a resource on a client-owned (odd) stream. The payload is
  // a 4-octet promised even stream id followed by an HPACK request-header block.
  // The block obeys the same CONTINUATION rules as HEADERS, so the assembly
  // machinery (_hdrSid/_hdrLen/_appendHdr/_finishHeaders) is reused with _hdrPush
  // holding the promised id to route the finish to _processPushPromise.
  _beginPushPromise(sid, flags, buf, start, len) {
    // RFC 9113 6.6: PUSH_PROMISE MUST be sent on a client-initiated (odd) stream
    // that is open or half-closed(remote). Stream 0 or an even id is a connection
    // error. Measured on node v26.12.1: GOAWAY(PROTOCOL_ERROR).
    if (sid === 0 || (sid & 1) === 0) {
      return this._fatal(PROTOCOL_ERROR, 'PUSH_PROMISE on a non-client stream');
    }
    // RFC 9113 6.6: a client that has set SETTINGS_ENABLE_PUSH to 0 MUST treat
    // receipt as a connection error of type PROTOCOL_ERROR (measured on node).
    if (!this.localSettings.enablePush) {
      return this._fatal(PROTOCOL_ERROR, 'PUSH_PROMISE with push disabled');
    }
    if (len < 4) {
      return this._fatal(FRAME_SIZE_ERROR, 'PUSH_PROMISE payload too short');
    }
    let p = start;
    let l = len;
    if (flags & F_PADDED) {
      if (len < 1) {
        return this._fatal(PROTOCOL_ERROR, 'padded PUSH_PROMISE with an empty payload');
      }
      const pad = buf[p];
      p += 1;
      l -= 1 + pad;
      if (l < 4) {
        return this._fatal(PROTOCOL_ERROR, 'pad larger than the PUSH_PROMISE payload');
      }
    }
    const promisedSid = buf.readUInt32BE(p) & 0x7fffffff;
    p += 4;
    l -= 4;
    // The promised id must be even and non-zero (RFC 9113 6.6).
    if (promisedSid === 0 || (promisedSid & 1) !== 0) {
      return this._fatal(PROTOCOL_ERROR, 'PUSH_PROMISE promised id is not even');
    }
    // A promised id already in use is a connection error (RFC 9113 5.1.1).
    if (this._streams.has(promisedSid)) {
      return this._fatal(PROTOCOL_ERROR, 'PUSH_PROMISE promised id already in use');
    }
    this._hdrCont = 0;
    if (flags & F_END_HEADERS) {
      this._processPushPromise(sid, promisedSid, buf.subarray(p, p + l));
      return this._open;
    }
    this._hdrSid = sid;
    this._hdrEnd = false;
    this._hdrPush = promisedSid;
    this._hdrLen = 0;
    this._appendHdr(buf, p, l);
    return this._open;
  }

  // Decode the promise block and create the pushed stream. The HPACK block MUST
  // be decoded even if the application will refuse the push, because the dynamic
  // table state advances for every block on the connection.
  _processPushPromise(parentSid, promisedSid, block) {
    const flat = FLAT;
    try {
      this._decoder.decode(block, 0, block.length, flat);
    } catch (err) {
      if (err instanceof HPackError) {
        this._fatal(COMPRESSION_ERROR, err.message);
      } else {
        throw err;
      }
      return;
    }
    const fields = flat.items;
    const fcount = flat.count;
    // maxReservedRemoteStreams: a pushed stream stays reserved until its
    // response HEADERS arrive (measured: the count releases on the HEADERS,
    // not on 'end'). Over the cap the promise is refused with RST_STREAM(CANCEL)
    // and no stream is created -- the late HEADERS/DATA for the id are then the
    // silent discards below (_maxPushSid covers them). node's nghttp2 does the
    // same refusal; the block was decoded above either way, HPACK state advances.
    let reserved = 0;
    for (const s of this._streams.values()) {
      if (s._pushed && !s.responseEmitted) {
        reserved++;
      }
    }
    if (reserved >= this._maxReservedRemoteStreams) {
      if (promisedSid > this._maxPushSid) {
        this._maxPushSid = promisedSid;
      }
      const body = Buffer.allocUnsafe(4);
      body.writeUInt32BE(CANCEL, 0);
      this._frame(RST_STREAM, 0, promisedSid, body, true);
      return;
    }
    const headers = this._toObject(fields, fcount);
    // Create the pushed stream. Its writable half is pre-ended: the client never
    // sends a request body for a push (node: writableEnded=true, writableFinished=true
    // at the 'stream' event, measured v26.12.1).
    const stream = new ClientHttp2Stream(this, promisedSid);
    this._trackClose(stream);
    stream._pushed = true;
    stream.rstCode = 0;
    stream._headersSent = true;
    stream._endSent = true;
    if (promisedSid > this._maxPushSid) {
      this._maxPushSid = promisedSid;
    }
    this._streams.set(promisedSid, stream);
    // End the writable without putting anything on the wire: _pump sees
    // _endSent=true and does nothing; _writableDone fires the Duplex final callback.
    stream.end();
    stream._writableDone();
    // The Duplex defers writableFinished to nextTick, but the application must see
    // it true at the 'stream' event (node: measured v26.12.1). Settle it now.
    stream._writableState.finished = true;
    // node emits session 'stream' with (pushedStream, requestHeaders, flags=4).
    // flags 4 = NGHTTP2_FLAG_END_HEADERS, the only flag PUSH_PROMISE carries that
    // the JS layer surfaces (measured v26.12.1). Synchronous so the application's
    // 'push' subscription is in place before the response HEADERS arrive in the
    // same read batch.
    this.emit('stream', stream, headers, 4);
  }

  // The shape cache: the server's design, ported. A block whose bytes have been
  // seen before under the same dynamic-table state is decoded from a cached field
  // list rather than through the HPACK decoder again. The cache is dropped if the
  // decoder's epoch moves (a table-size change from the peer invalidates every
  // stored index), and gives up when the miss ratio proves the workload does not
  // repeat (same heuristic as session.js _processHeaders).
  _shapeSlot(block) {
    if (this._shapeEpoch !== this._decoder.epoch) {
      this._shapes.clear();
      this._shapeEpoch = this._decoder.epoch;
    }
    return hashBlock(block);
  }

  // The same bucket for a block that arrived in pieces: the hash is over the
  // same octets, so a block that arrives whole and one that arrives split are
  // the same cache entry.
  _shapeSlotPieces(pieces, from, to) {
    if (this._shapeEpoch !== this._decoder.epoch) {
      this._shapes.clear();
      this._shapeEpoch = this._decoder.epoch;
    }
    return hashPieces(pieces, from, to);
  }

  // A header block that arrived whole and one that arrived in pieces are the
  // same block: the octets are the same, so the hash, the cache entry and the
  // fields are the same. `block` is null for the split one, and the range
  // [from, to) of `pieces` is where the block sits in the views.
  _processHeaders(sid, end, block, flags) {
    this._processBlock(sid, end, block, null, 0, 0, flags);
  }

  _processBlockPieces(sid, end, pieces, from, to, flags) {
    this._processBlock(sid, end, null, pieces, from, to, flags);
  }

  _processBlock(sid, end, block, pieces, from, to, flags) {
    const stream = this._streams.get(sid);
    const flat = FLAT;
    // The cache is disabled (null) when stream === undefined (we must decode for
    // HPACK state but there is nothing to cache it for) and when the application
    // opted out via shapeCache: false.
    const shapes = stream === undefined ? null : this._shapes;
    const slot = shapes === null ? 0
      : (block !== null ? this._shapeSlot(block) : this._shapeSlotPieces(pieces, from, to));
    const hit = shapes === null ? undefined : shapes.get(slot);
    // where this response's field list comes from: the scratch's backing array
    // on a decode, a cached copy on a shape hit -- both read by index, so the
    // valid length travels next to the list (same as the server session)
    let fields;
    let fcount;
    try {
      if (hit !== undefined && (block !== null
        ? hit.block.equals(block)
        : pieceEquals(hit.block, pieces, from, to))) {
        this._shapeHits++;
        fields = hit.flat;
        fcount = fields.length;
      } else {
        if (block !== null) {
          this._decoder.decode(block, 0, block.length, flat);
        } else {
          this._decoder.decodePieces(pieces, from, to, flat);
        }
        fields = flat.items;
        fcount = flat.count;
        if (shapes !== null) {
          if (++this._shapeMisses > 64 && this._shapeHits * 4 < this._shapeMisses) {
            this._shapes = null;
          } else {
            if (shapes.size >= MAX_SHAPES) {
              shapes.clear();
            }
            shapes.set(slot, {
              block: block !== null ? Buffer.from(block) : pieceConcat(pieces, from, to),
              flat: fields.slice(0, fcount)
            });
          }
        }
      }
    } catch (err) {
      if (err instanceof HPackError) {
        this._fatal(COMPRESSION_ERROR, err.message);
      } else {
        throw err;
      }
      return;
    }
    if (stream === undefined) {
      // HPACK is in sync (the block decoded above). nghttp2 owns the verdict for
      // a block naming no live stream (session_process_headers_frame falls
      // through to nghttp2_session_on_request_headers_received,
      // nghttp2_session.c): for a client an idle stream -- any id the client has
      // not sent, odd or even -- is a connection error (NGHTTP2_ERR_PROTO, hence
      // GOAWAY(PROTOCOL_ERROR) on the wire and ERR_HTTP2_ERROR on the stream and
      // the session), while a closed stream at or below last_sent_stream_id is
      // silently ignored (NGHTTP2_ERR_IGN_HEADER_BLOCK). Both measured on node
      // v26.12.1 (tools/probe-client-unopened-headers.js). The client allocates
      // odd ids sequentially, so the odd ids below _nextSid are exactly the
      // opened-and-closed ones.
      if ((sid & 1) === 1 && sid < this._nextSid) {
        return;
      }
      // A pushed stream the application destroyed: late frames are silently
      // ignored, the session lives (measured on node v26.12.1).
      if ((sid & 1) === 0 && sid <= this._maxPushSid) {
        return;
      }
      return this._fatal(PROTOCOL_ERROR, 'response HEADERS on an unopened stream id');
    }
    if (this._refuseHeaders(sid, stream, fields, fcount)) {
      return;
    }
    const headers = this._toObject(fields, fcount);
    // RFC 8336 3.2 + core.js emitStreamEvent: a 421 (Misdirected Request) answer
    // deletes the origin the request carried from originSet, and it does so here --
    // before 'response' is scheduled, so a handler that reads `session.originSet`
    // from inside 'response' already sees the entry gone. The helper is called for
    // its lazy-seed side effect exactly as node's `session[kState].originSet =
    // initOriginSet(session)` assignment does: on the first 421 of a session the set
    // is created from the socket before the delete, and over h2c both stacks leave
    // the set unobservable (the getter is encrypted-gated). `:status` is a number by
    // now, so the comparison is node's numeric `status === 421`.
    if (headers[':status'] === 421) {
      this._initOriginSet().delete(stream._origin);
    }
    stream._onHeaders(headers, end, flags);
    if (end) {
      this._dropStream(sid);
    }
  }

  // The response pseudo-header rules node enforces in nghttp2's C layer
  // (nghttp2_http.c nghttp2_http_on_response_headers); node's JS carries no
  // such check, so the verdicts are measured (tools/probe-client-status.js):
  // a response block without exactly one :status, or carrying a request-only
  // pseudo-header, and a trailer block with any pseudo-header at all (RFC 9113
  // 8.1) are all the same per-stream verdict -- ERR_HTTP2_STREAM_ERROR on the
  // stream, RST_STREAM(PROTOCOL_ERROR) on the wire, the session stays up.
  // Both shapes used to reach the application unvalidated. Returns true when
  // the block was refused, so the caller does not build or hand over the
  // header object.
  _refuseHeaders(sid, stream, fields, count) {
    let status = 0;
    let bad = null;
    for (let i = 0; i < count; i += 2) {
      const name = fields[i];
      if (name.charCodeAt(0) !== 58) {
        continue;
      }
      // The first block is the response and must carry exactly one :status;
      // every block after it is trailers, where any pseudo-header at all is
      // malformed (RFC 9113 8.1).
      if (!stream.responseEmitted && name === ':status') {
        status++;
      } else {
        bad = name;
        break;
      }
    }
    if (stream.responseEmitted) {
      // Trailers: legal whenever no pseudo-header appears at all -- in
      // particular they carry no :status (the node:http2 server's own
      // addTrailers blocks are the differential case,
      // test/client/client-stream.test.js).
      if (bad === null) {
        return false;
      }
    } else if (bad === null && status === 1) {
      // The response: exactly one :status and no other pseudo-header.
      return false;
    }
    // The per-stream verdict: RST_STREAM on the wire, ERR_HTTP2_STREAM_ERROR
    // on the stream, no 'response'/'trailers' event, the connection lives on.
    this.rstStream(sid, PROTOCOL_ERROR);
    stream._onRst(PROTOCOL_ERROR);
    return true;
  }

  _toObject(fields, count) {
    const o = nullProto();
    for (let i = 0; i < count; i += 2) {
      const name = fields[i];
      let value = fields[i + 1];
      if (name === ':status') {
        value = value | 0;
      }
      const have = o[name];
      if (have === undefined) {
        o[name] = name === 'set-cookie' ? [value] : value;
      } else if (name === 'cookie') {
        o[name] = `${have}; ${value}`;
      } else if (name === 'set-cookie') {
        have.push(value);
      } else if (SINGLE_VALUE_FIELDS.has(name)) {
        // node keeps the first copy and ignores the rest (toHeaderObject,
        // SINGLE_VALUE_FIELDS in lib/session.js) -- the response path must read a
        // repeated field the same way the server side does.
      } else {
        o[name] = `${have}, ${value}`;
      }
    }
    return o;
  }

  _onDataFrame(sid, flags, buf, start, len) {
    let p = start;
    let l = len;
    if (flags & F_PADDED) {
      // RFC 9113 6.1: the payload is the pad length, the data, and the padding.
      // A padded frame with no payload has no pad length to read, and node
      // answers that with GOAWAY(PROTOCOL_ERROR) (measured, tools/probe-pad-bad.js).
      if (len === 0) {
        this._fatal(PROTOCOL_ERROR, 'padded DATA frame with an empty payload'); return;
      }
      const pad = buf[p];
      p += 1;
      l -= 1 + pad;
      if (l < 0) {
        this._fatal(PROTOCOL_ERROR, 'pad larger than the DATA payload'); return;
      }
    }
    const stream = this._streams.get(sid);
    if (stream === undefined) {
      return;
    }
    // Handed over as a view, the way the server hands DATA to its application: it is
    // a slice of the chunk node gave us, which node never rewrites after delivery.
    // A padded frame's payload is a view too -- the pad length and the padding are
    // taken off the views before this point (frames.js trimPadding). The old
    // copy-out existed only because the client used to accumulate every read into a
    // shared buffer it reused -- that buffer is gone.
    stream._onData(buf.subarray(p, p + l));
    // Book the consumed bytes as credit owed to BOTH windows; the frames themselves
    // wait for the half-window threshold at the end of this read batch. Marking the
    // stream in _debt is what tells that pass which streams to visit.
    if (l > 0) {
      this._connRecvUsed += l;
      stream._recvUsed += l;
      if (!stream._debtMarked) {
        stream._debtMarked = true; this._debt.push(stream);
      }
    }
    if (flags & F_END_STREAM) {
      stream._endRead(); this._dropStream(sid);
    }
  }

  // A payload that straddled two reads: one piece per read, each a view of the
  // chunk node read off the socket. No copy, and the pieces are the frame's bytes
  // in the order the peer sent them.
  _onDataPieces(sid, flags, pieces, total) {
    const stream = this._streams.get(sid);
    if (stream === undefined) {
      return;
    }
    for (let i = 0; i < pieces.length; i++) {
      // A piece the padding took all of is empty, and an empty chunk is not a
      // chunk: node emits no 'data' for one.
      if (pieces[i] !== null) {
        stream._onData(pieces[i]);
      }
    }
    // Book the consumed bytes as credit owed to BOTH windows; the frames themselves
    // wait for the half-window threshold at the end of this read batch. Marking the
    // stream in _debt is what tells that pass which streams to visit.
    if (total > 0) {
      this._connRecvUsed += total;
      stream._recvUsed += total;
      if (!stream._debtMarked) {
        stream._debtMarked = true; this._debt.push(stream);
      }
    }
    if (flags & F_END_STREAM) {
      stream._endRead(); this._dropStream(sid);
    }
  }

  _onWindowUpdate(sid, inc) {
    if (inc === 0) {
      this._fatal(PROTOCOL_ERROR, 'WINDOW_UPDATE increment of 0'); return;
    }
    if (sid === 0) {
      this._connSendWin += inc;
      for (const s of this._streams.values()) {
        this._pump(s);
      }
      return;
    }
    const stream = this._streams.get(sid);
    if (stream === undefined) {
      return;
    }
    stream._sendWin += inc;
    this._pump(stream);
  }

  _onRst(sid, code) {
    const stream = this._streams.get(sid);
    if (stream === undefined) {
      return;
    }
    this._dropStream(sid);
    stream._onRst(code);
  }

  // Outbound reset: a stream the application abandons mid-flight (destroy/close)
  // owes the peer an RST_STREAM so its resources are freed. No-ops once the stream
  // is gone -- including the inbound-RST path, where _onRst has already dropped it,
  // so we never answer a reset with a reset of our own.
  rstStream(sid, code) {
    if (this.destroyed || !this._open || !this._streams.has(sid)) {
      return;
    }
    const body = Buffer.allocUnsafe(4);
    body.writeUInt32BE((code === undefined ? CANCEL : code) >>> 0, 0);
    this._frame(RST_STREAM, 0, sid, body, true);
    this._dropStream(sid);
  }

  _onGoaway(buf, start, len) {
    if (len < 8) {
      this._fatal(FRAME_SIZE_ERROR, 'GOAWAY payload too short'); return this._open;
    }
    const lastStreamID = buf.readUInt32BE(start) & 0x7fffffff;
    const code = buf.readUInt32BE(start + 4);
    let debug = null;
    if (len > 8) {
      // allocUnsafe + copy rather than Buffer.from(subarray) for the pooled
      // allocation (OPTIMIZATION.md item 18)
      debug = Buffer.allocUnsafe(len - 8);
      buf.copy(debug, 0, start + 8, start + len);
    }
    this._goawayCode = code;
    this._goawayLastStreamID = lastStreamID;
    // Streams the peer refused to process (id above lastStreamID) may be retried;
    // for M1 we report them and stop opening new ones past the mark.
    if (lastStreamID < this._nextSid - 2) {
      this.closed = true;
    }
    // node hands the third goaway argument as undefined when the peer sent no debug
    // data (not an empty buffer), so the measured event shape matches only if we do.
    this.emit('goaway', code, lastStreamID, debug === null || debug.length === 0
      ? undefined : debug);
    // node begins the graceful shutdown on a NO_ERROR goaway regardless of the
    // live streams (internal/http2/core.js onGoawayData: `session.close()` --
    // "No new streams permitted, but existing streams may close naturally on
    // their own"). Closing only when the stream map is already empty left the
    // session open forever when the last stream ended after the GOAWAY arrived
    // (measured on live node: it closes, h2lean hung).
    if (code === NO_ERROR) {
      this.close();
    }
    return this._open;
  }

  // Credit release. Every update due from one read batch is serialised into the
  // session's own `_wuBuf` and leaves there as ONE write at the end of the pass (see
  // _creditRecv); this method only appends. Two properties make it safe.
  //
  // The buffer is owned by this session and never handed to the application, so it may
  // be kept and rewritten the way the per-frame allocation used to be: one 13-octet
  // memcpy replaces one Buffer.allocUnsafe plus one copy per update, and an eight-step
  // download closes with one small write instead of eight.
  //
  // Callers must be inside a credit pass: the bytes only reach the socket when
  // _creditRecv() empties _wuBuf at the end of the sweep.
  _windowUpdate(sid, inc) {
    if (inc <= 0) {
      return;
    }
    let buf = this._wuBuf;
    if (buf === null) {
      buf = this._wuBuf = Buffer.allocUnsafe(13 * 8);
    }
    const len = this._wuLen;
    if (len + HEADER_LEN + 4 > buf.length) {
      const bigger = Buffer.allocUnsafe((len + HEADER_LEN + 4) * 2);
      buf.copy(bigger, 0, 0, len);
      buf = this._wuBuf = bigger;
    }
    writeHeader(buf, len, 4, WINDOW_UPDATE, 0, sid);
    buf.writeUInt32BE(inc >>> 0, len + HEADER_LEN);
    this._wuLen = len + HEADER_LEN + 4;
  }

  // ---- session API ------------------------------------------------------------
  ping(payload, cb) {
    if (this.destroyed) {
      throw httpErr('ERR_HTTP2_INVALID_SESSION', 'The session has been destroyed');
    }
    // node lets a caller pass the callback as the only argument: ping(cb).
    if (typeof payload === 'function') {
      cb = payload; payload = undefined;
    }
    const body = Buffer.allocUnsafe(8);
    if (payload === undefined) {
      const seq = this._pingSeq++;
      body.writeUInt32BE(seq >>> 0, 0);
      body.writeUInt32BE(0, 4);
    } else {
      // Buffer.from(payload) would clone the caller's whole buffer just to take the
      // first eight octets out of it; when the caller already passed a Buffer, copy
      // straight out of it. The fallback keeps whatever else a caller may pass
      // (a Uint8Array, a string) working the way Buffer.from decoded it.
      if (Buffer.isBuffer(payload)) {
        payload.copy(body, 0, 0, 8);
      } else {
        Buffer.from(payload).copy(body, 0, 0, 8);
      }
    }
    if (typeof cb === 'function') {
      this._pings.set(body.toString('latin1'), { cb, at: process.hrtime.bigint() });
    }
    this._frame(PING, 0, 0, body, true);
    return body;
  }

  // node's settings() takes a settings OBJECT, not a SETTINGS payload (core.js
  // settings() -> submitSettings()). Only the entries the caller named go on the wire,
  // in ascending setting-id order rather than the object's key order: measured on a live
  // node 26 client against a raw peer, {headerTableSize, maxConcurrentStreams, enablePush}
  // arrived as headerTableSize=8192, enablePush=0, maxConcurrentStreams=7. The server
  // side's settings() (lib/session.js) re-advertises everything it has ever advertised
  // instead; this is the client following node, not the server's shape.
  settings(settings, cb) {
    if (this.destroyed) {
      throw httpErr('ERR_HTTP2_INVALID_SESSION', 'The session has been destroyed');
    }
    checkSettings(settings);
    // node validates the callback only when it is truthy, so settings({}, null) is legal.
    if (cb && typeof cb !== 'function') {
      throw extError('ERR_INVALID_ARG_TYPE',
        `The "callback" argument must be of type function. Received ${received(cb)}`);
    }
    const submit = () => {
      // submitSettings returns without submitting on a destroyed session, and the
      // 'connect' listener below can fire after one (core.js).
      if (this.destroyed) {
        return;
      }
      if (this._settingsPending >= MAX_PENDING_SETTINGS_ACK) {
        this.destroy(httpErr('ERR_HTTP2_MAX_PENDING_SETTINGS_ACK',
          'Maximum number of pending settings acknowledgements'));
        return;
      }
      const entries = [];
      const values = [];
      if (settings) {
        // maxHeaderSize is the deprecated alias of maxHeaderListSize: it wins when the
        // two differ, and node warns exactly then (internal/http2/util.js
        // updateSettingsBuffer). The wire entry is id 6 either way.
        let listSize = settings.maxHeaderListSize;
        if (typeof settings.maxHeaderSize === 'number') {
          if (settings.maxHeaderSize !== listSize) {
            process.emitWarning('settings.maxHeaderSize overwrite settings.maxHeaderListSize');
          }
          listSize = settings.maxHeaderSize;
        }
        // ID_BY_SETTING is built from SETTINGS_BY_ID in id order, so walking its keys
        // produces the ascending order node submits in without a sort.
        for (const name of Object.keys(ID_BY_SETTING)) {
          const value = name === 'maxHeaderListSize' ? listSize : settings[name];
          if (value === undefined) {
            continue;
          }
          values.push(name, value);
          entries.push(ID_BY_SETTING[name], typeof value === 'boolean' ? (value ? 1 : 0) : value);
        }
      }
      const body = Buffer.allocUnsafe(entries.length * 3);
      for (let i = 0; i < entries.length; i += 2) {
        const at = i * 3;
        body.writeUInt16BE(entries[i], at);
        body.writeUInt32BE(entries[i + 1] >>> 0, at + 2);
      }
      // The values are applied when THIS submit's ACK arrives, not now: measured on a
      // live node 26 client for all seven settings, the first 'localSettings' event (the
      // ACK for the connection's own initial frame) still reports the old value and the
      // second reports the new one -- headerTableSize 8192 arrived as [4096, 8192],
      // maxHeaderListSize 1234 as [4294967295, 1234]. A setting takes effect when it is
      // acknowledged, so the queue entry carries what it will change.
      this._settingsAcks.push({ cb, values });
      this._frame(SETTINGS, 0, 0, body, true);
      this._settingsPending++;
      this.pendingSettingsAck = true;
    };
    // While connecting node defers the whole submit to 'connect' rather than refusing it.
    if (this.connecting) {
      this.once('connect', submit);
      return;
    }
    submit();
  }

  goaway(code, lastStreamID, reason) {
    if (this.destroyed) {
      throw httpErr('ERR_HTTP2_INVALID_SESSION', 'The session has been destroyed');
    }
    if (code === undefined) {
      code = NO_ERROR;
    }
    if (lastStreamID === undefined || lastStreamID === null) {
      // node's goaway() defaults lastStreamID to 0 and the C layer substitutes
      // nghttp2_session_get_last_proc_stream_id (src/node_http2.cc Http2Session::Goaway:
      // `if (lastStreamID <= 0) lastStreamID = ...get_last_proc_stream_id`). For a
      // CLIENT the processed streams are the server-initiated pushes (even ids), so
      // that number is the highest promised id seen -- _maxPushSid, 0 with no pushes.
      // Deriving it from _nextSid (the next odd request id) named a client-initiated
      // stream the server itself opened, which nghttp2 reads as PROTOCOL_ERROR: a
      // graceful close() echo after the peer's GOAWAY then killed the peer's open
      // stream (measured on live node: its client sends last=0,
      // h2lean sent last=1 and the node server answered ERR_HTTP2_ERROR).
      lastStreamID = this._maxPushSid;
    }
    const tail = reason ? Buffer.byteLength(reason) : 0;
    const body = Buffer.alloc(8 + tail);
    body.writeUInt32BE(lastStreamID >>> 0, 0);
    body.writeUInt32BE(code >>> 0, 4);
    if (tail) {
      body.write(reason, 8);
    }
    this._frame(GOAWAY, 0, 0, body, true);
    this.closed = true;
  }

  _initOriginSet() {
    // The client half of core.js's shared base: same lazy helper, same seed rule.
    // Reads `this.socket` (the proxySocket getter, the raw socket underneath) for
    // servername/remoteAddress/remotePort/remoteFamily.
    let set = this._originSet;
    if (set === undefined) {
      const socket = this.socket;
      set = this._originSet = new Set();
      let hostName = socket.servername;
      if (hostName === null || hostName === false || hostName === undefined) {
        hostName = socket.remoteFamily === 'IPv6'
          ? `[${socket.remoteAddress}]`
          : socket.remoteAddress;
      }
      let originString = `https://${hostName}`;
      if (socket.remotePort != null) {
        originString += `:${socket.remotePort}`;
      }
      set.add(urlOrigin(originString));
    }
    return set;
  }

  get originSet() {
    // core.js's getter returns ArrayFrom(initOriginSet()) only for an encrypted,
    // live session; an h2c client gets undefined.
    if (!this.encrypted || this.destroyed) {
      return undefined;
    }
    return [...this._initOriginSet()];
  }

  close(code, cb) {
    if (typeof code === 'function') {
      cb = code; code = undefined;
    }
    this._closeRequested = true;
    if (!this.destroyed) {
      this.goaway(code === undefined ? NO_ERROR : code);
    }
    this._maybeClose();
    if (typeof cb === 'function') {
      if (this.closed && this._streams.size === 0) {
        process.nextTick(cb);
      } else {
        this.once('close', cb);
      }
    }
  }

  // node's routing for an async listener's rejection when EventEmitter
  // captureRejections is on (internal/http2/core.js:1668, the shared
  // Http2Session base): a 'stream' listener's rejection destroys that stream
  // -- for a push, the pushed stream the listener was handed -- every other
  // event's destroys the session.
  [EventEmitter.captureRejectionSymbol](err, event, ...args) {
    if (event === 'stream') {
      const stream = args[0];
      // node's streams run with autoDestroy:false (measured on live node:
      // _readableState.autoDestroy false on a pushed stream), so the rejection's
      // destroy(err) always reaches a live Duplex and emits 'error'. h2lean's
      // ClientHttp2Stream auto-destroys when both halves complete, and streams.destroy()
      // silently drops an error handed to an already-destroyed stream -- a rejection
      // that lands after a small push body has been fully read would then close the
      // stream with no 'error' at all (the upstream capture-rejection block-4 shape,
      // measured on live node: it routes 'kaboom', h2lean went silent). A destroy(err)
      // that lands while the completion-driven autoDestroy is still queued loses the
      // error the same way, so the emission is caught by a flag: a stream that does
      // not report the error synchronously is finished by a direct 'error' emit --
      // reproducing node's observable behaviour for that race, while a still-live
      // stream keeps the normal destroy(err) path and its RST.
      if (stream.destroyed) {
        if (!stream._readableState.errorEmitted && !stream._writableState.errorEmitted) {
          stream.emit('error', err);
        }
        return;
      }
      let reported = false;
      stream.once('error', () => {
        reported = true;
      });
      stream.destroy(err);
      if (!reported) {
        stream.emit('error', err);
      }
    } else {
      this.destroy(err);
    }
  }

  destroy(err, code) {
    if (this.destroyed) {
      return;
    }
    // node's destroy(error = NGHTTP2_NO_ERROR, code) normalizes before closeSession
    // (core.js:1622-1637): a number IS the code and builds ERR_HTTP2_SESSION_ERROR for
    // any non-zero value, and an error with no code implies INTERNAL_ERROR. Measured on
    // a live node client: destroy(8) reports 'Session closed with error code 8' on the
    // session and on the open stream, whose rstCode is 8; destroy(new Error('boom'), 1)
    // keeps the caller's error and gives the stream rstCode 1. The code is recorded for
    // the streams' _destroy derivation, which reads it as sessionCode.
    if (typeof err === 'number') {
      code = err;
      err = code !== NO_ERROR
        ? httpErr('ERR_HTTP2_SESSION_ERROR', `Session closed with error code ${code}`)
        : undefined;
    }
    if (code === undefined && err != null) {
      code = INTERNAL_ERROR;
    }
    this._destroyCode = code === undefined ? NO_ERROR : code;
    // The goodbye is staged before the teardown, while goaway() still accepts a call.
    // node's closeSession terminates the nghttp2 session with this code
    // (core.js:1265 handle.destroy(code, socket.destroyed)), which puts
    // GOAWAY(lastStreamID, code) on the wire ahead of the socket going: measured on a
    // live node client for destroy(), destroy(err), destroy(8) and destroy(err, 1) --
    // codes 0, 2, 8 and 1, last=0 with no pushes -- and again after a graceful close()
    // had already sent one, so the goodbye is not conditional on the session being open.
    // Nothing goes out before the handshake (_started): a destroy while connecting was
    // measured to leave the peer with a bare socket close.
    //
    // The open streams are reset with the same code, all but the last of them. Measured
    // for one, two, three and four open streams: N-1 RST_STREAM frames in ascending id
    // order, then the GOAWAY; one open stream produces no reset at all, and a stream the
    // application had already destroyed is not counted. The codes track the session's
    // (0, 2 and 8 for the three destroy shapes). Why the last one is exempt is in the C++
    // layer (node_http2.cc / nghttp2), which was not read -- the rule here is the wire
    // measurement, not a derivation from it.
    if (this._started) {
      const open = [...this._streams.keys()];
      for (let i = 0; i < open.length - 1; i++) {
        const body = Buffer.allocUnsafe(4);
        body.writeUInt32BE(this._destroyCode >>> 0, 0);
        this._frame(RST_STREAM, 0, open[i], body, true);
      }
      this.goaway(this._destroyCode);
    }
    this.destroyed = true;
    this.closed = true;
    this._open = false;
    this._closeError = err || null;
    this._destroyScheduled = true;
    for (const s of this._streams.values()) {
      if (!s.destroyed) {
        s.destroy(err);
      }
    }
    this._streams.clear();
    // Requests whose HEADERS never went on the wire are cancelled, not handed the
    // session's error: node distinguishes the two (internal/http2/core.js:1254-1260,
    // state.pendingStreams.forEach((stream) => stream.destroy(cancel))) so an
    // application can tell "my request was never sent" from "the connection broke
    // under it". Measured on a TLS handshake failure: node answers a queued request
    // with stream error ERR_HTTP2_STREAM_CANCEL before the session error, h2lean used
    // to leave it silent.
    if (this._queuedReq !== null && this._queuedReq.length > 0) {
      for (const r of this._queuedReq) {
        if (!r.stream.destroyed) {
          r.stream.destroy(cancelErr(err));
        }
      }
      this._queuedReq = null;
    }
    for (const r of this._outboundQueue) {
      if (!r.stream.destroyed) {
        r.stream.destroy(cancelErr(err));
      }
    }
    // reassigned, not length-cleared (OPTIMIZATION.md item 38)
    this._outboundQueue = [];
    const socket = this._socket;
    // The goodbye has to reach the socket before it goes. destroy() used to drop the
    // staged batch outright (_discardOut), which is why the peer saw a bare close; the
    // batch is handed over here the way _fatal hands over its GOAWAY, and end() below
    // flushes it ahead of the FIN.
    this._commit();
    const batch = this._outQ;
    this._outQ = [];
    this._outLen = 0;
    // _commit drops the staging buffer only when it held bytes; _discardOut dropped it
    // either way, and so does this.
    this._stage = null;
    this._stageLen = 0;
    if (batch.length !== 0 && socket.writable && !socket.destroyed) {
      if (batch.length === 1) {
        socket.write(batch[0]);
      } else {
        socket.cork();
        for (let i = 0; i < batch.length - 1; i++) {
          socket.write(batch[i]);
        }
        socket.write(batch[batch.length - 1]);
        socket.uncork();
      }
    }
    if (!socket.destroyed) {
      // Bytes already handed to the socket -- the GOAWAY a fatal inbound frame
      // leaves with (_fatal) -- are flushed by end() before the FIN; an outright
      // destroy() drops them whenever a write is still in flight. With nothing
      // queued the destroy is the immediate kill it always was.
      if (socket.writable && !socket.writableEnded && socket.writableLength > 0) {
        socket.end();
      } else {
        socket.destroy();
      }
    }
    process.nextTick(() => {
      if (!this._closeEmitted) {
        this._emitClose(err);
      }
    });
  }

  ref() {
    this._socket.ref(); return this;
  }
  unref() {
    this._socket.unref(); return this;
  }

  // node's session.socket: the safe Proxy, built once and cached; dropped on close.
  get socket() {
    if (this._proxySocket === null) {
      this._proxySocket = new Proxy(this, proxySocketHandler);
    }
    return this._proxySocket;
  }

  setTimeout(msecs, callback) {
    // node's session.setTimeout sets an idle timer; with no activity it emits
    // 'timeout' and, absent a listener, destroys the session. Kept faithful here.
    if (this._idleTimer) {
      clearTimeout(this._idleTimer);
    }
    this._idleCb = callback || null;
    if (msecs > 0 && !this.destroyed) {
      this._idleTimer = setTimeout(() => {
        this._idleTimer = null;
        this.emit('timeout');
        if (this._idleCb === null && !this.destroyed) {
          this.destroy();
        }
      }, msecs);
      this._idleTimer.unref();
    }
    return this;
  }

  setNextStreamID(id) {
    if (this.destroyed) {
      throw httpErr('ERR_HTTP2_INVALID_SESSION', 'The session has been destroyed');
    }
    this._nextSid = id; return this;
  }
  setLocalWindowSize(size) {
    if (this.destroyed) {
      throw httpErr('ERR_HTTP2_INVALID_SESSION', 'The session has been destroyed');
    }
    // RFC 9113 6.9.2: the change rescales the peer's send windows retroactively, so the
    // window we maintain moves by the same delta rather than only applying to streams
    // opened from here on -- and _creditRecv()'s threshold is derived from it per call.
    this._connRecvWin += size - this.localSettings.initialWindowSize;
    this.localSettings.initialWindowSize = size;
    return this;
  }

  // ---- teardown ---------------------------------------------------------------
  // A client connection is keep-alive: finishing the last stream must NOT tear the
  // session down, or the next request() on the same session would find it destroyed.
  // Only an explicit close()/goaway (which sets _closeRequested), a peer GOAWAY that
  // left nothing live, or destroy() ends it -- then it ends once the outbound queue and
  // the open streams have both drained.
  _maybeClose() {
    if (this.destroyed || this._closeEmitted) {
      return;
    }
    if (this._closeRequested && this._streams.size === 0 && this._wireDone.size === 0
      && this._outboundQueue.length === 0 && this._outLen === 0) {
      this.destroy();
    }
  }

  // The 'close' subscription is installed at construction, before any event can
  // fire, so the wire-done bookkeeping never races the Duplex's own teardown.
  // It is the one subscription per stream the session keeps: the mark it clears
  // is what lets a graceful close outlive an unread response body (see _wireDone).
  _trackClose(stream) {
    stream.on('close', () => {
      this._wireDone.delete(stream);
      if (this.destroyed && !this._closeEmitted && !this._destroyScheduled
        && this._wireDone.size === 0) {
        // The socket is already gone (_onSocketClose deferred to this moment):
        // the last wire-done stream has closed, so the session 'close' is owed
        // now -- after the stream's, the order node emits them in. When
        // destroy() owns the emission (_destroyScheduled) it runs its own
        // error-carrying nextTick after the streams close; this must not race it.
        this._emitClose();
      } else {
        this._maybeClose();
      }
    });
  }

  // _debt is drained at every point that ends a stream's life -- _dropStream (inbound
  // END_STREAM, RST, outbound reset, and the destroy path, which goes through
  // rstStream) -- so the list never grows across a long-lived connection and never
  // visits a closed stream. The mark is the guard: a stream that took no unsent credit
  // costs one property read here, and the scan only runs for the few that did.
  //
  // Dropping a stream that still owes stream-level credit is deliberate, not a leak: the
  // connection's share of those octets was booked into _connRecvUsed when they arrived
  // (as on the server, where _abandon() moves them), and the stream itself is closed, so
  // nobody can send another DATA on it that the missing WINDOW_UPDATE could unblock.
  _removeDebt(stream) {
    const debt = this._debt;
    for (let i = 0; i < debt.length; i++) {
      if (debt[i] === stream) {
        debt[i] = debt[debt.length - 1];
        debt.pop();
        stream._debtMarked = false;
        return;
      }
    }
  }

  _dropStream(sid) {
    const stream = this._streams.get(sid);
    if (stream !== undefined && stream._debtMarked) {
      this._removeDebt(stream);
    }
    this._streams.delete(sid);
    if (stream !== undefined) {
      // The wire life is over; the Duplex may still owe 'end'/'close' (an unread
      // body). The session's graceful close waits for that 'close' (_wireDone).
      this._wireDone.add(stream);
    }
    this._drainQueue();
    this._maybeClose();
  }

  _fatal(code, reason) {
    if (!this._open) {
      return false;
    }
    this._open = false;
    // node's client answers a fatal inbound frame with GOAWAY on the wire and
    // ERR_HTTP2_ERROR on the stream and on the session (measured,
    // tools/probe-client-pad-bad.js and tools/probe-client-hdr-pad.js).
    // destroy() drops the staged batch and resets the socket, so the batch --
    // the GOAWAY and anything staged behind it -- is handed to the socket here,
    // and destroy() leaves a queued batch alive (it ends the socket instead of
    // resetting it while bytes are still queued, so a write already in flight
    // drains first).
    this.goaway(code, undefined, reason);
    this._commit();
    const batch = this._outQ;
    this._outQ = [];
    this._outLen = 0;
    const socket = this._socket;
    if (batch.length !== 0 && socket.writable && !socket.destroyed) {
      if (batch.length === 1) {
        socket.write(batch[0]);
      } else {
        socket.cork();
        for (let i = 0; i < batch.length - 1; i++) {
          socket.write(batch[i]);
        }
        socket.write(batch[batch.length - 1]);
        socket.uncork();
      }
    }
    const err = httpErr('ERR_HTTP2_ERROR', reason);
    err.http2Code = code;
    this.destroy(err);
    return false;
  }

  _onSocketError(err) {
    if (err && err.code === 'ECONNRESET' && this._goawayCode !== null) {
      return this.destroy();
    }
    this.destroy(err);
  }

  _onSocketClose() {
    this._socket[kBoundSession] = undefined;
    this._proxySocket = undefined;
    if (this._closeEmitted) {
      return;
    }
    this.destroyed = true;
    // node's socketOnClose (internal/http2/core.js:3653) closes every live stream
    // and every pending one with NGHTTP2_CANCEL before it closes the session, and
    // CANCEL is not an error (core.js:2543), so the application sees 'close' with
    // rstCode 8 and no 'error'. Without this sweep a request whose HEADERS went out
    // but whose answer never came stayed in _streams forever: the session emitted
    // 'close' with the stream still live and never destroyed (measured on a server
    // that destroys its session from the 'stream' handler -- node reports req-close
    // 8, h2lean left req.destroyed false indefinitely).
    for (const stream of [...this._streams.values()]) {
      if (!stream.destroyed) {
        this._dropStream(stream.id);
        stream._onRst(CANCEL);
      }
    }
    for (const q of [this._queuedReq, this._outboundQueue]) {
      if (q === null) {
        continue;
      }
      for (const r of q) {
        if (!r.stream.destroyed) {
          r.stream._onRst(CANCEL);
        }
      }
    }
    this._queuedReq = null;
    // reassigned, not length-cleared (OPTIMIZATION.md item 38)
    this._outboundQueue = [];
    // destroy() owns the emission when it was called (fatal or graceful): its
    // nextTick carries the error and runs after the streams' 'close'. Only the
    // pure peer-FIN shape -- destroyed here, no destroy() pending -- defers to
    // _trackClose below.
    if (this._destroyScheduled) {
      return;
    }
    // A wire-done stream whose Duplex has not closed yet (an empty response body
    // ends on the next tick) still owes its 'close'. node emits the session
    // 'close' after the streams', not ahead of them (kMaybeDestroy waits for
    // state.streams to empty), so the emission waits for _wireDone to clear --
    // _trackClose finishes it. Without this the peer's FIN raced the stream's
    // 'close' and the application never saw the stream event at all (measured on
    // live node, the upstream capture-rejection shape).
    if (this._wireDone.size > 0) {
      return;
    }
    this._emitClose();
  }

  _emitClose(err) {
    if (this._closeEmitted) {
      return;
    }
    this._closeEmitted = true;
    // A deferred emission (the graceful peer-FIN path) still carries whatever
    // error destroy() was handed, so a fatal teardown never loses its 'error'.
    if (!err) {
      err = this._closeError;
    }
    if (err) {
      this.emit('error', err);
    }
    this.emit('close');
  }

  // ---- read-only surface (node-compatible) ------------------------------------
  // The last GOAWAY this side received. Before any GOAWAY node reports the code 0
  // (NO_ERROR) and lastStreamID 0; we mirror that default rather than leaking null.
  get goawayCode() {
    return this._goawayCode === null ? 0 : this._goawayCode;
  }
  get goawayLastStreamID() {
    return this._goawayLastStreamID === null ? 0 : this._goawayLastStreamID;
  }

  // node's `state` is an nghttp2-internal snapshot. The fields an application can
  // legitimately read are the window sizes and the stream id bookkeeping, so those
  // are provided; the dynamic-table figures are reported live off the codecs.
  get state() {
    return {
      effectiveLocalWindowSize: this.localSettings.initialWindowSize,
      effectiveRecvDataLength: 0,
      nextStreamID: this._nextSid,
      localWindowSize: this.localSettings.initialWindowSize,
      lastProcStreamID: this._goawayLastStreamID === null ? 0 : this._goawayLastStreamID,
      remoteWindowSize: this._connSendWin,
      outboundQueueSize: this._outboundQueue.length,
      deflateDynamicTableSize: this._encoder.size,
      inflateDynamicTableSize: this._decoder.size
    };
  }
}

// Helpers ---------------------------------------------------------------------

// Pseudo-headers must precede regular fields (RFC 9113 8.1); the application may
// have interleaved them, so re-order :method, :path, :scheme, :authority, :protocol
// to the front and leave the rest in place.
function orderPseudo(list) {
  const pseudo = [];
  const fields = [];
  for (let i = 0; i < list.length; i += 2) {
    (list[i].charCodeAt(0) === 58 ? pseudo : fields).push(list[i], list[i + 1]);
  }
  const byName = (n) => {
    for (let i = 0; i < pseudo.length; i += 2) {
      if (pseudo[i] === n) {
        return pseudo[i + 1];
      }
    }
    return undefined;
  };
  const out = [];
  for (const n of [':method', ':path', ':scheme', ':authority', ':protocol']) {
    const v = byName(n);
    if (v !== undefined) {
      out.push(n, v);
    }
  }
  for (let i = 0; i < pseudo.length; i += 2) {
    if (out.indexOf(pseudo[i]) === -1) {
      out.push(pseudo[i], pseudo[i + 1]);
    }
  }
  for (let i = 0; i < fields.length; i += 2) {
    out.push(fields[i], fields[i + 1]);
  }
  return out;
}

function httpErr(code, msg) {
  const err = new Error(msg);
  err.code = code;
  return err;
}

// The shape of node's ERR_HTTP2_STREAM_CANCEL (internal/errors.js:1335): the cause
// rides along as .cause, and the message names it the way util did when node built
// the string there. With no cause (a plain destroy()) the bare message is what node
// emits too.
function cancelErr(cause) {
  const err = httpErr('ERR_HTTP2_STREAM_CANCEL', 'The pending stream has been canceled'
    + (cause ? ` (caused by: ${cause.message})` : ''));
  if (cause) {
    err.cause = cause;
  }
  return err;
}

// Top-level emit helper for the deferred END_STREAM path: matches the module-level
// `emit()` pattern used elsewhere so the process.nextTick call in _pump does not
// capture a closure over the stream.
function emitWantTrailers(stream) {
  // node's onStreamTrailers sets state.trailersReady first and only then looks at
  // whether the stream is still alive, and it closes the deferred END_STREAM itself
  // when the application never asked for the event: an empty trailer block. Without
  // that fallback a request written with waitForTrailers and no listener leaves the
  // message unfinished for good.
  stream._trailersReady = true;
  if (stream.destroyed) {
    return;
  }
  if (!stream.emit('wantTrailers')) {
    stream.sendTrailers({});
  }
}

module.exports = { ClientHttp2Session };
