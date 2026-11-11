// HTTP/2 connection state machine (server side, h2c).
//
// Two performance decisions shape this file:
//  * one output staging buffer per connection: every frame produced while draining
//    one socket read is serialised into it and goes out in a single write(), and
//    flow-control credits are coalesced over the same batch;
//  * whole frames are parsed in place out of the chunk node handed us. A frame
//    that straddles two reads is not copied into a buffer of the session's own
//    either: its payload is kept as the views of the chunks it arrived in, one
//    view per read. A DATA payload is handed to the application as those views,
//    which is what node does with the same frame (tools/probe-chunking.js
//    measures both stacks delivering a cut payload as the pieces it arrived in);
//    a header block is decoded out of them (lib/hpack.js decodePieces); and the
//    control frames -- small and fixed-size -- are put in a buffer of their own
//    for the parse. A padded DATA payload is no copy either: its views are not
//    the payload (RFC 9113 6.1 -- the first octet is the pad length, the last
//    ones are the padding), so the padding is taken off the views once the frame
//    is complete (lib/frames.js trimPadding).
'use strict';

const { EventEmitter } = require('node:events');
const f = require('./frames');
const { Decoder, Encoder, HPackError } = require('./hpack');
const { Http2Stream, errorOf, emitNT } = require('./stream');
const { pieceCopyTo, pieceSlice, pieceConcat, pieceEquals } = require('./pieces');
const { nullProto, hasBadValue } = require('./headers');
// One shared 499 ms clock for the long idle timeouts, instead of a libuv timer
// handle per connection. Vendored verbatim; see lib/fast-timer.js.
const { setTimeout: idleSetTimeout, clearTimeout: idleClearTimeout } =
  require('./fast-timer');

const {
  DATA, HEADERS, PRIORITY, RST_STREAM, SETTINGS, PUSH_PROMISE, PING, GOAWAY,
  WINDOW_UPDATE, CONTINUATION,
  F_END_STREAM, F_ACK, F_END_HEADERS, F_PADDED, F_PRIORITY,
  NO_ERROR, PROTOCOL_ERROR, INTERNAL_ERROR, FLOW_CONTROL_ERROR, STREAM_CLOSED,
  FRAME_SIZE_ERROR, REFUSED_STREAM, COMPRESSION_ERROR, ENHANCE_YOUR_CALM,
  PREFACE_LEN, HEADER_LEN, readHeader, writeHeader, trimPadding
} = f;

// The values the session operates on when the application says nothing. Derived from node's own
// getDefaultSettings() so the numbers the guards compare against are the ones node's guards compare
// against. They are NOT advertised by default: what goes into a SETTINGS frame is decided by
// _advertised, because an advertised value is a promise to the peer rather than a local choice.
const OUR_DEFAULTS = { ...require('node:http2').getDefaultSettings(), enablePush: false };

// What node actually has in effect before the peer's SETTINGS arrives. Read off
// a live session: doc/api/http2.md quotes different numbers for
// initialWindowSize and maxHeaderListSize than the running binary reports, and
// the running values are what peers observe.
const PEER_DEFAULTS = {
  headerTableSize: 4096,
  enablePush: true,
  maxConcurrentStreams: 4294967295,
  initialWindowSize: 65535,
  maxFrameSize: 16384,
  maxHeaderListSize: 4294967295,
  enableConnectProtocol: false
};

// SETTINGS identifiers, from http2.constants.NGHTTP2_SETTINGS_*.
// The numbering is NOT the order the names are usually listed in:
// 3 is maxConcurrentStreams and 4 is initialWindowSize.
const SETTINGS_BY_ID = {
  1: 'headerTableSize',
  2: 'enablePush',
  3: 'maxConcurrentStreams',
  4: 'initialWindowSize',
  5: 'maxFrameSize',
  6: 'maxHeaderListSize',
  8: 'enableConnectProtocol'
};

const ID_BY_SETTING = Object.fromEntries(
  Object.entries(SETTINGS_BY_ID).map(([id, name]) => [name, Number(id)]));

const MAX_REFUSED_HISTORY = 32;
const MAX_DROPPED_HISTORY = 256;
const MAX_SHAPES = 128;

// RFC 7541 4.1: the size of an entry is the length of its name plus the length of its value
// plus 32 -- the computation RFC 9113 6.5.2 states SETTINGS_MAX_HEADER_LIST_SIZE in terms of.
// That is the number node's `maxHeaderListSize` is compared against, measured on
// v26.12.1 with tools/probe-inbound-limits.js: a 4-field block whose names and values total 46
// octets is REFUSED under a limit of 48 (46 + 4*32 = 174), and for a 5-field block the line
// between served and refused falls between 87 and 93 decoded octets (250 - 5*32 = 90). The
// plain sum of the field octets is what h2lean compared, and it is 128 entry octets away from
// node's verdict for a four-field request: three of the measured blocks were served that node
// refuses. test/inbound-limits.test.js pins both sides of the line.
const ENTRY_OVERHEAD = 32;

// How many bytes a session may have queued for the socket while a write is still
// outstanding. It has to be an absolute number: derived from the size of the staging
// region it becomes a feedback loop, because whatever fits raises the limit and lets
// the next body in. Measured without it on 64 connections x 32 streams with 256 KB
// responses: the staging reached 16 MB per session (1 GB of RSS for 64 connections,
// and a peer chooses the body size), one write() carried 8 MB, and the compaction
// behind the frozen region moved 251 KB per request.
//
// The ceiling itself is a syscall/memory trade measured in one session
// (OPTIMIZATION.md item 34): at 128 KiB a 256 KB response leaves in two writev calls
// with a park/resume round-trip between them; at 256 KiB it leaves in one and the
// download case is ~7% cheaper per request (arm ranges non-overlap), for a doubled
// worst case -- 256 KiB held per session -- against a peer that stops reading.
const MAX_OUT_BATCH = 256 * 1024;

// How much a connection may make this session hold for answers it is not allowed to send. A body
// that flow control will not pass is parked on its stream until the peer opens the window, which is
// right per request and unbounded per connection: a client that never sends a WINDOW_UPDATE can
// make the server keep a live stream plus its unsent body for every HEADERS it puts on the wire.
// Measured with tools/probe-stream-concurrency.js -- 200 000 streams on one connection, a 14-byte
// response each, no credit ever granted -- the session held 195 319 live streams and 170 MB after a
// forced collection, i.e. 892 bytes per parked stream whose queue entry was 23 bytes: what costs
// here is the stream record, not its payload. So the charge has both terms, and the number is then
// a statement about memory rather than about one dimension of it -- at the measured cost, about
// 8 000 parked streams per connection, the same order as the point where node's own session bound
// stopped the identical flood (13 454 streams, same probe, maxSessionMemory left at its default).
// The socket-queue half of the retention is not counted because it is already bounded absolutely by
// MAX_OUT_BATCH above.
const MAX_SESSION_HOLD = 8 * 1024 * 1024;
const STREAM_HOLD_CHARGE = 1024;

// Smallest payload that is worth queuing by reference instead of copying into the
// staging buffer. Below it the extra chunk in the writev batch costs more than the
// memcpy -- the break-even is measured at 4 KB (OPTIMIZATION.md item 15) -- and one
// frame at the default maxFrameSize of 16 384 is comfortably above it either way.
const BODY_MIN = 4096;

// First size of the staging buffer; doubled per frame that does not fit, so a batch of
// header blocks costs one allocation and one copy of each block.
const STAGE_MIN = 1024;

// The 'stream' event carries node's options object. Two constants instead of a
// literal per request: the payload never changes, and 1.9M requests is 1.9M
// objects the young generation would otherwise have to collect.
const OPT_END_STREAM = { endStream: true };
const OPT_NO_END_STREAM = { endStream: false };

// How long the teardown waits for the goodbye to reach the peer before the socket is
// taken away anyway. The FIN is queued behind whatever we wrote, so a peer that stopped
// reading would hold it forever -- node does not have that problem because its data sits
// inside nghttp2 until the window allows it, while h2lean hands it to the socket. The
// wait itself is what makes the difference between a closed connection and a reset
// (h2spec's http2/7), so it gets a bound of its own: one second is three orders of
// magnitude longer than the flush takes when the peer is reading.
const SOCKET_CLOSE_GRACE = 1000;

// ---- inbound request validation ---------------------------------------------
//
// RFC 9113 8.1, 8.2.2, 8.3 and 8.5 are not style advice. Whatever sits in front of us
// translates these fields into HTTP/1.1 one line at a time, and every rule below exists
// because a receiver on the far side of that translation can read a field differently
// than we do. That is request smuggling, and it is a named CVE family in this exact
// stack: CVE-2020-8287 (Node.js took the first of two copies of a header field and
// ignored the second). The frame-level pair of the same family -- a SETTINGS frame with
// an enormous number of entries (CVE-2020-11080) and a header block spread over an
// enormous number of CONTINUATION frames (CVE-2024-28182) -- is bounded by
// MAX_SETTINGS_ENTRIES and MAX_CONTINUATIONS just below.
//
// The reference for what is refused and how is nghttp2 -- the HTTP/2 stack node:http2
// embeds -- lib/nghttp2_http.c: check_pseudo_header(), http_request_on_header(),
// nghttp2_http_on_request_headers(). Every rule below was checked against a live
// node:http2 server, and the same matrix is asserted in test/request-validation.test.js,
// so this file mirrors measured behaviour rather than a reading of the RFC.
//
// Two different actions, and the distinction is deliberate:
//  * a pseudo-header that is missing, repeated, empty, unknown, out of order, or a
//    connection-specific / te / content-length violation -> RST_STREAM PROTOCOL_ERROR,
//    the stream never reaches the application. nghttp2 is strict here: "we are strict
//    for pseudo header field. One bad character should lead to fail".
//  * a *regular* field whose name contains a character that is not allowed, but is not
//    upper case -> the field is dropped and the stream lives. Measured on node: a name
//    of 'bad name' arrives as nothing, the handler still runs. nghttp2 calls this
//    NGHTTP2_ERR_IGN_HTTP_HEADER, with the comment that illegal regular headers float
//    around the public internet in quantity and killing a stream over one would break
//    sites. An upper-case name is NOT in that class: it is a hard error (also measured).
//  * a *regular* field whose VALUE carries a character that may not appear in a field
//    value -> the field is dropped and the stream lives, the same action as the name
//    case above. Measured on node, character by character (bench/micro/probe-value-matrix.js):
//    every control character but HTAB, and DEL, makes the field disappear while the
//    request is still served. The class is the one lib/headers.js refuses on the way
//    out, and the reason is the same: a value with a CR in it is a line two readers
//    may disagree about (CVE-2020-8287's class).
const TOKEN_RE = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/; // RFC 9110 9: method is a token
const SCHEME_RE = /^[A-Za-z][A-Za-z0-9+\-.]*$/; // RFC 3986 3.1
const NUM_RE = /^[0-9]+$/; // RFC 9110 8.6: Content-Length = 1*DIGIT
// RFC 9113 8.3.1 :authority is host/pport, i.e. URI authority syntax (RFC 3986 3.2).
const AUTHORITY_RE = /^[A-Za-z0-9._~%!$&'()*+,;=:@[\]-]+$/;
// Field names on an HTTP/2 connection must be lower case (RFC 9113 8.2.1). Split into
// two tests because the two halves have different consequences, see above.
const NAME_LOWER_RE = /^[!#$%&'*+\-.^_`|~0-9a-z]+$/;
const HAS_UPPER = /[A-Z]/;
// RFC 9113 8.2.2: connection-specific fields may not be sent in HTTP/2 at all. The same
// five names lib/headers.js refuses on the way out; kept separate because the action
// here is different (refuse the stream, not throw at the application).
const INBOUND_FORBIDDEN = new Set(['connection', 'keep-alive', 'proxy-connection',
  'transfer-encoding', 'upgrade']);

// How many SETTINGS entries one frame may carry. CVE-2020-11080 is the shape: a frame
// with 2 400 entries (14 400 bytes, and SETTINGS is exempt from the max frame size limit
// -- see _allowFrame) repeated until the CPU is gone, because every entry is parsed even
// though the peer meant nothing by them. nghttp2 1.41 answered with a limit and node has
// it: measured on a live node:http2 server, 32 entries are accepted and 33 bring
// GOAWAY(ENHANCE_YOUR_CALM) with the debug string 'SETTINGS: too many setting entries'.
// Same number here, so a peer that is fine with node is fine with us.
const MAX_SETTINGS_ENTRIES = 32;

// How many CONTINUATION frames may finish one header block. CVE-2024-28182 is the shape:
// an unbounded number of CONTINUATION frames is unbounded work, and nghttp2 additionally
// kept decoding them after the stream was reset just to keep HPACK in sync. The limit in
// nghttp2 is NGHTTP2_DEFAULT_MAX_CONTINUATIONS = 8 (lib/nghttp2_session.h, read from the
// v1.70.0 source), and it is what node answers with: measured 8 frames served, 9 brought
// GOAWAY(INTERNAL_ERROR). Eight is generous for a real block -- at the default frame size
// it is 8 x 16 KB of headers before the limit bites.
const MAX_CONTINUATIONS = 8;

// The rapid-reset budget: one token per inbound RST_STREAM, and a session that runs out
// is closed. CVE-2023-44487 ("HTTP/2 Rapid Reset", exploited in the wild in August through
// October 2023) is the shape this bounds: a client opens a stream and resets it
// immediately, in a loop. Every stream costs the server an allocation, a 'stream' event
// and a teardown, while the client pays one 13-octet frame -- and the concurrency limit
// does not bind, because no stream is ever left open long enough to count against it.
// Measured here before the fix: 3 000 open-and-reset pairs were all served, the handler
// fired 3 000 times and the connection never objected.
//
// The numbers are nghttp2's, which is what node:http2 ships (v1.57.0 added the mitigation;
// lib/nghttp2_session.c v1.70.0 session_update_stream_reset_ratelim() with
// NGHTTP2_DEFAULT_STREAM_RESET_BURST = 1000 and _RATE = 33 from lib/nghttp2_session.h):
// the bucket starts full, gains 33 tokens per *elapsed second* -- integer seconds, so a
// burst inside one second does not refill it -- drains one per reset, and on an empty
// drain the server sends GOAWAY(INTERNAL_ERROR). Server side only, as in nghttp2: the
// client is not the endpoint being exhausted.
//
// They are copied rather than invented, because they are the observable budget a peer has
// against node: measured on a live node:http2 server, 1 000 resets pass and the 1 001st is
// answered with GOAWAY last=<highest stream received> code=2. A client that is allowed
// that against node must not be throttled harder here, and one that exhausts node must not
// get more work out of us.
const RST_BURST = 1000;
const RST_RATE = 33; // tokens per second

// FNV-1a over the octets of a header block. Math.imul keeps the accumulator in
// 32 bits, which is also what makes the hash cheap: no strings are allocated.
function hashBlock(buf) {
  let h = 0x811c9dc5;
  for (let i = 0; i < buf.length; i++) {
    h = Math.imul(h ^ buf[i], 0x01000193);
  }
  return h >>> 0;
}

// The same hash over the views a header block arrived in. A block that a socket
// read split has no buffer of its own, and the shape cache's key is the hash of
// the block's octets.
function hashPieces(pieces, start, end) {
  let h = 0x811c9dc5;
  let off = 0;
  for (let i = 0; i < pieces.length; i++) {
    const p = pieces[i];
    const pend = off + p.length;
    if (pend > start && off < end) {
      const a = start > off ? start - off : 0;
      const b = end < pend ? end - off : p.length;
      for (let k = a; k < b; k++) {
        h = Math.imul(h ^ p[k], 0x01000193);
      }
    }
    off = pend;
    if (off >= end) {
      break;
    }
  }
  return h >>> 0;
}

// The idle-timeout callback. It takes the session as its argument because both
// the native setTimeout and FastTimer forward a timer argument to the callback,
// which is what lets setTimeout() allocate nothing per connection.
function onIdleTimeout(session) {
  session._idleTimer = null;
  session.emit('timeout');
  const cb = session._idleCb;
  if (cb !== null) {
    cb.call(session);
  }
}

class Http2Session extends EventEmitter {
  constructor(socket, options) {
    super();
    const opts = options || {};
    this.socket = socket;
    this.server = null; // set by the server on attach
    this.type = 0; // NGHTTP2_SESSION_SERVER
    this.closed = false;
    this.destroyed = false;
    this.connecting = false;
    // node reads both of these off the socket. Over h2c there is no encryption and
    // no ALPN; over h2s a peer that negotiated nothing reports false rather than
    // a protocol name, which is h2 with prior knowledge on a TLS connection.
    this.encrypted = socket.encrypted === true;
    this.alpnProtocol = socket.alpnProtocol === undefined ? null : socket.alpnProtocol;
    // What the application sees. node keeps the received values in a private state object and
    // exposes them through getters that answer NO_ERROR / 0 when no GOAWAY has arrived
    // (`this[kState].goawayCode || NGHTTP2_NO_ERROR`), so `session.goawayCode` reads 0 on a live
    // connection rather than null. The nulls are kept here as _goawayCode because destroy()
    // needs to know whether the peer has said anything at all -- and node's `goawayCode` says
    // nothing about a teardown this side started, which is why destroy() does not write it.
    this._goawayCode = null;
    this._goawayLastStreamID = null;
    this.pendingSettingsAck = true;
    this.originSet = [];
    this.localSettings = { ...OUR_DEFAULTS, ...opts.settings };
    // Which of those values the application asked for, in the order it asked for them, and only
    // those go on the wire. Measured on a live node server: created with no options its first
    // SETTINGS frame carries an EMPTY payload, and created with
    // settings {maxConcurrentStreams: 100, maxFrameSize: 32768} it advertises exactly those two
    // values in that order and no others -- the same rule is visible in node's internal
    // getDefaultSettings(), which emits a setting only when its flag bit is set.
    // The difference is not cosmetic. A number we advertise is a number a peer plans around, and
    // SETTINGS_MAX_CONCURRENT_STREAMS decides how hard implementations push a connection before
    // they count it as exceeded: h2spec sends the advertised limit plus one HEADERS frames, so
    // advertising the unlimited default turns its concurrency check into four billion streams.
    // Nothing is lost by keeping silent, because the local values still guard the inbound path, and
    // the one promise the old comment worried about -- push -- is kept by the frame rule, not by
    // the advertisement: a peer that sends PUSH_PROMISE gets GOAWAY(PROTOCOL_ERROR) in _onFrame.
    this._advertised = new Set((opts.advertised !== undefined
      ? opts.advertised : Object.keys(opts.settings || {}))
      .filter((k) => ID_BY_SETTING[k] !== undefined));
    this.remoteSettings = { ...PEER_DEFAULTS };
    this.state = {
      effectiveLocalWindowSize: this.localSettings.initialWindowSize,
      effectiveRecvDataLength: 0,
      nextStreamID: 2,
      localWindowSize: this.localSettings.initialWindowSize,
      lastProcStreamID: 0,
      remoteWindowSize: 65535,
      outboundQueueSize: 0,
      deflateDynamicTableSize: 0,
      inflateDynamicTableSize: 0
    };

    this._streams = new Map();
    this._decoder = new Decoder(this.localSettings.headerTableSize);
    // The two halves of the HPACK state, and neither is shared. Each one is bounded by
    // the SETTINGS of the endpoint that OWNS it: the decoder holds the table our peer
    // builds for us, so its ceiling is what WE advertised; the encoder builds the table
    // the peer will decode, so its ceiling is what THE PEER advertised (RFC 7541 4.2).
    // Blocks that depend on the encoder's indices are cached per session with them.
    this._encoder = new Encoder(this.remoteSettings.headerTableSize,
      this.localSettings.headerTableSize);
    this._blocks = new WeakMap();
    this._shapes = opts.shapeCache === false ? null : new Map();
    this._shapeEpoch = 0;
    this._shapeHits = 0;
    this._shapeMisses = 0;
    // shareHeaders: the application is handed a frozen headers object, and two
    // requests whose HPACK block is byte-identical get the SAME object. That
    // removes the per-request object construction for repeating requests; it
    // also means any write through req.headers is a mutation of other requests,
    // so everything the app receives is frozen and the setters throw
    // (OPTIMIZATION.md item 22 for both sides of the trade-off).
    this._share = opts.shareHeaders === true;
    this._flat = [];
    // The compat layer needs the decoded field list as it came off the wire.
    // Copying it is a per-request allocation, so it only happens for servers
    // that actually listen for 'request'.
    this._wantRaw = opts.compat === true;

    // Outbound frames go to the socket as one corked writev batch. A body is queued as
    // the buffers that already hold it -- a frame header and a view of the
    // application's buffer -- so assembling a response costs references rather than
    // memcpys. Everything else is copied into a staging buffer, because a hundred tiny
    // responses want one write each rather than two chunks apiece: node's write path
    // charges per chunk, and the staging buffer makes a batch ONE chunk. The shape,
    // the BODY_MIN threshold and the measurements behind it: OPTIMIZATION.md item 15.
    this._outQ = [];
    this._stage = null;
    this._stageLen = 0; // bytes of _stage not yet queued
    this._stageCap = STAGE_MIN; // size of the next staging buffer
    this._outLen = 0; // queued bytes the socket has not been given
    this._heldBytes = 0; // bytes of response parked on streams that lack credit
    this._frozenLen = 0; // bytes handed over, callback still owed
    this._writing = false;
    this._connSendWin = PEER_DEFAULTS.initialWindowSize;
    this._prevInitialWin = PEER_DEFAULTS.initialWindowSize;
    this._connRecvUsed = 0; // consumed bytes owed back to the connection
    this._blocked = null; // streams waiting on credit

    // A frame that arrives in pieces keeps its payload as the views it arrived in
    // (see _fragPieces), so the session holds no buffer per connection for it.
    // The only octets the session copies are the nine of a frame header that a
    // read split: they wait in this scratch until the header is complete.
    this._fragHdr = Buffer.allocUnsafe(HEADER_LEN);
    this._frag = 0; // bytes of a straddling frame
    this._fragTarget = null;
    // A straddling DATA payload is NOT assembled: it is kept as the views of the
    // chunks the peer sent it in -- one per read -- and handed to the application
    // as they are. Node does the same: its payload reaches the application in the
    // pieces it arrived in, not in one piece per frame (measured with a frame whose
    // payload was cut in two: node's handler gets two chunks, tools/probe-chunking.js),
    // so this is parity rather than a shortcut.
    //
    // A PADDED payload is the one case where the views are not the payload: its
    // first octet is the pad length and its last octets are the padding (RFC 9113
    // 6.1). The padding is taken off the views when the frame is complete
    // (frames.js trimPadding), so even that case costs no copy.
    this._fragPieces = null;
    // The header of the frame being assembled, kept HERE and not read back from
    // frames.js's FH_TYPE/FH_FLAGS/FH_SID. Those are module-level slots shared by every
    // session in the process, and a straddling frame is dispatched in a LATER _onData()
    // call: another connection's readHeader() in between replaces them, and this session
    // dispatches a frame its peer never sent. The same shape was measured on the client
    // with 8 sessions x 64 streams of 256 kB (bench/micro/probe-client-framedrop.js): a DATA frame
    // delivered as WINDOW_UPDATE/0 and one whole frame missing per affected stream. A
    // server sees it whenever two connections' reads interleave with a straddling inbound
    // frame -- a big request body on a busy listener is exactly that.
    this._fragType = 0;
    this._fragFlags = 0;
    this._fragSid = 0;
    this._prefaceSeen = 0;
    // The peer's very first frame has to be its SETTINGS (see _allowFrame); the flag is
    // cleared the moment one frame has been judged, so the rule costs one boolean test per
    // frame header for the rest of the connection's life.
    this._firstFramePending = true;

    // The header block of a request, while it is being assembled. A block whose
    // frames all arrived whole is kept in one buffer (_hdr); a block with a frame
    // that straddled a read is kept as the views it arrived in (_hdrPieces).
    this._hdr = null;
    this._hdrPieces = null;
    this._hdrLen = 0;
    // The length of the header block fragment in the frame being read: the payload
    // without the pad length, the priority block and the padding. A padded frame
    // carries its padding at the END of the payload (RFC 9113 6.2), so the block
    // does not run to the end of it -- see _headersPrefix.
    this._hdrFrag = 0;
    this._hdrSid = -1;
    this._hdrFlags = 0;
    this._hdrCont = 0; // CONTINUATION frames in this block
    // The first octets of a HEADERS payload -- the pad length and the priority
    // block -- are at most six, and a payload that arrived in pieces has no
    // buffer to read them from: they are copied into this scratch.
    this._hdrPrefix = Buffer.allocUnsafe(6);

    // Rapid-reset token bucket, per connection. See RST_BURST/RST_RATE above; the state is
    // session data precisely because the budget is one peer's behaviour on one connection.
    // The second is fixed at construction so that a catch-up grant can only ever be the
    // time this connection has actually existed.
    this._rstTokens = RST_BURST;
    this._rstSec = (Date.now() / 1000) | 0;

    this._maxInboundSid = 0;
    this._refused = [];
    this._dropped = null; // ids a reset ended, newest last
    // ids whose message ended in the read batch being processed, see _dropStream. Cleared by the
    // next read: after this batch the stream has been answered and forgotten, and a frame for it
    // is 5.1's discard rather than its connection error -- which is exactly what node's verdict
    // for the same sequence does once the response has gone out (measured with GAP= in
    // tools/probe-inbound-limits.js, cases U1, U3, U8, U9).
    this._ended = [];
    this._open = true;
    this._closing = false;
    this._closeEmitted = false;
    this._liveStreams = 0;
    this._goawaySent = false;
    this._idleTimer = null;
    this._idleCb = null;
    this._inBatch = false;
    this._flushScheduled = false;
    this._flushSoon = () => {
      this._flushScheduled = false;
      this._flush();
    };
    // one closure per session, so that handing the staged bytes to the socket
    // costs no allocation per write
    this._onWrittenCb = () => this._onWritten();

    socket.on('data', (d) => this._onData(d));
    socket.on('error', (err) => this._onSocketError(err));
    socket.on('end', () => this.destroy(NO_ERROR));
    // 'finish' means everything we wrote reached the OS, so the GOAWAY of a close() is never
    // cut off by a destroy -- but it is not the end of the session either. node keeps reading
    // until the PEER closes, which is what lets it receive and report the peer's answer to that
    // GOAWAY; the wait ends at the peer's FIN ('end' above) or at 'close'.
    socket.on('finish', () => {
      if (!this._closing) {
        this.destroy(NO_ERROR);
      }
    });
    socket.on('close', () => this._onSocketClose());

    this._sendSettings();
  }

  // ---- outbound queue --------------------------------------------------------
  // A payload reaches the socket either by reference or by copy, and the difference is
  // who owns the memory. `owned` says it may be kept by reference: the buffer is either
  // one this session allocated for the frame (SETTINGS, RST_STREAM, GOAWAY, PING) or a
  // view of what the application handed to stream.end(), which it may not mutate before
  // the bytes are on the wire -- the same guarantee node:http2 gives. Bodies above
  // BODY_MIN take that path and are never copied. Everything else travels by copy,
  // because it is a view into memory the next call rewrites -- the HPACK scratch block
  // in lib/headers.js -- and those payloads are tens of bytes.
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
  // those bytes may already be on their way to the peer, which is exactly what the
  // copyWithin compaction used to have to dance around -- so a full buffer is handed
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

  // Frames produced outside a read batch -- a handler that answers from the
  // 'end' callback (which is a nextTick emission) or after an await -- would
  // otherwise sit in the queue until the next request arrived. Node drains the
  // tick queue before microtasks, so one microtask per batch still coalesces
  // everything produced in the same turn of the loop into a single write().
  _schedule() {
    if (this._flushScheduled) {
      return;
    }
    this._flushScheduled = true;
    queueMicrotask(this._flushSoon);
  }

  _flush() {
    // leaving a batch, whatever the reason: from here on frames are produced
    // outside a socket read and have to schedule their own write
    this._inBatch = false;
    this._writePending();
  }

  // Hands the queued frames to the socket. The batch is written corked, so the whole
  // of it goes out through one _writev -- both transports implement it (net.Socket's
  // own cork() bumps state.corked, which is what makes clearBuffer() take the vector
  // path), and that is what keeps a HEADERS + DATA + WINDOW_UPDATE batch in a single
  // TCP segment even though the frames were never concatenated into one buffer.
  //
  // Still only one batch at a time: the buffers we hand over belong to the socket
  // until the callback fires -- a TLS socket encrypts at flush time, which can be well
  // after the next socket read -- and an application that keeps writing to a buffer it
  // passed to stream.end() would otherwise change bytes already promised. The queue
  // fills up behind the outstanding write and the cap in sendData() defers streams for
  // lack of room, which _onWritten() restarts.
  _writePending() {
    if (this._outLen === 0) {
      return;
    }
    if (!this._open) {
      this._outQ = [];
      this._stage = null;
      this._stageLen = 0;
      this._outLen = 0;
      this._writing = false;
      return;
    }
    if (this._writing) {
      return;
    }
    this._writing = true;
    this._commit();
    const batch = this._outQ;
    this._outQ = [];
    this._frozenLen = this._outLen;
    this._outLen = 0;
    const socket = this.socket;
    if (batch.length === 1) {
      // the common case for a small response: everything the batch holds went into one
      // staging buffer, so there is nothing to coalesce and corking would only add a
      // round trip through the stream's own buffer (OPTIMIZATION.md item 16)
      socket.write(batch[0], this._onWrittenCb);
    } else {
      // one callback for the batch, put on its last chunk: writes complete in order, so
      // the last one is the point at which every buffer in it is the socket's no longer
      socket.cork();
      for (let i = 0; i < batch.length - 1; i++) {
        socket.write(batch[i]);
      }
      socket.write(batch[batch.length - 1], this._onWrittenCb);
      socket.uncork();
    }
  }

  _onWritten() {
    this._writing = false;
    // Dropping the batch is the whole release: the frames were never assembled into
    // anything shared, so there is no tail to move down and nothing to reset.
    this._frozenLen = 0;
    // Room in the queue is a reason to stall a stream, so its release is also a
    // reason to restart one: without this, a stream deferred for lack of room has
    // no event that ever looks at it again unless the peer also owes it credit.
    this._releaseAll();
    this._writePending();
    // NOT here, and this is a measured trade-off rather than an oversight. Finishing the close
    // as soon as our own write lands does two things at once: it lets a peer that says nothing
    // after our GOAWAY go away (the session is destroyed and the FIN goes out, which is what node
    // does -- see the silent-peer case in test/goaway.test.js), and it also destroys the session
    // before the peer's answer to the GOAWAY can be read, which costs seventeen event-order
    // traces that agree with node today. Deferring the same look-over with `setImmediate(this...)`,
    // which is how node re-runs it (`setImmediate(() => session[kMaybeDestroy]())` in Http2Stream's
    // teardown, "Delay with setImmediate so we don't do it on the nghttp2 stack"), still costs
    // seven: the session 'close' then goes out ahead of the peer's, in the idle-timeout and goaway
    // traces. The read-side finish (`_onGoaway` -> close) and the stream teardown are therefore the
    // only re-checks, and a silent peer keeps the connection. Closing that gap means re-deriving
    // the traces against node, not moving this call around -- and part of it is not ours to close:
    // see tools/probe-stream-teardown.js for node closing a server stream whose request body was
    // never read, the trigger its own re-check waits for.
  }

  _sendSettings() {
    const s = this.localSettings;
    // Only what the application set, in the order it set it -- see _advertised in the constructor.
    // An empty list is a legal and, for node, the usual SETTINGS frame: a zero-length payload.
    const names = [...this._advertised].filter((k) => ID_BY_SETTING[k] !== undefined
      && s[k] !== undefined);
    // Six octets per setting (RFC 9113 6.5), so the size is known before the loop and
    // every octet of it is written here -- allocUnsafe rather than alloc, which would
    // zero memory we are about to fill. Once per session, so this is the code saying
    // what it means; there is no measurable claim attached to it.
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
    this._flush();
  }

  sendHeaders(sid, block, end) {
    this.state.deflateDynamicTableSize = this._encoder.size;
    const max = this.remoteSettings.maxFrameSize;
    if (block.length <= max) {
      this._frame(HEADERS, F_END_HEADERS | (end ? F_END_STREAM : 0), sid, block);
      return;
    }
    this._frame(HEADERS, 0, sid, block.subarray(0, max));
    let off = max;
    for (;;) {
      const n = Math.min(block.length - off, max);
      const last = off + n >= block.length;
      this._frame(CONTINUATION, last ? F_END_HEADERS | (end ? F_END_STREAM : 0) : 0,
        sid, block.subarray(off, off + n));
      off += n;
      if (last) {
        return;
      }
    }
  }

  // Writes DATA within the stream and connection windows. Returns true when
  // everything went out; false means the caller should wait for WINDOW_UPDATE.
  sendData(stream, buf, end) {
    if (stream._queued !== null) { // preserve order behind a stall
      if (buf.length > 0) {
        stream._queued.push(buf);
        this._heldBytes += buf.length;
      }
      if (end) {
        stream._endQueued = true;
      }
      return false;
    }
    const total = buf.length;
    const max = this.remoteSettings.maxFrameSize;
    let off = 0;
    while (off < total) {
      // Room in the queue is a third limit next to the two windows. Without it every
      // stream produced its whole body while one write was still in flight, because
      // _flush() cannot do anything while writing: 64 connections serving 256 KB
      // responses staged 16 MB each, put 8 MB into a single write(), and moved
      // 251 KB per request through the compaction that has just been removed.
      const room = Math.min(this._connSendWin, stream._sendWin,
        MAX_OUT_BATCH - this._outLen - HEADER_LEN);
      const take = Math.min(room, max, total - off);
      if (take <= 0) {
        break;
      }
      const last = end && off + take === total;
      // owned: a view of the application's buffer, which is not copied any more
      this._frame(DATA, last ? F_END_STREAM : 0, stream.id, buf.subarray(off, off + take),
        true);
      this._connSendWin -= take;
      stream._sendWin -= take;
      off += take;
    }
    const done = off >= total;
    // an empty body still owes the peer END_STREAM; go through _frame() so that
    // a response finished outside a read batch also gets its write scheduled
    if (done && end && total === 0) {
      this._frame(DATA, F_END_STREAM, stream.id, null);
    }
    if (!done) {
      stream._queued = [buf.subarray(off)];
      this._heldBytes += total - off;
      if (end) {
        stream._endQueued = true;
      }
      if (this._blocked === null) {
        this._blocked = new Set();
      }
      this._blocked.add(stream);
      this.state.outboundQueueSize = this._blocked.size;
    } else if (end) {
      stream._endLocalSide();
    }
    this.state.remoteWindowSize = this._connSendWin;
    if (this._outLen >= 32 * 1024) {
      this._flush();
    }
    return done && this._connSendWin > 0 && stream._sendWin > 0;
  }

  sendRst(sid, code, reason) {
    const tail = code !== 0 && reason ? Buffer.byteLength(reason) : 0;
    const body = Buffer.alloc(4 + tail);
    body.writeUInt32BE(code, 0);
    if (tail) {
      body.write(reason, 4);
    }
    this._frame(RST_STREAM, 0, sid, body, true);
  }

  sendGoaway(lastStreamID, code, reason) {
    // Writing a GOAWAY is not an event: node emits 'goaway' only when one is RECEIVED
    // (internal/http2/core.js:769, and the comment above Http2Session.prototype.goaway says
    // "sending a goaway frame has no other effect"), so the application's 'goaway' listener is
    // the peer's decision, never our own. Announcing our own here made a listener fire on a
    // shutdown it had no part in.
    const tail = reason ? Buffer.byteLength(reason) : 0;
    const body = Buffer.alloc(8 + tail);
    body.writeUInt32BE(lastStreamID >>> 0, 0);
    body.writeUInt32BE(code, 4);
    if (tail) {
      body.write(reason, 8);
    }
    this._frame(GOAWAY, 0, 0, body, true);
    this._goawaySent = true;
  }

  _windowUpdate(sid, inc) {
    if (inc <= 0) {
      return;
    }
    this._need(HEADER_LEN + 4);
    const off = this._stageLen;
    writeHeader(this._stage, off, 4, WINDOW_UPDATE, 0, sid);
    this._stage.writeUInt32BE(inc, off + HEADER_LEN);
    this._stageLen = off + HEADER_LEN + 4;
    this._outLen += HEADER_LEN + 4;
    // same rule as _frame(): a credit released between batches has to reach the
    // socket by itself, or the sender waits for a window update we never wrote
    if (!this._inBatch) {
      this._schedule();
    }
  }

  // ---- inbound ----------------------------------------------------------------
  _onData(chunk) {
    const n = chunk.length;
    let i = 0;
    this._inBatch = true;
    this._ended.length = 0; // the last batch's finished streams are no longer here
    this._touchIdleTimer();

    if (this._prefaceSeen < PREFACE_LEN) {
      const take = Math.min(PREFACE_LEN - this._prefaceSeen, n);
      const expected = f.PREFACE.subarray(this._prefaceSeen, this._prefaceSeen + take);
      if (!chunk.subarray(0, take).equals(expected)) {
        return this._fatal(PROTOCOL_ERROR, 'connection preface malformed');
      }
      this._prefaceSeen += take;
      // node's server-side session fires 'connect' once the peer's preface is in,
      // before any SETTINGS have been decoded
      if (this._prefaceSeen === PREFACE_LEN) {
        this.emit('connect');
      }
      i = take;
      // the chunk stopped inside the preface: nothing was produced, but the
      // batch flag must not leak into whatever the peer sends next
      if (i >= n) {
        this._endBatch(); return;
      }
    }

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
          const ok = this._dispatchPieces(this._fragType, this._fragFlags, this._fragSid, pieces, len);
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
      if (!this._dispatch(chunk, i + HEADER_LEN, f.FH_LEN, f.FH_TYPE, f.FH_FLAGS, f.FH_SID)) {
        return;
      }
      i += total;
      if (!this._open) {
        return;
      }
    }

    this._endBatch();
  }

  _allowFrame(type, len, flags) {
    // RFC 9113 3.4 requires the client's first frame to be a SETTINGS frame, and nghttp2
    // -- the stack node:http2 runs -- enforces it literally: once the connection preface
    // has been read, the next frame must be SETTINGS and must not carry the ACK flag,
    // otherwise the session is terminated with GOAWAY(PROTOCOL_ERROR) and the debug
    // string "SETTINGS expected" (lib/nghttp2_session.c v1.70.0, the
    // NGHTTP2_IB_READ_FIRST_SETTINGS state). Measured against a live node server: HEADERS,
    // PING and a SETTINGS-with-ACK each get exactly that, where this accepted all three.
    // It is worth the check because the settings parameterise everything that follows --
    // initial window, header table size, concurrent streams -- so a request answered
    // before any of them is announced is work done against numbers that may still move.
    if (this._firstFramePending) {
      this._firstFramePending = false;
      if (type !== SETTINGS || (flags & F_ACK) !== 0) {
        this._fatal(PROTOCOL_ERROR, 'SETTINGS expected');
        return false;
      }
    }
    if (type !== PING && type !== SETTINGS && len > this.localSettings.maxFrameSize) {
      this._fatal(FRAME_SIZE_ERROR, 'frame larger than our advertised max frame size');
      return false;
    }
    return true;
  }

  _dispatch(buf, start, len, type, flags, sid) {
    try {
      this._handle(type, flags, sid, buf, start, len);
    } catch (err) {
      return this._onFrameError(err, sid);
    }
    return this._open;
  }

  // A frame whose payload straddled two reads is dispatched as the pieces it
  // arrived in. A DATA payload is handed to the application as those pieces; a
  // header block is decoded out of them; the control frames -- small and of a
  // fixed size -- are put in a buffer of their own for the parse, which is the
  // parse the whole-frame path uses.
  _dispatchPieces(type, flags, sid, pieces, len) {
    try {
      // RFC 9113 6.10: the same rule the whole-frame path applies (see _handle).
      // A header block that is unfinished may only be followed by CONTINUATION.
      if (this._hdrSid !== -1 && type !== CONTINUATION) {
        this._fatal(PROTOCOL_ERROR, 'frame interleaved with an unfinished header block');
        return this._open;
      }
      if (type === DATA) {
        // RFC 9113 6.1: a padded payload's views are not the payload -- the pad
        // length is at the front and the padding at the back -- so the padding
        // comes off the views and the flow control counts the data alone.
        let data = len;
        if (flags & F_PADDED) {
          // A padded frame with no payload has no pad length to read, and node
          // answers that with GOAWAY(PROTOCOL_ERROR) (measured, tools/probe-pad-bad.js).
          if (len === 0) {
            this._fatal(PROTOCOL_ERROR, 'padded DATA frame with an empty payload');
            return this._open;
          }
          data = trimPadding(pieces);
          if (data < 0) {
            this._fatal(PROTOCOL_ERROR, 'pad larger than the DATA payload');
            return this._open;
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
          this._fatal(PROTOCOL_ERROR, 'CONTINUATION without a matching HEADERS');
          return this._open;
        }
        // CVE-2024-28182: one block may not be spread over an unbounded number of frames.
        // Counted here rather than by size because the cost being bounded is the frame
        // handling itself; the byte bound of _appendHdr stays as it is, for the other one.
        if (++this._hdrCont > MAX_CONTINUATIONS) {
          this._fatal(INTERNAL_ERROR, 'CONTINUATION: too many frames for one block');
          return this._open;
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
      this._handle(type, flags, sid, buf, 0, len);
    } catch (err) {
      return this._onFrameError(err, sid);
    }
    return this._open;
  }

  _handle(type, flags, sid, buf, start, len) {
    // RFC 9113 6.10: once a header block has begun, the only frames that may follow are
    // CONTINUATION frames for the same stream. An interruption is a connection error, not
    // a thing to skip over -- nghttp2 answers it with
    // GOAWAY(PROTOCOL_ERROR) 'unexpected non-CONTINUATION frame or stream_id is invalid',
    // measured on node for a SETTINGS, PING, WINDOW_UPDATE, RST_STREAM and DATA frame all
    // five. Note that this also covers HEADERS, which used to have its own check here.
    if (this._hdrSid !== -1 && type !== CONTINUATION) {
      this._fatal(PROTOCOL_ERROR, 'frame interleaved with an unfinished header block');
      return;
    }
    switch (type) {
      case SETTINGS:
        // RFC 9113 6.5: SETTINGS is a connection control frame -- it names no stream, and an
        // ACK carries no entries. Measured on node (h2spec http2/6.5), a SETTINGS on stream 1
        // gets GOAWAY(PROTOCOL_ERROR) and a SETTINGS|ACK with one octet of payload gets
        // GOAWAY(FRAME_SIZE_ERROR). This answered both with an ACK of its own and kept the
        // connection up, so a peer that had misframed its own settings never heard about it.
        if (sid !== 0) {
          this._fatal(PROTOCOL_ERROR, 'SETTINGS with a non-zero stream id');
          return;
        }
        if (flags & F_ACK) {
          if (len !== 0) {
            this._fatal(FRAME_SIZE_ERROR, 'SETTINGS ACK must have an empty payload');
            return;
          }
          this.pendingSettingsAck = false;
          return;
        }
        this._applySettings(buf, start, len);
        this.emit('remoteSettings', this.remoteSettings);
        this._frame(SETTINGS, F_ACK, 0, null);
        return;
      case PING: {
        // RFC 9113 6.7 puts the same two requirements on both halves of the exchange: stream
        // id 0 and a payload of exactly 8 octets. Measured on node (h2spec http2/6.7 and
        // 5.4.1): a PING on stream 3 gets GOAWAY(PROTOCOL_ERROR), and a PING|ACK with 4 octets
        // gets GOAWAY(FRAME_SIZE_ERROR). This ACKed the first -- echoing an identifier it is not
        // allowed to interpret -- and answered the second with nothing at all.
        if (sid !== 0) {
          this._fatal(PROTOCOL_ERROR, 'PING with a non-zero stream id'); return;
        }
        if (len !== 8) {
          this._fatal(FRAME_SIZE_ERROR, 'PING payload must be 8 octets'); return;
        }
        // The payload goes to the application, which may keep it. A PING that arrived
        // in pieces is put in a buffer of its own for the parse -- a fresh one, not a
        // reused one -- so the view does not change under the application
        // (AGENTS.md 5: issued buffers stay valid). Eight octets, once per PING: a
        // copy is the cheap way out, and the copy is made once and handed to both
        // halves of the exchange. Node's payload comes out of the C++ layer as a
        // fresh buffer, so it is stable there.
        // copy through allocUnsafe rather than Buffer.from(subarray): the payload goes
        // to the application and the ACK frame, so it must be a fresh buffer either
        // way, and the pooled allocation of a small buffer is the cheaper way to get
        // one -- see the block cache in lib/headers.js for the same measurement
        const payload = Buffer.allocUnsafe(8);
        buf.copy(payload, 0, start, start + 8);
        if (flags & F_ACK) {
          this.emit('ping', true, payload);
          return;
        }
        this._frame(PING, F_ACK, 0, payload);
        this.emit('ping', false, payload);
        return;
      }
      case HEADERS:
        this._beginHeaders(sid, flags, buf, start, len);
        return;
      case CONTINUATION:
        if (this._hdrSid !== sid) {
          this._fatal(PROTOCOL_ERROR, 'CONTINUATION without a matching HEADERS');
          return;
        }
        // CVE-2024-28182: one block may not be spread over an unbounded number of frames.
        // Counted here rather than by size because the cost being bounded is the frame
        // handling itself; the byte bound of _appendHdr stays as it is, for the other one.
        if (++this._hdrCont > MAX_CONTINUATIONS) {
          this._fatal(INTERNAL_ERROR, 'CONTINUATION: too many frames for one block');
          return;
        }
        this._appendHdr(buf, start, len);
        if (flags & F_END_HEADERS) {
          this._finishHeaders();
        }
        return;
      case DATA:
        this._onDataFrame(sid, flags, buf, start, len);
        return;
      case WINDOW_UPDATE:
        if (len !== 4) {
          this._fatal(FRAME_SIZE_ERROR, 'WINDOW_UPDATE payload'); return;
        }
        this._onWindowUpdate(sid, buf.readUInt32BE(start) & 0x7fffffff);
        return;
      case RST_STREAM:
        if (len !== 4) {
          this._fatal(FRAME_SIZE_ERROR, 'RST_STREAM payload'); return;
        }
        this._onRst(sid, buf.readUInt32BE(start));
        return;
      case GOAWAY:
        this._onGoaway(buf, start, len);
        return;
      case PRIORITY:
        // RFC 9113 6.3: the payload is fixed at 5 octets, and the frame always names a stream --
        // a PRIORITY received on stream 0 MUST be answered with a connection error of type
        // PROTOCOL_ERROR. The frame's CONTENTS are deprecated (RFC 9218) and ignored below, but
        // neither its length nor its stream id is ignorable: measured on node (h2spec http2/6.3),
        // a 4-octet PRIORITY gets GOAWAY(FRAME_SIZE_ERROR), while node silently drops a PRIORITY
        // on stream 0 -- the one place the parity rule and the RFC disagree, so we follow the RFC.
        // The contents also carry the self-dependency RFC 7540 5.3.1 made a stream error of type
        // PROTOCOL_ERROR, and that rule is gone: RFC 9113 5.3.2 deprecated the priority
        // signalling and 6.3 keeps only the two rules above. Both stacks drop a self-dependent
        // PRIORITY (probe case K4: ACK ANSWERED, the frame ignored) and h2spec's http2/5.3.1 arm
        // still asks for RFC 7540's PROTOCOL_ERROR -- do not bend this to the tool (AGENTS.md 10).
        if (len !== 5) {
          this._fatal(FRAME_SIZE_ERROR, 'PRIORITY payload must be 5 octets'); return;
        }
        if (sid === 0) {
          this._fatal(PROTOCOL_ERROR, 'PRIORITY on stream 0'); return;
        }
        return; // RFC 9218: weight-only, ignored
      case PUSH_PROMISE:
        this._fatal(PROTOCOL_ERROR, 'peer sent PUSH_PROMISE');
        break;

      default:
        // RFC 9113 5.5: "Implementations MUST discard frames that have unknown or unsupported
        // types." Measured on a live node server with tools/probe-inbound-limits.js: type 10
        // (ALTSVC, RFC 7807), type 99 and type 0xff with a 4 KiB payload all leave the
        // connection up and the next request served, and h2lean answers the same. Answering a
        // fatal error here would break every extension frame a peer has not told us about.
        // What is NOT ignored is a frame that interrupts a field block -- 5.5 forbids those
        // outright, and that is the case above.
                                        // unknown frame types are ignored
    }
  }

  _applySettings(buf, start, len) {
    if (len % 6 !== 0) {
      this._fatal(FRAME_SIZE_ERROR, 'SETTINGS payload size'); return;
    }
    // CVE-2020-11080: the frame size limit does not cover SETTINGS (_allowFrame exempts
    // it, as RFC 9113 6.2 requires), so without an entry bound a peer can hand us any
    // number of entries to parse. 32 is what node accepts; measured, see the constant.
    if (len / 6 > MAX_SETTINGS_ENTRIES) {
      this._fatal(ENHANCE_YOUR_CALM, 'SETTINGS: too many setting entries');
      return;
    }
    const seen = this.remoteSettings;
    for (let i = 0; i + 6 <= len; i += 6) {
      const id = buf.readUInt16BE(start + i);
      const value = buf.readUInt32BE(start + i + 2);
      const name = SETTINGS_BY_ID[id];
      if (name === undefined) {
        continue;
      } // unknown settings are ignored
      if (id === 0x5 && (value < 16384 || value > 16777215)) {
        this._fatal(PROTOCOL_ERROR, 'SETTINGS_MAX_FRAME_SIZE out of range');
        return;
      }
      if (id === 0x4 && value > 0x7fffffff) {
        this._fatal(FLOW_CONTROL_ERROR, 'SETTINGS_INITIAL_WINDOW_SIZE out of range');
        return;
      }
      if (id === 0x1 && value > 0x7fffffff) {
        this._fatal(COMPRESSION_ERROR, 'SETTINGS_HEADER_TABLE_SIZE out of range');
        return;
      }
      // RFC 9113 6.5.2 defines SETTINGS_ENABLE_PUSH as a boolean, and 6.5.1 makes a value
      // outside its definition a PROTOCOL_ERROR. Measured on node (h2spec http2/6.5.2):
      // ENABLE_PUSH=2 ends the connection; an unknown setting id is still ignored above.
      if (id === 0x2 && value > 1) {
        this._fatal(PROTOCOL_ERROR, 'SETTINGS_ENABLE_PUSH must be 0 or 1');
        return;
      }
      seen[name] = value;
    }
    // RFC 9113 6.9.2: a new INITIAL_WINDOW_SIZE shifts every open stream window
    // by the same delta, and may legitimately drive it negative
    const delta = seen.initialWindowSize - this._prevInitialWin;
    if (delta !== 0) {
      this._prevInitialWin = seen.initialWindowSize;
      for (const s of this._streams.values()) {
        s._sendWin += delta;
      }
    }
    // Only the encoder's table is bounded by the peer's setting. Our own decoder keeps
    // the size WE advertised, and the peer changes it by sending a size update, which
    // Decoder.setMaxSize checks against that -- resizing the decoder here instead made
    // us evict entries a peer with a small table had legitimately indexed for us.
    this._encoder.setPeerTableSize(seen.headerTableSize);
    this.state.remoteWindowSize = this._connSendWin;
  }

  // ---- HEADERS assembly -------------------------------------------------------
  // The pad length and the priority block at the front of a HEADERS payload
  // (RFC 9113 6.2 and 6.3). `head` is the buffer the payload's first octets are
  // in, `base` the offset of the payload inside it and `headLen` how many of the
  // payload's octets are there: the whole-frame path has all of them, the split
  // path copies the first six into a scratch. Returns the offset of the header
  // block inside the payload, or -1 after refusing the frame.
  //
  // The length of the block itself is left in _hdrFrag, because it is not the
  // rest of the payload: a padded frame carries its padding at the END of the
  // payload (RFC 9113 6.2: "The remainder of the frame payload is the header
  // block fragment"), so the block ends where the padding starts. Feeding the
  // padding to the HPACK decoder is what a server must not do: the padding is
  // not part of the block, and a block that ends with eight zero octets decodes
  // as a literal header with no name and no value -- which is what h2lean
  // answered to a valid padded HEADERS with GOAWAY(COMPRESSION_ERROR), while node
  // served the request (measured: tools/probe-hdr-pad.js, "the RFC shape: pad
  // length, block, padding"; the same frame is what h2spec's generic/3.2 sends,
  // and h2spec is the check that failed).
  _headersPrefix(head, base, headLen, sid, flags, len) {
    let p = 0;
    let l = len;
    if (flags & F_PADDED) {
      // A padded frame with no payload has no pad length octet to read at all,
      // and node answers that with GOAWAY(PROTOCOL_ERROR) (measured,
      // tools/probe-hdr-pad.js: 'HEADERS: insufficient padding space').
      if (headLen < 1) {
        this._fatal(PROTOCOL_ERROR, 'padded HEADERS frame with an empty payload');
        return -1;
      }
      const pad = head[base];
      p += 1;
      l -= 1 + pad;
      if (l < 0) {
        this._fatal(PROTOCOL_ERROR, 'pad larger than the HEADERS payload'); return -1;
      }
    }
    if (flags & F_PRIORITY) {
      // The 5 octets were skipped without ever being read, which is how both of the rules
      // RFC 9113 5.3 puts on them went unenforced. Measured on node (h2spec http2/5.3.1 and
      // 6.3): a HEADERS frame whose priority block is truncated is GOAWAY(FRAME_SIZE_ERROR),
      // and a stream that depends on itself is GOAWAY(PROTOCOL_ERROR) -- a connection error,
      // not a stream one, because the dependency is a statement about the whole tree.
      if (l < 5) {
        this._fatal(FRAME_SIZE_ERROR, 'HEADERS priority block must be 5 octets');
        return -1;
      }
      if ((head.readUInt32BE(base + p) & 0x7fffffff) === sid) {
        this._fatal(PROTOCOL_ERROR, 'HEADERS depends on itself');
        return -1;
      }
      p += 5; l -= 5;
    }
    this._hdrFrag = l;
    return p;
  }

  _beginHeaders(sid, flags, buf, start, len) {
    if ((sid & 1) === 0) {
      this._fatal(PROTOCOL_ERROR, 'even-numbered stream id from a client');
      return;
    }
    this._hdrCont = 0; // one block, however many frames it takes
    const p = this._headersPrefix(buf, start, len, sid, flags, len);
    if (p < 0) {
      return;
    }
    const l = this._hdrFrag;
    if (flags & F_END_HEADERS) {
      this._processHeaders(sid, (flags & F_END_STREAM) !== 0, buf.subarray(start + p, start + p + l));
      return;
    }
    this._hdrSid = sid;
    this._hdrEnd = (flags & F_END_STREAM) !== 0;
    this._hdrLen = 0;
    this._appendHdr(buf, start + p, l);
  }

  // A HEADERS frame whose payload straddled two reads: the pad length and the
  // priority block are at its front and are at most six octets, so they are
  // copied into the scratch and read there. The header block itself stays in
  // the views and is decoded out of them.
  _beginHeadersPieces(sid, flags, pieces, len) {
    if ((sid & 1) === 0) {
      this._fatal(PROTOCOL_ERROR, 'even-numbered stream id from a client');
      return;
    }
    this._hdrCont = 0;
    const pre = len < 6 ? len : 6;
    pieceCopyTo(this._hdrPrefix, 0, pieces, 0, pre);
    const p = this._headersPrefix(this._hdrPrefix, 0, pre, sid, flags, len);
    if (p < 0) {
      return;
    }
    const l = this._hdrFrag;
    if (flags & F_END_HEADERS) {
      this._processBlockPieces(sid, (flags & F_END_STREAM) !== 0, pieces, p, p + l);
      return;
    }
    this._hdrSid = sid;
    this._hdrEnd = (flags & F_END_STREAM) !== 0;
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
    // No maxHeaderListSize test here, although this is where the octets are counted: the limit
    // is about the size of the header LIST, which is only known once the block is decoded, and
    // h2lean applies it there (_processHeaders). Measured against node, an over-limit block
    // arriving in one HEADERS frame gets RST_STREAM(ENHANCE_YOUR_CALM) and the connection
    // stays up; a check on the compressed length here made it GOAWAY(PROTOCOL_ERROR) instead,
    // which turns one bad request into an error for every other client on the session. The
    // buffer is bounded without it: MAX_CONTINUATIONS frames of at most
    // localSettings.maxFrameSize octets each, 8 x 16 KiB by default.
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
    const sid = this._hdrSid;
    const end = this._hdrEnd;
    if (this._hdrPieces !== null) {
      const pieces = this._hdrPieces;
      const len = this._hdrLen;
      this._hdrSid = -1;
      this._hdrLen = 0;
      this._hdrPieces = null;
      this._processBlockPieces(sid, end, pieces, 0, len);
      return;
    }
    const block = this._hdr.subarray(0, this._hdrLen);
    this._hdrSid = -1;
    this._hdrLen = 0;
    this._processHeaders(sid, end, block);
  }

  // Bucket for the request shape cache. A 32-bit number rather than a string
  // because latin1Slice allocated one string per request and the CPU profile
  // showed that cost as GC time; a collision is caught by the byte comparison
  // at the lookup site, which then simply replaces the older entry.
  // Identical bytes do NOT always mean identical fields -- HPACK indexes are
  // only meaningful against the dynamic table, so the cache is dropped as a
  // whole whenever the peer changes that table (decoder.epoch).
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

  // The frozen headers object for one shape. Object.freeze is shallow and
  // node's toHeaderObject puts set-cookie in an array, so that array has to be
  // frozen too -- otherwise push() would corrupt every other request sharing it.
  _sharedObject(fields) {
    const o = this._toObject(fields);
    for (let i = 0; i < fields.length; i += 2) {
      const v = o[fields[i]];
      if (Array.isArray(v)) {
        Object.freeze(v);
      }
    }
    return Object.freeze(o);
  }

  // Validates a decoded request (or trailer) field list. Returns null when the fields
  // may go to the application exactly as they are, { problem } when the stream has to be
  // refused, or { fields } when only fields with illegal names were dropped. See the
  // block comment on the constants above for why each rule exists and where the
  // behaviour came from.
  _requestProblem(src, trailer) {
    let method = null, path = null, scheme = null, authority = null, host = null;
    let protocol = null, contentLength = null;
    // RFC 9113 8.1: pseudo-header fields, if any, come before all regular fields. Once
    // one has been seen, a later pseudo-header is a violation -- and in a trailer block
    // no pseudo-header is allowed at all, so the flag starts in that state.
    let pseudoAllowed = !trailer;
    let kept = null; // copied list, only if something is dropped

    for (let i = 0; i < src.length; i += 2) {
      const name = src[i];
      const value = src[i + 1];

      if (name.charCodeAt(0) === 58 /* : */) {
        if (!pseudoAllowed) {
          return { problem: `pseudo-header ${name} after a field` };
        }
        // check_pseudo_header() in nghttp2: a repeated pseudo-header, or one with an
        // empty value, is refused. An empty value is the smuggling case -- a receiver
        // that drops the field and one that keeps it disagree about the request.
        if (value.length === 0) {
          return { problem: `${name} with an empty value` };
        }
        switch (name) {
          case ':method':
            if (method !== null) {
              return { problem: ':method repeated' };
            }
            if (!TOKEN_RE.test(value)) {
              return { problem: `:method ${value} is not a token` };
            }
            method = value;
            break;
          case ':path':
            if (path !== null) {
              return { problem: ':path repeated' };
            }
            path = value;
            break;
          case ':scheme':
            if (scheme !== null) {
              return { problem: ':scheme repeated' };
            }
            if (!SCHEME_RE.test(value)) {
              return { problem: `:scheme ${value} is not a scheme` };
            }
            scheme = value;
            break;
          case ':authority':
            if (authority !== null) {
              return { problem: ':authority repeated' };
            }
            if (!AUTHORITY_RE.test(value)) {
              return { problem: `:authority ${value} has illegal characters` };
            }
            authority = value;
            break;
          case ':protocol': // RFC 8441/9220, extended CONNECT
            if (protocol !== null) {
              return { problem: ':protocol repeated' };
            }
            // Extended CONNECT has to be offered by us before a peer may use it, and we
            // advertise enableConnectProtocol off by default -- which is what makes node
            // refuse a :protocol it was never offered.
            if (!this.localSettings.enableConnectProtocol) {
              return { problem: ':protocol without enableConnectProtocol' };
            }
            protocol = value;
            break;
          default:
            // RFC 9113 8.1: an unknown pseudo-header is a PROTOCOL_ERROR, and :status
            // lands here too -- a server never receives it on a request.
            return { problem: `unknown pseudo-header ${name}` };
        }
        continue;
      }

      pseudoAllowed = false;
      if (HAS_UPPER.test(name)) {
        return { problem: `field name is not lower-case: ${name}` };
      }
      if (!NAME_LOWER_RE.test(name)) {
        // Illegal character in a regular name: drop the field, keep the stream.
        if (kept === null) {
          kept = src.slice(0, i);
        }
        continue;
      }
      if (INBOUND_FORBIDDEN.has(name)) {
        return { problem: `${name} is a connection-specific field` };
      }
      if (name === 'te') {
        // RFC 9113 8.2.2: the TE field may appear only with the value "trailers". This is the
        // Transfer-Encoding hole of CVE-2020-8287 seen from the other side.
        if (value !== 'trailers') {
          return { problem: `te: ${value} is not "trailers"` };
        }
      } else if (name === 'content-length') {
        if (!NUM_RE.test(value)) {
          return { problem: `content-length ${value} is not numeric` };
        }
        // Two copies are the smuggling case itself; nghttp2 refuses a second one even
        // when both say the same thing, and node does too (measured).
        if (contentLength !== null) {
          return { problem: 'content-length repeated' };
        }
        contentLength = value;
      } else if (name === 'host') {
        if (!AUTHORITY_RE.test(value)) {
          if (kept === null) {
            kept = src.slice(0, i);
          } continue;
        }
        if (host !== null) {
          return { problem: 'host repeated' };
        }
        host = value;
      }
      // The last of the per-field rules, and the only one that looks at the value as a
      // whole: a character that may not appear in a field value makes the field
      // disappear, and the stream lives (NGHTTP2_ERR_IGN_HTTP_HEADER, measured on node:
      // bench/micro/probe-value-matrix.js). It sits after the rules above because those win: a
      // 'te' or a 'content-length' carrying a CR is a PROTOCOL_ERROR on node, not a
      // dropped field (bench/micro/probe-value-order.js).
      if (hasBadValue(value)) {
        if (kept === null) {
          kept = src.slice(0, i);
        }
        continue;
      }
      if (kept !== null) {
        kept.push(name, value);
      }
    }

    if (trailer) {
      return kept === null ? null : { fields: kept };
    }

    if (method === null) {
      return { problem: 'missing :method' };
    }
    if (method === 'CONNECT' && protocol === null) {
      // RFC 9113 8.5: for CONNECT the :path and :scheme fields are not applicable, and
      // the authority is everything the request is about.
      if (scheme !== null || path !== null) {
        return { problem: 'CONNECT must not carry :scheme or :path' };
      }
      if (authority === null) {
        return { problem: 'CONNECT without :authority' };
      }
      return kept === null ? null : { fields: kept };
    }
    if (protocol !== null && method !== 'CONNECT') {
      return { problem: ':protocol requires CONNECT' };
    }
    if (path === null) {
      return { problem: 'missing :path' };
    }
    if (scheme === null) {
      return { problem: 'missing :scheme' };
    }
    // RFC 9113 8.3.1: a client request carries the target in :authority or in host.
    if (authority === null && host === null) {
      return { problem: 'missing :authority and host' };
    }
    // check_path() in nghttp2: with an http or https scheme the path is an absolute
    // path, or "*" for a system-wide OPTIONS. Anything else is refused -- a relative
    // form is exactly what a downstream HTTP/1 translator would resolve on its own.
    if ((scheme === 'http' || scheme === 'https')
        && path.charCodeAt(0) !== 47 /* / */
        && !(method === 'OPTIONS' && path === '*')) {
      return { problem: `:path ${path} does not start with "/"` };
    }
    return kept === null ? null : { fields: kept };
  }

  // A header block that arrived whole and one that arrived in pieces are the
  // same block: the octets are the same, so the hash, the cache entry and the
  // verdict are the same. `block` is null for the split one, and the range
  // [from, to) of `pieces` is where the block sits in the views.
  _processHeaders(sid, end, block) {
    this._processBlock(sid, end, block, null, 0, 0);
  }

  _processBlockPieces(sid, end, pieces, from, to) {
    this._processBlock(sid, end, null, pieces, from, to);
  }

  _processBlock(sid, end, block, pieces, from, to) {
    const existing = this._streams.get(sid);
    const flat = this._flat;
    const shapes = existing === undefined ? this._shapes : null;
    const slot = shapes === null ? 0
      : (block !== null ? this._shapeSlot(block) : this._shapeSlotPieces(pieces, from, to));
    const hit = shapes === null ? undefined : shapes.get(slot);
    // where this request's field list and header object come from
    let fields = flat;
    let shared = null;
    let bytes = 0;
    // verdict of the inbound validator: null means the fields may go to the application
    let verdict;

    try {
      if (hit !== undefined && (block !== null
        ? hit.block.equals(block)
        : pieceEquals(hit.block, pieces, from, to))) {
        // the whole header block was seen before on this connection under the same table
        // state: neither decode nor rebuild -- and no re-validation either, because the
        // fields are the same ones that were checked when the entry was made
        this._shapeHits++;
        bytes = hit.bytes;
        fields = hit.flat;
        shared = hit.headers;
        verdict = hit.verdict;
      } else {
        if (block !== null) {
          this._decoder.decode(block, 0, block.length, flat);
        } else {
          this._decoder.decodePieces(pieces, from, to, flat);
        }
        for (let i = 0; i < flat.length; i++) {
          bytes += flat[i].length;
        }
        // flat is name, value, name, value: one entry overhead per field line
        bytes += (flat.length >> 1) * ENTRY_OVERHEAD;
        verdict = this._requestProblem(flat, existing !== undefined);
        if (verdict !== null && verdict.fields !== undefined) {
          fields = verdict.fields;
        }
        if (shapes !== null) {
          // A client that varies one header per request (x-request-id) makes
          // every lookup a miss and every miss two allocations, which is worse
          // than never caching. Give up once the ratio proves that case. With
          // shareHeaders on, freezing stays uniform either way.
          if (++this._shapeMisses > 64 && this._shapeHits * 4 < this._shapeMisses) {
            this._shapes = null;
          } else {
            if (shapes.size >= MAX_SHAPES) {
              shapes.clear();
            } // bounded: clients vary
            const entry = {
              block: block !== null ? Buffer.from(block) : pieceConcat(pieces, from, to),
              flat: fields.slice(0), bytes, headers: null,
              verdict
            };
            if (this._share) {
              entry.headers = this._sharedObject(fields);
              shared = entry.headers;
            }
            shapes.set(slot, entry);
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
    this.state.inflateDynamicTableSize = this._decoder.size;

    // RFC 9113 5.1 puts the state of the stream a frame names ahead of anything the frame says, so
    // this is decided before the request is even looked at: a HEADERS that arrives after the peer
    // closed the message with its own END_STREAM is a connection error of type STREAM_CLOSED
    // whichever fields the block holds -- measured on node (probe cases U3, U9), where the second
    // block is not a valid request at all and the verdict is still GOAWAY(STREAM_CLOSED). 5.1 reads a
    // frame on a closed stream as "MAY ... treat ... as a connection error", and h2lean takes that
    // option for BOTH the still-present (half-closed(remote)) and the just-closed (_ended) shape: a
    // stream error would be the other reading of half-closed(remote), but node does not make that
    // distinction and h2spec's half-closed(remote) HEADERS check waits for the error, not the silence
    // a discard would give. Sitting above the trailer rule below is also what keeps a LEGAL trailer,
    // which arrives with the message still open, on that path.
    //
    // The arms that fail on BOTH stacks are not this one: they are the frames that arrive after the
    // answer closed the stream, which 5.1 lets the endpoint discard, and the tool's expectation for
    // those is RFC 7540's ("MUST treat that as a connection error of type STREAM_CLOSED") where
    // RFC 9113 says "MAY". Answering them with an error is not a fix -- it would end the connection
    // on a frame the RFC lets the server keep quiet about (AGENTS.md 10).
    if (existing !== undefined ? existing.state.remoteClose === 1
      : (!this._isGone(sid) && this._ended.includes(sid))) {
      this._fatal(STREAM_CLOSED, 'HEADERS after the peer ended the stream');
      return;
    }
    if (existing === undefined) {
      if (sid <= this._maxInboundSid) {
        // An id at or below the highest one opened cannot open a NEW stream: 5.1.1 says a stream the
        // peer skipped past has already left "idle", so it cannot be established again here. Two
        // shapes reach this line. If this side has a record of the id -- refused, or ended by a
        // reset -- the frames that follow are that stream's, and the discard of 5.1 is right (probe
        // U15, U16, U19); node agrees. With no such record the peer is opening a stream on an id it
        // is not allowed to use: 5.1.1 names that an unexpected identifier and mandates a connection
        // error PROTOCOL_ERROR. node stays silent for it -- the deviation h2spec's 5.1.1 check flags
        // -- so this is the second place (after PRIORITY on stream 0) where we follow the RFC over
        // node. The id does not move either way, keeping the frames that follow in the same category.
        //
        // The discard is also what h2spec's two 'closed: ... after sending RST_STREAM frame' arms
        // fail on: RFC 9113 5.1 says of a frame that follows a reset "An endpoint MUST minimally
        // process and then discard any frames it receives in this state", where RFC 7540 made it a
        // stream error of type STREAM_CLOSED. node discards too (probe case U6: ACK), and the
        // conformance tool waits for the error -- on both stacks. Do not answer with an error to
        // satisfy it (AGENTS.md 10).
        if (this._isGone(sid)) {
          return; // refused or reset earlier: the discard of 5.1
        }
        this._fatal(PROTOCOL_ERROR, 'HEADERS on an id below the highest one opened');
        return;
      }
      // The id is taken before anything refuses the request, because the peer has opened it: the
      // frames it had already put on the wire for that id are then the in-flight frames of a stream
      // this side refused -- measured on node, probe case U17, where the DATA that follows the
      // refused HEADERS is ignored and the next request is served -- and not the 'idle' error of 5.1.
      this._maxInboundSid = sid;
      this.state.lastProcStreamID = sid;
    }

    // RFC 9113 8.1.1: a request that fails the message parsing rules is refused with
    // RST_STREAM(PROTOCOL_ERROR), before the application ever sees it. Checking here --
    // after decode, before any stream object or event -- is what makes the difference
    // between "the handler declined the request" and "the handler was never reached",
    // and only the second one denies a smuggling chain its second reader.
    if (verdict !== null && verdict.problem !== undefined) {
      this.sendRst(sid, PROTOCOL_ERROR);
      return;
    }

    if (existing !== undefined) {
      if (!end) {
        // RFC 9113 8.1: a HEADERS block after the request headers is the trailer block, and a
        // trailer closes the message -- it has to arrive with END_STREAM. Without it the body
        // would still be open with its tail already sent. Measured on node (h2spec http2/8.1):
        // RST_STREAM(PROTOCOL_ERROR), the connection untouched.
        this._streamError(existing, PROTOCOL_ERROR, 'trailing HEADERS without END_STREAM');
        return;
      }
      const trailers = this._share ? this._sharedObject(fields) : this._toObject(fields);
      existing.emit('trailers', trailers);
      if (end) {
        existing._remoteEnded();
      }
      return;
    }

    if (this._liveStreams >= this.localSettings.maxConcurrentStreams) {
      this.sendRst(sid, REFUSED_STREAM);
      this._refused.push(sid);
      if (this._refused.length > MAX_REFUSED_HISTORY) {
        this._refused.shift();
      }
      return;
    }

    // The same refusal, for a limit the concurrency guard cannot express. Advertising no
    // SETTINGS_MAX_CONCURRENT_STREAMS (what node does, and now what this does) leaves the guard
    // above comparing against the unlimited default, so a peer that opens streams and then refuses
    // to grant credit is limited by nothing: every HEADERS it sends costs a live stream and an
    // unpromotable response, and that total is what this checks. Refusing at the accept decision is
    // the only place the total can still be kept small -- a stream that exists already has a handler
    // holding its object, and its parked body has to stay parked for the peer that may still open the
    // window. REFUSED_STREAM is the code for "this request was not processed", so the peer may retry
    // it elsewhere, and the connection and every stream already served on it are untouched. The
    // refusal clears itself: credit returns, _release drains, the charge drops below the bound.
    // What is bounded is the cost of accepting, not the total ever: an application that ignores
    // backpressure can still write into a stalled stream without limit.
    if (this._heldCost() >= MAX_SESSION_HOLD) {
      this.sendRst(sid, REFUSED_STREAM);
      this._refused.push(sid);
      if (this._refused.length > MAX_REFUSED_HISTORY) {
        this._refused.shift();
      }
      return;
    }

    // the size limit is re-checked every request, not only when the shape was
    // first seen: the application may have lowered maxHeaderListSize since then.
    // RFC 9113 6.5.2 makes maxHeaderListSize advisory ("a lower limit than what is advertised
    // MAY be enforced") and 7 defines ENHANCE_YOUR_CALM as the code for a peer generating
    // excessive load -- that is what node answers, measured, together with the two things that
    // follow from it:
    // the refusal is per stream (a legal request on the same connection is still answered, and
    // an identical oversized block is refused again rather than served from the shape cache),
    // and the id joins the refused history so the peer's already-sent DATA for it is ignored
    // instead of becoming an error of ours.
    if (bytes > this.localSettings.maxHeaderListSize) {
      this.sendRst(sid, ENHANCE_YOUR_CALM);
      this._refused.push(sid);
      if (this._refused.length > MAX_REFUSED_HISTORY) {
        this._refused.shift();
      }
      return;
    }

    const headers = shared !== null ? shared
      : (this._share ? Object.freeze(this._toObject(fields)) : this._toObject(fields));
    // The one shape in which the content-length promise is already broken before a single body
    // octet could have arrived: the field promises a body and the HEADERS frame ends the message.
    // Refusing at the accept decision -- the same place the parse failures and the size limits are
    // refused -- is what makes the verdict match node's: measured (probe case P7, and T4 of
    // tools/probe-inbound-limits.js for the reading handler), node answers RST_STREAM
    // (PROTOCOL_ERROR) and the application never sees the stream, so nothing is answered on it.
    // Letting it through to Http2Stream._remoteEnded() instead resets the stream and then hands
    // the handler a destroyed one, and a handler that answers it puts a response on the wire for a
    // request this side has already refused.
    if (end && headers['content-length'] !== undefined && +headers['content-length'] > 0) {
      this.sendRst(sid, PROTOCOL_ERROR);
      // The frame that carried the refusal also carried the peer's END_STREAM, so the message is
      // over: the DATA that follows it (or a second HEADERS) is the error of 5.1 and not the
      // in-flight frame of a refused stream. Measured on node, probe case U2: GOAWAY
      // (STREAM_CLOSED). The refusals above never get here without having ended the message, and
      // the one that has not ended it (a malformed request without END_STREAM, probe case U17) is
      // the discard, which is what the absence of this record gives.
      if (this._ended.length < MAX_DROPPED_HISTORY) {
        this._ended.push(sid);
      }
      return;
    }
    const stream = new Http2Stream(this, sid);
    this._streams.set(sid, stream);
    this._liveStreams++;
    // content-length is a promise about the body, and the two checks that hold the peer to it
    // (see _onDataFrame and Http2Stream._remoteEnded) need it as a number. The inbound
    // validator has already proved the value is all digits, so this is a property read and a
    // coercion -- and -1, the "nothing was promised" marker, costs one comparison per DATA
    // frame for the requests that never sent the field at all.
    const promised = headers['content-length'];
    if (promised !== undefined) {
      stream._expLen = +promised;
    }
    // node reports END_STREAM while it parses the frame. That is safe here
    // because the readable events are deferred until a reader exists: the
    // handler attached inside the emit below still sees 'data' before 'end'.
    if (end) {
      stream._remoteEnded();
    }
    // node's argument order; the field list is the session's shared decode array
    // or a cached one, so the raw list handed out is always a copy
    this.emit('stream', stream, headers, end ? OPT_END_STREAM : OPT_NO_END_STREAM,
      this._wantRaw ? fields.slice(0) : null);
  }

  // node's toHeaderObject semantics: null prototype, :status coerced to a
  // number, set-cookie always an array, repeats joined.
  //
  // The map is a fast-properties object with a null prototype rather than a
  // { __proto__: null } literal: the literal is in V8's dictionary mode, and this
  // map is built once per request and read once per request by the application --
  // in the stream path and in the compat path alike (OPTIMIZATION.md item 4).
  _toObject(flat) {
    const o = nullProto();
    for (let i = 0; i < flat.length; i += 2) {
      const name = flat[i];
      let value = flat[i + 1];
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
      } else {
        o[name] = `${have}, ${value}`;
      }
    }
    return o;
  }

  // The checks and the accounting a DATA frame gets before its payload is handed
  // over. Split out of _onDataFrame because a payload that straddles two reads is
  // delivered in as many pieces as it has reads, and it is still ONE frame: the
  // window, the content-length promise and the body's length are decided once, for
  // the whole payload, before the first piece reaches the application.
  // Answers the stream to deliver to, or undefined when the payload is not
  // delivered at all -- a discarded frame, or a fatal error.
  _accountData(sid, l) {
    const stream = this._streams.get(sid);
    if (stream === undefined) {
      // A stream we refused or that a reset ended can still have data in flight from
      // the peer -- it sent before our RST arrived. Ignore those bytes, but give
      // the connection window credit for them, or enough refused uploads would
      // slowly deadlock the session.
      if (this._isGone(sid)) {
        this._connRecvUsed += l;
        return undefined;
      }
      // RFC 9113 5.1 and 8.2: the peer already closed the message with its END_STREAM, so a DATA
      // that follows it names a stream that has nothing left to receive. h2lean ends the connection
      // with STREAM_CLOSED -- the discretion 5.1 grants a closed stream ("MAY ... treat ... as a
      // connection error") and what h2spec's closed checks wait for; a silent discard is legal by
      // the letter but the conformance tool times out on it, and this is the just-closed id (it left
      // the table in this read), not the never-opened id of the skip branch below. node agrees here
      // (measured: GOAWAY(STREAM_CLOSED), "DATA: stream in half-closed(remote)").
      //
      // Note what the tool wants here and what the RFC says: h2spec 2.6.0 quotes RFC 7540's
      // "MUST treat that as a connection error of type STREAM_CLOSED", RFC 9113 5.1 turned that
      // into a "MAY ... as a connection error", and both stacks answer this shape the same way.
      // The arm that fails on both stacks is the other one -- the frame that arrives AFTER the
      // answer closed the stream, which is discarded below -- and it is not to be "fixed" by
      // ending the connection here (AGENTS.md 10).
      if (this._ended.includes(sid)) {
        this._fatal(STREAM_CLOSED, 'DATA after the peer ended the stream');
        return undefined;
      }
      // Neither refused, reset nor finished: an id this side has never been told about, which is
      // the 'idle' state of 5.1, and DATA is allowed in 'open' and 'half-closed (local)' only.
      // Measured on node (probe cases U5, U13, U14): GOAWAY(PROTOCOL_ERROR) -- including for the
      // connection-level sid 0 and for an even id, neither of which a client may ever open.
      if (sid === 0 || (sid & 1) === 0 || sid > this._maxInboundSid) {
        this._fatal(PROTOCOL_ERROR, 'DATA on a stream in the idle state');
        return undefined;
      }
      // An id at or below the highest one opened that this side never saw: a peer may skip ids,
      // and 5.1's discard is what node answers for it (probe cases U18, U20). It is also the
      // answer for a frame that arrives after this side's own answer closed the stream: RFC 9113
      // 5.1 allows a closed stream to be discarded ("An endpoint MUST minimally process and then
      // discard any frames it receives in this state"), node stays silent there (probe case U1
      // with the two frames in separate reads: ACK ANSWERED), and h2spec's http2/5.1 'closed'
      // arms fail on both stacks because of it. Silence is the RFC's answer, not a bug -- do not
      // end the connection here to satisfy the tool (AGENTS.md 10).
      this._connRecvUsed += l;
      return undefined;
    }
    if (stream.state.remoteClose === 1) {
      // The same rule while the stream is still in the table, which is how it reaches a server that
      // has not answered yet: measured on node (probe cases U10, U11) in both batching shapes, the
      // connection ends. RFC 9113 6.1 reads a DATA on a stream outside "open"/"half-closed(local)"
      // as a stream error of type STREAM_CLOSED, and h2spec's half-closed(remote) DATA and 6.1 checks
      // accept the reset, the GOAWAY, or a closed connection -- but node makes no present/closed
      // distinction and answers the connection error below on BOTH stacks, so h2lean keeps node's
      // verdict here rather than the stream error (which regressed the tool's DATA echo). Leaving it
      // is the honest call: it is node's deviation, the parity rule keeps it, and 5.1's closed-state
      // read is the discretionary one ("MAY ... treat ... as a connection error"). The arms that
      // fail on both stacks are the ones the tool writes for a frame that arrives after the answer
      // closed the stream -- the discard above -- not this one.
      this._fatal(STREAM_CLOSED, 'DATA after the peer ended the stream');
      return undefined;
    }
    if (l > this.state.localWindowSize) {
      this._fatal(FLOW_CONTROL_ERROR, 'connection flow control window exceeded');
      return undefined;
    }
    // RFC 9113 8.2 makes content-length a promise, and the body may not run past it. Measured
    // on node (h2spec http2/8.1.2.6): the frame that crosses the line ends the CONNECTION --
    // GOAWAY(INTERNAL_ERROR) -- rather than the stream, because nghttp2 reads an overflow as a
    // failure of the messaging layer, not of one request. The other direction, too few octets,
    // cannot be seen before the message ends: that one is caught in Http2Stream._remoteEnded()
    // and is a stream error. Both compare against the octets of this stream's body, padding
    // excluded, which is what `l` is here.
    if (stream._expLen >= 0 && stream._bodyLen + l > stream._expLen) {
      this._fatal(INTERNAL_ERROR, `body of ${stream._bodyLen + l} octets against ` +
        `content-length ${stream._expLen}`);
      return undefined;
    }
    stream._bodyLen += l;
    this.state.localWindowSize -= l;
    this.state.effectiveRecvDataLength += l;
    stream.state.localWindowSize -= l;
    return stream;
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
    const stream = this._accountData(sid, l);
    if (stream === undefined) {
      return;
    }
    // The stream decides when the credit is released: at once if the application
    // is reading, later if it is not. Either way the buffer it takes is one it may
    // keep: a view of the chunk node read off the socket, which node never writes
    // into again (tools/probe-read-allocator.js). A padded frame's payload is a
    // view too -- the pad length and the padding are taken off the views before
    // this point (frames.js trimPadding), so the copy is gone from this path as
    // well. Nothing here is recycled underneath the application, so the receive
    // path needs no copy at all.
    stream._recvData(buf.subarray(p, p + l));
    // 'end' must follow the last 'data'
    if (flags & F_END_STREAM) {
      stream._remoteEnded();
    }
  }

  // A payload that straddled two reads: one piece per read, each a view of the
  // chunk node read off the socket. No copy, and the pieces are the frame's bytes
  // in the order the peer sent them.
  _onDataPieces(sid, flags, pieces, len) {
    const stream = this._accountData(sid, len);
    if (stream === undefined) {
      return;
    }
    // The credit is released as the application takes each piece, exactly as it is
    // for a payload that arrived whole.
    for (let i = 0; i < pieces.length; i++) {
      // A piece the padding took all of is empty, and an empty chunk is not a
      // chunk: node emits no 'data' for one.
      if (pieces[i] !== null) {
        stream._recvData(pieces[i]);
      }
    }
    // 'end' must follow the last 'data'
    if (flags & F_END_STREAM) {
      stream._remoteEnded();
    }
  }

  _onWindowUpdate(sid, inc) {
    if (inc === 0) {
      this._fatal(PROTOCOL_ERROR, 'WINDOW_UPDATE with zero increment'); return;
    }
    if (sid === 0) {
      const win = this._connSendWin + inc;
      if (win > 0x7fffffff) {
        this._fatal(FLOW_CONTROL_ERROR, 'connection window overflow'); return;
      }
      this._connSendWin = win;
      this.state.remoteWindowSize = win;
      this._releaseAll();
      return;
    }
    const stream = this._streams.get(sid);
    if (stream === undefined) {
      // RFC 9113 5.1: an id above the highest one this side has ever opened names a stream in
      // the 'idle' state, and 6.9 lets WINDOW_UPDATE be sent only in 'open' and 'half-closed
      // (local)'. Measured on node (h2spec http2/5.1 and 6.9), WINDOW_UPDATE for an idle stream
      // is GOAWAY(PROTOCOL_ERROR). For an id at or below the highest one opened the stream has
      // been answered, refused or reset -- a credit note that arrived late -- and node drops it,
      // so this does too rather than turning the peer's own in-flight frame into an error.
      if (sid > this._maxInboundSid) {
        this._fatal(PROTOCOL_ERROR, 'WINDOW_UPDATE on an idle stream');
      }
      return;
    }
    const next = stream._sendWin + inc;
    if (next > 0x7fffffff) {
      this._streamError(stream, FLOW_CONTROL_ERROR, 'stream window overflow');
      return;
    }
    stream._sendWin = next;
    if (stream._queued !== null) {
      this._release(stream);
    }
    if (this._outLen >= 32 * 1024) {
      this._flush();
    }
  }

  // What this connection currently costs in answers it cannot send. Two terms because the flood
  // that needs bounding is many small stalled responses, and charging only their payloads would let
  // 8 MiB of bound stand for 350 000 live stream records -- see MAX_SESSION_HOLD for the measurement.
  _heldCost() {
    return this._heldBytes
      + (this._blocked === null ? 0 : this._blocked.size * STREAM_HOLD_CHARGE);
  }

  // Only streams that actually stalled are worth revisiting. Scanning every
  // stream on each connection-level WINDOW_UPDATE looks harmless at low load
  // and is quadratic under a large response body (OPTIMIZATION.md item 19).
  _releaseAll() {
    const blocked = this._blocked;
    if (blocked === null || blocked.size === 0) {
      return;
    }
    for (const s of blocked) {
      if (s._queued !== null) {
        this._release(s);
      }
    }
  }

  _release(stream) {
    const q = stream._queued;
    const max = this.remoteSettings.maxFrameSize;
    while (q.length > 0) {
      const buf = q[0];
      let sent = 0;
      while (sent < buf.length) {
        const room = Math.min(this._connSendWin, stream._sendWin, max,
          MAX_OUT_BATCH - this._outLen - HEADER_LEN);
        if (room <= 0) {
          // keep the unsent remainder where the next release can find it: that is a
          // WINDOW_UPDATE for a credit stall and a write callback for a full queue
          if (sent > 0) {
            q[0] = buf.subarray(sent);
          }
          return;
        }
        const take = Math.min(room, buf.length - sent);
        const last = q.length === 1 && sent + take === buf.length && stream._endQueued;
        this._frame(DATA, last ? F_END_STREAM : 0, stream.id, buf.subarray(sent, sent + take),
          true);
        this._connSendWin -= take;
        stream._sendWin -= take;
        // the byte is no longer held once the frame is on its way to the socket
        this._heldBytes -= take;
        sent += take;
      }
      q.shift();
    }
    stream._queued = null;
    this._blocked.delete(stream);
    this.state.outboundQueueSize = this._blocked.size;
    // A trailer block stashed by sendTrailers() belongs after the body it follows, and it
    // carries the END_STREAM the body was not allowed to send (RFC 9113 6.1: a trailing
    // HEADERS precedes the close -- measured against node: HEADERS/EH DATA HEADERS/ES/EH).
    if (stream._pendingTrailers !== null) {
      const trailers = stream._pendingTrailers;
      stream._pendingTrailers = null;
      this._frame(HEADERS, F_END_HEADERS | F_END_STREAM, stream.id, trailers, true);
      stream._endLocalSide();
    }
    // the writable side accepted bytes again: res.on('drain') is how an
    // application is allowed to resume writing after write() returned false
    process.nextTick(emitNT, stream, 'drain');
    if (stream._endQueued) {
      stream._endLocalSide();
    }
    if (this._outLen >= 32 * 1024) {
      this._flush();
    }
  }

  _onRst(sid, code) {
    // The budget is charged per frame received, before anything else and whether or not
    // the stream is still known: what is being limited is the rate at which a peer can
    // make us do this work, not the work of one particular stream.
    //
    // The clock is sampled only when the bucket has run empty. Deferring it is exactly
    // equivalent to refilling every second: between two samples the balance only
    // decreases, and min(burst, v + g1 + g2) == min(burst, min(burst, v + g1) + g2) for
    // non-negative gains, so one catch-up grant gives the same balance as stepwise ones
    // (OPTIMIZATION.md item 20 for the measured share of Date.now()).
    if (this._rstTokens === 0) {
      const d = ((Date.now() / 1000) | 0) - this._rstSec;
      if (d > 0) {
        this._rstSec += d; // never behind the sample we just took
        const gained = RST_RATE * d;
        this._rstTokens = gained > RST_BURST ? RST_BURST : gained;
      }
      if (this._rstTokens === 0) {
        this._fatal(INTERNAL_ERROR, 'RST_STREAM: too many resets in a short period');
        return;
      }
    }
    this._rstTokens--;

    if (sid === 0) {
      // RFC 9113 6.4: RST_STREAM always applies to a stream, so 0 is not a value it may carry.
      // Measured on node (h2spec http2/6.4): GOAWAY(PROTOCOL_ERROR), not a reset of nothing.
      this._fatal(PROTOCOL_ERROR, 'RST_STREAM with stream id 0');
      return;
    }
    const stream = this._streams.get(sid);
    if (stream === undefined) {
      // As for WINDOW_UPDATE: an id this side never opened names an 'idle' stream, and 6.4
      // allows RST_STREAM in 'open' and 'half-closed (local)' only. Measured on node (h2spec
      // http2/5.1 and 6.4): GOAWAY(PROTOCOL_ERROR). A reset for a stream that has already been
      // answered or refused is the peer's own late frame and is dropped.
      if (sid > this._maxInboundSid) {
        this._fatal(PROTOCOL_ERROR, 'RST_STREAM on an idle stream');
        return;
      }
      // Below the line the reset lands, even if there is nothing left to reset: it is the peer
      // closing the stream, and whatever it had already put on the wire for that id is then the
      // discard of 5.1 rather than the error of a frame after the peer's END_STREAM (U12, U19).
      this._rememberKilled(sid);
      return;
    }
    // The peer's RST_STREAM closes both halves at once: same teardown as our own
    // reset. Whether the application hears 'aborted' is decided there, by
    // whether the response was still open (lib/stream.js, _destroy).
    stream._destroy(code, undefined, false);
  }

  get goawayCode() {
    return this._goawayCode === null ? NO_ERROR : this._goawayCode;
  }

  get goawayLastStreamID() {
    return this._goawayLastStreamID || 0;
  }

  _onGoaway(buf, start, len) {
    // node begins this handler with `if (session.destroyed) return;`, because its nghttp2 handle
    // can report a frame after the session was destroyed. h2lean cannot: _dispatch() answers with
    // `this._open` and the read batch stops at the frame that destroyed the session, so a
    // destroyed session has no frames left to reach. That stop is what is tested -- in
    // test/goaway.test.js, 'the read batch stops at the frame that destroyed the session' -- and
    // a second check down here would be a line no test can ever make fail.
    if (len < 8) {
      this._fatal(FRAME_SIZE_ERROR, 'GOAWAY payload too short'); return;
    }
    this._goawayLastStreamID = buf.readUInt32BE(start);
    this._goawayCode = buf.readUInt32BE(start + 4);
    // the debug data goes to the application, which may keep it: the same reason as
    // the PING payload above. allocUnsafe + copy instead of Buffer.from(subarray) for
    // the pooled allocation (lib/headers.js, block cache). Node's copy comes out of
    // the C++ layer as a fresh buffer, so it does not move under the application there.
    let debug;
    if (len > 8) {
      debug = Buffer.allocUnsafe(len - 8);
      buf.copy(debug, 0, start + 8, start + len);
    }
    this.emit('goaway', this._goawayCode, this._goawayLastStreamID, debug);
    // node's rule on receipt (onGoawayData in internal/http2/core): a GOAWAY with NO_ERROR
    // starts a graceful shutdown of our own -- no new streams, existing ones finish naturally,
    // and we answer with our GOAWAY -- while any other code destroys the session at once. The
    // destroy is asked for with NO_ERROR even then, because the code that arrived describes the
    // PEER's reason, not a failure of this side; `goawayCode` keeps what was received.
    //
    // Streams above the peer's lastStreamID are deliberately NOT torn down here. That is what a
    // GOAWAY means -- the sender will not process anything newer -- and the streams it names are
    // the ones the SENDER opened; a server told to stop at stream 0 still owes an answer for the
    // request already in front of it. We destroyed them with PROTOCOL_ERROR, which closed a live
    // request the moment the peer said goodbye: measured against node on that shape (a request
    // that is not answered, then GOAWAY(NO_ERROR, lastStreamID 0)), node reports
    // closed && !destroyed with the stream still open, us a finished teardown.
    if (this._goawayCode === NO_ERROR) {
      this.close(); return;
    }
    // Any other code: node destroys the session at once, telling the application with
    // ERR_HTTP2_SESSION_ERROR -- carrying the code that arrived, in node's own words -- and
    // putting GOAWAY(NO_ERROR) on the wire, because there was no failure on this side.
    this.destroy(errorOf('ERR_HTTP2_SESSION_ERROR',
      `Session closed with error code ${this._goawayCode}`), NO_ERROR);
  }

  // ---- flow control credit, released on consumption -------------------------
  // RFC 9113 5.2.2 and 6.9: flow control exists so that a receiver unable to process
  // one stream's data can still serve the rest of the connection, and WINDOW_UPDATE is
  // the only way to give that credit back. So credit returns when the application has
  // actually taken the bytes, not when they land. That is what backpressure means in
  // HTTP/2: a body nobody reads costs the sender, not our heap. The connection
  // window is accounted separately because it must be restored even for a stream
  // that is destroyed before anyone reads it -- see _abandon().
  _consume(stream, n) {
    stream._recvUsed -= n; // connection share, released below
    this._connRecvUsed += n;
    // The peer's stream window only grows when we say so, and saying so once per
    // quarter window keeps the frame count down instead of one update per chunk.
    stream._unannounced += n;
    if (stream._unannounced << 2 >= this.localSettings.initialWindowSize) {
      const inc = stream._unannounced;
      stream._unannounced = 0;
      stream.state.localWindowSize += inc;
      this._windowUpdate(stream.id, inc);
    }
    // A reader that shows up between batches (a timer, an await) has to be able
    // to unblock the sender on its own: _endBatch() is not coming this turn.
    if (!this._inBatch) {
      this._creditConnection();
    }
  }

  // The connection-level half of the credit, coalesced into one frame.
  _creditConnection() {
    // Not every consumed octet is worth a frame. WINDOW_UPDATE is a signal about space, not an
    // accounting entry, and node's nghttp2 likewise lets the connection window run down before it
    // releases anything: measured on h2spec's http2/6.1 sequence (four octets discarded on a stream
    // that had already closed), our flush-per-read-batch put a WINDOW_UPDATE(sid 0) on the wire in
    // front of the frame the tool was waiting for, where node sent nothing at all. Measured on a
    // 640 KiB upload (tools/probe-conn-credit.js): node releases the connection window in steps of
    // 32768 octets -- half the 65535 it advertises -- and the same threshold here takes h2lean's
    // cadence from one frame per read batch to the same half-window steps, at the same throughput.
    // A peer that keeps sending is therefore credited before its window can run out, so the
    // anti-deadlock rule that this counter exists for still holds; below the threshold the bytes
    // stay owed, and the next batch pays them.
    if (this._connRecvUsed < this.localSettings.initialWindowSize >> 1) {
      return;
    }
    const inc = this._connRecvUsed;
    this._connRecvUsed = 0;
    this.state.localWindowSize += inc;
    this._windowUpdate(0, inc);
  }

  // Bytes that will never be read: the stream's own window no longer matters,
  // but the connection's share has to come back or the session deadlocks.
  _abandon(stream) {
    if (stream._recvUsed > 0) {
      this._connRecvUsed += stream._recvUsed;
      stream._recvUsed = 0;
      if (!this._inBatch) {
        this._creditConnection();
      }
    }
  }

  _endBatch() {
    this._creditConnection();
    this._flush();
    if (this._closing && this._liveStreams === 0) {
      this.socket.end();
    }
  }

  // ---- errors and lifecycle ---------------------------------------------------
  _onFrameError(err, sid) {
    if (err instanceof HPackError) {
      this._fatal(COMPRESSION_ERROR, err.message);
      return false;
    }
    const code = typeof err.code === 'string' ? err.code : '';
    if (code.startsWith('ERR_HTTP2') || code === 'ERR_INVALID_HTTP_TOKEN') {
      // the peer handed us something we refuse to interpret
      if (sid !== 0 && this._streams.has(sid)) {
        this._streamError(this._streams.get(sid), PROTOCOL_ERROR, err.message);
      } else {
        this._fatal(PROTOCOL_ERROR, err.message);
      }
      return false;
    }
    this._fatal(INTERNAL_ERROR, err.message);
    return false;
  }

  _streamError(stream, code, msg) {
    this.sendRst(stream.id, code);
    this._flush();
    stream._destroy(code, msg, false);
    if (this.listenerCount('frameError') > 0) {
      this.emit('frameError', DATA, stream.id, code);
    }
    process.nextTick(emitNT, stream, 'error', errorOf('ERR_HTTP2_STREAM_ERROR', msg));
  }

  _fatal(code, msg) {
    if (!this._open) {
      return;
    }
    this.sendGoaway(this._maxInboundSid, code, msg);
    this._flush();
    const err = errorOf('ERR_HTTP2_SESSION_ERROR', msg);
    err.http2Code = code;
    this.destroy(code);
    if (this.listenerCount('error') > 0) {
      this.emit('error', err);
    }
  }

  // Takes the stream object, not the id: the caller always holds it, and
  // Map.delete already reports whether the entry was there. `killed` says which of the two ways a
  // stream leaves the table it is: a reset (ours or the peer's) or the end of the message.
  _dropStream(stream, killed) {
    if (!this._streams.delete(stream.id)) {
      return;
    }
    this._liveStreams--;
    // inbound bytes the application never got still owe the connection window
    this._abandon(stream);
    if (killed) {
      // remember the id: DATA the peer already put on the wire will turn up next
      this._rememberKilled(stream.id);
    } else {
      // The other memory, and the shorter one by design: reaching here means both halves closed on
      // their own terms, so the remote half was closed by the peer's END_STREAM (it is the only
      // thing that sets remoteClose besides a reset, and a reset takes the path above). A frame for
      // that id is then the error of RFC 9113 5.1 -- until this read batch is over.
      if (this._ended.length < MAX_DROPPED_HISTORY) {
        this._ended.push(stream.id);
      }
    }
    // a stalled stream that gets reset must not be kept alive by the blocked set
    if (this._blocked !== null) {
      this._blocked.delete(stream);
    }
    // and the bytes it was holding leave the charge with it, or the session would go on paying for
    // a queue that no longer exists and refuse requests it could now serve
    if (stream._queued !== null) {
      for (const b of stream._queued) {
        this._heldBytes -= b.length;
      }
      stream._queued = null;
    }
    // a closing session whose last stream has gone has nothing left to carry
    if (this._closing) {
      this._maybeFinishClose();
    }
  }

  // Recently finished stream ids: a peer that is mid-upload keeps sending DATA
  // for them, and those frames are ours to ignore, not to treat as an error.
  _isGone(sid) {
    return this._refused.includes(sid) ||
           (this._dropped !== null && this._dropped.includes(sid));
  }

  // An id that a reset ended. Reached from _dropStream for a stream that was still in the table,
  // and from _onRst for one that was not: a reset that arrives for a stream this side has already
  // finished is the peer telling it to stop, and the frames it had put on the wire before that are
  // then the discard of RFC 9113 5.4 rather than the "after the peer's own END_STREAM" error.
  // Measured: node answers nothing at all for HEADERS|END_STREAM, RST_STREAM, DATA (probe U12).
  _rememberKilled(sid) {
    if (this._dropped === null) {
      this._dropped = [];
    }
    this._dropped.push(sid);
    if (this._dropped.length > MAX_DROPPED_HISTORY) {
      this._dropped.shift();
    }
  }

  close() {
    // node bails on both flags: a destroyed session cannot be closed, and close() is not a way
    // to undo a destroy.
    if (this.closed || this.destroyed) {
      return this;
    }
    this.closed = true;
    this._closing = true;
    this.sendGoaway(this._maxInboundSid, NO_ERROR);
    this._flush();
    this._maybeFinishClose();
    return this;
  }

  // node's [kMaybeDestroy](), plus the graceful close that nghttp2 completes for it: once the
  // session has nothing left to carry the connection is let go, and a session that still has
  // streams or bytes on their way is not. It is looked at when close() is asked for and when the
  // last stream goes, because close() typically hands its GOAWAY over and finds `_frozenLen`
  // covering it -- and a session that is closed with a live stream has to be finished by the
  // stream's own teardown rather than left sitting in server.sessions with the listening server
  // waiting on it.
  _maybeFinishClose() {
    if (!this.closed || this.destroyed) {
      return;
    }
    if (this._liveStreams > 0 || this._outLen > 0 || this._frozenLen > 0) {
      return;
    }
    this.socket.end();
    this.destroy(NO_ERROR);
  }

  // node's signature is destroy(error = NGHTTP2_NO_ERROR, code): the first argument may be the
  // Error the application is handed, in which case the second is the code that goes on the wire.
  // That is exactly what a peer's GOAWAY does -- destroy(new ERR_HTTP2_SESSION_ERROR(code),
  // NO_ERROR) -- and the measurement below is what made the distinction visible: the application
  // is told WHY (the code that arrived) while the peer is told goodbye with NO_ERROR, because
  // this side did not fail. A number as the first argument is h2lean's own convenience, used by
  // the socket handlers and _fatal(): destroy with this GOAWAY code and nothing to report.
  destroy(error = NO_ERROR, code) {
    if (this.destroyed) {
      return this;
    }
    let appError;
    if (typeof error === 'number') {
      code = error;
    } else {
      appError = error;
      if (code === undefined) {
        code = INTERNAL_ERROR;
      }
    }
    this.destroyed = true;
    // node hands the code to nghttp2 when the session is destroyed, and the peer receives a
    // GOAWAY carrying it -- measured against node 26: destroying an open session leaves the
    // peer with GOAWAY(NO_ERROR) on the wire and no local 'goaway' event on the session. The
    // frame has to be handed over before _open goes false, because _writePending() drops
    // whatever is queued once the session is not open. It is written even when a GOAWAY has
    // already arrived from the peer -- node answers one goodbye with one of its own -- and only
    // skipped when this side has already sent one (_fatal, close).
    if (this._open && !this._goawaySent) {
      this.sendGoaway(this._maxInboundSid, code);
      this._flush();
    }
    this._open = false;
    // `closed` is NOT touched here: node's closeSession() only sets the destroyed flag, so
    // `session.closed` keeps saying whether close() was asked for. Measured on a session
    // destroyed after a peer GOAWAY with a code: node reports closed:false destroyed:true.
    for (const s of [...this._streams.values()]) {
      s._destroy(code, undefined, false, appError);
    }
    this._streams.clear();
    this._liveStreams = 0;
    if (this._idleTimer !== null) {
      idleClearTimeout(this._idleTimer);
      this._idleTimer = null;
    }
    this._flush();
    // node's teardown shape (internal/http2/core.js, finishSessionClose): the socket is
    // ended first and the destroy waits for the write to be flushed, so the GOAWAY queued
    // above reaches the peer and the connection ends with a FIN instead of a reset.
    // h2lean used to destroy the socket outright, which threw that GOAWAY away and closed
    // the connection with a reset whenever the write had not reached the OS yet -- under
    // load that is what h2spec's http2/7 reads as a failure ("connection reset by peer"
    // where node's "connection closed" passes), measured in bench/micro/probe-goaway-ping.js.
    const socket = this.socket;
    if (socket.destroyed) {
      socket.destroy();
    } else {
      socket.end(() => {
        setImmediate(() => socket.destroy());
      });
      // The bound: a peer that stopped reading holds the flush (and the FIN behind it)
      // forever, and the socket would stay on the heap with it. The timer is unref'd --
      // it must not keep the process alive -- and cleared as soon as the socket is gone.
      const grace = setTimeout(() => socket.destroy(), SOCKET_CLOSE_GRACE);
      if (typeof grace.unref === 'function') {
        grace.unref();
      }
      socket.once('close', () => clearTimeout(grace));
    }
    // node's closeSession() hands the streams their error first and the session gets it in
    // finishSessionClose() afterwards, which is the order the application sees: stream 'error',
    // stream 'close', session 'error', session 'close'.
    if (appError !== undefined) {
      process.nextTick(emitNT, this, 'error', appError);
    }
    return this;
  }

  // The session's 'close', exactly once. node emits it when the SOCKET closes
  // (internal/http2/core.js, finishSessionClose: socket.on('close', () =>
  // emitClose(session, error))), and the server's own 'close' is emitted before it because
  // net.Server's handler was registered on the socket before this one.
  //
  // Moving it here is not optional. The socket half of node's teardown shape on its own (the
  // end-then-destroy in destroy()) left the session's 'close' coming out of destroy() on a
  // nextTick, where it OVERTOOK the server's own 'close' -- nine event-order traces went red
  // (simple, post, reset, clientReset, goaway, trailers, eof, eof-wait, eof-body) and all
  // twelve agree with node again only once the 'close' moved with it.
  _emitSessionClose() {
    if (this._closeEmitted) {
      return;
    }
    this._closeEmitted = true;
    process.nextTick(emitNT, this, 'close');
  }

  _onSocketError(err) {
    // node ignores an ECONNRESET that arrives after a GOAWAY was received: "there's nothing we can
    // do and the other side is fully within its rights to do so" (socketOnError in
    // internal/http2/core). Telling the application about it turned a peer's ordinary hang-up
    // into a session error that node reports nothing for -- measured on the 'destroy' scenario,
    // where the peer destroys itself and the reset lands right behind its GOAWAY.
    if (err !== undefined && err.code === 'ECONNRESET' && this._goawayCode !== null) {
      this.destroy(NO_ERROR);
      return;
    }
    if (this.listenerCount('socketError') > 0) {
      this.emit('socketError', err);
    } else if (this.listenerCount('error') > 0) {
      this.emit('error', err);
    }
    this.destroy(NO_ERROR);
  }

  _onSocketClose() {
    this._open = false;
    if (!this.destroyed) {
      this.destroy(NO_ERROR);
    }
    this._emitSessionClose();
  }

  // Idle timeout, node's setTimeout semantics: it counts silence, so every read
  // batch refreshes the handle instead of allocating a new one, and calling
  // setTimeout twice replaces the previous timer rather than leaving it armed.
  //
  // Delays above 1 s are armed on the shared clock in lib/fast-timer.js: one 499 ms
  // libuv handle for the whole process instead of one per connection, and a
  // refresh() that is an array write rather than a uv_timer_reset syscall. The
  // cost is accuracy -- such a timeout fires within +/- 500 ms of the deadline.
  // Shorter delays keep a native handle, so a 600 ms timeout stays precise.
  setTimeout(ms, cb) {
    if (this._idleTimer !== null) {
      idleClearTimeout(this._idleTimer);
      this._idleTimer = null;
    }
    this._idleCb = cb === undefined ? null : cb;
    if (ms === 0) {
      return this;
    }
    // the session is passed as the timer argument, which both the native and the
    // fast path forward to the callback: nothing is allocated per connection here
    const timer = idleSetTimeout(onIdleTimeout, ms, this);
    if (timer.unref) {
      timer.unref();
    } // the fast clock unrefs itself
    this._idleTimer = timer;
    return this;
  }

  _touchIdleTimer() {
    if (this._idleTimer !== null) {
      this._idleTimer.refresh();
    }
  }

  setLocalWindowSize(size) {
    this.state.localWindowSize = size;
    this.state.effectiveLocalWindowSize = size;
    return this;
  }

  setNextStreamID(id) {
    this.state.nextStreamID = id; return this;
  }

  // Advertise new settings to the peer. node's session.settings() returns nothing -- measured
  // on a live session of node 26, `session.settings()` answers undefined, so this does not hand
  // out the session for chaining either; a caller that wants the current values has
  // session.localSettings, which both stacks expose.
  settings(settings, cb) {
    if (settings) {
      Object.assign(this.localSettings, settings);
      // a value the application sets part-way through the connection is a promise it made, so it
      // joins the advertisement; the values it never touched stay private to this session
      for (const k of Object.keys(settings)) {
        if (ID_BY_SETTING[k] !== undefined) {
          this._advertised.add(k);
        }
      }
    }
    this._sendSettings();
    if (cb) {
      process.nextTick(cb);
    }
  }

  ping(opaque, cb) {
    const body = Buffer.alloc(8);
    if (opaque) {
      Buffer.from(opaque).copy(body);
    }
    this._frame(PING, 0, 0, body, true);
    this._flush();
    if (cb) {
      this.once('ping', () => cb());
    }
    return this;
  }

  goaway(code = NO_ERROR, lastStreamID = this._maxInboundSid, reason) {
    this.sendGoaway(lastStreamID, code, reason);
    this._flush();
    return this;
  }

  ref() {
    this.socket.ref(); return this;
  }

  unref() {
    this.socket.unref(); return this;
  }
}

module.exports = { Http2Session, OUR_DEFAULTS, PEER_DEFAULTS, SETTINGS_BY_ID,
  ID_BY_SETTING, MAX_OUT_BATCH, MAX_SESSION_HOLD, STREAM_HOLD_CHARGE, BODY_MIN,
  STAGE_MIN, MAX_SHAPES, hashBlock, hashPieces };
