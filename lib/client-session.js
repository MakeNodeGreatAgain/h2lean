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
'use strict';

const { EventEmitter } = require('node:events');
const tls = require('node:tls');
const f = require('./frames');
const { Decoder, Encoder, HPackError } = require('./hpack');
const { nullProto, isFieldName, lowerName, CONNECTION_SPECIFIC } = require('./headers');
const { ClientHttp2Stream } = require('./client-stream');
const { pieceByteAt, pieceSlice, pieceConcat, pieceEquals } = require('./pieces');
// The settings tables, defaults and buffer-path constants are the server's, reused
// verbatim so a client and a server in this one library never disagree about what a
// SETTINGS id means, what node advertises, or where the copy/reference break-even sits.
const {
  OUR_DEFAULTS, PEER_DEFAULTS, SETTINGS_BY_ID, ID_BY_SETTING, BODY_MIN, MAX_OUT_BATCH,
  STAGE_MIN, MAX_SHAPES, hashBlock, hashPieces
} = require('./session');

const {
  DATA, HEADERS, PRIORITY, RST_STREAM, SETTINGS, PUSH_PROMISE, PING, GOAWAY,
  WINDOW_UPDATE, CONTINUATION,
  F_END_STREAM, F_ACK, F_END_HEADERS, F_PADDED, F_PRIORITY,
  NO_ERROR, PROTOCOL_ERROR, FRAME_SIZE_ERROR, COMPRESSION_ERROR, CANCEL,
  HEADER_LEN, PREFACE, PREFACE_LEN, readHeader, writeHeader, trimPadding
} = f;

const MAX_SETTINGS_ENTRIES = 32; // CVE-2020-11080; same bound as the server
const MAX_CONTINUATIONS = 8; // CVE-2024-28182; same bound as the server
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

class ClientHttp2Session extends EventEmitter {
  constructor(socket, options, scheme, authority) {
    super();
    const opts = options || {};
    this.socket = socket;
    this.type = 1; // NGHTTP2_SESSION_CLIENT
    this.closed = false;
    this.destroyed = false;
    this.connecting = true;
    // encrypted/alpnProtocol are derived from the socket once it is connected
    // (_start), the way node's setupHandle fills them after 'secureConnect':
    // before that node's getters read undefined.
    this.pendingSettingsAck = true;
    this.originSet = [];
    this.localSettings = { ...OUR_DEFAULTS, ...opts.settings };
    this.remoteSettings = { ...PEER_DEFAULTS };
    this._scheme = scheme || 'http';
    this._authority = authority || 'localhost';

    // Only what the application asked for is advertised (see session.js _advertised:
    // a number we advertise is a number a peer plans around). Empty by default, which
    // is node's usual client SETTINGS frame too.
    this._advertised = new Set((opts.advertised !== undefined
      ? opts.advertised : Object.keys(opts.settings || {}))
      .filter((k) => ID_BY_SETTING[k] !== undefined));

    this._nextSid = 1; // client owns odd ids
    this._streams = new Map();
    this._outboundQueue = []; // requests waiting for a concurrency slot
    this._decoder = new Decoder(this.localSettings.headerTableSize);
    this._encoder = new Encoder(this.remoteSettings.headerTableSize,
      this.localSettings.headerTableSize);
    this._blocks = new WeakMap();
    this._flat = [];

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
    this._hdrCont = 0;
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
    this._goawayCode = null;
    this._goawayLastStreamID = null;
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
    if (this.socket.encrypted) {
      this.encrypted = true;
      this.alpnProtocol = this.socket.alpnProtocol;
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
    if (this._writing || !this.socket.writable) {
      return;
    }
    this._writing = true;
    this._commit();
    const batch = this._outQ;
    this._outQ = [];
    this._outLen = 0;
    const s = this.socket;
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
  }

  // request(): the node-shaped surface. Accepts an object, an array of pairs, or
  // nothing; fills the request pseudo-headers from the session's scheme/authority
  // unless the application supplied them; returns a ClientHttp2Stream.
  request(headersParam, options) {
    if (this.destroyed) {
      throw httpErr('ERR_HTTP2_INVALID_SESSION', 'The session has been destroyed');
    }
    if (this.closed) {
      throw httpErr('ERR_HTTP2_GOAWAY_SESSION', 'New streams cannot be created after receiving a GOAWAY');
    }
    const options2 = options ? { ...options } : {};
    const list = [];
    let method = 'GET';
    let sawMethod = false;

    const pushPseudo = (name, value) => {
      list.push(name, value);
    };
    const src = headersParam === undefined ? {} : headersParam;
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
      pushPseudo(':method', method);
    }

    let endStream = options2.endStream;
    if (endStream === undefined) {
      endStream = payloadMeaningless(method);
    }

    const stream = new ClientHttp2Stream(this, this._nextSid, {});
    this._nextSid += 2; // the id is reserved now, HEADERS may wait
    stream.endAfterHeaders = endStream;
    if (endStream) {
      stream.end();
    }

    // hp: the original headersParam object. When the application reuses the same
    // object across requests, the HPACK encoder produces identical bytes every time
    // after the dynamic table stabilises, and _sendRequest can use _blocks to replay
    // the encoded block without re-encoding. (The server's shareHeaders equivalent.)
    const req = { stream, list, end: endStream, hp: headersParam };
    if (this._started) {
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
  _addField(list, rawKey, rawValue) {
    if (rawValue === undefined) {
      return;
    }
    const key = lowerName(rawKey);
    // The colon test is spelled as a code-point read, where lib/headers.js uses
    // `key[0] === ':'`. Not a style difference: the one-character read boxes a new
    // one-char string, and charCodeAt is the cheaper spelling on every input --
    // the fall-through case and the match case alike (OPTIMIZATION.md item 33).
    if (key.charCodeAt(0) === 58 /* : */) {
      if (key !== ':method' && key !== ':path' && key !== ':scheme' && key !== ':authority'
        && key !== ':protocol') {
        throw httpErr('ERR_HTTP2_PSEUDOHEADER_NOT_ALLOWED', `cannot send ${key}`);
      }
      const single = Array.isArray(rawValue) ? rawValue[0] : rawValue;
      list.push(key, String(single));
      return;
    }
    if (!isFieldName(key)) {
      throw httpErr('ERR_INVALID_HTTP_TOKEN', `${key} is an invalid header token`);
    }
    if (CONNECTION_SPECIFIC.has(key)) {
      throw httpErr('ERR_HTTP2_INVALID_CONNECTION_HEADERS',
        `HTTP/1 Connection specific header is forbidden: ${key}`);
    }
    if (Array.isArray(rawValue)) {
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
    if (cached !== undefined && cached.epoch === this._encoder.epoch) {
      block = cached.block;
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
      if (hp !== undefined && hp !== null && typeof hp === 'object') {
        this._blocks.set(hp, { epoch: this._encoder.epoch, block });
      }
    }
    stream._sid = stream.id;
    this._sendHeaders(stream.id, block, end);
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
    for (let i = 0; i < list.length; i += 2) {
      cap += Encoder.maxBytesOf(list[i], list[i + 1]);
    }
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

  _sendHeaders(sid, block, end) {
    const max = this.remoteSettings.maxFrameSize;
    if (block.length <= max) {
      // owned=false: the batch keeps a copy, because `block` may be the encoder
      // scratch; one memcpy, and the double allocation the old by-reference path cost
      // is gone.
      this._frame(HEADERS, F_END_HEADERS | (end ? F_END_STREAM : 0), sid, block, false);
      return;
    }
    this._frame(HEADERS, 0, sid, block.subarray(0, max), false);
    let off = max;
    for (;;) {
      const n = Math.min(block.length - off, max);
      const last = off + n >= block.length;
      this._frame(CONTINUATION, last ? F_END_HEADERS | (end ? F_END_STREAM : 0) : 0,
        sid, block.subarray(off, off + n), false);
      off += n;
      if (last) {
        return;
      }
    }
  }

  // Flow-controlled send for the writable half of a stream. Called from the Duplex's
  // _write/_final and again from WINDOW_UPDATE / SETTINGS to resume a stalled body.
  _pump(stream) {
    // No bytes may leave before the stream's HEADERS: DATA on an unopened stream id
    // is a connection error, and nghttp2 answers it with GOAWAY(INTERNAL_ERROR). A
    // write() that arrives before 'connect' parks its payload in _pending and waits
    // for _sendRequest to flip _headersSent and call us again.
    if (!stream._headersSent) {
      return;
    }
    const max = this.remoteSettings.maxFrameSize;
    while (stream._pending !== null) {
      const room = Math.min(this._connSendWin, stream._sendWin, max);
      if (room <= 0) {
        return;
      } // stalled; WINDOW_UPDATE resumes
      const avail = stream._pending.length - stream._pendingOff;
      const take = Math.min(room, avail);
      const finishedByte = (stream._pendingOff + take) === stream._pending.length;
      const es = finishedByte && stream._writeCb === null && stream._finalCb !== null;
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
      this._frame(DATA, F_END_STREAM, stream.id, null, false);
      stream._writableDone();
    }
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
        && this.socket.writable) {
        // a copy, not a view -- the reason is the comment above -- taken through
        // allocUnsafe rather than Buffer.from(subarray): the pooled allocation is the
        // cheaper way to get a fresh buffer (OPTIMIZATION.md item 18)
        const out = Buffer.allocUnsafe(this._wuLen);
        this._wuBuf.copy(out, 0, 0, this._wuLen);
        this.socket.write(out);
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
          this.pendingSettingsAck = false;
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
        // A server pushing to us is legal HTTP/2, but push is out of v1 scope (as it
        // is for the server side); RFC 9113 6.6 makes an unexpected PUSH_PROMISE a
        // connection error of type PROTOCOL_ERROR, which is what node answers too.
        return this._fatal(PROTOCOL_ERROR, 'peer sent PUSH_PROMISE');
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
      this._processHeaders(sid, (flags & F_END_STREAM) !== 0, buf.subarray(p, p + l));
      return;
    }
    this._hdrSid = sid;
    this._hdrEnd = (flags & F_END_STREAM) !== 0;
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
    const sid = this._hdrSid, end = this._hdrEnd;
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
  _processHeaders(sid, end, block) {
    this._processBlock(sid, end, block, null, 0, 0);
  }

  _processBlockPieces(sid, end, pieces, from, to) {
    this._processBlock(sid, end, null, pieces, from, to);
  }

  _processBlock(sid, end, block, pieces, from, to) {
    const stream = this._streams.get(sid);
    const flat = this._flat;
    // The cache is disabled (null) when stream === undefined (we must decode for
    // HPACK state but there is nothing to cache it for) and when the application
    // opted out via shapeCache: false.
    const shapes = stream === undefined ? null : this._shapes;
    const slot = shapes === null ? 0
      : (block !== null ? this._shapeSlot(block) : this._shapeSlotPieces(pieces, from, to));
    const hit = shapes === null ? undefined : shapes.get(slot);
    let fields = flat;
    try {
      if (hit !== undefined && (block !== null
        ? hit.block.equals(block)
        : pieceEquals(hit.block, pieces, from, to))) {
        this._shapeHits++;
        fields = hit.flat;
      } else {
        if (block !== null) {
          this._decoder.decode(block, 0, block.length, flat);
        } else {
          this._decoder.decodePieces(pieces, from, to, flat);
        }
        if (shapes !== null) {
          if (++this._shapeMisses > 64 && this._shapeHits * 4 < this._shapeMisses) {
            this._shapes = null;
          } else {
            if (shapes.size >= MAX_SHAPES) {
              shapes.clear();
            }
            shapes.set(slot, {
              block: block !== null ? Buffer.from(block) : pieceConcat(pieces, from, to),
              flat: flat.slice(0)
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
      return this._fatal(PROTOCOL_ERROR, 'response HEADERS on an unopened stream id');
    }
    if (this._refuseHeaders(sid, stream, fields)) {
      return;
    }
    const headers = this._toObject(fields);
    stream._onHeaders(headers, end);
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
  _refuseHeaders(sid, stream, fields) {
    let status = 0;
    let bad = null;
    for (let i = 0; i < fields.length; i += 2) {
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
    // A GOAWAY with no live streams left ends the connection for a client too.
    if (this._streams.size === 0) {
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

  settings(payload, cb) {
    if (payload !== null && payload !== undefined) {
      const body = Buffer.from(payload);
      this._frame(SETTINGS, 0, 0, body, true);
      if (typeof cb === 'function') {
        process.nextTick(cb);
      }
    } else if (typeof payload === 'function') {
      process.nextTick(payload);
    }
  }

  goaway(code, lastStreamID, reason) {
    if (code === undefined) {
      code = NO_ERROR;
    }
    if (lastStreamID === undefined || lastStreamID === null) {
      lastStreamID = this._nextSid % 2 === 0 ? this._nextSid - 1 : this._nextSid - 2;
      if (lastStreamID < 0) {
        lastStreamID = 0;
      }
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

  destroy(err) {
    if (this.destroyed) {
      return;
    }
    this.destroyed = true;
    this.closed = true;
    this._open = false;
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
    this._outboundQueue.length = 0;
    const socket = this.socket;
    this._discardOut();
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
    this.socket.ref(); return this;
  }
  unref() {
    this.socket.unref(); return this;
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
    this._nextSid = id; return this;
  }
  setLocalWindowSize(size) {
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
    if (this._closeRequested && this._streams.size === 0 && this._outboundQueue.length === 0
      && this._outLen === 0) {
      this.destroy();
    }
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
    const socket = this.socket;
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
    if (this._closeEmitted) {
      return;
    }
    this.destroyed = true;
    this._emitClose();
  }

  _emitClose(err) {
    if (this._closeEmitted) {
      return;
    }
    this._closeEmitted = true;
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

module.exports = { ClientHttp2Session };
