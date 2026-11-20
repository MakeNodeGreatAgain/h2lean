// ClientHttp2Stream: the Duplex node hands back from `session.request()`.
//
// This mirrors `Http2Stream`/`ClientHttp2Stream` in node's bundled
// `internal/http2/core.js`. Two shapes are load-bearing for a drop-in:
//  * it is a `stream.Duplex` with `allowHalfOpen: true`, so ending the request
//    body (writable) does not close the response (readable). node's `Http2Stream`
//    extends Duplex (core.js:2128); h2lean's SERVER stream is an EventEmitter
//    (AGENTS.md 10) but a client stream the application `.pipe()`s and reads
//    `:status` off has to be a real Duplex or the common
//    `req.pipe(server).pipe(res)` idiom breaks.
//  * the protocol work (DATA framing, flow control, HPACK) belongs to the
//    session, not here. This file only moves bytes between the Duplex's two
//    queues and the session, and emits the events the application subscribes to
//    ('response', 'trailers', 'close', 'error', informational 'headers').
'use strict';

const { Duplex } = require('node:stream');
const { NO_ERROR } = require('./frames');
const { requestView } = require('./headers');
const { streamErrorOf } = require('./stream');

// `sentHeaders`/`sentTrailers`/`responseHeaders` and the state flags node exposes
// through symbols are kept as plain fields here; they are not part of the
// documented client surface an application is expected to touch, and pinning them
// to node's symbol identity is an M2 compatibility decision, not a round-trip one.
class ClientHttp2Stream extends Duplex {
  constructor(session, id, _opts) {
    super({ allowHalfOpen: true, decodeStrings: false });
    this.id = id;
    this.session = session;
    // Per-stream flow-control state; the connection window lives on the session.
    // Both start at the peer's advertised initial window (65535 until its SETTINGS
    // land) exactly as the server side does. The value written here is the
    // default one: the session overwrites it with the peer's number when the
    // stream is actually opened (RFC 9113 6.9.2 -- a stream runs on the peer's
    // SETTINGS_INITIAL_WINDOW_SIZE), which may be later than this construction.
    this._sendWin = session.remoteSettings.initialWindowSize;
    this._recvUsed = 0;
    // Whether this stream is already in the session's _debt list, i.e. whether the
    // next credit pass has to visit it. Declared here so every stream shares one
    // V8 object shape; only lib/client-session.js reads or writes it.
    this._debtMarked = false;
    // Bytes the application has written but that flow control has not yet let us
    // frame; kept by reference so a stalled upload costs no copy.
    this._pending = null;
    this._pendingOff = 0;
    this._writeCb = null;
    this._finalCb = null;
    // Whether the request HEADERS frame has been committed to the outbound queue.
    // _pump() returns early until this is true, so a write() that lands before the
    // 'connect' event (or before the queued request is framed) cannot put DATA on
    // the wire ahead of its stream's HEADERS -- nghttp2 reads that as INTERNAL_ERROR.
    this._headersSent = false;
    // Whether the request message is over. `endAfterHeaders` is the GET case:
    // HEADERS already carried END_STREAM, so `_final` must not emit an empty DATA.
    this._endSent = false;
    this.endAfterHeaders = false;
    // The readable half has seen END_STREAM (server finished the response body).
    this._remoteEnded = false;
    // node's STREAM_FLAGS_ABORTED, exposed through the `aborted` getter
    // (internal/http2/core.js:2289). Set once, by the reset that found the writable
    // half still open; closeStream guards the event on it.
    this.aborted = false;
    // The peer reset this stream. nghttp2 refuses DATA and HEADERS on a closed
    // stream, so _pump must settle the Duplex's writable half without framing.
    this._reset = false;
    // node keeps rstCode in the stream's C-side state, initialised with
    // NGHTTP2_NO_ERROR (internal/http2/core.js:2152 `rstCode: NGHTTP2_NO_ERROR`), so a
    // stream that closed without a reset reports 0 -- the upstream
    // max-reserved-streams test reads exactly that off the parent request.
    // h2lean only overwrites it on a real reset (_onRst).
    this.rstCode = 0;
    this.headersSent = false; // node flips this once HEADERS are sent
    // What request() was given, for the sentHeaders getter below. node keeps the caller's
    // map as kRawHeaders and folds it on the first read (internal/http2/core.js:2225).
    this._sentSrc = undefined;
    this._sentView = undefined;
    this.sentStatus = null;
    this.responseEmitted = false;
    // Set by the session for a stream created from PUSH_PROMISE: the first final
    // response block emits 'push' instead of 'response' (node's event model,
    // measured v26.12.1).
    this._pushed = false;
    this.additionalHeaders = [];
    this.trailers = null;
    // Set by session.request() when the application passed waitForTrailers: the last
    // DATA on the wire does not carry END_STREAM; instead 'wantTrailers' fires and
    // the app is expected to call sendTrailers() (or close()) to finish the message.
    // Matches node's STREAM_FLAGS_HAS_TRAILERS on a client stream.
    this._waitForTrailers = false;
    // node's `state.trailersReady`, and the same rule as on the server: the permission
    // to call sendTrailers() comes from the 'wantTrailers' callback, not from the
    // option. sendTrailers() is defined on the shared Http2Stream base in node
    // (internal/http2/core.js), so a client stream answers with the same three errors.
    this._trailersReady = false;
    // undefined until a trailer block is sent, the way node's kSentTrailers reads before
    // sendTrailers() (measured on a live node client); the server stream's field is the same.
    this.sentTrailers = undefined;
  }

  // node's client sentHeaders is the request map with the pseudo-headers request() resolved
  // -- ':method' GET, ':authority' from the connect authority, ':scheme' from the transport,
  // ':path' '/' -- folded out of what the application passed (prepareRequestHeadersObject /
  // ...Array, internal/http2/util.js:717/:627, read through the getter at core.js:2225).
  // Resolved against the session as it stood when the request was made, and built on the
  // first read so a request nobody inspects pays no copy.
  get sentHeaders() {
    // A stream created from PUSH_PROMISE has neither kSentHeaders nor kRawHeaders,
    // so node's getter answers undefined (core.js:2226 `if (this[kSentHeaders] ||
    // !this[kRawHeaders]) return this[kSentHeaders]`; measured on a live node client).
    // A request() with no headers argument is not that case: node prepares `{}` and
    // resolves the four pseudo-headers out of it (core.js:1871-1878).
    if (this._pushed) {
      return undefined;
    }
    if (this._sentView === undefined) {
      this._sentView = requestView(this._sentSrc, this.session._scheme, this.session._authority);
    }
    return this._sentView;
  }

  // ---- writable side ----------------------------------------------------------
  // The application's bytes go to the session, which frames them as DATA inside
  // the two windows. If there is no credit, `_pump` leaves `_pending` set and the
  // session resumes us on WINDOW_UPDATE; the Duplex callback is not run until the
  // bytes are handed over, which is what makes `write()` honour backpressure.
  _write(chunk, encoding, cb) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, encoding);
    this._pending = buf;
    this._pendingOff = 0;
    this._writeCb = cb;
    this.session._pump(this);
  }

  _final(cb) {
    this._finalCb = cb;
    this.session._pump(this);
  }

  // Called by the session once the writable half has been fully framed and
  // END_STREAM put on the wire.
  _writableDone() {
    this._endSent = true;
    const cb = this._finalCb;
    if (cb !== null) {
      this._finalCb = null; cb();
    }
  }

  // ---- readable side ----------------------------------------------------------
  // The application pulling does not gate what we have already buffered (we hand
  // DATA straight to `push`, returning flow-control credit as we consume it), so
  // `_read` has nothing to fetch. Withholding credit until here is the backpressure
  // refinement M2 adds; for a body that fits the window the response is complete
  // whether or not the consumer has read it yet.
  _read() {}

  // The session decoded a response (or informational / trailer) block for us.
  // `headers` is a null-prototype object with `:status` coerced to a number and
  // `set-cookie` as an array, built the way the server's `_toObject` builds a
  // request, so both stacks hand the application the same shape.
  _onHeaders(headers, end, flags) {
    const status = headers[':status'];
    if (status !== undefined && status >= 100 && status < 200 && !this.responseEmitted) {
      // 1xx informational (including the 100 Continue that node surfaces as
      // 'continue'): emit 'headers', keep waiting for the final response. The
      // 100-Continue translation node does is a 'headers' whose :status is 100.
      this.additionalHeaders.push(headers);
      // node's inbound emit carries the frame flags as the second argument
      // (`emit(stream, event, obj, flags, headers)`, internal/http2/core.js), which is
      // what the upstream sent-headers test asserts is a number.
      this.emit('headers', headers, flags);
      if (status === 100) {
        this.emit('continue', headers);
      }
    } else if (!this.responseEmitted) {
      this.responseEmitted = true;
      this.responseHeaders = headers;
      this.sentStatus = status;
      // node passes the raw HEADERS frame flags as the second argument and a
      // rawHeaders array as the third (core.js: emit(stream, event, obj, flags,
      // headers)); the rawHeaders array is deliberately not built -- one flat
      // array per response is a cost no h2lean consumer has asked for.
      this.emit(this._pushed ? 'push' : 'response', headers, flags);
    } else {
      // A second final-status block after the response is the trailer block.
      this.trailers = headers;
      this.emit('trailers', headers);
    }
    if (end) {
      this._endRead();
    }
  }

  // The session delivered a DATA payload for this stream.
  _onData(buf) {
    this._remoteEnded = false;
    this.push(buf);
  }

  _endRead() {
    if (this._remoteEnded) {
      return;
    }
    this._remoteEnded = true;
    this.push(null);
    // node kicks the readable half here so an unconsumed response still reaches
    // 'end' (and, with the writable finished, autoDestroy's 'close'): its
    // onStreamClose calls stream.read(0) on the client branch
    // (internal/http2/core.js:641). push(null) alone only sets state.ended; the
    // 'end' event waits for a read(), so a request the application never resumed
    // -- the upstream capture-rejection shape, an empty 500 body -- never closed
    // (measured on live node: req 'close' fires, h2lean did
    // not). read(0) is harmless when a consumer is already draining: endReadable
    // de-dupes the scheduled tick, and a buffered body keeps length > 0 so 'end'
    // still waits for the last chunk.
    this.read(0);
  }

  // The peer reset this stream. node runs two steps in this order -- closeStream, then
  // the readable decision in onStreamClose (internal/http2/core.js) -- and both are
  // observable: the writable half is closed first, and a clean reset waits for the
  // buffered body instead of discarding it.
  _onRst(code) {
    this.rstCode = code;
    this._reset = true;
    // closeStream turns on `_writableState.ending` -- whether end() was *called*, not
    // whether the octets went out. A request body still open is 'aborted' and then
    // ended, so the stream reports 'finish' and writableEnded like node's (measured on
    // v26.12.1 for codes 0, 1 and 8 against a raw peer, with and without END_STREAM).
    if (!this.writableEnded) {
      if (!this.aborted) {
        this.aborted = true;
        this.emit('aborted');
      }
      this.end();
    }
    // NO_ERROR is a clean end, not a reset that discards: node pushes null and destroys
    // on 'end' ("Preserve buffered reads after a clean reset"), so 'end' fires once the
    // body is consumed. A body nobody consumes keeps the stream open on node too --
    // measured, no 'close' until a read -- so the wait is parity, not a leak we invent.
    if (code === NO_ERROR && this.readable) {
      this._endRead();
      this.once('end', () => this.destroy());
      return;
    }
    // The error is the code's own name -- except for NO_ERROR and CANCEL, which are not
    // errors (core.js:2543, measured on a live node client for all 14 codes and three
    // outside the table). lib/stream.js streamErrorOf is the same rule the server uses.
    this.destroy(streamErrorOf(code));
  }

  // Destroying the Duplex is how the application abandons a request (`stream.destroy()`,
  // or `close()`). If the stream is still open on the wire this side owes the peer an
  // RST_STREAM; when it has already completed the session has dropped it and rstStream
  // is a no-op, so normal teardown never emits a stray reset.
  _destroy(err, cb) {
    this.session.rstStream(this.id);
    // node's _destroy pushes null unconditionally, after closeStream and before the
    // handle goes: a destroy that carries no error still ends the readable half, so a
    // CANCEL reset -- and an application destroy() with the body already consumed --
    // reports 'end' (measured on v26.12.1). onEofChunk is idempotent, so a readable half
    // that already saw END_STREAM is unaffected.
    this.push(null);
    cb(err);
  }

  // node's Http2Stream.sendTrailers(headers) -- the same base method the server stream
  // exposes, so the same three guards in the same order, all synchronous throws:
  // destroyed (node also refuses when the stream is closed; this Duplex has no separate
  // closed flag, its 'close' follows destroy) -> ERR_HTTP2_INVALID_STREAM, already sent
  // -> ERR_HTTP2_TRAILERS_ALREADY_SENT, and 'wantTrailers' not yet emitted
  // -> ERR_HTTP2_TRAILERS_NOT_READY.
  sendTrailers(trailers) {
    if (this.destroyed) {
      const err = new Error('The stream has been destroyed');
      err.code = 'ERR_HTTP2_INVALID_STREAM';
      throw err;
    }
    if (this.sentTrailers) {
      const err = new Error('Trailing headers have already been sent');
      err.code = 'ERR_HTTP2_TRAILERS_ALREADY_SENT';
      throw err;
    }
    if (!this._trailersReady) {
      const err = new Error(
        'Trailing headers cannot be sent until after the wantTrailers event is emitted');
      err.code = 'ERR_HTTP2_TRAILERS_NOT_READY';
      throw err;
    }
    // node drops the permission flag before touching the wire so a second
    // sendTrailers() hits the ALREADY_SENT guard. The bytes are built inside the
    // session, which also puts HEADERS+END_STREAM on the wire for this stream id.
    this._waitForTrailers = false;
    this._trailersReady = false;
    this.session._sendTrailers(this, trailers);
    // node records a null-prototype copy, not the caller's map
    // (`headers = ObjectAssign({ __proto__: null }, headers)` then
    // `this[kSentTrailers] = headers`, internal/http2/core.js sendTrailers), and only
    // once the block has been built -- a trailer map the encoder refuses leaves
    // sentTrailers unset on both stacks.
    this.sentTrailers = Object.assign({ __proto__: null }, trailers);
  }
}

module.exports = { ClientHttp2Stream };
