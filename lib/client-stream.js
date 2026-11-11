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
    this.rstCode = null;
    this.headersSent = false; // node flips this once HEADERS are sent
    this.sentHeaders = null;
    this.sentStatus = null;
    this.responseEmitted = false;
    this.additionalHeaders = [];
    this.trailers = null;
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
  _onHeaders(headers, end) {
    const status = headers[':status'];
    if (status !== undefined && status >= 100 && status < 200 && !this.responseEmitted) {
      // 1xx informational (including the 100 Continue that node surfaces as
      // 'continue'): emit 'headers', keep waiting for the final response. The
      // 100-Continue translation node does is a 'headers' whose :status is 100.
      this.additionalHeaders.push(headers);
      this.emit('headers', headers);
      if (status === 100) {
        this.emit('continue', headers);
      }
    } else if (!this.responseEmitted) {
      this.responseEmitted = true;
      this.responseHeaders = headers;
      this.sentStatus = status;
      this.emit('response', headers);
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
  }

  // The peer reset this stream, or we reset it locally.
  _onRst(code) {
    this.rstCode = code;
    // node emits 'aborted' then 'error' for a reset that arrives mid-response.
    // For M1 the observable requirement is: the stream errors and closes, and the
    // session is untouched. Refining the abort-vs-error ordering is M2.
    this.destroy(Object.assign(new Error('HTTP/2 stream error'), {
      code: 'ERR_HTTP2_STREAM_ERROR', code2: undefined }));
  }

  // Destroying the Duplex is how the application abandons a request (`stream.destroy()`,
  // or `close()`). If the stream is still open on the wire this side owes the peer an
  // RST_STREAM; when it has already completed the session has dropped it and rstStream
  // is a no-op, so normal teardown never emits a stray reset.
  _destroy(err, cb) {
    this.session.rstStream(this.id);
    cb(err);
  }
}

module.exports = { ClientHttp2Stream };
