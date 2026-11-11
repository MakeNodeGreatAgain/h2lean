// Http2Stream equivalent: the object a request handler is handed.
//
// v1 deliberately implements the parts of the contract that can be honoured
// honestly, and omits the ones that cannot:
//   * the read side releases flow-control credit as the application takes the
//     bytes. With no reader the peer's window stays shut, so an unread body
//     costs the sender instead of our heap; data that arrives before anyone is
//     listening is buffered and replayed in order, so late listeners lose
//     nothing. pause()/resume()/isPaused() are implemented on that mechanism --
//     there is no partial credit accounting, the side either is read or it isn't.
//   * a delivered chunk stays readable for as long as the application holds it:
//     it is either a view of the buffer node read off the socket, or the private
//     buffer a frame that straddled two reads was assembled into. Neither is ever
//     recycled underneath the application, so queueing a chunk nobody has read yet
//     is safe and the receive path performs no copy.
//   * write()/end() are real and respect both the stream and connection send
//     windows, returning false when the window is exhausted. A body larger than the
//     reference threshold is queued as a view of the caller's buffer and copied
//     nowhere, so the caller must not write into it before the bytes have gone out --
//     the same promise node:http2 makes.
//   * a response the application never started is started for it: the first write()
//     sends HEADERS{:status 200} before the DATA, as node does. An end() with no body at
//     all is the one shape where h2lean answers (200, empty DATA/ES) and node says nothing
//     -- measured, and recorded in AGENTS.md section 10.
'use strict';

const { EventEmitter } = require('node:events');
const frames = require('./frames');
const hdr = require('./headers');

const EMPTY = Buffer.alloc(0);

// process.nextTick(fn, args) instead of an arrow closure per event: the arrow
// allocated a new function object for every request, and in the compat layer
// that showed up as GC time.
function emitNT(target, name, arg) {
  if (arg === undefined) {
    target.emit(name);
  } else {
    target.emit(name, arg);
  }
}

function emitErrorCloseNT(target, err) {
  target.emit('error', err);
  target.emit('close');
}

// 'end' and 'close' for a stream whose two halves finished in the same turn.
// They were already going to run in the same tick drain, so folding them into
// one callback only removes one TickObject per request.
function emitEndCloseNT(target) {
  target.emit('end');
  target.emit('close');
}

const {
  ST_OPEN, ST_HALF_CLOSED_LOCAL, ST_HALF_CLOSED_REMOTE, ST_CLOSED, NO_ERROR, PROTOCOL_ERROR
} = frames;

class Http2Stream extends EventEmitter {
  constructor(session, id) {
    super();
    this.id = id;
    this.session = session;
    this.rstCode = 0;
    this.sentHeaders = undefined;
    this.sentInfoHeaders = [];
    this.sentTrailers = undefined;
    this.headersSent = false;
    this.pending = true;
    this.aborted = false;
    this.endAfterHeaders = false;
    this.allowHalfOpen = false;
    this.state = {
      state: ST_OPEN,
      weight: 16,
      sumDependencyWeight: 0,
      localClose: 0,
      remoteClose: 0,
      localWindowSize: session.localSettings.initialWindowSize
    };
    this._sendWin = session.remoteSettings.initialWindowSize;
    this._recvUsed = 0; // inbound bytes not yet released as credit
    this._unannounced = 0; // ...consumed but not yet sent back as credit
    this._expLen = -1; // content-length promised by the request, -1 = nothing
    this._bodyLen = 0; // ...octets of body actually received
    this._queued = null; // Buffers waiting on window credit
    this._pending = null; // inbound chunks nobody has read yet
    this._endPending = false; // END_STREAM arrived with no reader
    this._consumeState = 0; // 0 no reader, 1 delivering, 2 reader is full
    this._endQueued = false;
    this._waitForTrailers = false;
    this._pendingTrailers = null; // trailer block waiting behind a queued body
    this._destroyed = false;
    // The compat layer's handler for this stream's events, or null when the
    // application uses the stream API. See emit() below.
    this._compatDispatch = null;
  }

  get destroyed() {
    return this._destroyed;
  }
  get closed() {
    return this.state.state === ST_CLOSED;
  }

  respond(headers, options) {
    if (this.headersSent) {
      const err = errorOf('ERR_HTTP2_HEADERS_SENT', 'stream has already sent HEADERS');
      this.emit('error', err);
      throw err;
    }
    const opts = options || {};
    // node puts a `date` on every response of this API too: prepareResponseHeadersObject()
    // (internal/http2/core.js:2702) adds one unless the application set one itself or
    // `options.sendDate` says otherwise -- and its test is `sendDate == null || sendDate`,
    // so the default is on and only an explicit false (or any other falsy value) turns it
    // off. Measured against a live node server: respond({':status':200}) carries a date,
    // respond({':status':204}) and 304 do too, and sendDate:false removes it.
    const sendDate = opts.sendDate == null ? true : !!opts.sendDate;
    const block = hdr.block(headers, opts.sensitiveHeaders,
      { sendDate }, this.session);
    this.sentHeaders = headers;
    this.headersSent = true;
    this.pending = false;
    this._waitForTrailers = !!opts.waitForTrailers;
    // node does not infer END_STREAM from the request having ended: the local
    // side only closes when the application says so
    const empty = opts.endStream === true;
    this.session.sendHeaders(this.id, block, empty);
    if (empty) {
      // This response has already carried END_STREAM, and nothing may follow it on this
      // stream: RFC 9113 6.1 puts a trailing HEADERS *before* the close, and node shows
      // what that means for this combination -- respond({waitForTrailers:true,
      // endStream:true}) puts a single HEADERS/ES/EH on the wire and no trailer block.
      // So the outstanding request for trailers is dropped instead of answered after the
      // stream is closed, and 'wantTrailers' is not emitted for a stream that cannot send.
      this._waitForTrailers = false;
      this._endLocalSide();
    }
    return this;
  }

  write(chunk, encoding, cb) {
    if (typeof encoding === 'function') {
      cb = encoding; encoding = undefined;
    }
    if (this._endQueued || this.state.localClose === 1) {
      const err = errorOf('ERR_HTTP2_STREAM_CANCEL', 'stream write after end');
      if (cb) {
        return process.nextTick(cb, err);
      }
      this.emit('error', err);
      return false;
    }
    // The response starts itself if the application never called respond(). Node does this on
    // the first WRITE of a server stream, not on the first end(): measured, `write('x')` alone
    // puts HEADERS{:status 200} on the wire ahead of the DATA. Doing it here as well is what
    // keeps DATA from arriving on a stream that has no response headers yet -- a peer that sees
    // DATA first has nothing to attach it to, and nghttp2 answers that with a stream error.
    if (!this.headersSent) {
      this.respond({ ':status': 200 });
    }
    const buf = typeof chunk === 'string' ? Buffer.from(chunk, encoding || 'utf8') : chunk;
    const canContinue = this.session.sendData(this, buf, false);
    if (cb) {
      process.nextTick(cb);
    }
    return canContinue;
  }

  end(chunk, encoding, cb) {
    if (typeof chunk === 'function') {
      cb = chunk; chunk = null;
    } else if (typeof encoding === 'function') {
      cb = encoding; encoding = null;
    }
    if (!this.headersSent) {
      // A body takes the auto-respond of write() above; an empty end() is where h2lean and node
      // part company, and the split is written down in AGENTS.md section 10: node puts NOTHING on
      // the wire for `end()` before respond() -- no HEADERS, no DATA, no END_STREAM, and
      // headersSent stays false -- while h2lean answers a plain 200 rather than leave a stream
      // that no peer will ever see an answer on.
      this.respond({ ':status': 200 });
    }
    const buf = chunk == null ? EMPTY : (typeof chunk === 'string'
      ? Buffer.from(chunk, encoding || 'utf8') : chunk);
    // A stream that still owes a trailer block must not close the stream with its body.
    // RFC 9113 6.1 puts a trailing HEADERS before END_STREAM, and node's frame sequence
    // shows the placement: HEADERS/EH DATA HEADERS/ES/EH for a body, and no DATA at all
    // for an empty one -- where h2lean used to send DATA/ES first and then a trailer block
    // that no peer could still read. The application-level half-close happens here (that
    // is what emits 'wantTrailers'), the wire-level one happens in sendTrailers().
    const oweTrailers = this._waitForTrailers;
    this.session.sendData(this, buf, !oweTrailers);
    if (oweTrailers) {
      this._endLocalSide();
    }
    if (cb) {
      process.nextTick(cb);
    }
    return this;
  }

  sendTrailers(trailers) {
    if (!this._waitForTrailers) {
      this.emit('error', errorOf('ERR_HTTP2_TRAILERS_NOT_READY',
        'trailers cannot be sent unless a request is made by setting waitForTrailers'));
      return;
    }
    const block = hdr.block(trailers, undefined, { trailer: true }, this.session);
    this.sentTrailers = trailers;
    // the trailers are out: the flag that permitted this call must not keep asking for
    // wantTrailers, whatever happens to the bytes below
    this._waitForTrailers = false;
    if (this._queued !== null) {
      // The body is still waiting for credit or for the socket, and this block has to
      // follow it: END_STREAM rides on the trailers, so a HEADERS sent now would outrun
      // DATA that has not gone out. Stash a copy (the block is a view into shared scratch)
      // and let the session put it on the wire when the queue drains.
      this._pendingTrailers = Buffer.from(block);
      return this;
    }
    this.session.sendHeaders(this.id, block, true);
    // the local half closes for good now: the bytes that owed it are on the wire
    this._endLocalSide();
  }

  close(code = NO_ERROR, reason) {
    this._destroy(code, reason, true);
  }

  destroy(codeOrError, reason) {
    const code = typeof codeOrError === 'number' ? codeOrError : NO_ERROR;
    this._destroy(code, reason, true, typeof codeOrError === 'object' ? codeOrError : null);
  }

  // An informational (1xx) HEADERS block, sent before the final response.
  // node: Http2Stream.additionalHeaders().
  additionalHeaders(headers) {
    // `{ info: true }` is node's separate rule for these blocks: :status must be 100..199
    // (101 has its own error), there is no default status and no generated date.
    const block = hdr.block(headers, undefined, { info: true }, this.session);
    this.sentInfoHeaders.push(headers);
    this.session.sendHeaders(this.id, block, false);
    return this;
  }

  priority() {
    // RFC 9218 deprecates the priority scheme; node still accepts the call.
    // Returning true without reordering anything is the honest v1 answer.
    return true;
  }

  // Readable-side control, node's contract: while paused no credit is released,
  // so the sender is the one that waits, and whatever arrives in the meantime is
  // buffered and replayed on resume(). Returns this, like the Duplex does.
  pause() {
    this._pauseConsuming();
    return this;
  }

  resume() {
    this._startConsuming();
    return this;
  }

  isPaused() {
    return this._consumeState === 2;
  }

  // node maps a per-stream timeout onto the connection's idle timeout.
  setTimeout(ms, cb) {
    this.session.setTimeout(ms, cb);
    return this;
  }

  // --- internals used by the session ------------------------------------------
  // Both halves have to converge: a handler that answers synchronously inside
  // the 'stream' emit closes the local side BEFORE the peer's END_STREAM is
  // reported, and vice versa. Whichever call sees both halves closed is the one
  // that removes the stream from the session -- without it every request leaks
  // an Http2Stream (and _liveStreams never falls, so close() never finishes).
  _remoteEnded() {
    if (this.state.remoteClose === 1) {
      return;
    }
    // The other half of the content-length promise: too many octets is caught as they arrive
    // (session _onDataFrame, a connection error), too few can only be seen here, where the
    // message ends. Measured on node (h2spec http2/8.1.2.6): RST_STREAM(PROTOCOL_ERROR) and the
    // application is told ERR_HTTP2_STREAM_ERROR -- the short body is one request's failure, not
    // the session's, which is the same split nghttp2 makes and the reason the two directions of
    // the same lie get two different codes.
    if (this._expLen >= 0 && this._bodyLen < this._expLen) {
      this.session._streamError(this, PROTOCOL_ERROR, `content-length ${this._expLen} with only ` +
        `${this._bodyLen} octets received`);
      return;
    }
    this.state.remoteClose = 1;
    this.endAfterHeaders = true;
    // the readable side is done: 'end' after the last 'data', like a Readable.
    // No auto-destroy: the application still has to answer. node keeps the
    // writable side of an unanswered request open as well.
    if (this.state.localClose === 1) {
      this._closeNow();
      process.nextTick(emitEndCloseNT, this);
      return;
    }
    if (this.state.state === ST_OPEN) {
      this.state.state = ST_HALF_CLOSED_REMOTE;
    }
    if (!this._isConsuming()) {
      // Nothing has read the buffered bytes yet, so 'end' must not overtake them:
      // a reader that attaches later is owed the whole sequence, EOF included.
      this._endPending = true;
      return;
    }
    process.nextTick(emitNT, this, 'end');
  }

  _endLocalSide() {
    const first = this.state.localClose !== 1;
    this.state.localClose = 1;
    this._endQueued = false;
    if (this.state.state === ST_OPEN) {
      this.state.state = ST_HALF_CLOSED_LOCAL;
    }
    if (this._waitForTrailers) {
      // one shot: a later end() must not ask for trailers a second time. The flag
      // itself stays set, because sendTrailers() is only allowed while it is set.
      if (first) {
        process.nextTick(emitNT, this, 'wantTrailers');
      }
      return;
    }
    if (this.state.remoteClose === 1) {
      this._closeIfBothDone();
    }
  }

  _closeIfBothDone() {
    if (this._destroyed || this.state.localClose !== 1 || this.state.remoteClose !== 1) {
      return;
    }
    this._closeNow();
    // An EOF that arrived while nobody was reading is still owed to the application, and it
    // goes out before 'close' -- the same pair the both-halves-in-one-turn path above uses.
    // Without this the stream simply never reported 'end' for a request that ended on its
    // HEADERS frame (END_STREAM on HEADERS is every GET): _remoteEnded() had parked the EOF
    // in _endPending for a reader that never appeared, and the local close below was the last
    // event the stream ever produced. Measured against node on exactly that shape: it emits
    // stream.end then stream.close (test/event-order.test.js, scenario eof).
    if (this._endPending) {
      this._endPending = false;
      process.nextTick(emitEndCloseNT, this);
      return;
    }
    process.nextTick(emitNT, this, 'close');
  }

  _closeNow() {
    this.state.state = ST_CLOSED;
    this._destroyed = true;
    this._pending = null;
    this.session._dropStream(this, false);
  }

  _destroy(code, reason, sendRst, err) {
    if (this._destroyed) {
      return;
    }
    this._destroyed = true;
    this.rstCode = code;
    // node's rule for 'aborted' is about the RESPONSE, not about the request:
    // "if the writable side of the Http2Stream is still open, emit the 'aborted'
    // event" (internal/http2/core.js, closeStream). Measured against a live node
    // server: a peer that resets a stream whose request has already ended but
    // whose response never started does report 'aborted', and a peer that resets
    // a stream whose response was ended does not.
    const responseOpen = this.state.localClose !== 1;
    this.state.localClose = 1;
    this.state.state = ST_CLOSED;
    if (sendRst && this.session._open) {
      this.session.sendRst(this.id, code, reason);
    }
    this._pending = null;
    this.session._dropStream(this, true);
    // The event is emitted synchronously -- before the 'end' that an already-queued
    // tick would otherwise deliver first. Marking the remote half closed keeps a
    // late END_STREAM from emitting 'end' after 'close'.
    this.state.remoteClose = 1;
    if (responseOpen) {
      this.aborted = true;
      this.emit('aborted');
    }
    if (err) {
      process.nextTick(emitErrorCloseNT, this, err);
    } else {
      process.nextTick(emitNT, this, 'close');
    }
  }

  // --- the compat layer's handlers ------------------------------------------------
  // The compat layer does not subscribe to the stream's events: it installs one
  // dispatch function and the stream calls it -- subscriptions are an order of
  // magnitude more expensive per request than the dispatch (OPTIMIZATION.md
  // item 5) -- and the handlers want the stream as `this` anyway.
  //
  // The dispatch runs BEFORE the event reaches the emitter, which is the order
  // the subscriptions had: the compat layer's own handlers first, the
  // application's listeners after.
  emit(name, arg) {
    const dispatch = this._compatDispatch;
    if (dispatch !== null) {
      dispatch.call(this, name, arg);
    }
    // The argument list is forwarded as it came: a listener that counts its
    // arguments must not see an extra undefined.
    if (arguments.length === 1) {
      return super.emit(name);
    }
    if (arguments.length === 2) {
      return super.emit(name, arg);
    }
    return super.emit.apply(this, arguments);
  }

  // --- inbound flow control ----------------------------------------------------
  // Attaching a reader is how the application says "send me the body": replay
  // whatever arrived before it did. attach after super.on(), or the replay would
  // emit into an empty listener list. once()/addListener are covered by the
  // listenerCount() check at delivery time.
  on(name, fn) {
    const r = super.on(name, fn);
    if (name === 'data' || name === 'readable') {
      this._startConsuming();
    }
    return r;
  }

  once(name, fn) {
    const r = super.once(name, fn);
    if (name === 'data' || name === 'readable') {
      this._startConsuming();
    }
    return r;
  }

  // Credit goes back when the application takes the bytes, so a body nobody
  // reads closes the sender's window instead of growing our heap. The buffer of
  // unread chunks cannot exceed the window we advertised, because the peer is
  // not allowed to send more than that without a credit we never gave.
  _isConsuming() {
    if (this._consumeState === 2) {
      return false;
    } // the reader asked us to stop
    return this._consumeState === 1 || this.listenerCount('data') > 0 ||
           this.listenerCount('readable') > 0;
  }

  // Called when a reader shows up: replay what was buffered, in order, and let
  // later frames through directly.
  _startConsuming() {
    this._consumeState = 1;
    const pending = this._pending;
    if (pending !== null) {
      this._pending = null;
      for (let i = 0; i < pending.length; i++) {
        const chunk = pending[i];
        this._release(chunk.length);
        this.emit('data', chunk);
      }
    }
    if (this._endPending && !this._destroyed) {
      this._endPending = false;
      process.nextTick(emitNT, this, 'end');
    }
  }

  // Internal pause, used by the compat layer when its Readable is full. There is
  // no public pause()/resume() yet, and this is the only reason it exists.
  _pauseConsuming() {
    this._consumeState = 2;
  }

  _release(n) {
    if (n > 0) {
      this.session._consume(this, n);
    }
  }

  _recvData(buf) {
    const n = buf.length;
    if (n === 0) {
      return;
    } // an empty DATA frame carries no payload
    this._recvUsed += n;
    if (this._isConsuming()) {
      this._release(n);
      this.emit('data', buf);
      return;
    }
    // the chunk is either a view of a socket read or a buffer built for this frame
    // alone; both outlive the tick, so it can simply be queued
    if (this._pending === null) {
      this._pending = [];
    }
    this._pending.push(buf);
  }
}

function errorOf(code, msg) {
  const err = new Error(msg);
  err.code = code;
  return err;
}

module.exports = { Http2Stream, errorOf, emitNT };
