// Http2Stream equivalent: the object a request handler is handed.
//
// v1 deliberately implements the parts of the contract that can be honoured
// honestly, and omits the ones that cannot:
//   * the base is `stream.Stream`, not Duplex and not a bare EventEmitter. node's Http2Stream
//     extends Duplex (internal/http2/core.js:2128), but the part of that surface real code reaches
//     for is pipe() on the readable half, and the Duplex machinery behind it -- two state objects
//     plus the nextTick read loop -- is what OPTIMIZATION.md item 41 measured as ~+1.4 us/req
//     against the EventEmitter arm. Stream.prototype.pipe supplies that one method without the
//     cost. So `stream.pipe(dest)` works (test/pipe-parity.test.js). cork()/uncork() and the
//     writableCorked/writableHighWaterMark getters are present too, but as a counting stub:
//     node's Writable cork buffers write()/end() and defers the DATA until corked reaches 0
//     (measured on v26.12.1: write('AAA') while corked leaves writableLength 3 and the peer
//     receives nothing until the second uncork), while h2lean writes straight through -- its
//     per-read socket batch already coalesces a synchronous burst, so only the cross-tick
//     deferral differs. KNOWN DIVERGENCE, pinned in test/cork-parity.test.js. The rest of the
//     Duplex surface -- unpipe(), read(), writev() and the readable/writable/writableLength
//     state getters -- is still absent (AGENTS.md section 10). The one behavioural difference the
//     bare base leaves is pipe teardown: legacy Stream.prototype.pipe ends the dest only on the
//     source 'end', so a stream destroyed mid-pipe leaves the dest without the 'finish'/'unpipe'
//     node's Duplex pipe emits. Measured and pinned in test/pipe-parity.test.js.
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
//   * a response STARTED and then destroyed in the same turn of the loop is the other:
//     node submits the HEADERS to nghttp2 and nothing sends them before the destroy, so
//     the teardown discards them and the peer sees only RST_STREAM. respond() here frames
//     them into the session's staging buffer immediately and the batch leaves at the end
//     of the read, so they reach the peer. The trigger is the turn, not the empty write:
//     a non-empty write() flushes in the same turn on both stacks, and a destroy a
//     setImmediate later agrees too. headersSent and rstCode are the same either way.
//     Measured by tools/probe-zero-write.js, pinned in test/response-start.test.js, and
//     the same outbound-batch decision as the refusal case in AGENTS.md section 10 --
//     the staging buffer holds flat bytes with no per-stream handle to discard.
//   * an outbound header block over the session's maxSendHeaderBlockLength is refused the
//     way nghttp2 refuses it: 'frameError' on the stream, then a deferred close carrying
//     FRAME_SIZE_ERROR and a session close behind it -- not a throw out of the send call.
//     With no reader on the request half, node's own unread-request auto-close reaches the
//     stream first and reports rstCode 0 where h2lean reports 6: the two stacks schedule
//     the same pair in opposite orders, node's writable 'finish' before the frame pump
//     refuses the block and h2lean's half-close after it
//     (test/upstream/exceeds-server-trailer-size.test.js).
//   * respondWithFD()/respondWithFile() send a file body straight from the descriptor:
//     HEADERS are framed in the call (headersSent is true on return), then a chunked
//     read/pump walk writes the body through write()/end() with normal backpressure, the
//     same shape node's StreamPipe drives. statCheck (3-arg on the FD path, 2-arg on the
//     file path) may rewrite 'content-length' in the prepared copy and may cancel by
//     returning false; on the FD path cancelling leaves the descriptor open (the caller
//     owns it), on the file path it is closed. Non-regular files refuse seek options and
//     directories refuse outright (ERR_HTTP2_SEND_FILE / ERR_HTTP2_SEND_FILE_NOSEEK),
//     as node's doSendFileFD does. Read and async errors destroy the stream with
//     ERR_HTTP2_STREAM_ERROR(NGHTTP2_INTERNAL_ERROR) rather than throwing out of the call,
//     matching node's buildNgHeaderString/processRespondWithFD teardown: a bad field name
//     leaves headersSent true (node sets HEADERS_SENT before validating fields) and takes
//     the destroy path. An array-form headers argument passes through hdr.block as-is
//     instead of node's for..in over indices (which sends field names '0', '1', ...);
//     the content-length statCheck injects is only applied to object-form headers.
//   * 'finish' and writableFinished exist on the stream: 'finish' is queued on a
//     nextTick once the writable half has ended with every octet accepted by the
//     session (no queued DATA left), before any wantTrailers/close tick, and is never
//     emitted for a stream destroyed before completion -- node's Writable semantics,
//     measured on v26.12.1 (clean respondWithFD: finish after the pump completes, close
//     with writableFinished true; abort after respondWithFD: only close, writableFinished
//     false).
'use strict';

const fs = require('node:fs');
const { inspect } = require('node:util');
const { Stream, Writable } = require('node:stream');
const { deprecate } = require('node:util');
const frames = require('./frames');
const hdr = require('./headers');

const EMPTY = Buffer.alloc(0);

// node's Http2Stream sets no highWaterMark (internal/http2/core.js constructor passes only
// allowHalfOpen/decodeStrings/autoDestroy), so its writableHighWaterMark is the default
// Writable one -- 16384 on node 20, 65536 from 22 on. Read it off node itself once rather
// than hardcode a per-major constant that could drift; measured equal to a live node
// stream's writableHighWaterMark on v26.12.1.
const WRITABLE_HWM = new Writable().writableHighWaterMark;

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

// node's onStreamTrailers (internal/http2/core.js), the callback nghttp2 runs when the
// writable half of a stream that asked for trailers finishes. Two parts of it are easy
// to lose: `trailersReady` is set before the destroyed/closed check (so a stream that is
// already gone is still marked ready), and when nothing is listening for the event node
// does not leave the deferred END_STREAM outstanding -- it sends an empty trailer block
// itself and closes. An empty block is not a header section on the wire: it is the withheld
// END_STREAM on a zero-length DATA, see sendTrailers() below. Without that last part a
// `respond({waitForTrailers:true})` followed by `end()` with no 'wantTrailers' handler hangs
// the stream forever, which is exactly what upstream's
// test-http2-no-wanttrailers-listener.js tests.
//
// h2lean's compat layer is not a subscriber (see emit() below), so `false` from emit()
// does not by itself mean nobody answered: the compat handler calls sendTrailers() from
// inside the dispatch, and the sentTrailers check is what sees that it did.
function wantTrailersNT(stream) {
  stream._trailersReady = true;
  if (stream._destroyed || stream.closed) {
    return;
  }
  if (!stream.emit('wantTrailers') && !stream.sentTrailers) {
    stream.sendTrailers({});
  }
}

// 'end' and 'close' for a stream whose two halves finished in the same turn.
// They were already going to run in the same tick drain, so folding them into
// one callback only removes one TickObject per request.
function emitEndCloseNT(target) {
  target._closeDelivered = true;
  // node clears the stream's session reference in _destroy, before any of the close
  // events go out (internal/http2/core.js:2546) -- see _closeNow for why the drop waits
  // for this tick here.
  target.session = undefined;
  target.emit('end');
  target.emit('close');
}

// The plain-close half of the same pair: 'close' for a stream whose both halves finished
// in one turn, with no 'end' owed. Marks the node-equivalent close moment -- see
// _closeDelivered above.
function emitCloseDeliveredNT(target) {
  target._closeDelivered = true;
  target.session = undefined;
  target.emit('close');
}

// The scheduled half of the unread-request auto-close -- see _maybeAutoClose
// below for the rule and its measurement. The unread request half reports 'end'
// here, before the 'close' _destroy schedules behind it: node's trace for the
// shape does the same (test/event-order.test.js, scenario eof-body) -- the
// readable side is finished by the close, never having been read.
function autoCloseNT(stream) {
  if (stream._destroyed) {
    return;
  }
  process.nextTick(emitNT, stream, 'end');
  stream._destroy(NO_ERROR, undefined, true);
}

// node's onFileUnpipe closes an owned descriptor without caring about the error; the
// same empty callback goes to every fs.close the file paths issue.
function noopClose() {}

// The read chunk of the file pump. node's StreamPipe sizes its reads internally; the
// boundaries are not wire-visible here (each read feeds write(), which frames DATA by
// maxFrameSize and the flow-control windows either way), so a plain 64 KiB read is used.
const FILE_CHUNK = 65536;

// node tests `fd instanceof fsPromisesInternal.FileHandle`, but node:fs/promises does
// not export the class (it lives behind --expose-internals), so a real handle is
// recognised by its constructor name and the numeric fd member: an fs.promises.open()
// instance passes, a plain { fd } object does not. A subclass or a same-named replica
// passes too, which is the same leniency node's instanceof shows to subclasses.
const isFileHandle = (v) =>
  typeof v === 'object' && v !== null && typeof v.fd === 'number' &&
  typeof v.close === 'function' && v.constructor != null &&
  v.constructor.name === 'FileHandle';

// node's ERR_INVALID_ARG_TYPE received-tail (lib/internal/errors.js), measured against
// a live v26.12.1 respondWithFD: a named function renders `function ${name}` -- with
// the trailing space an anonymous one keeps --, instances name their constructor,
// primitives their typeof with the inspected value, null/undefined stand alone.
function receivedArgType(v) {
  switch (typeof v) {
    case 'function':
      return `function ${v.name}`;
    case 'object':
      if (v === null) {
        return 'null';
      }
      return v.constructor != null
        ? `an instance of ${v.constructor.name}` : 'an object with null prototype';
    case 'undefined':
      return 'undefined';
    case 'string':
      return `type string (${inspect(v)})`;
    case 'symbol':
      return `type symbol (${String(v)})`;
    default:
      return `type ${typeof v} (${inspect(v)})`;
  }
}

// The two TypeError shapes the argument guards raise, measured on the same binary:
// 'The "fd" argument must be of type number or an instance of FileHandle. Received ...'
// and `The property 'options.offset' is invalid. Received ${inspect(v)}`.
function argTypeError(msg, v) {
  const err = new TypeError(`${msg} Received ${receivedArgType(v)}`);
  err.code = 'ERR_INVALID_ARG_TYPE';
  return err;
}

function propertyValueError(name, v) {
  const err = new TypeError(
    `The property 'options.${name}' is invalid. Received ${inspect(v)}`);
  err.code = 'ERR_INVALID_ARG_VALUE';
  return err;
}

// node's options guard (assertIsObject plus the explicit checks of respondWithFD,
// internal/http2/core.js): an undefined options is an empty one, null and primitives
// are refused, and -- measured -- an array is refused for options (unlike the headers
// argument). offset/length/statCheck keep node's `!== undefined` tests, so a null
// offset is refused exactly the way a string one is.
function fdOptions(options) {
  if (options !== undefined &&
      (options === null || typeof options !== 'object' || Array.isArray(options))) {
    throw argTypeError('The "options" argument must be of type object.', options);
  }
  const opts = options === undefined ? {} : { ...options };
  if (opts.offset !== undefined && typeof opts.offset !== 'number') {
    throw propertyValueError('offset', opts.offset);
  }
  if (opts.length !== undefined && typeof opts.length !== 'number') {
    throw propertyValueError('length', opts.length);
  }
  if (opts.statCheck !== undefined && typeof opts.statCheck !== 'function') {
    throw propertyValueError('statCheck', opts.statCheck);
  }
  return opts;
}

// node's prepareResponseHeadersObject (internal/http2/core.js): a null-prototype copy
// of the caller's object with ':status' coerced in, so a statCheck that rewrites
// 'content-length' lands on the copy -- which is also what sentHeaders reports,
// exactly where node's kSentHeaders has it. The generated date is left to hdr.block,
// whose sendDate rule is the one prepare applies too. Status validation happens here,
// before the payload-forbidden check -- measured: ':status': 700 throws
// ERR_HTTP2_STATUS_INVALID out of the call, and ':status': '700' is coerced to 700
// first. The array form passes through untouched (hdr.block handles pairs natively);
// node's for..in over an array copies the index keys into the copy and sends field
// names '0', '1', ... instead -- the junk shape the header comment documents.
function prepareResponseHeadersObject(headersParam) {
  if (headersParam !== undefined &&
      (headersParam === null || typeof headersParam !== 'object')) {
    throw argTypeError('The "headers" argument must be an instance of Array or Object.',
      headersParam);
  }
  if (Array.isArray(headersParam)) {
    return { headers: headersParam, statusCode: 0, isArray: true };
  }
  const copy = { __proto__: null };
  let statusCode;
  // node guards the copy on oldHeaders !== null && !== undefined (core.js): a missing
  // headers argument is a valid response -- respondWithFD(fd) with no fields -- and an
  // unguarded Object.keys would turn it into a session-killing TypeError
  if (headersParam !== undefined && headersParam !== null) {
    const keys = Object.keys(headersParam);
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      copy[key] = headersParam[key];
      if (key === ':status') {
        statusCode = copy[key];
      }
    }
    copy[hdr.KSensitiveHeaders] = headersParam[hdr.KSensitiveHeaders];
  }
  statusCode = copy[':status'] = (statusCode | 0) || 200;
  if (statusCode < 200 || statusCode > 599) {
    throw errorOf('ERR_HTTP2_STATUS_INVALID', `Invalid status code: ${statusCode}`);
  }
  const neverIndex = copy[hdr.KSensitiveHeaders];
  if (neverIndex !== undefined && !Array.isArray(neverIndex)) {
    const err = new TypeError(
      `The property 'headers[http2.neverIndex]' is invalid. Received ${inspect(neverIndex)}`);
    err.code = 'ERR_INVALID_ARG_VALUE';
    throw err;
  }
  return { headers: copy, statusCode, isArray: false };
}

const {
  ST_OPEN, ST_HALF_CLOSED_LOCAL, ST_HALF_CLOSED_REMOTE, ST_CLOSED, NO_ERROR, PROTOCOL_ERROR,
  INTERNAL_ERROR
} = frames;

class Http2Stream extends Stream {
  constructor(session, id) {
    super();
    this.id = id;
    this.session = session;
    this.rstCode = 0;
    // What respond() put on the wire, for the `sentHeaders` getter below: the caller's map
    // plus the two values the encoder resolved. node keeps the same three pieces
    // (kRawHeaders / kSentHeaders, internal/http2/core.js:2225).
    this._sentSrc = undefined;
    this._sentStatus = 0;
    this._sentDate = undefined;
    this._sentView = undefined;
    this.sentInfoHeaders = [];
    this.sentTrailers = undefined;
    this.headersSent = false;
    this.pending = true;
    this.aborted = false;
    this.endAfterHeaders = false;
    this.allowHalfOpen = false;
    this._state = {
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
    this._corked = 0; // Writable cork count -- a stub, not a write buffer (see cork()/uncork())
    this._gracefulClose = false; // both halves closed on their own terms (_closeNow)
    // node's closed/destroyed flip only when nghttp2 has actually closed the stream -- for
    // a local end() that is when the send loop has carried the END_STREAM out, a tick after
    // the call. h2lean flips its state inside the call (see _closeNow), so the flag marks
    // the node-equivalent moment: the graceful close's event delivery. pushAllowed reads
    // it to keep the same-turn window node keeps open.
    this._closeDelivered = false;
    this._waitForTrailers = false;
    // node's `state.trailersReady` (internal/http2/core.js onStreamTrailers): the
    // permission to call sendTrailers(), granted by the 'wantTrailers' callback and
    // NOT by the waitForTrailers option. node keeps the two separate -- calling
    // sendTrailers() after respond({waitForTrailers:true}) but before 'wantTrailers'
    // has fired throws ERR_HTTP2_TRAILERS_NOT_READY, measured live.
    this._trailersReady = false;
    this._pendingTrailers = null; // trailer block waiting behind a queued body
    this._pendingTrailersBound = 0; // ...and its outbound estimate, checked when it drains
    // A refused outbound HEADERS (session._refuseBlock) left the wire a step behind the
    // application: no response HEADERS means no DATA may follow, and no END_STREAM means
    // the stream must not close gracefully. An informational block sets neither.
    // A refused PUSH_PROMISE sets it on the PROMISED stream at construction, before the
    // pushStream callback runs: nghttp2 has already closed that stream, so the respond()
    // and end() the callback makes submit into nothing (session._refusePush).
    this._blockRefused = false;
    this._destroyed = false;
    // The compat layer's handler for this stream's events, or null when the
    // application uses the stream API. See emit() below.
    this._compatDispatch = null;
    // node's `_writableState.ending`: the writable side has been told to end, whether
    // or not the last octet has been accepted yet. It is what decides 'aborted' in
    // node's closeStream (`if (!ending) emit('aborted')`), and the file paths set it
    // EARLY -- node's processRespondWithFD calls self.end() before the pump starts --
    // which is why a respondWithFD interrupted by a destroy reports no 'aborted'
    // (measured: only 'close' with writableFinished false). _state.localClose does not
    // cover that: it stays 0 until the pump reaches EOF.
    this._ending = false;
    // The writable-side finish state. 'finish' reports that every octet the
    // application handed to this stream (body writes or a file pump) has been
    // accepted by the session; writableFinished reads the flag set right
    // before the event. Both stay false for a stream destroyed before
    // completion (measured on v26.12.1: an abort after respondWithFD reports
    // 'close' with writableFinished false and no 'finish').
    this._writableFinished = false;
    this._finishQueued = false;
  }

  get destroyed() {
    return this._destroyed;
  }
  get closed() {
    return this._state.state === ST_CLOSED;
  }
  // node's public state getter answers {} once the stream is destroyed
  // (internal/http2/core.js:2304, `if (this.destroyed || id === undefined)`);
  // internal code reads _state directly, the way node reads kState.
  get state() {
    return this._destroyed ? {} : this._state;
  }

  // node's sentHeaders is the message that went out, not the object respond() was given:
  // a null-prototype map carrying the coerced ':status' number and the `date` the stack
  // generated (prepareResponseHeadersObject, internal/http2/core.js:2677-2708). Built on
  // the first read and kept, so repeated reads give one object the way node's memoized
  // kSentHeaders does, and a response nobody inspects pays no copy. A pushed stream's map
  // is a request, not a response, and pushStream() fills it directly.
  get sentHeaders() {
    if (this._sentView !== undefined) {
      return this._sentView;
    }
    if (this._sentSrc === undefined) {
      return undefined;
    }
    this._sentView = hdr.responseView(this._sentSrc, this._sentStatus, this._sentDate);
    return this._sentView;
  }

  respond(headers, options) {
    // node guards the stream's life first (internal/http2/core.js respond():
    // `if (this.destroyed || this.closed) throw new ERR_HTTP2_INVALID_STREAM()`),
    // then the one-response rule. Neither throws emits 'error' on the stream --
    // measured on v26.12.1: a second respond() throws ERR_HTTP2_HEADERS_SENT
    // ("Response has already been initiated.") with no 'error' event. An emit here
    // throws out of a stream that has no 'error' listener, which turns the
    // application's own throw into an uncaught exception node never produces.
    if (this._destroyed || this.closed) {
      throw errorOf('ERR_HTTP2_INVALID_STREAM', 'The stream has been destroyed');
    }
    if (this.headersSent) {
      throw errorOf('ERR_HTTP2_HEADERS_SENT', 'Response has already been initiated.');
    }
    // respond() with no headers is legal (node's prepareResponseHeaders defaults
    // the block to {}; the upstream HEAD-push test answers a push that way).
    if (headers === undefined) {
      headers = {};
    }
    const opts = options || {};
    // node closes the writable itself for a HEAD request (respond(),
    // internal/http2/core.js: `this.headRequest === true -> this.end()`), and a
    // pushed stream whose end() already ran answers with END_STREAM on the
    // HEADERS. Measured on the wire against live node: node puts
    // HEADERS/ES/EH and no DATA for both shapes; h2lean used to leave a regular
    // HEAD answer open and send the body, which nghttp2 reads as a protocol error.
    const head = this._requestHeaders !== undefined && this._requestHeaders[':method'] === 'HEAD';
    // node puts a `date` on every response of this API too: prepareResponseHeadersObject()
    // (internal/http2/core.js:2702) adds one unless the application set one itself or
    // `options.sendDate` says otherwise -- and its test is `sendDate == null || sendDate`,
    // so the default is on and only an explicit false (or any other falsy value) turns it
    // off. Measured against a live node server: respond({':status':200}) carries a date,
    // respond({':status':204}) and 304 do too, and sendDate:false removes it.
    const sendDate = opts.sendDate == null ? true : !!opts.sendDate;
    const block = hdr.block(headers, opts.sensitiveHeaders,
      { sendDate }, this.session);
    // Read the encoder's result slots before anything else can build a block: they are
    // what `sentHeaders` reports, so they have to be this response's, not a later one's.
    this._sentSrc = headers;
    this._sentStatus = hdr.sent.status;
    this._sentDate = hdr.sent.date;
    const bound = hdr.sent.bound;
    this.headersSent = true;
    this.pending = false;
    this._waitForTrailers = !!opts.waitForTrailers;
    // node does not infer END_STREAM from the request having ended: the local
    // side only closes when the application says so -- except for the two shapes
    // above, where node's own end() has already run or runs inside respond().
    const empty = opts.endStream === true || head || this._endQueued;
    if (this._blockRefused || this.session.sendHeaders(this.id, block, empty, bound)) {
      // The response HEADERS never reached the wire, so the stream never opened for a
      // body: nghttp2's response-DATA predicate requires NGHTTP2_STREAM_OPENED
      // (nghttp2_session_predicate_data_send, nghttp2_session.c), which only a sent
      // response HEADERS reaches. Measured on v26.12.1: the wire carries RST_STREAM(6)
      // and the GOAWAYs -- no HEADERS and no DATA, even for end('hello').
      this._blockRefused = true;
    }
    if (empty) {
      // This response has already carried END_STREAM, and nothing may follow it on this
      // stream: RFC 9113 6.1 puts a trailing HEADERS *before* the close, and node shows
      // what that means for this combination -- respond({waitForTrailers:true,
      // endStream:true}) puts a single HEADERS/ES/EH on the wire and no trailer block.
      // So the outstanding request for trailers is dropped instead of answered after the
      // stream is closed, and 'wantTrailers' is not emitted for a stream that cannot send.
      this._waitForTrailers = false;
      this._ending = true;
      this._endLocalSide();
    }
    return this;
  }

  // --- file responses (respondWithFD / respondWithFile) ---
  //
  // The step order below is node's respondWithFD (internal/http2/core.js), each guard
  // measured against a live v26.12.1: stream guards, the options copy and its
  // offset/length/statCheck checks, the waitForTrailers flag (before the fd is
  // validated -- an invalid fd with waitForTrailers set still marks the stream for
  // trailers), the fd argument itself, ownsFd = false, the prepared header copy, and
  // the payload-forbidden statuses (204/205/304 and a HEAD request). respondWithFile
  // is the same list with fs.open in front, ownsFd = true, and its own
  // non-regular-file rules.
  //
  // A FileHandle instance is accepted wherever node accepts one; `this.ownsFd` is set
  // unconditionally on each path -- node does the same, so an application that pre-set
  // stream.ownsFd before respondWithFD has it clobbered there.

  respondWithFD(fd, headers, options) {
    if (this._destroyed || this.closed) {
      throw errorOf('ERR_HTTP2_INVALID_STREAM', 'The stream has been destroyed');
    }
    if (this.headersSent) {
      throw errorOf('ERR_HTTP2_HEADERS_SENT', 'Response has already been initiated.');
    }
    const opts = fdOptions(options);
    if (opts.waitForTrailers) {
      this._waitForTrailers = true;
    }
    if (isFileHandle(fd)) {
      fd = fd.fd;
    } else if (typeof fd !== 'number') {
      throw argTypeError(
        'The "fd" argument must be of type number or an instance of FileHandle.', fd);
    }
    this.ownsFd = false;
    const sendDate = opts.sendDate == null ? true : !!opts.sendDate;
    const { headers: prepared, statusCode } = prepareResponseHeadersObject(headers);
    const head =
      this._requestHeaders !== undefined && this._requestHeaders[':method'] === 'HEAD';
    if (statusCode === 204 || statusCode === 205 || statusCode === 304 || head) {
      throw errorOf('ERR_HTTP2_PAYLOAD_FORBIDDEN',
        `Responses with ${statusCode} status must not have a payload`);
    }
    if (opts.statCheck !== undefined) {
      // node's doSendFD: the statCheck sees (stat, headers-copy, statOptions) with
      // statOptions defaulted to { offset: 0, length: -1 } -- on this path node never
      // injects 'content-length' and never checks that the fd is a regular file; both
      // are the application's job through the statCheck callback (which is exactly
      // what upstream's test-http2-respond-file-fd-range.js does). Returning false
      // cancels without touching the descriptor: on this path the application owns it.
      fs.fstat(fd, (err, stat) => {
        if (err) {
          this.destroy(err);
          return;
        }
        if (this._destroyed || this.closed) {
          this.destroy(errorOf('ERR_HTTP2_INVALID_STREAM', 'The stream has been destroyed'));
          return;
        }
        const statOptions = {
          offset: opts.offset !== undefined ? opts.offset : 0,
          length: opts.length !== undefined ? opts.length : -1
        };
        if (opts.statCheck.call(this, stat, prepared, statOptions) === false ||
            this.headersSent) {
          return;
        }
        this._processRespondWithFD(fd, prepared, statOptions.offset | 0,
          statOptions.length | 0, sendDate);
      });
      return;
    }
    this._processRespondWithFD(fd, prepared, opts.offset, opts.length, sendDate);
  }

  respondWithFile(path, headers, options) {
    if (this._destroyed || this.closed) {
      throw errorOf('ERR_HTTP2_INVALID_STREAM', 'The stream has been destroyed');
    }
    if (this.headersSent) {
      throw errorOf('ERR_HTTP2_HEADERS_SENT', 'Response has already been initiated.');
    }
    const opts = fdOptions(options);
    if (opts.waitForTrailers) {
      this._waitForTrailers = true;
    }
    this.ownsFd = true;
    const sendDate = opts.sendDate == null ? true : !!opts.sendDate;
    const { headers: prepared, statusCode, isArray } =
      prepareResponseHeadersObject(headers);
    const head =
      this._requestHeaders !== undefined && this._requestHeaders[':method'] === 'HEAD';
    if (statusCode === 204 || statusCode === 205 || statusCode === 304 || head) {
      throw errorOf('ERR_HTTP2_PAYLOAD_FORBIDDEN',
        `Responses with ${statusCode} status must not have a payload`);
    }
    fs.open(path, 'r', (err, fd) => {
      const onError = typeof opts.onError === 'function' ? opts.onError : null;
      if (err) {
        if (onError) {
          onError(err);
        } else {
          this.destroy(err);
        }
        return;
      }
      if (this._destroyed || this.closed) {
        fs.close(fd, noopClose);
        return;
      }
      fs.fstat(fd, (err2, stat) => {
        if (err2) {
          fs.close(fd, noopClose);
          if (onError) {
            onError(err2);
          } else {
            this.destroy(err2);
          }
          return;
        }
        // node's doSendFileFD: for a non-regular file, any seek option at all (or a
        // directory) is refused -- with the descriptor closed either way, and the
        // error routed through options.onError when the application gave one. A
        // non-regular file that survives the check gets node's unseekable sentinel
        // (-1, -1) in the copy; note node's `!== undefined || > 0` pair, kept as
        // measured.
        if (!stat.isFile()) {
          const isDirectory = stat.isDirectory();
          if ((opts.offset !== undefined || opts.offset > 0) ||
              (opts.length !== undefined || opts.length >= 0) || isDirectory) {
            fs.close(fd, noopClose);
            const e = isDirectory
              ? errorOf('ERR_HTTP2_SEND_FILE', 'Directories cannot be sent')
              : errorOf('ERR_HTTP2_SEND_FILE_NOSEEK',
                'Offset or length can only be specified for regular files');
            if (onError) {
              onError(e);
            } else {
              this.destroy(e);
            }
            return;
          }
          opts.offset = -1;
          opts.length = -1;
        }
        if (this._destroyed || this.closed) {
          fs.close(fd, noopClose);
          this.destroy(errorOf('ERR_HTTP2_INVALID_STREAM', 'The stream has been destroyed'));
          return;
        }
        const statOptions = {
          offset: opts.offset !== undefined ? opts.offset : 0,
          length: opts.length !== undefined ? opts.length : -1
        };
        if ((typeof opts.statCheck === 'function' &&
             opts.statCheck.call(this, stat, prepared) === false) || this.headersSent) {
          // on this path the stack owns the descriptor: a cancelled statCheck closes it
          fs.close(fd, noopClose);
          return;
        }
        if (stat.isFile()) {
          statOptions.length = statOptions.length < 0
            ? stat.size - (+statOptions.offset)
            : Math.min(stat.size - (+statOptions.offset), statOptions.length);
          if (!isArray) {
            prepared['content-length'] = statOptions.length;
          }
        }
        // node passes options.offset (post-mutation), not statOptions.offset
        this._processRespondWithFD(fd, prepared, opts.offset | 0,
          statOptions.length | 0, sendDate);
      });
    });
  }

  // node's processRespondWithFD: the HEADERS_SENT flag goes up BEFORE the fields are
  // validated, so a bad field name leaves headersSent true and takes the destroy path
  // below -- measured, the call does not throw. node also calls self.end() here --
  // the writable is "ending" from this turn on, which is why an interrupted file
  // pipe reports no 'aborted' -- while the END_STREAM itself waits for the pump's
  // EOF. The prepared copy is what sentHeaders reports (node's kSentHeaders = the
  // copy), and the encoding error and a refused block both leave the descriptor
  // with its owner.
  _processRespondWithFD(fd, prepared, offset, length, sendDate) {
    this.headersSent = true;
    this.pending = false;
    this._ending = true;
    let block;
    try {
      block = hdr.block(prepared, undefined, { sendDate }, this.session);
    } catch (err) {
      if (this.ownsFd) {
        fs.close(fd, noopClose);
      }
      this.destroy(err);
      return;
    }
    this._sentSrc = prepared;
    this._sentStatus = hdr.sent.status;
    this._sentDate = hdr.sent.date;
    const bound = hdr.sent.bound;
    if (this._blockRefused || this.session.sendHeaders(this.id, block, false, bound)) {
      this._blockRefused = true;
      // node has already called self.end() by this point (processRespondWithFD ends
      // the writable before the wire write), so the half-close happens even here.
      this._endLocalSide();
      return;
    }
    this._startFilePipe(fd, offset, length);
  }

  // node's startFilePipe: read the descriptor in chunks and hand each to the writable
  // side, end at EOF, honour the write path's backpressure; teardown closes an owned
  // descriptor (node's onFileUnpipe), and a read error other than EOF raises
  // NGHTTP2_INTERNAL_ERROR on the stream -- the client then reports
  // ERR_HTTP2_STREAM_ERROR with that rstCode, the shape upstream's
  // test-http2-respond-file-fd-invalid.js pins. The reads go through write()/end(),
  // so credit, the outbound batch and the session hold behave exactly as for a body
  // written by hand.
  _startFilePipe(fd, offset, length) {
    const pump = {
      remaining: length === undefined || length < 0 ? Infinity : length,
      position: offset === undefined || offset < 0 ? -1 : offset,
      stopped: false
    };
    this.once('close', () => {
      pump.stopped = true;
      if (this.ownsFd) {
        fs.close(fd, noopClose);
      }
    });
    const step = () => {
      if (pump.stopped || this._destroyed) {
        return;
      }
      const want = Math.min(FILE_CHUNK, pump.remaining);
      const buf = Buffer.allocUnsafe(want);
      const pos = pump.position < 0 ? null : pump.position;
      fs.read(fd, buf, 0, want, pos, (err, n) => {
        if (pump.stopped || this._destroyed) {
          return;
        }
        if (err && err.code !== 'EOF') {
          this.destroy(streamErrorOf(frames.INTERNAL_ERROR));
          return;
        }
        if (n === 0) {
          this._pumpEnd();
          return;
        }
        pump.remaining -= n;
        if (pump.position >= 0) {
          pump.position += n;
        }
        const view = n === want ? buf : buf.subarray(0, n);
        if (this.write(view)) {
          if (pump.remaining > 0) {
            step();
          } else {
            this._pumpEnd();
          }
        } else {
          this.once('drain', step);
        }
      });
    };
    step();
  }

  // The file pump's EOF. node's pump never runs Writable.end: StreamPipe writes into
  // the nghttp2 handle directly (core.js startFilePipe), the writable side was closed
  // in processRespondWithFD (`self._final = null; self.end()`) and the application's
  // end() is the no-op Writable.end() is on an ended stream. h2lean's pump shares the
  // application's write()/sendData path, so its EOF mirrors end()'s body without the
  // ended-guard, and the same waitForTrailers split applies.
  _pumpEnd() {
    const oweTrailers = this._waitForTrailers;
    this.session.sendData(this, EMPTY, !oweTrailers);
    if (oweTrailers) {
      this._endLocalSide();
    }
  }

  write(chunk, encoding, cb) {
    if (typeof encoding === 'function') {
      cb = encoding; encoding = undefined;
    }
    // The response starts itself if the application never called respond(). Node does this on
    // the first WRITE of a server stream, not on the first end(): measured, `write('x')` alone
    // puts HEADERS{:status 200} on the wire ahead of the DATA. Doing it here as well is what
    // keeps DATA from arriving on a stream that has no response headers yet -- a peer that sees
    // DATA first has nothing to attach it to, and nghttp2 answers that with a stream error.
    // The started response may itself end the writable (a HEAD request, see respond()),
    // so the after-end check below runs after this, not before.
    if (!this.headersSent && !this._endQueued && this._state.localClose !== 1) {
      this.respond({ ':status': 200 });
    }
    if (this._endQueued || this._state.localClose === 1) {
      // node: Writable's _write answers ERR_STREAM_WRITE_AFTER_END ("write after
      // end") through the callback and errorOrDestroy -- which emits 'error' and
      // destroys a LIVE stream, but is a no-op on one already destroyed
      // (internal/streams/writable.js; measured v26.12.1, pinned by
      // test/upstream-push.test.js, session-destroy).
      const err = errorOf('ERR_STREAM_WRITE_AFTER_END', 'write after end');
      if (cb) {
        process.nextTick(cb, err);
      }
      if (!this._destroyed) {
        this.destroy(err);
      } else if (this._gracefulClose) {
        // node's close is asynchronous, so a write in the same turn as the
        // respond()/end() that finished the stream still hits a LIVE writable:
        // errorOrDestroy emits 'error' (measured on live node: the HEAD-push shape,
        // respond() then write()). h2lean closes synchronously, so
        // the event is owed here instead. A reset or a session destroy is not
        // this shape -- there node's errorOrDestroy is a no-op and no 'error'
        // goes out (the session-destroy case of test/upstream-push.test.js).
        process.nextTick(emitNT, this, 'error', err);
      }
      return false;
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
    if (this._endQueued || this._ending || this._state.localClose === 1) {
      // node: Writable.end() on an ended writable is a no-op that still answers
      // the callback (internal/streams/writable.js end() -> writableEnded checks;
      // measured v26.12.1: respond() on a HEAD request ends the writable itself,
      // and the application's end() after it neither errors nor destroys).
      // The same state is what respondWithFD leaves: node's processRespondWithFD
      // closes the writable before the pump starts (`self._final = null; self.end()`,
      // core.js), so the application's end() after it is the no-op and the pump's
      // EOF goes out through the pipe, not through end(). h2lean's pump reaches for
      // _pumpEnd() for exactly that reason; write() after end IS the error -- see
      // write() above.
      if (cb) {
        process.nextTick(cb);
      }
      return this;
    }
    // node's Writable.end flips _writableState.ending here, before anything is sent
    // -- even when the body is still parked on window credit. 'aborted' reads it.
    this._ending = true;
    if (!this.headersSent) {
      // A body takes the auto-respond of write() above; an empty end() is where h2lean and node
      // part company, and the split is written down in AGENTS.md section 10: node puts NOTHING on
      // the wire for `end()` before respond() -- no HEADERS, no DATA, no END_STREAM, and
      // headersSent stays false -- while h2lean answers a plain 200 rather than leave a stream
      // that no peer will ever see an answer on.
      this.respond({ ':status': 200 });
    }
    if (this._state.localClose === 1) {
      // the auto-respond above already carried END_STREAM (a HEAD request):
      // there is no body to send, RFC 9113 8.1 forbids one on a HEAD answer
      if (cb) {
        process.nextTick(cb);
      }
      return this;
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

  // Writable-side corking, as a counting stub. node's Http2Stream inherits Writable.cork,
  // which buffers write()/end() and defers the DATA to the wire until corked reaches 0;
  // h2lean's write() sends straight through (the per-read socket batch already coalesces a
  // synchronous burst). Only the count is observable here -- the cross-tick deferral is a
  // KNOWN DIVERGENCE written up in the header and pinned in test/cork-parity.test.js.
  // cork()/uncork() return undefined like Writable's, and uncork() floors at 0 the way
  // Writable's does (internal/streams/writable.js: `if (state.corked) state.corked--`).
  cork() {
    this._corked++;
  }

  uncork() {
    if (this._corked > 0) {
      this._corked--;
    }
  }

  get writableCorked() {
    return this._corked;
  }

  get writableHighWaterMark() {
    return WRITABLE_HWM;
  }

  get writableFinished() {
    return this._writableFinished;
  }

  sendTrailers(trailers) {
    // node's three guards, in node's order, and all three as synchronous throws --
    // not emitted errors (internal/http2/core.js Http2Stream.sendTrailers). Measured
    // live: each of the three messages below is what a node stream raises in the
    // matching case, and the case that decides most is the third -- the permission is
    // `state.trailersReady`, granted by the 'wantTrailers' callback, so a call that
    // arrives after respond({waitForTrailers:true}) but before that callback throws
    // rather than quietly putting a trailer block on the wire.
    if (this._destroyed || this.closed) {
      throw errorOf('ERR_HTTP2_INVALID_STREAM', 'The stream has been destroyed');
    }
    if (this.sentTrailers) {
      throw errorOf('ERR_HTTP2_TRAILERS_ALREADY_SENT',
        'Trailing headers have already been sent');
    }
    if (!this._trailersReady) {
      throw errorOf('ERR_HTTP2_TRAILERS_NOT_READY',
        'Trailing headers cannot be sent until after the wantTrailers event is emitted');
    }
    const block = hdr.block(trailers, undefined, { trailer: true }, this.session);
    const bound = hdr.sent.bound;
    // node records a null-prototype copy, not the caller's map (`ObjectAssign({
    // __proto__: null }, headers)`, internal/http2/core.js sendTrailers -- the shared
    // base method, so the client stream records the same shape).
    this.sentTrailers = Object.assign({ __proto__: null }, trailers);
    // the trailers are out: the flag that permitted this call must not keep asking for
    // wantTrailers, whatever happens to the bytes below
    this._waitForTrailers = false;
    // An empty trailer block does not become a HEADERS frame. nghttp2 has nothing to
    // put in a fieldless trailing header section, so node lets the END_STREAM the body
    // withheld land on a zero-length DATA instead -- measured: HEADERS/EH, DATA len4
    // flags 0x0, DATA len0 flags 0x1. This is the shape of *every* compat response,
    // because node's (req,res) layer always asks for trailers (internal/http2/compat.js
    // kBeginSend passes waitForTrailers: true) and answers with the empty trailer map
    // when the application added none.
    const empty = block.length === 0;
    if (this._queued !== null) {
      // The body is still waiting for credit or for the socket, and this block has to
      // follow it: END_STREAM rides on the trailers, so a HEADERS sent now would outrun
      // DATA that has not gone out. Stash a copy (the block is a view into shared scratch;
      // the zero-length marker needs none) and let the session put it on the wire when the
      // queue drains.
      this._pendingTrailers = empty ? EMPTY : Buffer.from(block);
      this._pendingTrailersBound = bound;
      return this;
    }
    if (empty) {
      // sendData() frames the zero-length DATA and ends the local half with it.
      this.session.sendData(this, EMPTY, true);
      return this;
    }
    if (this.session.sendHeaders(this.id, block, true, bound)) {
      // The END_STREAM this block carried never went out, so the stream has not finished
      // on the wire and must not close on its own terms ahead of the deferred
      // FRAME_SIZE_ERROR close (session._refuseBlock).
      this._blockRefused = true;
    }
    // the local half closes for good now: the bytes that owed it are on the wire
    this._endLocalSide();
  }

  close(code = NO_ERROR, reason) {
    this._destroy(code, reason, true);
  }

  destroy(codeOrError, reason) {
    // node: an Error argument means NGHTTP2_INTERNAL_ERROR on the wire
    // (Http2Stream._destroy, internal/http2/core.js:2518); a number is the RST
    // code itself, and no argument at all is a graceful NO_ERROR close.
    const code = typeof codeOrError === 'number' ? codeOrError
      : (codeOrError != null ? INTERNAL_ERROR : NO_ERROR);
    this._destroy(code, reason, true, typeof codeOrError === 'object' ? codeOrError : null);
  }

  // An informational (1xx) HEADERS block, sent before the final response.
  // node: Http2Stream.additionalHeaders().
  additionalHeaders(headers) {
    // node: `if (this.destroyed || this.closed) throw new ERR_HTTP2_INVALID_STREAM()`
    // (internal/http2/core.js additionalHeaders()).
    if (this._destroyed || this.closed) {
      throw errorOf('ERR_HTTP2_INVALID_STREAM', 'The stream has been destroyed');
    }
    // `{ info: true }` is node's separate rule for these blocks: :status must be 100..199
    // (101 has its own error), there is no default status and no generated date.
    const block = hdr.block(headers, undefined, { info: true }, this.session);
    const bound = hdr.sent.bound;
    this.sentInfoHeaders.push(headers);
    this.session.sendHeaders(this.id, block, false, bound);
    return this;
  }

  priority() {
    // RFC 9218 deprecates the priority scheme; node still accepts the call but
    // refuses a destroyed stream (internal/http2/core.js:2617). Returning true
    // without reordering anything is the honest v1 answer. The DEP0194 warning
    // is attached below the class, byte-identical to node's (typo included).
    if (this._destroyed) {
      throw errorOf('ERR_HTTP2_INVALID_STREAM', 'The stream has been destroyed');
    }
    return true;
  }

  // node: ServerHttp2Stream.pushAllowed (internal/http2/core.js:2961).
  // Truthiness of remoteSettings.enablePush: the CLIENT's advertisement decides whether
  // the server may push. Measured on v26.12.1: a client with settings:{enablePush:false}
  // makes pushAllowed false and pushStream throw ERR_HTTP2_PUSH_DISABLED synchronously.
  get pushAllowed() {
    // node's ServerHttp2Stream.pushAllowed (core.js) reads the same terms, but node's
    // stream reaches `closed` only when nghttp2 has actually closed it -- for a local
    // end() that is the send-loop turn after the call, so a pushStream in the same turn
    // still sees a live stream (upstream's respond({})/end()/pushStream() shape,
    // test/upstream/respond-file-push.test.js). h2lean frames END_STREAM inside the call
    // and flips closed/destroyed there, a tick earlier than node, so a graceful close
    // whose event has not been delivered yet still counts as open. A peer reset has no
    // such window: node's nghttp2 closes that stream inside the read batch.
    if (this._destroyed) {
      return this._gracefulClose && !this._closeDelivered;
    }
    return !this.closed && !this.session.closed && !this.session.destroyed
      && !!this.session.remoteSettings.enablePush;
  }

  // node: ServerHttp2Stream.pushStream (internal/http2/core.js:2971).
  // Synchronous throws: ERR_HTTP2_PUSH_DISABLED, ERR_HTTP2_NESTED_PUSH.
  // Asynchronous errors (allocation, closed stream) go to callback via nextTick.
  // The pushed stream's readable side is pre-ended (a push has no request body);
  // the application answers it through the normal respond()/write()/end() path.
  pushStream(headers, options, callback) {
    if (!this.pushAllowed) {
      throw errorOf('ERR_HTTP2_PUSH_DISABLED', 'HTTP/2 client has disabled push streams');
    }
    if ((this.id & 1) === 0) {
      throw errorOf('ERR_HTTP2_NESTED_PUSH', 'Nested push is not allowed');
    }
    if (typeof options === 'function') {
      callback = options;
      options = undefined;
    }
    // node: validateFunction(callback, 'callback') -- the callback is mandatory,
    // checked before options/headers, and the error names what was received
    // (measured v26.12.1: pushStream(h, {}, 'callback') and pushStream(h) both
    // throw ERR_INVALID_ARG_TYPE; pinned by test/upstream-push.test.js).
    if (typeof callback !== 'function') {
      const received = callback === undefined ? 'Received undefined'
        : `Received type ${typeof callback}` +
          (typeof callback === 'string' ? ` ('${callback}')` : '');
      const err = new TypeError(`The "callback" argument must be of type function. ${received}`);
      err.code = 'ERR_INVALID_ARG_TYPE';
      throw err;
    }
    if (options === undefined || options === null) {
      options = {};
    }
    if (typeof options !== 'object' || Array.isArray(options)) {
      const err = new TypeError('The "options" argument must be of type object');
      err.code = 'ERR_INVALID_ARG_TYPE';
      throw err;
    }
    const endStream = !!options.endStream;

    // Defaults from the parent request (node: core.js pushStream fills :method GET,
    // :authority and :scheme from the request, :path '/'). The headers object is copied
    // into a null-proto map so the application's object is not mutated.
    const req = this._requestHeaders || {};
    const h = Object.assign(Object.create(null), headers);
    if (h[':method'] === undefined) {
      h[':method'] = 'GET';
    }
    if (h[':authority'] === undefined && req[':authority'] !== undefined) {
      h[':authority'] = req[':authority'];
    }
    if (h[':scheme'] === undefined && req[':scheme'] !== undefined) {
      h[':scheme'] = req[':scheme'];
    }
    if (h[':path'] === undefined) {
      h[':path'] = '/';
    }
    const isHead = h[':method'] === 'HEAD';

    // Allocate the promised id. Errors here are asynchronous (callback via nextTick),
    // matching node's nghttp2_submit_push_promise error path.
    let promisedId;
    try {
      promisedId = this.session._allocPushSid();
    } catch (err) {
      process.nextTick(callback, err);
      return;
    }

    // Validation errors are synchronous in node (buildNgHeaderString throws before
    // the frame is submitted). The id was already reserved; it stays consumed.
    const block = hdr.pushBlock(h, options.sensitiveHeaders, this.session);
    const refused = this.session.sendPushPromise(this.id, promisedId, block, hdr.sent.bound);

    // Create the pushed stream and register it. The readable side is pre-ended:
    // remoteClose=1 means _endLocalSide() takes the graceful close path (no RST),
    // and no inbound credit is ever charged for a request body that does not exist.
    const stream = new Http2Stream(this.session, promisedId);
    if (refused) {
      // nghttp2 closed the promised stream at prep time, before the callback below runs,
      // so the respond()/end() the application makes there submit into a closed stream
      // and reach no wire. The stream object is still handed over with no error -- node
      // creates it before the send loop too -- and its INTERNAL_ERROR close is the
      // deferred one session._refusePush schedules.
      stream._blockRefused = true;
    }
    // The pushed stream's map is a request: node fills kSentHeaders with the resolved
    // request map at pushStream() time (core.js:3038), so it is already final here and
    // the getter above hands it back unchanged.
    stream._sentView = h;
    stream._requestHeaders = h;
    this.session._streams.set(promisedId, stream);
    this.session._liveStreams++;
    stream._remoteEnded();

    if (endStream || isHead) {
      // node runs stream.end() here, but its Writable end only shuts the writable
      // half down -- the stream stays alive for the application's respond(), which
      // then carries END_STREAM (measured on live node: destroyed:false, closed:false
      // inside the callback). Marking the end instead of closing is
      // what keeps that shape; respond() reads _endQueued, write() refuses it.
      stream._endQueued = true;
    }
    process.nextTick(callback, null, stream, h, 0);
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
    if (this._state.remoteClose === 1) {
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
    this._state.remoteClose = 1;
    this.endAfterHeaders = true;
    // the readable side is done: 'end' after the last 'data', like a Readable.
    // No auto-destroy: the application still has to answer. node keeps the
    // writable side of an unanswered request open as well.
    if (this._state.localClose === 1 && !this._waitForTrailers) {
      // The local half is only *finished* once END_STREAM left on the wire. A stream that
      // still owes a trailer block owes that END_STREAM too -- the body DATA was framed
      // without it (sendTrailers() puts the close on the trailer block, or on an empty DATA
      // when the block has no fields) -- so closing here would leave the response
      // unanswered for the peer and drop the owed frame. node's rule is the same and lives
      // in the writable shutdown, not in the read path: a stream with
      // STREAM_FLAGS_HAS_TRAILERS set is not shut down early
      // (internal/http2/core.js, `Trailers are pending, so the writable side cannot be
      // shut down early`). Measured: a compat response that ends with no body against an
      // unread request put HEADERS then a zero-length DATA/END_STREAM on node's wire, with
      // the stream close after both; ending here sent the HEADERS only and the client
      // waited for an answer that never came.
      this._closeNow();
      process.nextTick(emitEndCloseNT, this);
      return;
    }
    if (this._state.state === ST_OPEN) {
      this._state.state = ST_HALF_CLOSED_REMOTE;
    }
    if (!this._isConsuming()) {
      // Nothing has read the buffered bytes yet, so 'end' must not overtake them:
      // a reader that attaches later is owed the whole sequence, EOF included.
      this._endPending = true;
      return;
    }
    process.nextTick(emitNT, this, 'end');
  }

  // The writable-side 'finish' -- node's Writable fires it once every write has been
  // accepted by the stream machinery and the writable half has ended; a destroy in
  // the same turn as the end() cancels it (upstream's test-http2-respond-with-fd-finish.js
  // pins both shapes: finish, then close with writableFinished true on a clean send;
  // close with writableFinished false and no 'finish' on an abort). The tick is also
  // what keeps the event behind 'wantTrailers' -- measured on v26.12.1, 'finish'
  // precedes 'wantTrailers' on the trailers shape. A body still queued on window
  // credit re-runs _endLocalSide when the queue drains (session._release), which is
  // where the second scheduling comes from.
  _scheduleFinish() {
    if (this._writableFinished || this._finishQueued) {
      return;
    }
    this._finishQueued = true;
    process.nextTick(() => {
      this._finishQueued = false;
      // not _destroyed here: a stream whose both halves close in the same turn the
      // writable ends flips ST_CLOSED synchronously (_closeNow) while the close EVENTS
      // are deferred -- and node's order for that shape is 'finish' before 'close',
      // with writableFinished true at the close (measured). The only cancel is a
      // writable that never drained: octets still parked on window credit.
      if (this._writableFinished || this._queued !== null) {
        return;
      }
      this._writableFinished = true;
      this.emit('finish');
    });
  }

  _endLocalSide() {
    const first = this._state.localClose !== 1;
    this._state.localClose = 1;
    this._endQueued = false;
    if (this._blockRefused) {
      // The half-close is the application's, and node records it whether or not the
      // octets went out (`_writableState.ending` is what decides 'aborted'). The close
      // machinery below must not run: it would finish the stream with NO_ERROR here,
      // synchronously, ahead of the deferred FRAME_SIZE_ERROR close node's onFrameError
      // schedules -- measured, node reports rstCode 6 for this shape and h2lean reported 0.
      return;
    }
    if (this._queued === null) {
      // every octet handed over has been accepted by the session: node's condition
      // for the writable 'finish'
      this._scheduleFinish();
    }
    if (this._state.state === ST_OPEN) {
      this._state.state = ST_HALF_CLOSED_LOCAL;
    }
    if (this._waitForTrailers) {
      // one shot: a later end() must not ask for trailers a second time. The flag
      // itself stays set, because sendTrailers() is only allowed while it is set.
      if (first) {
        process.nextTick(wantTrailersNT, this);
      }
      return;
    }
    if (this._state.remoteClose === 1) {
      this._closeIfBothDone();
    } else {
      // the response is over and the peer's half is not: this is the shape node
      // closes for the application (see _maybeAutoClose). A stream the peer has
      // already ended takes the graceful close above, which is also what node's
      // wire shows there -- no RST, because nghttp2 refuses to reset what both
      // halves have closed (tools/probe-stream-teardown.js, 'request-ended').
      this._maybeAutoClose();
    }
  }

  // node's Http2Stream[kMaybeDestroy] (internal/http2/core.js): a response the
  // server session has finished, with trailers neither requested nor sent
  // (STREAM_FLAGS_HAS_TRAILERS) and a request side nobody ever started to consume
  // (`!state.didRead && readableFlowing === null`), is closed by the session
  // itself -- `setImmediate(callStreamClose)` off the writable side's 'finish',
  // and closeStream submits RST_STREAM(NO_ERROR). Measured on v26.12.1
  // (tools/probe-stream-teardown.js, 'answer-only'): HEADERS, DATA, then
  // RST_STREAM(NO_ERROR) on the wire and the stream destroyed -- and the
  // boundaries with it: 'resume-then-answer' shows that touching the readable
  // side stops the auto-close, and 'request-ended' that a stream the peer already
  // ended never gets one. Without it a peer that sends HEADERS without END_STREAM
  // and no DATA holds a stream and a _liveStreams slot until the idle timeout,
  // and a closing session waits for it forever (AGENTS.md 10, item 2).
  // `_consumeState === 0` is this file's reading of `!didRead && flowing ===
  // null`: no reader attached and no pause()/resume() called -- either has
  // touched the request, and node stops there too.
  _maybeAutoClose() {
    if (this._destroyed || !this.headersSent || this._waitForTrailers ||
        this._consumeState !== 0) {
      return;
    }
    // node's own deferral: the RST goes out a turn after the response, and the
    // stream's 'close' with it (autoCloseNT above guards against a close() the
    // application made in between)
    setImmediate(autoCloseNT, this);
  }

  _closeIfBothDone() {
    if (this._destroyed || this._state.localClose !== 1 || this._state.remoteClose !== 1) {
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
    process.nextTick(emitCloseDeliveredNT, this);
  }

  _closeNow() {
    this._state.state = ST_CLOSED;
    this._destroyed = true;
    this._gracefulClose = true;
    this._pending = null;
    this.session._dropStream(this, false);
    // node drops the stream's session reference in _destroy (internal/http2/core.js:2546,
    // `this[kSession] = undefined`) -- which for node runs only when nghttp2 closes the
    // stream, a tick after the end() that queued this close. The drop therefore moves to
    // the close-delivery tick (emitEndCloseNT/emitCloseDeliveredNT), keeping the
    // same-turn window live for pushStream exactly as node's window is. The _destroy path
    // (a peer reset) keeps its own synchronous drop -- node's nghttp2 closes that stream
    // inside the read batch.
  }

  _destroy(code, reason, sendRst, err) {
    if (this._destroyed) {
      return;
    }
    this._destroyed = true;
    this.rstCode = code;
    // node's rule for 'aborted' is about the WRITABLE side: "if the writable side of
    // the Http2Stream is still open, emit the 'aborted' event" -- closeStream's
    // `const { ending } = stream._writableState; if (!ending) ...`. The same shapes
    // measured against a live node server: a peer that resets a stream whose response
    // never started and whose end() never ran reports 'aborted'; a stream whose
    // writable ended -- by end(), by a file pump (node's processRespondWithFD calls
    // self.end() before the pump) -- does not, even when the octets are still in
    // flight. _ending is the portable reading of `ending`; _state.localClose would
    // answer the file paths wrong.
    const responseOpen = !this._ending;
    this._state.localClose = 1;
    this._state.state = ST_CLOSED;
    if (sendRst && this.session._open) {
      this.session.sendRst(this.id, code, reason);
    }
    this._pending = null;
    this.session._dropStream(this, true);
    // The event is emitted synchronously -- before the 'end' that an already-queued
    // tick would otherwise deliver first. Marking the remote half closed keeps a
    // late END_STREAM from emitting 'end' after 'close'.
    this._state.remoteClose = 1;
    if (responseOpen) {
      this.aborted = true;
      this.emit('aborted');
    }
    // node clears the session reference before 'error'/'close' go out
    // (internal/http2/core.js:2546); 'aborted' above still sees it, the way
    // node's closeStream emits 'aborted' before _destroy runs.
    this.session = undefined;
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

// node wraps the whole method in util.deprecate, so the warning fires once per
// process with this exact message -- typo included, byte-identical to
// internal/http2/core.js:2620 (DEP0194).
Http2Stream.prototype.priority = deprecate(
  Http2Stream.prototype.priority,
  'http2Stream.priority is longer supported after priority signalling was deprecated in RFC 9113',
  'DEP0194');

function errorOf(code, msg) {
  const err = new Error(msg);
  err.code = code;
  return err;
}

// The error a stream reports when an RST_STREAM ends it, on either side. node's rule is
// one line (internal/http2/core.js:2543): `if (err == null && code !== NGHTTP2_NO_ERROR &&
// code !== NGHTTP2_CANCEL) err = new ERR_HTTP2_STREAM_ERROR(nameForErrorCode[code] || code)`.
// So a graceful reset (0) and a client abort (8) are NOT errors -- measured on a live node
// client and a live node server, both report only 'aborted'/'close' for those two codes --
// and every other code names itself in the message. h2lean used to answer every code with
// 'HTTP/2 stream error' on the client and with no error at all on the server.
function streamErrorOf(code) {
  if (code === frames.NO_ERROR || code === frames.CANCEL) {
    return null;
  }
  return errorOf('ERR_HTTP2_STREAM_ERROR',
    `Stream closed with error code ${frames.ERROR_NAMES[code] || code}`);
}

module.exports = { Http2Stream, errorOf, streamErrorOf, emitNT };
