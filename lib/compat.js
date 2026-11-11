// Compat layer: the http1-shaped (req, res) API over an HTTP/2 stream, matching
// the semantics of node:internal/http2/compat (that source was read through
// --expose-internals, and the frame sequence was confirmed against a live node
// server with tools/probe-wire.js).
//
// Deliberate differences from node, all of them loud rather than silent:
//   * req.pause()/read() only affect the local Readable. The flow-control window
//     is still replenished, because h2lean releases stream credit as the buffered
//     body is consumed and connection credit in half-window steps, neither of them
//     per read(). A paused request buffers in memory instead of slowing the peer down.
//   * res.cork()/uncork() are absent, not no-ops: h2lean already coalesces every
//     frame produced in one turn of the loop into a single write().
//   * res.write() returning false is honest backpressure, and 'drain' is emitted
//     when the send window reopens.
//   * createPushResponse() throws ERR_H2LEAN_NOT_IMPLEMENTED (no server push in
//     v1) instead of failing later with a protocol error.
//   * res 'finish' means the local half of the stream was closed, which for
//     h2lean happens as the frames are queued for the socket; node reports it
//     when the stream closes, which is later. A response the peer cut short
//     reports 'finish' on both stacks.
//   * the order of the teardown events differs: node reports the request's
//     events (its 'end', its 'aborted', its 'close') before the response's
//     ('finish', then 'close'), h2lean reports the response's first. Every
//     event fires on both stacks -- measured against a live node server over
//     the four shapes of bench/micro/probe-teardown-order.js, the sets are equal -- and
//     only their order within the teardown turn differs. One pair is not a
//     fixed difference at all: the request's 'end' against the 'aborted'
//     depends on whether the peer's END_STREAM and its reset arrive in one
//     read or in two, and the two stacks order that pair differently.
//   * the body is pulled, not pushed: nothing is read off the wire until the
//     application consumes it, so an unread upload applies backpressure to the
//     sender (node does the same) instead of buffering here.
//   * with the server option shareHeaders:true, req.headers is frozen and shared
//     between requests whose header block is identical, so req.url = ... and
//     req.method = ... throw instead of rewriting the request.
//   * the compat layer's handlers are not subscriptions on the stream: the stream
//     calls them through one dispatch function (lib/stream.js, emit()), so
//     stream.listenerCount('close') and friends report fewer listeners than they
//     do on node, whose compat layer subscribes to the same events
//     (internal/http2/compat, the stream.on() calls in the request and the
//     response constructor). Everything an application can observe about the
//     events themselves is unchanged: same handlers, same order, same arguments.
'use strict';

const { Readable, Stream } = require('node:stream');
const { emitNT } = require('./stream');
const { nullProto, trimName } = require('./headers');

function err(code, msg) {
  const e = new Error(msg);
  e.code = code;
  return e;
}

// node validates a header name before it normalises it. Every accessor that
// takes a name calls validateString(name, 'name') first (internal/http2/compat.js:
// setTrailer 581, getHeader 596, hasHeader 618, removeHeader 623, setHeader 639,
// appendHeader 663), and validateString throws ERR_INVALID_ARG_TYPE for anything
// that is not a string (internal/validators.js:165). Measured against a live node
// server (bench/micro/probe-nonname.js): the six accessors refuse a number with
//   The "name" argument must be of type string. Received type number (123)
// and accept the same name written as a string. Coercing the name instead --
// which is what this did -- was a silent divergence: the name the application
// meant and the name that reached the wire were not the same one, and the
// application was never told.
//
// The error is a TypeError with a code, like node's: ERR_INVALID_ARG_TYPE is
// declared as one (internal/errors.js:1413).
function checkName(name) {
  if (typeof name !== 'string') {
    const e = new TypeError(
      `The "name" argument must be of type string. Received ${describeType(name)}`);
    e.code = 'ERR_INVALID_ARG_TYPE';
    throw e;
  }
}

// node's wording for a value of the wrong type (internal/errors.js,
// determineSpecificType): the message is what an application reads when it
// catches the error, so it says what node says.
function describeType(value) {
  if (value === null) {
    return 'null';
  }
  if (value === undefined) {
    return 'undefined';
  }
  switch (typeof value) {
    case 'number':
      if (value === 0) {
        return 1 / value === -Infinity ? 'type number (-0)' : 'type number (0)';
      }
      return `type number (${value})`;
    case 'bigint': return `type bigint (${value}n)`;
    case 'boolean': return `type boolean (${value})`;
    case 'symbol': return `type symbol (${String(value)})`;
    case 'function': return `function ${value.name}`;
    case 'object': {
      const name = value.constructor?.name;
      return typeof name === 'string' && name !== '' ? `an instance of ${name}` : 'an instance of Object';
    }
    default: return `type ${typeof value}`;
  }
}

// node lets an application rewrite req.url / req.method. With shareHeaders the
// headers object belongs to every identical request on the connection, so the
// rewrite is refused loudly instead of silently corrupting another request.
function assertWritable(headers) {
  if (Object.isFrozen(headers)) {
    throw err('ERR_H2LEAN_FROZEN_HEADERS',
      'req.headers is frozen because the server was created with shareHeaders:true');
  }
}

// ---- request ------------------------------------------------------------------

// Module-level handlers, invoked with `this` bound to the Http2Stream, so no
// closure is allocated per request. The stream keeps the only reference to the
// request object, exactly like node keeps it under a symbol.
function onRequestData(chunk) {
  const req = this._request;
  if (req === undefined) {
    return;
  }
  // a Readable queues the chunk for an unknown amount of time, which is fine:
  // what the session delivers is either a view of a socket read or a buffer built
  // for that frame alone, and neither is recycled underneath us
  if (req.push(chunk)) {
    return;
  }
  // The application is not keeping up. node stops replenishing the window here;
  // doing the same costs the sender instead of our heap, so buffer nothing more
  // until _read() asks for it.
  this._pauseConsuming();
}

function onRequestEnd() {
  const req = this._request;
  if (req === undefined || req._ended) {
    return;
  }
  req._ended = true;
  req.push(null);
}

function onRequestError(error) {
  const req = this._request;
  if (req !== undefined) {
    req.destroy(error);
  }
}

function onRequestAborted() {
  const req = this._request;
  if (req === undefined || req._closed) {
    return;
  }
  req._aborted = true;
  req.emit('aborted');
}

function onRequestTrailers(trailers) {
  const req = this._request;
  if (req === undefined) {
    return;
  }
  req._trailers = trailers;
  req.emit('trailers', trailers);
}

function onRequestClose() {
  const req = this._request;
  if (req === undefined || req._closed) {
    return;
  }
  req._closed = true;
  if (!req._ended) {
    req._ended = true;
    req.push(null);
  }
  // node dumps a body nobody read when the stream closes: "If the user didn't
  // interact with incoming data and didn't pipe it, dump it for compatibility
  // with http1" (internal/http2/compat, onStreamCloseRequest). Without it the
  // request would never report 'end' at all: a Readable that nobody reads
  // never ends, and the EOF pushed above only reaches the application once
  // the Readable flows.
  if (!req._didRead) {
    req.resume();
  }
  // 'close' after 'end', so queue it behind the end the push above scheduled.
  // node emits it right here instead, so the two stacks differ in the order of
  // these two: see the difference list at the top of this file.
  process.nextTick(emitCloseNT, req);
}

function emitCloseNT(target) {
  target.emit('close');
}

function onRequestTimeout() {
  const req = this._request;
  if (req !== undefined && !req._closed) {
    req.emit('timeout');
  }
}

// The compat layer's handlers, called by the stream (lib/stream.js) when it emits
// an event, with the stream as `this`. The order is the order the subscriptions had:
// the request's handler before the response's for the three events both of them
// answer, and both before the application's own listeners.
function compatDispatch(name, arg) {
  switch (name) {
    case 'end':
      onRequestEnd.call(this, arg);
      break;
    case 'aborted':
      onRequestAborted.call(this, arg);
      // node's response handler for 'aborted' is a no-op -- "non-op for now"
      // (internal/http2/compat, onStreamAbortedResponse). The response is torn
      // down by the stream's 'close', which reports 'finish' and then 'close';
      // closing it here would report the 'close' without the 'finish' and
      // before the request's own 'close'.
      break;
    case 'trailers':
      onRequestTrailers.call(this, arg);
      break;
    case 'close':
      onRequestClose.call(this, arg);
      onResponseClose.call(this, arg);
      break;
    case 'timeout':
      onRequestTimeout.call(this, arg);
      onResponseTimeout.call(this, arg);
      break;
    case 'drain':
      onResponseDrain.call(this, arg);
      break;
  }
}

class Http2ServerRequest extends Readable {
  constructor(stream, headers, rawHeaders) {
    super({ autoDestroy: false });
    this._stream = stream;
    this._headers = headers;
    this._rawHeaders = rawHeaders === null ? [] : rawHeaders;
    this._trailers = { __proto__: null };
    this._rawTrailers = [];
    this._closed = false;
    this._ended = false;
    this._aborted = false;
    this._didRead = false;
    stream._request = this;
    // No 'data' listener here: see _read(). Attaching it eagerly would buffer
    // every unread upload in the Readable and would emit 'end' for bodies the
    // application never asked for, neither of which node does.
    //
    // The stream's other events are not subscribed to either: the stream calls
    // the compat layer's handlers through one dispatch function, which costs
    // one call per event instead of a subscription per event -- subscriptions
    // are an order of magnitude dearer per request (OPTIMIZATION.md item 5;
    // lib/stream.js, emit()). The 'error' handler stays a
    // subscription, because a stream the application never listens to must not
    // throw on an unhandled 'error' -- the subscription is what keeps that from
    // happening.
    stream._compatDispatch = compatDispatch;
    stream.on('error', onRequestError);
  }

  get aborted() {
    return this._aborted;
  }

  get complete() {
    return this._aborted || this.readableEnded || this._closed || this._stream.destroyed;
  }

  get stream() {
    return this._stream;
  }
  get headers() {
    return this._headers;
  }
  get rawHeaders() {
    return this._rawHeaders;
  }
  get trailers() {
    return this._trailers;
  }
  get rawTrailers() {
    return this._rawTrailers;
  }

  get httpVersionMajor() {
    return 2;
  }
  get httpVersionMinor() {
    return 0;
  }
  get httpVersion() {
    return '2.0';
  }

  // node hands out a Proxy over the stream pretending to be a socket. The real
  // socket is smaller, faster and answers the same questions (remoteAddress).
  get socket() {
    return this._stream.session.socket;
  }
  get connection() {
    return this.socket;
  }

  get method() {
    return this._headers[':method'];
  }
  set method(method) {
    if (typeof method !== 'string') {
      throw err('ERR_INVALID_ARG_TYPE', 'method must be a string');
    }
    if (method.trim() === '') {
      throw err('ERR_INVALID_ARG_VALUE', 'method must not be empty');
    }
    assertWritable(this._headers);
    this._headers[':method'] = method;
  }

  get authority() {
    const auth = this._headers[':authority'];
    if (auth !== undefined) {
      return auth;
    }
    const host = this._headers.host;
    return host === undefined ? undefined : String(host).replace(/:[\d]+$/, '');
  }

  get scheme() {
    return this._headers[':scheme'];
  }

  get url() {
    return this._headers[':path'];
  }
  set url(url) {
    assertWritable(this._headers);
    this._headers[':path'] = url;
  }

  setTimeout(msecs, callback) {
    if (!this._closed) {
      this._stream.setTimeout(msecs, callback);
    }
    return this;
  }

  // node's pattern, and the reason the compat layer gets real backpressure: the
  // stream only starts delivering when the application asks for data. Until then
  // no credit is released, so the sender is the one that waits.
  _read() {
    if (this._closed || this._ended) {
      return;
    }
    this._didRead = true;
    const stream = this._stream;
    // an 'end' that was deferred because nobody was reading is owed to us by the
    // replay below; anything else means EOF already happened and there is
    // nothing left to fetch
    const owedEnd = stream._endPending;
    if (stream.listenerCount('data') === 0) {
      stream.on('data', onRequestData);
    } else {
      stream._startConsuming();
    }
    if (!owedEnd && stream.state.remoteClose === 1) {
      this._ended = true;
      this.push(null);
    }
  }
}

// ---- response -----------------------------------------------------------------

function onResponseDrain() {
  const res = this._response;
  if (res !== undefined) {
    res.emit('drain');
  }
}

function onResponseClose() {
  const res = this._response;
  if (res === undefined || res._state.closed) {
    return;
  }
  res._state.closed = true;
  res.writable = false;
  // node emits 'finish' and 'close' together, when the stream closes
  // (internal/http2/compat, onStreamCloseResponse), so a response the peer cut
  // short still reports 'finish'. h2lean emits 'finish' when the application
  // ends the response, which is the same event one turn earlier; this covers
  // the case where the application never got that far.
  if (!res._state.finishEmitted) {
    res._state.finishEmitted = true;
    res.emit('finish');
  }
  res.emit('close');
}

function onResponseTimeout() {
  const res = this._response;
  if (res !== undefined && !res._state.closed) {
    res.emit('timeout');
  }
}

class Http2ServerResponse extends Stream {
  constructor(stream) {
    super();
    this._state = {
      closed: false,
      ending: false,
      destroyed: false,
      finishEmitted: false,
      sendDate: true,
      statusCode: 200,
      headRequest: stream._requestHeaders
        ? stream._requestHeaders[':method'] === 'HEAD'
        : false
    };
    // A fast null-prototype map, not a { __proto__: null } literal: the
    // response's headers are written once and read once per response, and the
    // literal's dictionary mode costs more for both (lib/headers.js, nullProto(),
    // OPTIMIZATION.md item 4). The trailers map stays a literal: it is empty in
    // every response that sends no trailers, and an empty fast map costs more to
    // make than the literal (OPTIMIZATION.md item 4).
    this._headers = nullProto();
    this._trailers = { __proto__: null };
    this._trailerNames = 0;
    this._stream = stream;
    stream._response = this;
    this.writable = true;
    this.req = stream._request;
    // The response's handlers are reached through the stream's dispatch, which
    // the request installed: see compatDispatch(). The request's handler runs
    // first for the events both of them answer, which is the order the two
    // sets of subscriptions had.
  }

  // Modules such as finalhandler only test the truthiness of this.
  get _header() {
    return this.headersSent;
  }

  get headersSent() {
    return this._stream.headersSent;
  }
  get stream() {
    return this._stream;
  }
  get socket() {
    return this._state.closed ? undefined : this._stream.session.socket;
  }
  get connection() {
    return this.socket;
  }

  get writableEnded() {
    return this._state.ending;
  }
  get finished() {
    return this._state.ending;
  }
  get writableFinished() {
    return this._stream.state.localClose === 1;
  }
  get writableNeedDrain() {
    return this._stream._queued !== null;
  }
  get writableLength() {
    const q = this._stream._queued;
    if (q === null) {
      return 0;
    }
    let n = 0;
    for (let i = 0; i < q.length; i++) {
      n += q[i].length;
    }
    return n;
  }

  get statusCode() {
    return this._state.statusCode;
  }
  set statusCode(code) {
    code |= 0;
    if (code >= 100 && code < 200) {
      throw err('ERR_HTTP2_INFO_STATUS_NOT_ALLOWED', 'informational status codes cannot be set on statusCode');
    }
    if (code < 100 || code > 599) {
      throw err('ERR_HTTP2_STATUS_INVALID', `invalid status code: ${code}`);
    }
    this._state.statusCode = code;
  }

  // HTTP/2 has no reason phrase; node warns and returns ''.
  get statusMessage() {
    process.emitWarning('HTTP/2 does not support a status message', 'Warning');
    return '';
  }
  set statusMessage(msg) {
    process.emitWarning('HTTP/2 does not support a status message', 'Warning');
  }

  get sendDate() {
    return this._state.sendDate;
  }
  set sendDate(bool) {
    this._state.sendDate = Boolean(bool);
  }

  getHeader(name) {
    checkName(name);
    return this._headers[trimName(name)];
  }
  getHeaderNames() {
    return Object.keys(this._headers);
  }
  getHeaders() {
    // The copy the application gets is a fast map as well, for the same
    // reason: it is read once per call and handed to code that reads it.
    const out = nullProto();
    const source = this._headers;
    for (const key of Object.keys(source)) {
      out[key] = source[key];
    }
    return out;
  }
  hasHeader(name) {
    checkName(name);
    return Object.hasOwn(this._headers, trimName(name));
  }

  removeHeader(name) {
    checkName(name);
    const key = trimName(name);
    if (this._stream.headersSent) {
      throw err('ERR_HTTP2_HEADERS_SENT', 'response has already been sent');
    }
    if (key === 'date') {
      this._state.sendDate = false;
      return;
    }
    delete this._headers[key];
  }

  setHeader(name, value) {
    checkName(name);
    if (this._stream.headersSent) {
      throw err('ERR_HTTP2_HEADERS_SENT', 'response has already been sent');
    }
    this._setHeader(name, value);
  }

  // The private half, the one writeHead() uses. node's kSetHeader does not
  // validate the name either (internal/http2/compat.js), so a name that is not
  // a string fails here the way it fails there: name.trim() is not a function.
  _setHeader(name, value) {
    this._headers[trimName(name)] = value;
  }

  appendHeader(name, value) {
    checkName(name);
    if (this._stream.headersSent) {
      throw err('ERR_HTTP2_HEADERS_SENT', 'response has already been sent');
    }
    const key = trimName(name);
    const current = this._headers[key];
    if (current === undefined) {
      this._headers[key] = value;
    } else if (Array.isArray(current)) {
      current.push(value);
    } else {
      this._headers[key] = [current, value];
    }
  }

  setTrailer(name, value) {
    checkName(name);
    const key = trimName(name);
    if (this._trailers[key] === undefined) {
      this._trailerNames++;
    }
    this._trailers[key] = value;
  }

  addTrailers(headers) {
    for (const key of Object.keys(headers)) {
      this.setTrailer(key, headers[key]);
    }
  }

  writeHead(statusCode, statusMessage, headers) {
    const state = this._state;
    if (state.closed || this._stream.destroyed || this._stream.closed) {
      return this;
    }
    if (this._stream.headersSent) {
      throw err('ERR_HTTP2_HEADERS_SENT', 'response has already been sent');
    }
    if (typeof statusMessage === 'string') {
      process.emitWarning('HTTP/2 does not support a status message', 'Warning');
    }
    if (headers === undefined && typeof statusMessage === 'object') {
      headers = statusMessage;
    }

    if (Array.isArray(headers)) {
      if (headers.length && Array.isArray(headers[0])) {
        for (let i = 0; i < headers.length; i++) {
          this._setHeader(headers[i][0], headers[i][1]);
        }
      } else {
        if (headers.length % 2 !== 0) {
          throw err('ERR_INVALID_ARG_VALUE', 'headers array must be flat key/value pairs');
        }
        for (let i = 0; i < headers.length; i += 2) {
          this._setHeader(headers[i], headers[i + 1]);
        }
      }
    } else if (typeof headers === 'object' && headers !== null) {
      for (const key of Object.keys(headers)) {
        this._setHeader(key, headers[key]);
      }
    }

    state.statusCode = statusCode;
    this._beginSend();
    return this;
  }

  _beginSend() {
    const state = this._state;
    // node writes the status into the same map as the rest, so getHeaderNames()
    // and getHeader(':status') report it after writeHead -- match that exactly
    this._headers[':status'] = state.statusCode;
    const stream = this._stream;
    if (!stream.headersSent) {
      // The map itself is the response's headers, as in node: its [kBeginSend]
      // hands the stream the very map it filled from the application's headers
      // (internal/http2/compat), so node has no second object on that path
      // either. A merged copy was built here until the header path was measured:
      // it bought nothing (OPTIMIZATION.md item 25) -- the map is never frozen
      // and the HPACK block cache -- which is keyed by the identity of a frozen
      // object -- cannot use it. h2lean's own stream.sentHeaders is that map;
      // node's server-side getter
      // returns undefined, because its respond() never sets the field.
      stream.respond(this._headers, {
        // a HEAD response must close the stream after the headers, and an
        // application that called end() before flushing also does
        endStream: state.ending || state.headRequest,
        sendDate: state.sendDate,
        waitForTrailers: true
      });
    }
    return this._headers;
  }

  flushHeaders() {
    const state = this._state;
    if (!state.closed && !this._stream.headersSent) {
      this.writeHead(state.statusCode);
    }
  }

  writeContinue() {
    return this.writeInformation(100);
  }

  writeInformation(statusCode, headers) {
    // node refuses 101 here, in the compat layer, with ERR_HTTP2_STATUS_INVALID -- its own
    // `statusCode === 101` check (internal/http2/compat:917) fires before the value can
    // reach stream.additionalHeaders(), which would have reported ERR_HTTP2_STATUS_101.
    // Both rules are therefore reachable, and which one an application sees depends on the
    // API it called: measured against a live node server, compat writeInformation(101)
    // throws ERR_HTTP2_STATUS_INVALID while stream.additionalHeaders({':status':101})
    // throws ERR_HTTP2_STATUS_101. Nothing else about 101 is a judgement call: the status
    // means "switch protocol", which HTTP/2 forbids outright (RFC 9113 8.5).
    if (typeof statusCode !== 'number' || statusCode < 100 || statusCode > 199
        || statusCode === 101) {
      throw err('ERR_HTTP2_STATUS_INVALID', `invalid informational status code: ${statusCode}`);
    }
    if (this._stream.headersSent || this._state.closed) {
      return false;
    }
    const out = { __proto__: null, ':status': statusCode };
    if (headers !== undefined && headers !== null) {
      for (const key of Object.keys(headers)) {
        out[key] = headers[key];
      }
    }
    this._stream.additionalHeaders(out);
    return true;
  }

  write(chunk, encoding, cb) {
    if (typeof encoding === 'function') {
      cb = encoding; encoding = 'utf8';
    }
    const state = this._state;
    let failure;
    if (state.ending) {
      failure = err('ERR_STREAM_WRITE_AFTER_END', 'write after end');
    } else if (state.closed) {
      failure = err('ERR_HTTP2_INVALID_STREAM', 'the stream has been closed');
    } else if (state.destroyed) {
      return false;
    }
    if (failure) {
      if (typeof cb === 'function') {
        process.nextTick(cb, failure);
      }
      this.destroy(failure);
      return false;
    }
    if (state.headRequest) {
      if (typeof cb === 'function') {
        process.nextTick(cb);
      }
      return true; // a HEAD response has no body
    }
    if (!this._stream.headersSent) {
      this.writeHead(state.statusCode);
    }
    return this._stream.write(chunk, encoding, cb);
  }

  end(chunk, encoding, cb) {
    const stream = this._stream;
    const state = this._state;
    if (typeof chunk === 'function') {
      cb = chunk; chunk = null;
    } else if (typeof encoding === 'function') {
      cb = encoding; encoding = 'utf8';
    }

    if (state.closed || state.ending) {
      if (typeof cb === 'function') {
        process.nextTick(cb);
      }
      return this;
    }
    if (chunk !== null && chunk !== undefined && !state.headRequest) {
      this.write(chunk, encoding);
    }
    state.ending = true;

    if (!stream.headersSent) {
      this.writeHead(state.statusCode);
    }

    // one nextTick for the whole completion, and the callback hung off the
    // event rather than scheduled separately -- node does the same. Queued here,
    // before the close below, so 'finish' precedes the teardown events.
    if (typeof cb === 'function') {
      this.once('finish', cb);
    }
    if (!state.closed && !state.finishEmitted) {
      state.finishEmitted = true;
      process.nextTick(emitNT, this, 'finish');
    }

    if (this._trailerNames > 0) {
      // node sends the trailers from the 'wantTrailers' callback; h2lean closes
      // the local side as soon as the application says so, so the recorded
      // trailers go out here, carrying END_STREAM with them
      stream.sendTrailers(this._trailers);
    } else {
      // The response was opened with waitForTrailers so that addTrailers() could
      // still insert a HEADERS frame. No trailers were added, so the stream must
      // not wait for them: drop the request and close the local half now.
      stream._waitForTrailers = false;
      stream.end();
    }
    return this;
  }

  destroy(error) {
    const state = this._state;
    if (state.destroyed) {
      return;
    }
    state.destroyed = true;
    this._stream.destroy(error || undefined);
  }

  setTimeout(msecs, callback) {
    if (this._state.closed) {
      return this;
    }
    this._stream.setTimeout(msecs, callback);
    return this;
  }

  createPushResponse(headers, callback) {
    const e = err('ERR_H2LEAN_NOT_IMPLEMENTED', 'h2lean v1 does not implement server push');
    if (typeof callback !== 'function') {
      throw e;
    }
    process.nextTick(callback, e);
  }
}

// node's default 'stream' handler. Called with the server as `this`.
function onServerStream(stream, headers, options, rawHeaders) {
  const server = this;
  const request = new Http2ServerRequest(stream, headers, rawHeaders);
  stream._requestHeaders = headers;
  const response = new Http2ServerResponse(stream);

  const method = headers[':method'];
  if (method === 'CONNECT') {
    if (!server.emit('connect', request, response)) {
      response.statusCode = 405;
      response.end();
    }
    return;
  }

  if (headers.expect !== undefined) {
    if (headers.expect === '100-continue') {
      if (server.listenerCount('checkContinue') > 0) {
        server.emit('checkContinue', request, response);
      } else {
        response.writeContinue();
        server.emit('request', request, response);
      }
    } else if (server.listenerCount('checkExpectation') > 0) {
      server.emit('checkExpectation', request, response);
    } else {
      response.statusCode = 417;
      response.end();
    }
    return;
  }

  server.emit('request', request, response);
}

module.exports = { Http2ServerRequest, Http2ServerResponse, onServerStream };
