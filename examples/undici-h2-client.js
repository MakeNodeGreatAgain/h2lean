// h2lean as an HTTP/2 CLIENT, driven by undici through a custom Dispatcher.
//
// An application that calls fetch() or undici.request() never sees a transport,
// so the usual way to put h2lean there is monkey-patching require('node:http2').
// undici has a better seam: everything above its API reaches the protocol engine
// through one method, Dispatcher#dispatch(options, handler). Subclass Dispatcher,
// answer that method with h2lean.connect(), and request(), fetch(), pipeline(), an
// Agent or a RetryAgent on top all run on h2lean's HTTP/2 -- frame parsing, HPACK
// and flow control are ours, undici's own machinery is untouched. No global patch,
// and the choice is per call site.
//
// The server is node's OWN node:http2: h2lean only acts as client here, so every
// 200 is node's nghttp2 accepting what our client put on the wire, and the reuse
// across the requests is one session's HPACK and flow-control state surviving.
//
// Not covered (this is an example, not a transport): CONNECT and upgrades, and
// 1xx informational responses.
//
//   npm install                # undici is a devDependency of this repo
//   node examples/undici-h2-client.js
'use strict';

const { once } = require('node:events');
const { STATUS_CODES } = require('node:http');
const { Readable } = require('node:stream');
const nodeHttp2 = require('node:http2'); // the server: node's own
const h2lean = require('../index.js');   // the client: ours
const { Dispatcher, request, fetch } = require('undici');

// RFC 9113 8.2.2: these name a connection, not a message, so an HTTP/2 peer must
// not receive them at all -- node's server refuses them as a stream error. undici's
// own HTTP/2 client drops the same set; 'host' goes too because ':authority' is
// what HTTP/2 uses to say it.
const CONNECTION_SPECIFIC = new Set([
  'connection', 'keep-alive', 'proxy-connection', 'transfer-encoding', 'upgrade', 'host'
]);

// undici's controller: the only way the caller talks back to the transport.
// pause()/resume() become flow control (stop reading, so stop crediting the peer);
// abort() destroys the stream, which sends RST_STREAM.
function createController(handler) {
  let stream = null;
  let aborted = false;
  let reason = null;
  const controller = {
    paused: false,
    rawHeaders: null,
    rawTrailers: null,
    get aborted() { return aborted; },
    get reason() { return reason; },
    pause() { controller.paused = true; if (stream !== null) stream.pause(); },
    resume() { controller.paused = false; if (stream !== null) stream.resume(); },
    abort(err) {
      if (aborted) return;
      aborted = true;
      reason = err;
      if (stream !== null) stream.destroy();
      handler.onResponseError?.(controller, err);
    },
    attach(s) { stream = s; if (aborted) s.destroy(); }
  };
  return controller;
}

// The request body, however undici hands it over: a string or Buffer, a Readable,
// or -- for fetch() -- a bare async iterable. Iterable bodies are written chunk by
// chunk with the stream's own backpressure; events.once() rejects if the stream
// errors while we wait for a drain, so a reset peer surfaces as a rejection.
async function writeBody(stream, body) {
  if (typeof body === 'object' && (typeof body.pipe === 'function' ||
      typeof body[Symbol.asyncIterator] === 'function')) {
    for await (const chunk of body) {
      if (!stream.write(chunk)) await once(stream, 'drain');
    }
    stream.end();
  } else {
    stream.end(body);
  }
}

// options -> HTTP/2 headers. Pseudo-fields first (node refuses a regular field
// before them); `new Headers()` is what turns undici's object or pair list into
// lower-cased names and single string values.
function h2Headers(options, url) {
  const headers = {
    ':method': options.method === undefined ? 'GET' : options.method,
    ':path': options.path,
    ':scheme': url.protocol.slice(0, -1),
    ':authority': url.host
  };
  for (const [name, value] of new Headers(options.headers)) {
    if (!CONNECTION_SPECIFIC.has(name)) headers[name] = value;
  }
  return headers;
}

class H2leanDispatcher extends Dispatcher {
  constructor() {
    super();
    this.sessions = new Map();
    this.closing = false;
  }

  // One h2lean session per origin, reused by every stream on it -- that reuse is
  // the whole exercise. 'connect'/'disconnect' are re-emitted in the shape undici's
  // Agent and Pool expect from a member dispatcher ((origin, targets, err)).
  sessionFor(url) {
    let session = this.sessions.get(url.origin);
    if (session === undefined || session.destroyed) {
      session = h2lean.connect(url.origin);
      session.on('error', () => {}); // each live stream reports it; there is no queue here
      session.on('connect', () => this.emit('connect', url, [this]));
      session.on('close', () => {
        this.sessions.delete(url.origin);
        this.emit('disconnect', url, [this], new Error('the HTTP/2 session closed'));
      });
      this.sessions.set(url.origin, session);
    }
    return session;
  }

  // true means "I took the job, no 'drain' is coming"; a false would make an
  // enclosing Pool hold this dispatcher out of rotation forever.
  dispatch(options, handler) {
    const controller = createController(handler);
    handler.onRequestStart?.(controller, null);
    if (controller.aborted) return true;
    if (this.closing) {
      controller.abort(new Error('the dispatcher is closed'));
      return true;
    }

    const url = new URL(options.origin);
    let stream;
    try {
      stream = this.sessionFor(url).request(h2Headers(options, url), {
        endStream: options.body === undefined || options.body === null
      });
    } catch (err) {
      controller.abort(err); // refused before anything went on the wire
      return true;
    }
    controller.attach(stream);

    const trailers = {};
    let ended = false;
    const fail = (err) => { if (!ended && !controller.aborted) controller.abort(err); };
    stream.on('response', (headers) => {
      const status = headers[':status'];
      if (status < 200 || controller.aborted) return; // 1xx is not part of this contract
      handler.onResponseStart?.(controller, status,
        Object.fromEntries(Object.entries(headers).filter(([name]) => !name.startsWith(':'))),
        STATUS_CODES[status]);
    });
    stream.on('data', (chunk) => {
      if (controller.aborted || ended) return;
      handler.onResponseData?.(controller, chunk);
      if (controller.paused) stream.pause(); // the handler's Readable filled up
    });
    stream.on('trailers', (headers) => { Object.assign(trailers, headers); });
    stream.on('end', () => {
      if (controller.aborted || ended) return;
      ended = true;
      handler.onResponseEnd?.(controller, trailers);
    });
    stream.on('error', fail);
    // A reset peer ends the stream with neither 'end' nor 'error': cover it here.
    stream.on('close', () => fail(new Error('the HTTP/2 stream closed before the response completed')));

    if (options.body !== undefined && options.body !== null) {
      writeBody(stream, options.body)
        .then(() => handler.onRequestSent?.(), (err) => { stream.destroy(); fail(err); });
    }
    return true;
  }

  async close() {
    this.closing = true;
    await Promise.all(Array.from(this.sessions.values(),
      (session) => new Promise((resolve) => session.close(resolve))));
    this.sessions.clear();
  }

  async destroy() {
    this.closing = true;
    for (const session of this.sessions.values()) session.destroy();
    this.sessions.clear();
  }
}

// --- the peer: node's own HTTP/2 server --------------------------------------
const sockets = new Set();
let streams = 0;
const server = nodeHttp2.createServer((req, res) => {
  // One entry per TCP connection, one count per stream: many streams on one
  // connection is what the session reuse buys.
  sockets.add(req.socket.remotePort);
  streams++;
  if (req.url === '/slow') {
    // Answers after the client has given up: the cancellation path is the point.
    const timer = setTimeout(() => { res.writeHead(200); res.end('too late'); }, 50);
    req.stream.on('close', () => clearTimeout(timer));
    return;
  }
  if (req.url === '/echo') {
    // A trailer after the body, so the run shows the 'trailers' event reaching
    // undici's handler too (node wants the names declared up front).
    res.setHeader('trailer', 'x-checksum');
    res.writeHead(200, { 'content-type': 'text/plain' });
    req.on('data', (chunk) => res.write(chunk));
    req.on('end', () => { res.addTrailers({ 'x-checksum': 'ok' }); res.end(); });
    return;
  }
  res.writeHead(200, { 'content-type': 'text/plain', 'x-powered-by': 'node:http2' });
  // Echoing x-test back proves the header mapping survived the trip: undici's
  // 'X-Test' arrived lower-cased, as a regular field after the pseudo-fields.
  res.end(`hello from node:http2 (method=${req.method}, x-test=${req.headers['x-test']})\n`);
});

server.listen(0, '127.0.0.1', async () => {
  const base = `http://127.0.0.1:${server.address().port}`; // h2c: cleartext prior-knowledge HTTP/2
  const dispatcher = new H2leanDispatcher();
  dispatcher.on('connect', (url) => console.log('event connect      ->', url.origin));
  dispatcher.on('disconnect', (url) => console.log('event disconnect   ->', url.origin));

  try {
    const get = await request(`${base}/`, { dispatcher, headers: { 'X-Test': 'kept' } });
    console.log('undici.request GET ->', get.statusCode, `via ${get.headers['x-powered-by']}`,
      JSON.stringify(await get.body.text()));

    // fetch(): the call site did not change except for `dispatcher`, and now the
    // protocol engine underneath it is h2lean's.
    const post = await fetch(`${base}/echo`, { method: 'POST', body: 'round trip', dispatcher });
    console.log('fetch POST /echo   ->', post.status, JSON.stringify(await post.text()));

    // A streaming upload: undici hands over a Readable, written into one stream.
    const streamed = await request(`${base}/echo`, {
      method: 'POST',
      dispatcher,
      body: Readable.from(['chunk-1:', 'chunk-2:', 'chunk-3'])
    });
    console.log('streaming POST     ->', streamed.statusCode,
      JSON.stringify(await streamed.body.text()), 'trailer', JSON.stringify(streamed.trailers));
    // Three requests, and the peer saw them all on one TCP connection. Read before
    // the cancellation below: a stream the client resets may or may not reach the
    // server's 'request' event, and this count must not depend on that race.
    console.log(`connections used   -> ${sockets.size} socket, ${streams} streams`);

    // An AbortSignal reaches the controller as abort(), so the HTTP/2 stream is
    // destroyed and RST_STREAM leaves for the peer.
    const ac = new AbortController();
    const cancelled = request(`${base}/slow`, { dispatcher, signal: ac.signal });
    ac.abort();
    try {
      await cancelled;
      console.error('cancel via signal  -> the request was NOT cancelled');
      process.exitCode = 1;
    } catch (err) {
      console.log('cancel via signal  ->', `${err.name}: ${err.message}`);
    }
  } catch (err) {
    console.error('h2lean as undici client failed:', err && err.stack || err);
    process.exitCode = 1;
  } finally {
    await dispatcher.close();
    server.close();
  }
});
