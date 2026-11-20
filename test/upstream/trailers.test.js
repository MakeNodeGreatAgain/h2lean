// Port of node's `test/parallel/test-http2-trailers.js` and
// `test/parallel/test-http2-no-wanttrailers-listener.js` to the differential harness used
// here (AGENTS.md §3, CONTRIBUTING.md §1: the upstream file is a scenario source, node's
// measured value is the expectation).
//
// What the two files decide between them:
//   1. the trailer round trip on the stream API in both directions, and with it the three
//      synchronous throws of `sendTrailers()` -- before the 'wantTrailers' callback
//      (ERR_HTTP2_TRAILERS_NOT_READY), twice (ERR_HTTP2_TRAILERS_ALREADY_SENT) and after
//      the stream is gone (ERR_HTTP2_INVALID_STREAM).
//   2. what an answer that owes a trailer block but is given no listener does: node's
//      `onStreamTrailers` sends an EMPTY trailer block itself, and the wire then carries no
//      trailing HEADERS at all -- the END_STREAM the body withheld lands on a zero-length
//      DATA instead (measured: HEADERS/EH, DATA len4 flags 0x0, DATA len0 flags 0x1). The
//      client must therefore see the body and no 'trailers' event.
//   3. the same rule on the compat layer, which is where it actually bites: node's
//      (req,res) response always asks for trailers (internal/http2/compat.js `kBeginSend`
//      passes `waitForTrailers: true`) and always answers with the response's trailer map,
//      so every plain `res.end()` is the empty case. A stack that turns that into an empty
//      HEADERS block hands the peer a trailing header section with no fields in it.
//
// Adaptations: `common.mustCall()` counters become recorded transcript lines, the client
// pseudo-headers are written out (h2lean's `connect().request()` takes the same defaults as
// node, and the port states the request it means), and each arm is bounded by a watchdog
// because "the stream hangs" is one of the answers this machinery can produce.
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const h2lean = require('../../index');
const { listen, connect, request } = require('../diff-harness');

const BODY = '<html><head></head><body><h1>this is some data</h2></body></html>';
const TRAILER_KEY = 'test-trailer';
const TRAILER_VALUE = 'testing';
const GET = { ':method': 'GET', ':path': '/', ':scheme': 'http', ':authority': 'localhost' };

// The one line where the two stacks disagree, and both measured values (see the case
// below for the reason). Folding it into `sentTwice` lets every other line be compared
// strictly, so the divergence stays a single named gap rather than a blanket tolerance.
const TWICE = {
  node: 'Error/ERR_HTTP2_TRAILERS_ALREADY_SENT',
  h2lean: 'Error/ERR_HTTP2_INVALID_STREAM'
};

// node reports a synchronous throw; the transcript records which of the three it was.
function throwOf(fn) {
  try {
    fn();
    return 'no throw';
  } catch (err) {
    return `${err.name}/${err.code}`;
  }
}

// A hang is one of the answers this file is testing (a trailer block that never goes out
// leaves both sides waiting), so each arm is bounded and reports it as a line.
function withTimeout(promise, label) {
  let timer;
  const guard = new Promise((resolve) => {
    timer = setTimeout(() => resolve([['HANG', label]]), 5000);
    timer.unref();
  });
  return Promise.race([promise, guard]).then((r) => {
    clearTimeout(timer);
    return r;
  });
}

// 1. test-http2-trailers.js: both halves of the trailer machinery, over one stack.
function trailersArm(lib) {
  return withTimeout(new Promise((resolve) => {
    const t = [];
    const srv = lib.createServer();
    srv.on('stream', (stream) => {
      stream.on('trailers', (h) => {
        t.push(['srv-trailers', h[TRAILER_KEY]]);
        stream.end(BODY);
      });
      stream.respond({ 'content-type': 'text/html', ':status': 200 },
        { waitForTrailers: true });
      t.push(['srv-notready', throwOf(() => stream.sendTrailers({}))]);
      stream.on('wantTrailers', () => {
        stream.sendTrailers({ [TRAILER_KEY]: TRAILER_VALUE });
        t.push(['srv-twice', throwOf(() => stream.sendTrailers({}))]);
      });
    });
    listen(srv).then((port) => {
      const client = connect(port);
      client.on('error', () => {});
      const req = client.request({ ':path': '/', ':method': 'POST' },
        { waitForTrailers: true });
      req.on('wantTrailers', () => req.sendTrailers({ [TRAILER_KEY]: TRAILER_VALUE }));
      const chunks = [];
      req.on('data', (c) => chunks.push(c));
      req.on('trailers', (h) => t.push(['cli-trailers', h[TRAILER_KEY]]));
      req.on('close', () => {
        t.push(['body', chunks.join('').length]);
        t.push(['cli-after-close', throwOf(() => req.sendTrailers({}))]);
        client.destroy();
        srv.close();
        resolve(t);
      });
      req.end('data');
    });
  }), 'trailers');
}

// 2. test-http2-no-wanttrailers-listener.js: nobody answers, so the stack answers with an
// empty block -- and an empty block is not a header section on the wire.
function nolistenArm(lib) {
  return withTimeout(new Promise((resolve) => {
    const t = [];
    const srv = lib.createServer();
    srv.on('stream', (stream) => {
      stream.respond(undefined, { waitForTrailers: true });
      stream.end('ok');
    });
    listen(srv).then((port) => {
      const client = connect(port);
      client.on('error', () => {});
      const req = client.request(GET);
      const chunks = [];
      req.on('data', (c) => chunks.push(c));
      req.on('trailers', (h) => t.push(['cli-trailers', JSON.stringify(h)]));
      req.on('wantTrailers', () => t.push(['cli-wantTrailers', 1]));
      req.on('close', () => {
        t.push(['body', chunks.join('')]);
        client.destroy();
        srv.close();
        resolve(t);
      });
      req.resume();
    });
  }), 'nolisten');
}

// 3. the compat shape: an ordinary `res.end()` is node's most frequent empty trailer.
function compatArm(lib) {
  return withTimeout((async () => {
    const srv = lib.createServer((req, res) => {
      res.writeHead(200, { 'content-type': 'text/plain' });
      res.end('body');
    });
    const port = await listen(srv);
    const client = connect(port);
    try {
      const res = await request(client, GET);
      return {
        status: res.headers[':status'],
        body: res.body,
        // null = no trailing HEADERS arrived; {} would mean an empty one did
        trailers: res.trailers,
        informational: res.informational,
        continued: res.continued
      };
    } finally {
      client.destroy();
      srv.close();
    }
  })(), 'compat');
}

function lineSet(t) {
  return t.map(([k, v]) => `${k} ${v}`).sort();
}

function expectedLines(twice) {
  return lineSet([
    ['srv-notready', 'Error/ERR_HTTP2_TRAILERS_NOT_READY'],
    ['cli-trailers', TRAILER_VALUE],
    ['srv-trailers', TRAILER_VALUE],
    ['srv-twice', twice],
    ['body', BODY.length],
    ['cli-after-close', 'Error/ERR_HTTP2_INVALID_STREAM']
  ]);
}

test('upstream trailers: the round trip and the three sendTrailers() throws', async () => {
  const native = await trailersArm(http2);
  const ours = await trailersArm(h2lean);
  // KNOWN DIVERGENCE, one error name on a call that both stacks refuse. node defers the
  // trailer submit itself: `sendTrailers()` records the block and hands the bytes to
  // `setImmediate(finishSendTrailers)` -- "Send the trailers in setImmediate so we don't do
  // it on nghttp2 stack" (internal/http2/core.js) -- so in the turn of the first call the
  // stream is still neither destroyed nor closed and the second call reaches the ALREADY_SENT
  // guard. h2lean frames the trailer inside the call, which closes the stream in that same
  // turn, so the second call meets the destroyed guard first and answers INVALID_STREAM.
  // The guard order here is node's own (destroyed, then already-sent, then not-ready, read
  // out of core.js in the same session); keeping node's verdict as well would mean deferring
  // the frame by a turn, which is a TickObject on every trailer-bearing response -- that is,
  // on every compat response. CONTRIBUTING.md §5.
  assert.deepStrictEqual(lineSet(native), expectedLines(TWICE.node));
  assert.deepStrictEqual(lineSet(ours), expectedLines(TWICE.h2lean));
});

test('upstream no-wanttrailers-listener: an unanswered trailer block ends the stream', async () => {
  const native = await nolistenArm(http2);
  const ours = await nolistenArm(h2lean);
  assert.deepStrictEqual(ours, native);
  assert.deepStrictEqual(native, [['body', 'ok']],
    'the body must arrive and no trailers event may be reported for an empty block');
});

test('upstream compat end(): no trailing HEADERS for a response that added no trailer', async () => {
  const native = await compatArm(http2);
  const ours = await compatArm(h2lean);
  assert.deepStrictEqual(ours, native);
  assert.strictEqual(native.trailers, null);
  assert.strictEqual(native.body, 'body');
});
