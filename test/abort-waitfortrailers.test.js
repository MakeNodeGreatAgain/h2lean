// Differential tests for TODO 4.3: options.signal (AbortSignal) and
// options.waitForTrailers on the client request path.
//
// Both features live on ClientHttp2Session.request() in node's own core.js --
// nowhere else. Node's stream.respond() does NOT read a signal (grep of
// internal/http2/core.js finds one hit inside `request()`'s body and none in
// the respond path); the TODO's claim that signal applies on the server respond
// path was wrong, and the h2lean surface mirrors node's.
//
// Node's shape (read from core.js in the same session):
//   * signal → validateAbortSignal; aborter destroys the stream with
//     `new AbortError(undefined, { cause: signal.reason })` -- name 'AbortError',
//     code 'ABORT_ERR', message 'The operation was aborted', cause = signal.reason.
//     Already-aborted signals destroy the stream synchronously before any frame;
//     a live signal installs an 'abort' listener disposed by the stream's 'close'.
//   * waitForTrailers → STREAM_FLAGS_HAS_TRAILERS on the stream, which makes
//     finishWrite skip shutdownWritable. The last DATA goes out WITHOUT
//     END_STREAM; nghttp2's onStreamTrailers callback fires 'wantTrailers' BEFORE
//     the Duplex 'finish' event; the wire stays open until sendTrailers() or
//     close() ends the message.
const test = require('node:test');
const assert = require('node:assert');
const nodeHttp2 = require('node:http2');
const h2lean = require('../index');

// ---- AbortSignal ---------------------------------------------------------

// Case 1: abort AFTER request() is called but before the server answers.
// Case 2: pass an ALREADY-aborted signal to request().
// Case 3: pass a non-AbortSignal → node throws ERR_INVALID_ARG_TYPE synchronously.
// Each case is run against a node server and against an h2lean server, so both
// stacks see identical traffic.
function abortCase(lib, mode, signalArg) {
  return new Promise((resolve) => {
    const srv = nodeHttp2.createServer((req, res) => {
      if (mode === 'server-hangs') {
        return; // never answer; the client aborts.
      }
      res.writeHead(200);
      res.end('ok');
    });
    srv.listen(0, '127.0.0.1', () => {
      const p = srv.address().port;
      const client = lib.connect(`http://127.0.0.1:${p}`);
      client.on('error', () => {});
      let thrown;
      let req;
      try {
        req = client.request({ ':method': 'GET', ':path': '/' },
          signalArg === undefined ? {} : { signal: signalArg });
      } catch (e) {
        thrown = { code: e.code, name: e.name };
        client.destroy();
        srv.close();
        resolve({ thrown });
        return;
      }
      const out = { err: null, close: false };
      req.on('error', (e) => {
        out.err = { name: e.name, code: e.code, message: e.message,
          causeName: e.cause && e.cause.name, causeMsg: e.cause && e.cause.message };
      });
      req.on('close', () => {
        out.close = true;
        client.destroy();
        srv.close();
        resolve({ thrown, ...out });
      });
      if (mode === 'abort-now') {
        setImmediate(() => {
          if (signalArg && signalArg.constructor && signalArg.reason !== undefined) {
            // AbortController.signal -- abort through the controller it came from.
            // Handled by the caller holding the controller.
          }
        });
      }
    });
  });
}

test('signal: already-aborted signal destroys the stream with AbortError', async () => {
  async function run(lib) {
    const ac = new AbortController();
    ac.abort(new Error('custom reason'));
    const { err, close } = await abortCase(lib, 'server-hangs', ac.signal);
    return { err, close };
  }
  const a = await run(nodeHttp2);
  const b = await run(h2lean);
  assert.deepStrictEqual(b, a);
  assert.strictEqual(a.err.name, 'AbortError', 'precondition: node reports AbortError');
  assert.strictEqual(a.err.code, 'ABORT_ERR');
  assert.strictEqual(a.err.message, 'The operation was aborted');
  assert.strictEqual(a.err.causeMsg, 'custom reason');
});

test('signal: default abort reason (no custom) matches node', async () => {
  async function run(lib) {
    const ac = new AbortController();
    ac.abort();
    const { err, close } = await abortCase(lib, 'server-hangs', ac.signal);
    return { err, close };
  }
  const a = await run(nodeHttp2);
  const b = await run(h2lean);
  assert.deepStrictEqual(b, a);
});

test('signal: mid-flight abort fires after request() is in flight', async () => {
  async function run(lib) {
    return new Promise((resolve) => {
      const srv = nodeHttp2.createServer(() => { /* never answer */ });
      srv.listen(0, '127.0.0.1', () => {
        const p = srv.address().port;
        const client = lib.connect(`http://127.0.0.1:${p}`);
        client.on('error', () => {});
        const ac = new AbortController();
        const req = client.request({ ':method': 'GET', ':path': '/' },
          { signal: ac.signal });
        const out = { err: null, close: false };
        req.on('error', (e) => {
          out.err = { name: e.name, code: e.code, causeName: e.cause && e.cause.name };
        });
        req.on('close', () => {
          out.close = true;
          client.destroy();
          srv.close();
          resolve(out);
        });
        setTimeout(() => ac.abort(), 50);
      });
    });
  }
  const a = await run(nodeHttp2);
  const b = await run(h2lean);
  assert.deepStrictEqual(b, a);
  assert.strictEqual(a.err.code, 'ABORT_ERR');
});

test('signal: bad argument throws ERR_INVALID_ARG_TYPE synchronously', async () => {
  // node's ClientHttp2Session.request() constructs the ClientHttp2Stream inside
  // its body and only then calls validateAbortSignal, so a bad signal throws
  // while leaving the fresh stream registered on the session (state
  // .pendingStreams on node, _queuedReq/_outboundQueue/_streams on h2lean).
  // A later session destroy emits ERR_HTTP2_STREAM_CANCEL on that orphan, and
  // both stacks reproduce the same. The probe attaches 'error' handlers to
  // every stream the session knows about before calling destroy(), so the
  // cancel is delivered rather than becoming an uncaughtException; the test
  // asserts only on the synchronous throw shape.
  function onAllStreams(client, handler) {
    for (const sym of Object.getOwnPropertySymbols(client)) {
      const state = client[sym];
      if (state && typeof state === 'object' && state.pendingStreams) {
        for (const s of state.pendingStreams) {
          s.on('error', handler);
        }
        if (state.streams && state.streams.values) {
          for (const s of state.streams.values()) {
            s.on('error', handler);
          }
        }
      }
    }
    if (client._queuedReq) {
      for (const r of client._queuedReq) {
        r.stream.on('error', handler);
      }
    }
    if (client._outboundQueue) {
      for (const r of client._outboundQueue) {
        r.stream.on('error', handler);
      }
    }
    if (client._streams) {
      for (const s of client._streams.values()) {
        s.on('error', handler);
      }
    }
  }
  async function run(lib) {
    return new Promise((resolve) => {
      const srv = nodeHttp2.createServer((req, res) => {
        res.writeHead(200); res.end('ok');
      });
      srv.listen(0, '127.0.0.1', () => {
        const p = srv.address().port;
        const client = lib.connect(`http://127.0.0.1:${p}`);
        client.on('error', () => {});
        client.once('connect', () => {
          let thrown;
          try {
            client.request({ ':method': 'GET', ':path': '/' }, { signal: 'not-a-signal' });
          } catch (e) {
            thrown = { code: e.code, name: e.name };
          }
          onAllStreams(client, () => {});
          client.destroy();
          srv.close();
          setImmediate(() => resolve(thrown));
        });
      });
    });
  }
  const a = await run(nodeHttp2);
  const b = await run(h2lean);
  assert.deepStrictEqual(b, a);
  assert.strictEqual(a.code, 'ERR_INVALID_ARG_TYPE');
});

// ---- waitForTrailers ----------------------------------------------------

// A client request with waitForTrailers:true: the last DATA does NOT carry
// END_STREAM; 'wantTrailers' fires (before the Duplex's 'finish', matching node's
// nghttp2 shutdown callback path); sendTrailers() puts HEADERS+END_STREAM with
// the trailer block; the server's stream sees them.
function trailerProbe(lib) {
  return new Promise((resolve) => {
    const received = [];
    const srv = nodeHttp2.createServer();
    srv.on('stream', (stream) => {
      stream.on('trailers', (t) => received.push({ ...t }));
      stream.on('end', () => {
        stream.respond({ ':status': 200 });
        stream.end('ok');
      });
      stream.resume();
    });
    srv.listen(0, '127.0.0.1', () => {
      const p = srv.address().port;
      const client = lib.connect(`http://127.0.0.1:${p}`);
      client.on('error', () => {});
      const req = client.request({ ':method': 'POST', ':path': '/' },
        { waitForTrailers: true });
      const order = [];
      req.on('wantTrailers', () => {
        order.push('wantTrailers');
        req.sendTrailers({ 'x-trailer': 'yes' });
      });
      req.on('finish', () => order.push('finish'));
      req.on('response', () => {
        order.push('response'); req.resume();
      });
      req.on('close', () => {
        order.push('close');
        client.destroy();
        srv.close();
        resolve({ order, received });
      });
      req.end('hello');
    });
  });
}

test('waitForTrailers + sendTrailers: h2lean matches node event order and server trailers',
  async () => {
    const a = await trailerProbe(nodeHttp2);
    const b = await trailerProbe(h2lean);
    assert.deepStrictEqual(b.order, a.order);
    assert.deepStrictEqual(b.received, a.received);
    assert.ok(a.order.indexOf('wantTrailers') < a.order.indexOf('finish'),
      'precondition: node emits wantTrailers before finish');
    assert.strictEqual(a.received.length, 1, 'precondition: node server saw one trailer block');
    assert.strictEqual(a.received[0]['x-trailer'], 'yes');
  });

test('sendTrailers without waitForTrailers throws ERR_HTTP2_TRAILERS_NOT_READY',
  async () => {
    async function run(lib) {
      return new Promise((resolve) => {
        const srv = nodeHttp2.createServer((req, res) => {
          res.writeHead(200); res.end('ok');
        });
        srv.listen(0, '127.0.0.1', () => {
          const p = srv.address().port;
          const client = lib.connect(`http://127.0.0.1:${p}`);
          client.on('error', () => {});
          const req = client.request({ ':method': 'POST', ':path': '/' });
          let thrown;
          try {
            req.sendTrailers({ x: 'y' });
          } catch (e) {
            thrown = { code: e.code, name: e.name };
          }
          req.on('response', () => req.resume());
          req.on('close', () => {
            client.destroy();
            srv.close();
            resolve(thrown);
          });
          req.end('body');
        });
      });
    }
    const a = await run(nodeHttp2);
    const b = await run(h2lean);
    assert.deepStrictEqual(b, a);
    assert.strictEqual(a.code, 'ERR_HTTP2_TRAILERS_NOT_READY');
  });
