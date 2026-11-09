// M2 client-stream surface: the per-request view an application holds. Everything
// here is a differential -- the same server drives node's ClientHttp2Stream and
// h2lite's, and the observable shapes (response headers, trailers, the 'close' that
// waits for both halves of the Duplex) must agree. The stream is a stream.Duplex,
// which is the load-bearing node compatibility point (AGENTS.md 10: h2lite's SERVER
// stream is an EventEmitter, but a client stream the app reads and pipes has to be a
// real Duplex or the `req.pipe(res)` idiom breaks).
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const h2lite = require('../../index');
const { listen, closeServer } = require('../diff-harness');

function startServer(handler) {
  const srv = http2.createServer(handler);
  srv.on('error', () => {});
  return listen(srv).then((port) => ({ srv, port }));
}

// node keeps an internal Symbol(kSensitiveHeaders) on its header objects; the
// application-visible contract is the string fields, and `date` is second-granular so
// it would make a sequential two-arm comparison flaky for no reason.
function fields(h) {
  const o = {};
  for (const k of Object.keys(h)) {
    if (k !== 'date') {
      o[k] = h[k];
    }
  }
  return o;
}

// One request through `impl`, capturing every observable in one plain object.
function observe(impl, port, path, body) {
  return new Promise((resolve, reject) => {
    const sess = impl.connect(`http://127.0.0.1:${port}`);
    sess.on('error', reject);
    const s = sess.request({ ':method': body === undefined ? 'GET' : 'POST',
      ':path': path });
    const events = [];
    const out = { events, response: null, trailers: null, data: '', closed: false };
    s.on('response', (h) => {
      events.push('response'); out.response = fields(h);
    });
    s.on('trailers', (t) => {
      events.push('trailers'); out.trailers = fields(t);
    });
    s.on('close', () => {
      events.push('close'); out.closed = true;
    });
    s.setEncoding('latin1');
    s.on('data', (d) => {
      out.data += d;
    });
    s.on('end', () => {
      events.push('end');
    });
    s.on('error', reject);
    s.on('end', () => {
      sess.destroy(); resolve(out);
    });
    if (body !== undefined) {
      s.end(body);
    }
  });
}

test('response headers: :status is a number and set-cookie is an array, as node reports',
  async () => {
    const { srv, port } = await startServer((req, res) => {
      res.writeHead(201, {
        'content-type': 'text/plain',
        'set-cookie': ['a=1; Path=/', 'b=2; Path=/'],
        'x-multi': ['one', 'two']
      });
      res.end('done');
    });
    try {
      const node = await observe(http2, port, '/');
      const lite = await observe(h2lite, port, '/');
      assert.strictEqual(lite.response[':status'], 201);
      assert.strictEqual(typeof lite.response[':status'], 'number',
        ':status must be a number, like node reports it');
      assert.deepStrictEqual(lite.response['set-cookie'], node.response['set-cookie']);
      assert.deepStrictEqual(lite.response['set-cookie'], ['a=1; Path=/', 'b=2; Path=/']);
      // a repeated non-cookie field is joined with ', ' exactly as node does
      assert.strictEqual(lite.response['x-multi'], node.response['x-multi']);
      assert.strictEqual(lite.response['x-multi'], 'one, two');
      assert.strictEqual(lite.data, 'done');
    } finally {
      await closeServer(srv);
    }
  });

test('trailers arrive on the "trailers" event and the event order matches node',
  async () => {
    const { srv, port } = await startServer((req, res) => {
      res.writeHead(200, { 'content-type': 'text/plain' });
      res.addTrailers({ 'x-checksum': 'abc123', 'x-count': '3' });
      res.end('body');
    });
    try {
      const node = await observe(http2, port, '/');
      const lite = await observe(h2lite, port, '/');
      assert.deepStrictEqual(lite.trailers, node.trailers, 'trailer fields differ');
      assert.deepStrictEqual(lite.trailers, { 'x-checksum': 'abc123', 'x-count': '3' });
      assert.deepStrictEqual(lite.events, node.events,
        `event order differs: lite ${lite.events} vs node ${node.events}`);
    } finally {
      await closeServer(srv);
    }
  });

test('the Duplex "close" fires once, after both halves of the exchange are done',
  async () => {
    const { srv, port } = await startServer((req, res) => {
      let n = 0;
      req.on('data', (d) => {
        n += d.length;
      });
      req.on('end', () => {
        res.writeHead(200); res.end('got' + n);
      });
    });
    try {
      const node = await observe(http2, port, '/', 'payload');
      const lite = await observe(h2lite, port, '/', 'payload');
      assert.strictEqual(lite.closed, true);
      assert.strictEqual(lite.events.filter((e) => e === 'close').length, 1,
        'close must fire exactly once');
      // 'close' comes last, after the body drained and the request half finished.
      assert.strictEqual(lite.events[lite.events.length - 1], 'close');
      assert.deepStrictEqual(lite.events, node.events);
      assert.strictEqual(lite.data, 'got7');
    } finally {
      await closeServer(srv);
    }
  });

test('an inbound RST_STREAM surfaces as a stream error, not a session crash',
  async () => {
    const srv = http2.createServer();
    srv.on('error', () => {});
    srv.on('stream', (stream, headers) => {
      // this side initiates the reset, but node still surfaces it as a stream error
      stream.on('error', () => {});
      if (headers[':path'] === '/reset') {
        stream.respond({ ':status': 200 }, { endStream: false });
        stream.write('first');
        setTimeout(() => stream.close(http2.constants.NGHTTP2_INTERNAL_ERROR), 20);
      } else {
        stream.respond({ ':status': 200 });
        stream.end('ok');
      }
    });
    const port = await listen(srv);
    try {
      const capture = (impl) => new Promise((resolve) => {
        const sess = impl.connect(`http://127.0.0.1:${port}`);
        const seen = { streamError: null, sessionError: null };
        sess.on('error', (e) => {
          seen.sessionError = e && e.code;
        });
        const s = sess.request({ ':method': 'GET', ':path': '/reset' });
        s.on('error', (e) => {
          seen.streamError = e && e.code;
        });
        s.resume();
        setTimeout(() => {
          seen.destroyed = s.destroyed; sess.destroy(); resolve(seen);
        }, 300);
      });
      const node = await capture(http2);
      const lite = await capture(h2lite);
      // node reports the reset on the stream; the session stays alive.
      assert.strictEqual(node.sessionError, null, 'node resets on the stream, not the session');
      assert.strictEqual(lite.sessionError, null,
        'an inbound RST must not tear down the whole client session');
      assert.ok(lite.streamError !== null || lite.destroyed,
        `the reset should reach the stream (node saw: ${JSON.stringify(node)})`);
    } finally {
      await closeServer(srv);
    }
  });
