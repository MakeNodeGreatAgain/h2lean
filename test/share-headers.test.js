// 'use strict' on purpose: assigning to a frozen property throws here and is
// silently ignored in a sloppy-mode caller, and both behaviours are asserted
// below, because the application's mode decides which one it gets.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const { listen, connect, request } = require('./diff-harness');
const h2lean = require('../index');

const A = { ':method': 'GET', ':path': '/a?q=1', ':scheme': 'http', ':authority': 'localhost', accept: '*/*' };
const B = { ...A, ':path': '/b?q=1' };

function collectingServer(options) {
  const seen = [];
  const srv = h2lean.createServer(options);
  srv.on('stream', (stream, headers) => {
    seen.push(headers);
    stream.respond({ ':status': 200 }, { endStream: true });
  });
  return { srv, seen };
}

test('shareHeaders off (default): a fresh writable object per request', async () => {
  const { srv, seen } = collectingServer();
  const port = await listen(srv);
  const client = connect(port);
  await request(client, A);
  await request(client, A);
  client.destroy();
  srv.close();

  assert.strictEqual(seen.length, 2);
  assert.notStrictEqual(seen[0], seen[1], 'identical blocks must still get separate objects');
  assert.strictEqual(Object.isFrozen(seen[0]), false);
  seen[0][':path'] = '/mutated';
  assert.strictEqual(seen[1][':path'], '/a?q=1');
});

test('shareHeaders on: one frozen object per distinct header block', async () => {
  const { srv, seen } = collectingServer({ shareHeaders: true });
  const port = await listen(srv);
  const client = connect(port);
  // nghttp2 re-encodes the first requests while its dynamic table settles, so
  // the same logical headers can arrive as different bytes a few times before
  // the block stops changing; eight requests is enough to reach a steady state
  for (let i = 0; i < 8; i++) {
    await request(client, A);
  }
  await request(client, B);
  await request(client, A);
  client.destroy();
  srv.close();

  const settled = seen[7];
  assert.strictEqual(settled, seen[6], 'a repeating block must be handed out as one object');
  assert.strictEqual(Object.isFrozen(settled), true);
  assert.strictEqual(settled[':path'], '/a?q=1');
  const other = seen[8];
  assert.notStrictEqual(other, settled, 'a different block is never aliased onto it');
  assert.strictEqual(other[':path'], '/b?q=1');
  assert.strictEqual(seen[9], settled);
});

test('shareHeaders on: writes throw instead of corrupting another request', async () => {
  const { srv, seen } = collectingServer({ shareHeaders: true });
  const port = await listen(srv);
  const client = connect(port);
  for (let i = 0; i < 8; i++) {
    await request(client, A);
  }
  client.destroy();
  srv.close();

  const shared = seen[7];
  assert.strictEqual(shared, seen[6], 'the block had settled');
  assert.throws(() => {
    shared[':path'] = '/x';
  }, TypeError);
  // duplicate values live in arrays, and Object.freeze is shallow, so the array
  // has to be frozen as well or push() would reach every sharing request
  assert.throws(() => {
    shared.accept = 'text/plain';
  }, TypeError);
  assert.strictEqual(shared[':path'], '/a?q=1');
});

test('shareHeaders on: set-cookie arrays are frozen too', async () => {
  const { srv, seen } = collectingServer({ shareHeaders: true });
  const port = await listen(srv);
  const client = connect(port);
  await request(client, { ...A, 'set-cookie': ['c=1', 'c=2'] });
  client.destroy();
  srv.close();

  const cookies = seen[0]['set-cookie'];
  assert.ok(Array.isArray(cookies), 'node shape: set-cookie is always an array');
  assert.deepStrictEqual(cookies, ['c=1', 'c=2']);
  assert.strictEqual(Object.isFrozen(cookies), true);
  assert.throws(() => cookies.push('c=3'), TypeError);
});

test('shareHeaders on: the compat setters refuse a frozen request', async () => {
  const errors = [];
  const srv = h2lean.createServer({ shareHeaders: true }, (req, res) => {
    try {
      req.url = '/rewritten';
    } catch (e) {
      errors.push(e.code);
    }
    try {
      req.method = 'POST';
    } catch (e) {
      errors.push(e.code);
    }
    res.writeHead(200, { 'x-url': req.url });
    res.end();
  });
  const port = await listen(srv);
  const client = connect(port);
  const res = await request(client, A);
  client.destroy();
  srv.close();

  assert.deepStrictEqual(errors, ['ERR_H2LEAN_FROZEN_HEADERS', 'ERR_H2LEAN_FROZEN_HEADERS']);
  assert.strictEqual(res.headers['x-url'], '/a?q=1', 'the request must not have been rewritten');
});

test('shareHeaders on: the size limit is still checked for a cached shape', async () => {
  // Headers are counted when the shape is first seen; the second identical
  // request must still be refused if the limit says so.
  const srv = h2lean.createServer({
    shareHeaders: true,
    settings: { maxHeaderListSize: 48 }
  });
  let answered = 0;
  srv.on('stream', (stream) => {
    answered++; stream.respond({ ':status': 200 }, { endStream: true });
  });
  const port = await listen(srv);
  const client = connect(port);
  const big = { ...A, ':path': '/a?q=1', 'x-padding': 'z'.repeat(64) };

  const codes = [];
  for (let i = 0; i < 2; i++) {
    codes.push(await new Promise((resolve) => {
      const req = client.request(big);
      req.resume();
      req.on('error', () => {});
      req.on('close', () => resolve(req.rstCode));
      setTimeout(() => resolve('timeout'), 1500);
      req.end();
    }));
  }
  client.destroy();
  srv.close();

  // ENHANCE_YOUR_CALM, the code node answers with for an oversized header list (RFC 9113 6.5.2
  // and 7, measured and pinned in test/inbound-limits.test.js) -- cached shape or not.
  assert.deepStrictEqual(codes, [11, 11],
    `ENHANCE_YOUR_CALM both times, cached shape or not: got ${codes}`);
  assert.strictEqual(answered, 0);
});
