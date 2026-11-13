// 'use strict' on purpose: assigning to a frozen property throws here and is
// silently ignored in a sloppy-mode caller, and both behaviours are asserted
// below, because the application's mode decides which one it gets.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const f = require('../lib/frames');
const { listen, connect, request, closeServer } = require('./diff-harness');
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

// The repeated-field semantics, measured on node by putting duplicate lines on the wire
// (its own client cannot send them: buildNgHeaderString throws ERR_HTTP2_HEADER_SINGLE_VALUE
// before anything leaves the process). The differential claim: h2lean reads a repeated
// field the way node reads it -- the first copy of a field in node's kSingleValueFields
// set (lib/session.js SINGLE_VALUE_FIELDS), "; " for cookie, the array for set-cookie,
// ", " for everything else. Joining a single-value field is the CVE-2020-8287 shape on
// the reading side: whatever translates these fields into HTTP/1.1 lines takes the
// first copy too, so a joined value is a value no downstream reader agrees with.
const RAW = (...bytes) => Buffer.from(bytes, 'latin1');
const AUTH = Buffer.from([0x01, 9, ...Buffer.from('localhost', 'latin1')]);
function lit(name, value) {
  const n = Buffer.from(name, 'latin1');
  const v = Buffer.from(value, 'latin1');
  return Buffer.concat([Buffer.from([0x00, n.length]), n, Buffer.from([v.length]), v]);
}
function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}
const DUPS = Buffer.concat([
  RAW(0x82, 0x84, 0x86), AUTH,
  lit('authorization', 'first'), lit('authorization', 'second'),
  lit('cookie', 'a=1'), lit('cookie', 'b=2'),
  lit('set-cookie', 's=1'), lit('set-cookie', 's=2'),
  lit('content-type', 'text/plain'), lit('content-type', 'text/html'),
  lit('x-multi', 'm1'), lit('x-multi', 'm2')
]);

async function fieldValues(impl, block) {
  const srv = impl.createServer();
  let seen = null;
  srv.on('stream', (stream, headers) => {
    seen = {
      authorization: headers.authorization,
      cookie: headers.cookie,
      'set-cookie': headers['set-cookie'],
      'content-type': headers['content-type'],
      'x-multi': headers['x-multi']
    };
    stream.respond({ ':status': 200 }, { endStream: true });
  });
  srv.on('error', () => {});
  const port = await listen(srv);
  await new Promise((resolve) => {
    const sock = net.connect(port, '127.0.0.1');
    sock.on('connect', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, RAW()),
        frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, block)]));
    });
    sock.on('data', () => {});
    sock.on('error', () => {});
    setTimeout(() => {
      sock.destroy();
      resolve();
    }, 250);
  });
  await closeServer(srv);
  return seen;
}

test('a repeated single-value field keeps the first copy, on both stacks', async () => {
  const lite = await fieldValues(h2lean, DUPS);
  const native = await fieldValues(require('node:http2'), DUPS);
  assert.deepStrictEqual(lite, native, 'h2lean must read the duplicates node reads');
  // node's measured toHeaderObject semantics, asserted directly so the test names
  // the behavior rather than only comparing against a moving node version:
  assert.strictEqual(lite.authorization, 'first');
  assert.strictEqual(lite.cookie, 'a=1; b=2');
  assert.deepStrictEqual(lite['set-cookie'], ['s=1', 's=2']);
  assert.strictEqual(lite['content-type'], 'text/plain');
  assert.strictEqual(lite['x-multi'], 'm1, m2');
});

// The outbound half of the strict-single-value rule, measured on node v26.12.1: a
// repeated single-value field -- object or flat-array form, regular or pseudo -- and a
// multi-element array on one all throw ERR_HTTP2_HEADER_SINGLE_VALUE synchronously,
// before any octet exists, and options.strictSingleValueFields: false turns the rule
// off. The inbound half (a repeated field read first-wins) is the test above.
test('outbound duplicates throw like node, and the option turns the rule off', async () => {
  const outcomes = async (impl, opts) => {
    const server = [];
    const client = [];
    // One malformed respond() per stream: node's respond() checks headersSent
    // before it parses the header list (internal/http2/core.js), so a second call
    // on one stream would only ever report ERR_HTTP2_HEADERS_SENT and hide the
    // verdict under test. Streams without a marker -- the opt-out client probes,
    // which no longer throw and so reach the wire -- are answered plainly.
    const attempts = {
      1: (stream) => stream.respond([[':status', 200], ['x-a', '1']]),
      2: (stream) => stream.respond({ ':status': 200, 'content-type': ['a', 'b'] },
        { endStream: true }),
      3: (stream) => stream.respond([':status', 200, ':status', 201], { endStream: true })
    };
    const srv = impl.createServer(opts);
    srv.on('stream', (stream, headers) => {
      const run = headers['x-probe'] && attempts[headers['x-probe']];
      if (!run) {
        stream.respond({ ':status': 200 }, { endStream: true });
        return;
      }
      try {
        run(stream);
        server.push('ok');
      } catch (e) {
        server.push(e instanceof TypeError && e.code === undefined ? 'TypeError' : e.code);
      }
      // a refused attempt sent nothing; the clean answer closes the stream. A served
      // attempt (rule off) carried END_STREAM already.
      if (!stream.headersSent) {
        stream.respond({ ':status': 200 }, { endStream: true });
      }
    });
    const port = await listen(srv);
    const session = impl.connect(`http://127.0.0.1:${port}`, opts);
    // request() throws synchronously, before any stream exists
    const probe = (run) => {
      try {
        run();
        client.push('ok');
      } catch (e) {
        client.push(e instanceof TypeError && e.code === undefined ? 'TypeError' : e.code);
      }
    };
    // With the rule off these two probes reach the wire -- a repeated authorization
    // and a repeated ':method'. The server refuses the duplicated pseudo-header
    // inbound (a protocol error on both stacks, regardless of the option, which is
    // outbound-only), so the streams carry 'error' -- listened for, since nobody
    // reads them.
    probe(() => {
      const st = session.request({ authorization: ['a', 'b'] });
      st.on('error', () => {});
      st.resume();
    });
    probe(() => {
      const st = session.request([':method', 'GET', ':method', 'POST']);
      st.on('error', () => {});
      st.resume();
    });
    // request() throws synchronously on the strict arm, before any stream exists;
    // with the rule off the two probes open streams the gate answers. The three
    // marked requests drive the attempts, one each, awaited in order so the server
    // array is ordered.
    for (const n of ['1', '2', '3']) {
      await new Promise((resolve) => {
        const r = session.request({ ':method': 'GET', ':path': '/', ':scheme': 'http',
          ':authority': 'localhost', 'x-probe': n });
        r.on('response', resolve);
        r.on('error', resolve);
        r.end();
      });
    }
    session.destroy();
    await closeServer(srv);
    return { server, client };
  };

  const lite = await outcomes(h2lean, {});
  const native = await outcomes(require('node:http2'), {});
  assert.deepStrictEqual(lite, native, 'h2lean must refuse what node refuses');
  assert.deepStrictEqual(lite.server, ['TypeError', 'ERR_HTTP2_HEADER_SINGLE_VALUE',
    'ERR_HTTP2_HEADER_SINGLE_VALUE']);
  assert.deepStrictEqual(lite.client, ['ERR_HTTP2_HEADER_SINGLE_VALUE',
    'ERR_HTTP2_HEADER_SINGLE_VALUE']);

  const liteOff = await outcomes(h2lean, { strictSingleValueFields: false });
  const nativeOff = await outcomes(require('node:http2'), { strictSingleValueFields: false });
  assert.deepStrictEqual(liteOff, nativeOff);
  assert.deepStrictEqual(liteOff.server, ['TypeError', 'ok', 'ok']);
  assert.deepStrictEqual(liteOff.client, ['ok', 'ok']);

  // a non-boolean is node's ERR_INVALID_ARG_TYPE, thrown where the option is read --
  // at createServer/connect time, before any socket exists
  for (const impl of [h2lean, require('node:http2')]) {
    assert.throws(() => impl.createServer({ strictSingleValueFields: 'x' }),
      (e) => e.code === 'ERR_INVALID_ARG_TYPE');
    assert.throws(() => impl.connect('http://127.0.0.1:1', { strictSingleValueFields: 5 }),
      (e) => e.code === 'ERR_INVALID_ARG_TYPE');
  }
});
