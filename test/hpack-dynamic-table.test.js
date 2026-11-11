// Session-level guarantees of the HPACK dynamic table (lib/hpack Encoder).
//
// The codec mechanics live in test/hpack.test.js; what is checked here is the part that
// only a connection can break: our table and the peer's must stay the same table, the
// whole-block cache must not outlive the indices it references, and two sessions must
// not share bytes that depend on one session's table.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const f = require('../lib/frames');
const h2lean = require('../index');
const hdr = require('../lib/headers');
const { Encoder } = require('../lib/hpack');
const { listen, connect, request } = require('./diff-harness');

// Three response shapes, frozen so that block() caches them per session. None of the
// values but :status 200 is in the static table, so an empty dynamic table costs a
// literal for content-type and x-kind every time.
const SHAPES = {
  '/a': Object.freeze({ ':status': 200, 'content-type': 'text/plain; charset=utf-8',
    'x-kind': 'alpha' }),
  '/b': Object.freeze({ ':status': 200, 'content-type': 'application/json', 'x-kind': 'beta' }),
  '/c': Object.freeze({ ':status': 404, 'content-type': 'text/plain; charset=utf-8',
    'x-kind': 'gamma' })
};

function shapeServer() {
  const srv = h2lean.createServer();
  srv.on('stream', (stream, headers) => {
    const shape = SHAPES[headers[':path']] || SHAPES['/a'];
    // sendDate:false keeps the generated `date` out of this fixture: the tests below
    // compare the decoded field list exactly and count the octets a settled shape costs,
    // and a line whose value changes once a second belongs to neither measurement. The
    // date itself is covered by test/response-date.test.js and measured in
    // bench/hpack-wire.js, which sends it on both servers.
    stream.respond(shape, { endStream: true, sendDate: false });
  });
  return srv;
}

// A client block for one path, built by our own encoder with a table nobody shares.
// The authority is not decoration: RFC 9113 8.3.1 requires :authority or host in a
// request, and a client that omits it now gets RST_STREAM PROTOCOL_ERROR -- which node's
// own nghttp2 has always done, so the block has to carry it to be a request at all.
function requestBlock(path) {
  const enc = new Encoder(4096);
  const dst = Buffer.allocUnsafe(256);
  let off = enc.write(dst, 0, ':method', 'GET', false);
  off = enc.write(dst, off, ':path', path, false);
  off = enc.write(dst, off, ':scheme', 'http', false);
  off = enc.write(dst, off, ':authority', 'localhost', false);
  return dst.subarray(0, off);
}

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// Drives the requests over one raw connection and returns, per response, the HEADERS
// payload size and the fields as they came off the decoder it builds from our octets.
async function driveRaw(port, paths) {
  const sock = net.connect(port, '127.0.0.1');
  const decoder = new h2lean.HPack.Decoder(4096);
  const seen = [];
  let pending = Buffer.alloc(0);
  const done = new Promise((resolve) => {
    sock.on('data', (d) => {
      pending = Buffer.concat([pending, d]);
      for (;;) {
        if (pending.length < f.HEADER_LEN) {
          return;
        }
        const len = (pending[0] << 16) | (pending[1] << 8) | pending[2];
        if (pending.length < f.HEADER_LEN + len) {
          return;
        }
        const type = pending[3];
        const flags = pending[4];
        const body = pending.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
        pending = pending.subarray(f.HEADER_LEN + len);
        if (type === f.HEADERS && (flags & 0x4)) {
          const flat = [];
          decoder.decode(body, 0, body.length, flat);
          const fields = [];
          for (let i = 0; i < flat.length; i += 2) {
            fields.push([flat[i], flat[i + 1]]);
          }
          seen.push({ bytes: body.length, fields });
          if (seen.length === paths.length) {
            resolve();
          }
        }
      }
    });
  });
  await new Promise((resolve) => sock.on('connect', resolve));
  const out = [];
  // RFC 9113 3.4: the client's first frame must be its SETTINGS, and node's stack
  // terminates a connection that starts any other way -- so a raw client that goes
  // straight to HEADERS gets GOAWAY(PROTOCOL_ERROR) "SETTINGS expected" and never sees a
  // response. The settings themselves are the defaults; only their presence matters here.
  out.push(frame(f.SETTINGS, 0, 0, Buffer.alloc(0)));
  for (let i = 0; i < paths.length; i++) {
    out.push(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1 + i * 2,
      requestBlock(paths[i])));
  }
  sock.write(Buffer.concat([f.PREFACE, ...out]));
  await done;
  sock.destroy();
  return { seen, tableSize: decoder.size };
}

test('a repeating response shape settles down to one octet per header line', async (t) => {
  const srv = shapeServer();
  t.after(() => srv.close());
  const port = await listen(srv);
  // /a warms up and lands in the cache, then /b and /c are inserted on top of it --
  // every index /a names moves -- and /a has to come back correct anyway.
  const paths = ['/a', '/a', '/a', '/b', '/b', '/a', '/c', '/c', '/a',
    '/a', '/b', '/b', '/c', '/c', '/a', '/b', '/c', '/a'];

  const { seen, tableSize } = await driveRaw(port, paths);
  assert.strictEqual(seen.length, paths.length);

  // Every response, including the ones whose indices had just moved, must still decode
  // to the fields the server meant. A table that drifted cannot produce this.
  for (let i = 0; i < paths.length; i++) {
    assert.deepStrictEqual(seen[i].fields,
      Object.entries(SHAPES[paths[i]]).map(([n, v]) => [n, String(v)]),
      `response ${i} on ${paths[i]}`);
  }
  // First sighting is a literal, the second inserts, the rest reference: the same shape
  // must get dramatically smaller, and the peer's table must have grown to hold it.
  const first = seen[0].bytes;
  const last = seen[seen.length - 1].bytes;
  assert.ok(first >= 20, `the first /a block was only ${first} bytes: nothing to win`);
  assert.ok(last <= 6, `the settled /a block is ${last} bytes, expected ~3 indexed octets`);
  assert.ok(tableSize > 0, 'the peer stored none of what we inserted');
});

test('node\'s nghttp2 decodes every block of an interleaved warm table', async (t) => {
  // The differential judge: not our own decoder, but the real one.
  const srv = shapeServer();
  t.after(() => srv.close());
  const port = await listen(srv);
  const client = connect(port);
  t.after(() => client.destroy());

  for (let r = 0; r < 4; r++) {
    for (const p of ['/a', '/b', '/c']) {
      const res = await request(client, {
        ':method': 'GET', ':path': p, ':scheme': 'http', ':authority': 'localhost'
      });
      const want = SHAPES[p];
      assert.strictEqual(res.headers[':status'], Number(want[':status']), `${p} round ${r}`);
      assert.strictEqual(res.headers['content-type'], want['content-type'], p);
      assert.strictEqual(res.headers['x-kind'], want['x-kind'], p);
    }
  }
});

test('two sessions with different table sizes do not share block bytes', async (t) => {
  // The peer that advertises no room must not be handed the other session's indexed
  // references, so the block cache has to live on the session. Both clients get the
  // same frozen response objects, which is what a shared cache would have exploited.
  const srv = shapeServer();
  t.after(() => srv.close());
  const port = await listen(srv);
  const http2 = require('node:http2');
  const wide = http2.connect(`http://127.0.0.1:${port}`);
  const narrow = http2.connect(`http://127.0.0.1:${port}`,
    { settings: { headerTableSize: 0 } });
  t.after(() => {
    wide.destroy(); narrow.destroy();
  });
  const errors = [];
  wide.on('error', (e) => errors.push(`wide: ${e.message}`));
  narrow.on('error', (e) => errors.push(`narrow: ${e.message}`));

  const get = (client) => new Promise((resolve, reject) => {
    const req = client.request({ ':method': 'GET', ':path': '/a', ':scheme': 'http',
      ':authority': 'localhost' });
    const h = {};
    req.on('response', (headers) => Object.assign(h, headers));
    req.resume();
    req.on('end', () => resolve(h));
    req.on('error', reject);
    setTimeout(() => reject(new Error('no response')), 4000);
  });

  // Interleaved on one server, so each session encodes the same frozen object against
  // its own table, and the narrow one goes first: its bytes are the literals that a
  // shared cache would have replaced with the wide session's references.
  for (let r = 0; r < 3; r++) {
    const narrowH = await get(narrow);
    const wideH = await get(wide);
    assert.strictEqual(narrowH['x-kind'], 'alpha', `narrow round ${r}`);
    assert.strictEqual(wideH['x-kind'], 'alpha', `wide round ${r}`);
  }
  assert.deepStrictEqual(errors, []);
});

test('the block cache does not swallow a dynamic table size update', () => {
  // A cached block is only a valid answer while nothing has to be announced with it:
  // RFC 7541 6.3 wants the new size named at the start of the next field block.
  const session = { _encoder: new Encoder(4096), _blocks: new WeakMap() };
  const frozen = Object.freeze({ ':status': 200, 'x-kind': 'alpha' });
  // The decoder follows the same blocks, so that its table is the encoder's table: a
  // block that names an index the decoder never heard of decodes to nothing.
  const dec = new h2lean.HPack.Decoder(4096);
  const decode = (buf) => {
    const flat = [];
    dec.decode(buf, 0, buf.length, flat);
    return flat;
  };
  let block;
  for (let i = 0; i < 3; i++) {
    block = hdr.block(frozen, undefined, null, session);
    decode(block);
  }
  const cached = session._blocks.get(frozen);
  assert.ok(cached, `a warm shape belongs in the cache, last block was ${block.length}B`);
  assert.notStrictEqual(cached.bytes[0] & 0xe0, 0x20, 'nothing to announce yet');
  assert.strictEqual(hdr.block(frozen, undefined, null, session), cached.bytes,
    'a hit is the cached Buffer itself');

  // The peer raises its advertised size above our cap: our table does not move, but the
  // size it assumes for it does, so the next block has to correct that.
  session._encoder.setPeerTableSize(8192);
  const resized = hdr.block(frozen, undefined, null, session);
  assert.strictEqual(resized[0] & 0xe0, 0x20, 'the size update went out with the block');
  assert.ok(resized.length > cached.bytes.length, 'and it is not part of the cached bytes');
  assert.strictEqual(session._encoder.pendingSize, null, 'announced once');
  // The announcement and the block are one buffer, the announcement in front of it
  // (RFC 7541 6.3). Decoding the whole thing says both at once: a decoder that read
  // the update as a field line, or a block that started before the update's octets,
  // would not produce these fields.
  assert.deepStrictEqual(decode(resized), [':status', '200', 'x-kind', 'alpha']);

  const again = hdr.block(frozen, undefined, null, session);
  assert.strictEqual(again, cached.bytes, 'and the cache is back, announcing nothing');
});

test('a peer that shrinks the table gets the announcement with the next block', async (t) => {
  // The judge is node's nghttp2, not our own decoder: it rejects a block whose size
  // update is not at its start with COMPRESSION_ERROR, and it rejects a size larger
  // than the table it gave us. A response that comes back whole is therefore a
  // response we announced correctly -- and the announcement rides with the block
  // itself, in one buffer (lib/headers.js block()).
  const srv = shapeServer();
  t.after(() => srv.close());
  const port = await listen(srv);
  const http2 = require('node:http2');
  const client = http2.connect(`http://127.0.0.1:${port}`,
    { settings: { headerTableSize: 1024 } });
  t.after(() => client.destroy());
  const errors = [];
  client.on('error', (e) => errors.push(e.message));

  // Two rounds: the first carries the announcement (the peer's SETTINGS shrink our
  // table before it reaches the request), the second is a block that announces
  // nothing, because the change was already announced.
  for (let r = 0; r < 2; r++) {
    const res = await request(client, {
      ':method': 'GET', ':path': '/a', ':scheme': 'http', ':authority': 'localhost'
    });
    assert.strictEqual(res.headers[':status'], 200, `round ${r}`);
    assert.strictEqual(res.headers['content-type'], SHAPES['/a']['content-type'], `round ${r}`);
    assert.strictEqual(res.headers['x-kind'], 'alpha', `round ${r}`);
  }
  assert.deepStrictEqual(errors, []);
});

test('a peer with no table room is never sent an insertion', () => {
  // maxSize 0 is what the narrow session of the earlier test ends up with. An insertion
  // would be legal on the wire -- both sides empty their table at once -- but it would
  // also bump the epoch and clear the caches on every single response, so the shape
  // never becomes a cache hit. The table must not be touched at all.
  const session = { _encoder: new Encoder(0), _blocks: new WeakMap() };
  const frozen = Object.freeze({ ':status': 200, 'x-kind': 'alpha', 'x-again': 'beta' });
  let last;
  for (let i = 0; i < 5; i++) {
    last = Buffer.from(hdr.block(frozen, undefined, null, session));
  }
  const enc = session._encoder;
  assert.strictEqual(enc.size, 0, 'our table stayed empty');
  assert.strictEqual(enc.dyn.length, 0);
  assert.strictEqual(enc.epoch, 0, 'and it was never churned');
  assert.ok(enc.cache.size > 0, 'the lines did get cached');
  assert.ok(session._blocks.get(frozen), 'the block did get cached');
  const dec = new h2lean.HPack.Decoder(0);
  const flat = [];
  dec.decode(last, 0, last.length, flat);
  assert.deepStrictEqual(flat, [':status', '200', 'x-kind', 'alpha', 'x-again', 'beta']);
});
