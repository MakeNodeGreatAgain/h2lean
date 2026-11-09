// Differential test of the HPACK codec against the real nghttp2 that node:http2
// embeds, in both directions:
//
//  1. encoder vs nghttp2's decoder: a raw h2c responder emits header blocks
//     built by lib/hpack, and a node:http2 *client* must see the exact values.
//  2. decoder vs nghttp2's encoder: the same client's HEADERS payloads are
//     recorded off the wire and decoded by lib/hpack; the result must equal what
//     a node:http2 *server* reports for the very same requests.
//
// Driving the identical client twice is what makes this differential: nobody
// hand-writes the expected header list.
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const http2 = require('node:http2');
const { Decoder, Encoder } = require('../lib/hpack');

const REQUESTS = 6;
const REQ_HEADERS = {
  ':method': 'GET',
  ':path': '/abc/def/ghij?x=1',
  'user-agent': 'probe/1.0',
  accept: '*/*',
  'x-trace': '0123456789abcdef0123456789abcdef',
  cookie: 'a=1; b=2; c=3'
};

const RESP_FIELDS = [
  [':status', '200', false],
  ['content-type', 'text/plain; charset=utf-8', false],
  ['server', 'h2lite/0.1', false],
  ['x-long-header', 'y'.repeat(200), false],
  ['set-cookie', 'sid=abc123; Path=/; HttpOnly', true] // sensitive -> never index
];

function frame(type, flags, sid, payload) {
  const len = payload ? payload.length : 0;
  const h = Buffer.allocUnsafe(9 + len);
  h[0] = len >> 16; h[1] = len >> 8; h[2] = len;
  h[3] = type; h[4] = flags;
  h.writeUInt32BE(sid, 5);
  if (len) {
    payload.copy(h, 9);
  }
  return h;
}

// ---- the response block, produced exclusively by lib/hpack --------------------
const enc = new Encoder();
const buf = Buffer.allocUnsafe(8192);
let off = 0;
for (const [n, v, never] of RESP_FIELDS) {
  off = enc.write(buf, off, n, v, never);
}
const RESP_BLOCK = Buffer.from(buf.subarray(0, off));

const OUR_SETTINGS = Buffer.alloc(12);
OUR_SETTINGS.writeUInt16BE(0x1, 0); OUR_SETTINGS.writeUInt32BE(1 << 20, 2);
OUR_SETTINGS.writeUInt16BE(0x5, 6); OUR_SETTINGS.writeUInt32BE(1 << 20, 8);

// ---- raw h2c responder: records the request HEADERS payloads ------------------
function startResponder(onHeaders) {
  return new Promise((resolve) => {
    const sockets = new Set();
    const srv = net.createServer((sock) => {
      sockets.add(sock);
      let preface = 0;
      let rest = Buffer.alloc(0);
      let decoder = new Decoder(4096);
      const flat = [];
      sock.setNoDelay(true);
      sock.on('error', () => {});
      sock.on('close', () => sockets.delete(sock));
      sock.write(frame(4, 0, 0, OUR_SETTINGS));
      sock.on('data', (d) => {
        if (preface < 24) {
          const take = Math.min(24 - preface, d.length);
          preface += take;
          d = d.subarray(take);
          if (!d.length) {
            return;
          }
        }
        let b = rest.length ? Buffer.concat([rest, d]) : d;
        for (;;) {
          if (b.length < 9) {
            break;
          }
          const len = (b[0] << 16) | (b[1] << 8) | b[2];
          if (b.length < 9 + len) {
            break;
          }
          const type = b[3];
          const flags = b[4];
          const sid = b.readUInt32BE(5) & 0x7fffffff;
          const payload = b.subarray(9, 9 + len);
          if (type === 4 && !(flags & 0x1)) {
            // the peer's advertised SETTINGS_HEADER_TABLE_SIZE bounds the table
            // *we* must maintain while decoding its blocks
            for (let i = 0; i + 6 <= len; i += 6) {
              if (payload.readUInt16BE(i) === 0x1) {
                decoder = new Decoder(payload.readUInt32BE(i + 2));
              }
            }
            sock.write(frame(4, 0x1, 0, null));
          } else if (type === 1 && (flags & 0x4)) {
            const n = decoder.decode(payload, 0, payload.length, flat);
            onHeaders(flat.slice(0, n * 2));
            sock.write(frame(1, 0x5, sid, RESP_BLOCK));
          }
          b = b.subarray(9 + len);
        }
        rest = b;
      });
    });
    srv.listen(0, '127.0.0.1', () => {
      srv._sockets = sockets;
      resolve(srv);
    });
  });
}

function stop(srv) {
  for (const s of srv._sockets) {
    s.destroy();
  }
  srv.close();
}

function drive(port, count) {
  return new Promise((resolve, reject) => {
    const client = http2.connect(`http://127.0.0.1:${port}`);
    client.on('error', reject);
    const got = [];
    let i = 0;
    const one = () => {
      const req = client.request(REQ_HEADERS);
      req.on('response', (h) => got.push(h));
      req.resume();
      req.on('end', () => {
        if (++i < count) {
          one();
        } else {
          client.close(); setImmediate(() => resolve(got));
        }
      });
      req.on('error', reject);
    };
    one();
  });
}

test('hpack encoder output is accepted by nghttp2', async () => {
  const srv = await startResponder(() => {});
  const responses = await drive(srv.address().port, REQUESTS);
  stop(srv);

  assert.strictEqual(responses.length, REQUESTS);
  for (const h of responses) {
    assert.strictEqual(h[':status'], 200);
    assert.strictEqual(h['content-type'], 'text/plain; charset=utf-8');
    assert.strictEqual(h.server, 'h2lite/0.1');
    assert.strictEqual(h['x-long-header'], 'y'.repeat(200));
    assert.deepStrictEqual(h['set-cookie'], ['sid=abc123; Path=/; HttpOnly']);
  }
});

test('hpack decoder reproduces what node:http2 reports for the same wire bytes', async () => {
  // (a) node's own decode of the client's blocks
  const nativeHeaders = await new Promise((resolve, reject) => {
    const out = [];
    const srv = http2.createServer();
    srv.on('stream', (stream, headers) => {
      out.push({ ...headers });
      stream.respond({ ':status': '200' }, { endStream: true });
      if (out.length === REQUESTS) {
        srv.close();
        setImmediate(() => resolve(out));
      }
    });
    srv.on('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      drive(srv.address().port, REQUESTS).catch(reject);
    });
  });

  // (b) lib/hpack's decode of the same client's wire bytes
  const ours = [];
  const srv2 = await startResponder((flat) => {
    const o = { __proto__: null };
    for (let i = 0; i < flat.length; i += 2) {
      o[flat[i]] = flat[i + 1];
    }
    ours.push(o);
  });
  await drive(srv2.address().port, REQUESTS);
  stop(srv2);

  assert.strictEqual(ours.length, REQUESTS);
  for (let i = 0; i < REQUESTS; i++) {
    // the two runs listen on different ephemeral ports, so :authority is the one
    // field that legitimately differs; check its shape, then compare the rest
    assert.match(String(ours[i][':authority']), /^127\.0\.0\.1:\d+$/);
    const mine = { ...ours[i] };
    const theirs = { ...nativeHeaders[i] };
    delete mine[':authority'];
    delete theirs[':authority'];
    assert.deepStrictEqual(Object.keys(mine).sort(), Object.keys(theirs).sort(),
      `request ${i}: field set`);
    for (const k of Object.keys(theirs)) {
      assert.strictEqual(String(mine[k]), String(theirs[k]), `request ${i}: field ${k}`);
    }
  }
});
