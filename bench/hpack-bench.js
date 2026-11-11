// HPACK throughput measurement against real wire data.
// Captures HEADERS payloads produced by node:http2's nghttp2 encoder, then times:
//   - our decoder, cold block (literals + Huffman) and warm block (indexed)
//   - object materialisation, node's own way vs a shape cache
//   - our encoder's per-field-line cost on the cache hit path
//   node --expose-internals bench/hpack-bench.js
const net = require('node:net');
const http2 = require('node:http2');
const util = require('node:internal/http2/util');
const { Decoder, Encoder } = require('../lib/hpack');

const N_REQ = 40;
const REQ = {
  ':method': 'GET',
  ':path': '/abc/def/ghij?x=1',
  'user-agent': 'probe/1.0',
  accept: '*/*',
  'x-trace': '0123456789abcdef0123456789abcdef',
  cookie: 'a=1; b=2; c=3'
};

function capture() {
  return new Promise((resolve) => {
    const blocks = [];
    const srv = net.createServer((sock) => {
      let preface = 0;
      sock.setNoDelay(true);
      sock.on('error', () => {});
      sock.on('data', (d) => {
        if (preface < 24) {
          const t = Math.min(24 - preface, d.length); preface += t; d = d.subarray(t);
        }
        let b = d;
        while (b.length >= 9) {
          const len = (b[0] << 16) | (b[1] << 8) | b[2];
          if (b.length < 9 + len) {
            break;
          }
          if (b[3] === 1) {
            blocks.push(Buffer.from(b.subarray(9, 9 + len)));
          }
          b = b.subarray(9 + len);
        }
      });
      srv._sock = sock;
    });
    srv.listen(0, '127.0.0.1', () => {
      const client = http2.connect(`http://127.0.0.1:${srv.address().port}`);
      client.on('error', () => {});
      for (let i = 0; i < N_REQ; i++) {
        client.request(REQ).on('error', () => {});
      }
      setTimeout(() => {
        client.destroy();
        srv._sock.destroy();
        srv.close();
        resolve(blocks);
      }, 400);
    });
  });
}

function time(label, fn, n) {
  for (let i = 0; i < 50_000; i++) {
    fn();
  }
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < n; i++) {
    fn();
  }
  const ns = Number(process.hrtime.bigint() - t0) / n;
  console.log(`${ns.toFixed(0).padStart(6)} ns/op  ${label}`);
  return ns;
}

capture().then((blocks) => {
  if (blocks.length < 5) {
    throw new Error(`only captured ${blocks.length} blocks`);
  }
  const cold = blocks[0];
  const warm = blocks[blocks.length - 1];
  console.log(`captured ${blocks.length} blocks: cold ${cold.length}B, warm ${warm.length}B`);

  // a decoder that has already seen the whole prefix of the stream, so the warm
  // block's dynamic references resolve exactly as they do on a live connection
  const primed = new Decoder(4096);
  const sink = [];
  for (const b of blocks) {
    primed.decode(b, 0, b.length, sink);
  }

  console.log('\n--- decode ---');
  const coldDec = new Decoder(4096);
  time('cold block (literals, huffman, table inserts)', () => coldDec.decode(cold, 0, cold.length, sink), 200_000);
  const warmNs = time('warm block (indexed refs only)      ', () => primed.decode(warm, 0, warm.length, sink), 200_000);
  const fieldCount = sink.length >> 1;
  console.log(`${(warmNs / fieldCount).toFixed(0).padStart(6)} ns/field  (warm path, ${fieldCount} fields)`);

  console.log('\n--- materialise req.headers from the decoded flat array ---');
  const flat = sink.slice(0);
  time('node toHeaderObject(flat)             ', () => {
    globalThis.h = util.toHeaderObject(flat, undefined);
  }, 200_000);
  const CACHE = new Map([['shape', (() => {
    const o = { __proto__: null };
    for (let i = 0; i < flat.length; i += 2) {
      o[flat[i]] = flat[i + 1];
    }
    return Object.freeze(o);
  })()]]);
  time('shape cache: fresh mutable copy     ', () => {
    globalThis.h = Object.assign(Object.create(null), CACHE.get('shape'));
  }, 200_000);

  console.log('\n--- full decode + object, as a server would do it ---');
  time('decode(warm) + toHeaderObject       ', () => {
    primed.decode(warm, 0, warm.length, sink);
    globalThis.h = util.toHeaderObject(sink, undefined);
  }, 200_000);

  console.log('\n--- encode (our field-line cache) ---');
  const enc = new Encoder();
  const dst = Buffer.allocUnsafe(8192);
  const fields = [[':status', '200'], ['content-type', 'text/plain'],
    ['server', 'h2lean'], ['x-trace', '0123456789abcdef0123456789abcdef']];
  for (const [n, v] of fields) {
    enc.write(dst, 0, n, v, false);
  } // warm the cache
  const ourNs = time('4 field lines via Encoder.write       ', () => {
    let off = 0;
    for (const [n, v] of fields) {
      off = enc.write(dst, off, n, v, false);
    }
    globalThis.off = off;
  }, 200_000);
  const obj = { ':status': '200', 'content-type': 'text/plain', server: 'h2lean',
    'x-trace': '0123456789abcdef0123456789abcdef' };
  const noErr = () => undefined;
  const nodeNs = time('node buildNgHeaderString(same 4)      ', () => {
    globalThis.s = util.buildNgHeaderString(obj, noErr, false);
  }, 200_000);
  console.log(`\nencode: ours ${ourNs.toFixed(0)}ns vs node ${nodeNs.toFixed(0)}ns for the same 4 fields ` +
              '(node then still splits that string in C++ and HPACK-encodes it)');
  process.exit(0);
});
