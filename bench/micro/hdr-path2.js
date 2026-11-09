// The per-response header path when the header VALUE is unique (the production case:
// a trace id echoed into the response). Nothing can be cached, so every octet is
// produced from scratch -- this is where the cost lives.
//
//   node bench/micro/hdr-path2.js
'use strict';
const hdr = require('../../lib/headers');
const { Encoder, huffEncode, huffBitLen } = require('../../lib/hpack');

function time(label, iters, fn) {
  for (let i = 0; i < iters / 10; i++) {
    fn(i);
  }
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < iters; i++) {
    fn(i);
  }
  const t1 = process.hrtime.bigint();
  console.log(`${((Number(t1 - t0) / iters)).toFixed(0).padStart(6)} ns/op   ${label}`);
}

const enc = new Encoder(4096, 4096);
const NAME = 'x-trace-id';
const VALUE = '0123456789abcdef0123456789abcdef'; // 32 chars, hex
const dst = Buffer.allocUnsafe(4096);

console.log('--- one field line whose value is new every time ---');
time('Encoder.write (cold: new value)', 200_000, (i) => {
  globalThis.sink = enc.write(dst, 0, NAME, String(i) + VALUE, false);
});
time('Encoder.compile (cold, no write)', 200_000, (i) => {
  globalThis.sink = enc.compile(NAME, String(i) + VALUE, false);
});
time('Encoder.literal (cold)', 200_000, (i) => {
  globalThis.sink = enc.literal(NAME, String(i) + VALUE, 0x00);
});
time('Encoder.literalStrInto (cold)', 200_000, (i) => {
  globalThis.sink = enc.literalStrInto(dst, 0, String(i) + VALUE, 5 * (i + VALUE).length);
});

console.log('\n--- the pieces of literalStr ---');
time('Buffer.from(s, "latin1")', 200_000, (i) => {
  globalThis.sink = Buffer.from(String(i) + VALUE, 'latin1');
});
const raw = Buffer.from(VALUE, 'latin1');
time('huffBitLen(raw)', 200_000, () => {
  globalThis.sink = huffBitLen(raw, 0, raw.length);
});
const hbuf = Buffer.allocUnsafe(64);
time('huffEncode(raw)', 200_000, () => {
  globalThis.sink = huffEncode(raw, 0, raw.length, hbuf, 0);
});
time('Buffer.allocUnsafe(36)', 200_000, () => {
  globalThis.sink = Buffer.allocUnsafe(36);
});
time('Buffer.concat([a,b])', 200_000, () => {
  globalThis.sink = Buffer.concat([hbuf.subarray(0, 4), hbuf.subarray(0, 21)]);
});
time('buf.copy(dst, 0)', 200_000, () => {
  hbuf.copy(dst, 0);
});

console.log('\n--- the pieces of compile() around it ---');
time('cacheKey() string build', 200_000, (i) => {
  globalThis.sink = `${NAME}\u0000${i}${VALUE}\u00000`;
});
const once = new Set();
time('once.add(key) on a growing set', 200_000, (i) => {
  once.add(`${NAME}\u0000${i}${VALUE}\u00000`);
  if (once.size > 8192) {
    once.clear();
  }
});

console.log('\n--- the whole response, one unique field among five ---');
const RESP = {
  ':status': 200,
  'content-type': 'text/plain',
  'server': 'prof',
  'x-other': 'constant',
  'x-trace': '0123456789abcdef0123456789abcdef'
};
for (let i = 0; i < 5; i++) {
  const p = hdr.parse(RESP, undefined, { sendDate: true });
  hdr.encode(p, enc);
}
time('parse() of 5 fields', 200_000, () => {
  globalThis.sink = hdr.parse(RESP, undefined, { sendDate: true });
});
time('parse()+encode() of 5 fields (all stable)', 200_000, () => {
  const p = hdr.parse(RESP, undefined, { sendDate: true });
  globalThis.sink = hdr.encode(p, enc);
});
time('parse()+encode() with 1 fresh value', 200_000, (i) => {
  const p = hdr.parse({ ...RESP, 'x-n': String(i) }, undefined, { sendDate: false });
  globalThis.sink = hdr.encode(p, enc);
});
console.log('');
