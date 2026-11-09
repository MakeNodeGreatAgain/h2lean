// Cost of the per-response header path when NOTHING can be cached: every response
// carries a different header set, which is what a production server sees when it
// echoes a request id into the response.
//
//   node bench/micro/hdr-path.js
'use strict';
const hdr = require('../../lib/headers');
const { Encoder } = require('../../lib/hpack');

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

const RESP = {
  ':status': 200,
  'content-type': 'text/plain',
  'server': 'prof',
  'x-trace': '0123456789abcdef0123456789abcdef'
};

const enc = new Encoder(4096, 4096);
// warm the encoder: the field lines have to be in its cache for the write path
for (let i = 0; i < 5; i++) {
  hdr.encode(hdr.parse(RESP, undefined, { sendDate: true }), enc);
}

console.log('--- the whole uncached path ---');
time('parse() only', 200_000, () => {
  globalThis.sink = hdr.parse(RESP, undefined, { sendDate: true });
});
const parsed = hdr.parse(RESP, undefined, { sendDate: true });
time('encode() only (encoder warm)', 200_000, () => {
  globalThis.sink = hdr.encode(parsed, enc);
});
time('encode() only (encoder cold: new field every time)', 200_000, (i) => {
  const p = hdr.parse({ ...RESP, 'x-n': String(i) }, undefined, { sendDate: false });
  globalThis.sink = hdr.encode(p, enc);
});
time('parse() of 5 fields with a fresh value', 200_000, (i) => {
  globalThis.sink = hdr.parse({ ...RESP, 'x-n': String(i) }, undefined, { sendDate: false });
});

console.log('\n--- the pieces of parse() ---');
time('Object.keys(resp)', 200_000, () => {
  globalThis.sink = Object.keys(RESP);
});
time('5x key.toLowerCase()', 200_000, () => {
  let s = null;
  for (const k of Object.keys(RESP)) {
    s = k.toLowerCase();
  }
  globalThis.sink = s;
});
const badValue = /[\u0000-\u0008\u000a\u000b\u000c\u000e-\u001f\u007f]/;
time('5x badValue.test(value)', 200_000, () => {
  let s = null;
  for (const k of Object.keys(RESP)) {
    s = badValue.test(String(RESP[k]));
  }
  globalThis.sink = s;
});
time('5x String(value)', 200_000, () => {
  let s = null;
  for (const k of Object.keys(RESP)) {
    s = String(RESP[k]);
  }
  globalThis.sink = s;
});
time('5x fields.push([k, v, false])', 200_000, () => {
  const fields = [];
  for (const k of Object.keys(RESP)) {
    fields.push([k, RESP[k], false]);
  }
  globalThis.sink = fields;
});
time('5x fields.push into a flat array', 200_000, () => {
  const fields = [];
  for (const k of Object.keys(RESP)) {
    fields.push(k, RESP[k], false);
  }
  globalThis.sink = fields;
});
time('String(200)', 200_000, () => {
  globalThis.sink = String(200);
});

console.log('\n--- the pieces of encode() ---');
const flat = [];
for (const k of Object.keys(RESP)) {
  flat.push(k, RESP[k], false);
}
time('5x cacheKey(name, value, flag)', 200_000, () => {
  let s = null;
  for (let i = 0; i < flat.length; i += 3) {
    s = `${flat[i]}\0${flat[i + 1]}\0${0}`;
  }
  globalThis.sink = s;
});
const cache = new Map();
for (let i = 0; i < flat.length; i += 3) {
  cache.set(`${flat[i]}\0${flat[i + 1]}\0${0}`, Buffer.from([1, 2, 3]));
}
time('5x cache.get(cacheKey)  [string key]', 200_000, () => {
  let s = null;
  for (let i = 0; i < flat.length; i += 3) {
    s = cache.get(`${flat[i]}\0${flat[i + 1]}\0${0}`);
  }
  globalThis.sink = s;
});
const nested = new Map();
for (let i = 0; i < flat.length; i += 3) {
  let inner = nested.get(flat[i]);
  if (inner === undefined) {
    inner = new Map(); nested.set(flat[i], inner);
  }
  inner.set(flat[i + 1], Buffer.from([1, 2, 3]));
}
time('5x nested.get(name).get(value)  [no key string]', 200_000, () => {
  let s = null;
  for (let i = 0; i < flat.length; i += 3) {
    s = nested.get(flat[i]).get(flat[i + 1]);
  }
  globalThis.sink = s;
});
const out = Buffer.allocUnsafe(256);
time('5x bytes.copy(out, off)', 200_000, () => {
  for (let i = 0; i < flat.length; i += 3) {
    cache.get(`${flat[i]}\0${flat[i + 1]}\0${0}`).copy(out, 0);
  }
  globalThis.sink = out[0];
});
console.log('');
