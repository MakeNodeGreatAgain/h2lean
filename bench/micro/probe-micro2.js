// Same three questions as out/probe-micro.js, measured without the noise of the
// surrounding operations:
//   1. the pseudo-header test alone, on one string, three spellings;
//   2. the token test alone, on the field names a request really carries (no
//      pseudo-headers -- those never reach this check), regex vs the VALID_NAMES cache;
//   3. the whole _encodeRequest body: N field lines written into a scratch that is
//      then either concat-ed with the size-update prefix or has the prefix written in
//      front of it by Encoder.sizeUpdate().
//   node bench/micro/probe-micro2.js
'use strict';

const { Encoder } = require('../../lib/hpack');
const { isFieldName } = require('../../lib/headers');

const SINK = { n: 0 };
const TOKEN_RE = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;

function time(label, iters, fn) {
  for (let i = 0; i < iters / 10; i++) {
    fn(i);
  }
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < iters; i++) {
    fn(i);
  }
  const t1 = process.hrtime.bigint();
  console.log(`  ${label.padEnd(40)} ${(Number(t1 - t0) / iters).toFixed(2)} ns`);
}

const ITERS = 5_000_000;

// ---- 1. pseudo test ------------------------------------------------------------
console.log('pseudo-header test on one key, hot string, 5M calls per arm');
const fieldKey = 'content-type';
const pseudoKey = ':method';
for (const [name, key] of [['field name', fieldKey], ['pseudo name', pseudoKey]]) {
  time(`${name}: charCodeAt(0) === 58`, ITERS, () => {
    SINK.n += key.charCodeAt(0) === 58 ? 1 : 0;
  });
  time(`${name}: key[0] === ':'`, ITERS, () => {
    SINK.n += key[0] === ':' ? 1 : 0;
  });
  time(`${name}: charAt(0) === ':'`, ITERS, () => {
    SINK.n += key.charAt(0) === ':' ? 1 : 0;
  });
  time(`${name}: startsWith(':')`, ITERS, () => {
    SINK.n += key.startsWith(':') ? 1 : 0;
  });
}

// ---- 2. token test -------------------------------------------------------------
const NAMES = ['content-type', 'content-length', 'user-agent', 'accept',
  'accept-encoding', 'authorization', 'x-request-id', 'cache-control',
  'cookie', 'if-none-match', 'date', 'x-forwarded-for'];
console.log(`\ntoken test, ${NAMES.length} legal names, 3M calls per arm`);
const N2 = 3_000_000;
time('TOKEN_RE.test (current)', N2, (i) => {
  SINK.n += TOKEN_RE.test(NAMES[i % NAMES.length]) ? 1 : 0;
});
time('isFieldName() -- VALID_NAMES cache', N2, (i) => {
  SINK.n += isFieldName(NAMES[i % NAMES.length]) ? 1 : 0;
});

// ---- 3. the encode + prefix path ------------------------------------------------
// Realistic request: 8 field lines, block ~200 bytes. Both arms do the same encoder
// work per request; they differ only in how the size-update octets get in front.
const LIST = [];
for (const n of NAMES.slice(0, 8)) {
  LIST.push(n, 'some-value-for-this-field');
}
const enc = new Encoder(4096, 4096);
const N3 = 200_000;

// How many bytes the update actually costs when pendingSize is set: a 4096-size
// announcement is 2 octets (5-bit prefix + continuation).
enc.pendingSize = 4096;
console.log(`\nsize-update prefix cost, ${N3} requests per arm`);
time('concat arm: encode + Buffer.concat', N3, () => {
  enc.pendingSize = 4096;
  const update = enc.takeSizeUpdate();
  let cap = 0;
  for (let i = 0; i < LIST.length; i += 2) {
    cap += Encoder.maxBytesOf(LIST[i], LIST[i + 1]);
  }
  const buf = Buffer.allocUnsafe(cap);
  enc.unsettled = false;
  let off = 0;
  for (let i = 0; i < LIST.length; i += 2) {
    off = enc.write(buf, off, LIST[i], LIST[i + 1], false);
  }
  const out = Buffer.concat([update, buf.subarray(0, off)]);
  SINK.n += out.length;
});
time('prefix arm: encode + sizeUpdate into buf', N3, () => {
  enc.pendingSize = 4096;
  const size = enc.pendingSize;
  let cap = 0;
  for (let i = 0; i < LIST.length; i += 2) {
    cap += Encoder.maxBytesOf(LIST[i], LIST[i + 1]);
  }
  const buf = Buffer.allocUnsafe(5 + cap);
  enc.unsettled = false;
  let off = 5;
  for (let i = 0; i < LIST.length; i += 2) {
    off = enc.write(buf, off, LIST[i], LIST[i + 1], false);
  }
  const start = off - enc.sizeUpdate(buf, 0, size);
  enc.pendingSize = null;
  const out = buf.subarray(start, off);
  SINK.n += out.length;
});
time('no-update arm (what the common case costs)', N3, () => {
  let cap = 0;
  for (let i = 0; i < LIST.length; i += 2) {
    cap += Encoder.maxBytesOf(LIST[i], LIST[i + 1]);
  }
  const buf = Buffer.allocUnsafe(cap);
  enc.unsettled = false;
  let off = 0;
  for (let i = 0; i < LIST.length; i += 2) {
    off = enc.write(buf, off, LIST[i], LIST[i + 1], false);
  }
  SINK.n += off;
});
console.log(`\n(sink ${SINK.n})`);
