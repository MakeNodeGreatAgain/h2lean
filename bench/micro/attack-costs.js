// What would each of the heavy functions cost if it were attacked?
//
//   node bench/micro/attack-costs.js
//
// One section per candidate, each with the CURRENT implementation as the first
// arm and the candidate next to it, so the delta is read off a single run.
//
//   1. the shape-cache key      (lib/session.js hashBlock)
//   2. the header map build     (lib/session.js _toObject)
//   3. the outbound parse       (lib/headers.js parse)
//   4. the compat setHeader     (lib/compat.js _setHeader)
//   5. Buffer.from(str) vs allocUnsafe + write   (the claim to check)
'use strict';

const N = 1000000;

function bench(label, fn) {
  for (let i = 0; i < 100000; i++) {
    fn();
  }
  globalThis.sink = null;
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < N; i++) {
    fn();
  }
  const t1 = process.hrtime.bigint();
  const ns = Number(t1 - t0) / N;
  console.log(`  ${label.padEnd(46)} ${ns.toFixed(1)} ns`);
  return ns;
}

// ---------------------------------------------------------------------------
// 1. the shape-cache key
// ---------------------------------------------------------------------------
// The cache is a Map keyed by a hash of the block's octets. The hash walks every
// byte in JS; the entry it finds is then compared with Buffer.equals, which is a
// native memcmp. So the block is read twice, once byte by byte in JS and once
// natively. The candidates hash fewer bytes: a collision is not a correctness
// problem, because the entry is compared before it is trusted.
console.log('1. the shape-cache key, over a 72-byte block');

const block = Buffer.from([
  0x82, 0x86, 0x84, 0x41, 0x0f, 0x77, 0x77, 0x77, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70,
  0x6c, 0x65, 0x2e, 0x63, 0x6f, 0x6d, 0x83, 0x2f, 0x61, 0x62, 0x63, 0x2f, 0x64, 0x65,
  0x66, 0x2f, 0x67, 0x68, 0x69, 0x6a, 0x3f, 0x78, 0x3d, 0x31, 0x87, 0x5b, 0x3a, 0x2f,
  0x2f, 0x68, 0x32, 0x63, 0x2e, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x73, 0x74,
  0x3a, 0x38, 0x30, 0x38, 0x30, 0x30, 0x40, 0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x00, 0x00
]);

function hashBlock(buf) { // today's
  let h = 0x811c9dc5;
  for (let i = 0; i < buf.length; i++) {
    h = Math.imul(h ^ buf[i], 0x01000193);
  }
  return h >>> 0;
}

function hashBlockSample(buf) { // length + 16 bytes spread over it
  const len = buf.length;
  if (len === 0) {
    return 0;
  }
  let h = Math.imul(len, 0x01000193);
  const step = (len / 16) | 0;
  for (let i = 0; i < len; i += step) {
    h = Math.imul(h ^ buf[i], 0x01000193);
  }
  h = Math.imul(h ^ buf[len - 1], 0x01000193);
  return h >>> 0;
}

function hashBlockHead(buf) { // the first 16 bytes and the length
  const len = buf.length;
  const n = len < 16 ? len : 16;
  let h = Math.imul(len, 0x01000193);
  for (let i = 0; i < n; i++) {
    h = Math.imul(h ^ buf[i], 0x01000193);
  }
  return h >>> 0;
}

bench('today: FNV over every byte', () => hashBlock(block));
bench('sample: length + 16 bytes spread over it', () => hashBlockSample(block));
bench('head: length + the first 16 bytes', () => hashBlockHead(block));
bench('the comparison it replaces (Buffer.equals)', () => block.equals(block));

// ---------------------------------------------------------------------------
// 2. the header map build
// ---------------------------------------------------------------------------
// _toObject walks the flat field list and, for every field, READS the map to
// see whether the name is already there, then compares the name against
// ':status', 'set-cookie' and 'cookie'. A shape that has no duplicate names and
// none of those three names needs none of it -- and the shape cache already
// knows the field list before the map is built.
console.log('\n2. the header map build, six fields');

const FLAT = [':method', 'GET', ':path', '/abc/def/ghij?x=1', ':scheme', 'http',
  ':authority', 'localhost', 'user-agent', 'probe/1.0', 'accept', '*/*'];

function nullProto() {
  return Object.setPrototypeOf({}, null);
}

function toObjectToday(flat) { // today's
  const o = nullProto();
  for (let i = 0; i < flat.length; i += 2) {
    const name = flat[i];
    let value = flat[i + 1];
    if (name === ':status') {
      value = value | 0;
    }
    const have = o[name];
    if (have === undefined) {
      o[name] = name === 'set-cookie' ? [value] : value;
    } else if (name === 'cookie') {
      o[name] = `${have}; ${value}`;
    } else if (name === 'set-cookie') {
      have.push(value);
    } else {
      o[name] = `${have}, ${value}`;
    }
  }
  return o;
}

function toObjectSimple(flat) { // no reads, no compares
  const o = nullProto();
  for (let i = 0; i < flat.length; i += 2) {
    o[flat[i]] = flat[i + 1];
  }
  return o;
}

const simple = toObjectToday(FLAT);
bench('today: read-then-store, three name compares', () => toObjectToday(FLAT));
bench('simple: store only', () => toObjectSimple(FLAT));
bench('the map alone, no fields', () => nullProto());
console.log(`  (the two maps agree: ${JSON.stringify(simple) === JSON.stringify(toObjectSimple(FLAT))})`);

// ---------------------------------------------------------------------------
// 3. the outbound parse
// ---------------------------------------------------------------------------
// parse() walks the application's header object: Object.keys() allocates an
// array, every key is lowercased (an allocation when the key is not already
// lower case) and every value goes through String() and a regular expression.
console.log('\n3. the outbound parse, four headers');

const RESP = { 'content-type': 'text/plain', 'content-length': '0', 'x-a': '1', date: 'Mon, 01 Jan 2024 00:00:00 GMT' };
const badValue = /[\u0000-\u0008\u000a\u000b\u000c\u000e-\u001f\u007f]/;

bench('Object.keys()', () => Object.keys(RESP));
bench('key.toLowerCase() on a lower-case key', () => 'content-type'.toLowerCase());
bench('String(value) on a string value', () => String('text/plain'));
bench('badValue.test(value)', () => badValue.test('text/plain'));
bench('the whole parse today', () => {
  const keys = Object.keys(RESP);
  const fields = [];
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i].toLowerCase();
    const v = String(RESP[keys[i]]);
    if (badValue.test(v)) {
      throw new Error('bad');
    }
    fields.push(key, v, false);
  }
  return fields;
});

// ---------------------------------------------------------------------------
// 4. the compat setHeader
// ---------------------------------------------------------------------------
console.log('\n4. the compat setHeader name normalisation');

bench('name.trim().toLowerCase()', () => 'Content-Type'.trim().toLowerCase());
bench('String(name).trim().toLowerCase()', () => String('Content-Type').trim().toLowerCase());
const LOWER = new Map([['Content-Type', 'content-type']]);
bench('a bounded Map of the normalised names', () => LOWER.get('Content-Type'));

// ---------------------------------------------------------------------------
// 5. Buffer.from(str) vs allocUnsafe + write
// ---------------------------------------------------------------------------
// The claim to check: Buffer.from(str) zero-fills and copies, and pairing
// allocUnsafe() with .write() is faster. h2lite's own hot paths do not encode
// strings into buffers -- the HPACK encoder writes latin1 straight into the
// destination -- but an application writing a string body does go through
// Buffer.from.
console.log('\n5. Buffer.from(str) vs allocUnsafe + write');

for (const [label, str] of [['8 bytes', 'hello'], ['64 bytes', 'x'.repeat(64)],
  ['4 KB', 'y'.repeat(4096)]]) {
  bench(`Buffer.from(str), ${label}`, () => Buffer.from(str));
  bench(`allocUnsafe + write, ${label}`, () => {
    const b = Buffer.allocUnsafe(Buffer.byteLength(str));
    b.write(str);
    return b;
  });
}
