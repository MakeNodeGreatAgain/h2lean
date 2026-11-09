// Scratch probe: how fast is a buffer -> latin1 string, by length and by route?
// The candidates are the ones the HPACK decoder could use:
//   toString('latin1', s, e)      -- what lib/hpack.js uses today
//   latin1Slice(s, e)            -- node's internal one-byte slice, if it is reachable
//   fromCharCode(...)            -- msgpackr's shortStringInJS shape
//   latin1Slice + fromCharCode   -- the same, with the ASCII check msgpackr does
'use strict';

const N = 200000;

function bench(name, fn) {
  fn(); // warm up
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < N; i++) {
    fn(i);
  }
  const t1 = process.hrtime.bigint();
  const ns = Number(t1 - t0) / N;
  console.log(`${name.padEnd(46)} ${ns.toFixed(1)} ns/op`);
}

console.log('node', process.version);
console.log('Buffer.prototype.latin1Slice:', typeof Buffer.prototype.latin1Slice);
console.log('buf.latin1Slice own:', typeof Buffer.alloc(4).latin1Slice);
console.log('');

const src = Buffer.from('user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)');
const latin1Slice = Buffer.prototype.latin1Slice;

for (const len of [1, 2, 3, 4, 8, 16, 32, 55]) {
  const end = len;
  console.log(`--- length ${len}`);
  bench('toString(latin1)', () => src.toString('latin1', 0, end));
  bench('latin1Slice', () => latin1Slice.call(src, 0, end));
  if (len <= 3) {
    bench('fromCharCode', () => String.fromCharCode(src[0], src[1], src[2]).slice(0, len));
  }
  bench('fromCharCode.apply', () => {
    const bytes = new Array(len);
    for (let i = 0; i < len; i++) {
      bytes[i] = src[i];
    }
    return String.fromCharCode.apply(String, bytes);
  });
}
