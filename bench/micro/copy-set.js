'use strict';
// Buffer.copy(dst, off) against dst.set(src, off) for the small buffers the
// header path copies: a field line is 5 to 30 octets.
//
//   node bench/micro/copy-set.js
const N = 2000000;
const dst = Buffer.allocUnsafe(256);
const src = Buffer.allocUnsafe(12);
src.write('0123456789ab');

function bench(name, fn) {
  for (let i = 0; i < 100000; i++) {
    fn();
  }
  const t0 = process.hrtime.bigint();
  let sink = 0;
  for (let i = 0; i < N; i++) {
    sink += fn();
  }
  const ns = Number(process.hrtime.bigint() - t0) / N;
  console.log(`${name.padEnd(22)} ${ns.toFixed(2)} ns  (sink ${sink & 1})`);
}

bench('copy()', () => {
  src.copy(dst, 8); return dst[8];
});
bench('set()', () => {
  dst.set(src, 8); return dst[8];
});
bench('copy()', () => {
  src.copy(dst, 8); return dst[8];
});
bench('set()', () => {
  dst.set(src, 8); return dst[8];
});
