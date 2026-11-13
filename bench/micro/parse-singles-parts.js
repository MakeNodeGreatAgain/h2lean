'use strict';
// Isolates the two shapes of the singles bookkeeping that parse() can use, to
// see what each component costs on its own before judging the merged result.
//   node bench/micro/parse-singles-parts.js
function bench(label, fn, n = 20_000_000) {
  for (let i = 0; i < 1_000_000; i++) {
    fn(i);
  }
  globalThis.sink = null;
  const t0 = Number(process.hrtime.bigint());
  for (let i = 0; i < n; i++) {
    fn(i);
  }
  const ns = (Number(process.hrtime.bigint()) - t0) / n;
  console.log(`${ns.toFixed(1).padStart(8)} ns/op   ${label}`);
}

const arr = [];
bench('new Set() per call', () => {
  globalThis.sink = new Set();
});
bench('arr.length = 0', () => {
  arr.length = 0;
});
bench('Set: has miss + has miss + add + add', () => {
  const s = new Set(['content-type']);
  globalThis.sink = s.has('date') || s.has('etag');
  s.add('date');
  s.add('etag');
});
bench('arr: length0 + indexOf x2 + push x2', () => {
  arr.length = 0;
  arr.push('content-type');
  globalThis.sink = arr.indexOf('date') !== -1 || arr.indexOf('etag') !== -1;
  arr.push('date');
  arr.push('etag');
});
