// What a response-header memo pays when it MISSES -- the case the user describes:
// the headers are unique per response, so nothing is ever reused.
//
//   node bench/micro/memo-cost.js
'use strict';

function time(label, iters, fn) {
  for (let i = 0; i < iters / 10; i++) {
    fn(i);
  }
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < iters; i++) {
    fn(i);
  }
  const t1 = process.hrtime.bigint();
  console.log(`${((Number(t1 - t0) / iters)).toFixed(1).padStart(7)} ns/op   ${label}`);
}

const SRC = {
  'content-type': 'text/plain',
  server: 'prof',
  'x-trace': '0123456789abcdef0123456789abcdef',
  'x-req': 'r-1'
};
const keys = Object.keys(SRC);

time('build the merged object (null proto)', 500_000, () => {
  const merged = { __proto__: null, ':status': 200 };
  for (let i = 0; i < keys.length; i++) {
    merged[keys[i]] = SRC[keys[i]];
  }
  globalThis.sink = merged;
});
time('build + Object.freeze', 500_000, () => {
  const merged = { __proto__: null, ':status': 200 };
  for (let i = 0; i < keys.length; i++) {
    merged[keys[i]] = SRC[keys[i]];
  }
  Object.freeze(merged);
  globalThis.sink = merged;
});
time('Object.freeze of a fresh 5-prop object', 500_000, () => {
  const merged = { __proto__: null, ':status': 200 };
  for (let i = 0; i < keys.length; i++) {
    merged[keys[i]] = SRC[keys[i]];
  }
  globalThis.sink = merged;
  Object.freeze(merged);
});
time('compare 5 fields against a stored object', 500_000, () => {
  const stored = { __proto__: null, ':status': 200 };
  for (let i = 0; i < keys.length; i++) {
    stored[keys[i]] = SRC[keys[i]];
  }
  let same = true;
  for (let i = 0; i < keys.length; i++) {
    if (stored[keys[i]] !== SRC[keys[i]]) {
      same = false; break;
    }
  }
  globalThis.sink = same;
});
const m = new Map();
time('Map.get by identity (a miss)', 500_000, () => {
  globalThis.sink = m.get(SRC);
});
time('Map.set + Map.delete (a bounded table churn)', 500_000, () => {
  m.set(SRC, 1);
  m.delete(m.keys().next().value);
});
console.log('');
