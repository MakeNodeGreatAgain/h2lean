'use strict';
// Clearing a reusable array: `arr.length = 0` (keep the object and its backing
// store) against `arr = []` (a fresh array every time). The claim under test:
// length=0 reuses the allocation and costs the GC less. Two metrics per shape:
// ns/op and GC collections counted while the loop runs -- the second is the one
// the claim is really about, since a young-generation allocation is a few ns
// and the time metric can hide the churn it creates.
//   node bench/micro/clear-array.js
const { PerformanceObserver } = require('node:perf_hooks');

// k = how many entries the array holds when it is cleared; the fill comes from
// a source array so both shapes do the same stores.
function makeBench(k) {
  const src = [];
  for (let i = 0; i < k; i++) {
    src.push('item-' + i);
  }
  return {
    reuse() {
      const arr = src.slice();
      globalThis.sink = arr;
      return () => {
        arr.length = 0;
        for (let j = 0; j < k; j++) {
          arr.push(src[j]);
        }
        globalThis.sink = arr;
      };
    },
    fresh() {
      return () => {
        const arr = [];
        for (let j = 0; j < k; j++) {
          arr.push(src[j]);
        }
        globalThis.sink = arr;
      };
    }
  };
}

// Counts GC pauses while fn runs n times. The observer's callback is asynchronous --
// it cannot fire inside a loop that blocks the main thread -- so the entries are
// flushed by yielding to the event loop before the counts are read.
async function measure(label, fn, n) {
  let gcs = 0;
  let gcNs = 0;
  const obs = new PerformanceObserver((list) => {
    for (const e of list.getEntries()) {
      gcs++;
      gcNs += e.duration;
    }
  });
  obs.observe({ entryTypes: ['gc'] });
  for (let i = 0; i < Math.floor(n / 20); i++) {
    fn(i);
  } // warm up / tier up
  gcs = 0;
  gcNs = 0;
  const t0 = Number(process.hrtime.bigint());
  for (let i = 0; i < n; i++) {
    fn(i);
  }
  const ns = (Number(process.hrtime.bigint()) - t0) / n;
  await new Promise((r) => setImmediate(r));
  await new Promise((r) => setImmediate(r));
  obs.disconnect();
  console.log(`${ns.toFixed(1).padStart(8)} ns/op   ${String(gcs).padStart(3)} GCs ` +
    `(${String(gcNs).padStart(6)} ms paused)   ${label}`);
}

const N = 5_000_000;
(async () => {
  console.log(`--- ${N.toLocaleString('en')} ops, fill to k entries, then clear ---`);
  for (const k of [2, 8, 64, 4096]) {
    const { reuse, fresh } = makeBench(k);
    const n = k > 64 ? Math.floor(N / 16) : N;
    await measure(`k=${String(k).padStart(4)}  length = 0`, reuse(), n);
    await measure(`k=${String(k).padStart(4)}  arr = []  `, fresh(), n);
  }
})();
