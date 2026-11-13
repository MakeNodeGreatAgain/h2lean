'use strict';
// Preallocation with a cursor (write via arr[c++], reset the cursor to reuse)
// against push and against the length=0 refill. The pattern trades cheaper
// writes for a holey array: new Array(n) stays HOLEY_ELEMENTS forever, so every
// later read carries a hole check push-shaped arrays do not. The metric is
// therefore the whole cycle -- fill, then read every slot back -- not the fill
// alone. This is the shape hpack's decode() out list lives in (fill by push,
// consumed by index), so the fill uses strings from a source array.
//   node bench/micro/prealloc.js
const { PerformanceObserver } = require('node:perf_hooks');

function makeBench(k) {
  const src = [];
  for (let i = 0; i < k; i++) {
    src.push('v-' + i);
  }
  let pre; // the preallocated array persists across ops, cursor reset each op
  const N_SLOTS = 2 * 4096;
  return {
    // fresh array + push, iterate
    fresh() {
      return () => {
        const arr = [];
        for (let j = 0; j < k; j++) {
          arr.push(src[j]);
        }
        let s = 0;
        for (let j = 0; j < arr.length; j++) {
          s += arr[j].length;
        }
        globalThis.sink = s;
      };
    },
    // reused array + length=0 + push, iterate
    refill() {
      const arr = [];
      return () => {
        arr.length = 0;
        for (let j = 0; j < k; j++) {
          arr.push(src[j]);
        }
        let s = 0;
        for (let j = 0; j < arr.length; j++) {
          s += arr[j].length;
        }
        globalThis.sink = s;
      };
    },
    // one preallocated array, cursor writes, cursor reset, iterate over cursor
    cursor() {
      if (pre === undefined) {
        pre = new Array(N_SLOTS);
      }
      return () => {
        let c = 0;
        for (let j = 0; j < k; j++) {
          pre[c++] = src[j];
        }
        let s = 0;
        for (let j = 0; j < c; j++) {
          s += pre[j].length;
        }
        globalThis.sink = s;
      };
    }
  };
}

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
    fn();
  }
  gcs = 0;
  gcNs = 0;
  const t0 = Number(process.hrtime.bigint());
  for (let i = 0; i < n; i++) {
    fn();
  }
  const ns = (Number(process.hrtime.bigint()) - t0) / n;
  await new Promise((r) => setImmediate(r));
  await new Promise((r) => setImmediate(r));
  obs.disconnect();
  console.log(`${ns.toFixed(1).padStart(9)} ns/op   ${String(gcs).padStart(4)} GCs ` +
    `(${gcNs.toFixed(0).padStart(6)} ms)   ${label}`);
}

(async () => {
  const N = 5_000_000;
  console.log(`--- ${N.toLocaleString('en')} ops, fill k slots then read them back ---`);
  for (const k of [4, 20, 64, 4096]) {
    const { fresh, refill, cursor } = makeBench(k);
    const n = k > 64 ? Math.floor(N / 16) : N;
    await measure(`k=${String(k).padStart(4)}  fresh [] + push    `, fresh(), n);
    await measure(`k=${String(k).padStart(4)}  length=0 + push    `, refill(), n);
    await measure(`k=${String(k).padStart(4)}  prealloc + cursor  `, cursor(), n);
  }
})();
