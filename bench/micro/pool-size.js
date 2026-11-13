'use strict';
// Buffer.poolSize against Buffer.allocUnsafe. Node pools an allocation only
// when size < poolSize >>> 1 (node/lib/buffer.js allocate()); at the default
// 8192 that means everything at or above 4096 octets is a direct external
// allocation. Two questions, both answered per poolSize and per size:
//   1. cost: ns/alloc and GC collections while allocating and dropping;
//   2. the pinning hazard: every pooled slice is a view into ONE pool
//      ArrayBuffer, so a single small slice kept alive retains the whole
//      poolSize -- measured as process.memoryUsage().arrayBuffers.
//   node bench/micro/pool-size.js
const { PerformanceObserver } = require('node:perf_hooks');

async function measure(poolSize, size, n) {
  Buffer.poolSize = poolSize;
  let sink;
  let gcs = 0;
  let gcNs = 0;
  const obs = new PerformanceObserver((list) => {
    for (const e of list.getEntries()) {
      gcs++;
      gcNs += e.duration;
    }
  });
  obs.observe({ entryTypes: ['gc'] });
  for (let i = 0; i < Math.floor(n / 10); i++) {
    sink = Buffer.allocUnsafe(size);
  }
  gcs = 0;
  gcNs = 0;
  const t0 = Number(process.hrtime.bigint());
  for (let i = 0; i < n; i++) {
    sink = Buffer.allocUnsafe(size);
  }
  const ns = (Number(process.hrtime.bigint()) - t0) / n;
  await new Promise((r) => setImmediate(r));
  await new Promise((r) => setImmediate(r));
  obs.disconnect();
  console.log(`${String(poolSize).padStart(6)}  ${String(size).padStart(6)}  ` +
    `${ns.toFixed(1).padStart(8)} ns/alloc   ${String(gcs).padStart(4)} GCs ` +
    `(${gcNs.toFixed(0).padStart(6)} ms)`);
  return sink;
}

(async () => {
  const N = 2_000_000;
  console.log('--- alloc + drop, per poolSize and size (threshold is poolSize >>> 1) ---');
  for (const poolSize of [8192, 32768, 65536]) {
    for (const size of [100, 4095, 4096, 8192]) {
      await measure(poolSize, size, size > 4096 ? Math.floor(N / 8) : N);
    }
  }

  // Pinning: one small pooled slice held alive pins the pool it came from.
  // A fresh pool per round is forced by allocating poolSize worth of slices
  // first and keeping only one.
  console.log('--- one 100-octet slice kept alive per fresh pool, arrayBuffers after gc() ---');
  // run with --expose-gc: the pool is external memory, only a full gc releases it
  const gc = globalThis.gc;
  if (gc === undefined) {
    throw new Error('run with node --expose-gc');
  }
  const sink = () => {};
  for (const poolSize of [8192, 65536]) {
    Buffer.poolSize = poolSize;
    gc();
    let keep;
    for (let i = 0; i < Math.ceil(poolSize / 100); i++) {
      keep = Buffer.allocUnsafe(100); // the last slice rolls over to a fresh pool
    }
    const pin = keep;
    gc();
    gc(); // the exhausted earlier pools are collected here, before the baseline
    const before = process.memoryUsage().arrayBuffers;
    for (let i = 0; i < 2000; i++) {
      sink(Buffer.allocUnsafe(100));
    }
    gc();
    gc(); // external memory can need a second pass to release
    gc();
    const after = process.memoryUsage().arrayBuffers;
    const retained = after - before;
    console.log(`poolSize ${String(poolSize).padStart(6)}: retained ${retained} octets ` +
      `(pool is ${poolSize}; the 100-octet slice pins all of it)`);
    sink(pin);
  }
})();
