'use strict';
// What the per-session decode scratch cost before the instance became module-level
// (const FLAT = new FlatList(64) in each session module): heap used by N live
// sessions, each carrying its own FlatList(64) wrapper + 64-slot array, measured
// as heapUsed delta over the retained instances. The module-level instance removes
// exactly this per live session. The instances are parked on globalThis and their
// liveness is re-checked after the second gc(): a module-top-level const is
// unreachable once the module wrapper returns, and the first version of this probe
// measured a heap that had already collected them.
//   node --expose-gc bench/micro/probe-flat-shared.js
const FlatList = require('../../lib/flat-list');

const N = 100000;

global.gc();
const baseline = process.memoryUsage().heapUsed;

const keep = new Array(N);
let sink = 0;
for (let i = 0; i < N; i++) {
  keep[i] = new FlatList(64);
  sink += keep[i].capacity;
}
if (sink !== N * 64) {
  throw new Error('instances lost');
}
globalThis.keep = keep;

global.gc();
const withInstances = process.memoryUsage().heapUsed;
if (globalThis.keep[5000].capacity !== 64 || globalThis.keep[5000].items.length !== 64) {
  throw new Error('instances collected before the second measurement');
}

console.log(`N=${N} FlatList(64) instances retained`);
console.log(`heapUsed delta: ${((withInstances - baseline) / 1048576).toFixed(2)} MiB`);
console.log(`per instance: ~${Math.round((withInstances - baseline) / N)} octets`);
