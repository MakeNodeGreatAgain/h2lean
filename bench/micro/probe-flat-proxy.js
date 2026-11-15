'use strict';
// Container-cycle probes for lib/flat-list.js:
//   1. Does the push-based fill cost anything against the shape it replaced?
//      Arms A and B run the exact container cycle decode does -- reset, cursor
//      writes `a[n++] = name; a[n++] = value` for one typical 8-line block, then
//      the consumer's read over items[0..n) -- with (A) a plain preallocated
//      array and the inline tail check, and (B) the FlatList filled through
//      push(), whose bound check doubles as the overflow detector. (The
//      intermediate cursor+end(n) shape was measured at the same granularity at
//      22-25 ns against 21-27 ns for arm A -- parity; OPTIMIZATION.md item 39.)
//   2. What would a Proxy set trap cost instead? Arm C wraps the array in a Proxy
//      whose set trap watches the index; in the real design the fill AND every
//      consumer read would cross the trap, because out.items would be the proxy
//      itself, so both are included.
// Arms alternate ABBA per round, 5 rounds, warmup excluded.
//   node bench/micro/probe-flat-proxy.js

const CAP = 64;
const LINES = 8; // 16 items per fill, a typical request block
const FlatList = require('../../lib/flat-list');

let grownFlag = false;
const handler = {
  set(target, prop, value) {
    const i = +prop;
    if (i >= CAP) {
      grownFlag = true;
    }
    target[prop] = value;
    return true;
  }
};

function cycleDirect(a) {
  let n = 0;
  for (let i = 0; i < LINES; i++) {
    a[n++] = i;
    a[n++] = i ^ 0x5a;
  }
  if (a.length > CAP) {
    // the grown() branch; never taken in this workload
  }
  let s = 0;
  for (let i = 0; i < n; i++) {
    s += a[i];
  }
  return s;
}

function cycleFlat(fl) {
  // identical fill shape; the writes go through the real push()
  fl.reset();
  const a = fl.items;
  for (let i = 0; i < LINES; i++) {
    fl.push(i);
    fl.push(i ^ 0x5a);
  }
  let s = 0;
  for (let i = 0; i < fl.count; i++) {
    s += a[i];
  }
  return s;
}

function cycleProxy(p) {
  // identical fill/read shape; every store and load crosses the trap machinery
  let n = 0;
  for (let i = 0; i < LINES; i++) {
    p[n++] = i;
    p[n++] = i ^ 0x5a;
  }
  let s = 0;
  for (let i = 0; i < n; i++) {
    s += p[i];
  }
  return s;
}

const direct = new Array(CAP);
const flat = new FlatList(CAP);
const proxied = new Proxy(new Array(CAP), handler);

let sink = 0;
for (let w = 0; w < 3e5; w++) {
  sink += cycleDirect(direct) + cycleFlat(flat) + cycleProxy(proxied);
}

const ROUNDS = 5;
const ITERS = 1e6;
const dNs = [];
const fNs = [];
const pNs = [];
for (let r = 0; r < ROUNDS; r++) {
  const order = [[cycleDirect, direct, dNs], [cycleFlat, flat, fNs], [cycleProxy, proxied, pNs]];
  if ((r & 1) === 1) {
    order.reverse();
  }
  for (const [cycle, out, acc] of order) {
    const t = process.hrtime.bigint();
    for (let i = 0; i < ITERS; i++) {
      sink += cycle(out);
    }
    acc.push(Number(process.hrtime.bigint() - t) / ITERS);
  }
}

const fmt = (a) => a.map((v) => v.toFixed(0)).join(' ');
console.log(`direct (array + inline tail check): ${fmt(dNs)} ns/block`);
console.log(`flat   (FlatList push() fill):      ${fmt(fNs)} ns/block`);
console.log(`proxy  (set-trapped array):         ${fmt(pNs)} ns/block`);
console.log(`ratios flat/direct, proxy/direct: ${
  fNs.map((v, i) => (v / dNs[i]).toFixed(2)).join(' ')} / ${
  pNs.map((v, i) => (v / dNs[i]).toFixed(2)).join(' ')}`);
console.log(`sink ${sink}, grownFlag ${grownFlag}`);
