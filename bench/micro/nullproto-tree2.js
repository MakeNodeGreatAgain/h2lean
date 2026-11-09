// What exactly bounds the transition tree of the shared root map?
//
//   node --expose-gc --allow-natives-syntax out/nullproto-tree2.js
//
// The tree is per PROCESS and the names in it come from the peer. The debug
// print showed "Transitions #1536" after 250000 distinct names, so V8 stopped
// adding to that root -- but the objects stayed fast, which means the new
// names went somewhere else. This finds out where, and measures the retained
// heap of the tree itself (the objects are collected, the tree is not).
'use strict';

function rootTransitions() {
  const root = Object.setPrototypeOf({}, null);
  const s = %DebugPrint(root);
  return s;
}

const root = Object.setPrototypeOf({}, null);

console.log('--- how many distinct names does ONE root accept?');
for (const n of [1000, 10000, 50000, 100000, 300000]) {
  for (let i = 0; i < n; i++) {
    const o = Object.setPrototypeOf({}, null);
    o['name-' + i] = 'v';
  }
  gc();
  const heap = process.memoryUsage().heapUsed;
  console.log(`  after ${String(n).padStart(7)} names: heap ${(heap / 1048576).toFixed(2)} MB`);
}

// The retained heap is what matters: the objects die with the request, the
// tree does not.
console.log('\n--- the retained heap of the tree');
gc();
gc();
const before = process.memoryUsage().heapUsed;
for (let i = 0; i < 300000; i++) {
  const o = Object.setPrototypeOf({}, null);
  o['distinct-' + i] = 'v';
}
gc();
gc();
const after = process.memoryUsage().heapUsed;
console.log(`  300000 distinct names: +${((after - before) / 1048576).toFixed(2)} MB retained`);

// And how much does the tree keep after ANOTHER 300000?
gc();
gc();
const before2 = process.memoryUsage().heapUsed;
for (let i = 0; i < 300000; i++) {
  const o = Object.setPrototypeOf({}, null);
  o['more-distinct-' + i] = 'v';
}
gc();
gc();
const after2 = process.memoryUsage().heapUsed;
console.log(`  another 300000:        +${((after2 - before2) / 1048576).toFixed(2)} MB retained`);

// And after ten times that?
gc();
gc();
const before3 = process.memoryUsage().heapUsed;
for (let i = 0; i < 3000000; i++) {
  const o = Object.setPrototypeOf({}, null);
  o['lots-' + i] = 'v';
}
gc();
gc();
const after3 = process.memoryUsage().heapUsed;
console.log(`  three million more:    +${((after3 - before3) / 1048576).toFixed(2)} MB retained`);

// And the root map now.
console.log('\n--- the root map now');
%DebugPrint(root);

// And the objects are still correct and fast.
const probe = Object.setPrototypeOf({}, null);
probe['final-check'] = 'ok';
console.log('\n  a fresh object reads back:', probe['final-check']);
console.log('  its prototype is null:   ', Object.getPrototypeOf(probe) === null);
console.log('  it is fast:              ', %HasFastProperties(probe));