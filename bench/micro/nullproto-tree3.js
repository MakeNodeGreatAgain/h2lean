// The realistic shape of the transition tree: maps with SEVERAL fields, and a
// distinct SET of names per request.
//
//   node --expose-gc --allow-natives-syntax out/nullproto-tree3.js
//
// The one-name-per-object test above grows a flat tree. A header map has four
// to eight fields, so the tree has depth: root -> {a} -> {a,b} -> {a,b,c}.
// The question is the same -- does the tree stay bounded when a peer sends a
// request whose header NAME SET differs every time?
//
// This is the shape of the attack that matters: the inbound map in
// lib/session.js _toObject is built from the names a peer chose, and every
// request may carry a different set.
'use strict';

const FIELDS = 6;

function buildDistinct(i) {
  const o = Object.setPrototypeOf({}, null);
  for (let f = 0; f < FIELDS; f++) o['f' + f + '-' + i] = 'v';
  return o;
}

function buildFixed(i) {
  const o = Object.setPrototypeOf({}, null);
  for (let f = 0; f < FIELDS; f++) o['field-' + f] = 'v';
  return o;
}

function heap() {
  gc();
  gc();
  return process.memoryUsage().heapUsed;
}

console.log(`--- ${FIELDS} fields per map`);

const base = heap();
console.log('  the heap at the start:', (base / 1048576).toFixed(2), 'MB');

for (const n of [1000, 10000, 100000, 300000]) {
  for (let i = 0; i < n; i++) buildDistinct(i);
  console.log(`  after ${String(n).padStart(7)} distinct name sets: ` +
    `${(heap() / 1048576).toFixed(2)} MB`);
}

// And a long run on top of that: does it keep growing?
console.log('\n--- does it keep growing?');
for (const round of [1, 2, 3, 4, 5]) {
  for (let i = 0; i < 300000; i++) buildDistinct(round * 1000000 + i);
  console.log(`  round ${round}: ${(heap() / 1048576).toFixed(2)} MB`);
}

// The fixed-name case for the comparison: the tree cannot grow, so any
// difference is the tree.
console.log('\n--- the same count with the SAME names');
const before = heap();
for (let i = 0; i < 1500000; i++) buildFixed(i);
console.log(`  1500000 fixed-name maps: ${((heap() - before) / 1048576).toFixed(2)} MB`);

// Correctness, one last time.
const o = buildDistinct(42);
let ok = true;
for (let f = 0; f < FIELDS; f++) if (o['f' + f + '-42'] !== 'v') ok = false;
console.log('\n  a map built late still reads back:', ok);
console.log('  its prototype is null:', Object.getPrototypeOf(o) === null);
console.log('  it is fast:', %HasFastProperties(o));