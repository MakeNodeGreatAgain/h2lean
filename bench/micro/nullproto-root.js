// How many transitions does the shared root map carry, and does V8 cap it?
//
//   node --expose-gc --allow-natives-syntax out/nullproto-root.js
//
// This is the question the whole change turns on. A fast null-prototype map
// starts from ONE root map, and every distinct field name a peer sends adds a
// transition to that root's transition tree. The tree is per PROCESS, so a peer
// that sends a new name per request grows the server's memory for the life of
// the process -- the class of growth AGENTS.md's VALID_NAMES_MAX bounds for the
// name cache.
//
// V8 caps a transition array (kMaxNumberOfTransitions) and normalises a map
// whose array is full back to dictionary mode, which would cap the tree. This
// measures whether that cap actually fires for this root, and what the heap
// does while it does not.
'use strict';

const root = Object.setPrototypeOf({}, null);
console.log('the root map, before any store:');
%DebugPrint(root);

// Distinct names, each on its own object, all from the same root.
const N = 200000;
const kept = [];
for (let i = 0; i < N; i++) {
  const o = Object.setPrototypeOf({}, null);
  o['x' + i] = 'v';
  if (i % 100 === 0) kept.push(o);
}

console.log(`\nafter ${N} distinct names, the root map again:`);
%DebugPrint(root);

// Is a FRESH object from that root still fast?
const probe = Object.setPrototypeOf({}, null);
probe['zzz-probe'] = 1;
console.log('\na fresh object is fast:', %HasFastProperties(probe));
console.log('its prototype:', Object.getPrototypeOf(probe));

// The heap, with the strings that carry the names removed from the picture:
// the same names, reused, so only the tree can grow.
console.log('\nthe heap with a FIXED name (the tree cannot grow):');
gc();
const before = process.memoryUsage().heapUsed;
for (let i = 0; i < N; i++) {
  const o = Object.setPrototypeOf({}, null);
  o['fixed-name'] = 'v';
}
gc();
const after = process.memoryUsage().heapUsed;
console.log(`  +${((after - before) / 1048576).toFixed(2)} MB for ${N} objects`);

console.log('\nthe heap with DISTINCT names (the tree grows):');
gc();
const before2 = process.memoryUsage().heapUsed;
for (let i = 0; i < N; i++) {
  const o = Object.setPrototypeOf({}, null);
  o['distinct-' + i] = 'v';
}
gc();
const after2 = process.memoryUsage().heapUsed;
console.log(`  +${((after2 - before2) / 1048576).toFixed(2)} MB for ${N} objects ` +
  `(${((after2 - before2) / N).toFixed(0)} bytes per name)`);