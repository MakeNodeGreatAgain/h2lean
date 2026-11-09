// What does one name normalisation cost, and can a cache beat it?
//
//   node bench/micro/norm-cache.js
//
// Three questions, all on the same input -- the field names a response carries:
//
//   1. What do the pieces of `String(name).trim().toLowerCase()` cost on their
//      own, and does the string that comes out differ from the one that went in?
//      A name that is already lower case and already trimmed -- which is what
//      HTTP/2 requires on the wire (RFC 9113 8.2.1) and what the compat layer
//      stores after setHeader() -- should not allocate.
//   2. Is `String(name)` worth replacing with a `typeof name === 'string'`
//      check? The answer changes what the code is allowed to do with a name
//      that is not a string, so the numbers are printed next to the branch.
//   3. Is a Map lookup cheaper than the scan it replaces? The key decides:
//      an internalised string (a literal, or a property name) has its hash
//      computed once and cached by V8, a fresh string has to be hashed every
//      time -- and the hash of a name costs about as much as scanning it.
//
// Everything is timed per call, ns, with the result fed into a sink so V8
// cannot hoist the work out of the loop.
'use strict';

const SINK = { n: 0 };

function time(label, iters, fn) {
  for (let i = 0; i < iters / 10; i++) {
    fn(i);
  }
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < iters; i++) {
    fn(i);
  }
  const t1 = process.hrtime.bigint();
  console.log(`  ${label.padEnd(46)} ${(Number(t1 - t0) / iters).toFixed(2)} ns`);
}

// The names a response carries. Lower case and trimmed already, because that
// is the shape the compat layer stores them in and the shape HTTP/2 wants.
const LOWER = ['content-type', 'content-length', 'cache-control', 'date',
  'server', 'etag', 'x-request-id'];
const MIXED = ['Content-Type', 'Content-Length', 'Cache-Control', 'Date',
  'Server', 'ETag', 'X-Request-Id'];
const ITERS = 3000000;

console.log(`normalisation, ${ITERS} calls per arm`);

// ---- 1. the pieces -------------------------------------------------------------
console.log('\nthe pieces, on an already-lower-case name');
time('toLowerCase()', ITERS, (i) => {
  SINK.n += LOWER[i % LOWER.length].toLowerCase().length;
});
time('trim()', ITERS, (i) => {
  SINK.n += LOWER[i % LOWER.length].trim().length;
});
time('String(name)', ITERS, (i) => {
  SINK.n += String(LOWER[i % LOWER.length]).length;
});
time('String(name).trim().toLowerCase()', ITERS, (i) => {
  SINK.n += String(LOWER[i % LOWER.length]).trim().toLowerCase().length;
});
time('name.trim().toLowerCase()', ITERS, (i) => {
  SINK.n += LOWER[i % LOWER.length].trim().toLowerCase().length;
});
time('typeof name === \'string\'', ITERS, (i) => {
  const n = LOWER[i % LOWER.length];
  SINK.n += typeof n === 'string' ? 1 : 0;
});

// does the string change? an unchanged result means no allocation
const lower = 'content-type';
console.log(`\n  toLowerCase() on a lower-case name returns the same string: ${lower.toLowerCase() === lower}`);
console.log(`  trim() on a trimmed name returns the same string:      ${lower.trim() === lower}`);
console.log(`  String(name) on a string returns the same string:       ${String(lower) === lower}`);

// ---- 2. String() versus a typeof branch ---------------------------------------
console.log('\nthe coercion, and the branch that would replace it');
time('String(name).trim().toLowerCase()', ITERS, (i) => {
  SINK.n += String(LOWER[i % LOWER.length]).trim().toLowerCase().length;
});
time('typeof-guarded trim().toLowerCase()', ITERS, (i) => {
  const n = LOWER[i % LOWER.length];
  SINK.n += (typeof n === 'string' ? n : String(n)).trim().toLowerCase().length;
});
time('typeof-guarded, mixed case', ITERS, (i) => {
  const n = MIXED[i % MIXED.length];
  SINK.n += (typeof n === 'string' ? n : String(n)).trim().toLowerCase().length;
});

// ---- 3. a cache against the scan ----------------------------------------------
console.log('\na Map against the scan, keyed by the name as it arrives');
const interned = new Map(); // keys are literals: internalised
for (const n of LOWER) {
  interned.set(n, n.trim().toLowerCase());
}
const fresh = new Map(); // keys are built per call: not internalised
for (const n of LOWER) {
  fresh.set(n.slice(0), n.trim().toLowerCase());
}
time('Map.get, internalised key', ITERS, (i) => {
  SINK.n += interned.get(LOWER[i % LOWER.length]).length;
});
time('Map.get, fresh equal key', ITERS, (i) => {
  SINK.n += fresh.get(LOWER[i % LOWER.length].slice(0)).length;
});
time('the scan it would replace', ITERS, (i) => {
  SINK.n += LOWER[i % LOWER.length].trim().toLowerCase().length;
});

// what a miss costs: the lookup, the normalisation, the insert
const growing = new Map();
time('Map miss + normalise + insert', ITERS, (i) => {
  const n = 'x-' + i;
  let v = growing.get(n);
  if (v === undefined) {
    v = n.trim().toLowerCase();
    growing.set(n, v);
  }
  SINK.n += v.length;
  if (growing.size > 1024) {
    growing.clear();
  }
});

// the case the compat path actually has: setHeader() normalises once, then
// parse() sees the normalised name and normalises it again
console.log('\nthe double normalisation in the compat path');
const stored = new Map(); // what setHeader() leaves behind
for (const n of MIXED) {
  stored.set(n.trim().toLowerCase(), 'v');
}
const storedKeys = [...stored.keys()];
time('toLowerCase() on the stored name', ITERS, (i) => {
  SINK.n += storedKeys[i % storedKeys.length].toLowerCase().length;
});
time('the whole normalisation', ITERS, (i) => {
  SINK.n += String(MIXED[i % MIXED.length]).trim().toLowerCase().length;
});

console.log(`\nsink ${SINK.n}`);
