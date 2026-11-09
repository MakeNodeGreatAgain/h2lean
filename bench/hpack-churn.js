// The cost of encoding one response header block, at the granularity where the dynamic
// table actually acts: hdr.block() against a session-shaped object, for a workload with
// S distinct response shapes repeating on one connection. The table's win is a smaller
// block; its price is that an insertion invalidates the caches, so a server whose shapes
// do not all fit in 4096 bytes of table re-encodes more often than one whose shapes do.
//
//   node bench/hpack-churn.js
//   node -r ./bench/no-table.js bench/hpack-churn.js   # the behaviour from before
'use strict';
const path = require('node:path');
const hdr = require(path.join(__dirname, '..', 'lib', 'headers'));
const hpack = require(path.join(__dirname, '..', 'lib', 'hpack'));
const { Encoder } = hpack;

// Which arm: bench/no-table.js marks itself on the module it patched.
const TABLE = hpack.noTable !== true;
const ROUNDS = Number(process.argv[2] || 2000);

function session() {
  return { _encoder: new Encoder(4096), _blocks: new WeakMap() };
}

// `n` frozen shapes of the kind a real server repeats: three fixed lines plus an
// ETag-like value that is one of n possibilities.
function shapes(n) {
  const out = [];
  for (let i = 0; i < n; i++) {
    out.push(Object.freeze({
      ':status': '200',
      'content-type': 'text/plain; charset=utf-8',
      server: 'h2lite/0.1',
      etag: `W/"${i}-${'x'.repeat(24)}"`
    }));
  }
  return out;
}

for (const n of [1, 4, 16, 64, 256, 1024]) {
  const set = shapes(n);
  const s = session();
  // warm: every shape seen at least three times, so a stable workload is measured and
  // not the insertion path
  for (let w = 0; w < 3 * n; w++) {
    hdr.block(set[w % n], undefined, null, s);
  }
  const bytes = hdr.block(set[0], undefined, null, s).length;
  const before = process.hrtime.bigint();
  let sink = 0;
  for (let r = 0; r < ROUNDS; r++) {
    sink += hdr.block(set[r % n], undefined, null, s).length;
  }
  const ns = Number(process.hrtime.bigint() - before) / ROUNDS;
  console.log(`shapes=${String(n).padStart(4)}  ${ns.toFixed(0)} ns/req  `
    + `block=${bytes}B  table=${s._encoder.size}B entries=${s._encoder.dyn.length}`
    + `  lineCache=${s._encoder.cacheCount} ${TABLE ? 'table' : 'no-table'}`);
  if (sink < 0) {
    throw new Error('unreachable');
  }
}
