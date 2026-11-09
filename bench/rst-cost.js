// What the rapid-reset budget costs per inbound RST_STREAM frame: the real _onRst against
// the same method with the token bucket taken out (the pre-fix body). Both arms are called
// the same way -- `fn.call(session, sid, code)` -- and both write the same field in the
// loop, so what is left in the difference is the bucket itself. Comparing a method against
// a standalone helper instead reports that helper's own inlining as a negative cost for
// the bucket: same-shape arms are not a nicety here.
//
// The arms are interleaved (A B A B) and averaged per arm, because run-to-run drift
// exceeds the effect otherwise.
//
//   node bench/rst-cost.js
'use strict';
const { Http2Session } = require('../lib/session');

// A session with nothing to tear down: the Map lookup misses, which is the cheapest case
// and therefore the one where a fixed overhead would be most visible. _rstSec is pinned in
// the future so the clock branch is not taken -- the arm below measures it separately.
function fake() {
  return { _streams: new Map(), _rstTokens: 1000, _rstSec: Number.MAX_SAFE_INTEGER };
}

// The pre-fix body: _onRst with the bucket block removed and nothing else changed.
function withoutBucket(sid) {
  return this._streams.get(sid) === undefined;
}

const withBucket = Http2Session.prototype._onRst;
const s = fake();
const N = 2_000_000;
const ROUNDS = 4;

function time(fn) {
  // keep the bucket full each iteration: otherwise the run walks into the refill branch
  // and, at the end, into the fatal path -- which is not what is being costed
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < N; i++) {
    s._rstTokens = 1000; fn.call(s, 1, 8);
  }
  return Number(process.hrtime.bigint() - t0) / N;
}

withBucket.call(s, 1, 8); time(withBucket); // warm both arms before timing them
withoutBucket.call(s, 1, 8); time(withoutBucket);

const a = [], b = [];
for (let r = 0; r < ROUNDS; r++) {
  a.push(time(withBucket));
  b.push(time(withoutBucket));
}
const mean = (v) => v.reduce((x, y) => x + y) / v.length;
const quiet = (v) => Math.min(...v); // the least-loaded run is the honest one
console.log(`with the bucket    ${a.map((x) => x.toFixed(1)).join('  ')}  min ${quiet(a).toFixed(1)} ns`);
console.log(`without the bucket ${b.map((x) => x.toFixed(1)).join('  ')}  min ${quiet(b).toFixed(1)} ns`);
console.log(`\nThe budget costs ${(quiet(a) - quiet(b)).toFixed(1)} ns per RST_STREAM frame`
  + ` (mean of the runs: ${(mean(a) - mean(b)).toFixed(1)} ns).`);
console.log('For scale: Date.now() alone, which is what the empty-bucket branch reads, is');
console.log(`  ${minOf(() => Date.now())} ns -- sampled only once a peer has spent its budget.`);

function minOf(fn) {
  let best = Infinity;
  for (let r = 0; r < 5; r++) {
    const t0 = process.hrtime.bigint();
    for (let i = 0; i < N / 10; i++) {
      fn();
    }
    best = Math.min(best, Number(process.hrtime.bigint() - t0) / (N / 10));
  }
  return best.toFixed(1);
}
