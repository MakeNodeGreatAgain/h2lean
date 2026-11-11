// Turns --cpu-prof files into a per-request cost table you can actually diff.
//
// A CPU profile reports shares (%), and shares are useless for comparing two
// programs: if one of them is 20% cheaper overall, every one of its lines looks
// different even when the code did not change. So this normalises each self-time
// entry to ns per request using the request count the benchmark itself printed,
// and sorts by the difference. Whatever is at the top is where the two programs
// really diverge, not just where one of them spends most of its time.
//
//   node tools/cpuprofile-report.js label:profile.cpuprofile:reqs ...
//
// example, with the request counts taken from the "[h2lean stream] req=..." line:
//   node tools/cpuprofile-report.js \
//     ours:prof/ours/CPU.x.cpuprofile:165309 node:prof/native/CPU.y.cpuprofile:174146
//
// Only self time is used, never cumulative: with a frame like node's writeBuffer,
// which is where the syscall and the kernel copy happen, cumulative would fold the
// whole request into one line and hide the answer.
//
// COMPARING h2lean WITH node:http2 THIS WAY DOES NOT WORK, and the numbers say so:
// the same run left 92% of our CPU visible to the sampler and only 15% of node's,
// because node's HTTP/2 core is C++ (nghttp2, StreamResource::DoWrite, uv_write)
// and V8 files those samples under (program) or not at all, while process.cpuUsage()
// charges them to the process anyway. So the diff column is meaningless across the
// two implementations -- it is meaningful only between two profiles of the SAME
// program. Use it to break down our own cost, and reach for a paired A/B (change one
// thing, re-measure cpuPerReq) to compare against node.
//
// Two gotchas found the hard way while using it:
//   * samples whose callFrame has no functionName are V8 builtin/C++ entries; they
//     all collapse into one anonymous bucket, which is where Buffer.copy lands. The
//     caller breakdown printed under the table resolves it -- that is how the payload
//     copy was identified as _copyActual <- copyImpl rather than left as "(anon)".
//   * test the absence with `!frame.functionName`, not `frame.functionName === ''`.
'use strict';
const fs = require('node:fs');

const args = process.argv.slice(2).map((a) => a.split(':'));
if (args.length < 1 || args.some((t) => t.length !== 3)) {
  console.error('usage: node tools/cpuprofile-report.js label:profile:reqs [label:profile:reqs ...]');
  process.exit(1);
}

function load(file, reqs) {
  const p = JSON.parse(fs.readFileSync(file, 'utf8'));
  const byId = new Map(p.nodes.map((n) => [n.id, n]));
  const self = new Map();
  let wall = 0;
  let idle = 0;
  for (let i = 0; i < p.samples.length; i++) {
    const dt = p.timeDeltas[i] || 0;
    wall += dt;
    const node = byId.get(p.samples[i]);
    if (!node) {
      continue;
    }
    if (node.callFrame.functionName === '(idle)') {
      idle += dt; continue;
    }
    const key = `${node.callFrame.functionName || '(anon)'}  `
      + `${(node.callFrame.url || '').replace(/^file:\/\//, '').replace(/^.*\/h2lean\//, '')}`
      + `:${node.callFrame.lineNumber + 1}`;
    self.set(key, (self.get(key) || 0) + dt);
  }
  // A profile's timeDeltas are MICROSECONDS (the same unit bench/summarize-profile.js
  // divides by 1000 to get milliseconds). The table is in nanoseconds per request, so
  // the conversion happens here: without it every entry rounds to 0 and the diff
  // column -- the whole point of the tool -- comes out empty.
  const per = (v) => v * 1000 / reqs; // ns per request
  return { busy: (wall - idle) * 1000, reqs, profiles: [p],
    self: new Map([...self].map(([k, v]) => [k, per(v)])) };
}

const runs = args.map(([label, file, reqs]) => ({ label, ...load(file, Number(reqs)) }));
const keys = new Set();
for (const r of runs) {
  for (const k of r.self.keys()) {
    keys.add(k);
  }
}

const rows = [...keys].map((k) => {
  const vals = runs.map((r) => r.self.get(k) || 0);
  // across two profiles of the same program the difference is the answer; with one
  // profile there is nothing to differ from, so sort on its own cost
  return { k, vals, diff: vals.length > 1 ? vals[0] - vals[1] : vals[0] };
}).sort((a, b) => Math.abs(b.diff) - Math.abs(a.diff));

console.log('');
for (const r of runs) {
  console.log(`  ${r.label.padEnd(8)} busy ${(r.busy / 1e6).toFixed(0)}ms over `
    + `${r.reqs} requests = ${(r.busy / r.reqs).toFixed(0)} ns/req sampled`);
}
const head = runs.map((r) => r.label.padStart(9)).join(' ');
console.log(`\n${head}  diff    function\n` + '-'.repeat(78));
for (const row of rows.slice(0, 26)) {
  const cells = row.vals.map((v) => v.toFixed(0).padStart(9)).join(' ');
  const pct = runs.length > 1 ? '' : `  ${(row.vals[0] * runs[0].reqs / runs[0].busy * 100).toFixed(1)}%`;
  console.log(`${cells}  ${row.diff.toFixed(0).padStart(7)}  ${row.k}${pct}`);
}

// Anonymous frames are V8 builtins and C++ entries; without their caller they say
// nothing. Group them one level up so Buffer.copy, memcmp and friends are named by
// the code that asked for them.
const parentOf = (p) => {
  const map = new Map();
  for (const n of p.nodes) {
    for (const c of (n.children || [])) {
      map.set(c, n.id);
    }
  }
  return map;
};
const name = (n) => !n ? '?' : `${n.callFrame.functionName || '(builtin)'}  `
  + `${(n.callFrame.url || '').replace(/^file:\/\//, '').replace(/^.*\/h2lean\//, '')}`
  + `:${n.callFrame.lineNumber + 1}`;
for (const r of runs) {
  if (!r.profiles) {
    continue;
  }
  for (const p of r.profiles) {
    const byId = new Map(p.nodes.map((n) => [n.id, n]));
    const parent = parentOf(p);
    const agg = new Map();
    for (let i = 0; i < p.samples.length; i++) {
      const n = byId.get(p.samples[i]);
      if (!n || n.callFrame.functionName || (n.callFrame.url || '') !== '') {
        continue;
      }
      const up = name(byId.get(parent.get(n.id)));
      agg.set(up, (agg.get(up) || 0) + (p.timeDeltas[i] || 0) * 1000 / r.reqs);
    }
    const top = [...agg].sort((a, b) => b[1] - a[1]).slice(0, 4);
    if (top.length) {
      console.log(`\n  ${r.label}: anonymous frames, by caller (ns/req)`);
      for (const [k, v] of top) {
        console.log(`      ${v.toFixed(0).padStart(7)}  <- ${k}`);
      }
    }
  }
}
console.log('');
