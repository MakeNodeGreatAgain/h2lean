// Aggregates a V8 .cpuprofile by self-time. Shows native (C++) frames too.
//   node bench/summarize-profile.js out/xxx.cpuprofile [topN]
const fs = require('node:fs');
const path = process.argv[2];
const TOP = Number(process.argv[3] || 25);
const p = JSON.parse(fs.readFileSync(path, 'utf8'));

const byId = new Map(p.nodes.map((n) => [n.id, n]));
const selfMs = new Map();

// timeDeltas align with positions; positions are samples taken at node ids
let total = 0;
for (let i = 0; i < p.samples.length; i++) {
  const dt = (p.timeDeltas[i] || 0) / 1000; // -> ms
  const id = p.samples[i];
  if (id == null) {
    continue;
  }
  total += dt;
  selfMs.set(id, (selfMs.get(id) || 0) + dt);
}

const rows = [];
for (const [id, ms] of selfMs) {
  const n = byId.get(id);
  if (!n) {
    continue;
  }
  const cf = n.callFrame;
  const name = cf.functionName || '(anonymous)';
  const loc = cf.url || '';
  const isNative = !loc || loc === '(native)' || cf.functionName === '(gc)';
  rows.push({ name, loc, ms, isNative });
}

// null-proto buckets: a frame legitimately named "constructor" or "toString" would
// otherwise hit the object's own prototype chain and turn the running sum into a string.
const native = Object.create(null);
const js = Object.create(null);
for (const r of rows) {
  const bucket = r.isNative ? native : js;
  const k = r.name;
  bucket[k] = (bucket[k] || 0) + r.ms;
}

function dump(title, obj) {
  const entries = Object.entries(obj).sort((a, b) => b[1] - a[1]).slice(0, TOP);
  console.log(`\n== ${title} (self ms, total ${total.toFixed(0)} ms) ==`);
  for (const [k, v] of entries) {
    console.log(`${(v * 100 / total).toFixed(1).padStart(5)}%  ${v.toFixed(0).padStart(7)}ms  ${k}`);
  }
}

const nativeTotal = Object.values(native).reduce((a, b) => a + b, 0);
const jsTotal = Object.values(js).reduce((a, b) => a + b, 0);
console.log(`total=${total.toFixed(0)}ms  native=${nativeTotal.toFixed(0)}ms (${(nativeTotal * 100 / total).toFixed(1)}%)  js=${jsTotal.toFixed(0)}ms (${(jsTotal * 100 / total).toFixed(1)}%)`);
dump('NATIVE / C++ frames', native);
dump('JS frames', js);
