// Paired client benchmark: the same request loop written against node:http2 and
// against h2lean, both talking to the same nghttpd recipe, in ABBA-interleaved
// rounds, aggregated by median. This is M3's measurement harness; the claim it
// exists to support is "drop-in AND faster", and it is built to fail that claim
// honestly:
//
//   * ONE server shape (nghttpd, C, static file, 4 MB windows) for both arms, so
//     the only moving part is the client library in the measuring process;
//   * rounds reverse arm order (ABBA) because the first arm of a pair
//     systematically wins on this machine's conventions -- a saving only counts
//     when the arms do NOT overlap;
//   * cpuPerReq (user+system read inside the client) is the verdict metric;
//     req/s is shown for scale. If the intra-arm spread exceeds the between-arm
//     difference, the summary says INSIDE NOISE, not "faster".
//
//   node bench/client/matrix.js --rounds=3 --duration=6
//   node bench/client/matrix.js --rounds=1 --duration=3   # quick smoke
//
// Flags: --rounds, --duration (s per run), --basePort, --bodies (comma list of
// response sizes, default 0,16384,262144). Raw samples go to out/client-raw.json.
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const ARMS = require('./arms');

const opt = Object.fromEntries(process.argv.slice(2).map((a) => {
  const m = /^--([^=]+)=(.*)$/.exec(a);
  return m ? [m[1], m[2]] : [a.slice(2), true];
}));
const rounds = Number(opt.rounds || 3);
const duration = Number(opt.duration || 6);
const basePort = Number(opt.basePort || 8930);
const bodies = String(opt.bodies || '0,16384,262144').split(',').map(Number);
const BASELINE = 'node client';

function median(xs) {
  if (!xs.length) {
    return NaN;
  }
  const s = xs.slice().sort((a, b) => a - b);
  const m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}
const fmt = (x, d = 0) => Number.isFinite(x)
  ? x.toLocaleString('en-US', { maximumFractionDigits: d, minimumFractionDigits: d }) : '?';

const samples = {};
for (const a of Object.keys(ARMS)) {
  samples[a] = {};
  for (const n of bodies) {
    samples[a][n] = { rps: [], cpu: [], n: [], fail: [] };
  }
}

let portCursor = basePort;
console.log(`paired client benchmark: ${Object.keys(ARMS).join(' vs ')}` +
            ` | bodies=${bodies.join(',')} rounds=${rounds} duration=${duration}s`);

for (const body of bodies) {
  for (let r = 0; r < rounds; r++) {
    const order = Object.keys(ARMS);
    if (r % 2 === 1) {
      order.reverse();
    } // ABBA
    for (const armName of order) {
      const port = ++portCursor;
      process.stdout.write(`  [${body}B r${r + 1}/${rounds}] ${armName} ... `);
      const res = spawnSync(process.execPath, [
        path.join(__dirname, 'run.js'),
        `--arm=${armName}`, `--body=${body}`, `--duration=${duration}`, `--port=${port}`
      ], { encoding: 'utf8' });
      const line = (res.stdout || '').split('\n').reverse().find((l) => l.startsWith('RESULT '));
      if (!line) {
        console.log(`NO RESULT (exit ${res.status}) ${(res.stderr || '').slice(0, 200)}`); process.exit(1);
      }
      const j = JSON.parse(line.slice('RESULT '.length));
      const s = samples[armName][body];
      s.rps.push(j.rps); s.cpu.push(j.cpuPerReq); s.n.push(j.n); s.fail.push(j.fail);
      console.log(`${fmt(j.rps)} req/s, ${fmt(j.cpuPerReq, 3)} us/req` +
                  (j.fail ? ` [fail=${j.fail}!]` : ''));
    }
  }
}

const agg = {};
for (const a of Object.keys(ARMS)) {
  agg[a] = {};
  for (const n of bodies) {
    const s = samples[a][n];
    agg[a][n] = {
      rps: median(s.rps), cpu: median(s.cpu),
      // spread: (max-min)/min over rounds, per arm -- the noise floor this
      // harness is willing to admit to.
      spread: s.rps.length > 1 ? (Math.max(...s.rps) - Math.min(...s.rps)) / Math.min(...s.rps) * 100 : 0,
      fail: s.fail.reduce((x, y) => x + y, 0)
    };
  }
}

fs.mkdirSync(path.join(__dirname, '..', '..', 'out'), { recursive: true });
fs.writeFileSync(path.join(__dirname, '..', '..', 'out', 'client-raw.json'),
  JSON.stringify({
    meta: {
      date: new Date().toISOString().slice(0, 10), node: process.version,
      os: `${os.type()} ${os.release()} (${os.arch()})`,
      cpu: os.cpus()[0] ? os.cpus()[0].model.trim() : 'unknown',
      cores: os.cpus().length
    },
    rounds, duration, bodies, samples, agg
  }, null, 2));

// Summary: the paired ratio per body, with the verdict gated on noise.
console.log('\nbody        node us/req  h2lean us/req  cpu ratio (node/h2lean)  verdict');
for (const n of bodies) {
  const base = agg[BASELINE][n];
  const lite = agg['h2lean client'][n];
  const ratio = base.cpu / lite.cpu;
  const floor = Math.max(base.spread, lite.spread);
  const verdict = ratio > 1 + floor / 100 ? 'h2lean faster (outside spread)'
    : ratio < 1 - floor / 100 ? 'node faster (outside spread)'
      : 'INSIDE NOISE';
  console.log(`${String(n).padEnd(11)} ${fmt(base.cpu, 3).padStart(13)} ${fmt(lite.cpu, 3).padStart(14)}` +
              `  ${fmt(ratio, 3).padStart(10)}x  (${fmt(base.spread, 1)}%/${fmt(lite.spread, 1)}% spread)  ${verdict}`);
}
console.log('\nraw samples: out/client-raw.json');
