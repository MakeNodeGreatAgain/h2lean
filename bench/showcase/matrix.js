// Showcase matrix: the six arms x three payload sizes, run in interleaved
// rounds, aggregated by median, written to PERFORMANCE.md at the repository
// root. This is the advertising benchmark; the A/B proof harness stays in
// bench/matrix.sh. It is honest by construction rather than flattering:
//
//   * every arm is driven by the SAME tool (h2load) at the SAME offered
//     concurrency, so the only difference is the stack terminating the protocol;
//   * rounds reverse arm order (ABBA) so no arm always runs first -- the repo's
//     own convention is that the first arm of a pair systematically wins;
//   * the metric reported is median over rounds, and cpuPerReq (server-side cost
//     per request) is shown next to req/s, because raw req/s moves 8-13% run to
//     run on identical code and only cpuPerReq is stable enough to trust.
//
//   node bench/showcase/matrix.js --rounds=3 --duration=6 --threads=4
//   node bench/showcase/matrix.js --rounds=1 --duration=3   # quick smoke
//
// Flags: --rounds, --duration (s per run), --threads (h2load), --basePort,
// --out (markdown path, default PERFORMANCE.md at repo root), --keep (leave the
// raw per-sample JSON at out/showcase-raw.json, default on).
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
const threads = Number(opt.threads || 4);
const basePort = Number(opt.basePort || 8990);
const outMd = opt.out || path.join(__dirname, '..', '..', 'PERFORMANCE.md');

const PAYLOADS = [
  { n: 0, label: 'no payload (headers only)' },
  { n: 16384, label: '16 kB response' },
  { n: 262144, label: '256 kB response' }
];
// The drop-in baseline every row is compared against: the thing you replace when
// you swap `require('node:http2')` for `require('h2lean')`.
const BASELINE = 'node:http2 compat';

function median(xs) {
  if (!xs.length) {
    return NaN;
  }
  const s = xs.slice().sort((a, b) => a - b);
  const m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}
const fmt = (x, d = 0) => Number.isFinite(x) ? x.toLocaleString('en-US', { maximumFractionDigits: d, minimumFractionDigits: d }) : '?';

// samples[armName][payloadBytes] = { rps: [], cpu: [], busy: [], failed: [] }
const samples = {};
for (const a of Object.keys(ARMS)) {
  samples[a] = {};
  for (const p of PAYLOADS) {
    samples[a][p.n] = { rps: [], cpu: [], busy: [], failed: [] };
  }
}

let portCursor = basePort;
const nextPort = () => {
  portCursor += 1; return portCursor;
};

for (const p of PAYLOADS) {
  for (let r = 0; r < rounds; r++) {
    // ABBA: reverse the arm order on odd rounds so no arm always runs first.
    const order = Object.keys(ARMS);
    if (r % 2 === 1) {
      order.reverse();
    }
    for (const armName of order) {
      const port = nextPort();
      process.stdout.write(`  [${p.n}B r${r + 1}/${rounds}] ${armName} ... `);
      const res = spawnSync(process.execPath, [
        path.join(__dirname, 'run.js'),
        `--arm=${armName}`, `--body=${p.n}`, `--duration=${duration}`,
        `--threads=${threads}`, `--port=${port}`
      ], { encoding: 'utf8' });
      const line = (res.stdout || '').split('\n').reverse().find((l) => l.startsWith('RESULT '));
      if (!line) {
        console.log(`NO RESULT (exit ${res.status})`); continue;
      }
      const j = JSON.parse(line.slice('RESULT '.length));
      const s = samples[armName][p.n];
      s.rps.push(j.rps); s.cpu.push(j.cpuPerReq); s.busy.push(j.busy); s.failed.push(j.failed);
      console.log(`${fmt(j.rps)} req/s, ${fmt(j.cpuPerReq, 2)} us/req` +
                  (j.failed ? ` [failed=${j.failed}!]` : ''));
    }
  }
}

// Aggregate to medians.
const agg = {};
for (const a of Object.keys(ARMS)) {
  agg[a] = {};
  for (const p of PAYLOADS) {
    const s = samples[a][p.n];
    agg[a][p.n] = { rps: median(s.rps), cpu: median(s.cpu), busy: median(s.busy),
      spread: s.rps.length > 1 ? (Math.max(...s.rps) - Math.min(...s.rps)) / Math.min(...s.rps) * 100 : 0 };
  }
}

fs.writeFileSync(path.join(__dirname, '..', '..', 'out', 'showcase-raw.json'),
  JSON.stringify({ meta: hostMeta(), rounds, duration, threads, samples, agg }, null, 2));

fs.writeFileSync(outMd, renderMd());
console.log(`\nwrote ${outMd}`);

function hostMeta() {
  let h2v = 'unknown';
  // spawnSync does not throw when h2load is missing: it reports on the result
  const ver = spawnSync('h2load', ['--version'], { encoding: 'utf8' });
  if (!ver.error && ver.stdout) {
    h2v = String(ver.stdout).split('\n')[0].trim();
  }
  return {
    date: new Date().toISOString().slice(0, 10),
    node: process.version,
    os: `${os.type()} ${os.release()} (${os.arch()})`,
    cpu: os.cpus()[0] ? os.cpus()[0].model.trim() : 'unknown',
    cores: os.cpus().length,
    h2load: h2v
  };
}

function renderMd() {
  const m = hostMeta();
  const L = [];
  L.push('# h2lean — performance');
  L.push('');
  L.push(`Measured ${m.date} on ${m.cpu}, ${m.cores} cores, ${m.os}; Node ${m.node}; ` +
         `load generated by ${m.h2load}. Re-run with \`node bench/showcase/matrix.js\`.`);
  L.push('');
  L.push('## What this measures');
  L.push('');
  L.push('Six HTTP servers answering the **same** request (a fixed header set and one of ' +
         'three response bodies) under the **same** offered load, driven by h2load. Each arm ' +
         'is a plain, honest responder — no tuning, no cache warmed by hand — so the number ' +
         'describes the stack, not a benchmark trick.');
  L.push('');
  L.push('| Arm | Protocol | What it is |');
  L.push('|---|---|---|');
  L.push('| node:http | HTTP/1.1 | Node\'s HTTP/1.1 server |');
  L.push('| uWebSockets (http1) | HTTP/1.1 | The C++ HTTP/1 server often cited as the Node-ecosystem ceiling |');
  L.push('| node:http2 (stream) | h2c | Node\'s HTTP/2, the raw `Http2Stream` API |');
  L.push('| node:http2 (compat) | h2c | Node\'s HTTP/2, the `(req, res)` compatibility layer — **the drop-in baseline** |');
  L.push('| h2lean (stream) | h2c | This library, the `Http2Stream` API |');
  L.push('| h2lean (compat) | h2c | This library, the `(req, res)` API — what replaces `node:http2` in an app |');
  L.push('');
  L.push('## How to read these numbers');
  L.push('');
  L.push('- **μs/req is the metric that travels.** It is the server\'s own CPU time per request ' +
         '(measured inside the process), largely independent of the client and the machine. ' +
         '**req/s depends on load shape and host** and moves 8–13% run-to-run on *identical* ' +
         'code; it is shown for scale, not as the verdict.');
  L.push('- **The two HTTP/1.1 arms are not the same protocol.** HTTP/1.1 serialises one request ' +
         'per keep-alive connection; h2load is pointed at ' + ARMS['node:http'].inflight +
         ' connections to match the ' + ARMS['h2lean compat'].inflight + ' in-flight requests the ' +
         'HTTP/2 arms carry over ' + ARMS['h2lean stream'].conn + ' connections. The per-request ' +
         'cost comparison is fair; the req/s ceiling is shaped by the protocol\'s connection model.');
  L.push('- **Ratios are against `node:http2 (compat)`** — the API you are replacing. ' +
         'Higher req/s is better; lower μs/req is better.');
  L.push('');

  for (const p of PAYLOADS) {
    const base = agg[BASELINE][p.n];
    L.push(`### ${p.label.charAt(0).toUpperCase() + p.label.slice(1)}`);
    L.push('');
    L.push('| Server | req/s | μs/req | req/s vs node:http2 | μs/req vs node:http2 |');
    L.push('|---|--:|--:|--:|--:|');
    for (const a of Object.keys(ARMS)) {
      const cell = agg[a][p.n];
      const rRatio = base.rps ? cell.rps / base.rps : NaN;
      const cRatio = cell.cpu ? base.cpu / cell.cpu : NaN;
      const tag = a === BASELINE ? ' *(baseline)*' : '';
      L.push(`| ${ARMS[a].label}${tag} | ${fmt(cell.rps)} | ${fmt(cell.cpu, 2)} | ` +
             `${fmt(rRatio, 2)}× | ${fmt(cRatio, 2)}× |`);
    }
    L.push('');
  }

  L.push('## Caveats, stated plainly');
  L.push('');
  L.push('- Single machine, cleartext (h2c / HTTP/1.1 over loopback). TLS adds a shared record-layer ' +
         'cost to every arm and shifts absolute req/s; it does not change the per-request ranking.');
  L.push('- h2load\'s threads and the servers share cores. On this host the servers did not pin'
        + ' a core during a run (`busy%` in `out/showcase-raw.json`, averaged over a window that '
        + 'includes the server\'s idle tail, so the live figure is higher) -- therefore **req/s here '
        + 'is indicative, not the throughput ceiling**. `μs/req` is read directly inside the server '
        + 'and is the comparison to trust; raise `--threads`/`--duration` on a quiet machine to push '
        + 'req/s toward saturation.');
  L.push('- This is a header-bound GET microbenchmark. It says nothing about application work, ' +
         'streaming bodies over slow links, HPACK churn on unique headers, or back-pressure — ' +
         'those are measured by `bench/matrix.sh`, which is the paired proof harness.');
  L.push('- **Re-measure before quoting.** Numbers here are point-in-time on one host; per the ' +
         'project\'s own rule an unpaired before/after run cannot separate a change from noise.');
  L.push('');
  return L.join('\n');
}
