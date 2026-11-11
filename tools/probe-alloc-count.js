// How many buffers does ONE request allocate, and which line of lib/ allocates them?
//
//   node tools/probe-alloc-count.js [h2lean|node] [requests]
//
// The counter sits on Buffer's allocation methods, so it sees every buffer the
// JavaScript layer makes -- and only that layer: node's http2 server does most of
// its work inside nghttp2, where a JS-level counter cannot see it. The h2lean
// number is therefore the whole of that server's cost, and the node number is
// only the JS part of node's. Same rule as a profile: the C++ core is invisible.
//
// Every allocation is attributed to the lib/ line that made it, by stack, so the
// census says WHERE the buffers come from, not only how many.
//
// Measured on 300 requests of a GET with a five-octet body (this session):
//
//   h2lean: 2.05 allocations per request, 1 050 octets
//     1.01/req   1 031 B   lib/session.js 504   the outbound staging buffer
//     1.00/req       5 B   lib/stream.js 152    the application's own bytes
//     0.02/req       0 B   lib/hpack.js 967     a cold encoder cache miss
//     0.01/req       0 B   lib/session.js 1462  a copy into the header-block cache
//
//   node: 0.00 allocations per request, 0 octets -- all of it in C++.
//
// The staging buffer is the only hot one, and bench/reuse-stage.js is the arm
// that takes it away: 2.05 -> 1.04 allocations and 1 050 -> 22 octets per
// request. What that buys is in CHANGELOG.md under "Measured".
'use strict';
const path = require('node:path');
const { spawn } = require('node:child_process');

const which = process.argv[2] || 'h2lean';
const N = Number(process.argv[3] || 300);

const mod = which === 'node'
  ? require('node:http2')
  : require(path.join(__dirname, '..', 'index.js'));

// ---- the census ----------------------------------------------------------------

const sites = new Map();
let counting = false;

function site() {
  const st = new Error().stack.split('\n');
  for (let i = 2; i < st.length; i++) {
    const l = st[i].trim();
    if (!l.startsWith('at ')) {
      continue;
    }
    if (l.includes('/lib/') || l.includes('node:internal')) {
      return l.slice(3);
    }
  }
  return 'other';
}

for (const name of ['alloc', 'allocUnsafe', 'allocUnsafeSlow', 'from', 'concat']) {
  const orig = Buffer[name];
  Buffer[name] = function (...args) {
    const out = orig.apply(this, args);
    if (counting) {
      const key = `${name}  ${site()}`;
      const c = sites.get(key) || { n: 0, bytes: 0 };
      c.n++;
      if (out && typeof out.length === 'number') {
        c.bytes += out.length;
      }
      sites.set(key, c);
    }
    return out;
  };
}

// ---- the server ----------------------------------------------------------------

const server = mod.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain', 'x-probe': 'v' });
  res.end('hello');
});

// ---- the client, in its own process so its allocations are not counted ----------

const CLIENT = `
const http2 = require('node:http2');
const port = Number(process.argv[1]);
const N = Number(process.argv[2]);
const session = http2.connect('http://127.0.0.1:' + port);
session.on('error', (e) => { console.error('client:', e.message); process.exit(1); });
let i = 0;
session.on('connect', () => next());
function next() {
  if (i >= N) { session.destroy(); return; }
  i++;
  const r = session.request({ ':path': '/' });
  r.on('response', () => r.resume());
  r.on('end', () => next());
  r.on('error', (e) => { console.error('client:', e.message); process.exit(1); });
  r.end();
}
`;

server.listen(0, '127.0.0.1', () => {
  const port = server.address().port;
  sites.clear();
  counting = true;
  const child = spawn(process.execPath, ['-e', CLIENT, String(port), String(N)],
    { stdio: ['ignore', 'inherit', 'inherit'] });
  child.on('exit', () => {
    counting = false;
    const rows = [...sites.entries()].sort((a, b) => b[1].n - a[1].n);
    let n = 0;
    let bytes = 0;
    for (const [, c] of rows) {
      n += c.n; bytes += c.bytes;
    }
    console.log(`\n== ${which}: ${n} buffer allocations for ${N} requests ` +
      `(${(n / N).toFixed(2)} per request), ${bytes} octets ` +
      `(${(bytes / N).toFixed(0)} per request)`);
    for (const [k, c] of rows) {
      console.log(`  ${(c.n / N).toFixed(2).padStart(6)}/req  ` +
        `${(c.bytes / N).toFixed(0).padStart(7)} B/req  ${k}`);
    }
    server.close();
    process.exit(0);
  });
});
