// End-to-end SERVER A/B for the stream.Duplex overhead (OPTIMIZATION.md item 41).
//
// Method (AGENTS.md 7): the metric is server-side cpuPerReq, measured inside the server
// process (a spawned child, bench/micro/duplex-server-arm.js). The load generator runs in
// THIS parent, so the child's process.cpuUsage() is clean. Both arms do identical work
// (read the whole upload body, answer 200); only body delivery differs (EventEmitter vs
// Duplex). ABBA interleaved, >=3 rounds, median per arm per payload. Run-to-run spread is
// printed so the noise is visible next to the delta.
//
//   node bench/micro/duplex-server-ab.js
'use strict';

const { spawn } = require('node:child_process');
const path = require('node:path');
const h2lean = require('../../index');

const ARM_SCRIPT = path.join(__dirname, 'duplex-server-arm.js');
const CONNECTIONS = Number(process.env.CONN || 8);
const STREAMS = Number(process.env.STREAMS || 8);
const LOAD_MS = Number(process.env.LOAD || 2500);
const ROUNDS = Number(process.env.ROUNDS || 5);
const PAYLOADS = (process.env.UPS || '1024,16384').split(',').map(Number);
let PORT = 8950;

function runArm(arm, up) {
  return new Promise((resolve) => {
    const port = PORT++;
    const child = spawn(process.execPath, [ARM_SCRIPT], {
      env: { ...process.env, PORT: String(port), ARM: arm, UP: String(up), DURATION: String(LOAD_MS + 600) },
      stdio: ['ignore', 'pipe', 'inherit']
    });
    let buf = '';
    let started = false;
    child.stdout.on('data', (d) => {
      buf += d;
      if (!started && buf.includes('listening')) {
        started = true;
        drive(port, up);
      }
    });
    child.on('exit', () => {
      const m = buf.match(/CPU=([\d.]+) REQ=(\d+) BUSY=([\d.]+)/);
      resolve(m ? { cpu: Number(m[1]), req: Number(m[2]), busy: Number(m[3]) } : null);
    });
  });
}

// Keep CONNECTIONS x STREAMS uploads in flight against `port` for LOAD_MS.
function drive(port, up) {
  const body = Buffer.alloc(up, 0x61);
  const HEADERS = { ':method': 'POST', ':path': '/', ':scheme': 'http', 'content-length': String(up) };
  const stopAt = Date.now() + LOAD_MS;
  const sessions = [];
  function one(client) {
    if (Date.now() > stopAt || client.destroyed) {
      return;
    }
    let s;
    try {
      s = client.request(HEADERS, { endStream: false });
    } catch {
      return;
    }
    s.on('error', () => {});
    s.on('close', () => one(client));
    s.end(body);
  }
  for (let c = 0; c < CONNECTIONS; c++) {
    const client = h2lean.connect('http://127.0.0.1:' + port);
    client.on('error', () => {});
    sessions.push(client);
    client.on('connect', () => {
      for (let k = 0; k < STREAMS; k++) {
        one(client);
      }
    });
  }
  setTimeout(() => {
    for (const s of sessions) {
      s.destroy();
    }
  }, LOAD_MS + 200);
}

function median(a) {
  const s = [...a].sort((x, y) => x - y);
  const m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

function med2(a) {
  return median(a.map((x) => x || 0));
}

(async () => {
  console.log(`conn=${CONNECTIONS} streams=${STREAMS} load=${LOAD_MS}ms rounds=${ROUNDS}`);
  for (const up of PAYLOADS) {
    const lean = [];
    const duplex = [];
    const leanR = [];
    const duplexR = [];
    const leanB = [];
    const duplexB = [];
    async function take(arm) {
      const x = await runArm(arm, up);
      const a = arm === 'lean' ? lean : duplex;
      const ar = arm === 'lean' ? leanR : duplexR;
      const ab = arm === 'lean' ? leanB : duplexB;
      a.push(x && x.cpu);
      ar.push(x && x.req);
      ab.push(x && x.busy);
    }
    for (let r = 0; r < ROUNDS; r++) {
      // ABBA on even rounds, BAAB on odd ones -- same total work, order alternates.
      if (r % 2 === 0) {
        await take('lean');
        await take('duplex');
        await take('duplex');
        await take('lean');
      } else {
        await take('duplex');
        await take('lean');
        await take('lean');
        await take('duplex');
      }
      process.stderr.write('.');
    }
    const L = lean.filter(Boolean);
    const D = duplex.filter(Boolean);
    const mL = median(L);
    const mD = median(D);
    console.log(`\nupload ${up}B: lean=${mL.toFixed(2)}us [${L.map((v) => v.toFixed(2)).join(' ')}] req~${med2(leanR)} busy~${med2(leanB)}%  ` +
      `| duplex=${mD.toFixed(2)}us [${D.map((v) => v.toFixed(2)).join(' ')}] req~${med2(duplexR)} busy~${med2(duplexB)}%  ` +
      `delta=${(mD - mL).toFixed(2)}us (x${(mD / mL).toFixed(2)})`);
  }
  process.exit(0);
})();
