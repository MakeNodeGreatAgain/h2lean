// NATIVE-Duplex decision matrix (OPTIMIZATION.md item 41, AGENTS.md 7): the true
// `Http2Stream extends Duplex` server arm, implemented as a patched copy of lib/
// (duplex-native-variant.patch) so the measurement is reproducible from this tree.
// Three arms -- lean EE / native Duplex / node:http2 -- four cases, ABCCBA x3
// rounds, server-side cpuPerReq self-reported by each child, load in this parent
// (one h2lean client shape for every arm). SINK/req is printed per arm to verify
// the same body octets were read (the READBODY rule).
//
// The variant carries two documented simplifications (its header comment): credit
// released on arrival instead of consumption (favours the duplex arm -- the real
// change would pay more), and end() split into _write + _final(EMPTY/ES) (one extra
// empty DATA frame per response, against it). It is a measurement vehicle, not a
// candidate: it breaks the EE suite's credit and event-order pins by design.
//
//   node bench/micro/duplex-native-ab.js
'use strict';

const { spawn, execFileSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const h2lean = require('../../index');

const ROOT = path.join(__dirname, '../..');
const VARIANT_DIR = path.join(ROOT, 'out/duplex-native-variant');
const ARM_SCRIPT = path.join(__dirname, 'duplex-native-arm.js');
const CONNECTIONS = Number(process.env.CONN || 8);
const STREAMS = Number(process.env.STREAMS || 8);
const LOAD_MS = Number(process.env.LOAD || 3600);
const ROUNDS = Number(process.env.ROUNDS || 3);
const CASES = [
  { up: 0, down: 0, label: 'GET 0B' },
  { up: 0, down: 1024, label: 'GET 1KB down' },
  { up: 1024, down: 0, label: 'POST 1KB up' },
  { up: 16384, down: 0, label: 'POST 16KB up' }
];
let PORT = Number(process.env.PORT || 8960);

// Build the variant: a pristine copy of the shipped lib plus the patch. git apply
// refuses a tree the patch no longer fits (the mutate-guards rule), so a lib/ edit
// that moves the converted lines fails loudly here instead of measuring a hybrid.
function buildVariant() {
  fs.rmSync(VARIANT_DIR, { recursive: true, force: true });
  fs.mkdirSync(VARIANT_DIR, { recursive: true });
  fs.cpSync(path.join(ROOT, 'lib'), path.join(VARIANT_DIR, 'lib'), { recursive: true });
  fs.copyFileSync(path.join(ROOT, 'index.js'), path.join(VARIANT_DIR, 'index.js'));
  execFileSync('git', ['apply', '--directory=out/duplex-native-variant',
    path.join(__dirname, 'duplex-native-variant.patch')], { cwd: ROOT });
  return path.join(VARIANT_DIR, 'index.js');
}

function runArm(arm, c, variant) {
  return new Promise((resolve) => {
    const port = PORT++;
    const child = spawn(process.execPath, [ARM_SCRIPT], {
      env: {
        ...process.env, PORT: String(port), ARM: arm, VARIANT: variant,
        UP: String(c.up), DOWN: String(c.down), DURATION: String(LOAD_MS + 1500)
      },
      stdio: ['ignore', 'pipe', 'inherit']
    });
    let buf = '';
    let started = false;
    child.stdout.on('data', (d) => {
      buf += d;
      if (!started && buf.includes('listening')) {
        started = true;
        drive(port, c);
      }
    });
    child.on('exit', () => {
      const m = buf.match(/CPU=([\d.]+) REQ=(\d+) BUSY=([\d.]+) SINK=(\d+)/);
      resolve(m ? { cpu: Number(m[1]), req: Number(m[2]), busy: Number(m[3]), sink: Number(m[4]) } : null);
    });
  });
}

// Keep CONNECTIONS x STREAMS requests in flight against `port` for LOAD_MS. The
// response body is sunk so every stream completes both halves (the slot loop).
function drive(port, c) {
  const body = c.up > 0 ? Buffer.alloc(c.up, 0x61) : null;
  const HEADERS = body
    ? { ':method': 'POST', ':path': '/', ':scheme': 'http', 'content-length': String(c.up) }
    : { ':method': 'GET', ':path': '/', ':scheme': 'http' };
  const stopAt = Date.now() + LOAD_MS;
  const sessions = [];
  function one(client) {
    if (Date.now() > stopAt || client.destroyed) {
      return;
    }
    let s;
    try {
      s = client.request(HEADERS, body ? { endStream: false } : undefined);
    } catch {
      return;
    }
    s.on('error', () => {});
    s.on('data', () => {});
    s.on('close', () => one(client));
    s.resume();
    if (body) {
      s.end(body);
    }
  }
  for (let k = 0; k < CONNECTIONS; k++) {
    const client = h2lean.connect('http://127.0.0.1:' + port);
    client.on('error', () => {});
    sessions.push(client);
    client.on('connect', () => {
      for (let j = 0; j < STREAMS; j++) {
        one(client);
      }
    });
  }
  setTimeout(() => {
    for (const s of sessions) {
      s.destroy();
    }
  }, LOAD_MS + 300);
}

function median(a) {
  const s = [...a].sort((x, y) => x - y);
  const m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}
function spread(a) {
  return Math.max(...a) - Math.min(...a);
}

(async () => {
  const variant = buildVariant();
  console.log(`conn=${CONNECTIONS} streams=${STREAMS} load=${LOAD_MS}ms rounds=${ROUNDS} (ABCCBA)`);
  for (const c of CASES) {
    const samples = { lean: [], duplex: [], node: [] };
    const sinks = { lean: [], duplex: [], node: [] };
    const busies = { lean: [], duplex: [], node: [] };
    async function take(arm) {
      const x = await runArm(arm, c, variant);
      if (x) {
        samples[arm].push(x.cpu);
        sinks[arm].push(x.req > 0 ? x.sink / x.req : 0);
        busies[arm].push(x.busy);
      }
    }
    for (let r = 0; r < ROUNDS; r++) {
      const order = r % 2 === 0
        ? ['lean', 'duplex', 'node', 'node', 'duplex', 'lean']
        : ['node', 'duplex', 'lean', 'lean', 'duplex', 'node'];
      for (const arm of order) {
        await take(arm);
      }
      process.stderr.write('.');
    }
    console.log(`\n== ${c.label} ==`);
    const med = {};
    for (const arm of ['lean', 'duplex', 'node']) {
      const a = samples[arm];
      med[arm] = median(a);
      console.log(`${arm.padEnd(7)} cpu=${med[arm].toFixed(3)} us/req  spread=${spread(a).toFixed(3)}  busy=${median(busies[arm]).toFixed(0)}%  sink/req=${median(sinks[arm]).toFixed(0)}`);
    }
    console.log(`duplex/lean = x${(med.duplex / med.lean).toFixed(3)}   node/lean = x${(med.node / med.lean).toFixed(3)}   duplex/node = x${(med.duplex / med.node).toFixed(3)}`);
  }
})();
