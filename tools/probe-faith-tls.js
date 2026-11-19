// Is the faith client arm's cpuPerReq gap (bench/client/) explained by TLS?
// The arm runs nghttpd with TLS+ALPN because faith 1.1.0 has no h2c prior
// knowledge (tools/probe-faith.js), while the node:http2 and h2lean arms run
// h2c. This probe removes that asymmetry: the EXACT bench/client slot loop
// (8x64, 800 ms warmup, frozen 4 s window, body consumed chunk by chunk) for
// all three clients against ONE nghttpd with TLS+ALPN -- the faith arm's own
// transport and certificate (the serverAuth fixture pair).
//
//   node tools/probe-faith-tls.js [bodyBytes]     (default 16384)
//
// Measured on this host (node v26.12.1, one session, 4 s windows):
//   body=16384:  node:http2 30.3 us/req, h2lean 25.5, faith 119.6
//   body=262144: node:http2 241.8,       h2lean 242.9, faith 615.8
//   body=0:      node:http2 11.0,        h2lean 5.3,   faith 93.2
// TLS costs the JS arms ~11-14 us/req at 16 KiB (their h2c matrix medians are
// ~18 and ~10); faith stays 4-5x above them on the SAME transport. The gap is
// the fetch/napi/reqwest path, not the record protection.
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const fixtures = require('../test/fixtures-tls');

const { cert, certPath, keyPath } = fixtures.serverAuth();
const dir = path.join(__dirname, '..', 'bench', 'client', 'dir');
fs.mkdirSync(path.join(dir, 'f'), { recursive: true });
const body = Number(process.argv[2] || 16384);
fs.writeFileSync(path.join(dir, 'f', 'index.html'), 'x'.repeat(body));
const port = 8991;
const srv = spawn('nghttpd',
  ['-m', '1024', '-n', '4', '-w', '22', '-W', '22', String(port), keyPath, certPath],
  { cwd: dir, stdio: ['ignore', 'ignore', 'inherit'] });
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const url = `https://127.0.0.1:${port}/f/index.html`;

function once(sess, slot) {
  return new Promise((resolve, reject) => {
    let bytes = 0;
    slot.pending = true;
    const s = sess.request({ ':method': 'GET', ':path': '/f/index.html' });
    s.on('data', (d) => {
      bytes += d.length;
    });
    s.on('error', (e) => {
      slot.pending = false; reject(e);
    });
    s.on('end', () => {
      slot.pending = false; resolve(bytes);
    });
  });
}

async function onceFetch(impl, agent, slot) {
  slot.pending = true;
  try {
    const res = await impl.fetch(url, { agent });
    let bytes = 0;
    const reader = res.body.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      bytes += value.length;
    }
    slot.pending = false;
    return bytes;
  } catch (e) {
    slot.pending = false;
    throw e;
  }
}

async function runArm(name, impl, isFetch) {
  const slots = [];
  const groups = [];
  for (let c = 0; c < 8; c++) {
    const sess = isFetch
      ? new impl.Agent({ tls: { extraRoots: [cert], revocation: { onecrl: false, crlite: false } } })
      : impl.connect(url, { ca: cert });
    if (!isFetch) {
      sess.on('error', () => {});
    }
    groups.push(sess);
    for (let i = 0; i < 64; i++) {
      const slot = { active: false, pending: false, done: 0, fail: 0 };
      slot.tick = async () => {
        if (!slot.active) {
          return;
        }
        try {
          await (isFetch ? onceFetch(impl, sess, slot) : once(sess, slot));
          slot.done++;
        } catch {
          slot.fail++; slot.active = false;
        }
        if (slot.active) {
          slot.tick();
        }
      };
      slots.push(slot);
    }
  }
  for (const slot of slots) {
    slot.active = true; slot.tick();
  }
  await wait(800);
  for (const slot of slots) {
    slot.done = 0; slot.fail = 0;
  }
  const duration = 4;
  const c0 = process.cpuUsage();
  await wait(duration * 1000);
  const cpu = process.cpuUsage(c0);
  for (const slot of slots) {
    slot.active = false;
  }
  let n = 0, fail = 0;
  for (const slot of slots) {
    n += slot.done; fail += slot.fail;
  }
  for (let i = 0; i < 200 && slots.some((s) => s.pending); i++) {
    await wait(20);
  }
  console.log(`${name} body=${body}: rps=${(n / duration).toFixed(0)} ` +
    `cpuPerReq=${((cpu.user + cpu.system) / n).toFixed(1)}us n=${n} fail=${fail}`);
  for (const g of groups) {
    if (isFetch) {
      g.close();
    } else {
      g.destroy();
    }
  }
  await wait(200);
}

(async () => {
  await wait(500);
  await runArm('node:http2 TLS', require('node:http2'), false);
  await runArm('h2lean     TLS', require('../index'), false);
  await runArm('faith      TLS', require('@passcod/faith'), true);
  srv.kill();
  process.exit(0);
})().catch((e) => {
  console.error(e); srv.kill(); process.exit(1);
});
