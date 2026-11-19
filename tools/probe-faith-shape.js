// Why does faith claim to be faster than node:http2 (its README "Is it
// faster?" says yes) while bench/client/ measures it 3-8x SLOWER per request?
// Because the two harnesses measure different things. This probe replicates
// faith's OWN bench shape, read out of passcod/faith bench/run.mjs,
// bench/lib/clients.mjs and bench/lib/servers.mjs (main, 2026-11):
//
//   * the h2 server is node:http2.createSecureServer IN THE SAME PROCESS as
//     the client, compat API, raised limits (maxSessionMemory 1024 MB,
//     maxConcurrentStreams 4096) -- so the server's JS work shares the measured
//     event loop with the client's;
//   * one reused session/agent per arm (their adapters' warm mode), closed-loop
//     workers, warmup discarded;
//   * the verdict metrics are WALL-CLOCK rps and event-loop delay, not CPU.
//     cpuPerReq is printed alongside to show the two metrics disagreeing.
//
//   node tools/probe-faith-shape.js
//
// Measured on this host (node v26.12.1, one run):
//   1 KiB  c16: node 35.7k rps / 39.7 us, h2lean 45.0k / 36.2, faith 21.1k / 124.7
//   64 KiB c16: node  7.0k rps / 176.9 us, h2lean  7.2k / 183.4, faith 11.4k / 250.7
//   64 KiB c64: node  7.1k rps / 190.5 us, h2lean  7.4k / 190.9, faith 14.2k / 217.4
// faith wins wall-clock rps on the 64 KiB slices (its Rust threads take TLS,
// HPACK and framing OFF the shared event loop, so the in-process node server
// answers sooner) while paying MORE total CPU per request -- and it loses even
// the wall-clock race at 1 KiB. Against a separate nghttpd (bench/client/) the
// off-loop offload buys nothing and only the CPU cost remains.
//
// One deviation from their server recipe, deliberate: the handler resumes the
// request. Without that, node's server answers then sends RST_STREAM(NO_ERROR)
// to discard the unconsumed request body; node's own client tolerates that
// reset, h2lean's currently errors on it (a client parity gap this probe
// surfaced, unfixed -- it strands the h2lean arm's workers at 64 KiB). Resuming
// is identical work for every arm and keeps the comparison about the clients.
const http2 = require('node:http2');
const h2lean = require('../index');
const faith = require('@passcod/faith');
const { monitorEventLoopDelay } = require('node:perf_hooks');
const fixtures = require('../test/fixtures-tls');

const { key, cert } = fixtures.serverAuth();
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

async function scenario(size, conc, samples) {
  const payload = Buffer.alloc(size, 0x78);
  const server = http2.createSecureServer({
    key, cert, allowHTTP1: false,
    maxSessionMemory: 1024,
    settings: { maxConcurrentStreams: 4096 }
  }, (req, res) => {
    req.resume();
    res.writeHead(200, {
      'content-type': 'application/octet-stream',
      'content-length': payload.length,
      'cache-control': 'no-store'
    });
    res.end(payload);
  });
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  const url = `https://127.0.0.1:${server.address().port}/payload/${size}`;
  const results = [];

  for (const name of ['node:http2', 'h2lean', 'faith']) {
    const impl = name === 'node:http2' ? http2 : name === 'h2lean' ? h2lean : null;
    const sess = impl
      ? impl.connect(url, { ca: cert, maxSessionMemory: 1024 })
      : new faith.Agent({ tls: { extraRoots: [cert], revocation: { onecrl: false, crlite: false } } });
    if (impl) {
      sess.on('error', () => {});
    }
    const request = impl
      ? () => new Promise((resolve, reject) => {
        const req = sess.request({ ':path': `/payload/${size}` });
        let bytes = 0;
        req.on('data', (c) => {
          bytes += c.length;
        });
        req.on('error', reject);
        req.on('end', () => resolve(bytes));
      })
      : async () => {
        const res = await faith.fetch(url, { agent: sess });
        const reader = res.body.getReader();
        let bytes = 0;
        for (;;) {
          const { done, value } = await reader.read();
          if (done) {
            break;
          }
          bytes += value.length;
        }
        return bytes;
      };
    // warmup, untimed (their runner discards 50-100 samples)
    await Promise.all(Array.from({ length: conc }, async () => {
      for (let i = 0; i < 50; i++) {
        await request();
      }
    }));
    const loop = monitorEventLoopDelay({ resolution: 1 });
    const c0 = process.cpuUsage();
    const t0 = process.hrtime.bigint();
    loop.enable();
    let issued = 0, fails = 0;
    await Promise.all(Array.from({ length: conc }, async () => {
      for (;;) {
        const n = issued++;
        if (n >= samples) {
          break;
        }
        try {
          await request();
        } catch {
          fails++;
        }
      }
    }));
    loop.disable();
    const wallMs = Number(process.hrtime.bigint() - t0) / 1e6;
    const cpu = process.cpuUsage(c0);
    results.push({
      arm: name, size, conc,
      rps: Math.round(samples / wallMs * 1000),
      cpuPerReq: +((cpu.user + cpu.system) / samples).toFixed(1),
      loopP99Ms: +(loop.percentile(99) / 1e6).toFixed(2),
      fails
    });
    if (impl) {
      sess.destroy();
    } else {
      sess.close();
    }
    await wait(150);
  }
  await new Promise((r) => {
    server.close(r); setTimeout(r, 300);
  });
  for (const r of results) {
    console.log(`size=${String(r.size).padStart(6)} c=${String(r.conc).padStart(3)} ` +
      `${r.arm.padEnd(10)} rps=${String(r.rps).padStart(7)} ` +
      `cpuPerReq=${String(r.cpuPerReq).padStart(7)}us loopP99=${r.loopP99Ms}ms fails=${r.fails}`);
  }
}

(async () => {
  // their quick-suite slices (1k/64k at c16) plus one high-concurrency point
  await scenario(1024, 16, 2000);
  await scenario(65536, 16, 1000);
  await scenario(65536, 64, 2000);
  process.exit(0);
})().catch((e) => {
  console.error(e); process.exit(1);
});
