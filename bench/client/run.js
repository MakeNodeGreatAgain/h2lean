// One client arm, one payload: start a dedicated nghttpd, run the load loop with
// either node:http2 or h2lite as the ONLY difference, and print a single JSON
// result line for matrix.js to collect.
//
//   node bench/client/run.js --arm="h2lite client" --body=16384 --duration=6 --port=8970
//
// The server is nghttpd (nghttp2's own C server) serving a fixed static file, so
// it is the same high-capacity responder for both arms and the bottleneck under
// measurement is the client process. Windows are set to 4 MB (-w/-W 22, i.e.
// 2**22-1) so a 256 kB response is not flow-control-throttled -- the same reason
// the showcase gives h2load -w/-W 22. -m 1024 keeps the per-connection
// concurrency cap far above the offered 64 streams per connection, and -n 4
// spreads accepted connections over worker threads. nghttpd is spawned with the
// docroot as its cwd and serves paths relative to it: an absolute --htdocs
// answered 404 even to nghttp's own client on this host (the working recipe is in
// test/client/client-roundtrip.test.js).
//
// Metrics, per the repo's benchmark conventions:
//   * req/s     -- requests COMPLETED inside a fixed measurement window. Slots
//                  keep re-issuing until the cutoff and the window's count is
//                  frozen there (in-flight stragglers are drained but not
//                  counted), so a faster arm never earns dead-slot idle time;
//   * cpuPerReq -- process.cpuUsage() delta (user+system: the full cost of owning
//                  the socket) over the window, read inside the client
//                  process. This is the metric that travels; raw req/s moves
//                  8-13% run-to-run on identical code.
// Both arms get the identical worker shape: conn x streams persistent
// connections, one in-flight request per slot, re-issued on response end, body
// consumed (a server that never reads is not a benchmark, and neither is a
// client that never reads -- the body bytes are what the decode path costs).
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const net = require('node:net');
const path = require('node:path');
const ARMS = require('./arms');

const opt = Object.fromEntries(process.argv.slice(2).map((a) => {
  const m = /^--([^=]+)=(.*)$/.exec(a);
  return m ? [m[1], m[2]] : [a.slice(2), true];
}));

const arm = ARMS[opt.arm];
if (!arm) {
  console.error('unknown arm: ' + opt.arm + ' (have: ' + Object.keys(ARMS).join(', ') + ')');
  process.exit(2);
}
const body = Number(opt.body || 0);
const duration = Number(opt.duration || 6);
const port = Number(opt.port || 8970);
const quiet = opt.verbose !== true && opt.verbose !== '1';

const impl = arm.impl === 'h2lite' ? require('../../index') : require(arm.impl);

// Docroot: one static file, written fresh every spawn, served at /f/index.html.
const dir = path.join(__dirname, 'dir');
fs.mkdirSync(path.join(dir, 'f'), { recursive: true });
fs.writeFileSync(path.join(dir, 'f', 'index.html'), 'x'.repeat(body));
const servedPath = '/f/index.html';

function ready(cb) {
  const deadline = Date.now() + 3000;
  const probe = () => {
    const s = net.connect(port, '127.0.0.1', () => {
      s.destroy(); cb(true);
    });
    s.on('error', () => {
      s.destroy();
      if (Date.now() < deadline) {
        setTimeout(probe, 25);
      } else {
        cb(false);
      }
    });
  };
  probe();
}

const srv = spawn('nghttpd',
  ['--no-tls', '-m', '1024', '-n', '4', '-w', '22', '-W', '22', String(port), '.'],
  { cwd: dir, stdio: ['ignore', 'ignore', 'pipe'] });
let srvErr = '';
srv.stderr.on('data', (d) => {
  srvErr += d;
});

const wait = (ms) => new Promise((r) => setTimeout(r, ms));

// One round-trip on one connection, resolving when the response body is fully
// consumed. Identical call shape on both arms -- same method, same listener set,
// same per-slot loop -- so differences in V8 inlining cannot be blamed on the
// harness; they would be the library's own.
function once(sess, headers, slot) {
  return new Promise((resolve, reject) => {
    let bytes = 0;
    slot.pending = true;
    const s = sess.request(headers);
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

(async () => {
  const up = await new Promise((r) => ready(r));
  if (!up) {
    console.error('nghttpd never came up: ' + srvErr.slice(0, 400)); srv.kill(); process.exit(1);
  }
  await wait(200);

  const headers = { ':method': 'GET', ':path': servedPath };
  const slots = [];
  for (let c = 0; c < arm.conn; c++) {
    const sess = impl.connect(`http://127.0.0.1:${port}`);
    sess.on('error', () => {});
    for (let i = 0; i < arm.streams; i++) {
      const slot = { sess, active: false, pending: false, done: 0, fail: 0 };
      slot.tick = async () => {
        if (!slot.active) {
          return;
        }
        try {
          await once(sess, headers, slot);
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

  // Warmup: let the stack reach steady state (TCP write buffers, HPACK dynamic
  // tables, V8 tier-up of the hot loops) BEFORE any counter is read, so the
  // measurement window costs none of the handshake or the interpretation ramp.
  for (const slot of slots) {
    slot.active = true; slot.tick();
  }
  await wait(800);
  let warm = 0;
  for (const slot of slots) {
    warm += slot.done;
  }
  for (const slot of slots) {
    slot.done = 0; slot.fail = 0;
  }

  // Window: cpu is read at both ends of exactly `duration` seconds of load; the
  // per-slot counts are frozen at the cutoff, then the stragglers drain quietly.
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

  const result = {
    arm: opt.arm, label: arm.label, impl: arm.impl, body,
    conn: arm.conn, streams: arm.streams, duration,
    n, fail, warm,
    rps: n / duration,
    cpuPerReq: (cpu.user + cpu.system) / n
  };
  if (!quiet) {
    console.log(`\n### ${arm.label} | body=${body}B slots=${arm.conn}x${arm.streams} ` +
                `dur=${duration}s`);
    console.log(`    ${result.rps.toFixed(0)} req/s, ${result.cpuPerReq.toFixed(3)} us/req, ` +
                `n=${n}, fail=${fail}, warmup=${warm}`);
  }
  console.log('RESULT ' + JSON.stringify(result));
  for (const sess of new Set(slots.map((s) => s.sess))) {
    sess.destroy();
  }
  srv.kill();
  process.exit(0);
})().catch((err) => {
  console.error('arm crashed: ' + (err && err.stack || err));
  srv.kill();
  process.exit(1);
});
