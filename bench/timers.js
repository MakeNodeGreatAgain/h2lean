// What the shared clock in lib/fast-timer.js buys, measured. Two parts: the
// per-timer cost (arm / refresh / clear) and the number of timer resources a
// process holds while N connections each carry an idle timeout.
//
//   node bench/timers.js           # 20000 timers, 500 real connections
//   N=50000 CONNS=1000 node bench/timers.js
//
// The refresh path matters most: Http2Session refreshes the idle timeout on every
// socket read, so it runs once per batch per connection for the life of the server.
'use strict';
const { createHook } = require('node:async_hooks');
const net = require('node:net');
const fastTimer = require('../lib/fast-timer');

// NATIVE=1 forces the session idle timeout back onto plain timers, so the same
// script measures both routings. It has to happen before h2lite is required,
// because lib/session.js picks the functions up at load time.
if (process.env.NATIVE === '1') {
  fastTimer.setTimeout = (cb, delay, arg) => setTimeout(cb, delay, arg);
  fastTimer.clearTimeout = (t) => clearTimeout(t);
}
const h2lite = require('../index');

const N = Number(process.env.N || 20000);
const CONNS = Number(process.env.CONNS || 500);
const noop = () => {};

function time(label, fn) {
  fn(); // warm up the shape
  const t0 = process.hrtime.bigint();
  fn();
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;
  console.log(`  ${label.padEnd(30)} ${ms.toFixed(1).padStart(8)} ms  ` +
              `${(ms * 1000 / N).toFixed(2)} us/unit`);
}

let keep = [];
if (process.env.NATIVE !== '1') {
  // the per-unit cost table compares the two mechanisms, so it is only printed
  // for the shared-clock run
  console.log(`\n== per-timer cost, N=${N}`);
  console.log(' native:');
  time('arm', () => {
    keep = []; for (let i = 0; i < N; i++) {
      keep.push(setTimeout(noop, 30000));
    }
  });
  time('refresh', () => {
    for (const t of keep) {
      t.refresh();
    }
  });
  time('clear', () => {
    for (const t of keep) {
      clearTimeout(t);
    }
  });
  console.log(' FastTimer:');
  time('arm', () => {
    keep = []; for (let i = 0; i < N; i++) {
      keep.push(fastTimer.setTimeout(noop, 30000, i));
    }
  });
  time('refresh', () => {
    for (const t of keep) {
      t.refresh();
    }
  });
  time('clear', () => {
    for (const t of keep) {
      fastTimer.clearTimeout(t);
    }
  });
  fastTimer.reset();
}

// Count the timer resources the process creates while the callback runs. Every
// native setTimeout shows up here as one Timeout async resource, which is exactly
// the thing the shared clock avoids multiplying.
function countTimers(fn) {
  let created = 0;
  const hook = createHook({ init: (p, type) => {
    if (type === 'Timeout') {
      created++;
    }
  } }).enable();
  return fn().then(async (result) => {
    hook.disable();
    return { created, result };
  });
}

async function storm(armMs) {
  const srv = h2lite.createServer();
  srv.on('session', (session) => {
    // a keep-alive idle timeout, which is the case FastTimer exists for
    if (armMs) {
      session.setTimeout(armMs);
    }
  });
  await new Promise((r) => srv.listen(0, '127.0.0.1', r));
  const port = srv.address().port;
  const socks = [];
  await new Promise((resolve) => {
    let ready = 0;
    for (let i = 0; i < CONNS; i++) {
      const s = net.connect(port, '127.0.0.1', () => {
        if (++ready === CONNS) {
          resolve();
        }
      });
      s.on('error', () => {});
      socks.push(s);
    }
  });
  // let the sessions exist, then tear everything down
  await new Promise((r) => setTimeout(r, 150));
  const sessions = srv.sessions.size;
  const rss = process.memoryUsage().rss;
  for (const s of socks) {
    s.destroy();
  }
  await new Promise((r) => setTimeout(r, 150));
  srv.close();
  await new Promise((r) => setTimeout(r, 150));
  return { sessions, rss };
}

(async () => {
  const label = process.env.NATIVE === '1' ? 'plain timers' : 'shared clock';
  console.log(`\n== ${CONNS} connections carrying a 30 s idle timeout [${label}]`);
  await storm(0); // warm up the shape
  const unarmed = await countTimers(() => storm(0));
  const armed = await countTimers(() => storm(30000));
  console.log(`    Timeout resources created: ${armed.created}  `
              + `(same connections without a timeout: ${unarmed.created})`);
  console.log(`    sessions armed: ${armed.result.sessions}, RSS `
              + `${(armed.result.rss / 1e6).toFixed(1)} MB vs unarmed `
              + `${(unarmed.result.rss / 1e6).toFixed(1)} MB`);
  process.exit(0);
})();
