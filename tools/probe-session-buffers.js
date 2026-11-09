// What does one session itself allocate?
//
// tools/probe-mem.js measures the process's RSS and ArrayBuffer totals; this one
// looks at the sessions themselves, so the per-connection number has a name for
// every buffer in it.
//
// Usage: node --expose-gc tools/probe-session-buffers.js [connections]
'use strict';
const net = require('node:net');
const h2lite = require('../index');
const f = require('../lib/frames');

const N = Number(process.argv[2] || 200);

function openIdle(port) {
  return new Promise((resolve) => {
    const socks = [];
    let done = 0;
    const finish = () => resolve(socks);
    const one = () => {
      const sock = net.connect(port, '127.0.0.1', () => {
        sock.write(Buffer.from(f.PREFACE));
        const sh = Buffer.alloc(f.HEADER_LEN);
        f.writeHeader(sh, 0, 0, f.SETTINGS, 0, 0);
        sock.write(sh);
        socks.push(sock);
      });
      sock.on('error', () => {});
      sock.on('connect', () => {
        if (++done === N) {
          finish();
        }
      });
    };
    let opened = 0;
    const batch = () => {
      for (let i = 0; i < 25 && opened < N; i++, opened++) {
        one();
      }
      if (opened < N) {
        setTimeout(batch, 20);
      }
    };
    batch();
    setTimeout(finish, 2000 + N * 2);
  });
}

(async () => {
  const sessions = [];
  const srv = h2lite.createServer();
  srv.on('session', (s) => sessions.push(s));
  await new Promise((r) => srv.listen(0, '127.0.0.1', r));
  const socks = await openIdle(srv.address().port);
  await new Promise((r) => setTimeout(r, 300));

  // The session's own buffers, and the buffers one level down -- the slab was a
  // session field holding a buffer, and the HPACK decoder's scratch is the decoder
  // field's: a walk that stops at the session's own properties would not see them.
  const per = new Map();
  const count = (name, len) => per.set(name, (per.get(name) || 0) + len);
  for (const s of sessions) {
    for (const key of Object.keys(s)) {
      const v = s[key];
      if (Buffer.isBuffer(v)) {
        count(key, v.length); continue;
      }
      if (v === null || typeof v !== 'object') {
        continue;
      }
      for (const inner of Object.keys(v)) {
        const iv = v[inner];
        if (Buffer.isBuffer(iv)) {
          count(`${key}.${inner}`, iv.length);
        }
      }
    }
  }
  const n = Math.max(1, sessions.length);
  let total = 0;
  for (const [key, bytes] of [...per].sort((a, b) => b[1] - a[1])) {
    total += bytes;
    console.log(`  ${key.padEnd(14)} ${bytes} octets over ${sessions.length} sessions`
      + ` = ${(bytes / n).toFixed(0)} per session`);
  }
  console.log(`  ${'TOTAL'.padEnd(14)} ${total} = ${(total / n).toFixed(0)} per session`
    + `  (${sessions.length} sessions, ${socks.length} sockets)`);
  for (const s of socks) {
    s.destroy();
  }
  srv.close();
  process.exit(0);
})();
