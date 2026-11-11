// What does an idle connection cost?
//
// The slab was allocated per session -- Buffer.allocUnsafe(maxFrameSize + 9),
// 16 393 octets by default -- and it was used only for a frame that straddles a
// socket read. This measures the memory an idle connection costs on both stacks:
// the RSS it adds, and the octets it allocates (arrayBuffers), which is the
// number that does not depend on whether the pages were ever touched.
//
// Usage: node --expose-gc tools/probe-mem.js [connections]
'use strict';
const net = require('node:net');
const http2 = require('node:http2');
const h2lean = require('../index');
const f = require('../lib/frames');

const N = Number(process.argv[2] || 200);

function openIdle(port, _impl) {
  return new Promise((resolve) => {
    const socks = [];
    let done = 0;
    let failed = 0;
    const finish = () => resolve({ socks, failed });
    const one = () => {
      const sock = net.connect(port, '127.0.0.1', () => {
        sock.write(Buffer.from(f.PREFACE));
        const sh = Buffer.alloc(f.HEADER_LEN);
        f.writeHeader(sh, 0, 0, f.SETTINGS, 0, 0);
        sock.write(sh);
        socks.push(sock);
      });
      sock.on('error', () => {
        failed++;
      });
      sock.on('connect', () => {
        if (++done === N) {
          finish();
        }
      });
    };
    // The connections are opened in batches: the listen queue of the OS refuses
    // the ones that arrive faster than the server accepts them.
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
    // the backlog may refuse some of them; do not wait for that forever
    setTimeout(finish, 2000 + N * 2);
  });
}

async function run(impl) {
  const server = impl === 'node'
    ? http2.createServer((req, res) => res.end('ok'))
    : h2lean.createServer((req, res) => res.end('ok'));
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  process.stdout.write(`${impl}: listening on ${port}\n`);
  global.gc();
  const before = process.memoryUsage().rss;
  const beforeAb = process.memoryUsage().arrayBuffers;
  const { socks, failed } = await openIdle(port, impl);
  process.stdout.write(`${impl}: ${socks.length} sockets open, ${failed} refused\n`);
  // let the sessions allocate what they allocate
  await new Promise((r) => setTimeout(r, 300));
  global.gc();
  const after = process.memoryUsage().rss;
  const afterAb = process.memoryUsage().arrayBuffers;
  const n = Math.max(1, socks.length);
  console.log(`${impl.padEnd(7)} ${socks.length} idle connections: RSS +${after - before} octets`
    + ` = ${((after - before) / n).toFixed(0)} octets per connection`
    + ` | arrayBuffers +${afterAb - beforeAb} = ${((afterAb - beforeAb) / n).toFixed(0)} per connection`);
  for (const s of socks) {
    s.destroy();
  }
  server.close();
}

(async () => {
  for (const impl of ['node', 'h2lean']) {
    await run(impl);
  }
  process.exit(0);
})();
