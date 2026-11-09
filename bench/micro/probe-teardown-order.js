// The compat teardown order, node against h2lite.
//
//   node bench/micro/probe-teardown-order.js
//
// Four shapes, each recorded on both stacks:
//
//   normal   -- the handler answers and ends; the peer never resets
//   reset    -- the peer resets before the response was sent
//   answered -- the handler answered and ended; the peer resets afterwards
//   unread   -- nobody reads the request body; the peer resets
//
// What the compat layer's header comment says today is that "the order of the
// teardown events ... differs: node emits req 'close' (and even req 'end')
// before the response finishes". This prints what each stack actually does.
'use strict';
const http2 = require('node:http2');
const h2lite = require('../../index');

const GET = { ':method': 'GET', ':path': '/x', ':scheme': 'http', ':authority': 'localhost' };

function scenario(impl, kind) {
  return new Promise((resolve) => {
    const log = [];
    const server = impl.createServer((req, res) => {
      req.on('aborted', () => log.push('req.aborted'));
      req.on('close', () => log.push('req.close'));
      req.on('end', () => log.push('req.end'));
      res.on('finish', () => log.push('res.finish'));
      res.on('close', () => log.push('res.close'));
      if (kind !== 'unread') {
        req.resume();
      }
      if (kind === 'normal' || kind === 'answered') {
        res.writeHead(200);
        res.end('ok');
      }
    });
    server.listen(0, '127.0.0.1', () => {
      const client = http2.connect(`http://127.0.0.1:${server.address().port}`);
      client.on('error', () => {});
      const req = client.request(GET);
      req.on('error', () => {});
      req.resume();
      const done = () => setTimeout(() => {
        client.destroy();
        server.close();
        resolve(log);
      }, 200);
      if (kind === 'reset' || kind === 'answered' || kind === 'unread') {
        setTimeout(() => {
          req.close(8); done();
        }, 80);
      } else {
        req.on('end', () => setTimeout(done, 80));
      }
    });
  });
}

(async () => {
  for (const kind of ['normal', 'reset', 'answered', 'unread']) {
    const native = await scenario(http2, kind);
    const ours = await scenario(h2lite, kind);
    const same = JSON.stringify(native) === JSON.stringify(ours) ? '  ' : '<>';
    console.log(`${kind.padEnd(9)} node  : ${JSON.stringify(native)}`);
    console.log(`${kind.padEnd(9)} h2lite: ${JSON.stringify(ours)} ${same}`);
  }
})();
