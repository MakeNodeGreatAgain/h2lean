// Measures who reports the peer's answer to our GOAWAY, and how often.
//
// The shape: the peer destroys its client (node's client answers a destroy with a GOAWAY of its
// own) and the server closes in the same tick, so the answer lands in the window between the
// session's teardown and the socket's. node's session is destroyed when its graceful close
// completes (internal/http2/core.js: handle.ongracefulclosecomplete -> kMaybeDestroy), h2lite's
// when the GOAWAY is written (lib/session.js: close() -> _maybeFinishClose), and the answer is
// read only if it arrives before that. The outcome is therefore a race on BOTH stacks.
//
//   node tools/probe-goaway-echo.js [rounds]
//
// Load the machine (a few busy loops) to see node's race open up: on an idle machine both stacks
// report the echo in every round; under load node stops reporting it in a third of the rounds.
'use strict';
const nodeHttp2 = require('node:http2');
const h2lite = require('../index');

// The shape of test/event-order.test.js's 'compat' scenario: the peer destroys its client and
// the server closes in the same tick, 120 ms after the response.
function run(impl, rounds) {
  return new Promise((resolve) => {
    const shapes = new Map();
    const next = (round) => {
      if (round >= rounds) {
        resolve(shapes); return;
      }
      const events = [];
      const server = impl.createServer((req, res) => {
        res.writeHead(200); res.end('ok');
      });
      server.on('session', (session) => {
        session.on('goaway', () => events.push('session.goaway'));
        session.on('close', () => events.push('session.close'));
      });
      server.listen(0, '127.0.0.1', () => {
        const client = nodeHttp2.connect(`http://127.0.0.1:${server.address().port}`);
        client.on('error', () => {});
        const req = client.request({ ':method': 'GET', ':path': '/x',
          ':scheme': 'http', ':authority': 'localhost' });
        req.resume();
        req.on('end', () => {
          setTimeout(() => {
            client.destroy();
            server.close();
            setTimeout(() => {
              const key = events.join(',') || '(nothing)';
              shapes.set(key, (shapes.get(key) || 0) + 1);
              next(round + 1);
            }, 150);
          }, 120);
        });
      });
    };
    next(0);
  });
}

(async () => {
  const rounds = Number(process.argv[2] || 30);
  for (const [label, impl] of [['node  ', nodeHttp2], ['h2lite', h2lite]]) {
    const shapes = await run(impl, rounds);
    console.log(`${label}: ${JSON.stringify([...shapes.entries()])}`);
  }
})();
