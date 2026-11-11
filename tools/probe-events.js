// Event-order probe (M8). Records, in order, which events fire on which object,
// and in which turn of the event loop they fire, for a scripted exchange. Then
// the same script runs against node:http2 and against h2lean and the two traces
// are compared -- the wire is already proven identical, what is left is the
// temporal contract the application sees.
//   node tools/probe-events.js native
//   node tools/probe-events.js h2lean
const http2 = require('node:http2');
const assert = require('node:assert');

const kind = process.argv[2] === 'h2lean' ? 'h2lean' : 'native';
const impl = kind === 'h2lean' ? require('../index') : http2;

// A phase advances once per loop turn, so "same phase" means "the application
// could not have observed anything in between" and a phase jump means a real
// deferral. setImmediate runs in the check phase, after the I/O callback and its
// tick queue have drained, which is exactly the boundary we want to classify.
let phase = 0;
const log = [];
(function nextPhase() {
  phase++;
  setImmediate(nextPhase);
})();

function record(label, emitter, names) {
  for (const name of names) {
    emitter.on(name, function (...args) {
      let detail = '';
      if (name === 'data') {
        detail = ` len=${args[0].length}`;
      } else if (name === 'error') {
        detail = ` ${args[0] && args[0].code}`;
      } else if (name === 'trailers') {
        detail = ` ${JSON.stringify(args[0])}`;
      }
      log.push(`${phase} ${label}.${name}${detail}`);
    });
  }
}

const SCENARIO = process.env.SCENARIO || 'simple';

const server = impl.createServer();
record('server', server, ['session', 'stream', 'request', 'checkContinue', 'sessionError', 'close']);

server.on('stream', (stream, _headers) => {
  record('stream', stream, ['data', 'end', 'aborted', 'close', 'error', 'wantTrailers', 'drain', 'trailers']);
  record('session', stream.session, ['close', 'error', 'localClose', 'remoteClose', 'goaway', 'aborted', 'streamClosed', 'frameError']);
  if (SCENARIO === 'compat') {
    return;
  }
  if (SCENARIO === 'post') {
    let n = 0;
    stream.on('data', (d) => {
      n += d.length;
    });
    stream.on('end', () => {
      stream.respond({ ':status': 200, 'x-read': String(n) });
      stream.end('ok');
    });
    return;
  }
  stream.respond({ ':status': 200, 'x-a': '1' });
  stream.write('bo');
  stream.end('dy');
});

// The compat layer needs its own listener wiring, so run it as a separate server.
const compatServer = impl.createServer((req, res) => {
  record('req', req, ['data', 'end', 'aborted', 'close', 'error', 'readable']);
  record('res', res, ['finish', 'close', 'error', 'drain']);
  res.writeHead(200, { 'x-a': '1' });
  res.addTrailers({ 'x-tail': 't' });
  res.end('body');
});

function run(target) {
  return new Promise((resolve) => {
    target.listen(0, '127.0.0.1', () => {
      const client = http2.connect(`http://127.0.0.1:${target.address().port}`);
      client.on('error', () => {});
      const req = client.request(
        { ':method': SCENARIO === 'post' ? 'POST' : 'GET', ':path': '/x', ':scheme': 'http', ':authority': 'localhost' });
      req.resume();
      req.on('end', () => finish());
      req.on('error', () => finish());
      req.on('close', () => finish());
      let ended = false;
      function finish() {
        if (ended) {
          return;
        }
        ended = true;
        setTimeout(() => {
          client.destroy();
          target.close();
          setTimeout(resolve, 120);
        }, 120);
      }
      if (SCENARIO === 'post') {
        req.end(Buffer.alloc(9000, 0x71));
      } else {
        req.end();
      }
    });
  });
}

(async () => {
  assert.ok(['simple', 'post', 'compat'].includes(SCENARIO));
  await run(SCENARIO === 'compat' ? compatServer : server);
  console.log(`--- ${kind} ${SCENARIO}`);
  for (const line of log) {
    console.log(line);
  }
  process.exit(0);
})();
