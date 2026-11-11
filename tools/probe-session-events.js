// Session-level event probe (M8). Prints every event a server-side session and
// its streams fire for one scripted lifecycle, so the same lifecycle can be run
// against node:http2 and h2lean and the traces compared.
//   SCENARIO=normal|goaway|destroy|reset|clientReset node tools/probe-session-events.js native
//   SCENARIO=normal|goaway|destroy|reset|clientReset node tools/probe-session-events.js h2lean
const http2 = require('node:http2');

const kind = process.argv[2] === 'h2lean' ? 'h2lean' : 'native';
const impl = kind === 'h2lean' ? require('../index') : http2;
const SCENARIO = process.env.SCENARIO || 'normal';

const log = [];
const SESSION_EVENTS = ['connect', 'close', 'error', 'frameError', 'goaway', 'localClose',
  'remoteClose', 'aborted', 'streamClosed', 'streamError', 'socketError', 'timeout',
  'remoteSettings', 'ping'];
const STREAM_EVENTS = ['aborted', 'close', 'error', 'end', 'data', 'wantTrailers'];
const SERVER_EVENTS = ['session', 'stream', 'request', 'close', 'sessionError',
  'connectionError', 'error'];

function rec(label, emitter, names) {
  for (const name of names) {
    emitter.on(name, (...args) => {
      let detail = '';
      if (name === 'goaway') {
        detail = ` code=${args[0]} last=${args[1]}`;
      } else if (name === 'streamClosed') {
        detail = ` code=${args[0]}`;
      } else if (name === 'frameError' || name === 'streamError') {
        detail = ` type=${args[0]} code=${args[1]}`;
      } else if (name === 'error' || name === 'sessionError' || name === 'connectionError') {
        detail = ` ${args[0] && args[0].code}`;
      } else if (name === 'data') {
        detail = ` len=${args[0].length}`;
      }
      log.push(`${label}.${name}${detail}`);
    });
  }
}

const server = impl.createServer();
rec('server', server, SERVER_EVENTS);
server.on('session', (session) => rec('session', session, SESSION_EVENTS));
server.on('stream', (stream) => {
  rec('stream', stream, STREAM_EVENTS);
  if (SCENARIO === 'reset') {
    stream.respond({ ':status': 200 });
    stream.close(8);
    return;
  }
  stream.on('data', () => {});
  stream.on('end', () => {
    stream.respond({ ':status': 200 });
    stream.end('ok');
  });
});

server.listen(0, '127.0.0.1', () => {
  const client = http2.connect(`http://127.0.0.1:${server.address().port}`);
  client.on('error', () => {});
  const req = client.request({ ':method': 'GET', ':path': '/x', ':scheme': 'http',
    ':authority': 'localhost' });
  req.resume();
  req.on('error', () => {});
  if (SCENARIO === 'clientReset') {
    req.on('response', () => req.close(8));
  } else if (SCENARIO === 'goaway') {
    req.on('end', () => {
      client.goaway();
      setTimeout(() => client.destroy(), 60);
    });
  } else if (SCENARIO === 'destroy') {
    req.on('response', () => client.destroy());
  } else {
    req.on('end', () => req.destroy());
  }

  setTimeout(() => {
    if (SCENARIO === 'goaway' || SCENARIO === 'destroy') {
      server.close();
    }
    setTimeout(() => {
      console.log(`--- ${kind} ${SCENARIO}`);
      console.log(log.join('\n'));
      process.exit(0);
    }, 200);
  }, 250);
});
