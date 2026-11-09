// When the application answers a request whose inbound half never ended, node closes the stream
// anyway -- `Http2Stream[kMaybeDestroy]()` (internal/http2/core) says "we've submitted a response
// from our server session, have not attempted to process any incoming data, and have no trailers",
// and calls `stream.close()` from the writable side's 'finish'. h2lite has no such rule, so
// `_liveStreams` stays at 1 and a closing session waits for a stream that will never finish.
//
// Each case prints what the peer saw on the wire, what the stream and session look like after the
// response went out, and the live-stream count on both sides, so the boundary of node's rule is
// measured rather than read off a comment.
//
//   node tools/probe-stream-teardown.js            -> both stacks
//   node tools/probe-stream-teardown.js node       -> node only
'use strict';
const net = require('node:net');
const f = require('../lib/frames');
const h2lite = require('../index');
const nodeHttp2 = require('node:http2');
const { listen } = require('../test/diff-harness');

const CODES = { 0: 'NO_ERROR', 1: 'PROTOCOL_ERROR', 2: 'INTERNAL_ERROR', 3: 'FLOW_CONTROL_ERROR',
  5: 'STREAM_CLOSED', 6: 'FRAME_SIZE_ERROR', 7: 'REFUSED_STREAM', 8: 'CANCEL',
  9: 'COMPRESSION_ERROR', 11: 'ENHANCE_YOUR_CALM' };
const code = (n) => CODES[n] !== undefined ? `${n}:${CODES[n]}` : String(n);
const NAMES = { 0: 'DATA', 1: 'HEADERS', 3: 'RST_STREAM', 4: 'SETTINGS', 7: 'GOAWAY', 8: 'WINDOW_UPDATE' };

const REQ = Buffer.from([0x82, 0x84, 0x86, 0x01, 9, ...Buffer.from('localhost', 'latin1')]);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// handler(stream) says what the application does with the request.
const CASES = {
  // the response is finished, the request body was never touched
  'answer-only': (stream) => {
    stream.end('body');
  },
  // the application looks at the request: node sets didRead and stops auto-closing
  'resume-then-answer': (stream) => {
    stream.resume(); stream.end('body');
  },
  'read-then-answer': (stream) => {
    stream.on('data', () => {});
    stream.on('end', () => stream.end('body'));
  },
  // the application has not finished writing, so there is nothing to close yet
  'answer-never-ended': (stream) => {
    stream.write('body');
  },
  // the control: a request that did end, which both stacks already handle
  'request-ended': (stream) => {
    stream.end('body');
  }
};

async function run(impl, name, label, closeSession) {
  const srv = impl.createServer();
  let session = null;
  let stream = null;
  srv.on('session', (s) => {
    session = s;
  });
  srv.on('stream', (s) => {
    stream = s; s.on('error', () => {}); CASES[label](s);
  });
  srv.on('error', () => {});
  await listen(srv);

  const seen = [];
  const peerState = [];
  const sock = net.connect(srv.address().port, '127.0.0.1');
  let acc = Buffer.alloc(0);
  sock.on('error', (e) => peerState.push('err:' + e.code));
  sock.on('end', () => peerState.push('fin'));
  sock.on('data', (d) => {
    acc = Buffer.concat([acc, d]);
    for (;;) {
      if (acc.length < f.HEADER_LEN) {
        return;
      }
      const len = (acc[0] << 16) | (acc[1] << 8) | acc[2];
      if (acc.length < f.HEADER_LEN + len) {
        return;
      }
      const type = acc[3];
      const flags = acc[4];
      const body = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
      acc = acc.subarray(f.HEADER_LEN + len);
      const nm = NAMES[type] !== undefined ? NAMES[type] : 'type' + type;
      // RST_STREAM carries just the code, GOAWAY carries last-stream-id then the code
      if (type === f.RST_STREAM && body.length >= 4) {
        seen.push(`RST_STREAM(${code(body.readUInt32BE(0))})`);
      } else if (type === f.GOAWAY && body.length >= 8) {
        seen.push(`GOAWAY(${code(body.readUInt32BE(4))})`);
      } else if (type === f.HEADERS && (flags & f.F_END_STREAM)) {
        seen.push('HEADERS+END_STREAM');
      } else {
        seen.push(nm);
      }
    }
  });
  await new Promise((r) => sock.on('connect', r));
  sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
  await sleep(100);
  const endStream = label === 'request-ended' ? f.F_END_STREAM : 0;
  sock.write(frame(f.HEADERS, f.F_END_HEADERS | endStream, 1, REQ));
  await sleep(200);
  if (closeSession) {
    session.close();
  }
  await sleep(250);

  const live = session._liveStreams !== undefined
    ? session._liveStreams : (session._streams && session._streams.size);
    // the peer's own view of the goodbye: `end()` and `destroy()` both put a FIN on the wire when
    // everything written has been acked, which is why the socket.end() in _maybeFinishClose has no
    // observable effect of its own -- and `writable=false` afterwards is node having gone all the way
    // through its socket, where h2lite leaves the read half standing.
  console.log(`${name} ${label.padEnd(20)} live=${live === undefined ? '?' : live}` +
    ` stream.closed=${stream && stream.closed} stream.destroyed=${stream && stream.destroyed}` +
    ` session=${session.closed}/${session.destroyed}\n` +
    `       peer saw: ${seen.join(' ') || '(nothing)'}  fin=${sock.readableEnded}` +
    `  writable=${sock.writable}  ${peerState.join(',') || '(no socket events)'}`);
  sock.destroy();
  srv.close();
  await sleep(50);
}

const only = process.argv[2];
(async () => {
  for (const close of [false, true]) {
    console.log(`\n--- session.close() ${close ? 'after the response' : 'not called'} ---`);
    for (const label of Object.keys(CASES)) {
      if (!only || only === 'node') {
        await run(nodeHttp2, 'node  ', label, close);
      }
      if (!only || only === 'h2lite') {
        await run(h2lite, 'h2lite', label, close);
      }
    }
  }
})();
