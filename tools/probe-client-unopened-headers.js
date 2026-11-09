// What does a client answer when a server sends response HEADERS on a stream
// id that names no live stream? The verdict is nghttp2's C layer
// (nghttp2_session.c: session_process_headers_frame falls through to
// nghttp2_session_on_request_headers_received when the stream lookup misses,
// and session_detect_idle_stream decides): for a client an idle stream -- any
// id the client has not sent, odd or even -- is a connection error of
// NGHTTP2_ERR_PROTO (GOAWAY(PROTOCOL_ERROR) on the wire, ERR_HTTP2_ERROR on
// the stream and the session), while a closed stream at or below
// last_sent_stream_id is silently ignored (NGHTTP2_ERR_IGN_HEADER_BLOCK).
// node's JS has no such check, so the shapes are measured (AGENTS.md 3).
//   a) HEADERS (:status 200, END_STREAM) on an idle odd sid (3) while sid 1 is open
//   b) HEADERS (:status 200, END_STREAM) again on sid 1 after the response ended
//   c) HEADERS (:status 200, END_STREAM) on an even sid (2, push territory)
//
// Usage: node tools/probe-client-unopened-headers.js
'use strict';
const net = require('node:net');
const http2 = require('node:http2');
const h2lite = require('../index');
const f = require('../lib/frames');

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

const OK = Buffer.from([0x88]); // :status 200, indexed

// `plan` is the list of sids to send HEADERS(OK, END_STREAM) on, in order.
function rawServer(plan) {
  return new Promise((resolve) => {
    const sockets = [];
    const srv = net.createServer((sock) => {
      sockets.push(sock);
      sock.on('error', () => {});
      sock.write(frame(f.SETTINGS, 0, 0, Buffer.alloc(0)));
      let gotPreface = false;
      sock.on('data', () => {
        // The first data event carries the client's preface and its request
        // HEADERS on sid 1; each planned block goes out on its own timer after
        // that. Once is enough -- later data events schedule nothing.
        if (gotPreface) {
          return;
        }
        gotPreface = true;
        for (let i = 0; i < plan.length; i++) {
          setTimeout(() => {
            sock.write(frame(f.HEADERS, f.F_END_STREAM | f.F_END_HEADERS, plan[i], OK));
          }, 120 + i * 300);
        }
      });
      setTimeout(() => sock.destroy(), 1400);
    });
    srv.listen(0, '127.0.0.1', () => resolve({ srv, sockets }));
  });
}

function run(impl, plan) {
  return rawServer(plan).then(({ srv, sockets }) => new Promise((resolve) => {
    const url = `http://127.0.0.1:${srv.address().port}`;
    const sess = impl === 'node' ? http2.connect(url) : h2lite.connect(url);
    const said = [];
    sess.on('goaway', (code) => said.push(`goaway ${code}`));
    sess.on('error', (e) => said.push(`session error ${e.code || ''} ${e.message}`));
    sess.on('close', () => said.push('close'));
    const req = sess.request({ ':path': '/' });
    req.on('response', (h) => said.push(`response ${JSON.stringify(h[':status'])}`));
    req.on('error', (e) => said.push(`stream error ${e.code || ''} ${e.message}`));
    req.on('data', () => {});
    req.on('end', () => said.push('end'));
    req.on('close', () => said.push('stream close'));
    setTimeout(() => {
      sess.destroy();
      for (const s of sockets) {
        s.destroy();
      }
      srv.close();
      resolve(said);
    }, 1600);
  }));
}

(async () => {
  const cases = [
    { name: 'HEADERS on idle odd sid 3 (sid 1 open)', plan: [3] },
    { name: 'HEADERS again on closed sid 1', plan: [1, 1] },
    { name: 'HEADERS on even sid 2', plan: [2] }
  ];
  for (const c of cases) {
    const node = await run('node', c.plan);
    const lite = await run('h2lite', c.plan);
    console.log(`--- ${c.name}`);
    console.log(`    node   says: ${node.join(' | ') || 'nothing'}`);
    console.log(`    h2lite says: ${lite.join(' | ') || 'nothing'}`);
  }
})();
