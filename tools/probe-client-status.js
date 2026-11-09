// What does a client answer when the server's response HEADERS violate the
// response pseudo-header rules? nghttp2 (node's C layer, nghttp2_http.c
// nghttp2_http_on_response_headers) enforces these on receive; node's JS has
// no such check, so the shapes are mapped by measurement (AGENTS.md 3):
//   a) response with no :status at all
//   b) response with a request-only pseudo (:path) alongside :status
//   c) response with two :status
//   d) a trailer block (END_STREAM HEADERS) carrying a pseudo-header
// The session must survive the stream-level refusals: the verdict is per
// stream, RST_STREAM, connection stays up.
//
// Usage: node tools/probe-client-status.js
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

// HPACK literals (no huffman, no index): 0x00, len(name), name, len(value), value.
function lit(name, value) {
  const n = Buffer.from(name);
  const v = Buffer.from(value);
  return Buffer.concat([Buffer.from([0, n.length]), n, Buffer.from([v.length]), v]);
}

const OK = Buffer.from([0x88]); // :status 200 indexed

function rawServer(blocks, flags, sid) {
  // `blocks` is the sequence of header blocks to send on the stream: one for
  // the plain response cases, response-then-trailers for the trailer case.
  return new Promise((resolve) => {
    const seen = [];
    const sockets = [];
    const srv = net.createServer((sock) => {
      sockets.push(sock);
      sock.on('error', () => {});
      sock.write(frame(f.SETTINGS, 0, 0, Buffer.alloc(0)));
      let gotPreface = false;
      let next = 0;
      sock.on('data', (d) => {
        if (!gotPreface) {
          if (d.length < f.PREFACE_LEN) {
            return;
          }
          gotPreface = true;
          d = d.subarray(f.PREFACE_LEN);
        }
        let off = 0;
        while (off + f.HEADER_LEN <= d.length) {
          const len = (d[off] << 16) | (d[off + 1] << 8) | d[off + 2];
          const type = d[off + 3];
          if (off + f.HEADER_LEN + len > d.length) {
            break;
          }
          let what = `type ${type} flags ${d[off + 4]}`;
          if (type === f.RST_STREAM) {
            what += ` rst ${d.readUInt32BE(off + f.HEADER_LEN)}`;
          }
          if (type === f.GOAWAY) {
            what += ` goaway ${d.readUInt32BE(off + f.HEADER_LEN + 4)}`;
          }
          seen.push(what);
          off += f.HEADER_LEN + len;
          if (type === f.HEADERS && next < blocks.length) {
            sock.write(frame(f.HEADERS, (next === 0 ? flags : f.F_END_STREAM) | f.F_END_HEADERS, sid, blocks[next]));
            next++;
            if (next < blocks.length) {
              // The trailer block goes out after the client has had the
              // response; a stream that opens nothing still answers PING.
              setTimeout(() => {
                if (next < blocks.length) {
                  sock.write(frame(f.HEADERS, f.F_END_STREAM | f.F_END_HEADERS, sid, blocks[next]));
                  next++;
                }
              }, 150);
            }
          }
        }
      });
      setTimeout(() => sock.destroy(), 800);
    });
    srv.listen(0, '127.0.0.1', () => resolve({ srv, sockets, seen }));
  });
}

function run(impl, blocks, flags, sid) {
  return rawServer(blocks, flags, sid).then(({ srv, sockets, seen }) => new Promise((resolve) => {
    const url = `http://127.0.0.1:${srv.address().port}`;
    const sess = impl === 'node' ? http2.connect(url) : h2lite.connect(url);
    const said = [];
    sess.on('goaway', (code) => said.push(`goaway ${code}`));
    sess.on('error', (e) => said.push(`session error ${e.code || e.message}`));
    sess.on('close', () => said.push('close'));
    const req = sess.request({ ':path': '/' });
    req.on('response', (h) => said.push(`response ${JSON.stringify(h[':status'])}`));
    req.on('error', (e) => said.push(`stream error ${e.code || e.message}`));
    req.on('data', () => {});
    req.on('end', () => said.push('end'));
    req.on('close', () => said.push('stream close'));
    setTimeout(() => {
      sess.destroy();
      for (const s of sockets) {
        s.destroy();
      }
      srv.close();
      resolve({ said, seen });
    }, 900);
  }));
}

(async () => {
  const cases = [
    { name: 'no :status, one regular field',
      blocks: [lit('content-type', 'text/plain')], flags: f.F_END_STREAM, sid: 1 },
    { name: 'request pseudo :path on a response',
      blocks: [Buffer.concat([OK, lit(':path', '/')])], flags: f.F_END_STREAM, sid: 1 },
    { name: 'two :status',
      blocks: [Buffer.concat([OK, OK])], flags: f.F_END_STREAM, sid: 1 },
    { name: 'pseudo-header in a trailer block',
      blocks: [OK, lit(':status', '200')], flags: 0, sid: 1 }
  ];
  for (const c of cases) {
    const node = await run('node', c.blocks, c.flags, c.sid);
    const lite = await run('h2lite', c.blocks, c.flags, c.sid);
    console.log(`--- ${c.name}`);
    console.log(`    node   says: ${node.said.join(' | ') || 'nothing'}`);
    console.log(`    node   gets: ${node.seen.join(' | ') || 'nothing'}`);
    console.log(`    h2lite says: ${lite.said.join(' | ') || 'nothing'}`);
    console.log(`    h2lite gets: ${lite.seen.join(' | ') || 'nothing'}`);
  }
})();
