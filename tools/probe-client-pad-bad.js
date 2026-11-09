// What does a client answer when the server sends a padded DATA frame whose
// padding does not fit, or one with an empty payload?
//
// RFC 9113 6.1: "If the length of the padding is the length of the frame payload
// or greater, the recipient MUST treat this as a connection error of type
// PROTOCOL_ERROR." A padded frame with an empty payload has no pad-length octet
// at all, which is also malformed.
//
// The server is raw; the client is node's and h2lite's. Both sides are reported:
// what the client says (goaway, error, close) and what the server receives.
//
// Usage: node tools/probe-client-pad-bad.js
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

function rawServer(payload, flags) {
  return new Promise((resolve) => {
    const seen = [];
    const sockets = [];
    const srv = net.createServer((sock) => {
      sockets.push(sock);
      sock.on('error', () => {});
      sock.write(frame(f.SETTINGS, 0, 0, Buffer.alloc(0)));
      let gotPreface = false;
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
          let what = `type ${type} flags ${d[off + 4]} len ${len}`;
          if (type === f.GOAWAY) {
            what += ` code ${d.readUInt32BE(off + f.HEADER_LEN + 4)}`;
          }
          seen.push(what);
          off += f.HEADER_LEN + len;
          if (type === f.HEADERS) {
            // The answer comes before the malformed frame, so the client has a
            // stream to receive it on.
            sock.write(frame(f.HEADERS, f.F_END_HEADERS, 1, Buffer.from([0x88])));
            sock.write(frame(f.DATA, flags, 1, payload));
          }
        }
      });
      setTimeout(() => sock.destroy(), 800);
    });
    srv.listen(0, '127.0.0.1', () => resolve({ srv, sockets, seen }));
  });
}

function run(impl, payload, flags) {
  return rawServer(payload, flags).then(({ srv, sockets, seen }) => new Promise((resolve) => {
    const url = `http://127.0.0.1:${srv.address().port}`;
    const sess = impl === 'node' ? http2.connect(url) : h2lite.connect(url);
    const said = [];
    sess.on('goaway', (code, _last) => {
      said.push(`goaway ${code}`);
    });
    sess.on('error', (e) => {
      said.push(`session error ${e.code || e.message}`);
    });
    sess.on('close', () => {
      said.push('close');
    });
    const req = sess.request({ ':path': '/' });
    req.on('error', (e) => {
      said.push(`stream error ${e.code || e.message}`);
    });
    req.on('data', () => {});
    req.on('end', () => {
      said.push('end');
    });
    req.on('close', () => {
      said.push('stream close');
    });
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
    { name: 'pad length 200 over a 4 octet payload', payload: Buffer.concat([Buffer.from([200]), Buffer.alloc(3)]), flags: f.F_PADDED },
    { name: 'PADDED with an empty payload', payload: Buffer.alloc(0), flags: f.F_PADDED }
  ];
  for (const c of cases) {
    const node = await run('node', c.payload, c.flags);
    const lite = await run('h2lite', c.payload, c.flags);
    console.log(`--- ${c.name}`);
    console.log(`    node   says: ${node.said.join(' | ') || 'nothing'}`);
    console.log(`    node   gets: ${node.seen.join(' | ') || 'nothing'}`);
    console.log(`    h2lite says: ${lite.said.join(' | ') || 'nothing'}`);
    console.log(`    h2lite gets: ${lite.seen.join(' | ') || 'nothing'}`);
  }
})();
