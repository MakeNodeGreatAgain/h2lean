// What does node do with a padded DATA frame whose padding does not fit?
//
// RFC 9113 6.1: "If the length of the padding is the length of the frame payload
// or greater, the recipient MUST treat this as a connection error of type
// PROTOCOL_ERROR." A padded frame with an empty payload has no pad-length octet
// at all, which is also malformed.
//
// The server is node's or h2lean's; the client is raw. Three shapes are tried:
//   * a pad length larger than the payload that carries it;
//   * a pad length equal to the rest of the payload (legal);
//   * PADDED set on a frame with an empty payload.
//
// Usage: node tools/probe-pad-bad.js
'use strict';
const net = require('node:net');
const http2 = require('node:http2');
const h2lean = require('../index');
const f = require('../lib/frames');

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

function rawClient(port, payload, flags) {
  return new Promise((resolve) => {
    const seen = [];
    const sock = net.connect(port, '127.0.0.1', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
      sock.write(frame(f.HEADERS, f.F_END_HEADERS, 1,
        Buffer.concat([Buffer.from([0x82, 0x87, 0x84, 0x41, 0x09]), Buffer.from('127.0.0.1')])));
      sock.write(frame(f.DATA, flags, 1, payload));
    });
    sock.on('data', (d) => {
      let off = 0;
      while (off + f.HEADER_LEN <= d.length) {
        const len = (d[off] << 16) | (d[off + 1] << 8) | d[off + 2];
        const type = d[off + 3];
        if (off + f.HEADER_LEN + len > d.length) {
          break;
        }
        let what = `type ${type} flags ${d[off + 4]} len ${len}`;
        if (type === f.RST_STREAM) {
          what += ` code ${d.readUInt32BE(off + f.HEADER_LEN)}`;
        }
        if (type === f.GOAWAY) {
          what += ` code ${d.readUInt32BE(off + f.HEADER_LEN)}`;
        }
        seen.push(what);
        off += f.HEADER_LEN + len;
      }
    });
    sock.on('close', () => resolve(seen));
    sock.on('error', (e) => {
      seen.push(`socket error ${e.code}`); resolve(seen);
    });
    setTimeout(() => {
      sock.destroy(); resolve(seen);
    }, 800);
  });
}

function h2leanServer(payload, flags) {
  return new Promise((resolve) => {
    const srv = h2lean.createServer();
    srv.on('stream', (stream) => {
      stream.on('error', () => {});
      stream.on('data', () => {});
      stream.on('end', () => {
        if (!stream.destroyed) {
          stream.respond({ ':status': 200 }, { endStream: true });
        }
      });
    });
    srv.on('session', (s) => {
      s.on('error', () => {});
    });
    srv.listen(0, '127.0.0.1', async () => {
      const seen = await rawClient(srv.address().port, payload, flags);
      srv.close();
      resolve(seen);
    });
  });
}

async function nodeServer(payload, flags) {
  const srv = http2.createServer();
  srv.on('stream', (stream) => {
    stream.on('error', () => {});
    stream.on('data', () => {});
    stream.on('end', () => {
      if (!stream.destroyed) {
        stream.respond({ ':status': 200 }, { endStream: true });
      }
    });
  });
  srv.on('session', (s) => {
    s.on('error', () => {});
  });
  await new Promise((r) => srv.listen(0, '127.0.0.1', r));
  const seen = await rawClient(srv.address().port, payload, flags);
  srv.close();
  return seen;
}

(async () => {
  const cases = [
    { name: 'pad length 200 over a 4 octet payload', payload: Buffer.concat([Buffer.from([200]), Buffer.alloc(3)]), flags: f.F_PADDED },
    { name: 'pad length equal to the rest of the payload', payload: Buffer.concat([Buffer.from([3]), Buffer.alloc(3)]), flags: f.F_PADDED },
    { name: 'PADDED with an empty payload', payload: Buffer.alloc(0), flags: f.F_PADDED }
  ];
  for (const c of cases) {
    const nodeSeen = await nodeServer(c.payload, c.flags);
    const liteSeen = await h2leanServer(c.payload, c.flags);
    console.log(`--- ${c.name}`);
    console.log(`    node:   ${nodeSeen.join(' | ') || 'nothing'}`);
    console.log(`    h2lean: ${liteSeen.join(' | ') || 'nothing'}`);
  }
})();
