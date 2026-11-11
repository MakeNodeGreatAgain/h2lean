// What does node do with a padded HEADERS frame whose padding does not fit?
//
// RFC 9113 6.2: "If the length of the padding is the length of the frame payload
// or greater, the recipient MUST treat this as a connection error of type
// PROTOCOL_ERROR." A padded frame with an empty payload has no pad-length octet
// at all, which is also malformed.
//
// The server is node's or h2lean's; the client is raw. Three shapes are tried:
//   * a pad length larger than the payload that carries it;
//   * a pad length equal to the rest of the payload (legal);
//   * PADDED set on a frame with an empty payload.
//
// Usage: node tools/probe-hdr-pad.js
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

// The header block of the request: :method GET, :scheme http, :path /, :authority
// "localhost". The block has to be COMPLETE and it has to say what it means:
// 0x01 is a literal without indexing whose name is the static index 1
// (:authority) and 0x09 is the length of its value -- nine octets that have to
// follow. A block that stops after the length is a truncated literal, and one
// that names the wrong index is a different header: 0x0f is index 15, which is
// :set-cookie, and a request may not carry that. Either way the decoder says
// COMPRESSION_ERROR before it ever looks at the padding, which hides the thing
// this probe is about.
const REQ = Buffer.from([
  0x82, // :method GET
  0x86, // :scheme http
  0x84, // :path /
  0x01, 0x09, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x73, 0x74 // :authority localhost
]);

function rawClient(port, headersPayload, headersFlags) {
  return new Promise((resolve) => {
    const seen = [];
    const sock = net.connect(port, '127.0.0.1', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
      sock.write(frame(f.HEADERS, headersFlags, 1, headersPayload));
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
          what += ` last ${d.readUInt32BE(off + f.HEADER_LEN)}`
            + ` error ${d.readUInt32BE(off + f.HEADER_LEN + 4)}`;
        }
        if (type === f.GOAWAY && len > 4) {
          what += ` debug "${d.toString('latin1', off + f.HEADER_LEN + 8, off + f.HEADER_LEN + len)}"`;
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
    { name: 'pad length 200 over the block',
      payload: Buffer.concat([Buffer.from([200]), REQ]), flags: f.F_PADDED },
    { name: 'pad length equal to the rest of the payload',
      payload: Buffer.concat([Buffer.from([REQ.length]), REQ]), flags: f.F_PADDED },
    { name: 'the RFC shape: pad length, block, padding',
      payload: Buffer.concat([Buffer.from([8]), REQ, Buffer.alloc(8)]),
      flags: f.F_PADDED | f.F_END_STREAM | f.F_END_HEADERS },
    { name: 'PADDED with an empty payload',
      payload: Buffer.alloc(0), flags: f.F_PADDED }
  ];
  for (const c of cases) {
    const nodeSeen = await nodeServer(c.payload, c.flags);
    const liteSeen = await h2leanServer(c.payload, c.flags);
    console.log(`--- ${c.name}`);
    console.log(`    node:   ${nodeSeen.join(' | ') || 'nothing'}`);
    console.log(`    h2lean: ${liteSeen.join(' | ') || 'nothing'}`);
  }
})();
