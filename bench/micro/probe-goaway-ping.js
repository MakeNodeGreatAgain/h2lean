// h2spec's http2/7 shape, byte for byte, and the manner in which the connection ends.
//
//   node bench/micro/probe-goaway-ping.js
//
// h2spec writes a GOAWAY carrying an error code no version of the spec defines (255),
// with a PING behind it, and the check passes for a FIN and fails for a reset: the
// tool reads "the endpoint closed the connection" and calls ECONNRESET a failure.
//
// The two frames are written in ONE write here, which is what h2spec does and what
// the probe in tools/probe-inbound-limits.js does NOT do (it writes them one after
// the other), so the two land in one read on the server and the server's teardown
// sees the PING it may still have to answer.
'use strict';
const net = require('node:net');
const f = require('../../lib/frames');
const h2lean = require('../../index');
const nodeHttp2 = require('node:http2');
const { listen } = require('../test/diff-harness');

const PREFACE = Buffer.from('PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n', 'latin1');

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// GOAWAY with the last-stream id 0 and an error code of `code`.
function goaway(code) {
  const payload = Buffer.alloc(8);
  payload.writeUInt32BE(0, 0);
  payload.writeUInt32BE(code, 4);
  return frame(f.GOAWAY, 0, 0, payload);
}

const PING = frame(f.PING, 0, 0, Buffer.from('h2spec00', 'latin1'));

function drive(port, send) {
  return new Promise((resolve) => {
    const seen = [];
    let acc = Buffer.alloc(0);
    const sock = net.connect(port, '127.0.0.1');
    sock.on('error', (e) => seen.push('sock:' + e.code));
    sock.on('end', () => seen.push('FIN'));
    sock.on('close', () => seen.push('closed'));
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
        const type = acc[3], flags = acc[4];
        const payload = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
        acc = acc.subarray(f.HEADER_LEN + len);
        if (type === f.SETTINGS && !(flags & f.F_ACK)) {
          seen.push('SETTINGS');
          sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
          send(sock);
        } else if (type === f.GOAWAY) {
          seen.push(`GOAWAY(${payload.readUInt32BE(4)})`);
        } else if (type === f.PING) {
          seen.push(flags & f.F_ACK ? 'PING-ACK' : 'PING');
        }
      }
    });
    sock.write(PREFACE);
    sock.write(frame(f.SETTINGS, 0, 0, Buffer.alloc(0)));
    setTimeout(() => {
      sock.destroy();
      resolve(seen.join(' '));
    }, 400);
  });
}

function serve(impl) {
  const srv = impl.createServer();
  srv.on('stream', (stream) => {
    stream.on('error', () => {});
    stream.respond({ ':status': 200 });
    stream.end('ok');
  });
  srv.on('error', () => {});
  return listen(srv).then((port) => ({ srv, port }));
}

(async () => {
  for (const [name, impl] of [['node', nodeHttp2], ['h2lean', h2lean]]) {
    const { srv, port } = await serve(impl);
    const one = await drive(port, (sock) => {
      sock.write(Buffer.concat([goaway(255), PING]));
    });
    const two = await drive(port, (sock) => {
      sock.write(goaway(255));
      sock.write(PING);
    });
    console.log(`${name.padEnd(7)} one write : ${one}`);
    console.log(`${name.padEnd(7)} two writes: ${two}`);
    srv.close();
  }
})();
