// Why a 1 500 000-octet literal can lose its verdict on the way back.
//
// The case in test/inbound-limits.test.js expects GOAWAY(COMPRESSION_ERROR) from both stacks
// when a request carries a literal value longer than the cap. Node refuses the value as soon
// as it has read the length prefix -- so it closes the connection with megabytes still unread
// at its side, and a close with unread inbound data is an RST. An RST discards whatever the
// peer has already sent us, so the GOAWAY can be lost before the client reads it, and the
// case reports `nothing` for a refusal that was on the wire.
//
//   node tools/probe-big-lit.js [rounds] [valueOctets]
//
// Per round it prints whether the GOAWAY arrived, what the client's socket ended with, and
// how many octets the server had read when it closed -- the number that says whether the
// close was an RST.
'use strict';
const net = require('node:net');
const http2 = require('node:http2');
const f = require('../lib/frames');

const ROUNDS = Number(process.argv[2] || 20);
const VALUE = Number(process.argv[3] || 1500000);

const REQ = Buffer.from([0x82, 0x84, 0x86, 0x01, 9, ...Buffer.from('localhost', 'latin1')]);
const EOH = f.F_END_HEADERS, EOS = f.F_END_STREAM;

// RFC 7541 5.1 -- the integer representation with the 7-bit prefix an indexed field uses
function int7(prefix, n) {
  if (n < 0x80 - 1) {
    return Buffer.from([prefix | n]);
  }
  const bytes = [prefix | 0x7f];
  n -= 0x80 - 1;
  while (n >= 0x80) {
    bytes.push(n & 0x7f | 0x80); n = Math.floor(n / 128);
  }
  bytes.push(n);
  return Buffer.from(bytes);
}

// one literal field line, never indexed, no Huffman
function lit(nameLen, valueLen) {
  return Buffer.concat([
    Buffer.from([0x00]), int7(0x00, nameLen), Buffer.from('x'.repeat(nameLen), 'latin1'),
    int7(0x00, valueLen), Buffer.from('y'.repeat(valueLen), 'latin1')
  ]);
}

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

const block = Buffer.concat([REQ, lit(1, VALUE)]);
const frameBytes = f.HEADER_LEN + block.length;

function round(port) {
  return new Promise((resolve) => {
    const t0 = Date.now();
    const sock = net.connect(port, '127.0.0.1');
    let acc = Buffer.alloc(0);
    let goaway = null;
    let err = null;
    let sent = false;
    sock.on('error', (e) => {
      err = e.code;
    });
    sock.on('connect', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
    });
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
        if (type === f.SETTINGS && !(flags & f.F_ACK) && !sent) {
          sent = true;
          sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
          sock.write(frame(f.HEADERS, EOH | EOS, 1, block));
        } else if (type === f.GOAWAY) {
          goaway = { code: payload.readUInt32BE(4), at: Date.now() - t0 };
        }
      }
    });
    setTimeout(() => {
      sock.destroy();
      resolve({ goaway, err });
    }, 2000);
  });
}

(async () => {
  const srv = http2.createServer({
    settings: { maxFrameSize: 2097152, maxHeaderListSize: 10000000 }
  });
  srv.on('stream', (s) => {
    s.on('error', () => {});
  });
  srv.on('session', (s) => {
    s.on('error', () => {});
  });
  // the raw socket of the connection: its bytesRead at the close is what the server had read
  // when it refused
  let read = null;
  srv.on('connection', (sock) => {
    sock.on('close', () => {
      read = sock.bytesRead;
    });
  });
  await new Promise((r) => srv.listen(0, '127.0.0.1', r));
  const port = srv.address().port;
  let got = 0, lost = 0;
  for (let i = 0; i < ROUNDS; i++) {
    const r = await round(port);
    if (r.goaway) {
      got++;
    } else {
      lost++;
    }
    console.log(`round ${i + 1}: goaway=${r.goaway ? r.goaway.code : 'NONE'}`
      + ` at=${r.goaway ? r.goaway.at : '-'}ms err=${r.err}`
      + ` serverRead=${read} of ${frameBytes} (unread ${frameBytes - (read || 0)})`);
  }
  console.log(`\ngot ${got} / lost ${lost} of ${ROUNDS}`);
  srv.close();
  process.exit(0);
})();
