// Which characters in an OUTBOUND field value does each server refuse?
//
//   node bench/micro/probe-out-value-matrix.js native
//   node bench/micro/probe-out-value-matrix.js h2lite
//
// One server per character: the handler answers with 'x-c: a<CHAR>b' and reports
// what happened -- the value went out, or the server threw. The wire is printed
// as well, because "the value went out" has to mean "the value is on the wire".
'use strict';
const net = require('node:net');
const f = require('../../lib/frames');

const kind = process.argv[2] === 'h2lite' ? 'h2lite' : 'native';
const impl = kind === 'h2lite' ? require('../../index') : require('node:http2');

const CHARS = [0x00, 0x01, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x1f,
  0x20, 0x7e, 0x7f, 0x80, 0xff];

const BLOCK = Buffer.from([0x82, 0x84, 0x86, 0x01, 9, ...Buffer.from('localhost', 'latin1')]);

function one(c, done) {
  let outcome = 'no answer';
  const server = impl.createServer((req, res) => {
    try {
      res.writeHead(200, { 'x-c': 'a' + String.fromCharCode(c) + 'b' });
      res.end();
      outcome = 'sent';
    } catch (err) {
      outcome = 'threw ' + (err.code || err.name);
    }
  });
  server.listen(0, '127.0.0.1', () => {
    const head = Buffer.alloc(f.HEADER_LEN);
    f.writeHeader(head, 0, BLOCK.length, f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1);
    const emptySettings = Buffer.alloc(f.HEADER_LEN);
    f.writeHeader(emptySettings, 0, 0, f.SETTINGS, 0, 0);
    const sock = net.connect(server.address().port, '127.0.0.1', () => {
      sock.write(Buffer.concat([f.PREFACE, emptySettings, head, BLOCK]));
    });
    let buf = Buffer.alloc(0);
    let wire = null;
    sock.on('data', (d) => {
      buf = Buffer.concat([buf, d]);
      for (;;) {
        if (buf.length < f.HEADER_LEN) {
          return;
        }
        const len = (buf[0] << 16) | (buf[1] << 8) | buf[2];
        if (buf.length < f.HEADER_LEN + len) {
          return;
        }
        const type = buf[3];
        const flags = buf[4];
        const payload = buf.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
        buf = buf.subarray(f.HEADER_LEN + len);
        if (type === f.HEADERS) {
          wire = payload.toString('latin1');
        }
        if (type === f.GOAWAY) {
          wire = 'GOAWAY ' + payload.readUInt32BE(4) + ' ' +
          payload.subarray(8).toString('latin1');
        }
        if ((type === f.HEADERS && (flags & f.F_END_STREAM)) || type === f.GOAWAY) {
          console.log(`  0x${c.toString(16).padStart(2, '0')}  ${outcome}` +
            (wire === null ? '  (no headers frame)' : '  wire=' + JSON.stringify(wire)));
          sock.destroy();
          server.close();
          done();
          return;
        }
      }
    });
    sock.on('error', () => {});
    setTimeout(() => {
      console.log(`  0x${c.toString(16).padStart(2, '0')}  ${outcome}  (timeout)`);
      sock.destroy();
      server.close();
      done();
    }, 700);
  });
}

let i = 0;
(function next() {
  if (i >= CHARS.length) {
    return;
  }
  const c = CHARS[i++];
  one(c, next);
})();
