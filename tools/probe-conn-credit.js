// How far does node let the CONNECTION receive window run down before it answers an upload with
// WINDOW_UPDATE(sid 0)? The client sends DATA as fast as the connection window allows and records,
// for every credit frame, how many body octets had gone out at that moment and how big the grant
// was. h2lite is measured the same way, so the threshold in _creditConnection() is a compared
// number: measured on node 26, the grants come in steps of 32768 octets, half the 65535 window it
// advertises. The upload also has to finish, which is the anti-deadlock half of the same number.
//
//   node tools/probe-conn-credit.js
'use strict';
const net = require('node:net');
const f = require('../lib/frames');
const h2lite = require('../index');
const nodeHttp2 = require('node:http2');

const REQ = Buffer.from([0x82, 0x84, 0x86, 0x01, 9, ...Buffer.from('localhost', 'latin1')]);
const CHUNK = 16384;
const TOTAL = 40 * CHUNK; // 640 KiB, plenty past one connection window

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

function run(label, impl) {
  return new Promise((done) => {
    const srv = impl.createServer();
    srv.on('stream', (stream) => {
      stream.on('error', () => {});
      let got = 0;
      stream.on('data', (d) => {
        got += d.length;
      });
      stream.on('end', () => {
        stream.respond({ ':status': 200 }, { endStream: true });
        console.log(`    ${label}: the application read ${got}`);
      });
    });
    srv.listen(0, '127.0.0.1', () => {
      const sock = net.connect(srv.address().port, '127.0.0.1');
      let sent = 0; // body octets on the wire
      let credit = 65535; // the connection window both stacks start us with
      let streamCredit = 65535; // ...and the stream window the same size
      let acc = Buffer.alloc(0);
      let headed = false;
      const marks = [];
      sock.on('error', () => {});
      sock.on('timeout', () => {
        console.log(`    ${label}: stalled after ${sent}, credits at ` + marks.join(','));
        sock.destroy(); srv.close(done);
      });
      function push() {
        if (!headed) {
          sock.write(frame(f.HEADERS, f.F_END_HEADERS, 1, REQ)); headed = true;
        }
        while (sent < TOTAL && credit >= CHUNK && streamCredit >= CHUNK) {
          const left = TOTAL - sent;
          const n = Math.min(CHUNK, left);
          sock.write(frame(f.DATA, n === left ? f.F_END_STREAM : 0, 1, Buffer.alloc(n, 0x61)));
          credit -= n;
          streamCredit -= n;
          sent += n;
        }
      }
      sock.setTimeout(1500);
      sock.on('data', (d) => {
        sock.setTimeout(1500);
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
          const sid = acc.readUInt32BE(5) & 0x7fffffff;
          const payload = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
          acc = acc.subarray(f.HEADER_LEN + len);
          if (type === f.SETTINGS && !(flags & f.F_ACK)) {
            sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
            push();
          } else if (type === f.WINDOW_UPDATE && sid === 0) {
            marks.push(`${sent}+${payload.readUInt32BE(0)}`);
            credit += payload.readUInt32BE(0);
            push();
          } else if (type === f.WINDOW_UPDATE && sid !== 0) {
            marks.push(`(s${sid}+${payload.readUInt32BE(0)})`);
            streamCredit += payload.readUInt32BE(0);
            push();
          } else if (type === f.HEADERS) {
            console.log(`    ${label}: answered at sent=${sent}, WINDOW_UPDATE(0) marks: `
              + marks.join(' '));
            sock.destroy();
            srv.close(done);
          }
        }
      });
      sock.on('connect', () => {
        sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
      });
    });
  });
}

(async () => {
  await run('node', nodeHttp2);
  await run('h2lite', h2lite);
})();
