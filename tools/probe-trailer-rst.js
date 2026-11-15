'use strict';
// The trailer-refusal shape of test/request-validation.test.js ('a trailer block
// may not contain pseudo-headers') driven against a live node server: after the
// refusal RST(PROTOCOL_ERROR), does the auto-close of the unread request add a
// second RST(NO_ERROR), or does the refusal's reset already close the stream?
//   node tools/probe-trailer-rst.js
const net = require('node:net');
const f = require('../lib/frames');

const NAMES = { 0: 'DATA', 1: 'HEADERS', 3: 'RST_STREAM', 4: 'SETTINGS', 7: 'GOAWAY' };
const B = (b) => Buffer.from(b === undefined ? [] : [b]);
// :method GET (indexed 2 -> 0x82? indexed field with static index 2 is 0x82... the
// test's GOOD block is reused: :method,:path,:scheme,:authority minimal request)
const GOOD = Buffer.from([0x82, 0x84, 0x86, 0x41, 9, ...Buffer.from('localhost', 'latin1')]);

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

const srv = require('node:http2').createServer();
srv.on('stream', (stream) => {
  stream.on('trailers', () => {}); stream.on('error', () => {});
  stream.respond({ ':status': 200 });
  stream.end('body');
});
srv.on('error', () => {});
srv.listen(0, '127.0.0.1', () => {
  const port = srv.address().port;
  const seen = [];
  const sock = net.connect(port, '127.0.0.1');
  let acc = Buffer.alloc(0);
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
      if (type === f.RST_STREAM && body.length >= 4) {
        seen.push(`RST(${body.readUInt32BE(0)})`);
      } else if (type === f.HEADERS && (flags & f.F_END_HEADERS)) {
        seen.push('headers');
      } else {
        seen.push(nm);
      }
    }
  });
  sock.on('connect', () => {
    sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, B()),
      frame(f.HEADERS, f.F_END_HEADERS, 1, GOOD),
      frame(f.DATA, 0, 1, Buffer.from('5')),
      frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, B(0x82))
    ]));
  });
  setTimeout(() => {
    console.log('node saw:', seen.join(' '));
    sock.destroy();
    srv.close();
  }, 400);
});
