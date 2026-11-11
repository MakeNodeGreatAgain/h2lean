// Which characters in an INBOUND field value make the field disappear?
//
//   node bench/micro/probe-value-matrix.js native
//   node bench/micro/probe-value-matrix.js h2lean
//
// One request per character, each on its own stream, each carrying one field
// 'x-c' whose value is 'a' + <char> + 'b'. The handler prints what it saw, so
// the answer is "which characters does the peer drop" -- measured, not guessed.
'use strict';
const net = require('node:net');
const f = require('../../lib/frames');

const kind = process.argv[2] === 'h2lean' ? 'h2lean' : 'native';
const impl = kind === 'h2lean' ? require('../../index') : require('node:http2');

const CHARS = [0x00, 0x01, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x1f,
  0x20, 0x7e, 0x7f, 0x80, 0xff];

function handler(req, res) {
  const v = req.headers['x-c'];
  console.log('  id=' + (req.stream ? req.stream.id : req.id) + ' x-c=' +
    (v === undefined ? 'DROPPED' : JSON.stringify(v)));
  res.writeHead(200, { 'x-a': '1' });
  res.end();
}

const server = impl.createServer(handler);

function blockFor(value) {
  return Buffer.concat([
    Buffer.from([0x82, 0x84, 0x86, 0x01, 9, ...Buffer.from('localhost', 'latin1')]),
    Buffer.from([0x00, 0x03, ...Buffer.from('x-c', 'latin1'), value.length,
      ...value])
  ]);
}

server.listen(0, '127.0.0.1', () => {
  const sock = net.connect(server.address().port, '127.0.0.1', () => {
    const parts = [f.PREFACE];
    const emptySettings = Buffer.alloc(f.HEADER_LEN);
    f.writeHeader(emptySettings, 0, 0, f.SETTINGS, 0, 0);
    parts.push(emptySettings);
    CHARS.forEach((c, i) => {
      const value = Buffer.from([0x61, c, 0x62]);
      const block = blockFor(value);
      const head = Buffer.alloc(f.HEADER_LEN);
      f.writeHeader(head, 0, block.length, f.HEADERS,
        f.F_END_HEADERS | f.F_END_STREAM, 1 + 2 * i);
      parts.push(head, block);
    });
    sock.write(Buffer.concat(parts));
  });
  sock.on('data', () => {});
  sock.on('error', (e) => console.log('socket error', e.code));
  setTimeout(() => {
    sock.destroy(); server.close(); process.exit(0);
  }, 1500);
});
