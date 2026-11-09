// Which rule wins when a regular field's value carries a CR *and* the field has
// a rule of its own?
//
//   node bench/micro/probe-value-order.js native
//   node bench/micro/probe-value-order.js h2lite
//
// One request per case: the field is dropped (nghttp2's IGN_HTTP_HEADER), the
// stream is refused (PROTOCOL_ERROR), or the request is served with the field
// in it. The verdict is what the peer saw.
'use strict';
const net = require('node:net');
const f = require('../../lib/frames');

const kind = process.argv[2] === 'h2lite' ? 'h2lite' : 'native';
const impl = kind === 'h2lite' ? require('../../index') : require('node:http2');

// (an old `const CR = 0x0d` was read by nothing and is gone; the cases below
// carry the CR as a literal)
const CASES = [
  { name: 'x-a', value: '1\r' },
  { name: 'te', value: 'trailers\r' },
  { name: 'content-length', value: '5\r' },
  { name: 'host', value: 'a\rb' },
  { name: 'x-a', value: '1\r', twice: true }
];

function handler(req, res) {
  console.log('    HANDLER', JSON.stringify(req.headers));
  res.writeHead(200, { 'x-a': '1' });
  res.end();
}

const server = impl.createServer(handler);

function blockFor(name, value) {
  const n = Buffer.from(name, 'latin1');
  const v = Buffer.from(value, 'latin1');
  const line = Buffer.from([0x00, n.length, ...n, v.length, ...v]);
  return Buffer.concat([
    Buffer.from([0x82, 0x84, 0x86, 0x01, 9, ...Buffer.from('localhost', 'latin1')]),
    line
  ]);
}

let i = 0;
function next() {
  if (i >= CASES.length) {
    setTimeout(() => {
      server.close(); process.exit(0);
    }, 200);
    return;
  }
  const c = CASES[i++];
  const block = blockFor(c.name, c.value);
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, block.length, f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM,
    1 + 2 * (i - 1));
  const emptySettings = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(emptySettings, 0, 0, f.SETTINGS, 0, 0);
  const sock = net.connect(server.address().port, '127.0.0.1', () => {
    sock.write(Buffer.concat([f.PREFACE, emptySettings, head, block]));
  });
  let buf = Buffer.alloc(0);
  let verdict = 'nothing';
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
      const payload = buf.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
      buf = buf.subarray(f.HEADER_LEN + len);
      if (type === f.RST_STREAM) {
        verdict = 'RST_STREAM ' + payload.readUInt32BE(0);
      } else if (type === f.GOAWAY) {
        verdict = 'GOAWAY ' + payload.readUInt32BE(4) + ' ' +
          JSON.stringify(payload.subarray(8).toString('latin1'));
      } else if (type === f.HEADERS) {
        verdict = 'served';
      }
      if (verdict !== 'nothing') {
        console.log(`  ${c.name}: ${JSON.stringify(c.value)} -> ${verdict}`);
        sock.destroy();
        next();
        return;
      }
    }
  });
  sock.on('error', () => {});
  setTimeout(() => {
    if (verdict === 'nothing') {
      console.log(`  ${c.name}: ${JSON.stringify(c.value)} -> nothing`);
      sock.destroy();
      next();
    }
  }, 600);
}

server.listen(0, '127.0.0.1', next);
