// Wire probe: starts one server (node:http2 or h2lean) in this process, sends a
// single hand-built HTTP/2 request over a raw socket and prints every frame that
// comes back. Used to diff the two stacks frame by frame -- the observable
// protocol is what a peer actually sees, not what our objects say.
//   node tools/probe-wire.js native
//   node tools/probe-wire.js h2lean
//   MODE=continue|head|trailers node tools/probe-wire.js native
const net = require('node:net');
const f = require('../lib/frames');
const { Decoder } = require('../lib/hpack');

const kind = process.argv[2] === 'h2lean' ? 'h2lean' : 'native';
const impl = kind === 'h2lean' ? require('../index') : require('node:http2');
const MODE = process.env.MODE || 'plain';

function handler(req, res) {
  if (MODE === 'continue') {
    res.writeContinue();
  }
  if (MODE === 'trailers') {
    res.setHeader('content-type', 'text/plain');
    res.writeHead(200, { 'x-a': '1' });
    res.addTrailers({ 'x-tail': 'end-of-body' });
    res.end('body');
    return;
  }
  res.writeHead(200, { 'x-a': '1' });
  res.end(MODE === 'empty' ? undefined : 'body');
}

const server = impl.createServer(handler);

// :method GET, :path "/" and :scheme http from the static table, then
// :authority as a literal with an indexed name (static 1) -- nghttp2 refuses a
// request without :authority or host, so the probe has to carry one
const BLOCK = Buffer.from([
  0x82, 0x84, 0x86,
  0x01, 9, ...Buffer.from('localhost', 'latin1')
]);

// literal without indexing, new name -- lengths computed, never counted by hand
function literal(name, value) {
  const n = Buffer.from(name, 'latin1');
  const v = Buffer.from(value, 'latin1');
  return Buffer.from([0x00, n.length, ...n, v.length, ...v]);
}

// expect: 100-continue as a literal without indexing, new name
const CONTINUE_BLOCK = Buffer.concat([BLOCK, literal('expect', '100-continue')]);

const REQUEST_BLOCK = MODE === 'continue' ? CONTINUE_BLOCK : BLOCK;

function decodeBlock(payload) {
  const out = [];
  decoder.decode(payload, 0, payload.length, out);
  const fields = [];
  for (let i = 0; i < out.length; i += 2) {
    fields.push(`${out[i]}=${out[i + 1]}`);
  }
  return fields.join(' ');
}

const decoder = new Decoder(4096);

server.on('sessionError', (e) => console.log(kind, 'sessionError', e.code, e.message));

server.listen(0, '127.0.0.1', () => {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, REQUEST_BLOCK.length, f.HEADERS,
    f.F_END_HEADERS | f.F_END_STREAM, 1);
  const emptySettings = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(emptySettings, 0, 0, f.SETTINGS, 0, 0);
  const sock = net.connect(server.address().port, '127.0.0.1', () => {
    // preface + empty SETTINGS + the request
    sock.write(Buffer.concat([f.PREFACE, emptySettings, head, REQUEST_BLOCK]));
  });
  let buf = Buffer.alloc(0);
  let done = false;
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
      const sid = buf.readUInt32BE(5) & 0x7fffffff;
      const payload = buf.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
      buf = buf.subarray(f.HEADER_LEN + len);
      let detail = '';
      if (type === f.HEADERS || type === f.CONTINUATION) {
        detail = ' ' + decodeBlock(payload);
      } else if (type === f.DATA) {
        detail = ` "${payload.toString('latin1')}"`;
      } else if (type === f.SETTINGS && len === 0) {
        detail = ' (none)';
      } else if (type === f.RST_STREAM) {
        detail = ` code=${payload.readUInt32BE(0)}`;
      } else if (type === f.GOAWAY) {
        detail = ` lastStream=${payload.readUInt32BE(0)} code=${payload.readUInt32BE(4)}` +
                 ` reason="${payload.subarray(8).toString('latin1')}"`;
      }
      console.log(`${kind}  type=${type} flags=0x${flags.toString(16)} len=${len} sid=${sid}${detail}`);
      if ((type === f.HEADERS || type === f.DATA) && (flags & f.F_END_STREAM)) {
        done = true;
      }
      if (type === f.GOAWAY) {
        done = true;
      }
      if (done) {
        break;
      }
    }
    if (done) {
      sock.destroy();
      server.close();
    }
  });
  sock.on('error', (e) => console.log(kind, 'socket error', e.code));
  setTimeout(() => {
    console.log(kind, 'stopped after 1200ms, done =', done); process.exit(0);
  }, 1200);
});
