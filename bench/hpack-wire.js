// What a repeating response costs on the wire, measured at a raw socket: one
// connection, the same response shape for every request, and the HEADERS payload
// bytes summed per response. This is the number the HPACK dynamic table is for --
// CPU is measured by bench/matrix.sh, and a smaller header block is the whole point.
//
//   node bench/hpack-wire.js --server=h2lite --api=stream
//   node bench/hpack-wire.js --server=native --api=compat
//
//   NOTABLE=1  h2lite with its insertion path disabled, i.e. the behaviour from
//              before the dynamic table existed. A measurement patch in the bench
//              process, not an option in the library.
'use strict';
const net = require('node:net');
const f = require('../lib/frames');
const hpack = require('../lib/hpack');

const arg = (name, dflt) => {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit === undefined ? dflt : hit.split('=')[1];
};
const SERVER = arg('server', 'h2lite');
const API = arg('api', 'stream');
const N = Number(arg('requests', 40));
const TABLE = process.env.NOTABLE !== '1';

if (SERVER === 'h2lite' && !TABLE) {
  // The pre-feature encoder: a static index when the pair is known, a literal
  // otherwise, nothing ever inserted, and every line cached from the first use.
  hpack.Encoder.prototype.emit = function (dst, off, name, value, neverIndex) {
    const full = neverIndex ? undefined : hpack.FULL_INDEX.get(`${name}\0${value}`);
    let out;
    if (full !== undefined) {
      const b = Buffer.allocUnsafe(4);
      out = b.subarray(0, hpack.writeInt(b, 0, full, 7, 0x80));
    } else {
      out = this.literal(name, value, neverIndex ? 0x10 : 0x00);
    }
    this.remember(name, value, neverIndex, out);
    return this.put(dst, off, out);
  };
}

// The shape bench/h2lite-h2.js answers with, so the two are about the same work.
// Both servers are asked to add a `date`: node's stream API does it on its own, and
// h2lite only does it when told, so without this the two would not be sending the same
// fields. `date` also costs bytes, and it repeats for a whole second -- so it is one of
// the lines the table buys back.
const FIELDS = {
  'content-type': 'text/plain',
  server: 'prof',
  'x-trace': '0123456789abcdef0123456789abcdef'
};
const FROZEN = Object.freeze({ ':status': '200', ...FIELDS });

const impl = SERVER === 'native' ? require('node:http2') : require('../index');
const server = API === 'compat'
  ? impl.createServer((req, res) => {
    res.writeHead(200, FIELDS); res.end();
  })
  : impl.createServer();
if (API !== 'compat') {
  server.on('stream', (stream) => {
    // node builds a fresh object per response whether we hand it one or not; h2lite gets
    // the frozen constant, which is the shape the matrix runs on
    if (SERVER === 'native') {
      stream.respond({ ':status': '200', ...FIELDS });
    } else {
      stream.respond(FROZEN, { sendDate: true });
    }
    stream.end();
  });
}

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// :method GET, :path /, :scheme http from the static table, then :authority -- a peer
// built on nghttp2 refuses a request without it, so the client has to carry one.
const REQUEST = Buffer.from([0x82, 0x84, 0x86, 0x01, 9,
  ...Buffer.from('localhost', 'latin1')]);

server.listen(0, '127.0.0.1', () => {
  const port = server.address().port;
  const sock = net.connect(port, '127.0.0.1');
  let pending = Buffer.alloc(0);
  const perStream = new Map();
  sock.on('error', (e) => {
    console.log('sock error', e.message);
  });
  sock.on('data', (d) => {
    pending = Buffer.concat([pending, d]);
    for (;;) {
      if (pending.length < f.HEADER_LEN) {
        return;
      }
      const len = (pending[0] << 16) | (pending[1] << 8) | pending[2];
      if (pending.length < f.HEADER_LEN + len) {
        return;
      }
      const type = pending[3];
      const sid = pending.readUInt32BE(5) & 0x7fffffff;
      // HEADERS plus any CONTINUATION of the same stream: what a response costs in
      // header octets is the sum of its field-block frames
      if (type === f.HEADERS || type === f.CONTINUATION) {
        perStream.set(sid, (perStream.get(sid) || 0) + len);
      }
      pending = pending.subarray(f.HEADER_LEN + len);
      if (type === f.GOAWAY) {
        return;
      }
    }
  });
  sock.on('connect', () => {
    const out = [f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))];
    for (let i = 0; i < N; i++) {
      out.push(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1 + i * 2, REQUEST));
    }
    sock.write(Buffer.concat(out));
    setTimeout(() => {
      const ordered = [];
      for (let i = 0; i < N; i++) {
        ordered.push(perStream.get(1 + i * 2) || 0);
      }
      report(ordered);
      sock.destroy();
      server.close();
    }, 800);
  });
});

function report(ordered) {
  const got = ordered.filter((n) => n > 0);
  const tail = got.slice(Math.min(8, got.length));
  const mean = tail.reduce((a, b) => a + b, 0) / (tail.length || 1);
  console.log(`${SERVER}/${API} table=${TABLE} responses=${got.length}/${N}`);
  console.log(`  first four blocks: ${got.slice(0, 4).join(' ')} bytes`);
  console.log(`  settled mean:      ${mean.toFixed(2)} bytes per response`);
  const total = got.reduce((a, b) => a + b, 0);
  console.log(`  whole run:         ${total} header bytes for ${got.length} responses`
    + ` (${(total / (got.length || 1)).toFixed(2)} B/req)`);
}
