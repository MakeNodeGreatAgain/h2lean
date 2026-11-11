// A request whose header block arrives in two reads.
//
// The block is decoded out of the views it arrived in (lib/hpack.js decodePieces),
// not out of a copy of them, and the cut may fall anywhere: inside a literal,
// between two field lines, inside the frame header, or exactly at the end of it
// -- where the payload arrives as an empty view and the octets that follow, and
// the decoder's cursor may not stop on the empty one.
//
// The same exchange is driven against node:http2 and the two are compared, because
// a block that is decoded wrongly is a request the application never sees. The
// piece path is counted too: a block that arrived whole is decoded by the
// contiguous path, and this test is about the other one.
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const http2 = require('node:http2');
const f = require('../lib/frames');
const h2lean = require('../index');

// :method POST (3), :path / (4) and :scheme http (6) are static indexes; the
// :authority and the x-split field are literals, so the block holds octets that
// only the decoder can turn back into names and values -- and a cut inside one of
// them is a cut inside a string.
const BLOCK = Buffer.concat([
  Buffer.from([0x83, 0x84, 0x86]),
  Buffer.from([0x00, 10, ...Buffer.from(':authority', 'latin1'), 9,
    ...Buffer.from('localhost', 'latin1')]),
  Buffer.from([0x00, 7, ...Buffer.from('x-split', 'latin1'), 5,
    ...Buffer.from('value', 'latin1')])
]);

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// The frames in a byte string, in the order they arrived.
function frames(buf) {
  const out = [];
  let at = 0;
  while (at + f.HEADER_LEN <= buf.length) {
    const len = buf.readUIntBE(at, 3) & 0xffffff; // 24-bit length
    const type = buf[at + 3];
    if (at + f.HEADER_LEN + len > buf.length) {
      break;
    }
    out.push({ type, payload: buf.subarray(at + f.HEADER_LEN, at + f.HEADER_LEN + len) });
    at += f.HEADER_LEN + len;
  }
  return out;
}

// One exchange: the request's HEADERS frame is cut at `cut`, and the two halves
// are written in different ticks so the server's read boundary falls there. What
// comes back is what the application saw and what the peer answered.
function exchange(impl, cut) {
  return new Promise((resolve, reject) => {
    const seen = { fields: null, status: null };
    let pieces = 0;
    const srv = impl.createServer();
    srv.on('session', (s) => {
      // The piece path is the one that decodes a block out of views; the count is
      // what tells the two paths apart from the outside.
      const orig = s._processBlockPieces;
      s._processBlockPieces = function (...args) {
        pieces++;
        return orig.apply(this, args);
      };
    });
    srv.on('request', (req, res) => {
      // The string keys only: node's headers carry a Symbol(sensitiveHeaders)
      // beside them, and a symbol is not a field.
      const fields = {};
      for (const key of Object.keys(req.headers)) {
        fields[key] = req.headers[key];
      }
      seen.fields = fields;
      res.writeHead(200, { 'content-type': 'text/plain' });
      res.end('ok');
    });
    srv.on('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const port = srv.address().port;
      const sock = net.connect(port, '127.0.0.1', () => {
        const head = frame(f.HEADERS, f.F_END_HEADERS, 1, BLOCK);
        sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
        sock.write(head.subarray(0, cut));
        setTimeout(() => sock.write(head.subarray(cut)), 30);
      });
      sock.on('error', () => {});
      let out = Buffer.alloc(0);
      sock.on('data', (d) => {
        out = Buffer.concat([out, d]);
        for (const fr of frames(out)) {
          if (fr.type !== f.HEADERS) {
            continue;
          }
          // The response block is decoded with the same decoder the server uses:
          // the fields it carries are the answer this test compares.
          const dec = new h2lean.HPack.Decoder(4096);
          const fields = [];
          dec.decode(fr.payload, 0, fr.payload.length, fields);
          for (let i = 0; i < fields.length; i += 2) {
            if (fields[i] === ':status') {
              seen.status = Number(fields[i + 1]);
            }
          }
          if (seen.status !== null) {
            sock.destroy();
            srv.close();
            resolve({ ...seen, pieces });
            return;
          }
        }
      });
      setTimeout(() => {
        sock.destroy();
        srv.close();
        reject(new Error('no answer'));
      }, 3000);
    });
  });
}

// Where the cut may fall. The frame is 9 octets of header and 40 of block, so
// these are: inside the frame header, at the first octet of the block, inside a
// literal name, inside a literal value, between two field lines, at the last
// octet of the block, and -- the control -- the whole frame in one write.
const CUTS = [5, 8, 9, 10, 12, 16, 20, 26, 30, 34, 40, 49];

test('a request block split across two reads is decoded like node\'s', async () => {
  for (const cut of CUTS) {
    const ours = await exchange(h2lean, cut);
    const node = await exchange(http2, cut);
    assert.deepStrictEqual(ours.fields, node.fields, `cut at ${cut}: the fields differ`);
    assert.strictEqual(ours.status, 200, `cut at ${cut}: h2lean answered ${ours.status}`);
    assert.strictEqual(node.status, 200, `cut at ${cut}: node answered ${node.status}`);
    assert.strictEqual(ours.fields['x-split'], 'value', `cut at ${cut}: the literal was lost`);
    assert.strictEqual(ours.fields[':authority'], 'localhost', `cut at ${cut}: the authority was lost`);
    // Every cut but the last one splits the frame, and a split frame is decoded
    // out of its pieces. The last one does not, and the contiguous path answers it.
    if (cut < 49) {
      assert.ok(ours.pieces > 0, `cut at ${cut}: the block was not decoded out of the pieces`);
    }
  }
});
