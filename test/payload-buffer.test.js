// The buffer handed to the application for a DATA payload is a *view* whenever
// the frame arrived whole inside one socket read, because node never writes into
// a chunk it has already delivered (tools/probe-read-allocator.js measures that
// on both transports). A frame that straddles two reads is handed over as the
// pieces it arrived in -- one view per read -- because that is what node does with
// the same frame: its payload reaches the application in the pieces it arrived in,
// not in one piece per frame (measured with a payload cut in two: node's handler
// gets two chunks, tools/probe-chunking.js). There is no buffer of the session's
// own behind that: the session holds no per-connection frame buffer, so there is
// no memory that could be rewritten under the application.
//
// These tests retain every chunk until the body is complete and then compare the
// retained bytes against the pattern the peer sent, so a payload assembled in
// recycled memory shows up as a mismatch rather than as a flaky timeout.
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const http2 = require('node:http2');
const f = require('../lib/frames');
const h2lean = require('../index');

const MAX_PAYLOAD = 16384; // what we advertise as maxFrameSize

function listen(server) {
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve(server.address().port));
  });
}

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// Byte i of the body. The pattern must NOT be periodic at the frame size: with
// the obvious (i * 7 + 3) & 0xff and 16384-byte frames, frame 2 carries exactly
// the same bytes as frame 1 at the same offsets (16384 * 7 is a multiple of 256),
// so one frame's payload overwriting the other's is invisible and the test passes
// even when the copy it is meant to guard is removed. A multiplicative hash has
// no such period.
const byteAt = (i) => (((i * 2654435761) >>> 24) & 0xff);

function body(n, from) {
  const b = Buffer.allocUnsafe(n);
  for (let i = 0; i < n; i++) {
    b[i] = byteAt(from + i);
  }
  return b;
}

// Checks retained chunks against the pattern and reports the first bad offset.
function firstMismatch(kept) {
  let at = 0;
  for (const chunk of kept) {
    for (let i = 0; i < chunk.length; i++) {
      if (chunk[i] !== byteAt(at + i)) {
        return at + i;
      }
    }
    at += chunk.length;
  }
  return -1;
}

// True when two buffers cover any of the same bytes. Sharing an ArrayBuffer is not
// enough: node hands 16 KB allocations out of one pooled 65 600-byte store, so the
// payload buffer and a session buffer can sit in the same ArrayBuffer without
// overlapping. Only the byte ranges say whether one can be written through the other.
function intersects(a, b) {
  return a.buffer === b.buffer
    && a.byteOffset < b.byteOffset + b.byteLength
    && b.byteOffset < a.byteOffset + a.byteLength;
}

// The buffers a session owns: the ones it writes into again. A DATA payload that
// shared bytes with one of them would change under the application, so the payload
// is checked against all of them.
function sessionBuffers(session) {
  const out = [];
  for (const key of Object.keys(session)) {
    const v = session[key];
    if (Buffer.isBuffer(v)) {
      out.push([key, v]);
    }
  }
  return out;
}

test('a max-size frame split across reads is delivered intact and retained safely', async (t) => {
  const srv = h2lean.createServer();
  let session = null;
  srv.on('session', (s) => {
    session = s;
  });

  const delivered = new Promise((resolve) => {
    srv.on('stream', (stream) => {
      const kept = [];
      let shared = 0;
      stream.on('data', (d) => {
        kept.push(d);
        // A DATA payload must never share bytes with a buffer the session owns:
        // that memory is written into again, so the next frame would overwrite
        // what the application is still holding.
        for (const [, buf] of sessionBuffers(session)) {
          if (intersects(d, buf)) {
            shared++;
          }
        }
      });
      stream.on('end', () => {
        resolve({
          total: kept.reduce((n, c) => n + c.length, 0),
          mismatch: firstMismatch(kept),
          chunks: kept.length,
          shared
        });
        stream.respond({ ':status': 200 }, { endStream: true });
      });
    });
  });
  t.after(() => srv.close());
  const port = await listen(srv);

  // :method POST (3), :path / (4), :scheme http (6) -- all from the static table,
  // plus a literal :authority, because RFC 9113 8.3.1 requires the target and a request
  // without it is refused before it reaches the application (node does the same). No
  // END_STREAM because a body follows.
  const block = Buffer.concat([Buffer.from([0x83, 0x84, 0x86]), Buffer.from(
    [0x00, 10, ...Buffer.from(':authority', 'latin1'), 9,
      ...Buffer.from('localhost', 'latin1')])]);
  const first = frame(f.DATA, 0, 1, body(MAX_PAYLOAD, 0));
  const second = frame(f.DATA, f.F_END_STREAM, 1, body(MAX_PAYLOAD, MAX_PAYLOAD));

  await new Promise((resolve, reject) => {
    const sock = net.connect(port, '127.0.0.1', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0)),
        frame(f.HEADERS, f.F_END_HEADERS, 1, block)]));
      // Split both frames so that each one is assembled from the views it arrived
      // in, and the second assembly happens after the first frame's view was
      // handed to the app: that is what makes a wrongly elided copy observable.
      setTimeout(() => sock.write(first.subarray(0, 5)), 20);
      setTimeout(() => sock.write(first.subarray(5)), 40);
      setTimeout(() => sock.write(second.subarray(0, f.HEADER_LEN + 100)), 60);
      setTimeout(() => {
        sock.write(second.subarray(f.HEADER_LEN + 100));
      }, 80);
      setTimeout(() => {
        sock.destroy(); resolve();
      }, 300);
    });
    sock.setTimeout(3000);
    sock.on('timeout', () => {
      sock.destroy(); reject(new Error('server went silent'));
    });
    sock.on('error', () => {});
  });

  const seen = await delivered;
  assert.strictEqual(seen.total, MAX_PAYLOAD * 2);
  // The first frame's payload arrives in one write and the second frame's in two,
  // so the body reaches the application as three pieces -- one per write that
  // carried payload octets, the way node hands the same body over.
  assert.strictEqual(seen.chunks, 3, 'one piece per read that carried payload');
  assert.strictEqual(seen.mismatch, -1, 'retained chunks were overwritten');
  assert.strictEqual(seen.shared, 0, 'a DATA payload shares bytes with a session buffer');
  // The session keeps no buffer per connection for a frame: the only octets it
  // copies are the nine of a frame header that a read split and the six of a
  // HEADERS prefix. The outbound staging buffer is not part of this -- it grows
  // with the answer, and it is bounded by MAX_OUT_BATCH.
  const owned = sessionBuffers(session)
    .filter(([key]) => key !== '_stage')
    .map(([, buf]) => buf.length);
  assert.ok(owned.every((n) => n < MAX_PAYLOAD),
    `the session holds a buffer of ${owned.join(', ')} octets`);
});

// The request block both servers accept: :method POST, :path / and :scheme http
// from the static table, plus a literal :authority (RFC 9113 8.3.1 wants the
// target, and a request without it is refused before it reaches the application).
const REQ_BLOCK = Buffer.concat([Buffer.from([0x83, 0x84, 0x86]), Buffer.from(
  [0x00, 10, ...Buffer.from(':authority', 'latin1'), 9,
    ...Buffer.from('localhost', 'latin1')])]);

// A raw client that sends one request whose DATA payload is cut in two writes,
// so the server's first read ends inside the payload.
function uploadInPieces(port) {
  return new Promise((resolve) => {
    const sock = net.connect(port, '127.0.0.1', () => {
      const data = frame(f.DATA, f.F_END_STREAM, 1, body(MAX_PAYLOAD, 0));
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0)),
        frame(f.HEADERS, f.F_END_HEADERS, 1, REQ_BLOCK)]));
      sock.write(data.subarray(0, f.HEADER_LEN + 5));
      setTimeout(() => sock.write(data.subarray(f.HEADER_LEN + 5)), 20);
    });
    sock.on('error', () => {});
    setTimeout(() => {
      sock.destroy(); resolve();
    }, 300);
  });
}

// The chunks a server hands its handler for that body.
async function chunksSeen(impl) {
  const srv = impl.createServer();
  const chunks = [];
  srv.on('stream', (stream) => {
    stream.on('data', (d) => chunks.push(d));
    stream.on('end', () => stream.respond({ ':status': 200 }, { endStream: true }));
  });
  srv.on('error', () => {});
  const port = await listen(srv);
  await uploadInPieces(port);
  await new Promise((r) => setTimeout(r, 100));
  srv.close();
  return chunks;
}

test('a payload cut across two reads arrives in pieces, like node\'s', async () => {
  // The shape of the delivery is not h2lean's to choose. Node hands a payload to
  // the application in the pieces it arrived in -- a frame whose payload was cut
  // in two reaches node's handler as two chunks (tools/probe-chunking.js) --
  // so a frame-atomic delivery would be a divergence from node in the one place
  // the application can see it. The number of pieces is not compared: where the
  // read boundary falls is the socket's business, not the protocol's. What is
  // compared is that both stacks split the payload, and that the bytes are the
  // same either way.
  const node = await chunksSeen(http2);
  const ours = await chunksSeen(h2lean);
  assert.ok(node.length > 1, `node delivered the payload in ${node.length} piece(s)`);
  assert.ok(ours.length > 1, `h2lean delivered the payload in ${ours.length} piece(s)`);
  assert.deepStrictEqual(Buffer.concat(ours), body(MAX_PAYLOAD, 0));
});

test('a padded payload that straddles two reads arrives without its padding', async (t) => {
  // A padded DATA frame carries the pad length as its first octet and the padding
  // as its last ones, so the views of the chunks it arrived in are NOT the payload:
  // the pad length has to come off the first view and the padding off the last one.
  // That is done on the views themselves -- the frame is not assembled -- so the
  // payload still reaches the application in pieces, like an unpadded one.
  //
  // Two shapes, because there are two assembly sites: one where the read boundary
  // falls inside the payload, and one where it falls inside the frame HEADER. The
  // second one reaches the site that decides what a straddling frame is while its
  // header is still incomplete.
  const PAD = 5;
  const DATA_LEN = 100;
  const data = body(DATA_LEN, 0);
  const padded = frame(f.DATA, f.F_PADDED | f.F_END_STREAM, 1,
    Buffer.concat([Buffer.from([PAD]), data, Buffer.alloc(PAD)]));

  const shapes = [
    { name: 'the payload straddles the reads', cuts: [f.HEADER_LEN + 5] },
    { name: 'the frame header straddles the reads', cuts: [5, f.HEADER_LEN + 5] }
  ];

  for (const shape of shapes) {
    const srv = h2lean.createServer();
    const delivered = new Promise((resolve) => {
      srv.on('stream', (stream) => {
        const kept = [];
        stream.on('data', (d) => kept.push(d));
        stream.on('end', () => {
          resolve(kept);
          stream.respond({ ':status': 200 }, { endStream: true });
        });
      });
    });
    t.after(() => srv.close());
    const port = await listen(srv);

    await new Promise((resolve) => {
      const sock = net.connect(port, '127.0.0.1', () => {
        sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0)),
          frame(f.HEADERS, f.F_END_HEADERS, 1, REQ_BLOCK)]));
        // Each write lands in its own read, so the boundary falls where the shape
        // puts it. The writes are staggered by a gap wide enough that the socket
        // cannot merge two of them into one segment: writes issued in the same tick
        // are coalesced, and then the header never straddles at all.
        let at = 0;
        shape.cuts.forEach((cut, k) => {
          setTimeout(() => {
            sock.write(padded.subarray(at, cut));
            at = cut;
          }, 30 * (k + 1));
        });
        setTimeout(() => {
          sock.write(padded.subarray(at));
        }, 30 * (shape.cuts.length + 1));
        setTimeout(() => {
          sock.destroy(); resolve();
        }, 300 + 30 * shape.cuts.length);
      });
      sock.on('error', () => {});
    });

    const kept = await delivered;
    assert.deepStrictEqual(Buffer.concat(kept), data,
      `the padding is not part of the body (${shape.name})`);
    // The frame was not assembled: the payload arrives in the pieces it was read
    // in, so the padding was trimmed off the views rather than out of a copy.
    assert.ok(kept.length > 1,
      `h2lean delivered the padded payload in ${kept.length} piece(s) (${shape.name})`);
  }
});

test('retained chunks of a 70 KB upload stay intact (compat API, h2c)', async (t) => {
  const SIZE = 70000;
  const payload = body(SIZE, 0);
  let result = null;
  const srv = h2lean.createServer({}, (req, res) => {
    const kept = [];
    req.on('data', (d) => kept.push(d));
    req.on('end', () => {
      result = { total: kept.reduce((n, c) => n + c.length, 0), mismatch: firstMismatch(kept) };
      res.writeHead(200, { 'content-type': 'text/plain' });
      res.end('ok');
    });
  });
  t.after(() => srv.close());
  const port = await listen(srv);

  const client = http2.connect(`http://127.0.0.1:${port}`);
  client.on('error', () => {});
  const status = await new Promise((resolve, reject) => {
    const req = client.request({ ':method': 'POST', ':path': '/upload' });
    req.on('response', (h) => resolve(h[':status']));
    req.on('error', reject);
    req.end(payload);
  });
  client.destroy();

  assert.strictEqual(status, 200);
  assert.strictEqual(result.total, SIZE);
  assert.strictEqual(result.mismatch, -1, 'retained chunks were overwritten');
});
