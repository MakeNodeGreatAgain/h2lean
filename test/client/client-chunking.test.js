// How the client hands a DATA payload to the application when the frame straddles
// two socket reads.
//
// Node delivers a payload in the pieces it arrived in, not in one piece per frame:
// a frame whose payload was cut in two reaches node's client as several chunks
// (tools/probe-chunking.js). h2lean's client used to assemble such a payload
// into a buffer of its own -- one memcpy of the whole payload -- and hand it over
// as a single chunk, which is both the copy and a divergence from node.
//
// The number of pieces is not compared: where the read boundary falls is the
// socket's business, not the protocol's. What is compared is that both clients
// split the payload and that the bytes are the same either way.
//
// A PADDED frame is the exception: its first payload octet is the pad length and
// its last ones are the padding, so the views of the chunks are not the payload.
// The padding is trimmed off the views (the first and the last one), not out of
// an assembled copy, so a padded payload also arrives in pieces.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const http2 = require('node:http2');
const h2lean = require('../../index');
const f = require('../../lib/frames');

const PAYLOAD = 16384;

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// A raw server that answers with one DATA frame, cut so that the client's read
// ends inside the frame.
//
// The server sends its SETTINGS as soon as the connection is up and waits for the
// request's HEADERS frame before it answers. Both halves matter: h2lean's client
// holds its first request back until the peer's SETTINGS have been applied
// (lib/client-session.js, _enqueue), so a server that answers SETTINGS only after
// the request deadlocks against it; and answering on the client's preface instead
// of on its request would race the request against the response.
//
// `headerCut` splits the response's HEADERS frame itself, so that the block the
// client decodes straddles its reads: the cut may fall inside the frame header or
// inside the block. The DATA frame follows it.
function rawServer({ padded = false, cuts = [f.HEADER_LEN + 5], size = PAYLOAD,
  headerCut = null } = {}) {
  return new Promise((resolve) => {
    const sockets = [];
    const srv = net.createServer((sock) => {
      sockets.push(sock);
      sock.on('error', () => {});
      let buf = Buffer.alloc(0);
      let answered = false;
      sock.write(frame(f.SETTINGS, 0, 0, Buffer.alloc(0)));
      sock.on('data', (d) => {
        if (answered) {
          return;
        }
        buf = Buffer.concat([buf, d]);
        // The preface is 24 octets; after it come frames. Answer once, when the
        // request's HEADERS frame is complete.
        let off = f.PREFACE_LEN;
        while (off + f.HEADER_LEN <= buf.length) {
          const len = (buf[off] << 16) | (buf[off + 1] << 8) | buf[off + 2];
          const type = buf[off + 3];
          const flags = buf[off + 4];
          if (off + f.HEADER_LEN + len > buf.length) {
            break;
          }
          off += f.HEADER_LEN + len;
          if (type !== f.HEADERS || (flags & f.F_END_HEADERS) === 0) {
            continue;
          }
          answered = true;
          const body = Buffer.alloc(size);
          for (let i = 0; i < size; i++) {
            body[i] = i & 0xff;
          }
          const payload = padded
            ? Buffer.concat([Buffer.from([5]), body, Buffer.alloc(5)])
            : body;
          const data = frame(f.DATA, (padded ? f.F_PADDED : 0) | f.F_END_STREAM, 1, payload);
          const headers = frame(f.HEADERS, f.F_END_HEADERS, 1, Buffer.from([0x88])); // :status 200
          // The header block first, then the body. When the block is split, the
          // two halves take the first two slots and the body starts after them.
          const base = headerCut === null ? 0 : 60;
          if (headerCut === null) {
            sock.write(headers);
          } else {
            setTimeout(() => sock.write(headers.subarray(0, headerCut)), 30);
            setTimeout(() => sock.write(headers.subarray(headerCut)), 60);
          }
          // Each write lands in its own read, so the boundary falls where the cuts
          // put it. The writes are staggered by a gap wide enough that the socket
          // cannot merge two of them into one segment: writes issued in the same tick
          // are coalesced, and then the header never straddles at all.
          let at = 0;
          cuts.forEach((cut, k) => {
            setTimeout(() => {
              sock.write(data.subarray(at, cut));
              at = cut;
            }, 30 * (k + 1) + base);
          });
          setTimeout(() => sock.write(data.subarray(at)), 30 * (cuts.length + 1) + base);
          return;
        }
      });
    });
    srv.listen(0, '127.0.0.1', () => resolve({ srv, sockets }));
  });
}

// The chunks one client implementation delivers for that answer.
//
// The deadline turns a silent peer into a failure instead of a hang: a client
// that never sees the answer would otherwise hold the file open until the
// runner's timeout, and the file would stop reporting altogether.
function grab(impl, port) {
  return new Promise((resolve, reject) => {
    const sess = impl.connect(`http://127.0.0.1:${port}`);
    const deadline = setTimeout(() => {
      sess.destroy();
      reject(new Error('no answer'));
    }, 3000);
    const done = (fn) => (arg) => {
      clearTimeout(deadline); fn(arg);
    };
    sess.on('error', done(reject));
    const req = sess.request({ ':path': '/' });
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('error', done(reject));
    req.on('end', done(() => {
      sess.destroy(); resolve(chunks);
    }));
  });
}

// The headers one client implementation sees for that answer, and whether the
// block was decoded out of the views it arrived in.
function grabHeaders(impl, port) {
  return new Promise((resolve, reject) => {
    const sess = impl.connect(`http://127.0.0.1:${port}`);
    // The piece path is the one that decodes a block out of views; the count is
    // what tells the two paths apart from the outside. Node's client has no
    // such path, so the counter stays at zero there.
    let pieces = 0;
    if (typeof sess._processBlockPieces === 'function') {
      const orig = sess._processBlockPieces;
      sess._processBlockPieces = function (...args) {
        pieces++;
        return orig.apply(this, args);
      };
    }
    const deadline = setTimeout(() => {
      sess.destroy();
      reject(new Error('no response'));
    }, 3000);
    const done = (fn) => (arg) => {
      clearTimeout(deadline); fn(arg);
    };
    sess.on('error', done(reject));
    const req = sess.request({ ':path': '/' });
    req.on('response', done((h) => {
      sess.destroy(); resolve({ headers: h, pieces });
    }));
    req.on('error', done(reject));
  });
}

test('a payload cut across two reads arrives in pieces, like node\'s', async () => {
  const { srv, sockets } = await rawServer();
  try {
    const node = await grab(http2, srv.address().port);
    const ours = await grab(h2lean, srv.address().port);
    assert.ok(node.length > 1, `node delivered the payload in ${node.length} piece(s)`);
    assert.ok(ours.length > 1, `h2lean delivered the payload in ${ours.length} piece(s)`);
    assert.deepStrictEqual(Buffer.concat(ours), Buffer.concat(node));
    assert.strictEqual(Buffer.concat(ours).length, PAYLOAD);
  } finally {
    for (const s of sockets) {
      s.destroy();
    }
    srv.close();
  }
});

test('a response header block that straddles two reads is decoded', async () => {
  // The client decodes the response block out of the views it arrived in
  // (lib/hpack.js decodePieces), so the block may be cut anywhere: inside the
  // frame header, or at the first octet of the block -- where the payload
  // arrives as an empty view and the octets that follow, and the decoder's
  // cursor may not stop on the empty one.
  const shapes = [
    { name: 'the frame header straddles the reads', cut: 5 },
    { name: 'the block straddles the reads', cut: f.HEADER_LEN }
  ];
  for (const shape of shapes) {
    const { srv, sockets } = await rawServer({ headerCut: shape.cut });
    try {
      const ours = await grabHeaders(h2lean, srv.address().port);
      assert.strictEqual(ours.headers[':status'], 200,
        `h2lean saw ${ours.headers[':status']} (${shape.name})`);
      // The block was decoded out of the views it arrived in, not out of a copy.
      assert.ok(ours.pieces > 0,
        `the block was not decoded out of the pieces (${shape.name})`);
      const node = await grabHeaders(http2, srv.address().port);
      assert.strictEqual(node.headers[':status'], 200,
        `node saw ${node.headers[':status']} (${shape.name})`);
    } finally {
      for (const s of sockets) {
        s.destroy();
      }
      srv.close();
    }
  }
});

test('a padded payload that straddles two reads arrives without its padding', async () => {
  // Two shapes, because there are two assembly sites: one where the read boundary
  // falls inside the payload, and one where it falls inside the frame HEADER. The
  // second one reaches the site that decides what a straddling frame is while its
  // header is still incomplete.
  const shapes = [
    { name: 'the payload straddles the reads', cuts: [f.HEADER_LEN + 5] },
    { name: 'the frame header straddles the reads', cuts: [5, f.HEADER_LEN + 5] }
  ];
    // A padded frame carries six octets more than its body (the pad length and the
    // padding), and a frame may not exceed the peer's max frame size -- 16384 by
    // default -- so the padded body is smaller than the unpadded one.
  const SIZE = 100;
  for (const shape of shapes) {
    const { srv, sockets } = await rawServer({ padded: true, cuts: shape.cuts, size: SIZE });
    try {
      const ours = await grab(h2lean, srv.address().port);
      assert.strictEqual(Buffer.concat(ours).length, SIZE,
        `the padding is not part of the body (${shape.name})`);
      // The frame was not assembled: the payload arrives in the pieces it was read
      // in, with the padding trimmed off the first and the last of them.
      assert.ok(ours.length > 1,
        `h2lean delivered the padded payload in ${ours.length} piece(s) (${shape.name})`);
      const expect = Buffer.alloc(SIZE);
      for (let i = 0; i < SIZE; i++) {
        expect[i] = i & 0xff;
      }
      assert.deepStrictEqual(Buffer.concat(ours), expect);
    } finally {
      for (const s of sockets) {
        s.destroy();
      }
      srv.close();
    }
  }
});
