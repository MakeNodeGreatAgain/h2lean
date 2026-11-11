// Outbound batching has to have a ceiling. Frames are queued per session and handed
// to the socket as one writev batch, but a batch cannot be issued while the previous
// one is still outstanding -- so if producing frames ignores that, every stream in the
// read batch keeps appending and the queue grows without bound.
//
// Measured before the cap was introduced, with h2load at 64 connections x 32 streams
// and 256 KB responses: _out reached 16 777 216 bytes on every session (1 GB of RSS
// for 64 connections, and a peer chooses the body size), one socket.write() carried
// 8 132 943 bytes, and the compaction in _onWritten moved 251 686 bytes per request.
//
// Reproducing that needs two conditions at once. A node client asking for eight bodies
// on one connection passes with no cap at all: the client's 65 535-byte connection
// window is itself a bound, and it drains the staging buffer faster than a loopback
// fills it.
// What removes the bound is a peer that (a) authorises a lot of credit, as h2load does
// with --download-window-bits=30, and (b) is slow enough that a write is still
// outstanding while that credit arrives. So the peer here is raw, advertises a 4 MB
// window and 16 MB frames, and keeps sending WINDOW_UPDATE on stream 0, while the
// server's write callback is deliberately held back. With the cap the queue stops at
// ~131 KB; without it the same run stages 2 MB.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const http2 = require('node:http2');
const f = require('../lib/frames');
const h2lean = require('../index');
const { MAX_OUT_BATCH } = require('../lib/session');
const { listen, request, closeServer } = require('./diff-harness');

const GET = { ':method': 'GET', ':path': '/big' };
const SIZE = 256 * 1024;
const STREAMS = 4;
const BODY = Buffer.alloc(SIZE, 0x6f);
const CREDIT = 1024 * 1024; // per WINDOW_UPDATE, as a peer may grant

// A whole request header block for the raw peer: :method GET, :path /, :scheme http from
// the static table, then :authority as a literal with an indexed name (static 1). The
// authority is required by RFC 9113 8.3.1 -- a block without it is refused with
// RST_STREAM before the server produces any response bytes, so the batching this test
// measures would never happen.
const REQ = Buffer.from([0x82, 0x84, 0x86, 0x01, 9,
  ...Buffer.from('localhost', 'latin1')]);

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

function setting(id, value) {
  const b = Buffer.alloc(6);
  b.writeUInt16BE(id, 0);
  b.writeUInt32BE(value, 2);
  return b;
}

function windowUpdate(sid, increment) {
  const b = Buffer.alloc(4);
  b.writeUInt32BE(increment, 0);
  return frame(f.WINDOW_UPDATE, 0, sid, b);
}

test('a peer with a large window cannot grow the outbound queue past the cap', async (t) => {
  const srv = h2lean.createServer();
  let session = null;
  srv.on('session', (s) => {
    session = s;
  });
  srv.on('stream', (stream) => {
    stream.respond({ ':status': 200, 'content-length': String(SIZE) });
    // end() with the whole body: everything the staging buffer is not allowed to absorb
    stream.end(BODY);
  });
  t.after(() => srv.close());
  const port = await listen(srv);

  // Peak staging is read where it is created: _frame() is the only thing that queues
  // bytes, so wrapping it costs nothing and cannot miss a spike between two samples
  // the way polling the queue length would.
  let peak = 0;
  const realFrame = h2lean.Http2Session.prototype._frame;
  h2lean.Http2Session.prototype._frame = function (type, flags, sid, payload, owned) {
    realFrame.call(this, type, flags, sid, payload, owned);
    if (this._outLen > peak) {
      peak = this._outLen;
    }
  };
  t.after(() => {
    h2lean.Http2Session.prototype._frame = realFrame;
  });

  // DATA bytes per stream id, from a parser that keeps the tail of a partial frame:
  // the response arrives as far more bytes than one read can carry.
  const got = new Map();
  const bad = { at: -1 };
  let pending = Buffer.alloc(0);
  function consume() {
    for (;;) {
      if (pending.length < f.HEADER_LEN) {
        return;
      }
      const len = (pending[0] << 16) | (pending[1] << 8) | pending[2];
      const type = pending[3];
      const sid = pending[5] << 24 | pending[6] << 16 | pending[7] << 8 | pending[8];
      if (pending.length < f.HEADER_LEN + len) {
        return;
      }
      if (type === f.DATA) {
        const payload = pending.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
        const seen = got.get(sid) || 0;
        for (let i = 0; i < payload.length; i++) {
          if (payload[i] !== 0x6f && bad.at < 0) {
            bad.at = seen + i;
          }
        }
        got.set(sid, seen + payload.length);
      }
      pending = pending.subarray(f.HEADER_LEN + len);
    }
  }

  const sock = net.connect(port, '127.0.0.1');
  t.after(() => sock.destroy());
  sock.on('data', (d) => {
    pending = Buffer.concat([pending, d]); consume();
  });
  sock.on('error', () => {});

  await new Promise((resolve) => {
    sock.on('connect', resolve);
  });
  // :method GET (0x82), :path / (0x84), :scheme http (0x86) from the static table, plus
  // :authority as a literal with an indexed name (static 1). RFC 9113 8.3.1 requires the
  // target, and a request without it is refused before any bytes are produced -- which
  // would leave this test with nothing to batch.
  const opener = frame(f.SETTINGS, 0, 0,
    Buffer.concat([setting(4, 4 * 1024 * 1024), setting(5, 16777215)]));
  const heads = [];
  for (let i = 0; i < STREAMS; i++) {
    heads.push(frame(f.HEADERS, f.F_END_HEADERS, 1 + i * 2, REQ));
  }
  sock.write(Buffer.concat([f.PREFACE, opener, ...heads]));
  await new Promise((r) => {
    session ? r() : srv.once('session', r);
  });

  // (b) of the two conditions: the bytes are on their way, but the queue is not ours
  // again for 60 ms, so everything produced meanwhile has somewhere to go. Only the
  // write that carries a callback is held: a batch is written corked, chunk by chunk.
  const realWrite = session.socket.write.bind(session.socket);
  session.socket.write = (buf, cb) => (cb === undefined
    ? realWrite(buf)
    : realWrite(buf, () => setTimeout(cb, 60)));
  t.after(() => {
    session.socket.write = realWrite;
  });

  // (a): credit for the connection keeps arriving while that write is outstanding.
  const timer = setInterval(() => sock.write(windowUpdate(0, CREDIT)), 10);
  t.after(() => clearInterval(timer));

  const deadline = Date.now() + 8000;
  const complete = () => {
    for (let i = 0; i < STREAMS; i++) {
      if ((got.get(1 + i * 2) || 0) < SIZE) {
        return false;
      }
    }
    return true;
  };
  while (!complete() && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 20));
  }

  // Nothing may be dropped or reordered by the cap -- it is allowed to delay bytes,
  // never to lose them -- and the staging has to stay inside its ceiling.
  for (let i = 0; i < STREAMS; i++) {
    assert.strictEqual(got.get(1 + i * 2) || 0, SIZE,
      `stream ${1 + i * 2} got its whole body`);
  }
  assert.strictEqual(bad.at, -1, `foreign byte at offset ${bad.at}`);
  // A header block is not bound by the ceiling -- the cap applies to DATA, which is
  // what a peer can drive -- so the slack is one HEADERS frame's worth of them.
  assert.ok(peak <= MAX_OUT_BATCH + 64 * 1024,
    `the outbound queue grew to ${peak} bytes with ${STREAMS} bodies in flight`);
});

test('a response larger than the batch ceiling still reaches the peer complete', async (t) => {
  // The path where the body is deferred for lack of staging space and released by
  // the write callback instead of by a WINDOW_UPDATE: if nothing pumps it there,
  // this is where it hangs.
  const SIZE2 = 3 * 1024 * 1024;
  const body = Buffer.alloc(SIZE2, 0x7a);
  const srv = h2lean.createServer();
  srv.on('stream', (stream) => {
    stream.respond({ ':status': 200, 'content-length': String(SIZE2) });
    stream.end(body);
  });
  t.after(() => srv.close());
  const port = await listen(srv);
  const client = http2.connect(`http://127.0.0.1:${port}`);
  t.after(() => client.destroy());
  client.on('error', () => {});

  const res = await new Promise((resolve, reject) => {
    const req = client.request({ ':method': 'GET', ':path': '/big' });
    const chunks = [];
    req.on('data', (d) => chunks.push(d));
    req.on('response', () => {});
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
  assert.strictEqual(res.length, SIZE2);
  assert.ok(/^z+$/.test(res.toString('latin1')), 'and nothing else did');
});

test('a peer that stops reading still gets intact bytes when it resumes', async (t) => {
  // The staging buffer is handed to the queue and must never be written into again:
  // libuv reads the bytes we passed at its own moment, which can be long after the
  // frame that follows us was produced. Nothing here can observe that except a peer
  // that lets the socket fill up and then reads everything back.
  const srv = h2lean.createServer();
  srv.on('stream', (stream) => {
    // x-sid makes every header block unique to its stream. Without it the corruption
    // this test looks for is invisible: rewriting a queued block with the next one's
    // bytes still decodes to exactly the same fields.
    stream.respond({ ':status': 200, 'content-length': String(SIZE),
      'x-sid': String(stream.id) });
    stream.end(BODY);
  });
  t.after(() => srv.close());
  const port = await listen(srv);

  const N = 12;
  const got = new Map();
  const bad = { at: -1 };
  // Header blocks travel through the staging buffer, so they are the bytes a wrongly
  // reused buffer would corrupt first. Decoding each one is how that becomes visible:
  // the body counters alone stay correct while a HEADERS frame on the wire does not.
  const decoder = new h2lean.HPack.Decoder(4096);
  const decoded = { seen: 0, wrong: '' };
  let pending = Buffer.alloc(0);
  function consume() {
    for (;;) {
      if (pending.length < f.HEADER_LEN) {
        return;
      }
      const len = (pending[0] << 16) | (pending[1] << 8) | pending[2];
      const type = pending[3];
      const sid = pending[5] << 24 | pending[6] << 16 | pending[7] << 8 | pending[8];
      if (pending.length < f.HEADER_LEN + len) {
        return;
      }
      if (type === f.DATA) {
        const payload = pending.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
        const seen = got.get(sid) || 0;
        for (let i = 0; i < payload.length; i++) {
          if (payload[i] !== 0x6f && bad.at < 0) {
            bad.at = seen + i;
          }
        }
        got.set(sid, seen + payload.length);
      } else if (type === f.HEADERS) {
        decoded.seen++;
        const block = pending.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
        const fields = [];
        try {
          decoder.decode(block, 0, block.length, fields);
        } catch (err) {
          if (!decoded.wrong) {
            decoded.wrong = `stream ${sid}: ${err.message}`;
          }
          continue;
        }
        const h = Object.fromEntries(
          fields.filter((_, i) => i % 2 === 0).map((k, i) => [k, fields[2 * i + 1]]));
        if (!decoded.wrong && (h[':status'] !== '200'
            || h['content-length'] !== String(SIZE)
            || h['x-sid'] !== String(sid))) {
          decoded.wrong = `stream ${sid}: got ${JSON.stringify(h)}`;
        }
      }
      pending = pending.subarray(f.HEADER_LEN + len);
    }
  }

  const sock = net.connect(port, '127.0.0.1');
  t.after(() => sock.destroy());
  sock.on('error', () => {});
  sock.on('data', (d) => {
    pending = Buffer.concat([pending, d]); consume();
  });
  await new Promise((resolve) => {
    sock.on('connect', resolve);
  });

  const heads = [];
  for (let i = 0; i < N; i++) {
    heads.push(frame(f.HEADERS, f.F_END_HEADERS, 1 + i * 2, REQ));
  }
  // A peer is allowed to authorise a lot of credit and then be slow to read it -- that
  // is the combination that leaves bytes sitting in the socket's queue, which is what
  // this test is after. The credit is granted up front (4 MB per stream) and topped up
  // at the connection level while nobody is reading.
  const opener = frame(f.SETTINGS, 0, 0,
    Buffer.concat([setting(4, 4 * 1024 * 1024), setting(5, 16777215)]));
  sock.write(Buffer.concat([f.PREFACE, opener, ...heads]));
  const timer = setInterval(() => sock.write(windowUpdate(0, CREDIT)), 50);
  t.after(() => clearInterval(timer));

  // Read nothing for a while: the server's socket fills, its writes pile up in node's
  // own queue, and every frame produced after that is staged behind bytes the peer has
  // not received yet.
  sock.pause();
  await new Promise((r) => setTimeout(r, 400));
  sock.resume();

  const deadline = Date.now() + 15000;
  const complete = () => {
    for (let i = 0; i < N; i++) {
      if ((got.get(1 + i * 2) || 0) < SIZE) {
        return false;
      }
    }
    return true;
  };
  while (!complete() && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 20));
  }

  for (let i = 0; i < N; i++) {
    assert.strictEqual(got.get(1 + i * 2) || 0, SIZE, `stream ${1 + i * 2} body incomplete`);
  }
  assert.strictEqual(bad.at, -1, `foreign byte at offset ${bad.at}`);
  assert.strictEqual(decoded.seen, N, 'one HEADERS frame per stream');
  assert.strictEqual(decoded.wrong, '', 'a HEADERS block arrived rewritten');
});

// True when two buffers cover any of the same bytes. Sharing an ArrayBuffer is not
// enough -- node hands small allocations out of one pooled 65 600-byte store -- so the
// byte ranges are compared.
function intersects(a, b) {
  return a.buffer === b.buffer
    && a.byteOffset < b.byteOffset + b.byteLength
    && b.byteOffset < a.byteOffset + a.byteLength;
}

test('two chunks in the outbound queue never cover the same bytes', async (t) => {
  // Everything handed to socket.write() goes to libuv as a view: the bytes are read at
  // the kernel's convenience, not ours. So a chunk that is already queued is promised,
  // and two queued chunks must not overlap. This checks the invariant rather than its
  // consequence, because the consequence is hard to catch on purpose: a loopback socket
  // normally takes the bytes before the next frame is produced, and then a broken
  // staging buffer ships a perfectly valid response.
  const srv = h2lean.createServer();
  srv.on('stream', (stream) => {
    stream.respond({ ':status': 200, 'content-length': String(SIZE) });
    stream.end(BODY);
  });
  t.after(() => srv.close());
  const port = await listen(srv);
  const client = http2.connect(`http://127.0.0.1:${port}`);
  t.after(() => client.destroy());
  client.on('error', () => {});

  const overlaps = [];
  let checks = 0;
  const realFrame = h2lean.Http2Session.prototype._frame;
  h2lean.Http2Session.prototype._frame = function (type, flags, sid, payload, owned) {
    const r = realFrame.call(this, type, flags, sid, payload, owned);
    const q = this._outQ;
    checks++;
    for (let i = 0; i < q.length; i++) {
      for (let j = i + 1; j < q.length; j++) {
        if (intersects(q[i], q[j])) {
          if (overlaps.length < 3) {
            overlaps.push(`${q[i].byteOffset}..${q[i].byteOffset + q[i].length} with `
              + `${q[j].byteOffset}..${q[j].byteOffset + q[j].length}`);
          }
        }
      }
    }
    return r;
  };
  t.after(() => {
    h2lean.Http2Session.prototype._frame = realFrame;
  });

  const all = [];
  for (let i = 0; i < 6; i++) {
    all.push(request(client, GET));
  }
  const results = await Promise.all(all);
  for (const r of results) {
    assert.strictEqual(r.body.length, SIZE);
  }
  assert.ok(checks > 20, `the queue was inspected only ${checks} times`);
  assert.deepStrictEqual(overlaps, [], 'queued chunks cover the same bytes');
});

// ---- hold ceiling: RST(REFUSED_STREAM) on a stalled flood ------------------------------------

// A peer that sends many HEADERS and never returns credit causes every response to park.
// The hold ceiling is the guard that bounds what one connection can make the server retain
// in that state. This test exercises the exact path tools/probe-stream-concurrency.js proved
// at 200k streams, at a scale a test can run in under 10 seconds: each response body is
// large enough that only a handful of parked streams reach the ceiling.
test('a stalled flood beyond the hold ceiling is refused with RST(REFUSED_STREAM) and the '
  + 'connection survives', async (t) => {
  // Each stream parks 1 MiB after the first one drains the initial 65535-byte connection window.
  // The ceiling fires at k where heldCost(k) = (65535 - k*1MiB) + k*STREAM_HOLD_CHARGE >= 8MiB.
  // With k=9: 983041 + 8*1048576 + 9*1024 = 9380865 > 8388608, so the 10th HEADERS is refused.
  const PARK = 1024 * 1024;
  const parkBody = Buffer.alloc(PARK, 0x62);
  const srv = h2lean.createServer();
  let accepted = 0;
  srv.on('stream', (stream) => {
    accepted++;
    stream.on('error', () => {});
    stream.respond({ ':status': 200, 'content-length': String(PARK) });
    stream.end(parkBody);
  });
  srv.on('error', () => {});
  t.after(() => closeServer(srv));
  const port = await listen(srv);

  const sock = net.connect(port, '127.0.0.1');
  t.after(() => sock.destroy());
  sock.on('error', () => {});

  const frames = [];
  let pending = Buffer.alloc(0);
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
      const sid = pending[5] << 24 | pending[6] << 16 | pending[7] << 8 | pending[8];
      const payload = pending.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
      pending = pending.subarray(f.HEADER_LEN + len);
      if (type === f.RST_STREAM) {
        frames.push({ type: 'RST', sid, code: payload.readUInt32BE(0) });
      } else if (type === f.GOAWAY) {
        frames.push({ type: 'GOAWAY', code: payload.readUInt32BE(4) });
      } else if (type === f.HEADERS) {
        frames.push({ type: 'HEADERS', sid });
      }
    }
  });

  await new Promise((r) => {
    sock.on('connect', r);
  });
  // Open the connection: preface + empty SETTINGS (the handshake).
  sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
  await new Promise((r) => setTimeout(r, 50));

  // Flood: 20 HEADERS with END_STREAM, no WINDOW_UPDATE ever.
  const heads = [];
  for (let i = 0; i < 20; i++) {
    heads.push(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1 + i * 2, REQ));
  }
  sock.write(Buffer.concat(heads));
  await new Promise((r) => setTimeout(r, 200));

  // At least one RST(REFUSED_STREAM=7) must appear.
  const refusals = frames.filter((fr) => fr.type === 'RST' && fr.code === 7);
  assert.ok(refusals.length > 0,
    `no RST(REFUSED_STREAM) among ${frames.filter((fr) => fr.type === 'RST').length} RSTs`);

  // No GOAWAY: the connection must survive.
  const goaways = frames.filter((fr) => fr.type === 'GOAWAY');
  assert.strictEqual(goaways.length, 0, 'the flood brought a GOAWAY');

  // Self-clearing: grant credit at the connection level AND per-stream so that _releaseAll
  // can actually drain parked bodies. After all streams drain, heldCost drops below the
  // ceiling and a new HEADERS is accepted.
  const connWin = Buffer.alloc(4);
  connWin.writeUInt32BE(20 * 1024 * 1024, 0);
  sock.write(frame(f.WINDOW_UPDATE, 0, 0, connWin));
  // Per-stream credit: each stream has _sendWin = 65535 but parked body = 1 MiB, so the
  // connection-level update alone is not enough to release a stream. Grant each stream its
  // full body worth of window.
  for (let i = 0; i < 20; i++) {
    const sid = 1 + i * 2;
    const sw = Buffer.alloc(4);
    sw.writeUInt32BE(PARK, 0);
    sock.write(frame(f.WINDOW_UPDATE, 0, sid, sw));
  }
  await new Promise((r) => setTimeout(r, 500));

  const before = accepted;
  sock.write(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 41, REQ));
  await new Promise((r) => setTimeout(r, 100));
  assert.ok(accepted > before, 'a HEADERS after the drain was not accepted');
});
