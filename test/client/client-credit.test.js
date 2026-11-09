// The client's receive-credit pass, driven through a stub socket so the three rules the
// credit path has to hold at once can be pinned without a scheduler in the loop:
//
//   1. credit owed but not yet due is CARRIED to a later pass, not forgotten (a lost
//      carry is a stalled download: the peer waits for a WINDOW_UPDATE that never comes);
//   2. the updates due from one pass leave as ONE write, in wire order, correctly framed
//      (RFC 9113 6.9: the increment is what the window gained, and a malformed control
//      frame is a connection error the peer reports for the whole session);
//   3. a WINDOW_UPDATE never overtakes a frame that is still QUEUED, and a frame that is
//      merely in flight is already ahead of it on the wire.
//
// Plus the ownership rule of AGENTS.md 5: a chunk handed to the socket belongs to the
// socket, and here every handed-out chunk is snapshotted and re-checked at the end, so
// the buffer the credit pass coalesces into cannot be rewritten under an outstanding
// write. A real socket would make that invisible (loopback takes the bytes immediately);
// the stub makes it observable and deterministic -- and the two passes are deliberately
// given DIFFERENT increments, so an implementation that handed over a view of the shared
// buffer would show the first write's octets changing under it.
//
// Not a differential: node's client releases credit inside nghttp2's C, so there is no
// JS surface to compare against here. The increments are pinned against the rule the
// library states (half of the 65 535 a peer runs on until it has read our SETTINGS),
// and the cadence a live server sees is covered by the round-trip file.
//
// Every DATA payload here is <= 16 384 because that is the SETTINGS_MAX_FRAME_SIZE the
// client advertises, and _allowFrame() refuses anything longer -- as well it should: a
// probe that got that far measured GOAWAY(FRAME_SIZE_ERROR) instead of credit.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const { EventEmitter } = require('node:events');
const { ClientHttp2Session } = require('../../lib/client-session');
const f = require('../../lib/frames');

const MAX_FRAME = 16384; // what we advertise, so what the peer may send

function frame(type, flags, sid, payload) {
  const len = payload ? payload.length : 0;
  const buf = Buffer.allocUnsafe(f.HEADER_LEN + len);
  f.writeHeader(buf, 0, len, type, flags, sid);
  if (len) {
    payload.copy(buf, f.HEADER_LEN);
  }
  return buf;
}

// One socket read carrying DATA frames of the given sizes on `sid`. Each size stays under
// MAX_FRAME; the point of batching them into one chunk is that one read is one credit
// period, whatever number of frames it happened to contain.
function data(sid, sizes, fill) {
  const out = [];
  for (const n of sizes) {
    assert.ok(n <= MAX_FRAME, `a ${n}-octet DATA frame is over the advertised frame size`);
    out.push(frame(f.DATA, 0, sid, Buffer.alloc(n, fill)));
  }
  return Buffer.concat(out);
}

// A socket that records what it was given and holds the write callback until the test
// asks for it: not running the callback is what keeps `_writing` true, i.e. exactly the
// state a client is in while a batch is still on its way to the peer.
function stubSocket() {
  const sock = new EventEmitter();
  sock.writable = true;
  sock.connecting = false;
  sock.destroyed = false;
  const written = [];
  sock.write = (buf, cb) => {
    written.push({ buf, snap: Buffer.from(buf), cb: cb || null, done: false });
    return true;
  };
  sock.cork = () => {};
  sock.uncork = () => {};
  sock.destroy = () => {
    sock.destroyed = true;
  };
  sock.ref = () => sock;
  sock.unref = () => sock;
  return { sock, written };
}

// Let every outstanding write complete, which drains what the queue held behind it and
// can hand over a new batch; iterate until the socket has nothing pending.
function drain(written) {
  for (let guard = 0; guard < 30; guard++) {
    const pending = written.filter((w) => w.cb && !w.done);
    if (pending.length === 0) {
      return;
    }
    for (const w of pending) {
      w.done = true; w.cb();
    }
  }
  throw new Error('the outbound queue never drained');
}

// Every frame the socket was ever given, in wire order.
function transcript(written) {
  const out = [];
  const all = Buffer.concat(written.map((w) => w.buf));
  let i = 0;
  if (all.subarray(0, 24).toString('latin1').startsWith('PRI * HTTP/2.0')) {
    i = 24;
  }
  while (i + f.HEADER_LEN <= all.length) {
    f.readHeader(all, i);
    const at = i + f.HEADER_LEN;
    const entry = { type: f.FH_TYPE, flags: f.FH_FLAGS, sid: f.FH_SID, len: f.FH_LEN };
    if (entry.type === f.WINDOW_UPDATE) {
      entry.inc = all.readUInt32BE(at);
    }
    out.push(entry);
    i = at + f.FH_LEN;
  }
  return out;
}

const updates = (list) => list.filter((e) => e.type === f.WINDOW_UPDATE)
  .map((e) => `${e.sid}/${e.inc}`);

// The whole transcript as `NAME/sid`, for the assertions that are about ORDER rather
// than about credit.
const NAMES = { [f.DATA]: 'DATA', [f.HEADERS]: 'HEADERS', [f.PRIORITY]: 'PRIORITY',
  [f.RST_STREAM]: 'RST', [f.SETTINGS]: 'SETTINGS', [f.PUSH_PROMISE]: 'PUSH',
  [f.PING]: 'PING', [f.GOAWAY]: 'GOAWAY', [f.WINDOW_UPDATE]: 'WU',
  [f.CONTINUATION]: 'CONT' };
const wire = (list) => list.map((e) => `${NAMES[e.type] || e.type}/${e.sid}`).join(' ');

// One client session on the stub, past the handshake and with one request open: the peer
// has advertised nothing (so both sides run the 65 535 default, which is what every real
// arm here does too) and its SETTINGS have been applied, which is what lets the request
// out of the concurrency gate in _enqueue(). Everything the handshake produced is already
// on the wire and acknowledged, so the session starts from a quiet socket.
async function connected() {
  const { sock, written } = stubSocket();
  const sess = new ClientHttp2Session(sock, {}, 'http', 'localhost');
  sock.emit('data', frame(f.SETTINGS, 0, 0, null));
  const stream = sess.request({ ':method': 'GET', ':path': '/' });
  await new Promise((r) => setImmediate(r));
  drain(written);
  assert.strictEqual(sess._writing, false, 'the handshake should be fully written by now');
  assert.strictEqual(sess._outLen, 0, 'the handshake should have left the queue by now');
  assert.deepStrictEqual(updates(transcript(written)), [], 'handshake must not emit credit');
  return { sock, sess, stream, written };
}

test('credit below the half-window step is carried, and the release leaves as one write',
  async () => {
    const { sock, sess, stream, written } = await connected();
    const before = written.length;

    // 16 000 and then 32 000 octets: both under the 32 767 step, so nothing is owed yet
    // -- and nothing may go out either. An update per read batch is the cadence that was
    // measured worse than node's half-window steps.
    sock.emit('data', data(1, [16000], 0x61));
    sock.emit('data', data(1, [16000], 0x61));
    assert.deepStrictEqual(updates(transcript(written)), []);
    assert.strictEqual(written.length, before, 'a sub-threshold batch must not write at all');
    assert.strictEqual(stream._recvUsed, 32000, 'the owed credit must accumulate');
    assert.strictEqual(sess._debt.length, 1, 'the stream must still be in the debt list');

    // 16 000 more crosses the step: the stream and the connection each now owe the whole
    // 48 000, and the two frames leave in the SAME write -- that is the coalescing.
    sock.emit('data', data(1, [16000], 0x62));
    const due = updates(transcript(written));
    assert.deepStrictEqual(due, ['1/48000', '0/48000'],
      `expected the stream update then the connection update, got ${due}`);
    assert.strictEqual(written.length, before + 1,
      'the two updates due from one pass must cost one write');
    assert.strictEqual(stream._recvUsed, 0, 'released credit must not be paid twice');
    assert.strictEqual(sess._debt.length, 0, 'a released stream must leave the debt list');

    // carried again (32 000), and the connection window -- restored once already -- must
    // not be released early either
    const before2 = written.length;
    sock.emit('data', data(1, [16000, 16000], 0x63));
    assert.strictEqual(written.length, before2, 'a carried batch must not write either');

    // and the carried 32 000 is paid by the pass that pushes it past the step: 17 000 new
    // octets settle 49 000. A different amount from the first release on purpose (above).
    sock.emit('data', data(1, [16000, 1000], 0x64));
    assert.deepStrictEqual(updates(transcript(written)).slice(-2), ['1/49000', '0/49000']);

    // nothing the socket was given was rewritten after it was handed over
    for (const w of written) {
      assert.deepStrictEqual(Buffer.from(w.buf), w.snap, 'an issued chunk changed under the socket');
    }
    sess.destroy();
  });

test('a WINDOW_UPDATE does not overtake a queued HEADERS, and does not wait for one',
  async () => {
    const { sock, sess, written } = await connected();
    const pending = () => written.filter((w) => w.cb && !w.done).length;

    // A second request, so that a HEADERS is in flight when the credit comes due. Nothing
    // is QUEUED behind it: that is the state in which the credit pass may hand its bytes
    // to the socket directly, and they still go out behind the HEADERS.
    sess.request({ ':method': 'GET', ':path': '/two' });
    await new Promise((r) => setImmediate(r));
    assert.ok(pending() > 0, 'the second request should be an outstanding write');
    const list0 = transcript(written);
    const headersAt = list0.findIndex((e) => e.type === f.HEADERS && e.sid === 3);
    assert.ok(headersAt >= 0, 'the second HEADERS is missing from the transcript');
    const before = written.length;

    // credit for stream 1 while that write is still in flight
    sock.emit('data', data(1, [16000, 16000, 16000], 0x61));
    assert.strictEqual(written.length, before + 1,
      'the release must not sit in the queue until the in-flight write completes');

    // a second release, still with the first write unacknowledged and a different
    // increment: the buffer the pass coalesces into is reused, so an issued VIEW would
    // now read 49 000 where the socket was given 48 000
    sock.emit('data', data(1, [16000, 16000], 0x62));
    sock.emit('data', data(1, [16000, 1000], 0x63));

    const list = transcript(written);
    assert.ok(list.every((e, i) => e.type !== f.WINDOW_UPDATE || i > headersAt),
      'a WINDOW_UPDATE went out ahead of the HEADERS that opened its own stream');
    assert.deepStrictEqual(updates(list), ['1/48000', '0/48000', '1/49000', '0/49000']);
    for (const w of written) {
      assert.deepStrictEqual(Buffer.from(w.buf), w.snap, 'an issued chunk changed under the socket');
    }
    drain(written);
    sess.destroy();
  });

// The other half of rule 3, and the reason the direct write is conditional at all. Here
// the handshake write is deliberately left unacknowledged, so the SETTINGS ACK and the
// request's HEADERS are still QUEUED (`_outLen > 0`) when credit comes due: writing the
// update straight to the socket would put it on the wire ahead of the HEADERS that opens
// the stream it names, and the peer reads a WINDOW_UPDATE for a stream it has not seen --
// an error of its own making, reported for the whole connection.
test('credit that comes due while frames are queued goes behind them', async () => {
  const { sock, written } = stubSocket();
  const sess = new ClientHttp2Session(sock, {}, 'http', 'localhost');
  sock.emit('data', frame(f.SETTINGS, 0, 0, null));
  sess.request({ ':method': 'GET', ':path': '/' });
  await new Promise((r) => setImmediate(r));
  // one write on the wire (preface + SETTINGS), everything else held behind it
  assert.strictEqual(written.length, 1, 'the handshake write must still be outstanding');
  assert.ok(sess._outLen > 0, 'the SETTINGS ACK and the HEADERS must still be queued');
  const queued = sess._outLen;

  sock.emit('data', data(1, [16000, 16000, 16000], 0x61));
  assert.strictEqual(written.length, 1,
    'an update overtaking the queue reaches the peer before the HEADERS it names');
  assert.strictEqual(sess._outLen, queued + 26, 'both updates must have joined the queue');

  drain(written);
  const list = transcript(written);
  assert.strictEqual(wire(list),
    'SETTINGS/0 SETTINGS/0 HEADERS/1 WU/1 WU/0', // ours, then the ACK of the peer's
    'the queued frames must still leave first, the stream update before the connection one');
  assert.deepStrictEqual(updates(list), ['1/48000', '0/48000']);
  for (const w of written) {
    assert.deepStrictEqual(Buffer.from(w.buf), w.snap, 'an issued chunk changed under the socket');
  }
  sess.destroy();
});
