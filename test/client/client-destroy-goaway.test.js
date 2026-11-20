// The goodbye a client session puts on the wire when it is destroyed, and the resets
// that go with it. node's closeSession (internal/http2/core.js:1241-1268) terminates the
// nghttp2 session with the derived code -- handle.destroy(code, socket.destroyed) --
// which submits GOAWAY(lastStreamID, code) before the socket goes, and destroys the open
// streams with the session's error first.
//
// Measured on a live node v26.12.1 client against a raw frame-decoding peer, in the same
// session, through the same peer for both arms:
//   * destroy() / destroy(err) / destroy(8) / destroy(err, 1) send
//     GOAWAY(last=0, code=0 / 2 / 8 / 1). last is 0 with no pushes: for a CLIENT the
//     processed streams are the server-initiated ones, so nghttp2's
//     get_last_proc_stream_id is the highest promised id, not the next request id;
//   * the goodbye is not conditional on the session still being open. A graceful
//     close() first sends GOAWAY(NO_ERROR), and a destroy afterwards sends a SECOND
//     GOAWAY carrying the destroy code -- both in the same tick and a tick apart;
//   * with no request at all the GOAWAY still goes out;
//   * a destroy while connecting puts nothing on the wire;
//   * the open streams are reset ALL BUT THE LAST of them: one open stream produces no
//     RST_STREAM, two produce RST(1), three produce RST(1) RST(3), four produce
//     RST(1) RST(3) RST(5), each in ascending id order and ahead of the GOAWAY. A stream
//     the application already destroyed is not counted: with two requests and the second
//     one destroyed first, the session destroy sends the GOAWAY alone. Why the last open
//     stream is exempt lives in the C++ layer (node_http2.cc / nghttp2), which was not
//     read -- the rule pinned here is the wire measurement, repeated across four stream
//     counts and three codes, not a derivation;
//   * the reset code is each stream's OWN derivation, not the goodbye's.
//     Http2Stream._destroy (core.js:2504-2521) reads sessionCode =
//     goawayCode || destroyCode and only falls back to the error when that is zero:
//     node's internal AbortError to CANCEL(8), anything else to INTERNAL_ERROR(2). So
//     destroy(err, 0) sends GOAWAY(0,0) beside RST(1,2), and destroy(abortError, 0)
//     sends GOAWAY(0,0) beside RST(3,8) -- both measured, the second through a request
//     aborted by its own AbortSignal, which is how an application holds that error;
//   * the closed flag belongs to close() alone (core.js:1657). A bare goaway() leaves it
//     false and the next request() still reaches the wire; close() sets it synchronously
//     and refuses the next request ERR_HTTP2_GOAWAY_SESSION; destroy() leaves it false
//     while destroyed is true. All three measured on a live node client.
//
// Not pinned, both measured and below the HTTP/2 surface: whether the peer sees a clean
// socket close or ECONNRESET after the goodbye (node's finishSessionClose ends the socket
// and then destroys it a tick later, so which one the peer observes is a race -- the
// CONTRIBUTING.md 5 GOAWAY-echo family), and whether the TCP connection of a
// destroy-while-connecting completes before it goes (node's does, h2lean's does not;
// neither puts a frame on the wire and both report the same session error and 'close').
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const http2 = require('node:http2');
const h2lean = require('../../index');
const f = require('../../lib/frames');
const { DEFERRED_REQUEST_ERROR } = require('../node-version');

const PREFACE = 'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n';

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// A raw peer that completes the handshake and records every frame the client sends, in
// order. It never answers and never closes its own side, so the only frames on the wire
// are the ones the destroy under test produced. Socket events are deliberately not
// recorded: see the header.
function peer(wire) {
  return new Promise((resolve) => {
    const srv = net.createServer((sock) => {
      let buf = Buffer.alloc(0);
      let prefaceSeen = false;
      sock.on('data', (d) => {
        buf = Buffer.concat([buf, d]);
        if (!prefaceSeen) {
          const i = buf.indexOf(PREFACE);
          if (i < 0) {
            return;
          }
          prefaceSeen = true;
          buf = buf.subarray(i + PREFACE.length);
          sock.write(frame(f.SETTINGS, 0, 0, Buffer.alloc(0)));
        }
        while (buf.length >= f.HEADER_LEN) {
          const len = buf.readUIntBE(0, 3);
          if (buf.length < f.HEADER_LEN + len) {
            break;
          }
          const type = buf[3];
          const flags = buf[4];
          const sid = buf.readUInt32BE(5) & 0x7fffffff;
          const body = buf.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
          buf = buf.subarray(f.HEADER_LEN + len);
          if (type === f.SETTINGS && !(flags & f.F_ACK)) {
            sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
          } else if (type === f.RST_STREAM) {
            wire.push(`RST(${sid},${body.readUInt32BE(0)})`);
          } else if (type === f.GOAWAY) {
            wire.push(`GOAWAY(${body.readUInt32BE(0)},${body.readUInt32BE(4)})`);
          } else if (type === f.HEADERS) {
            wire.push(`HEADERS(${sid})`);
          }
        }
      });
      sock.on('error', () => {});
    });
    srv.on('error', () => {});
    // srv.close() waits for open connections, so a client that never closes its socket
    // would hang the file rather than fail a test. Track them and end them explicitly.
    srv.sockets = [];
    srv.on('connection', (s) => {
      srv.sockets.push(s); s.on('error', () => {});
    });
    srv.listen(0, '127.0.0.1', () => resolve({ srv, port: srv.address().port }));
  });
}

function closePeer(srv) {
  return new Promise((resolve) => {
    for (const s of srv.sockets) {
      s.destroy();
    }
    srv.close(resolve);
  });
}

// `count` requests are made and left open; `pre` destroys the last of them before the
// session destroy; `abortFirst` aborts the first one through its own AbortSignal and
// hands the resulting error to `destroy`; `closeFirst` runs a graceful close() first;
// `connecting` destroys before the 'connect' event. The wire is returned in order --
// frame order is the point -- and the session's own reports sorted, since the teardown
// turn's internal order is not what is under test.
async function transcript(impl, opts) {
  const wire = [];
  const { srv, port } = await peer(wire);
  const out = [];
  let aborted;
  try {
    const client = impl.connect(`http://127.0.0.1:${port}`);
    client.on('error', (e) => out.push(`session error ${e.code}/${e.message}`));
    client.on('close', () => out.push('session close'));
    if (!opts.connecting) {
      await new Promise((r) => client.once('connect', r));
      const reqs = [];
      let ac;
      for (let i = 0; i < (opts.count || 0); i++) {
        let ropts;
        if (opts.abortFirst && i === 0) {
          ac = new AbortController();
          ropts = { signal: ac.signal };
        }
        const req = client.request({ ':path': `/${i}` }, ropts);
        req.on('error', (e) => {
          if (opts.abortFirst && i === 0) {
            aborted = e;
          }
        });
        req.on('close', () => out.push(`close ${i} rstCode=${req.rstCode}`));
        req.resume();
        reqs.push(req);
      }
      if (opts.count) {
        await new Promise((r) => setTimeout(r, 60));
      }
      if (opts.abortFirst) {
        ac.abort();
        await new Promise((r) => setTimeout(r, 60));
        wire.length = 0;
      }
      if (opts.pre) {
        reqs[reqs.length - 1].destroy();
        await new Promise((r) => setTimeout(r, 60));
        wire.length = 0;
      }
      if (opts.closeFirst) {
        client.close();
        if (opts.tick) {
          await new Promise((r) => setImmediate(r));
        }
      }
    }
    opts.destroy(client, aborted);
    await new Promise((r) => setTimeout(r, 150));
  } finally {
    await closePeer(srv);
  }
  return { wire, events: out.sort() };
}

const SHAPES = [
  ['no argument', (c) => c.destroy(), 0],
  ['an error', (c) => c.destroy(new Error('boom')), 2],
  ['a numeric code', (c) => c.destroy(8), 8],
  ['an error and a code', (c) => c.destroy(new Error('boom'), 1), 1]
];

test('a destroy says goodbye with the code it derived', async () => {
  for (const [label, destroy, code] of SHAPES) {
    const native = await transcript(http2, { count: 1, destroy });
    assert.deepStrictEqual(native.wire, ['HEADERS(1)', `GOAWAY(0,${code})`],
      `node wire, ${label}`);
    const ours = await transcript(h2lean, { count: 1, destroy });
    assert.deepStrictEqual(ours, native, label);
  }
});

test('the goodbye goes out with no stream, and after a graceful close', async () => {
  const shapes = [
    ['no request', { count: 0, destroy: (c) => c.destroy() }, ['GOAWAY(0,0)']],
    ['a close first', { count: 1, closeFirst: true, destroy: (c) => c.destroy(2) },
      ['HEADERS(1)', 'GOAWAY(0,0)', 'GOAWAY(0,2)']],
    ['a close a tick earlier', { count: 2, closeFirst: true, tick: true,
      destroy: (c) => c.destroy(new Error('boom')) },
    ['HEADERS(1)', 'HEADERS(3)', 'GOAWAY(0,0)', 'RST(1,2)', 'GOAWAY(0,2)']]
  ];
  for (const [label, opts, wire] of shapes) {
    const native = await transcript(http2, opts);
    assert.deepStrictEqual(native.wire, wire, `node wire, ${label}`);
    const ours = await transcript(h2lean, opts);
    assert.deepStrictEqual(ours, native, label);
  }
});

test('every open stream but the last is reset with the session code', async () => {
  for (const count of [1, 2, 3, 4]) {
    const expected = [];
    for (let i = 0; i < count; i++) {
      expected.push(`HEADERS(${i * 2 + 1})`);
    }
    for (let i = 0; i < count - 1; i++) {
      expected.push(`RST(${i * 2 + 1},2)`);
    }
    expected.push('GOAWAY(0,2)');
    const opts = { count, destroy: (c) => c.destroy(new Error('boom')) };
    const native = await transcript(http2, opts);
    assert.deepStrictEqual(native.wire, expected, `node wire, ${count} streams`);
    const ours = await transcript(h2lean, opts);
    assert.deepStrictEqual(ours, native, `${count} streams`);
    // The code reaches every stream, the one that was reset and the one that was not.
    for (let i = 0; i < count; i++) {
      assert.ok(native.events.includes(`close ${i} rstCode=2`), `node rstCode ${i}`);
    }
  }
});

test('a stream already destroyed is not counted, and a connecting session sends nothing',
  async () => {
    const shapes = [
      ['the second request destroyed first',
        { count: 2, pre: true, destroy: (c) => c.destroy() }, ['GOAWAY(0,0)']],
      ['destroyed while connecting',
        { connecting: true, destroy: (c) => c.destroy(new Error('boom')) }, []]
    ];
    for (const [label, opts, wire] of shapes) {
      const native = await transcript(http2, opts);
      assert.deepStrictEqual(native.wire, wire, `node wire, ${label}`);
      const ours = await transcript(h2lean, opts);
      assert.deepStrictEqual(ours.wire, native.wire, label);
      assert.deepStrictEqual(ours.events, native.events, `${label} events`);
    }
  });

test('the reset code is the stream derivation, not the goodbye code', async () => {
  // An explicit zero code zeroes the GOAWAY and not the resets: with no goawayCode and
  // destroyCode 0 the derivation falls through to the error (core.js:2512-2520).
  const plain = { count: 2, destroy: (c) => c.destroy(new Error('boom'), 0) };
  const native = await transcript(http2, plain);
  assert.deepStrictEqual(native.wire,
    ['HEADERS(1)', 'HEADERS(3)', 'RST(1,2)', 'GOAWAY(0,0)'], 'node wire');
  const ours = await transcript(h2lean, plain);
  assert.deepStrictEqual(ours, native, 'a plain error and code 0');
  // node's own AbortError takes the CANCEL arm instead. The application holds it from
  // the aborted request's 'error'; three requests leave two open after the abort, so
  // exactly one reset goes out and it carries 8.
  const abort = { count: 3, abortFirst: true, destroy: (c, e) => c.destroy(e, 0) };
  const nativeAbort = await transcript(http2, abort);
  assert.deepStrictEqual(nativeAbort.wire, ['RST(3,8)', 'GOAWAY(0,0)'], 'node abort wire');
  const oursAbort = await transcript(h2lean, abort);
  assert.deepStrictEqual(oursAbort, nativeAbort, 'an AbortError and code 0');
});

// What a call leaves the closed flag at, and whether the next request still reaches the
// wire. The peer never answers, so a request that was submitted stays open and its
// HEADERS is the evidence; one that was refused reports its error instead.
async function afterCall(impl, call) {
  const wire = [];
  const { srv, port } = await peer(wire);
  const late = [];
  let flags;
  let seen;
  try {
    const client = impl.connect(`http://127.0.0.1:${port}`);
    client.on('error', () => {});
    await new Promise((r) => client.once('connect', r));
    const first = client.request({ ':path': '/first' });
    first.on('error', () => {});
    first.resume();
    await new Promise((r) => setTimeout(r, 60));
    call(client);
    flags = `closed=${client.closed} destroyed=${client.destroyed}`;
    // node 20/22 throw the session-state error synchronously out of request(); 24+ (and
    // h2lean on every version) accept the call and defer the error to the stream. Record
    // the throw as a late line so the flags and wire stay differential across versions.
    let req;
    try {
      req = client.request({ ':path': '/after' });
    } catch (e) {
      late.push(`threw ${e.code}`);
    }
    if (req !== undefined) {
      req.on('error', (e) => late.push(`error ${e.code}`));
      req.on('close', () => late.push(`close ${req.rstCode}`));
      req.resume();
    }
    await new Promise((r) => setTimeout(r, 150));
    // The peer teardown closes the socket, and a live request answers that with a
    // CANCEL -- after the exchange under test is over, and on a different turn per arm.
    seen = { wire: wire.slice(), late: late.slice().sort() };
  } finally {
    await closePeer(srv);
  }
  return { wire: seen.wire, flags, late: seen.late };
}

test('the closed flag belongs to close() alone', async () => {
  // The sixth field is the code node 20/22 throw synchronously from the late request()
  // (null where the call is accepted on every version); the fifth is the deferred late
  // transcript node 24+ and h2lean report. Only that field is gated (test/node-version.js).
  const shapes = [
    ['goaway()', (c) => c.goaway(), 'closed=false destroyed=false',
      ['HEADERS(1)', 'GOAWAY(0,0)', 'HEADERS(3)'], [], null],
    ['close()', (c) => c.close(), 'closed=true destroyed=false',
      ['HEADERS(1)', 'GOAWAY(0,0)'], ['close 2', 'error ERR_HTTP2_GOAWAY_SESSION'],
      'ERR_HTTP2_GOAWAY_SESSION'],
    ['destroy()', (c) => c.destroy(), 'closed=false destroyed=true',
      ['HEADERS(1)', 'GOAWAY(0,0)'], ['close 2', 'error ERR_HTTP2_INVALID_SESSION'],
      'ERR_HTTP2_INVALID_SESSION']
  ];
  for (const [label, call, flags, wire, late, threw] of shapes) {
    const nodeLate = DEFERRED_REQUEST_ERROR || threw === null ? late : [`threw ${threw}`];
    const native = await afterCall(http2, call);
    assert.strictEqual(native.flags, flags, `node flags, ${label}`);
    assert.deepStrictEqual(native.wire, wire, `node wire, ${label}`);
    assert.deepStrictEqual(native.late, nodeLate, `node late request, ${label}`);
    const ours = await afterCall(h2lean, call);
    assert.strictEqual(ours.flags, flags, `h2lean flags, ${label}`);
    assert.deepStrictEqual(ours.wire, wire, `h2lean wire, ${label}`);
    assert.deepStrictEqual(ours.late, late, `h2lean late request, ${label}`);
  }
});
