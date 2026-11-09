// The rapid-reset budget (lib/session.js _onRst), which is the mitigation for
// CVE-2023-44487: a client opens a stream and resets it immediately, in a loop, and the
// server pays an allocation, a 'stream' event and a teardown for a 13-octet frame. The
// concurrency limit does not help -- no stream lives long enough to count against it.
//
// The budget itself is nghttp2's, node:http2 ships it: burst 1000, 33 tokens per elapsed
// second, one token per inbound RST_STREAM, GOAWAY(INTERNAL_ERROR) when a reset arrives
// with nothing left (lib/nghttp2_session.c v1.70.0, session_update_stream_reset_ratelim(),
// constants from lib/nghttp2_session.h). Two of those are asserted against a live
// node:http2 server below, in both directions: 1 000 resets must be served (a client that
// is legal against node must not be throttled harder here) and a flood must be stopped
// (a client that exhausts node must not get more work out of us).
//
// The exact 33-per-second rate is deliberately NOT asserted: measuring it means waiting on
// a wall-clock second inside the test, which is flaky by construction. What is checked
// instead is the shape of the bucket -- a gap grants budget, and the grant is capped at
// the burst -- with the clock pinned so the refill cannot mask the boundary.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const f = require('../lib/frames');
const h2lite = require('../index');
const { Http2Session } = require('../lib/session');
const { listen, closeServer } = require('./diff-harness');

const INTERNAL_ERROR = 2;
const BUDGET = 1000; // nghttp2's NGHTTP2_DEFAULT_STREAM_RESET_BURST
const REQ = Buffer.from([0x82, 0x84, 0x86, 0x01, 9,
  ...Buffer.from('localhost', 'latin1')]);
const CANCEL = Buffer.from([0, 0, 0, 8]);

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// A session-shaped object: _onRst only reads the bucket, looks the stream up and calls
// _fatal. The clock is pinned by leaving _rstSec in the future, so d <= 0 on every call
// and the refill cannot quietly extend a run that is supposed to run out.
// Anything the method reaches for that this object does not carry is answered from the
// prototype, so the fake does not have to be rewritten every time the reset path gains a
// helper -- it still owns the two pieces of state that decide the verdict, the bucket and
// the stream table.
function bucket(tokens) {
  const fatal = [];
  const target = {
    _streams: new Map(),
    _dropped: null, // written by _rememberKilled: reset ids the late DATA may name
    _rstTokens: tokens === undefined ? BUDGET : tokens,
    _rstSec: Number.MAX_SAFE_INTEGER,
    fatal,
    _fatal(code, _msg) {
      fatal.push(code);
    },
    rst(sid) {
      Http2Session.prototype._onRst.call(this, sid, 8);
    }
  };
  return new Proxy(target, {
    get(t, k) {
      if (k in t) {
        return t[k];
      }
      const v = Http2Session.prototype[k];
      return typeof v === 'function' ? v.bind(t) : v;
    }
  });
}

test('the budget is spent one token per reset and the next reset closes the session', () => {
  const s = bucket();
  for (let i = 1; i <= BUDGET; i++) {
    s.rst(i * 2 - 1);
    assert.strictEqual(s.fatal.length, 0, `reset ${i} of ${BUDGET} was refused`);
  }
  s.rst(BUDGET * 2 + 1);
  assert.deepStrictEqual(s.fatal, [INTERNAL_ERROR],
    'the reset after the budget did not close the connection with INTERNAL_ERROR');
  // That the session is closed only once is _fatal's own guard (it returns when the
  // session is no longer open) and the live flood below covers it end to end.
});

test('an idle gap grants budget again, capped at the burst', () => {
  // Empty the bucket, then let time pass. The grant is min(burst, tokens + rate * d), so
  // a long gap must restore exactly the burst -- not the burst plus the accumulated rate,
  // which is what a missing cap would allow and what would make the flood limit useless
  // after any pause.
  const drained = bucket(0);
  drained._rstSec = ((Date.now() / 1000) | 0) - 10_000; // ~10 000 seconds elapsed
  let accepted = 0;
  for (let i = 1; i <= BUDGET + 5; i++) {
    const before = drained.fatal.length;
    drained.rst(i * 2 - 1);
    if (drained.fatal.length === before) {
      accepted++;
    }
  }
  assert.strictEqual(accepted, BUDGET,
    `a 10 000 second gap granted ${accepted} resets, the cap is the burst = ${BUDGET}`);
});

test('a gap of one second grants more than nothing', () => {
  const s = bucket(0);
  s._rstSec = ((Date.now() / 1000) | 0) - 1;
  let accepted = 0;
  for (let i = 1; i <= 200; i++) {
    const before = s.fatal.length;
    s.rst(i * 2 - 1);
    if (s.fatal.length === before) {
      accepted++;
    } else {
      break;
    }
  }
  assert.ok(accepted >= 30, `one second granted only ${accepted} resets`);
  assert.ok(accepted < BUDGET, `one second granted ${accepted} resets -- a full burst?`);
});

// ---- the same two boundaries against a live node:http2 server -------------------------------

// Open and immediately reset `count` streams, and report what the server did. The streams
// are answered by the handler, so this measures the real cost of a reset stream rather
// than the cost of ignoring one.
async function flood(impl, count) {
  const srv = impl.createServer();
  let reached = 0;
  srv.on('stream', (stream) => {
    reached++;
    // The flood outlives the connection: the socket goes away while the server still has
    // queued streams of it, and each one ends with an error. Without a listener that error
    // reaches the process as an uncaughtException after the test has already been
    // reported -- which node:test counts as a failed file, silently.
    stream.on('error', () => {});
    stream.respond({ ':status': 200 }, { endStream: true });
  });
  srv.on('session', (session) => session.on('error', () => {}));
  srv.on('connection', (sock) => sock.on('error', () => {}));
  srv.on('error', () => {});
  const port = await listen(srv);

  const result = await new Promise((resolve) => {
    const parts = [];
    for (let i = 1; i <= count; i++) {
      const sid = i * 2 - 1;
      parts.push(frame(f.HEADERS, f.F_END_HEADERS, sid, REQ));
      parts.push(frame(f.RST_STREAM, 0, sid, CANCEL));
    }
    const out = { goaway: 0, lastStream: 0, reached: 0 };
    const sock = net.connect(port, '127.0.0.1');
    let acc = Buffer.alloc(0);
    sock.on('error', () => {});
    sock.on('data', (d) => {
      acc = Buffer.concat([acc, d]);
      for (;;) {
        if (acc.length < f.HEADER_LEN) {
          return;
        }
        const len = (acc[0] << 16) | (acc[1] << 8) | acc[2];
        if (acc.length < f.HEADER_LEN + len) {
          return;
        }
        const type = acc[3], flags = acc[4];
        const payload = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
        acc = acc.subarray(f.HEADER_LEN + len);
        if (type === f.SETTINGS && !(flags & f.F_ACK)) {
          // the preface and our SETTINGS went out on 'connect' -- a second preface here
          // would be a connection error of its own and would mask the budget
          sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
          sock.write(Buffer.concat(parts));
        } else if (type === f.GOAWAY && out.goaway === 0) {
          out.goaway = payload.readUInt32BE(4);
          out.lastStream = payload.readUInt32BE(0);
        }
      }
    });
    // connect writes the preface itself: the flood starts when the SETTINGS arrive
    sock.on('connect', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
    });
    setTimeout(() => {
      out.reached = reached;
      sock.destroy();
      resolve(out);
    }, 1200);
  });
  await closeServer(srv);
  return result;
}

for (const impl of [{ name: 'h2lite', mod: h2lite }, { name: 'node', mod: require('node:http2') }]) {
  test(`${impl.name}: ${BUDGET} resets are within the budget, a flood is not`, async () => {
    const quiet = await flood(impl.mod, BUDGET);
    assert.strictEqual(quiet.goaway, 0,
      `the connection was closed after ${BUDGET} resets (code ${quiet.goaway})`);
    assert.strictEqual(quiet.reached, BUDGET, 'not every stream reached the handler');

    const loud = await flood(impl.mod, 3 * BUDGET);
    assert.strictEqual(loud.goaway, INTERNAL_ERROR,
      `a flood was not stopped with GOAWAY(INTERNAL_ERROR), code=${loud.goaway}`);
    // the budget, not the whole flood: a stopped connection must have refused the rest
    assert.ok(loud.reached < 3 * BUDGET, `the handler still ran ${loud.reached} times`);
    assert.ok(loud.lastStream >= 2 * BUDGET + 1 && loud.lastStream <= 3 * BUDGET * 2 + 1,
      `GOAWAY named stream ${loud.lastStream}, which is not the last one received`);
  });
}

test('a normal client that resets a handful of streams is not disturbed', async () => {
  // The false-positive check. Real clients do reset streams -- curl, browsers and h2load
  // cancel requests that finish early -- so 40 resets interleaved with real requests must
  // leave the connection usable. A budget of 1000 exists to stop a flood, not to punish
  // ordinary traffic.
  const srv = h2lite.createServer();
  const paths = [];
  srv.on('stream', (stream, headers) => {
    paths.push(headers[':path']);
    stream.on('error', () => {});
    stream.respond({ ':status': 200 }, { endStream: true });
  });
  srv.on('error', () => {});
  const port = await listen(srv);

  const seen = await new Promise((resolve) => {
    const out = [];
    const sock = net.connect(port, '127.0.0.1');
    let acc = Buffer.alloc(0);
    sock.on('data', (d) => {
      acc = Buffer.concat([acc, d]);
      for (;;) {
        if (acc.length < f.HEADER_LEN) {
          return;
        }
        const len = (acc[0] << 16) | (acc[1] << 8) | acc[2];
        if (acc.length < f.HEADER_LEN + len) {
          return;
        }
        const type = acc[3], flags = acc[4];
        acc = acc.subarray(f.HEADER_LEN + len);
        if (type === f.SETTINGS && !(flags & f.F_ACK)) {
          sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
          const parts = [];
          for (let i = 0; i < 40; i++) {
            const sid = i * 2 + 1;
            parts.push(frame(f.HEADERS, f.F_END_HEADERS, sid, REQ));
            parts.push(frame(f.RST_STREAM, 0, sid, CANCEL));
          }
          // and then the requests this client actually wants answered
          parts.push(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 81, REQ));
          sock.write(Buffer.concat(parts));
        } else if (type === f.HEADERS && (flags & f.F_END_HEADERS)) {
          out.push('HEADERS');
        } else if (type === f.GOAWAY) {
          out.push('GOAWAY');
        }
      }
    });
    sock.on('connect', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
    });
    setTimeout(() => {
      sock.destroy(); resolve(out);
    }, 900);
  });
  await closeServer(srv);
  assert.ok(!seen.includes('GOAWAY'), 'the connection was closed for 40 ordinary resets');
  // Every stream still reached the handler -- and was answered before its reset arrived,
  // which is why the frame count here is not the interesting number: the connection
  // surviving and the last request being served is.
  assert.strictEqual(paths.length, 41, 'a reset stream was not handled as a normal one');
});
