// Connection lifetime policy: what an idle HTTP/2 connection does, measured against node 26.
//
// The rules below were read off a live node server before being written into the code, because
// the documentation and node 26 disagree here:
//   * `server.sessionTimeout` -- documented for years, and NOT present on node 26's server at
//     all. It is therefore not implemented either: an h2lean-only property with no node
//     counterpart is a new surface, not a compatibility fix.
//   * `server.setTimeout(msecs)` only records `this.timeout` (internal/http2/core.js:3541) and
//     attaches the callback; it does not walk the sessions. Measured with an idle connection
//     and server.setTimeout(200): no 'timeout' on the session, no close, the connection simply
//     stays. h2lean used to arm every live session from here -- inventing an event node does
//     not emit, and skipping every connection accepted afterwards.
//   * `session.setTimeout(msecs)` is the per-connection policy: it counts silence, activity
//     refreshes it, and when it fires the session only emits 'timeout'. node's `callTimeout()`
//     destroys nothing, so closing an idle connection stays the application's call -- the test
//     below sends a request AFTER the timeout fired and expects a normal answer.
//   * `server.updateSettings(settings)` merges into the server's own settings and reaches the
//     NEXT connection, verified on the wire rather than through a getter.
//
// Delays are all below 1 s on purpose: above that h2lean arms on the shared clock in
// lib/fast-timer.js, whose accuracy is +/- 500 ms by design (that routing is what
// bench/timers.js measures).
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const f = require('../lib/frames');
const h2lean = require('../index');
const nodeHttp2 = require('node:http2');
const { listen, connect } = require('./diff-harness');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// One server, one client, a scripted timeline. `arm` runs on the session as soon as it exists;
// the report is every event the server saw, in order, plus what the client saw.
async function run(impl, arm, script) {
  const log = [];
  const srv = impl.createServer();
  let armed;
  const ready = new Promise((resolve) => {
    srv.on('session', (session) => {
      log.push('session');
      session.on('timeout', () => log.push('timeout'));
      session.on('close', () => log.push('close'));
      session.on('goaway', (code) => log.push('goaway:' + code));
      session.on('error', (e) => log.push('error:' + e.code));
      armed = arm(session, srv, log);
      resolve(session);
    });
  });
  srv.on('stream', (stream) => {
    log.push('stream');
    stream.on('error', () => {});
    stream.respond({ ':status': 200 }, { endStream: true });
  });
  srv.on('error', () => {});
  const port = await listen(srv);
  const client = connect(port);
  client.on('close', () => log.push('client.close'));
  client.on('goaway', (code) => log.push('client.goaway:' + code));
  client.on('error', (e) => log.push('client.error:' + e.code));
  await ready;
  try {
    await script(client, log, srv, armed);
    await sleep(150);
  } finally {
    // Teardown happens even when the script threw: a failing assertion left a listening server
    // and a live session behind, the file's process never exited, and the runner sat waiting for
    // a result that had already been decided -- one broken guard turning into a suite that hangs.
    client.destroy();
    await sleep(100);
    // raced, not awaited: server.close() waits for its session set to empty, and a close that
    // never completes would put the hang back where it was found -- a failing assertion has to
    // produce a failure, not a test file that stops reporting anything.
    srv.close();
    await Promise.race([new Promise((r) => srv.on('close', r)), sleep(500)]);
  }
  // The one order this file cannot pin, and it is not a protocol property: the server's
  // 'goaway:N' (raised when the destroyed client's GOAWAY is read) and the client's own 'close'
  // belong to two different objects in the same tick, and node does not order them -- measured
  // on the node arm itself, which reports both `goaway:0 client.close` and
  // `client.close goaway:0` across runs. Both events are still required and neither is moved
  // past a third one: only an adjacent swap of exactly this pair is forgiven.
  return canonical(log);
}

function canonical(log) {
  const out = [...log];
  for (let i = 1; i < out.length; i++) {
    if (out[i - 1] === 'client.close' && /^goaway:/.test(out[i])) {
      out[i - 1] = out[i];
      out[i] = 'client.close';
    }
  }
  return out.join(' ');
}

// A request that must be answered; reports what the client got back.
async function ask(client, log) {
  const req = client.request({ ':method': 'GET', ':path': '/' });
  req.resume();
  const got = await new Promise((resolve) => {
    req.on('response', (h) => resolve(h[':status']));
    req.on('error', (e) => resolve('err:' + e.code));
    req.end();
    setTimeout(() => resolve('none'), 500);
  });
  log.push('resp:' + got);
}

const armSession = (ms, cb) => (session) => session.setTimeout(ms, cb);
const armServer = (ms) => (session, srv) => srv.setTimeout(ms);

test('a session timeout fires on silence and closes nothing', async () => {
  const got = [];
  for (const impl of [nodeHttp2, h2lean]) {
    got.push(await run(impl, armSession(300), async (client, log) => {
      await sleep(600);
      await ask(client, log); // after the timeout: still a live connection
    }));
  }
  const want = 'session timeout stream resp:200 goaway:0 client.close close';
  assert.strictEqual(got[0], want, `node: ${got[0]}`);
  assert.strictEqual(got[1], want, `h2lean: ${got[1]} vs node: ${got[0]}`);
});

test('activity refreshes the session timeout', async () => {
  // 400 ms timeout, a request at 200 ms: nothing may have fired 350 ms after it, and the
  // timeout must arrive once the connection really goes quiet.
  const got = [];
  for (const impl of [nodeHttp2, h2lean]) {
    got.push(await run(impl, armSession(400), async (client, log) => {
      await sleep(200);
      await ask(client, log);
      await sleep(350);
      log.push('checkpoint:' + (log.includes('timeout') ? 'fired' : 'quiet'));
      await sleep(400);
    }));
  }
  const want = 'session stream resp:200 checkpoint:quiet timeout goaway:0 client.close close';
  assert.strictEqual(got[0], want, `node: ${got[0]}`);
  assert.strictEqual(got[1], want, `h2lean: ${got[1]} vs node: ${got[0]}`);
});

test('server.setTimeout does not arm a session timeout', async () => {
  // The regression this pins: h2lean used to loop over this._sessions here, which produced a
  // 'timeout' node does not produce at the server level.
  const got = [];
  for (const impl of [nodeHttp2, h2lean]) {
    got.push(await run(impl, armServer(200), async (client, log) => {
      await sleep(600);
      await ask(client, log);
    }));
  }
  const want = 'session stream resp:200 goaway:0 client.close close';
  assert.doesNotMatch(got[0], /timeout/, `node emitted a timeout: ${got[0]}`);
  assert.strictEqual(got[0], want, `node: ${got[0]}`);
  assert.strictEqual(got[1], want, `h2lean: ${got[1]} vs node: ${got[0]}`);
});

test('setTimeout(0) disarms an armed session', async () => {
  const got = [];
  for (const impl of [nodeHttp2, h2lean]) {
    got.push(await run(impl, (session) => {
      session.setTimeout(300);
      session.setTimeout(0);
    }, async (client, log) => {
      await sleep(600);
      await ask(client, log);
    }));
  }
  const want = 'session stream resp:200 goaway:0 client.close close';
  assert.strictEqual(got[0], want, `node: ${got[0]}`);
  assert.strictEqual(got[1], want, `h2lean: ${got[1]} vs node: ${got[0]}`);
});

test('the callback runs after the event, on the session', async () => {
  // node's setTimeout(msecs, callback) is `this.on('timeout', callback)` after the assignment,
  // so a listener registered by the call runs after one that was already attached -- recorded,
  // not assumed.
  const got = [];
  for (const impl of [nodeHttp2, h2lean]) {
    got.push(await run(impl, (session, srv, log) => {
      session.setTimeout(300, () => log.push('cb'));
    }, async (_client, _log) => {
      await sleep(600);
    }));
  }
  const want = 'session timeout cb goaway:0 client.close close';
  assert.strictEqual(got[0], want, `node: ${got[0]}`);
  assert.strictEqual(got[1], want, `h2lean: ${got[1]} vs node: ${got[0]}`);
});

test('a destroyed session never reports a timeout', async () => {
  // The timer belongs to the session, so destroying the session has to disarm it: otherwise a
  // connection that came and went keeps firing 'timeout' at its deadline, and on the shared
  // clock in lib/fast-timer.js it also stays in that clock's list until then.
  //
  // Counted per event rather than as one interleaved trace: after an explicit destroy the order
  // in which the two observers -- this side's session and the peer's client -- notice it is
  // timing, not protocol (the same reasoning as in test/frame-limits.test.js). What is pinned is
  // which events happen at all: no 'timeout', the session closed, and the peer was told with a
  // GOAWAY(NO_ERROR), which is what node's destroy() does and h2lean did not.
  const got = [];
  for (const impl of [nodeHttp2, h2lean]) {
    const seen = { timeout: 0, close: 0, goaway: 'none' };
    const srv = impl.createServer();
    srv.on('session', (session) => {
      session.on('timeout', () => {
        seen.timeout++;
      });
      session.on('close', () => {
        seen.close++;
      });
      session.setTimeout(300);
      setTimeout(() => session.destroy(), 100);
    });
    srv.on('stream', (stream) => {
      stream.on('error', () => {});
    });
    srv.on('error', () => {});
    const port = await listen(srv);
    const client = connect(port);
    client.on('goaway', (code) => {
      seen.goaway = code;
    });
    client.on('error', () => {});
    await sleep(700);
    client.destroy();
    srv.close();
    await sleep(50);
    got.push(JSON.stringify(seen));
  }
  const want = JSON.stringify({ timeout: 0, close: 1, goaway: 0 });
  assert.strictEqual(got[0], want, `node: ${got[0]}`);
  assert.strictEqual(got[1], want, `h2lean: ${got[1]} vs node: ${got[0]}`);
});

// ---- settings that apply to the connections that come after -----------------------------

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// One raw connection, answered with the entries of the SETTINGS frame the server sent first.
// Read off the wire because that is the only place an updated setting has to show up: a
// getter can be stale while the frame is right, and the reverse.
function firstSettings(port) {
  return new Promise((resolve) => {
    const sock = net.connect(port, '127.0.0.1');
    let acc = Buffer.alloc(0);
    let done = false;
    const finish = (value) => {
      if (!done) {
        done = true; sock.destroy(); resolve(value);
      }
    };
    sock.on('error', () => finish(null));
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
        const body = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
        acc = acc.subarray(f.HEADER_LEN + len);
        if (type === f.SETTINGS && !(flags & f.F_ACK)) {
          const out = {};
          for (let i = 0; i + 6 <= body.length; i += 6) {
            out[body.readUInt16BE(i)] = body.readUInt32BE(i + 2);
          }
          finish(out);
          return;
        }
      }
    });
    sock.on('connect', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
    });
    setTimeout(() => finish(null), 800);
  });
}

const ID_MAX_CONCURRENT_STREAMS = 3; // RFC 9113 6.5.2

test('server.updateSettings reaches the next connection, not the live one', async () => {
  // The two stacks do not advertise the same SET of settings (h2lean sends the full set it has
  // in effect, node sends an empty first frame -- see the header comment in lib/server.js), so
  // what is compared is the effect of the call: after it, a new connection must be told
  // max_concurrent_streams = 7, and the session that was already open must still be running.
  for (const impl of [nodeHttp2, h2lean]) {
    const name = impl === nodeHttp2 ? 'node' : 'h2lean';
    assert.strictEqual(typeof impl.createServer().updateSettings, 'function',
      `${name}: server.updateSettings is part of node 26's Http2Server`);
    const srv = impl.createServer();
    const live = [];
    srv.on('session', (session) => live.push(session));
    srv.on('stream', (stream) => {
      stream.on('error', () => {});
    });
    srv.on('error', () => {});
    const port = await listen(srv);
    let client = null;
    try {
      const before = await firstSettings(port);
      // the raw connection above is a connection too, so the session belonging to the client
      // below is found by position, not by counting from zero
      const known = live.length;
      client = connect(port);
      await sleep(150);
      assert.strictEqual(live.length, known + 1,
        `${name}: the client connection did not become exactly one session`);
      const session = live[known];
      srv.updateSettings({ maxConcurrentStreams: 7 });
      await sleep(100);
      const after = await firstSettings(port);
      assert.strictEqual(after[ID_MAX_CONCURRENT_STREAMS], 7,
        `${name}: the next connection was not told (${JSON.stringify(after)})`);
      assert.notStrictEqual(before[ID_MAX_CONCURRENT_STREAMS], 7,
        `${name}: the value was already 7 before the call, so the test proves nothing`);
      assert.strictEqual(session.closed, false,
        `${name}: updateSettings closed a session that was already open`);
      // node's session.settings() answers nothing at all (measured on a live session; on a
      // destroyed one it throws ERR_HTTP2_INVALID_SESSION), so this is checked on the open one
      assert.strictEqual(session.settings(), undefined,
        `${name}: session.settings() returned a value where node returns none`);
    } finally {
      // same reason as in run(): an assertion failure here must not leave the server listening
      if (client !== null) {
        client.destroy();
      }
      srv.close();
      await sleep(50);
    }
  }
});

test('an invalid settings argument is refused', async () => {
  for (const impl of [nodeHttp2, h2lean]) {
    const name = impl === nodeHttp2 ? 'node' : 'h2lean';
    const srv = impl.createServer();
    let code = null;
    try {
      srv.updateSettings(null);
    } catch (e) {
      code = e.constructor.name;
    }
    assert.strictEqual(code, 'TypeError', `${name}: updateSettings(null) -> ${code}`);
    srv.close();
  }
});

// ---- the h2lean-only helper -----------------------------------------------------------------

test('closeIdleSessions closes the quiet session and leaves the busy one', async () => {
  // node 26 does not have this method any more (measured: typeof undefined on its server), so
  // there is nothing to compare against -- what is pinned is that the helper h2lean kept does
  // what its name says, and that node really no longer offers it.
  assert.strictEqual(typeof nodeHttp2.createServer().closeIdleSessions, 'undefined',
    'node 26 has closeIdleSessions again: this test should become a differential one');
  const log = [];
  const srv = h2lean.createServer();
  srv.on('session', (session) => {
    session.on('close', () => log.push('session.close'));
    session.on('goaway', (code) => log.push('goaway:' + code));
  });
  srv.on('stream', (stream) => {
    stream.on('error', () => {});
  }); // never answered
  srv.on('error', () => {});
  const port = await listen(srv);
  const idle = connect(port);
  await sleep(100);
  const busy = connect(port);
  // The busy connection gets its own goodbye log: a session that was left alone must not be sent
  // a GOAWAY at all. Checking only `busy.destroyed` cannot see the difference -- a client with a
  // live stream absorbs a GOAWAY without tearing itself down, so closing every session anyway
  // looked like closing none of them.
  const busyGoaway = [];
  busy.on('goaway', (code) => busyGoaway.push(code));
  const req = busy.request({ ':method': 'GET', ':path': '/' });
  req.on('error', () => {});
  req.resume();
  req.end();
  await sleep(150);
  srv.closeIdleSessions();
  await sleep(300);
  const idleClosed = idle.destroyed;
  const busyClosed = busy.destroyed;
  idle.destroy();
  busy.destroy();
  srv.close();
  await sleep(50);
  assert.strictEqual(idleClosed, true, `the quiet session survived (${log.join(' ')})`);
  assert.strictEqual(busyClosed, false, 'the session with a live stream was closed');
  assert.deepStrictEqual(busyGoaway, [],
    `closeIdleSessions said goodbye to the busy connection too (${busyGoaway})`);
  assert.ok(log.includes('goaway:0'), `no GOAWAY was sent to the idle session (${log.join(' ')})`);
});
