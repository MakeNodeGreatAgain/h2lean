// Idle timeouts above one second share the vendored clock in lib/fast-timer.js:
// one 499 ms handle for the whole process instead of one per connection. What is
// pinned here is the routing, the silence semantics, and -- the actual point --
// that arming many connection timeouts no longer creates many timer handles.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const { createHook } = require('node:async_hooks');
const h2lean = require('../index');
const fastTimer = require('../lib/fast-timer');
const { listen, connect, request } = require('./diff-harness');

const GET = { ':method': 'GET', ':path': '/x', ':scheme': 'http', ':authority': 'localhost' };

// Counts how many Timeout resources the process creates while the callback runs.
function countNewTimerResources(fn) {
  let created = 0;
  const hook = createHook({
    init(provider, type) {
      if (type === 'Timeout') {
        created++;
      }
    }
  }).enable();
  try {
    fn();
  } finally {
    hook.disable();
  }
  return created;
}

function armIdleTimeouts(srv, ms, sessions) {
  for (const session of [...srv._sessions].slice(0, sessions)) {
    session.setTimeout(ms, undefined);
  }
}

test('a long idle timeout goes to the shared clock and creates no new handle', async (t) => {
  const srv = h2lean.createServer();
  srv.on('stream', (stream) => stream.respond({ ':status': 200 }, { endStream: true }));
  t.after(() => srv.close());
  const port = await listen(srv);
  const clients = [connect(port), connect(port), connect(port)];
  t.after(() => clients.forEach((c) => c.destroy()));
  for (const c of clients) {
    await request(c, GET);
  }

  // the shared clock is created once per process, so warm it up outside the hook:
  // what must be free is the per-connection part
  armIdleTimeouts(srv, 5000, 1);
  const created = countNewTimerResources(() => armIdleTimeouts(srv, 5000, 3));
  assert.strictEqual(created, 0,
    'three more 5 s timeouts must not each create a timer handle');
  const sessions = [...srv._sessions];
  assert.strictEqual(sessions.length, 3);
  for (const session of sessions) {
    assert.ok(session._idleTimer, 'each session holds a timer');
    assert.strictEqual(session._idleTimer[fastTimer.kFastTimer], true,
      'and it is a FastTimer on the shared clock');
  }

  // a short timeout still needs a precise native handle
  const createdShort = countNewTimerResources(() => armIdleTimeouts(srv, 500, 3));
  assert.strictEqual(createdShort, 3, 'sub-second timeouts stay native');
  for (const session of sessions) {
    session.setTimeout(0);
  }
});

test('the shared timeout fires on silence, within the clock resolution', async (t) => {
  const srv = h2lean.createServer();
  srv.on('stream', (stream) => stream.respond({ ':status': 200 }, { endStream: true }));
  t.after(() => srv.close());
  const port = await listen(srv);
  const client = connect(port);
  t.after(() => client.destroy());
  await request(client, GET);

  const session = [...srv._sessions][0];
  const firedAt = await new Promise((resolve) => {
    const start = Date.now();
    const onT = () => {
      clearTimeout(deadline); resolve(Date.now() - start);
    };
    session.on('timeout', onT);
    session.setTimeout(1500);
    const deadline = setTimeout(() => resolve('never'), 5000);
  });

  assert.notStrictEqual(firedAt, 'never', 'the timeout must fire');
  assert.ok(firedAt >= 1200 && firedAt <= 3000,
    `fired after ${firedAt}ms, expected roughly 1500ms +/- the 500ms clock`);
  assert.strictEqual(session._idleTimer, null, 'the timer released itself');
});

test('traffic refreshes the idle timeout; silence afterwards still fires', async (t) => {
  const srv = h2lean.createServer();
  srv.on('stream', (stream) => stream.respond({ ':status': 200 }, { endStream: true }));
  t.after(() => srv.close());
  const port = await listen(srv);
  const client = connect(port);
  t.after(() => client.destroy());

  const session = await new Promise((resolve) => {
    request(client, GET).then(() => resolve([...srv._sessions][0]));
  });

  let fired = 0;
  session.on('timeout', () => {
    fired++;
  });
  session.setTimeout(1500);

  // keep the connection busy past the deadline: a refresh every 500 ms
  //
  // Cleaned up twice on purpose: cleared here because the phase below measures what happens once
  // the connection really goes quiet, and registered with t.after because an assertion that fails
  // before this line used to leave a ref'd interval running, which kept the test file's process
  // alive forever -- the failure was then invisible, because the runner never got to print it.
  const beat = setInterval(() => {
    request(client, GET).catch(() => {});
  }, 500);
  t.after(() => clearInterval(beat));
  await new Promise((r) => setTimeout(r, 2600));
  assert.strictEqual(fired, 0, 'traffic must hold the idle timeout back');
  clearInterval(beat);

  const extra = await new Promise((resolve) => {
    const onT = () => {
      session.off('timeout', onT); clearTimeout(timer); resolve(1);
    };
    session.on('timeout', onT);
    const timer = setTimeout(() => resolve(0), 3500);
  });
  assert.strictEqual(extra, 1, 'silence after the traffic still times out');
});

test('setTimeout twice replaces the previous timer, ms 0 disarms', async (t) => {
  const srv = h2lean.createServer();
  srv.on('stream', (stream) => stream.respond({ ':status': 200 }, { endStream: true }));
  t.after(() => srv.close());
  const port = await listen(srv);
  const client = connect(port);
  t.after(() => client.destroy());
  await request(client, GET);

  const session = [...srv._sessions][0];
  const events = [];
  session.on('timeout', () => events.push('timeout'));
  session.setTimeout(1500);
  session.setTimeout(15000, () => events.push('cb')); // replaces the first one
  assert.strictEqual(typeof session._idleTimer, 'object');
  await new Promise((r) => setTimeout(r, 2200));
  assert.deepStrictEqual(events, [], 'the replaced timer must not fire');

  session.setTimeout(0); // disarms
  assert.strictEqual(session._idleTimer, null);
  await new Promise((r) => setTimeout(r, 600));
  assert.deepStrictEqual(events, []);
});
