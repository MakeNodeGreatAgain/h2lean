// Port of node's `test/parallel/test-http2-client-destroy.js` (CONTRIBUTING.md §1: the
// upstream file is a scenario source, node's measured value is the expectation).
//
// Upstream's nine blocks, in order: destroy through the session and through the raw socket
// after 'connect'; destroy before any operation; destroy before the peer's GOAWAY; destroy
// before the socket connects; close before it connects; a request AbortSignal, live and
// already aborted; a session AbortSignal on both transports; and a request signal the
// *server* aborts from its 'stream' handler.
//
// Three of them turn on rules that are easy to get wrong and are all read out of node's
// bundled internal/http2/core.js:
//
//  * request() keeps argument validation synchronous but DEFERS a session-state failure to
//    the returned stream (:1836-1844 captures it, :1930-1931 dispatches
//    `process.nextTick(requestOnError)` = `this.destroy(error)`). The reason is in node's own
//    comment: a retry issued from a stream callback must not throw before the session's
//    lifecycle handlers have run. So `client.request()` after destroy() or close() returns a
//    stream that errors and closes; it does not throw.
//  * ping/settings/goaway/setNextStreamID/setLocalWindowSize each open with
//    `if (this.destroyed) throw new ERR_HTTP2_INVALID_SESSION()`. The guard is on destroyed
//    alone, not on closed.
//  * requestOnConnect opens with `if (session === undefined || session.destroyed) return;`,
//    and a destroyed stream has already cleared its own kSession (_destroy:2546). A request
//    abandoned while the socket was still connecting therefore submits nothing -- the server
//    never sees a stream for it -- and a session closed in the meantime refuses the request
//    with ERR_HTTP2_GOAWAY_SESSION instead.
//
// A fourth rule is what makes the destroy-before-goaway block close its request at all:
// socketOnClose (:3653) closes every live stream and every pending one with NGHTTP2_CANCEL
// before it closes the session, and CANCEL is not an error (:2543). Without that sweep a
// request whose HEADERS went out but whose answer never came stays live forever -- the
// session emits 'close' while the stream is still open and never destroyed.
//
// The session-level AbortSignal needs no http2 code at all: core.js wires a signal only on
// the request path (:1916-1928). `connect(url, {signal})` works because the options reach
// net.connect / tls.connect, which own the abort listener -- which is why the h2c and h2s
// transcripts below are identical, and why the listener count is 1 from connect() onward
// rather than appearing when the first request is made.
//
// Upstream reaches the raw socket through `client[kSocket]`, an internal symbol. The public
// way to hold the same object is options.createConnection, which both stacks call as
// `createConnection(authority, options)` with the URL as the first argument (core.js:3705);
// the socket it returns is the one the session binds, so destroying it is upstream's second
// destroyCallback.
//
// NOT recorded, deliberately: in the destroy-before-goaway block the request's rstCode and
// any socket error are a race, not a property of either stack. The peer destroys its session
// from the 'stream' handler, so the GOAWAY echo this side writes lands either before the
// socket dies (rstCode 0, and on some runs an EPIPE surfaced as 'error') or after it
// (rstCode 8, silent) -- the GOAWAY-echo window CONTRIBUTING.md §5 describes. Measured over
// repeated runs: both outcomes occur on h2lean, node reports 8. The block's subject is that
// 'close' fires on the session and on the request and that destroy() inside 'close' is
// harmless, and those three lines are stable on both stacks.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const { listenerCount } = require('node:events');
const http2 = require('node:http2');
const h2lean = require('../../index');
const certificates = require('../fixtures-tls');
const { listen, closeServer } = require('../diff-harness');

const INVALID_SESSION = 'ERR_HTTP2_INVALID_SESSION/Error/The session has been destroyed';
const STREAM_CANCEL = 'ERR_HTTP2_STREAM_CANCEL/Error/The pending stream has been canceled';
const GOAWAY_SESSION =
  'ERR_HTTP2_GOAWAY_SESSION/Error/New streams cannot be created after receiving a GOAWAY';

// One block, recorded as a sorted line set. `body` gets the server, a recorder, and a
// `finish` that stops the exchange; `hold` keeps the client so finish can destroy it.
// A transcript that never arrives is a line, not a cancelled file: the deadline records
// HANG and tears down, so a liveness regression fails the test that caused it instead of
// stopping the runner.
function scenario(lib, body, secure) {
  const t = [];
  let finished = false;
  let client;
  let resolve;
  const done = new Promise((r) => {
    resolve = r;
  });
  const push = (line) => t.push(line);
  const srv = secure ? lib.createSecureServer(certificates()) : lib.createServer();
  const finish = () => {
    if (finished) {
      return;
    }
    finished = true;
    clearTimeout(timer);
    if (client) {
      client.destroy();
    }
    closeServer(srv).then(() => resolve(t.sort()));
  };
  const timer = setTimeout(() => {
    push('HANG');
    finish();
  }, 5000);
  srv.on('error', () => {});
  srv.on('session', (s) => {
    s.on('error', () => {});
  });
  body({
    srv,
    push,
    finish,
    setClient: (c) => {
      client = c;
    }
  });
  return done;
}

// The five session methods upstream calls after destroy(), each recorded as the error it
// throws -- or as `no-throw`, which is the regression this block exists to catch.
function throws(push, label, fn) {
  try {
    fn();
    push(`${label} no-throw`);
  } catch (e) {
    push(`${label} ${e.code}/${e.name}/${e.message}`);
  }
}

// ---- block 1: destroy after 'connect', through the session and through the raw socket ----

const AFTER_CONNECT = [
  'close destroyed=true',
  'pre destroyed=false sockDestroyed=false'
];

function afterConnect(lib, via) {
  return scenario(lib, ({ srv, push, finish, setClient }) => {
    listen(srv).then((port) => {
      let raw;
      // createConnection is the public way to hold the socket upstream reaches through
      // client[kSocket]. The authority is the URL, not a {port, host} pair (core.js:3705).
      const opts = via === 'socket'
        ? {
          createConnection: (a) => {
            raw = net.connect({ port: a.port, host: a.hostname });
            return raw;
          }
        }
        : {};
      const client = lib.connect(`http://127.0.0.1:${port}`, opts);
      setClient(client);
      client.on('error', () => {});
      client.on('connect', () => {
        const sock = via === 'socket' ? raw : client.socket;
        push(`pre destroyed=${client.destroyed} sockDestroyed=${sock.destroyed}`);
        if (via === 'socket') {
          raw.destroy();
        } else {
          client.destroy();
        }
        client.on('close', () => {
          push(`close destroyed=${client.destroyed}`);
          finish();
        });
      });
    });
  });
}

test('upstream client-destroy: destroy() after connect', async () => {
  const native = await afterConnect(http2, 'session');
  const ours = await afterConnect(h2lean, 'session');
  assert.deepStrictEqual(native, AFTER_CONNECT);
  assert.deepStrictEqual(ours, native);
});

test('upstream client-destroy: destroying the raw socket after connect', async () => {
  const native = await afterConnect(http2, 'socket');
  const ours = await afterConnect(h2lean, 'socket');
  assert.deepStrictEqual(native, AFTER_CONNECT);
  assert.deepStrictEqual(ours, native);
});

// ---- block 2: destroy before any operation -------------------------------------------------

const BEFORE_OPS = [
  'cli-close',
  `goaway ${INVALID_SESSION}`,
  'imm destroyed=true',
  `imm-ping ${INVALID_SESSION}`,
  `imm-setNextStreamID ${INVALID_SESSION}`,
  'pending-close',
  'pending-error ERR_HTTP2_INVALID_SESSION/The session has been destroyed',
  `ping ${INVALID_SESSION}`,
  'post-close',
  'post-error ERR_HTTP2_INVALID_SESSION/The session has been destroyed',
  'req-close',
  `req-error ${STREAM_CANCEL}`,
  `setLocalWindowSize ${INVALID_SESSION}`,
  `setNextStreamID ${INVALID_SESSION}`,
  `settings ${INVALID_SESSION}`
];

function beforeOps(lib) {
  return scenario(lib, ({ srv, push, finish, setClient }) => {
    listen(srv).then((port) => {
      const client = lib.connect(`http://127.0.0.1:${port}`);
      setClient(client);
      client.on('error', () => {});
      const req = client.request();
      req.on('error', (e) => push(`req-error ${e.code}/${e.name}/${e.message}`));
      req.on('response', () => push('req-response'));
      req.on('end', () => push('req-end'));
      req.on('close', () => push('req-close'));
      client.destroy();
      throws(push, 'setNextStreamID', () => client.setNextStreamID());
      throws(push, 'setLocalWindowSize', () => client.setLocalWindowSize());
      throws(push, 'ping', () => client.ping());
      throws(push, 'settings', () => client.settings({}));
      throws(push, 'goaway', () => client.goaway());
      // request() after destroy: a stream that errors and closes, not a throw.
      const pending = client.request();
      pending.on('response', () => push('pending-response'));
      pending.on('error', (e) => push(`pending-error ${e.code}/${e.message}`));
      pending.on('close', () => push('pending-close'));
      client.on('close', () => {
        push('cli-close');
        const post = client.request();
        post.on('response', () => push('post-response'));
        post.on('error', (e) => push(`post-error ${e.code}/${e.message}`));
        post.on('close', () => push('post-close'));
      });
      client.close(); // a no-op once destroyed
      // destroy() defers state.destroyed by a setImmediate; the guards hold after it too.
      setImmediate(() => {
        throws(push, 'imm-setNextStreamID', () => client.setNextStreamID());
        throws(push, 'imm-ping', () => client.ping());
        client.close();
        push(`imm destroyed=${client.destroyed}`);
        setTimeout(finish, 200);
      });
      req.resume();
    });
  });
}

test('upstream client-destroy: destroy before operations', async () => {
  const native = await beforeOps(http2);
  const ours = await beforeOps(h2lean);
  assert.deepStrictEqual(native, BEFORE_OPS);
  assert.deepStrictEqual(ours, native);
});

// ---- block 3: the peer destroys its session from 'stream' ----------------------------------

// Only the three stable lines: see the header comment on the rstCode / socket-error race.
const BEFORE_GOAWAY = ['cli-close', 'req-close', 'srv-stream'];

function beforeGoaway(lib) {
  return scenario(lib, ({ srv, push, finish, setClient }) => {
    srv.on('stream', (st) => {
      st.on('error', () => {});
      push('srv-stream');
      st.session.destroy();
    });
    listen(srv).then((port) => {
      const client = lib.connect(`http://127.0.0.1:${port}`);
      setClient(client);
      client.on('error', () => {});
      client.on('close', () => {
        push('cli-close');
        client.destroy(); // upstream: calling destroy in here should not matter
        setTimeout(finish, 150);
      });
      const req = client.request();
      req.on('error', () => {});
      req.on('close', () => push('req-close'));
    });
  });
}

test('upstream client-destroy: destroy before goaway', async () => {
  const native = await beforeGoaway(http2);
  const ours = await beforeGoaway(h2lean);
  assert.deepStrictEqual(native, BEFORE_GOAWAY);
  assert.deepStrictEqual(ours, native);
});

// ---- block 4: req.destroy() before the socket connects -------------------------------------

const BEFORE_CONNECT = ['req-close', 'srv-connection'];

function beforeConnect(lib) {
  return scenario(lib, (ctx) => {
    const { srv, push, finish, setClient } = ctx;
    srv.on('stream', () => push('srv-stream'));
    srv.on('connection', () => {
      push('srv-connection');
      setTimeout(finish, 150);
    });
    listen(srv).then((port) => {
      const client = lib.connect(`http://127.0.0.1:${port}`);
      setClient(client);
      client.on('error', () => {});
      const req = client.request();
      req.on('error', (e) => push(`req-error ${e.code}`));
      req.on('close', () => push('req-close'));
      req.destroy();
    });
  });
}

test('upstream client-destroy: destroy before connect submits nothing', async () => {
  const native = await beforeConnect(http2);
  const ours = await beforeConnect(h2lean);
  assert.deepStrictEqual(native, BEFORE_CONNECT);
  assert.deepStrictEqual(ours, native);
});

// ---- block 5: client.close() before the socket connects ------------------------------------

const CLOSE_BEFORE_CONNECT = ['cli-close', 'req-close', `req-error ${GOAWAY_SESSION}`];

function closeBeforeConnect(lib) {
  return scenario(lib, ({ srv, push, finish, setClient }) => {
    srv.on('stream', () => push('srv-stream'));
    listen(srv).then((port) => {
      const client = lib.connect(`http://127.0.0.1:${port}`);
      setClient(client);
      client.on('error', () => {});
      client.on('close', () => push('cli-close'));
      const req = client.request();
      req.on('error', (e) => push(`req-error ${e.code}/${e.name}/${e.message}`));
      req.on('response', () => push('req-response'));
      req.on('end', () => push('req-end'));
      req.on('close', () => {
        push('req-close');
        setTimeout(finish, 150);
      });
      client.close();
      req.resume();
    });
  });
}

test('upstream client-destroy: close before connect refuses the pending request', async () => {
  const native = await closeBeforeConnect(http2);
  const ours = await closeBeforeConnect(h2lean);
  assert.deepStrictEqual(native, CLOSE_BEFORE_CONNECT);
  assert.deepStrictEqual(ours, native);
});

// ---- blocks 6 and 7: a request AbortSignal, live and already aborted -----------------------

const SIGNAL_LIVE = [
  'aborted listeners=0 aborted=false destroyed=true',
  'cli-close',
  'post listeners=1 aborted=false destroyed=false',
  'pre listeners=0',
  'req-close',
  'req-error ABORT_ERR/AbortError'
];
const SIGNAL_PREABORTED = [
  'cli-close',
  'post listeners=0 aborted=false destroyed=true',
  'pre listeners=0',
  'req-close',
  'req-error ABORT_ERR/AbortError'
];

function requestSignal(lib, preaborted) {
  return scenario(lib, ({ srv, push, finish, setClient }) => {
    srv.on('stream', () => push('srv-stream'));
    listen(srv).then((port) => {
      const controller = new AbortController();
      const { signal } = controller;
      if (preaborted) {
        controller.abort();
      }
      const client = lib.connect(`http://127.0.0.1:${port}`);
      setClient(client);
      client.on('error', () => push(`cli-error listeners=${listenerCount(signal, 'abort')}`));
      client.on('close', () => push('cli-close'));
      push(`pre listeners=${listenerCount(signal, 'abort')}`);
      const req = client.request({}, { signal });
      // An already-aborted signal destroys on the same tick and attaches no listener;
      // a live one attaches exactly one and destroys synchronously when it fires.
      push(`post listeners=${listenerCount(signal, 'abort')} aborted=${req.aborted}`
        + ` destroyed=${req.destroyed}`);
      req.on('error', (e) => push(`req-error ${e.code}/${e.name}`));
      req.on('close', () => {
        push('req-close');
        setTimeout(finish, 150);
      });
      if (!preaborted) {
        controller.abort();
        push(`aborted listeners=${listenerCount(signal, 'abort')} aborted=${req.aborted}`
          + ` destroyed=${req.destroyed}`);
      }
    });
  });
}

test('upstream client-destroy: a live request signal', async () => {
  const native = await requestSignal(http2, false);
  const ours = await requestSignal(h2lean, false);
  assert.deepStrictEqual(native, SIGNAL_LIVE);
  assert.deepStrictEqual(ours, native);
});

test('upstream client-destroy: an already-aborted request signal', async () => {
  const native = await requestSignal(http2, true);
  const ours = await requestSignal(h2lean, true);
  assert.deepStrictEqual(native, SIGNAL_PREABORTED);
  assert.deepStrictEqual(ours, native);
});

// ---- block 8: a session AbortSignal, on both transports ------------------------------------

// The listener belongs to net/tls, not to http2: it is attached by connect() itself, the
// session reports ABORT_ERR, and the request that was already made is cancelled rather than
// aborted -- it never had a signal of its own.
const CONNECT_SIGNAL = [
  'after-connect listeners=1',
  'after-request listeners=1 aborted=false destroyed=false',
  'cli-close',
  'cli-error ABORT_ERR/AbortError listeners=0',
  'post-abort listeners=0 destroyed=false',
  'req-close',
  'req-error ERR_HTTP2_STREAM_CANCEL/Error aborted=false destroyed=true'
];

function connectSignal(lib, secure) {
  return scenario(lib, ({ srv, push, finish, setClient }) => {
    const controller = new AbortController();
    const { signal } = controller;
    srv.on('stream', () => push('srv-stream'));
    listen(srv).then((port) => {
      const url = `${secure ? 'https' : 'http'}://127.0.0.1:${port}`;
      // The certificate is the self-signed loopback fixture test/fixtures-tls.js generates;
      // rejecting it would fail the handshake before the signal is ever exercised.
      const opts = secure ? { signal, rejectUnauthorized: false } : { signal };
      const client = lib.connect(url, opts);
      setClient(client);
      push(`after-connect listeners=${listenerCount(signal, 'abort')}`);
      client.on('close', () => push('cli-close'));
      client.on('error', (e) => push(`cli-error ${e.code}/${e.name}`
        + ` listeners=${listenerCount(signal, 'abort')}`));
      const req = client.request({}, {});
      push(`after-request listeners=${listenerCount(signal, 'abort')}`
        + ` aborted=${req.aborted} destroyed=${req.destroyed}`);
      req.on('error', (e) => push(`req-error ${e.code}/${e.name}`
        + ` aborted=${req.aborted} destroyed=${req.destroyed}`));
      req.on('response', () => push('req-response'));
      req.on('close', () => {
        push('req-close');
        setTimeout(finish, 150);
      });
      controller.abort();
      push(`post-abort listeners=${listenerCount(signal, 'abort')}`
        + ` destroyed=${req.destroyed}`);
    });
  }, secure);
}

test('upstream client-destroy: a session signal over h2c', async () => {
  const native = await connectSignal(http2, false);
  const ours = await connectSignal(h2lean, false);
  assert.deepStrictEqual(native, CONNECT_SIGNAL);
  assert.deepStrictEqual(ours, native);
});

test('upstream client-destroy: a session signal over h2s', async () => {
  const native = await connectSignal(http2, true);
  const ours = await connectSignal(h2lean, true);
  assert.deepStrictEqual(native, CONNECT_SIGNAL);
  assert.deepStrictEqual(ours, native);
});

// ---- block 9: the server aborts the request's signal from its 'stream' handler --------------

// The wire consequence is the point: an AbortError destroys the stream with NGHTTP2_CANCEL,
// so the server's stream closes with rstCode 8 and no 'error' of its own.
const SERVER_ABORT = [
  'cli-close',
  'listeners=1',
  'req-close',
  'req-error ABORT_ERR/AbortError',
  'srv-close 8'
];

function serverAbort(lib) {
  return scenario(lib, ({ srv, push, finish, setClient }) => {
    const controller = new AbortController();
    const { signal } = controller;
    srv.on('stream', (st) => {
      st.on('error', () => push('srv-error'));
      st.on('close', () => push(`srv-close ${st.rstCode}`));
      controller.abort();
    });
    listen(srv).then((port) => {
      const client = lib.connect(`http://127.0.0.1:${port}`);
      setClient(client);
      client.on('error', () => {});
      client.on('close', () => push('cli-close'));
      const req = client.request({}, { signal });
      push(`listeners=${listenerCount(signal, 'abort')}`);
      req.on('error', (e) => {
        push(`req-error ${e.code}/${e.name}`);
        client.close();
      });
      req.on('close', () => {
        push('req-close');
        setTimeout(finish, 200);
      });
    });
  });
}

test('upstream client-destroy: the server aborts the request signal', async () => {
  const native = await serverAbort(http2);
  const ours = await serverAbort(h2lean);
  assert.deepStrictEqual(native, SERVER_ABORT);
  assert.deepStrictEqual(ours, native);
});
