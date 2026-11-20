// The outbound header-block bound on the three shapes test/upstream/exceeds-server-trailer-size.test.js
// does not reach: a CLIENT request HEADERS, a CLIENT trailer HEADERS, and a server
// PUSH_PROMISE. The bound itself -- nghttp2's estimate 12 + 12*fields +
// sum(name.length + value.length), plus 5 for HEADERS and 0 for PUSH_PROMISE, compared
// against maxSendHeaderBlockLength (default 65536) at *prep* time -- is pinned there.
//
// What differs per shape is which stream nghttp2 closes on the prep-error path
// (nghttp2_session.c, session_prep_frame), and that is what these cases measure:
//
//  * a request HEADERS opens the stream, so nghttp2 closes it itself with
//    NGHTTP2_REFUSED_STREAM (7). Nothing reaches the wire -- the server arm sees no
//    stream at all -- and node's deferred close finds the stream already closed, so
//    rstCode stays 7. Measured on v26.12.1: frameError(1, 6, sid),
//    ERR_HTTP2_STREAM_ERROR naming NGHTTP2_REFUSED_STREAM, close 7, session close.
//  * a trailer HEADERS is submitted on a stream that is already open, so nghttp2 closes
//    nothing and node's own deferred close(FRAME_SIZE_ERROR) is what lands: rstCode 6,
//    and RST_STREAM(6) DOES go out -- the peer reports ERR_HTTP2_STREAM_ERROR / 6.
//  * a PUSH_PROMISE rides on the PARENT's id, so 'frameError' lands there (5, 6,
//    parentSid), while nghttp2 closes the PROMISED stream with NGHTTP2_INTERNAL_ERROR
//    (2). The parent's own response still goes out and closes it with NO_ERROR, and the
//    client never sees a push. The pushStream callback still runs with no error: node
//    hands it to process.nextTick (core.js:3048) and the send loop refuses the frame
//    after that tick, which is the order pinned here.
//
// The GOAWAY echo is deliberately not recorded: whether the peer's GOAWAY arrives before
// the local session finishes closing is a race both stacks scheduling differs on
// (CONTRIBUTING.md §2), and node's server sends two of them where h2lean sends one.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const h2lean = require('../index');
const { listen, closeServer } = require('./diff-harness');

const REFUSED = 'ERR_HTTP2_STREAM_ERROR:Stream closed with error code NGHTTP2_REFUSED_STREAM';
const SIZE_ERROR = 'ERR_HTTP2_STREAM_ERROR:Stream closed with error code NGHTTP2_FRAME_SIZE_ERROR';
const INTERNAL = 'ERR_HTTP2_STREAM_ERROR:Stream closed with error code NGHTTP2_INTERNAL_ERROR';

// The shared recording shell: a sorted line set, a gate that stops recording once the
// exchange is over (the teardown closes sessions a served exchange would otherwise
// report), and a deadline that turns a transcript that never arrives into a line.
function recorder() {
  const t = [];
  const state = { client: undefined, srv: undefined };
  let recording = true;
  let finished = false;
  let resolve;
  const done = new Promise((r) => {
    resolve = r;
  });
  const push = (line) => {
    if (recording) {
      t.push(line);
    }
  };
  const finish = () => {
    if (finished) {
      return;
    }
    finished = true;
    recording = false;
    clearTimeout(timer);
    if (state.client) {
      state.client.destroy();
    }
    closeServer(state.srv).then(() => resolve(t.sort()));
  };
  const start = (srv) => {
    state.srv = srv;
    timer = setTimeout(() => {
      push('HANG');
      finish();
    }, 8000);
  };
  let timer;
  return { push, finish, start, state, done };
}

// A client request whose block estimate is 120+n: the four pseudo-headers node fills
// (:method GET, :scheme http, :path /, and an :authority the application pins to one
// character so the listening port's digit count cannot move the line) plus 'x-big'.
// 12 + 5*12 + (10+11+11+6+5+n) + 5 = 120+n.
function requestArm(lib, n, bound) {
  const opts = bound === undefined ? {} : { maxSendHeaderBlockLength: bound };
  const { push, finish, start, state, done } = recorder();
  let client;
  // The inbound list limit is raised so the default outbound boundary is reachable: a
  // five-field request costs 203+n in HPACK entry octets against an estimate of 120+n,
  // so at the default 65536 the server would answer ENHANCE_YOUR_CALM first.
  const srv = lib.createServer({ settings: { maxHeaderListSize: 1 << 20 } });
  start(srv);
  srv.on('error', () => {});
  srv.on('session', (s) => {
    s.on('error', () => {});
    s.on('close', () => push(`sess-close ${s.goawayCode}`));
  });
  srv.on('stream', (st) => {
    st.on('error', (e) => push(`srv-error ${e.code}`));
    st.on('close', () => push(`srv-close ${st.rstCode}`));
    st.resume();
    st.respond({ ':status': 200 });
    st.end('ok');
  });
  listen(srv).then((port) => {
    client = lib.connect(`http://127.0.0.1:${port}`, opts);
    state.client = client;
    client.on('error', () => {});
    client.on('close', () => push('cli-sess-close'));
    const req = client.request({ ':path': '/', ':authority': 'a', 'x-big': 'X'.repeat(n) });
    req.on('response', () => push('response'));
    req.on('error', (e) => push(`error ${e.code}:${e.message}`));
    req.on('frameError', (a, b, c) => push(`frameError ${a}/${b}/${c}`));
    req.on('end', () => push('end'));
    req.on('close', () => {
      push(`close ${req.rstCode}`);
      setTimeout(finish, 150);
    });
    req.resume();
    req.end();
  });
  return done;
}

// A client trailer block whose estimate is 34+n: one field, 'x-big'. The request is a
// POST with a body, because node ends a GET at its HEADERS (isPayloadMeaningless,
// core.js:1896) and 'wantTrailers' never fires for one.
function trailerArm(lib, n, bound) {
  const opts = bound === undefined ? {} : { maxSendHeaderBlockLength: bound };
  const { push, finish, start, state, done } = recorder();
  let client;
  const srv = lib.createServer();
  start(srv);
  srv.on('error', () => {});
  srv.on('session', (s) => {
    s.on('error', () => {});
    s.on('close', () => push(`sess-close ${s.goawayCode}`));
  });
  srv.on('stream', (st) => {
    st.on('error', (e) => push(`srv-error ${e.code}`));
    st.on('close', () => push(`srv-close ${st.rstCode}`));
    st.resume();
    st.on('end', () => {
      st.respond({ ':status': 200 }, { waitForTrailers: true });
      st.on('wantTrailers', () => st.sendTrailers({ 'x-small': 'ok' }));
      st.end('body');
    });
  });
  listen(srv).then((port) => {
    client = lib.connect(`http://127.0.0.1:${port}`, opts);
    state.client = client;
    client.on('error', () => {});
    client.on('close', () => push('cli-sess-close'));
    const req = client.request({ ':path': '/', ':method': 'POST' }, { waitForTrailers: true });
    req.on('response', () => push('response'));
    req.on('error', (e) => push(`error ${e.code}:${e.message}`));
    req.on('frameError', (a, b, c) => push(`frameError ${a}/${b}/${c}`));
    req.on('trailers', () => push('trailers'));
    req.on('end', () => push('end'));
    req.on('close', () => {
      push(`close ${req.rstCode}`);
      setTimeout(finish, 150);
    });
    req.on('wantTrailers', () => req.sendTrailers({ 'x-big': 'X'.repeat(n) }));
    req.resume();
    req.end('body');
  });
  return done;
}

// A server PUSH_PROMISE whose estimate is 121+n: the same four pseudo-headers (node
// fills :method/:scheme and takes :authority from the parent request, which the client
// pinned to 'a') plus ':path' '/pushed' and 'x-big', and no priority spec.
// 12 + 5*12 + (10+11+11+12+5+n) = 121+n.
function pushArm(lib, n, bound) {
  const opts = bound === undefined ? {} : { maxSendHeaderBlockLength: bound };
  const { push, finish, start, state, done } = recorder();
  let client;
  const srv = lib.createServer(opts);
  start(srv);
  srv.on('error', () => {});
  srv.on('session', (s) => {
    s.on('error', () => {});
    s.on('close', () => push(`sess-close ${s.goawayCode}`));
  });
  srv.on('stream', (st) => {
    st.on('error', (e) => push(`srv-error ${e.code}`));
    st.on('frameError', (a, b, c) => push(`srv-frameError ${a}/${b}/${c}`));
    st.on('close', () => push(`srv-close ${st.rstCode}`));
    st.resume();
    st.respond({ ':status': 200 });
    st.pushStream({ ':path': '/pushed', 'x-big': 'X'.repeat(n) }, (err, pushed) => {
      push(`push-cb ${err ? err.code : 'ok'}`);
      if (pushed) {
        pushed.on('error', (e) => push(`push-error ${e.code}:${e.message}`));
        pushed.on('close', () => push(`push-close ${pushed.rstCode}`));
        pushed.respond({ ':status': 200 });
        pushed.end('pushed');
      }
    });
    st.end('parent');
  });
  listen(srv).then((port) => {
    client = lib.connect(`http://127.0.0.1:${port}`);
    state.client = client;
    client.on('error', () => {});
    client.on('close', () => push('cli-sess-close'));
    client.on('stream', (pushed) => {
      push('cli-push-stream');
      pushed.on('error', (e) => push(`cli-push-error ${e.code}`));
      pushed.on('close', () => push(`cli-push-close ${pushed.rstCode}`));
      pushed.resume();
    });
    const req = client.request({ ':path': '/', ':authority': 'a' });
    req.on('response', () => push('response'));
    req.on('error', (e) => push(`error ${e.code}`));
    req.on('end', () => push('end'));
    req.on('close', () => {
      push(`close ${req.rstCode}`);
      setTimeout(finish, 150);
    });
    req.resume();
    req.end();
  });
  return done;
}

// node's measured transcripts, so a change in either stack is a decision, not a drift.
const REQ_REFUSED = [
  'cli-sess-close',
  `error ${REFUSED}`,
  'frameError 1/6/1',
  'close 7',
  'sess-close 0'
].sort();
const REQ_SERVED = ['close 0', 'end', 'response', 'srv-close 0'].sort();

const TRAILER_REFUSED = [
  'cli-sess-close',
  `error ${SIZE_ERROR}`,
  'frameError 1/6/1',
  'close 6',
  'sess-close 0',
  'srv-close 6',
  'srv-error ERR_HTTP2_STREAM_ERROR'
].sort();
const TRAILER_SERVED = ['close 0', 'end', 'response', 'srv-close 0', 'trailers'].sort();

const PUSH_REFUSED = [
  'cli-sess-close',
  'close 0',
  'end',
  `push-error ${INTERNAL}`,
  'push-close 2',
  'push-cb ok',
  'response',
  'sess-close 0',
  'srv-close 0',
  'srv-frameError 5/6/1'
].sort();
const PUSH_SERVED = [
  'cli-push-close 0',
  'cli-push-stream',
  'close 0',
  'end',
  'push-close 0',
  'push-cb ok',
  'response',
  'srv-close 0'
].sort();

test('a client request HEADERS over the bound: REFUSED_STREAM, nothing on the wire', async () => {
  const native = await requestArm(http2, 81, 200);
  const ours = await requestArm(h2lean, 81, 200);
  assert.deepStrictEqual(native, REQ_REFUSED);
  assert.deepStrictEqual(ours, native);
});

test('the client request bound is the same estimate: 120+n for a pinned authority', async () => {
  for (const [n, bound, want] of [[80, 200, REQ_SERVED], [81, 200, REQ_REFUSED],
    [65416, undefined, REQ_SERVED], [65417, undefined, REQ_REFUSED]]) {
    const native = await requestArm(http2, n, bound);
    const ours = await requestArm(h2lean, n, bound);
    assert.deepStrictEqual(native, want, `node n=${n} bound=${bound}`);
    assert.deepStrictEqual(ours, native, `h2lean n=${n} bound=${bound}`);
  }
});

test('a client trailer HEADERS over the bound: FRAME_SIZE_ERROR, and the RST goes out', async () => {
  const native = await trailerArm(http2, 117, 150);
  const ours = await trailerArm(h2lean, 117, 150);
  assert.deepStrictEqual(native, TRAILER_REFUSED);
  assert.deepStrictEqual(ours, native);
});

test('the client trailer bound: 34+n, and the served side still delivers trailers', async () => {
  const native = await trailerArm(http2, 116, 150);
  const ours = await trailerArm(h2lean, 116, 150);
  assert.deepStrictEqual(native, TRAILER_SERVED);
  assert.deepStrictEqual(ours, native);
});

test('a PUSH_PROMISE over the bound: frameError on the parent, INTERNAL_ERROR on the push', async () => {
  const native = await pushArm(http2, 30, 150);
  const ours = await pushArm(h2lean, 30, 150);
  assert.deepStrictEqual(native, PUSH_REFUSED);
  assert.deepStrictEqual(ours, native);
});

test('the PUSH_PROMISE bound carries no priority spec: 121+n', async () => {
  for (const [n, bound, want] of [[29, 150, PUSH_SERVED], [30, 150, PUSH_REFUSED],
    [65415, undefined, PUSH_SERVED], [65416, undefined, PUSH_REFUSED]]) {
    const native = await pushArm(http2, n, bound);
    const ours = await pushArm(h2lean, n, bound);
    assert.deepStrictEqual(native, want, `node n=${n} bound=${bound}`);
    assert.deepStrictEqual(ours, native, `h2lean n=${n} bound=${bound}`);
  }
});

test('the client takes maxSendHeaderBlockLength as a number and coerces it', async () => {
  // node's updateOptionsBuffer (internal/http2/util.js:249) gates on typeof alone and
  // writes a Uint32 slot: 200.9 bounds like 200, and a non-number is ignored entirely.
  for (const [bound, want] of [[200.9, REQ_REFUSED], ['200', REQ_SERVED], [null, REQ_SERVED]]) {
    const native = await requestArm(http2, 81, bound);
    const ours = await requestArm(h2lean, 81, bound);
    assert.deepStrictEqual(native, want, `node bound=${JSON.stringify(bound)}`);
    assert.deepStrictEqual(ours, native, `h2lean bound=${JSON.stringify(bound)}`);
  }
});
