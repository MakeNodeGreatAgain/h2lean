// Ports of node's own upstream push tests (test/parallel/test-http2-* files that
// drive pushStream / createPushResponse / captureRejections). Each port runs the
// SAME assertions against both stacks: h2lean's server in one arm and node's
// (internal/http2/core) server in the other, with the opposing stack's client as
// the peer, so a failure names an implementation gap and not a test artefact.
// The upstream sources are the test/parallel files of nodejs/node tagged
// v26.12.1; each test names the upstream file it ports.
//
// Adaptations, each marked at its test:
//   * upstream's common/countdown is inlined (one promise helper for all ports);
//   * test-http2-respond-file-push needs respondWithFD, which h2lean does not
//     implement; the port pushes respond()+end(data) and keeps the body-integrity
//     half of the check (the fd/last-modified halves are fd-serving specifics);
//   * test-http2-server-push-stream-errors mocks internalBinding('http2') under
//     --expose-internals to pin nghttp2 error codes -- no h2lean equivalent, not
//     ported; test-http2-max-session-memory-* pins maxSessionMemory, an nghttp2
//     memory-accounting knob h2lean does not implement, likewise not ported;
//   * common.expectWarning(DEP0194) cannot be captured per-arm inside one
//     process, so the session-destroy port asserts the observable behaviour
//     (priority() answers ERR_HTTP2_INVALID_STREAM once the session is gone) and
//     never touches the deprecated accessor;
//   * upstream reads the Writable-internal push._writableState.ended off a HEAD
//     push; h2lean's ServerHttp2Stream is an EventEmitter by design (no writable
//     state object), so the port pins the end-at-creation fact through each
//     stack's own marker and the wire-visible consequence (write refuses with
//     ERR_STREAM_WRITE_AFTER_END), which both arms must agree on;
//   * upstream's file-push calls pushStream AFTER the parent's end(). Whether
//     that is legal rides on when closed/destroyed turn true -- the exact
//     teardown divergence AGENTS.md §10 and test/server-push.test.js already
//     record (node tears a stream down only once the peer's half ends; h2lean
//     finishes the local state with the local end). The port pushes before the
//     parent's end(), which is the same wire sequence either way.
const test = require('node:test');
const assert = require('node:assert');
const events = require('node:events');
const nodeHttp2 = require('node:http2');
const h2lean = require('../index');

function listen(server) {
  return new Promise((resolve, reject) => {
    server.listen(0, '127.0.0.1', () => resolve(server.address().port));
    server.once('error', reject);
  });
}

// A client whose error events are swallowed: every port below ends connections
// through GOAWAY/RST races and an unhandled session 'error' is noise, not signal.
function connect(impl, port, opts) {
  const client = impl.connect(`http://127.0.0.1:${port}`, opts);
  client.on('error', () => {});
  return client;
}

// Ends a test even when one arm hangs: the upstream files rely on the test
// runner's process timeout, which here would kill every other test in the file.
// The watchdog timer is unref'd on purpose -- the harness timeout is the backstop
// for the test itself; this race only turns a hang into a reported failure.
function withTimeout(promise, ms, label) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timeout: ${label}`)), ms);
    timer.unref();
    promise.then(resolve, reject);
  });
}

// wait-event helper for the streams whose 'close' ends a scenario
function once(emitter, name) {
  return new Promise((resolve) => emitter.once(name, resolve));
}

// A broken liveness guard in either arm can leave a listening socket or session
// ref'd after the tests settle -- the exact failure class test/diff-harness.js
// documents ("a broken close hangs the test FILE instead of failing a test").
// The results are complete when the last test finishes, so a ref'd timer turns
// any such leak into a bounded wait with the full report flushed.
let settled = 0;
test.after(() => {
  clearTimeout(settled);
  settled = setTimeout(() => process.exit(process.exitCode || 0), 2000);
});

// one push round trip: `serverImpl` answers '/' with one pushStream (the handler
// decides the promise headers and the pushed body), the peer client collects the
// transcript. Resolves once the main response ended and a grace period let push
// events land -- the same 80ms window test/server-push.test.js pins.
function pushRoundTrip(serverImpl, peerImpl, opts) {
  /* eslint-disable no-async-promise-executor -- the executor awaits the peer
     transcript and rejects from both the push callback and the await, which
     keeps one resolve/reject pair for the whole round trip */
  return new Promise(async (resolveMain, rejectMain) => {
    const { path = '/', pushPath = '/foobar', pushBody = 'pushed by server data',
      mainBody = 'test', respondHeaders = { 'content-type': 'text/html', ':status': 200 } } =
      opts || {};
    const srv = serverImpl.createServer();
    const pushes = [];
    srv.on('stream', (stream, headers) => {
      if (headers[':path'] === path) {
        stream.pushStream({
          ':scheme': 'http', ':path': pushPath,
          ':authority': `localhost:${srv.address().port}`
        }, (err, push) => {
          if (err) {
            return rejectMain(err);
          }
          push.respond(respondHeaders);
          if (pushBody !== null) {
            push.end(pushBody);
          }
          const entry = { id: push.id, events: [] };
          pushes.push(entry);
          push.on('aborted', () => entry.events.push('aborted'));
          push.on('error', (e) => entry.events.push(`error:${e.code}`));
        });
      }
      stream.respond(respondHeaders);
      if (mainBody !== null) {
        stream.end(mainBody);
      }
    });
    const port = await listen(srv);
    const client = connect(peerImpl, port);
    const streamEvents = [];
    client.on('stream', (stream, promiseHeaders, flags) => {
      streamEvents.push({
        id: stream.id,
        promiseHeaders: { ...promiseHeaders },
        flags,
        events: []
      });
      const entry = streamEvents[streamEvents.length - 1];
      stream.on('push', (h, f) => entry.events.push(['push', { ...h }, f]));
      stream.on('response', (h) => entry.events.push(['response', { ...h }]));
      stream.setEncoding('utf8');
      const chunks = [];
      stream.on('data', (c) => chunks.push(c));
      stream.on('end', () => entry.events.push(['end', chunks.join('')]));
      stream.on('close', () => entry.events.push(['close']));
      stream.on('error', (e) => entry.events.push(['error', e.code]));
      stream.on('aborted', () => entry.events.push(['aborted']));
      stream.resume();
    });
    const req = client.request({ ':path': path });
    let mainHeaders = null;
    const mainChunks = [];
    req.on('response', (h) => {
      mainHeaders = { ...h };
    });
    req.setEncoding('utf8');
    req.on('data', (c) => mainChunks.push(c));
    req.on('error', rejectMain);
    req.end();
    const finish = () => {
      client.destroy();
      srv.close();
      for (const s of srv.sessions || []) {
        s.destroy();
      }
      resolveMain({
        main: { headers: mainHeaders, body: mainChunks.join('') },
        streamEvents, pushes
      });
    };
    req.on('end', () => setTimeout(finish, 80));
    // a lost 'end' shows up as the caller's timeout failure, not a hanging file
  });
  /* eslint-enable no-async-promise-executor */
}

// ---- test-http2-server-push-stream.js --------------------------------------
// pushStream happy path: the client session fires 'stream' for the promise
// (id even, promise headers verbatim), the pushed stream answers 'push' (not
// 'response') with the responder's headers, and the main body arrives whole.
// The nested-push half of the upstream file is its own test below.
async function upstreamPushStream(impl, peer) {
  const t = await pushRoundTrip(impl, peer, {
    respondHeaders: { 'content-type': 'text/html', ':status': 200,
      'x-push-data': 'pushed by server' }
  });
  assert.strictEqual(t.main.headers[':status'], 200);
  assert.strictEqual(t.main.body, 'test');
  assert.strictEqual(t.streamEvents.length, 1);
  const ev = t.streamEvents[0];
  assert.strictEqual(ev.id % 2, 0); // server-initiated: even id
  assert.strictEqual(ev.promiseHeaders[':scheme'], 'http');
  assert.strictEqual(ev.promiseHeaders[':path'], '/foobar');
  assert.strictEqual(ev.events.length, 3, `push events: ${JSON.stringify(ev.events)}`);
  const [pushE, endE, closeE] = ev.events;
  assert.strictEqual(pushE[0], 'push');
  assert.strictEqual(pushE[1][':status'], 200);
  assert.strictEqual(pushE[1]['content-type'], 'text/html');
  assert.strictEqual(pushE[1]['x-push-data'], 'pushed by server');
  assert.strictEqual(typeof pushE[2], 'number'); // flags
  assert.deepStrictEqual(endE, ['end', 'pushed by server data']);
  assert.deepStrictEqual(closeE, ['close']);
}

test('upstream server-push-stream: h2lean server, node client', async () => {
  await withTimeout(upstreamPushStream(h2lean, nodeHttp2), 10000, 'h2lean srv');
});

test('upstream server-push-stream: node server, h2lean client', async () => {
  await withTimeout(upstreamPushStream(nodeHttp2, h2lean), 10000, 'node srv');
});

test('upstream pushStream: nested push throws ERR_HTTP2_NESTED_PUSH', async () => {
  for (const impl of [h2lean, nodeHttp2]) {
    const srv = impl.createServer();
    let caught = null;
    srv.on('stream', (stream) => {
      stream.pushStream({ ':path': '/child' }, (err, push) => {
        assert.ifError(err);
        push.respond();
        // nested while the push stream is still open: both stacks must name
        // the nesting, not the permission. After push.end() the answer depends
        // on when the stream's closed/destroyed getters flip -- the teardown
        // divergence of AGENTS.md §10 (node keeps pushAllowed true until the
        // peer ends its half, h2lean ends the local state with the local end),
        // measured side by side on live node.
        try {
          push.pushStream({}, () => {});
        } catch (e) {
          caught = e;
        }
        push.end('child');
        stream.end('done');
      });
      stream.respond();
    });
    const port = await listen(srv);
    const client = connect(nodeHttp2, port);
    const req = client.request();
    req.resume();
    await withTimeout(once(req, 'close'), 8000, `nested ${caught}`);
    assert.strictEqual(caught.code, 'ERR_HTTP2_NESTED_PUSH');
    assert.strictEqual(caught.name, 'Error');
    client.destroy();
    srv.close();
    for (const s of srv.sessions || []) {
      s.destroy();
    }
  }
});

// ---- test-http2-server-push-stream-errors-args.js --------------------------
// pushStream argument/header validation happens synchronously, before any
// frame leaves: callback type first, then the connection-header rule.
test('upstream pushStream argument validation', async () => {
  for (const impl of [h2lean, nodeHttp2]) {
    const srv = impl.createServer();
    const seen = [];
    srv.on('stream', (stream) => {
      try {
        stream.pushStream({ ':scheme': 'http', ':path': '/foobar' }, {}, 'callback');
      } catch (e) {
        seen.push(['args', e.code, e.name]);
      }
      try {
        stream.pushStream({ connection: 'test' }, {}, () => {});
      } catch (e) {
        seen.push(['conn', e.code, e.name, e.message]);
      }
      stream.end('test');
    });
    const port = await listen(srv);
    const client = connect(nodeHttp2, port);
    const req = client.request({ ':path': '/' });
    let body = '';
    req.setEncoding('utf8');
    req.on('data', (d) => {
      body += d;
    });
    await withTimeout(once(req, 'end'), 8000, 'arg-validation end');
    assert.strictEqual(body, 'test');
    assert.deepStrictEqual(seen[0], ['args', 'ERR_INVALID_ARG_TYPE', 'TypeError']);
    assert.deepStrictEqual(seen[1], ['conn', 'ERR_HTTP2_INVALID_CONNECTION_HEADERS',
      'TypeError', 'HTTP/1 Connection specific headers are forbidden: "connection"']);
    client.destroy();
    srv.close();
    for (const s of srv.sessions || []) {
      s.destroy();
    }
  }
});

// ---- test-http2-server-push-stream-head.js ---------------------------------
// A HEAD promise ends the push stream at creation: nothing may be written to
// it afterwards (ERR_STREAM_WRITE_AFTER_END surfaces on the stream, the way a
// Writable reports it), and the client sees push + end with no data.
test('upstream HEAD push: write after end', async () => {
  for (const impl of [h2lean, nodeHttp2]) {
    const srv = impl.createServer();
    let pushEnded = null;
    let writeAfterEnd = null;
    let writeReturn = null;
    srv.on('stream', (stream, _headers) => {
      stream.pushStream({
        ':scheme': 'http', ':method': 'HEAD', ':path': '/',
        ':authority': `localhost:${srv.address().port}`
      }, (err, push) => {
        assert.ifError(err);
        // the end-at-creation marker: node's is the Writable's own state object,
        // h2lean's is the _endQueued flag that plays the same role on its
        // EventEmitter stream (both are internal names for one wire fact)
        pushEnded = push._writableState ? push._writableState.ended : push._endQueued;
        push.respond();
        push.on('error', (e) => {
          writeAfterEnd = [e.name, e.code, e.message];
        });
        writeReturn = push.write('test');
        stream.end('test');
      });
      stream.respond({ 'content-type': 'text/html', ':status': 200 });
    });
    const port = await listen(srv);
    const client = connect(nodeHttp2, port);
    const seen = [];
    client.on('stream', (stream, headers) => {
      seen.push(headers[':method']);
      stream.on('push', () => {
        stream.on('data', () => seen.push('DATA'));
        stream.on('end', () => seen.push('END'));
      });
    });
    const req = client.request();
    let body = '';
    req.setEncoding('utf8');
    req.on('data', (d) => {
      body += d;
    });
    await withTimeout(once(req, 'end'), 8000, 'HEAD push end');
    assert.strictEqual(body, 'test');
    assert.strictEqual(pushEnded, true);
    assert.strictEqual(writeReturn, false);
    assert.deepStrictEqual(writeAfterEnd,
      ['Error', 'ERR_STREAM_WRITE_AFTER_END', 'write after end']);
    await new Promise((r) => setTimeout(r, 80));
    assert.deepStrictEqual(seen, ['HEAD', 'END']);
    client.destroy();
    srv.close();
    for (const s of srv.sessions || []) {
      s.destroy();
    }
  }
});

// ---- test-http2-server-push-disabled.js ------------------------------------
// enablePush:false in the client SETTINGS reaches the server as
// remoteSettings.enablePush === false; pushAllowed is false and pushStream
// throws ERR_HTTP2_PUSH_DISABLED synchronously (the callback must not fire).
test('upstream push disabled by client settings', async () => {
  for (const impl of [h2lean, nodeHttp2]) {
    const srv = impl.createServer();
    let remoteEnablePush = null;
    let pushAllowed = null;
    let threw = null;
    let callbackFired = false;
    srv.on('session', (session) => {
      session.on('remoteSettings', (settings) => {
        if (settings.enablePush === false) {
          remoteEnablePush = false;
        }
      });
    });
    srv.on('stream', (stream) => {
      pushAllowed = stream.pushAllowed;
      try {
        stream.pushStream({ ':scheme': 'http', ':path': '/foobar' },
          () => {
            callbackFired = true;
          });
      } catch (e) {
        threw = e.code;
      }
      stream.respond({ ':status': 200 });
      stream.end('test');
    });
    const port = await listen(srv);
    const client = connect(nodeHttp2, port, { settings: { enablePush: false } });
    const req = client.request({ ':path': '/' });
    client.on('stream', () => {
      throw new Error('push arrived although disabled');
    });
    let body = '';
    req.setEncoding('utf8');
    req.on('data', (d) => {
      body += d;
    });
    await withTimeout(once(req, 'end'), 8000, 'push-disabled end');
    assert.strictEqual(body, 'test');
    assert.strictEqual(remoteEnablePush, false);
    assert.strictEqual(pushAllowed, false);
    assert.strictEqual(threw, 'ERR_HTTP2_PUSH_DISABLED');
    assert.strictEqual(callbackFired, false);
    client.destroy();
    srv.close();
    for (const s of srv.sessions || []) {
      s.destroy();
    }
  }
});

// ---- test-http2-server-stream-session-destroy.js ---------------------------
// session.destroy() inside the stream handler: the stream detaches from the
// session (undefined getter), state reads as {}, and every operation answers
// ERR_HTTP2_INVALID_STREAM. pushStream reports PUSH_DISABLED (node checks the
// permission flag before the liveness). The body still reaches the client and
// both halves close. The deprecated stream.priority accessor is not touched --
// DEP0194 is process-global and cannot be asserted per-arm in one process.
test('upstream session destroy detaches the stream', async () => {
  for (const impl of [h2lean, nodeHttp2]) {
    const srv = impl.createServer();
    const checks = [];
    srv.on('stream', (stream) => {
      stream.session.destroy();
      checks.push(['session', stream.session]);
      checks.push(['state', JSON.stringify(stream.state)]);
      const code = (fn) => {
        try {
          fn(); return 'no-throw';
        } catch (e) {
          return e.code;
        }
      };
      checks.push(['additionalHeaders', code(() => stream.additionalHeaders())]);
      checks.push(['respond', code(() => stream.respond())]);
      checks.push(['pushStream', code(() => stream.pushStream({}, () => {}))]);
      stream.on('error', () => checks.push(['stream-error', 'fired']));
      stream.write('data', (e) => checks.push(['write-cb', e && e.code]));
      stream.on('close', () => checks.push(['close', true]));
    });
    const port = await listen(srv);
    const client = connect(nodeHttp2, port);
    const req = client.request();
    req.resume();
    req.on('error', () => {});
    await withTimeout(Promise.all([once(req, 'close'), once(req, 'end')
      .catch(() => 'no-end')]), 8000, 'session-destroy close');
    const map = Object.fromEntries(checks.filter((c) => c[0] !== 'close'));
    assert.strictEqual(map.session, undefined);
    assert.strictEqual(map.state, '{}');
    assert.strictEqual(map.additionalHeaders, 'ERR_HTTP2_INVALID_STREAM');
    assert.strictEqual(map.respond, 'ERR_HTTP2_INVALID_STREAM');
    assert.strictEqual(map.pushStream, 'ERR_HTTP2_PUSH_DISABLED');
    assert.strictEqual(map['stream-error'], undefined, 'no stream error after destroy');
    assert.strictEqual(map['write-cb'], 'ERR_STREAM_WRITE_AFTER_END');
    client.destroy();
    srv.close();
    for (const s of srv.sessions || []) {
      s.destroy();
    }
  }
});

// ---- test-http2-options-max-reserved-streams.js ----------------------------
// maxReservedRemoteStreams is the CLIENT's budget for server pushes: the first
// push lives, the second is refused with RST_STREAM(CANCEL = 8) before its
// 'stream' event can matter; the session 'stream' event fires once only.
test('upstream maxReservedRemoteStreams refuses the second push', async () => {
  for (const clientImpl of [nodeHttp2, h2lean]) {
    const srv = h2lean.createServer();
    srv.on('stream', (stream) => {
      stream.pushStream({ ':path': '/foobar' }, (err, pushed) => {
        assert.ifError(err);
        pushed.respond();
        pushed.end();
      });
      stream.pushStream({ ':path': '/foobar' }, (err, pushed) => {
        assert.ifError(err);
        pushed.respond();
        pushed.resume();
      });
      stream.respond();
      stream.end('hello world');
    });
    const port = await listen(srv);
    const client = connect(clientImpl, port, { maxReservedRemoteStreams: 1 });
    const streamEvents = [];
    client.on('stream', (stream) => {
      streamEvents.push(stream.id);
      stream.on('error', () => {});
      stream.resume();
    });
    const req = client.request();
    req.resume();
    req.on('error', () => {});
    await withTimeout(once(req, 'close'), 8000, 'max-reserved close');
    await new Promise((r) => setTimeout(r, 100));
    assert.strictEqual(streamEvents.length, 1,
      `only the first push may reach the session (saw ${streamEvents.length})`);
    assert.strictEqual(req.rstCode, 0); // the request itself was answered normally
    client.destroy();
    srv.close();
    for (const s of srv.sessions || []) {
      s.destroy();
    }
  }
});

// ---- test-http2-compat-serverresponse-createpushresponse.js ----------------
// compat (req, res) surface: res.createPushResponse pushes with the parent's
// authority, its callback gets (null err, pushRes); after the parent stream
// closed the same call answers ERR_HTTP2_INVALID_STREAM through the callback,
// and a non-function callback throws ERR_INVALID_ARG_TYPE synchronously.
test('upstream compat createPushResponse', async () => {
  const pushExpect = 'This is a server-initiated response';
  const servExpect = 'This is a client-initiated response';
  for (const impl of [h2lean, nodeHttp2]) {
    const srv = impl.createServer((request, response) => {
      assert.strictEqual(response.stream.id % 2, 1);
      response.write(servExpect);
      let typeThrew = null;
      try {
        response.createPushResponse({ ':path': '/pushed', ':method': 'GET' }, undefined);
      } catch (e) {
        typeThrew = [e.code, e.name];
      }
      assert.deepStrictEqual(typeThrew, ['ERR_INVALID_ARG_TYPE', 'TypeError']);
      response.stream.on('close', () => {
        response.createPushResponse({ ':path': '/pushed', ':method': 'GET' },
          (error) => {
            assert.strictEqual(error.code, 'ERR_HTTP2_INVALID_STREAM');
          });
      });
      response.createPushResponse({ ':path': '/pushed', ':method': 'GET' },
        (error, push) => {
          assert.ifError(error);
          assert.strictEqual(push.stream.id % 2, 0);
          push.end(pushExpect);
          response.end();
        });
    });
    const port = await listen(srv);
    const client = connect(nodeHttp2, port);
    const req = client.request({ ':path': '/', ':method': 'GET' });
    const pushed = [];
    let pushBody = '';
    let pushStatus = null;
    client.on('stream', (pushStream, headers) => {
      pushed.push({
        path: headers[':path'], method: headers[':method'],
        scheme: headers[':scheme']
      });
      pushStream.on('push', (h) => {
        pushStatus = h[':status'];
      });
      pushStream.setEncoding('utf8');
      pushStream.on('data', (c) => {
        pushBody += c;
      });
      pushStream.on('error', () => {});
    });
    let mainStatus = null;
    req.on('response', (h) => {
      mainStatus = h[':status'];
    });
    let mainBody = '';
    req.setEncoding('utf8');
    req.on('data', (c) => {
      mainBody += c;
    });
    req.on('error', () => {});
    await withTimeout(once(req, 'end'), 8000, 'createPushResponse end');
    await new Promise((r) => setTimeout(r, 100));
    assert.strictEqual(mainStatus, 200);
    assert.strictEqual(mainBody, servExpect);
    assert.deepStrictEqual(pushed, [{ path: '/pushed', method: 'GET', scheme: 'http' }]);
    assert.strictEqual(pushStatus, 200);
    assert.strictEqual(pushBody, pushExpect);
    client.destroy();
    srv.close();
    for (const s of srv.sessions || []) {
      s.destroy();
    }
  }
});

// ---- test-http2-client-data-end.js ------------------------------------------
// A push body split over write() + a setImmediate end() must arrive whole,
// interleaved with the main response ('te' + 'st' from the parent).
test('upstream push body split across write/end', async () => {
  for (const impl of [h2lean, nodeHttp2]) {
    const srv = impl.createServer();
    srv.on('stream', (stream, headers) => {
      if (headers[':path'] === '/') {
        stream.pushStream({ ':path': '/foobar' }, (err, push) => {
          assert.ifError(err);
          push.respond({ 'content-type': 'text/html', 'x-push-data': 'pushed by server' });
          push.write('pushed by server ');
          setImmediate(() => push.end('data'));
          stream.end('st');
        });
      }
      stream.respond({ 'content-type': 'text/html' });
      stream.write('te');
    });
    const port = await listen(srv);
    const client = connect(nodeHttp2, port);
    const req = client.request();
    let pushData = '';
    let pushStatus = null;
    const waiters = [];
    client.on('stream', (stream) => {
      stream.on('push', (h) => {
        pushStatus = h;
      });
      stream.setEncoding('utf8');
      stream.on('data', (d) => {
        pushData += d;
      });
      stream.on('error', () => {});
      waiters.push(once(stream, 'close'));
    });
    let data = '';
    req.setEncoding('utf8');
    req.on('data', (d) => {
      data += d;
    });
    req.on('error', () => {});
    waiters.push(once(req, 'close'));
    await withTimeout(once(req, 'end').then(() => Promise.all(waiters)), 8000, 'split push');
    if (pushStatus) {
      assert.strictEqual(pushStatus[':status'], 200);
      assert.strictEqual(pushStatus['content-type'], 'text/html');
      assert.strictEqual(pushStatus['x-push-data'], 'pushed by server');
    }
    assert.strictEqual(pushData, 'pushed by server data');
    assert.strictEqual(data, 'test');
    client.destroy();
    srv.close();
    for (const s of srv.sessions || []) {
      s.destroy();
    }
  }
});

// ---- test-http2-dont-lose-data.js -------------------------------------------
// The push body must be readable even though the client deferred attaching its
// data handlers to the parent until the push had fully ended (upstream comment:
// removing the setImmediate makes the old bug disappear).
test('upstream dont-lose-data: deferred parent read', async () => {
  for (const impl of [h2lean, nodeHttp2]) {
    const srv = impl.createServer();
    srv.on('stream', (s) => {
      assert.strictEqual(s.pushAllowed, true);
      s.pushStream({ ':path': '/file' }, (err, pushStream) => {
        assert.ifError(err);
        pushStream.respond();
        pushStream.end('a push stream');
      });
      s.respond();
      s.end('hello world');
    });
    const port = await listen(srv);
    const client = connect(nodeHttp2, port);
    const req = client.request();
    let pushStream = null;
    client.on('stream', (s, headers) => {
      assert.strictEqual(headers[':path'], '/file');
      pushStream = s;
      s.resume(); // keep the credit flowing while the body is buffered
    });
    const done = new Promise((resolve, reject) => {
      req.on('response', () => {
        let pushData = '';
        pushStream.setEncoding('utf8');
        pushStream.on('data', (d) => {
          pushData += d;
        });
        pushStream.on('end', () => {
          assert.strictEqual(pushData, 'a push stream');
          // the parent is read only from the next turn of the loop
          setImmediate(() => {
            let data = '';
            req.setEncoding('utf8');
            req.on('data', (d) => {
              data += d;
            });
            req.on('end', () => {
              try {
                assert.strictEqual(data, 'hello world'); resolve();
              } catch (e) {
                reject(e);
              }
            });
          });
        });
        pushStream.on('error', reject);
      });
      req.on('error', reject);
    });
    await withTimeout(done, 8000, 'dont-lose-data');
    client.destroy();
    srv.close();
    for (const s of srv.sessions || []) {
      s.destroy();
    }
  }
});

// ---- test-http2-respond-file-push.js (adapted) ------------------------------
// Upstream serves the pushed body with respondWithFD; h2lean does not implement
// fd serving, so the port pushes respond({content-type, content-length}) +
// end(data). The check kept from upstream is the wire-visible half: the promise
// reaches the client with those headers and the whole body arrives.
test('upstream respond-file-push (adapted: respond+end)', async () => {
  const data = '.'.repeat(4096) + 'end of body';
  for (const impl of [h2lean, nodeHttp2]) {
    const srv = impl.createServer();
    srv.on('stream', (stream) => {
      // upstream ends the parent before pushing; see the header note -- the
      // legality of that order is the recorded teardown divergence, so the
      // port pushes first and the wire sequence stays identical
      stream.pushStream({ ':path': '/file.txt', ':method': 'GET' }, (err, push) => {
        assert.ifError(err);
        push.respond({
          'content-type': 'text/plain',
          'content-length': Buffer.byteLength(data)
        });
        push.end(data);
      });
      stream.respond({});
      stream.end();
    });
    const port = await listen(srv);
    const client = connect(nodeHttp2, port);
    const req = client.request({});
    req.resume();
    req.on('error', () => {});
    const pushDone = new Promise((resolve, reject) => {
      client.on('stream', (stream, headers) => {
        assert.strictEqual(headers[':path'], '/file.txt');
        stream.on('push', (h) => {
          assert.strictEqual(h['content-type'], 'text/plain');
          assert.strictEqual(+h['content-length'], data.length);
        });
        stream.setEncoding('utf8');
        let check = '';
        stream.on('data', (c) => {
          check += c;
        });
        stream.on('end', () => {
          try {
            assert.strictEqual(check, data); resolve();
          } catch (e) {
            reject(e);
          }
        });
        stream.on('error', reject);
      });
    });
    await withTimeout(Promise.all([once(req, 'close'), pushDone]), 8000, 'file-push');
    client.destroy();
    srv.close();
    for (const s of srv.sessions || []) {
      s.destroy();
    }
  }
});

// ---- test-http2-capture-rejection.js ----------------------------------------
// EventEmitter.captureRejections routing, each of upstream's four blocks as its
// own test (the global flag is restored after every arm):
//   stream handler rejects AFTER respond  -> stream 'error' (the rejection),
//     client sees the 200 and then ERR_HTTP2_STREAM_ERROR;
//   stream handler rejects BEFORE respond -> 500 auto-answer, no stream error;
//   request handler rejects               -> 500 with content-type stripped;
//   client stream handler rejects         -> the push stream gets the rejection.
async function captureArm(run) {
  const prev = events.captureRejections;
  events.captureRejections = true;
  try {
    await withTimeout(run(), 8000, 'capture-rejection');
  } finally {
    events.captureRejections = prev;
  }
}

test('upstream captureRejections: server stream after respond', async () => {
  for (const impl of [h2lean, nodeHttp2]) {
    await captureArm(async () => {
      const srv = impl.createServer();
      const kaboom = new Error('kaboom');
      let routed = null;
      srv.on('stream', async (stream) => {
        srv.close();
        stream.respond({ ':status': 200 });
        stream.on('error', (err) => {
          routed = err === kaboom ? 'kaboom' : `other:${err}`;
        });
        throw kaboom;
      });
      const port = await listen(srv);
      const client = connect(nodeHttp2, port);
      const req = client.request();
      let reqError = null;
      req.on('error', (err) => {
        reqError = err.code;
      });
      req.resume();
      await withTimeout(once(req, 'close'), 6000, 'block1 close');
      assert.strictEqual(routed, 'kaboom');
      assert.strictEqual(reqError, 'ERR_HTTP2_STREAM_ERROR');
      client.destroy();
      for (const s of srv.sessions || []) {
        s.destroy();
      }
    });
  }
});

test('upstream captureRejections: server stream before respond', async () => {
  for (const impl of [h2lean, nodeHttp2]) {
    await captureArm(async () => {
      const srv = impl.createServer();
      let errored = false;
      srv.on('stream', async (stream) => {
        srv.close();
        stream.on('error', () => {
          errored = true;
        });
        throw new Error('kaboom');
      });
      const port = await listen(srv);
      const client = connect(nodeHttp2, port);
      const req = client.request();
      const status = new Promise((resolve) => req.on('response', (h) => resolve(h[':status'])));
      req.on('error', () => {});
      req.resume();
      await withTimeout(Promise.all([status, once(req, 'close')]), 6000, 'block2 close');
      assert.strictEqual(await status, 500);
      assert.strictEqual(errored, false);
      client.destroy();
      for (const s of srv.sessions || []) {
        s.destroy();
      }
    });
  }
});

test('upstream captureRejections: compat request handler', async () => {
  for (const impl of [h2lean, nodeHttp2]) {
    await captureArm(async () => {
      const srv = impl.createServer(async (req, res) => {
        srv.close();
        res.setHeader('content-type', 'application/json');
        throw new Error('kaboom');
      });
      const port = await listen(srv);
      const client = connect(nodeHttp2, port);
      const req = client.request();
      const headers = new Promise((resolve) => req.on('response', (h) => resolve({ ...h })));
      req.on('error', () => {});
      req.resume();
      await withTimeout(Promise.all([headers, once(req, 'close')]), 6000, 'block3 close');
      const h = await headers;
      assert.strictEqual(h[':status'], 500);
      assert.strictEqual(Object.hasOwn(h, 'content-type'), false);
      client.destroy();
      for (const s of srv.sessions || []) {
        s.destroy();
      }
    });
  }
});

test('upstream captureRejections: client stream handler', async () => {
  for (const clientImpl of [nodeHttp2, h2lean]) {
    await captureArm(async () => {
      const srv = h2lean.createServer();
      srv.on('stream', async (stream) => {
        stream.pushStream({ ':scheme': 'http', ':path': '/foobar' }, (err, push) => {
          assert.ifError(err);
          push.respond({ 'content-type': 'text/html', ':status': 200 });
          push.end('pushed by the server');
          stream.end('test');
        });
        stream.respond({ ':status': 200 });
        srv.close();
      });
      const port = await listen(srv);
      const session = connect(clientImpl, port);
      const req = session.request();
      req.resume();
      req.on('error', () => {});
      const kaboom = new Error('kaboom');
      let routed = null;
      let markRouted;
      const routedNT = new Promise((r) => {
        markRouted = r;
      });
      session.on('stream', async (stream) => {
        session.close();
        stream.on('error', (err) => {
          routed = err === kaboom ? 'kaboom' : `other:${err}`;
          markRouted();
        });
        stream.resume();
        throw kaboom;
      });
      req.end();
      // The awaited unit is the routed error itself: node's captureRejectionSymbol
      // destroy(err) reaches the pushed stream's 'error' through the teardown path,
      // which on v20 lands after the request's 'close' (measured: waiting only for
      // req 'close' asserts one tick too early there). The upstream file asserts in
      // the stream's own 'close', the same point from the other side.
      await withTimeout(routedNT, 6000, 'block4 routed error');
      assert.strictEqual(routed, 'kaboom');
      session.destroy();
      srv.close();
      for (const s of srv.sessions || []) {
        s.destroy();
      }
    });
  }
});
