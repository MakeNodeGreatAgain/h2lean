// Server push: h2lean's pushStream() driven by node:http2's client (a real nghttp2
// peer). The differential arm runs the same handler against node's own server, so
// anything that differs is an implementation gap, not a test artefact.
//
// Measured event model (v26.12.1, source + live probes): the client session emits
// 'stream' for PUSH_PROMISE (promise headers, flags=4 END_HEADERS); the pushed
// stream emits 'push' for its response headers (instead of 'response'). There is
// NO session-level 'push' event.
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const h2lean = require('../index');

function listen(server) {
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve(server.address().port));
  });
}

function connect(port, opts) {
  const client = http2.connect(`http://127.0.0.1:${port}`, opts);
  client.on('error', () => {});
  return client;
}

function closeServer(srv) {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      for (const s of [...(srv.sessions || [])]) {
        s.destroy();
      }
      srv.close(resolve);
      setTimeout(resolve, 500);
      if (srv.unref) {
        srv.unref();
      }
    }, 1000);
    srv.close(() => {
      clearTimeout(timer); resolve();
    });
  });
}

// Collect the full push transcript from a node client: session 'stream' events,
// pushed 'push'/'response', data, end, close. Returns a promise that resolves
// once the main request ends and a short grace period lets push events land.
function pushTranscript(client, path) {
  return new Promise((resolve, reject) => {
    const pushes = [];
    client.on('stream', (stream, headers, flags) => {
      const entry = { id: stream.id, promiseHeaders: { ...headers }, flags, events: [] };
      pushes.push(entry);
      stream.on('push', (h) => entry.events.push(['push', { ...h }]));
      stream.on('response', (h) => entry.events.push(['response', { ...h }]));
      const chunks = [];
      stream.setEncoding('utf8');
      stream.on('data', (c) => chunks.push(c));
      stream.on('end', () => entry.events.push(['end', chunks.join('')]));
      stream.on('close', () => entry.events.push(['close']));
      stream.on('error', (e) => entry.events.push(['error', e.code]));
    });
    const req = client.request({ ':path': path || '/' });
    const mainChunks = [];
    let mainHeaders = null;
    req.on('response', (h) => {
      mainHeaders = { ...h };
    });
    req.setEncoding('utf8');
    req.on('data', (c) => mainChunks.push(c));
    req.on('end', () => {
      setTimeout(() => resolve({
        main: { headers: mainHeaders, body: mainChunks.join('') },
        pushes
      }), 80);
    });
    req.on('error', reject);
    req.end();
  });
}

// ---- round trip ------------------------------------------------------------

test('push round trip: h2lean server → node client', async () => {
  const srv = h2lean.createServer();
  srv.on('stream', (stream) => {
    stream.pushStream({ ':path': '/style.css' }, (err, ps) => {
      assert.ifError(err);
      ps.respond({ ':status': 200, 'content-type': 'text/css' });
      ps.end('body { color: red }');
    });
    stream.respond({ ':status': 200, 'content-type': 'text/html' });
    stream.end('<html></html>');
  });
  const port = await listen(srv);
  const client = connect(port);
  const t = await pushTranscript(client);
  client.destroy();
  await closeServer(srv);

  assert.strictEqual(t.main.headers[':status'], 200);
  assert.strictEqual(t.main.body, '<html></html>');
  assert.strictEqual(t.pushes.length, 1);
  const p = t.pushes[0];
  assert.strictEqual(p.id, 2);
  assert.strictEqual(p.id % 2, 0);
  assert.strictEqual(p.promiseHeaders[':path'], '/style.css');
  assert.strictEqual(p.promiseHeaders[':method'], 'GET');
  assert.strictEqual(p.flags, 4); // END_HEADERS
  assert.strictEqual(p.events[0][0], 'push');
  assert.strictEqual(p.events[0][1][':status'], 200);
  assert.strictEqual(p.events[0][1]['content-type'], 'text/css');
  assert.deepStrictEqual(p.events[1], ['end', 'body { color: red }']);
  assert.deepStrictEqual(p.events[2], ['close']);
});

test('push round trip: node server → node client (differential baseline)', async () => {
  const srv = http2.createServer();
  srv.on('stream', (stream) => {
    stream.pushStream({ ':path': '/style.css' }, (err, ps) => {
      assert.ifError(err);
      ps.respond({ ':status': 200, 'content-type': 'text/css' });
      ps.end('body { color: red }');
    });
    stream.respond({ ':status': 200, 'content-type': 'text/html' });
    stream.end('<html></html>');
  });
  const port = await listen(srv);
  const client = connect(port);
  const t = await pushTranscript(client);
  client.destroy();
  await closeServer(srv);

  assert.strictEqual(t.pushes.length, 1);
  const p = t.pushes[0];
  assert.strictEqual(p.id, 2);
  assert.strictEqual(p.promiseHeaders[':path'], '/style.css');
  assert.strictEqual(p.promiseHeaders[':method'], 'GET');
  assert.strictEqual(p.flags, 4);
  assert.strictEqual(p.events[0][0], 'push');
  assert.strictEqual(p.events[0][1][':status'], 200);
  assert.strictEqual(p.events[0][1]['content-type'], 'text/css');
  assert.deepStrictEqual(p.events[1], ['end', 'body { color: red }']);
  assert.deepStrictEqual(p.events[2], ['close']);
});

// ---- pushAllowed and synchronous errors ------------------------------------

test('pushAllowed is false when the client disables push', async () => {
  const srv = h2lean.createServer();
  let allowed = null;
  let thrown = null;
  srv.on('stream', (stream) => {
    allowed = stream.pushAllowed;
    try {
      stream.pushStream({ ':path': '/x' }, () => {});
    } catch (e) {
      thrown = e.code;
    }
    stream.respond({ ':status': 200 });
    stream.end();
  });
  const port = await listen(srv);
  const client = connect(port, { settings: { enablePush: false } });
  const req = client.request({ ':path': '/' });
  req.resume();
  await new Promise((r) => req.on('end', r));
  client.destroy();
  await closeServer(srv);

  assert.strictEqual(allowed, false);
  assert.strictEqual(thrown, 'ERR_HTTP2_PUSH_DISABLED');
});

test('pushAllowed is true by default (client advertises nothing)', async () => {
  const srv = h2lean.createServer();
  let allowed = null;
  srv.on('stream', (stream) => {
    allowed = stream.pushAllowed;
    stream.respond({ ':status': 200 });
    stream.end();
  });
  const port = await listen(srv);
  const client = connect(port);
  const req = client.request({ ':path': '/' });
  req.resume();
  await new Promise((r) => req.on('end', r));
  client.destroy();
  await closeServer(srv);

  assert.strictEqual(allowed, true);
});

test('nested push throws ERR_HTTP2_NESTED_PUSH', async () => {
  const srv = h2lean.createServer();
  let nestedCode = null;
  srv.on('stream', (stream) => {
    stream.pushStream({ ':path': '/p' }, (err, ps) => {
      assert.ifError(err);
      try {
        ps.pushStream({ ':path': '/nested' }, () => {});
      } catch (e) {
        nestedCode = e.code;
      }
      ps.respond({ ':status': 200 });
      ps.end();
    });
    stream.respond({ ':status': 200 });
    stream.end();
  });
  const port = await listen(srv);
  const client = connect(port);
  client.on('stream', (s) => s.resume());
  const req = client.request({ ':path': '/' });
  req.resume();
  await new Promise((r) => req.on('end', r));
  await new Promise((r) => setTimeout(r, 50));
  client.destroy();
  await closeServer(srv);

  assert.strictEqual(nestedCode, 'ERR_HTTP2_NESTED_PUSH');
});

// ---- header defaults -------------------------------------------------------

test('push promise headers default :method GET, :path /, :authority and :scheme from request',
  async () => {
    const srv = h2lean.createServer();
    let promiseHeaders = null;
    srv.on('stream', (stream) => {
      stream.pushStream({}, (err, ps, h) => {
        assert.ifError(err);
        promiseHeaders = h;
        ps.respond({ ':status': 200 });
        ps.end();
      });
      stream.respond({ ':status': 200 });
      stream.end();
    });
    const port = await listen(srv);
    const client = connect(port);
    const t = await pushTranscript(client);
    client.destroy();
    await closeServer(srv);

    assert.strictEqual(promiseHeaders[':method'], 'GET');
    assert.strictEqual(promiseHeaders[':path'], '/');
    assert.strictEqual(promiseHeaders[':scheme'], 'http');
    assert.ok(promiseHeaders[':authority'].includes('127.0.0.1'));
    // The client sees the same defaults in the promise headers
    assert.strictEqual(t.pushes[0].promiseHeaders[':method'], 'GET');
    assert.strictEqual(t.pushes[0].promiseHeaders[':path'], '/');
  });

// ---- HEAD → endStream ------------------------------------------------------

test('HEAD push ends the stream immediately (endStream implied)', async () => {
  const srv = h2lean.createServer();
  let closed = null;
  let headersSent = null;
  srv.on('stream', (stream) => {
    stream.pushStream({ ':method': 'HEAD', ':path': '/head' }, (err, ps) => {
      assert.ifError(err);
      closed = ps.closed;
      headersSent = ps.headersSent;
    });
    stream.respond({ ':status': 200 });
    stream.end();
  });
  const port = await listen(srv);
  const client = connect(port);
  client.on('stream', (s) => s.resume());
  const req = client.request({ ':path': '/' });
  req.resume();
  await new Promise((r) => req.on('end', r));
  await new Promise((r) => setTimeout(r, 50));
  client.destroy();
  await closeServer(srv);

  assert.strictEqual(closed, true);
  assert.strictEqual(headersSent, true);
});

// ---- multiple pushes -------------------------------------------------------

test('multiple pushes reserve sequential even ids 2, 4, 6', async () => {
  const srv = h2lean.createServer();
  const ids = [];
  srv.on('stream', (stream) => {
    for (const path of ['/a', '/b', '/c']) {
      stream.pushStream({ ':path': path }, (err, ps) => {
        assert.ifError(err);
        ids.push(ps.id);
        ps.respond({ ':status': 200 });
        ps.end(path);
      });
    }
    stream.respond({ ':status': 200 });
    stream.end('main');
  });
  const port = await listen(srv);
  const client = connect(port);
  const t = await pushTranscript(client);
  client.destroy();
  await closeServer(srv);

  assert.deepStrictEqual(ids, [2, 4, 6]);
  assert.strictEqual(t.pushes.length, 3);
  assert.deepStrictEqual(t.pushes.map((p) => p.id), [2, 4, 6]);
  assert.deepStrictEqual(t.pushes.map((p) => p.promiseHeaders[':path']), ['/a', '/b', '/c']);
});

// ---- CONTINUATION for a large promise block --------------------------------

test('a promise block larger than maxFrameSize splits into CONTINUATION', async () => {
  const srv = h2lean.createServer();
  let pushOk = false;
  srv.on('stream', (stream) => {
    // 20000 octets exceeds the default 16384 maxFrameSize, forcing CONTINUATION.
    const bigValue = 'z'.repeat(20000);
    stream.pushStream({ ':path': '/big', 'x-big': bigValue }, (err, ps) => {
      assert.ifError(err);
      pushOk = true;
      ps.respond({ ':status': 200 });
      ps.end('ok');
    });
    stream.respond({ ':status': 200 });
    stream.end('main');
  });
  const port = await listen(srv);
  const client = connect(port);
  const t = await pushTranscript(client);
  client.destroy();
  await closeServer(srv);

  assert.strictEqual(pushOk, true);
  assert.strictEqual(t.pushes.length, 1);
  assert.strictEqual(t.pushes[0].promiseHeaders['x-big'], 'z'.repeat(20000));
  assert.deepStrictEqual(t.pushes[0].events[1], ['end', 'ok']);
});

// ---- GOAWAY last-stream-id is not moved by push ----------------------------

test('GOAWAY last-stream-id names the highest inbound odd id, not push ids', async () => {
  const srv = h2lean.createServer();
  srv.on('stream', (stream) => {
    stream.pushStream({ ':path': '/p1' }, (err, ps) => {
      assert.ifError(err);
      ps.respond({ ':status': 200 });
      ps.end();
    });
    stream.pushStream({ ':path': '/p2' }, (err, ps) => {
      assert.ifError(err);
      ps.respond({ ':status': 200 });
      ps.end();
    });
    stream.respond({ ':status': 200 });
    stream.end();
  });
  const port = await listen(srv);
  const client = connect(port);
  client.on('stream', (s) => s.resume());
  const req = client.request({ ':path': '/' });
  req.resume();
  await new Promise((r) => req.on('end', r));
  await new Promise((r) => setTimeout(r, 80));
  // Server-initiated graceful close: GOAWAY last-stream-id must be 1 (the request),
  // not 4 (the highest push id).
  const goaway = await new Promise((resolve) => {
    client.on('goaway', (code, lastSid) => resolve({ code, lastSid }));
    srv.close();
  });
  client.destroy();
  await closeServer(srv);

  assert.strictEqual(goaway.code, 0); // NO_ERROR
  assert.strictEqual(goaway.lastSid, 1);
});

// ---- client refusal (RST_STREAM on push id) --------------------------------

test('client refusal: destroying a pushed stream closes the server-side push', async () => {
  const srv = h2lean.createServer();
  let pushRef = null;
  let pushClosed = false;
  let pushRstCode = null;
  srv.on('stream', (stream) => {
    stream.pushStream({ ':path': '/refused' }, (err, ps) => {
      assert.ifError(err);
      pushRef = ps;
      ps.on('close', () => {
        pushClosed = true;
        pushRstCode = ps.rstCode;
      });
      // Do not respond — wait for the client to refuse
    });
    stream.respond({ ':status': 200 });
    stream.end();
  });
  const port = await listen(srv);
  const client = connect(port);
  client.on('stream', (s) => {
    s.destroy(); // refuse the push
  });
  const req = client.request({ ':path': '/' });
  req.resume();
  await new Promise((r) => req.on('end', r));
  await new Promise((r) => setTimeout(r, 100));
  client.destroy();
  await closeServer(srv);

  assert.strictEqual(pushClosed, true);
  assert.strictEqual(pushRef.destroyed, true);
  // node's client destroy() without a code sends RST_STREAM(NO_ERROR=0)
  assert.strictEqual(pushRstCode, 0);
});

// ---- options.endStream -----------------------------------------------------

test('options.endStream closes the pushed stream without a body', async () => {
  const srv = h2lean.createServer();
  srv.on('stream', (stream) => {
    stream.pushStream({ ':path': '/empty' }, { endStream: true }, (err, ps) => {
      assert.ifError(err);
      assert.strictEqual(ps.closed, true);
    });
    stream.respond({ ':status': 200 });
    stream.end();
  });
  const port = await listen(srv);
  const client = connect(port);
  const t = await pushTranscript(client);
  client.destroy();
  await closeServer(srv);

  assert.strictEqual(t.pushes.length, 1);
  // endStream on pushStream means the response is just headers + END_STREAM
  assert.strictEqual(t.pushes[0].events[0][0], 'push');
  assert.strictEqual(t.pushes[0].events[0][1][':status'], 200);
  assert.deepStrictEqual(t.pushes[0].events[1], ['end', '']);
});

// ---- callback shape --------------------------------------------------------

test('pushStream callback receives (null, stream, headers, 0)', async () => {
  const srv = h2lean.createServer();
  let cbArgs = null;
  srv.on('stream', (stream) => {
    stream.pushStream({ ':path': '/cb' }, function (err, ps, h, flags) {
      cbArgs = { err, id: ps.id, headers: h, flags };
      ps.respond({ ':status': 200 });
      ps.end();
    });
    stream.respond({ ':status': 200 });
    stream.end();
  });
  const port = await listen(srv);
  const client = connect(port);
  client.on('stream', (s) => s.resume());
  const req = client.request({ ':path': '/' });
  req.resume();
  await new Promise((r) => req.on('end', r));
  await new Promise((r) => setTimeout(r, 50));
  client.destroy();
  await closeServer(srv);

  assert.strictEqual(cbArgs.err, null);
  assert.strictEqual(cbArgs.id, 2);
  assert.strictEqual(cbArgs.headers[':path'], '/cb');
  assert.strictEqual(cbArgs.flags, 0);
});

// ---- options as function (callback shorthand) ------------------------------

test('options may be the callback function directly', async () => {
  const srv = h2lean.createServer();
  let called = false;
  srv.on('stream', (stream) => {
    stream.pushStream({ ':path': '/fn' }, (err, ps) => {
      assert.ifError(err);
      called = true;
      ps.respond({ ':status': 200 });
      ps.end();
    });
    stream.respond({ ':status': 200 });
    stream.end();
  });
  const port = await listen(srv);
  const client = connect(port);
  client.on('stream', (s) => s.resume());
  const req = client.request({ ':path': '/' });
  req.resume();
  await new Promise((r) => req.on('end', r));
  await new Promise((r) => setTimeout(r, 50));
  client.destroy();
  await closeServer(srv);

  assert.strictEqual(called, true);
});
