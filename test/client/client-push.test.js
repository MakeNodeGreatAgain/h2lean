// Client PUSH_PROMISE reception: h2lean.connect() receiving pushes from both
// node:http2 and h2lean servers, compared against what node's own client reports
// for the same exchange.
//
// The event model (measured on node v26.12.1):
//   * session emits 'stream' with (pushedStream, requestHeaders, flags=4)
//   * the pushed stream emits 'push' (NOT 'response') for the response headers
//   * the pushed stream's writable is pre-ended (writableFinished=true)
//   * destroying the pushed stream sends RST_STREAM(NO_ERROR); late frames for
//     it are silently ignored, the session lives
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const h2lean = require('../../index');
const { listen, closeServer } = require('../diff-harness');

// A server that pushes one resource alongside the main response.
// `impl` is 'node' or 'h2lean'; both expose createServer with the stream API.
function pushServer(impl, pushPath, pushBody) {
  const mod = impl === 'node' ? http2 : h2lean;
  const srv = mod.createServer();
  srv.on('stream', (s) => {
    s.respond({ ':status': 200 });
    s.pushStream({ ':path': pushPath || '/pushed' }, (err, ps) => {
      if (err) {
        return;
      }
      ps.respond({ ':status': 200, 'content-type': 'text/plain' });
      ps.end(pushBody || 'push-body');
    });
    s.end('main-body');
  });
  srv.on('error', () => {});
  return srv;
}

// Collect the push transcript through a client implementation.
function pushTranscript(client, reqPath) {
  return new Promise((resolve, reject) => {
    const t = { sessionStream: null, push: null, data: '', ended: false, rstCode: null,
      pushedSentHeaders: null, reqSentKeys: null };
    const timer = setTimeout(() => reject(new Error('push transcript timeout')), 5000);
    client.on('stream', (pushed, headers, flags) => {
      t.sessionStream = { id: pushed.id, headers, flags, writableFinished: pushed.writableFinished };
      // A pushed stream was never given a request map, so node's sentHeaders getter has
      // nothing to fold and answers undefined (core.js:2226, measured on a live client).
      t.pushedSentHeaders = pushed.sentHeaders === undefined ? 'undefined' : 'object';
      pushed.on('push', (h) => {
        t.push = h;
      });
      pushed.on('response', () => {
        t.responseUnexpected = true;
      });
      pushed.on('data', (d) => {
        t.data += d.toString();
      });
      pushed.on('end', () => {
        t.ended = true;
      });
      pushed.on('close', () => {
        t.rstCode = pushed.rstCode;
        clearTimeout(timer);
        resolve(t);
      });
      pushed.resume();
    });
    client.on('error', (e) => {
      clearTimeout(timer); reject(e);
    });
    const req = client.request({ ':path': reqPath || '/' });
    t.reqSentKeys = Object.keys(req.sentHeaders).join(',');
    req.on('error', () => {});
    req.resume();
  });
}

// ---- differential: node server, both clients --------------------------------

test('node server push: h2lean client matches node client transcript', async () => {
  const srv = pushServer('node', '/style.css', 'body{}');
  const port = await listen(srv);
  try {
    const nodeClient = http2.connect(`http://127.0.0.1:${port}`);
    nodeClient.on('error', () => {});
    const nodeT = await pushTranscript(nodeClient);
    nodeClient.close();

    const leanClient = h2lean.connect(`http://127.0.0.1:${port}`);
    leanClient.on('error', () => {});
    const leanT = await pushTranscript(leanClient);
    leanClient.close();

    assert.strictEqual(leanT.sessionStream.id, nodeT.sessionStream.id);
    assert.strictEqual(leanT.sessionStream.flags, nodeT.sessionStream.flags);
    assert.strictEqual(leanT.sessionStream.flags, 4);
    assert.strictEqual(leanT.sessionStream.writableFinished, true);
    assert.strictEqual(leanT.sessionStream.headers[':path'], '/style.css');
    assert.strictEqual(leanT.sessionStream.headers[':method'], 'GET');
    assert.strictEqual(leanT.push[':status'], nodeT.push[':status']);
    assert.strictEqual(leanT.push[':status'], 200);
    assert.strictEqual(leanT.data, nodeT.data);
    assert.strictEqual(leanT.data, 'body{}');
    assert.strictEqual(leanT.ended, true);
    assert.strictEqual(leanT.rstCode, 0);
    assert.strictEqual(leanT.responseUnexpected, undefined);
    assert.strictEqual(leanT.pushedSentHeaders, nodeT.pushedSentHeaders);
    assert.strictEqual(nodeT.pushedSentHeaders, 'undefined');
    assert.strictEqual(leanT.reqSentKeys, nodeT.reqSentKeys);
  } finally {
    await closeServer(srv);
  }
});

// ---- h2lean round trip -------------------------------------------------------

test('h2lean server push → h2lean client: full round trip', async () => {
  const srv = pushServer('h2lean', '/app.js', 'console.log(1)');
  const port = await listen(srv);
  try {
    const client = h2lean.connect(`http://127.0.0.1:${port}`);
    client.on('error', () => {});
    const t = await pushTranscript(client);
    client.close();

    assert.strictEqual(t.sessionStream.id, 2);
    assert.strictEqual(t.sessionStream.flags, 4);
    assert.strictEqual(t.sessionStream.writableFinished, true);
    assert.strictEqual(t.sessionStream.headers[':path'], '/app.js');
    assert.strictEqual(t.push[':status'], 200);
    assert.strictEqual(t.push['content-type'], 'text/plain');
    assert.strictEqual(t.data, 'console.log(1)');
    assert.strictEqual(t.ended, true);
    assert.strictEqual(t.rstCode, 0);
  } finally {
    await closeServer(srv);
  }
});

// ---- push event, not response ------------------------------------------------

test('pushed stream emits push, never response', async () => {
  const srv = pushServer('node');
  const port = await listen(srv);
  try {
    const client = h2lean.connect(`http://127.0.0.1:${port}`);
    client.on('error', () => {});
    const t = await pushTranscript(client);
    client.close();
    assert.strictEqual(t.responseUnexpected, undefined);
    assert.ok(t.push !== null);
  } finally {
    await closeServer(srv);
  }
});

// ---- writable pre-ended ------------------------------------------------------

test('pushed stream writable is ended at the stream event', async () => {
  const srv = pushServer('h2lean');
  const port = await listen(srv);
  try {
    const client = h2lean.connect(`http://127.0.0.1:${port}`);
    client.on('error', () => {});
    const result = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('timeout')), 5000);
      client.on('stream', (pushed) => {
        clearTimeout(timer);
        resolve({
          writableEnded: pushed.writableEnded,
          writableFinished: pushed.writableFinished,
          destroyed: pushed.destroyed
        });
        pushed.resume();
        pushed.on('close', () => client.close());
      });
      const req = client.request({ ':path': '/' });
      req.resume();
    });
    assert.strictEqual(result.writableEnded, true);
    assert.strictEqual(result.writableFinished, true);
    assert.strictEqual(result.destroyed, false);
  } finally {
    await closeServer(srv);
  }
});

// ---- client refusal ----------------------------------------------------------

test('destroying the pushed stream sends RST_STREAM(0), session lives', async () => {
  const srv = pushServer('h2lean');
  const port = await listen(srv);
  try {
    const client = h2lean.connect(`http://127.0.0.1:${port}`);
    const result = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('timeout')), 5000);
      client.on('error', (e) => {
        clearTimeout(timer); reject(e);
      });
      client.on('stream', (pushed) => {
        pushed.destroy();
      });
      const req = client.request({ ':path': '/' });
      req.on('error', () => {});
      req.on('data', () => {});
      req.on('end', () => {
        setTimeout(() => {
          clearTimeout(timer);
          resolve({ alive: !client.destroyed && !client.closed });
          client.close();
        }, 150);
      });
    });
    assert.strictEqual(result.alive, true);
  } finally {
    await closeServer(srv);
  }
});

// ---- push disabled -----------------------------------------------------------

test('enablePush=false: server pushAllowed is false, no push arrives', async () => {
  const srv = h2lean.createServer();
  let serverPushAllowed = null;
  srv.on('stream', (s) => {
    serverPushAllowed = s.pushAllowed;
    s.respond({ ':status': 200 });
    if (s.pushAllowed) {
      s.pushStream({ ':path': '/p' }, (err, ps) => {
        if (!err) {
          ps.respond({ ':status': 200 }); ps.end('x');
        }
      });
    }
    s.end('main');
  });
  srv.on('error', () => {});
  const port = await listen(srv);
  try {
    const client = h2lean.connect(`http://127.0.0.1:${port}`, { settings: { enablePush: false } });
    client.on('error', () => {});
    let gotPush = false;
    client.on('stream', () => {
      gotPush = true;
    });
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('timeout')), 5000);
      const req = client.request({ ':path': '/' });
      req.on('response', (h) => assert.strictEqual(h[':status'], 200));
      req.on('data', () => {});
      req.on('end', () => {
        clearTimeout(timer); resolve();
      });
    });
    assert.strictEqual(gotPush, false);
    assert.strictEqual(serverPushAllowed, false);
    client.close();
  } finally {
    await closeServer(srv);
  }
});

// ---- multiple pushes ---------------------------------------------------------

test('multiple pushes get sequential even ids', async () => {
  const mod = h2lean;
  const srv = mod.createServer();
  srv.on('stream', (s) => {
    s.respond({ ':status': 200 });
    s.pushStream({ ':path': '/a' }, (err, ps) => {
      if (!err) {
        ps.respond({ ':status': 200 }); ps.end('a');
      }
    });
    s.pushStream({ ':path': '/b' }, (err, ps) => {
      if (!err) {
        ps.respond({ ':status': 200 }); ps.end('b');
      }
    });
    s.pushStream({ ':path': '/c' }, (err, ps) => {
      if (!err) {
        ps.respond({ ':status': 200 }); ps.end('c');
      }
    });
    s.end('main');
  });
  srv.on('error', () => {});
  const port = await listen(srv);
  try {
    const client = h2lean.connect(`http://127.0.0.1:${port}`);
    client.on('error', () => {});
    const ids = [];
    const bodies = [];
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('timeout')), 5000);
      let closed = 0;
      client.on('stream', (pushed) => {
        ids.push(pushed.id);
        let body = '';
        pushed.on('data', (d) => {
          body += d.toString();
        });
        pushed.on('close', () => {
          bodies.push(body);
          if (++closed === 3) {
            clearTimeout(timer); resolve();
          }
        });
        pushed.resume();
      });
      const req = client.request({ ':path': '/' });
      req.resume();
    });
    client.close();
    assert.deepStrictEqual(ids, [2, 4, 6]);
    assert.deepStrictEqual(bodies, ['a', 'b', 'c']);
  } finally {
    await closeServer(srv);
  }
});

// ---- CONTINUATION split ------------------------------------------------------

test('large push promise block split into CONTINUATION is reassembled', async () => {
  const srv = h2lean.createServer();
  const longPath = '/x'.repeat(10000);
  srv.on('stream', (s) => {
    s.respond({ ':status': 200 });
    s.pushStream({ ':path': longPath }, (err, ps) => {
      if (!err) {
        ps.respond({ ':status': 200 }); ps.end('ok');
      }
    });
    s.end('main');
  });
  srv.on('error', () => {});
  const port = await listen(srv);
  try {
    const client = h2lean.connect(`http://127.0.0.1:${port}`);
    client.on('error', () => {});
    const t = await pushTranscript(client);
    client.close();
    assert.strictEqual(t.sessionStream.headers[':path'], longPath);
    assert.strictEqual(t.push[':status'], 200);
    assert.strictEqual(t.data, 'ok');
  } finally {
    await closeServer(srv);
  }
});

// ---- request headers in the stream event -------------------------------------

test('stream event carries request pseudo-headers with defaults', async () => {
  const srv = http2.createServer();
  srv.on('stream', (s) => {
    s.respond({ ':status': 200 });
    s.pushStream({ ':path': '/res' }, (err, ps) => {
      if (!err) {
        ps.respond({ ':status': 200 }); ps.end('r');
      }
    });
    s.end('m');
  });
  srv.on('error', () => {});
  const port = await listen(srv);
  try {
    const client = h2lean.connect(`http://127.0.0.1:${port}`);
    client.on('error', () => {});
    const t = await pushTranscript(client);
    client.close();
    const h = t.sessionStream.headers;
    assert.strictEqual(h[':method'], 'GET');
    assert.strictEqual(h[':path'], '/res');
    assert.strictEqual(h[':scheme'], 'http');
    assert.ok(h[':authority'].includes('127.0.0.1'));
  } finally {
    await closeServer(srv);
  }
});

// ---- node client receives h2lean push (cross-stack) --------------------------

test('h2lean server push → node client: node reports the same model', async () => {
  const srv = pushServer('h2lean', '/cross', 'cross-body');
  const port = await listen(srv);
  try {
    const client = http2.connect(`http://127.0.0.1:${port}`);
    client.on('error', () => {});
    const t = await pushTranscript(client);
    client.close();
    assert.strictEqual(t.sessionStream.id, 2);
    assert.strictEqual(t.sessionStream.flags, 4);
    assert.strictEqual(t.sessionStream.writableFinished, true);
    assert.strictEqual(t.push[':status'], 200);
    assert.strictEqual(t.data, 'cross-body');
    assert.strictEqual(t.rstCode, 0);
  } finally {
    await closeServer(srv);
  }
});
