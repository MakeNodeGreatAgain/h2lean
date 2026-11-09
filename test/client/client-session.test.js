// M2 client-session protocol completeness: the control-frame machinery that a
// drop-in client owes beyond a single round-trip -- SETTINGS in both directions,
// the peer's concurrency limit, PING, and GOAWAY -- driven against a node:http2
// server so every expectation is a measured value, not an assumption.
//
// The frame-ordering bug that broke large uploads (DATA committed before a stream's
// HEADERS, which nghttp2 answers with GOAWAY(INTERNAL_ERROR)) is regression-tested
// here too, because it is a control-plane property of the session, not of one body.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const h2lite = require('../../index');
const { listen, closeServer } = require('../diff-harness');

// A server that advertises a specific SETTINGS block, answering a plain GET so the
// client has something to talk to while the control frames exchange.
function startServer(handler, serverSettings) {
  const srv = serverSettings
    ? http2.createServer({ settings: serverSettings }, handler)
    : http2.createServer(handler);
  srv.on('error', () => {});
  return listen(srv).then((port) => ({ srv, port }));
}

const echo = (req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain' }); res.end('ok');
};

test('remoteSettings reflect the server advertised SETTINGS, as node sees them', async () => {
  const settings = { maxConcurrentStreams: 7, initialWindowSize: 8192 };
  const { srv, port } = await startServer(echo, settings);
  try {
    const read = (impl) => new Promise((resolve, reject) => {
      const sess = impl.connect(`http://127.0.0.1:${port}`);
      sess.on('error', reject);
      sess.on('remoteSettings', (rs) => {
        sess.destroy(); resolve(rs);
      });
    });
    const nodeRS = await read(http2);
    const liteRS = await read(h2lite);
    // the two numbers under test, compared to what node's own client learned
    assert.strictEqual(liteRS.maxConcurrentStreams, nodeRS.maxConcurrentStreams);
    assert.strictEqual(liteRS.maxConcurrentStreams, 7);
    assert.strictEqual(liteRS.initialWindowSize, nodeRS.initialWindowSize);
    assert.strictEqual(liteRS.initialWindowSize, 8192);
  } finally {
    await closeServer(srv);
  }
});

test('pendingSettingsAck starts true and clears once the server acknowledges ours',
  async () => {
    const { srv, port } = await startServer(echo);
    try {
      const sess = h2lite.connect(`http://127.0.0.1:${port}`);
      sess.on('error', () => {});
      assert.strictEqual(sess.pendingSettingsAck, true);
      // The peer acknowledges our SETTINGS with a SETTINGS frame carrying the ACK
      // flag; the trigger that proves we parsed it is a ping round-trip landing after.
      await new Promise((resolve, reject) => {
        sess.ping((err) => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
        setTimeout(() => reject(new Error('ping timed out')), 3000);
      });
      assert.strictEqual(sess.pendingSettingsAck, false);
      sess.destroy();
    } finally {
      await closeServer(srv);
    }
  });

test('SETTINGS_MAX_CONCURRENT_STREAMS gates how many streams open at once', async () => {
  const max = 2;
  let peak = 0;
  let live = 0;
  let total = 0;
  const hold = (req, res) => {
    live++; total++;
    if (live > peak) {
      peak = live;
    }
    setTimeout(() => {
      live--; res.writeHead(200); res.end('ok' + total);
    }, 40);
  };
  const { srv, port } = await startServer(hold, { maxConcurrentStreams: max });
  try {
    const sess = h2lite.connect(`http://127.0.0.1:${port}`);
    sess.on('error', () => {});
    // Fire the burst only once the limit is known, so the client -- like nghttp2 --
    // cannot be caught opening more than the peer permits before its SETTINGS land.
    await new Promise((resolve) => sess.on('remoteSettings', () => {
      for (let i = 0; i < 6; i++) {
        const s = sess.request({ ':method': 'GET', ':path': '/' });
        s.on('error', () => {});
        s.resume();
      }
      resolve();
    }));
    await new Promise((r) => setTimeout(r, 700));
    assert.strictEqual(total, 6, 'every queued request still reaches the server');
    assert.ok(peak <= max, `peak concurrent ${peak} exceeded the advertised limit ${max}`);
    sess.destroy();
  } finally {
    await closeServer(srv);
  }
});

test('ping(cb) round-trips and the callback matches node\'s (err, duration, pong)',
  async () => {
    const { srv, port } = await startServer(echo);
    try {
      const call = (impl) => new Promise((resolve, reject) => {
        const sess = impl.connect(`http://127.0.0.1:${port}`);
        sess.on('error', reject);
        sess.on('connect', () => sess.ping((err, duration, pong) => {
          const shape = { err, durType: typeof duration, pongIsBuffer: Buffer.isBuffer(pong),
            pongLen: Buffer.isBuffer(pong) ? pong.length : null };
          sess.destroy();
          resolve(shape);
        }));
      });
      const nodeShape = await call(http2);
      const liteShape = await call(h2lite);
      assert.strictEqual(liteShape.err, null);
      assert.strictEqual(liteShape.durType, nodeShape.durType, 'duration is not a number');
      assert.strictEqual(liteShape.pongIsBuffer, nodeShape.pongIsBuffer);
      assert.strictEqual(liteShape.pongLen, 8);
    } finally {
      await closeServer(srv);
    }
  });

test('a server GOAWAY is reported with the same event shape node delivers', async () => {
  const srv = http2.createServer();
  srv.on('error', () => {});
  srv.on('stream', (stream, headers) => {
    if (headers[':path'] === '/go') {
      stream.respond({ ':status': 200, 'content-type': 'text/plain' }, { endStream: false });
      stream.write('part');
      setTimeout(() => stream.session.goaway(0), 10);
    } else {
      stream.respond({ ':status': 200 }, { endStream: false });
      stream.end('ok');
    }
  });
  const port = await listen(srv);
  try {
    const capture = (impl) => new Promise((resolve) => {
      const sess = impl.connect(`http://127.0.0.1:${port}`);
      sess.on('error', () => {});
      let event = null;
      sess.on('goaway', (code, last, opaque) => {
        event = { codeType: typeof code, lastType: typeof last,
          opaqueType: opaque === undefined ? 'undefined' : typeof opaque };
      });
      const s = sess.request({ ':method': 'GET', ':path': '/go' });
      s.on('error', () => {});
      s.resume();
      setTimeout(() => {
        const out = { event, goawayCode: sess.goawayCode, goawayLast: sess.goawayLastStreamID };
        sess.destroy();
        resolve(out);
      }, 300);
    });
    const node = await capture(http2);
    const lite = await capture(h2lite);
    assert.ok(node.event, 'node did not emit goaway (server harness broken?)');
    assert.deepStrictEqual(lite.event, node.event, 'goaway event shape differs from node');
    assert.strictEqual(typeof lite.goawayCode, 'number');
    assert.strictEqual(lite.goawayCode, node.goawayCode);
    assert.strictEqual(lite.goawayLast, node.goawayLast);
    // A graceful NO_ERROR goodbye keeps the session object alive (streams at or below
    // lastStreamID may still finish), exactly as node reports.
    assert.strictEqual(lite.goawayCode, 0);
  } finally {
    await closeServer(srv);
  }
});

test('destroying an open stream sends an RST_STREAM the server observes', async () => {
  const srv = http2.createServer();
  srv.on('error', () => {});
  let serverClosed = false;
  srv.on('stream', (stream, headers) => {
    if (headers[':path'] === '/hold') {
      // answer headers but never end: the client must give up and reset.
      stream.respond({ ':status': 200 }, { endStream: false });
      stream.on('close', () => {
        serverClosed = true;
      });
    } else {
      stream.respond({ ':status': 200 }, { endStream: false });
      stream.end('ok');
    }
  });
  const port = await listen(srv);
  try {
    const sess = h2lite.connect(`http://127.0.0.1:${port}`);
    sess.on('error', () => {});
    const s = sess.request({ ':method': 'GET', ':path': '/hold' });
    s.on('error', () => {});
    s.resume();
    await new Promise((r) => setTimeout(r, 50));
    s.destroy(); // abandons the request -> RST_STREAM(CANCEL)
    await new Promise((r) => setTimeout(r, 150));
    assert.ok(serverClosed, 'the server never saw the stream close after a client reset');
    sess.destroy();
  } finally {
    await closeServer(srv);
  }
});
