// M1 client round-trip: h2lean.connect() driven against servers it does not control.
//
// The whole differential suite up to now has h2lean on the SERVING side and an
// external client knocking on it. This file inverts that: h2lean is the client, and
// the question is whether the frames it puts on the wire -- preface, SETTINGS,
// HEADERS with request pseudo-headers, flow-controlled DATA, WINDOW_UPDATE credit,
// SETTINGS ACK -- are accepted by an independent stack and whether the answers come
// back identical to what node's own client reports for the same exchange.
//
// Two peers are used on purpose:
//   * node:http2 -- gives the byte-for-byte differential (same server, both clients,
//     compare everything the application can observe).
//   * nghttpd (nghttp2's own server) -- a strict reference implementation that is not
//     this repository and not node, so a framing bug the two share still shows up. It
//     is skipped, not failed, when the binary is absent, exactly like interop-external.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const path = require('node:path');
const fs = require('node:fs');
const { spawn, spawnSync } = require('node:child_process');
const h2lean = require('../../index');
const { listen, closeServer } = require('../diff-harness');

// A version line and a zero status is enough to know nghttpd is runnable; a machine
// without it gets a skip rather than a failure that would read like a protocol bug.
function available(bin) {
  const r = spawnSync(bin, ['--version'], { encoding: 'utf8', timeout: 5000 });
  return !r.error && r.status === 0;
}
const NGHTTPD = available('nghttpd');

// An echo server shared by both differential arms: it answers a GET with a fixed body
// and echoes any request body back for a POST, tagging the response with the byte count
// so a truncated or reordered upload is caught by the comparison and not by luck.
function startEchoServer() {
  const srv = http2.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      const echoed = Buffer.concat(chunks);
      const body = req.method === 'GET' || echoed.length === 0
        ? Buffer.from('hello world')
        : Buffer.concat([Buffer.from('echo:'), echoed]);
      res.writeHead(200, {
        'content-type': 'text/plain',
        'x-echo-len': String(echoed.length)
      });
      res.end(body);
    });
  });
  srv.on('error', () => {});
  return listen(srv).then((port) => ({ srv, port }));
}

// One observable request through EITHER client implementation. `impl` is
// `node:http2` or `h2lean`; both expose connect()/request() with the same surface,
// which is the point of the drop-in promise. Everything the application can see about
// the answer is captured in one plain object so the two arms can be deep-compared.
// node hangs an internal `Symbol(kSensitiveHeaders)` off its header object; the
// application-visible contract is the string-named fields, so those (not the symbol)
// are what the differential compares.
function fields(h) {
  const o = {};
  // `date` is second-granular and the two arms run sequentially, so a straddle would
  // read as a false difference; it is not a protocol-correctness field for this test.
  for (const k of Object.keys(h)) {
    if (k !== 'date') {
      o[k] = h[k];
    }
  }
  return o;
}

function grab(impl, port, headers, body) {
  return new Promise((resolve, reject) => {
    const sess = impl.connect(`http://127.0.0.1:${port}`);
    sess.on('error', reject);
    const opts = body === undefined ? {} : { endStream: false };
    const s = sess.request(headers, opts);
    const out = { headers: null, data: '' };
    s.on('response', (h) => {
      out.headers = fields(h);
    });
    s.setEncoding('latin1');
    s.on('data', (d) => {
      out.data += d;
    });
    s.on('error', reject);
    s.on('end', () => {
      sess.destroy();
      resolve(out);
    });
    if (body !== undefined) {
      s.end(body);
    }
  });
}

test('GET round-trip: h2lean matches node\'s own client frame for frame', async () => {
  const { srv, port } = await startEchoServer();
  try {
    const nodeOut = await grab(http2, port, { ':method': 'GET', ':path': '/' });
    const liteOut = await grab(h2lean, port, { ':method': 'GET', ':path': '/' });
    assert.strictEqual(liteOut.data, 'hello world');
    assert.deepStrictEqual(liteOut.headers, nodeOut.headers,
      'response headers differ from what node observed');
  } finally {
    await closeServer(srv);
  }
});

test('POST round-trip: a body inside the initial window echoes back intact', async () => {
  const { srv, port } = await startEchoServer();
  try {
    const payload = 'ping';
    const headers = { ':method': 'POST', ':path': '/', 'content-type': 'text/plain' };
    const nodeOut = await grab(http2, port, headers, payload);
    const liteOut = await grab(h2lean, port, headers, payload);
    assert.strictEqual(liteOut.data, 'echo:ping');
    assert.strictEqual(liteOut.headers['x-echo-len'], String(payload.length));
    assert.deepStrictEqual(liteOut.headers, nodeOut.headers);
  } finally {
    await closeServer(srv);
  }
});

test('large upload: a body past the initial window drives WINDOW_UPDATE flow control',
  async () => {
    const { srv, port } = await startEchoServer();
    try {
      // 200 KB is well past the 65535-octet initial window, so the client must send
      // 65535, stall, wait for the server to consume and return credit, then resume.
      // The whole body has to arrive for the echo length to match -- this is the case
      // that a DATA-before-HEADERS framing bug used to break with GOAWAY(INTERNAL_ERROR).
      const payload = 'x'.repeat(200000);
      const headers = { ':method': 'POST', ':path': '/', 'content-type': 'text/plain' };
      const liteOut = await grab(h2lean, port, headers, payload);
      assert.strictEqual(liteOut.headers['x-echo-len'], String(payload.length),
        `server received ${liteOut.headers['x-echo-len']} of ${payload.length} octets`);
      assert.strictEqual(liteOut.data.length, 'echo:'.length + payload.length);
      assert.strictEqual(liteOut.data, 'echo:' + payload);
    } finally {
      await closeServer(srv);
    }
  });

// A download the client cannot receive in one window, handed over in pieces each
// smaller than the half-window credit step. The point is the client's receive side:
// every read batch leaves the stream owing credit that is NOT yet due, and the debt
// has to survive to a later pass. Release it wrong -- forget the accumulated bytes,
// or hand the WINDOW_UPDATE to the socket in a way the peer rejects -- and node stops
// at its own window and the body arrives truncated (or not at all), on both arms.
test('large download in sub-window pieces: receive credit returns in half-window steps',
  async () => {
    const SIZE = 200000;
    const PIECE = 6000; // < 32767, the half of the default window
    const srv = http2.createServer((req, res) => {
      res.writeHead(200, { 'content-type': 'application/octet-stream' });
      const piece = Buffer.alloc(PIECE, 0x61);
      let sent = 0;
      // one write per check-in: separate DATA frames, separate socket reads, so no
      // single read batch ever carries enough octets to pay the debt by itself
      const next = () => setImmediate(() => {
        if (sent >= SIZE) {
          res.end(); return;
        }
        const n = Math.min(PIECE, SIZE - sent);
        sent += n;
        res.write(n === PIECE ? piece : piece.subarray(0, n));
        next();
      });
      next();
    });
    srv.on('error', () => {});
    const port = await listen(srv);
    try {
      const nodeOut = await grab(http2, port, { ':method': 'GET', ':path': '/' });
      const liteOut = await grab(h2lean, port, { ':method': 'GET', ':path': '/' });
      assert.strictEqual(nodeOut.data.length, SIZE, 'node did not receive the whole body');
      assert.strictEqual(liteOut.data.length, SIZE,
        `h2lean received ${liteOut.data.length} of ${SIZE} octets -- credit stalled`);
      assert.strictEqual(liteOut.data, nodeOut.data);
    } finally {
      await closeServer(srv);
    }
  });

test('many sequential streams on one connection are matched to the right responses',
  async () => {
    const { srv, port } = await startEchoServer();
    try {
      // A single session, several requests back to back: each must land its own DATA
      // on its own stream, with the stream id tracked correctly through the shared reader.
      const sess = h2lean.connect(`http://127.0.0.1:${port}`);
      sess.on('error', () => {});
      const results = [];
      for (let i = 0; i < 8; i++) {
        const r = await new Promise((resolve, reject) => {
          const s = sess.request({ ':method': 'POST', ':path': '/',
            'content-type': 'text/plain' }, { endStream: false });
          let data = '';
          s.on('response', (h) => {
            s._status = h[':status'];
          });
          s.setEncoding('latin1');
          s.on('data', (d) => {
            data += d;
          });
          s.on('error', reject);
          s.on('end', () => resolve({ status: s._status, data }));
          s.end('n' + i);
        });
        results.push(r);
      }
      sess.destroy();
      for (let i = 0; i < 8; i++) {
        assert.strictEqual(results[i].status, 200);
        assert.strictEqual(results[i].data, 'echo:n' + i,
          `stream ${i} got the wrong body: ${results[i].data}`);
      }
    } finally {
      await closeServer(srv);
    }
  });

test('connect() fills the request pseudo-headers the application omitted', async () => {
  const { srv, port } = await startEchoServer();
  try {
    // An empty header set must still be a valid GET / with :scheme and :authority added
    // from the session -- the same completion node performs, so `request()` with no
    // arguments behaves like node's.
    const sess = h2lean.connect(`http://127.0.0.1:${port}`);
    sess.on('error', () => {});
    const s = sess.request({});
    const out = await new Promise((resolve, reject) => {
      let status, data = '';
      s.on('response', (h) => {
        status = h[':status'];
      });
      s.setEncoding('latin1');
      s.on('data', (d) => {
        data += d;
      });
      s.on('error', reject);
      s.on('end', () => resolve({ status, data }));
    });
    sess.destroy();
    assert.strictEqual(out.status, 200);
    assert.strictEqual(out.data, 'hello world');
  } finally {
    await closeServer(srv);
  }
});

// The nghttpd arm: our client against nghttp2's own server. nghttpd resolves a file
// only when it is started with the docroot as its working directory and given `.` (an
// absolute docroot answered 404 even to nghttp's own reference client on this host), so
// the child is spawned with cwd set to the docroot. The strict reference stack accepting
// our exchange end to end -- preface, SETTINGS, HEADERS, the 200 body -- is the point.
test('h2lean client speaks h2c to nghttpd and reads the served file',
  { skip: NGHTTPD ? false : 'nghttpd not installed' }, async () => {
    const docroot = path.join(__dirname, '..', '..', 'out', 'ngd-root');
    const served = 'nghttpd-static-body\n';
    fs.mkdirSync(docroot, { recursive: true });
    fs.writeFileSync(path.join(docroot, 'index.html'), served);
    const port = 8400 + (process.pid % 400);
    const child = spawn('nghttpd', ['--no-tls', String(port), '.'],
      { cwd: docroot, stdio: ['ignore', 'ignore', 'pipe'] });
    let err = '';
    child.stderr.on('data', (d) => {
      err += d;
    });
    const ready = await new Promise((resolve) => {
      const deadline = Date.now() + 2000;
      const probe = () => {
        const sock = netConnect(port, () => {
          sock.destroy(); resolve(true);
        });
        sock.on('error', () => {
          sock.destroy();
          if (Date.now() < deadline) {
            setTimeout(probe, 40);
          } else {
            resolve(false);
          }
        });
      };
      probe();
    });
    try {
      assert.ok(ready, `nghttpd did not come up: ${err}`);
      const out = await new Promise((resolve, reject) => {
        const sess = h2lean.connect(`http://127.0.0.1:${port}`);
        sess.on('error', reject);
        const s = sess.request({ ':method': 'GET', ':path': '/index.html' });
        let status, data = '';
        s.on('response', (h) => {
          status = h[':status'];
        });
        s.setEncoding('latin1');
        s.on('data', (d) => {
          data += d;
        });
        s.on('error', reject);
        s.on('end', () => {
          sess.destroy(); resolve({ status, data });
        });
      });
      assert.strictEqual(out.status, 200, `nghttpd status: ${out.status}, stderr: ${err}`);
      assert.strictEqual(out.data, served);
    } finally {
      child.kill('SIGTERM');
    }
  });

// Small raw-socket probe so the nghttpd readiness wait does not depend on node:http2's
// own connect handshake (which would try to speak HTTP/2 before the listener is ready).
function netConnect(port, onReady) {
  const net = require('node:net');
  const sock = net.connect(port, '127.0.0.1', onReady);
  return sock;
}
