// What `stream.pipe()` does on the server `stream`-API Http2Stream, measured against a live
// node:http2 server in this same run.
//
// node's Http2Stream extends Duplex (internal/http2/core.js:2128), so a server stream is both a
// pipe SOURCE (request body -> dest) and a pipe DEST (source -> response body). h2lean's server
// stream extends `stream.Stream` (lib/stream.js) -- the legacy EventEmitter-derived base, chosen
// over Duplex on the measurement in OPTIMIZATION.md item 41 -- which supplies pipe() through
// Stream.prototype.pipe while leaving the Duplex machinery (two state objects, the nextTick read
// loop) out. The stream is also readable (data/end) and writable (write/end returning a real
// backpressure boolean, 'drain' emitted when the peer returns flow-control credit in
// lib/session.js). The three shapes below are what that choice buys and what it does not:
//
//   * DEST side (`source.pipe(stream)`): pipe() is read off the SOURCE and drives the dest through
//     write()/end/'drain' -- already present. A body past the send window (102400 octets over the
//     65535 initial window) is delivered IN FULL and ends cleanly on BOTH stacks. Not a gap.
//   * SOURCE side (`stream.pipe(sink)`): this is what `extends Stream` fixes. The legacy
//     Stream.prototype.pipe forwards the readable half to a dest, so the request body arrives in
//     full and the sink finishes -- parity with node, where the EventEmitter base used to throw
//     `stream.pipe is not a function`.
//   * SOURCE destroyed mid-pipe: the residual, and the reason a bare base is not the whole Duplex.
//     node's Duplex pipe tears the pipe down as pipe,finish,unpipe,close on the dest (it ends the
//     dest and unpipes it as the source dies). The legacy Stream.prototype.pipe only reads the
//     source 'end' to call dest.end(), so a destroy that never emits 'end' leaves the dest at
//     pipe,close -- no 'finish', no 'unpipe'. The delivered bytes and rstCode still agree.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const { Readable, Writable } = require('node:stream');
const nodeHttp2 = require('node:http2');
const h2lean = require('../index');
const { closeServer } = require('./diff-harness');

const PIECE = 'x'.repeat(512);
const N = 200; // 200 * 512 = 102400 octets, past the 65535 initial send window

// The fixed client for every shape is node's own, so the only variable is the server arm.
function bigSource(n) {
  let i = 0;
  return new Readable({ read() {
    this.push(i++ < n ? PIECE : null);
  } });
}

// source-side: the handler pipes the uploaded request body into a sink and reports what the
// sink received (or that pipe threw). 12 octets are uploaded.
function driveSource(impl) {
  return new Promise((resolve) => {
    let done = false;
    const srv = impl.createServer();
    srv.on('stream', (stream) => {
      stream.on('error', () => {});
      const chunks = [];
      const sink = new Writable({ write(c, _e, cb) {
        chunks.push(c); cb();
      } });
      let finish = false;
      sink.on('finish', () => {
        finish = true;
      });
      let threw = null;
      try {
        stream.pipe(sink);
      } catch {
        threw = true; stream.resume();
      }
      setTimeout(() => {
        if (done) {
          return;
        } done = true;
        resolve({ threw, delivered: Buffer.concat(chunks).length, finish });
        if (!stream.destroyed) {
          stream.end('ack');
        }
        client.destroy(); closeServer(srv).then(() => {});
      }, 150);
    });
    let client;
    srv.listen(0, '127.0.0.1', () => {
      client = nodeHttp2.connect(`http://127.0.0.1:${srv.address().port}`);
      client.on('error', () => {});
      const r = client.request({ ':method': 'POST', ':path': '/' });
      r.on('error', () => {}); r.resume();
      r.end('hello-upload'); // 12 octets
    });
    setTimeout(() => {
      if (!done) {
        done = true; resolve({ note: 'deadline' });
      }
    }, 900);
  });
}

// dest-side: the handler pipes a body larger than the send window into the response stream;
// the client consumes it whole. Reports the bytes that arrived and whether it ended.
function driveDest(impl) {
  return new Promise((resolve) => {
    let drains = 0;
    const srv = impl.createServer();
    srv.on('stream', (stream) => {
      stream.on('drain', () => {
        drains++;
      });
      stream.on('error', () => {});
      stream.respond({ ':status': 200 });
      try {
        bigSource(N).pipe(stream);
      } catch { /* node arm never throws here */ }
    });
    srv.listen(0, '127.0.0.1', () => {
      const client = nodeHttp2.connect(`http://127.0.0.1:${srv.address().port}`);
      client.on('error', () => {});
      const r = client.request({ ':method': 'GET', ':path': '/' });
      let got = 0; let ended = false;
      r.on('data', (d) => {
        got += d.length;
      });
      r.on('end', () => {
        ended = true;
      });
      r.on('error', () => {});
      r.on('close', () => {
        client.destroy();
        closeServer(srv).then(() => resolve({ got, ended, drains }));
      });
      r.end();
    });
  });
}

// source destroyed mid-pipe: the handler pipes the request body into a sink, then destroys the
// stream after the first chunk. Reports the event the DEST (sink) observed, in order.
function driveSourceError(impl) {
  return new Promise((resolve) => {
    let done = false;
    const sinkEvents = [];
    const srv = impl.createServer();
    srv.on('stream', (stream) => {
      stream.on('error', () => {});
      const sink = new Writable({ write(_c, _e, cb) {
        cb();
      } });
      for (const ev of ['error', 'close', 'finish', 'unpipe', 'pipe']) {
        sink.on(ev, () => sinkEvents.push(ev));
      }
      let threw = null; let seen = false;
      stream.on('data', () => {
        if (seen) {
          return;
        }
        seen = true;
        try {
          stream.pipe(sink);
        } catch {
          threw = true;
        }
        if (threw === null) {
          setImmediate(() => stream.destroy());
        }
      });
      setTimeout(() => {
        if (done) {
          return;
        } done = true;
        resolve({ threw, rst: stream.rstCode, sinkEvents: sinkEvents.slice() });
        client.destroy(); closeServer(srv).then(() => {});
      }, 200);
    });
    let client;
    srv.listen(0, '127.0.0.1', () => {
      client = nodeHttp2.connect(`http://127.0.0.1:${srv.address().port}`);
      client.on('error', () => {});
      const r = client.request({ ':method': 'POST', ':path': '/' });
      r.on('error', () => {}); r.resume();
      r.write(Buffer.alloc(200000, 65)); // several frames; destroyed after the first arrives
      r.end();
    });
    setTimeout(() => {
      if (!done) {
        done = true; resolve({ note: 'deadline', sinkEvents: sinkEvents.slice() });
      }
    }, 1000);
  });
}

test('pipe as a DEST delivers a body past the send window on both stacks', async () => {
  const node = await driveDest(nodeHttp2);
  assert.strictEqual(node.got, N * PIECE.length, `node: ${JSON.stringify(node)}`);
  assert.strictEqual(node.ended, true, `node ended: ${JSON.stringify(node)}`);
  assert.ok(node.drains >= 1, `node drained: ${JSON.stringify(node)}`);
  const ours = await driveDest(h2lean);
  // Already at parity: write() + 'drain' exist, so a source piped INTO the stream works even
  // without the stream having its own pipe(). The byte total and clean end are the point.
  assert.strictEqual(ours.got, node.got, `h2lean: ${JSON.stringify(ours)}`);
  assert.strictEqual(ours.ended, true, `h2lean ended: ${JSON.stringify(ours)}`);
});

test('pipe as a SOURCE delivers the request body on both stacks', async () => {
  const node = await driveSource(nodeHttp2);
  assert.strictEqual(node.threw, null, `node threw: ${JSON.stringify(node)}`);
  assert.strictEqual(node.delivered, 12, `node delivered: ${JSON.stringify(node)}`);
  assert.strictEqual(node.finish, true, `node sink finished: ${JSON.stringify(node)}`);
  const ours = await driveSource(h2lean);
  // The gap `extends Stream` closes: Stream.prototype.pipe forwards the readable half to a dest,
  // so the uploaded body arrives in full and the sink finishes, exactly as on node.
  assert.strictEqual(ours.threw, null, `h2lean: ${JSON.stringify(ours)}`);
  assert.strictEqual(ours.delivered, node.delivered, `h2lean delivered: ${JSON.stringify(ours)}`);
  assert.strictEqual(ours.finish, true, `h2lean sink finished: ${JSON.stringify(ours)}`);
});

test('KNOWN DIVERGENCE: a source destroyed mid-pipe leaves the dest at pipe,close here', async () => {
  const node = await driveSourceError(nodeHttp2);
  assert.strictEqual(node.threw, null, `node threw: ${JSON.stringify(node)}`);
  assert.deepStrictEqual(node.sinkEvents, ['pipe', 'finish', 'unpipe', 'close'],
    `node sink events: ${JSON.stringify(node)}`);
  const ours = await driveSourceError(h2lean);
  assert.strictEqual(ours.threw, null, `h2lean: ${JSON.stringify(ours)}`);
  assert.strictEqual(ours.rst, node.rst, `h2lean rst: ${JSON.stringify(ours)}`);
  // The residual of the bare base: legacy Stream.prototype.pipe ends the dest only on the source
  // 'end'. A destroy() never emits 'end' and legacy pipe has no unpipe-on-dest-close, so the dest
  // sees pipe,close -- node's Duplex pipe additionally ends it (finish) and unpipes it. The bytes
  // already delivered and rstCode agree; only the dest teardown differs. Closing it needs the
  // Duplex pipe, i.e. option B, which item 41 measured as a per-request cost this base avoids.
  assert.deepStrictEqual(ours.sinkEvents, ['pipe', 'close'],
    `h2lean sink events: ${JSON.stringify(ours)}`);
});
