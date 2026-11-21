// Port of node's `test/parallel/test-http2-file-reset-pending-write.js` (CONTRIBUTING.md
// §1: the upstream file is a scenario source, node's measured value is the expectation).
//
// Upstream's block (run for respondWithFD and again for respondWithFile): a 1 MB file
// leaves over an in-memory duplex pair, the server-side socket's _write callback is
// deferred once the client has reset the stream, the client cancels after its first
// data, and the stream must close anyway -- then the held write callback is invoked
// after ten setImmediate turns and must be safe to call. The point is that stream
// teardown does not wait for a native write whose callback nobody answered yet.
// The client is node's on both arms; the server's reset-vs-pending-write interplay is
// what is under test.
//
// Adaptations: the fixture file is generated into a per-run temp directory; the gc()
// calls are dropped (the flag they need is not part of the suite command line and the
// assertion here is the callback surviving, not wrapper collection); the mustCall
// chain becomes promise resolution.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { duplexPair } = require('node:stream');
const { setImmediate } = require('node:timers/promises');
const h2lean = require('../../index');

const CONTENT = Buffer.alloc(1024 * 1024);

async function run(useFD) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'h2-up-rpw-'));
  const filename = path.join(dir, 'http2-file');
  fs.writeFileSync(filename, CONTENT);
  try {
    const [clientSide, serverSide] = duplexPair();
    let reset = false;
    let pendingWrite;
    const originalWrite = serverSide._write;
    serverSide._write = function (buf, enc, cb) {
      originalWrite.call(this, buf, enc, () => {
        // Deliver the data, but allow the client to reset the stream before
        // completing the corresponding write.
        setImmediate().then(() => {
          if (reset) {
            assert.strictEqual(pendingWrite, undefined);
            pendingWrite = cb;
          } else {
            cb();
          }
        });
      });
    };

    const server = h2lean.createServer();
    const fd = useFD ? fs.openSync(filename, 'r') : undefined;
    const streamClosed = new Promise((resolve) => {
      server.on('stream', (stream) => {
        stream.on('close', resolve);
        if (useFD) {
          stream.respondWithFD(fd);
        } else {
          stream.respondWithFile(filename);
        }
      });
    });
    server.emit('connection', serverSide);

    const client = http2.connect('http://localhost', {
      createConnection: () => clientSide,
      settings: { initialWindowSize: 1024 * 1024 }
    });
    client.on('error', () => {});
    client.on('connect', () => {
      client.setLocalWindowSize(1024 * 1024);
    });
    const req = client.request();
    req.once('data', () => {
      reset = true;
      req.close(http2.constants.NGHTTP2_CANCEL);
    });
    req.on('error', () => {});
    req.resume();

    await streamClosed;
    // Let deferred cleanup and unpipe callbacks run, then invoke the wrapper while
    // its write is still awaiting completion.
    for (let i = 0; i < 10; i++) {
      await setImmediate();
    }
    assert.strictEqual(typeof pendingWrite, 'function');
    reset = false;
    pendingWrite();
    pendingWrite = undefined;
    await setImmediate();

    clientSide.destroy();
    server.close();
    if (useFD) {
      fs.closeSync(fd);
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

test('upstream: a reset stream closes with a pending file write outstanding', async () => {
  await run(false);
  await run(true);
});
