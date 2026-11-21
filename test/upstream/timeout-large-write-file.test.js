// Port of node's `test/sequential/test-http2-timeout-large-write-file.js` (CONTRIBUTING.md
// §1: the upstream file is a scenario source, node's measured value is the expectation).
//
// Upstream's single block: a 3 MB file leaves through respondWithFD() to a client with a
// 65535-octet initial window, and the client pauses reading for longer than the server
// timeout once 500 KB have arrived, in cycles, until the file is done. The server has a
// 500 ms timeout and asserts on every 'timeout' event that data was not still flowing --
// the timeout must never fire while the backing stream is mid-write, because the
// in-flight response is what the timer is measuring against. The client is node's on
// both arms because the server's timeout-vs-write interaction is what is under test.
//
// Adaptations: the file is written into a per-run temp directory instead of the shared
// tmpdir fixture; the TLS pair comes from test/fixtures-tls.js (node's agent1 pair is
// not available here; the client connects with rejectUnauthorized:false exactly as
// upstream does); the mustCallAtLeast(0) semantics become an assertion on every
// 'timeout' event; platformTimeout is 1:1 (the multipliers apply to slow CI platforms,
// not to this run).
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const h2lean = require('../../index');
const certificates = require('../fixtures-tls');
const { closeServer } = require('../diff-harness');

const writeSize = 3000000;
const minReadSize = 500000;
const serverTimeout = 500;
let offsetTimeout = 100;
let didReceiveData = false;
const content = Buffer.alloc(writeSize, 0x44);

async function arm(serverLib) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'h2-up-tlw-'));
  const filepath = path.join(dir, 'http2-large-write.tmp');
  fs.writeFileSync(filepath, content);
  const fd = fs.openSync(filepath, 'r');
  const { key, cert } = certificates();
  const server = serverLib.createSecureServer({
    key,
    cert
  });
  server.on('stream', (stream) => {
    stream.respondWithFD(fd, {
      'content-type': 'application/octet-stream',
      'content-length': content.length.toString(),
      'vary': 'Accept-Encoding'
    });
    stream.end();
  });
  server.setTimeout(serverTimeout);
  server.on('timeout', () => {
    assert.ok(!didReceiveData, 'Should not timeout');
  });
  const port = await new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve(server.address().port));
  });
  try {
    await new Promise((resolve, reject) => {
      const client = http2.connect(`https://localhost:${port}`, {
        rejectUnauthorized: false,
        settings: { initialWindowSize: 65535 }
      });
      client.on('error', reject);
      const req = client.request({ ':path': '/' });
      req.end();
      const resume = () => req.resume();
      let receivedBufferLength = 0;
      let firstReceivedAt;
      req.on('data', (buf) => {
        if (receivedBufferLength === 0) {
          didReceiveData = false;
          firstReceivedAt = Date.now();
        }
        receivedBufferLength += buf.length;
        if (receivedBufferLength >= minReadSize &&
            receivedBufferLength < writeSize) {
          didReceiveData = true;
          receivedBufferLength = 0;
          req.pause();
          setTimeout(
            resume,
            serverTimeout + offsetTimeout - (Date.now() - firstReceivedAt)
          );
          offsetTimeout = 0;
        }
      });
      req.on('end', () => {
        client.close();
        resolve();
      });
      req.on('error', reject);
    });
  } finally {
    await closeServer(server);
    fs.closeSync(fd);
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

test('upstream: a large write outlives the server timeout without firing it', async () => {
  try {
    await arm(http2);
    await arm(h2lean);
  } finally {
    didReceiveData = false;
  }
});
