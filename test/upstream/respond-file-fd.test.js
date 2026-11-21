// Port of node's `test/parallel/test-http2-respond-file-fd.js` (CONTRIBUTING.md §1: the
// upstream file is a scenario source, node's measured value is the expectation).
//
// Upstream's single block: a server answers a request with respondWithFD() carrying
// content-type and content-length, and the client reads the whole body back off the fd;
// the fd is closed when the server closes. The fixture file is generated into a per-run
// temp directory instead of node's fixtures/; the client is node's on both arms because
// the server's file pump is what is under test; upstream's mustCall chain becomes
// promise resolution.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const h2lean = require('../../index');
const { listen, closeServer } = require('../diff-harness');

const DATA = Buffer.from('The quick brown fox jumps over the lazy dog.\n'.repeat(8));

async function arm(serverLib) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'h2-up-ffd-'));
  const file = path.join(dir, 'file');
  fs.writeFileSync(file, DATA);
  const fd = fs.openSync(file, 'r');
  const srv = serverLib.createServer();
  const port = await listen(srv);
  try {
    await new Promise((resolve, reject) => {
      srv.on('stream', (stream) => {
        stream.respondWithFD(fd, {
          'content-type': 'text/plain; charset=utf-8',
          'content-length': DATA.length
        });
      });
      srv.on('close', () => fs.closeSync(fd));

      const client = http2.connect(`http://127.0.0.1:${port}`);
      client.on('error', reject);
      const req = client.request({ ':path': '/' });
      req.on('response', (headers) => {
        try {
          assert.strictEqual(headers[':status'], 200);
          assert.strictEqual(headers['content-type'], 'text/plain; charset=utf-8');
          assert.strictEqual(+headers['content-length'], DATA.length);
        } catch (err) {
          reject(err);
        }
      });
      req.setEncoding('utf8');
      const chunks = [];
      req.on('data', (chunk) => chunks.push(chunk));
      req.on('end', () => {
        try {
          assert.strictEqual(chunks.join(''), DATA.toString('utf8'));
          resolve();
        } catch (err) {
          reject(err);
        }
      });
      req.on('error', reject);
      req.end();
    });
  } finally {
    await closeServer(srv);
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

test('upstream: respondWithFD serves the whole fd with the declared headers', async () => {
  await arm(http2);
  await arm(h2lean);
});
