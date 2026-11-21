// Port of node's `test/parallel/test-http2-respond-file-filehandle.js` (CONTRIBUTING.md §1:
// the upstream file is a scenario source, node's measured value is the expectation).
//
// Upstream's single block: the descriptor is a FileHandle from fs.promises.open, handed
// to respondWithFD() with content-type and content-length; the client asserts the headers
// and reads the file body to the end. The fd-errors port already pins the argument-type
// acceptance; this one pins the FileHandle round trip. The client is node's on both arms
// because the server's file pump is what is under test.
//
// Adaptations: the fixture file is generated into a per-run temp directory instead of
// node's fixtures/elipses.txt; the mustCall chain becomes promise resolution; the
// FileHandle closes when the server closes, as upstream's server 'close' handler does.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const h2lean = require('../../index');
const { listen, closeServer } = require('../diff-harness');

const DATA = Buffer.from('The quick brown fox jumps over the lazy dog.\n'.repeat(4));

async function arm(serverLib) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'h2-up-fhandle-'));
  const file = path.join(dir, 'file.txt');
  fs.writeFileSync(file, DATA);
  const stat = fs.statSync(file);
  const fileHandle = await fs.promises.open(file, 'r');
  const srv = serverLib.createServer();
  const port = await listen(srv);
  let client;
  try {
    srv.on('stream', (stream) => {
      stream.respondWithFD(fileHandle, {
        'content-type': 'text/plain',
        'content-length': stat.size
      });
    });
    await new Promise((resolve, reject) => {
      client = http2.connect(`http://127.0.0.1:${port}`);
      client.on('error', reject);
      const req = client.request();
      req.on('response', (headers) => {
        try {
          assert.strictEqual(headers['content-type'], 'text/plain');
          assert.strictEqual(+headers['content-length'], DATA.length);
        } catch (err) {
          reject(err);
        }
      });
      req.setEncoding('utf8');
      let check = '';
      req.on('data', (chunk) => {
        check += chunk;
      });
      req.on('end', () => {
        try {
          assert.strictEqual(check, DATA.toString('utf8'));
          resolve();
        } catch (err) {
          reject(err);
        }
      });
      req.on('error', reject);
      req.end();
    });
  } finally {
    // node 20/22 leave this client connection open after the server closes: without
    // the destroy the file's process keeps a live socket and the runner sits on it
    // until the per-file timeout.
    client.destroy();
    await closeServer(srv);
    await fileHandle.close();
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

test('upstream: respondWithFD accepts a FileHandle', async () => {
  await arm(http2);
  await arm(h2lean);
});
