// Port of node's `test/parallel/test-http2-respond-file-push.js` (CONTRIBUTING.md §1: the
// upstream file is a scenario source, node's measured value is the expectation).
//
// Upstream's single block: the main stream answers empty with respond({}) + end(), then
// pushStream()s a child whose answer is respondWithFD() with content-type, content-length
// and last-modified. The client watches its 'stream' event, asserts the pushed 'push'
// headers and reads the pushed body to the end; both the main request and the push must
// complete before anything closes. The client is node's on both arms because the server's
// pushed-file pump is what is under test.
//
// Adaptations: the fixture file is generated into a per-run temp directory instead of
// node's fixtures/elipses.txt; upstream calls stream.end() twice in the handler (its own
// quirk -- the second end() is a no-op) and that shape is kept; the mustCall/mustSucceed
// chain becomes promise resolution; the fd is closed on server close like upstream's
// server 'close' handler does.
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
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'h2-up-fpush-'));
  const file = path.join(dir, 'file.txt');
  fs.writeFileSync(file, DATA);
  const stat = fs.statSync(file);
  const fd = fs.openSync(file, 'r');
  const srv = serverLib.createServer();
  const port = await listen(srv);
  let client;
  try {
    await new Promise((resolve, reject) => {
      srv.on('stream', (stream) => {
        stream.respond({});
        stream.end();

        stream.pushStream({
          ':path': '/file.txt',
          ':method': 'GET'
        }, (err, pushed) => {
          if (err) {
            reject(err);
            return;
          }
          pushed.respondWithFD(fd, {
            'content-type': 'text/plain',
            'content-length': stat.size,
            'last-modified': stat.mtime.toUTCString()
          });
        });

        stream.end();
      });
      srv.on('close', () => fs.closeSync(fd));

      client = http2.connect(`http://127.0.0.1:${port}`);
      client.on('error', reject);
      let pending = 2;
      const done = () => {
        if (--pending === 0) {
          resolve();
        }
      };

      const req = client.request({});
      req.on('response', () => {});
      req.resume();
      req.on('end', done);

      client.on('stream', (stream) => {
        stream.on('push', (headers) => {
          try {
            assert.strictEqual(headers['content-type'], 'text/plain');
            assert.strictEqual(+headers['content-length'], DATA.length);
            assert.strictEqual(headers['last-modified'], stat.mtime.toUTCString());
          } catch (err) {
            reject(err);
          }
        });
        stream.setEncoding('utf8');
        let check = '';
        stream.on('data', (chunk) => {
          check += chunk;
        });
        stream.on('end', () => {
          try {
            assert.strictEqual(check, DATA.toString('utf8'));
            done();
          } catch (err) {
            reject(err);
          }
        });
      });

      req.end();
    });
  } finally {
    // node 20/22 leave this client connection open after the server closes: without
    // the destroy the file's process keeps a live socket and the runner sits on it
    // until the per-file timeout.
    client.destroy();
    await closeServer(srv);
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

test('upstream: a pushed stream is answered by respondWithFD', async () => {
  await arm(http2);
  await arm(h2lean);
});
