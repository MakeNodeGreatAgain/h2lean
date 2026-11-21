// Port of node's `test/parallel/test-http2-respond-file-fd-invalid.js` (CONTRIBUTING.md §1:
// the upstream file is a scenario source, node's measured value is the expectation).
//
// Upstream's single block: the server picks the first file descriptor fstatSync refuses,
// hands it to respondWithFD(), and the read failure closes the stream with
// NGHTTP2_INTERNAL_ERROR -- the server stream reports ERR_HTTP2_STREAM_ERROR, the client
// still sees the response HEADERS (the submission is synchronous, the read is not) and
// then the RST via req.rstCode, with no data and no end. The client is node's on both
// arms because the file-pump error path is what is under test.
//
// Adaptations: the bad fd is found the same way (fs.fstatSync(++fd) in a loop); the
// expectsError check becomes explicit code/name/message assertions; the mustCall chain
// becomes collected events with the same expectations.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const fs = require('node:fs');
const h2lean = require('../../index');
const { listen, closeServer } = require('../diff-harness');

const { NGHTTP2_INTERNAL_ERROR } = http2.constants;

function expectStreamError(err) {
  assert.strictEqual(err.code, 'ERR_HTTP2_STREAM_ERROR');
  assert.strictEqual(err.name, 'Error');
  assert.strictEqual(err.message, 'Stream closed with error code NGHTTP2_INTERNAL_ERROR');
}

async function arm(serverLib) {
  const srv = serverLib.createServer();
  const port = await listen(srv);
  try {
    await new Promise((resolve) => {
      let serverDone = false;
      let clientDone = false;
      const finish = () => {
        if (serverDone && clientDone) {
          resolve();
        }
      };
      srv.on('stream', (stream) => {
        let fd = 2;
        // Get first known bad file descriptor.
        try {
          while (fs.fstatSync(++fd)) {
            // fstatSync succeeding means the fd is taken; keep going
          }
        } catch {
          // Do nothing; we now have an invalid fd
        }
        stream.respondWithFD(fd);
        stream.on('error', (err) => {
          expectStreamError(err);
          serverDone = true;
          finish();
        });
      });

      const client = http2.connect(`http://127.0.0.1:${port}`);
      client.on('error', () => {});
      const req = client.request();
      req.on('response', () => {});
      req.on('error', expectStreamError);
      req.on('data', () => {
        assert.fail('no data may reach the client');
      });
      req.on('end', () => {
        assert.fail('no end may reach the client');
      });
      req.on('close', () => {
        assert.strictEqual(req.rstCode, NGHTTP2_INTERNAL_ERROR);
        clientDone = true;
        finish();
      });
      req.end();
    });
  } finally {
    await closeServer(srv);
  }
}

test('upstream: a bad fd closes the stream with NGHTTP2_INTERNAL_ERROR', async () => {
  await arm(http2);
  await arm(h2lean);
});
