// Port of node's `test/parallel/test-http2-respond-file-fd-range.js` (CONTRIBUTING.md §1:
// the upstream file is a scenario source, node's measured value is the expectation).
//
// Upstream's single block: a byte range is requested through the `range` header, parsed
// into offset/length for respondWithFD's statCheck, which clamps the advertised
// content-length to what the file actually holds past the offset (the comment upstream
// carries says it plainly: this is not a proper Range implementation, it is the minimal
// byte-range shape). Two requests: bytes=8-11 (three octets) and bytes=8-28 (twenty
// asked, nine held). The client is node's on both arms because the pump's offset/length
// handling is what is under test.
//
// Adaptations: the fixture is 17 octets of 'a' generated into a per-run temp directory
// instead of node's fixtures/printA.js (its length is what the clamps are computed
// against); the mustCall/countdown chain becomes promise resolution over both requests.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const h2lean = require('../../index');
const { listen, closeServer } = require('../diff-harness');

const DATA = Buffer.from('a'.repeat(17));

function getOffsetLength(range) {
  if (range === undefined) {
    return [0, -1];
  }
  const r = /bytes=(\d+)-(\d+)/.exec(range);
  return [+r[1], +r[2] - +r[1]];
}

async function arm(serverLib) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'h2-up-frange-'));
  const file = path.join(dir, 'file.txt');
  fs.writeFileSync(file, DATA);
  const fd = fs.openSync(file, 'r');
  const srv = serverLib.createServer();
  const port = await listen(srv);
  let client;
  try {
    srv.on('stream', (stream, headers) => {
      const [offset, length] = getOffsetLength(headers.range);
      stream.respondWithFD(fd, {
        'content-type': 'text/plain'
      }, {
        statCheck: (s, hdrs, options) => {
          assert.strictEqual(options.length, length);
          assert.strictEqual(options.offset, offset);
          hdrs['content-length'] = Math.min(options.length, s.size - offset);
        },
        offset,
        length
      });
    });
    await new Promise((resolve, reject) => {
      let pending = 2;
      const done = () => {
        if (--pending === 0) {
          resolve();
        }
      };
      client = http2.connect(`http://127.0.0.1:${port}`);
      client.on('error', reject);
      const run = (range, wantLength, wantBody) => {
        const req = client.request({ range });
        req.on('response', (headers) => {
          try {
            assert.strictEqual(headers['content-type'], 'text/plain');
            assert.strictEqual(+headers['content-length'], wantLength);
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
            assert.strictEqual(check, DATA.toString('utf8', 8, 8 + wantBody));
            done();
          } catch (err) {
            reject(err);
          }
        });
        req.on('error', reject);
        req.end();
      };
      run('bytes=8-11', 3, 3);
      run('bytes=8-28', 9, 9);
    });
  } finally {
    // node 20/22 leave this client connection open after the server closes: without
    // the destroy the file's process keeps a live socket and the runner sits on it
    // until the per-file timeout.
    client.destroy();
    await closeServer(srv);
    fs.closeSync(fd);
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

test('upstream: respondWithFD serves a requested byte range', async () => {
  await arm(http2);
  await arm(h2lean);
});
