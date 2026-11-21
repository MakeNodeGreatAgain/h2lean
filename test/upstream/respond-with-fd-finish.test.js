// Port of node's `test/parallel/test-http2-respond-with-fd-finish.js` (CONTRIBUTING.md §1:
// the upstream file is a scenario source, node's measured value is the expectation).
//
// Upstream's two blocks, in order:
//
//  1. Clean completion: respondWithFD's 'finish' must reflect the file pipe's EOF, not the
//     moment respondWithFD was called -- 'finish' fires with writableFinished true, 'close'
//     reports writableFinished true, no stream error, and the compat res 'finish' fires too.
//
//  2. Aborted before the pipe completes: the stream is destroyed synchronously after
//     respondWithFD. No 'finish', no 'aborted' (node's closeStream reads the writable's
//     ending flag, and processRespondWithFD calls self.end() before the pump starts, so the
//     legacy criterion "writable was not ending" never holds here), 'close' reports
//     writableFinished false.
//
// DIVERGENCE FROM UPSTREAM'S OWN ASSERTIONS, measured on node v26.12.1: block 2 marks the
// compat res 'finish' as mustNotCall, but a live node server fires it -- the abort shape
// reports [res finish, res close] on the compat side (the request handler runs, the stream
// closes, and onStreamCloseResponse emits res finish + close with only a head-request
// guard). The port pins the measured behavior: the res events fire on both stacks.
//
// The abort shape also differs on the wire in a way that is already this project's
// documented divergence (the queued-HEADERS discard on a same-turn destroy): node drops the
// response it had queued in the turn and the client sees no 'response' event at all, while
// h2lean's HEADERS were framed inside the call and reach the peer. The stream-side and
// compat-side transcripts are pinned per stack; what is pinned on both is the writable
// semantics this file exists for: no 'finish', no 'aborted', 'close' with
// writableFinished false.
//
// Adaptations: the fixture is generated into a per-run temp directory; the duplex-less
// mustCall chain becomes event collection compared across the two stacks; the client is
// node's on both arms. `stream.ownsFd = true` is kept from upstream but is a no-op on both
// stacks -- respondWithFD sets ownsFd false unconditionally (node's core.js does the same),
// so the aborted block's fd is never closed, exactly as upstream leaves it.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const h2lean = require('../../index');
const { listen, closeServer } = require('../diff-harness');

const SIZE = 64;

// Runs one block; reports every stream-side and compat-side event with the state the
// upstream assertions read at the moment they fire.
async function arm(serverLib, abort) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'h2-up-ffin-'));
  const file = path.join(dir, 'small.bin');
  fs.writeFileSync(file, Buffer.alloc(SIZE));
  const srv = serverLib.createServer();
  const port = await listen(srv);
  const seen = { stream: [], res: [], streamErrorWf: null, response: false };
  try {
    await new Promise((resolve) => {
      srv.on('request', (req, res) => {
        res.on('finish', () => seen.res.push('finish'));
        res.on('close', () => seen.res.push('close'));
      });
      srv.on('stream', (stream) => {
        stream.on('finish', () => seen.stream.push('finish'));
        stream.on('aborted', () => seen.stream.push('aborted'));
        stream.on('error', () => {
          seen.stream.push('error');
        });
        stream.on('close', () => {
          seen.stream.push('close');
          seen.streamErrorWf = stream.writableFinished;
          resolve();
        });
        const fd = fs.openSync(file, 'r');
        stream.ownsFd = true;
        stream.respondWithFD(fd, { 'content-length': SIZE });
        if (abort) {
          // Synchronously interrupt the pipe before it can read EOF (upstream's shape).
          stream.destroy();
        }
      });

      const client = http2.connect(`http://127.0.0.1:${port}`);
      client.on('error', () => {});
      const req = client.request({ ':path': '/' });
      req.on('response', () => {
        seen.response = true;
      });
      req.on('error', () => {});
      const chunks = [];
      req.on('data', (chunk) => chunks.push(chunk));
      req.on('close', () => {
        seen.bodyLength = Buffer.concat(chunks).length;
        client.close();
      });
      req.resume();
      req.end();
      setTimeout(resolve, 2000).unref();
    });
  } finally {
    await closeServer(srv);
    fs.rmSync(dir, { recursive: true, force: true });
  }
  return seen;
}

test('upstream: a completed file pipe reports finish and writableFinished', async () => {
  const nodeGot = await arm(http2, false);
  const leanGot = await arm(h2lean, false);
  const want = {
    stream: ['finish', 'close'],
    res: ['finish', 'close'],
    streamErrorWf: true,
    response: true,
    bodyLength: SIZE
  };
  assert.deepStrictEqual(nodeGot, want, `node: ${JSON.stringify(nodeGot)}`);
  assert.deepStrictEqual(leanGot, want, `h2lean: ${JSON.stringify(leanGot)}`);
});

test('upstream: an aborted file pipe reports no finish and no aborted', async () => {
  const nodeGot = await arm(http2, true);
  const leanGot = await arm(h2lean, true);
  // The stream-side and compat-side semantics are the shared claim; the client's view of
  // the response is the queued-HEADERS divergence (see the header): node discards the
  // response queued in the same turn, h2lean delivers the HEADERS it framed in the call.
  const want = {
    stream: ['close'],
    res: ['finish', 'close'],
    streamErrorWf: false,
    bodyLength: 0
  };
  assert.deepStrictEqual(nodeGot, { ...want, response: false },
    `node: ${JSON.stringify(nodeGot)}`);
  assert.deepStrictEqual(leanGot, { ...want, response: true },
    `h2lean: ${JSON.stringify(leanGot)}`);
});
