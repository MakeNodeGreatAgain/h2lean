// Port of node's `test/parallel/test-http2-large-write-destroy.js` (CONTRIBUTING.md §1:
// the upstream file is a scenario source, node's measured value is the expectation).
//
// The scenario answers a request with a content-length promise double the body it then
// writes, with waitForTrailers set, writes 60000 octets and destroys the stream in the
// same turn. Upstream's point is that destroy() must reach the wire as RST_STREAM rather
// than crash the C++ layer; the differential point is what each side reports afterwards.
//
// Measured on v26.12.1, the wire is the same on both stacks: HEADERS (no END_STREAM),
// four DATA frames of 16384/16384/16384/10848, then RST_STREAM with code 0. The promise
// of 120000 octets is never kept and never reported -- a reset ends the message, so the
// content-length check does not apply. 'wantTrailers' does not fire on either stack.
//
// What the scenario turns on is the client's reading of RST_STREAM(0). node treats a
// NO_ERROR reset as a clean end: onStreamClose pushes null and defers the destroy to
// 'end' ("Preserve buffered reads after a clean reset", internal/http2/core.js), so the
// whole body is delivered and 'end' fires before 'close' with rstCode 0. The server
// stream reports 'aborted' -- closeStream turns on whether end() was *called*, and
// destroy() called it -- and closes with rstCode 0, no error.
//
// The teardown is upstream's: client.close() then server.close(). An abrupt
// client.destroy() is a different scenario -- node's session.destroy submits a GOAWAY
// first, and the peer reports 'goaway' where an h2lean client's socket reset reports
// ECONNRESET -- so it is not used here.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const h2lean = require('../../index');
const certificates = require('../fixtures-tls');
const { listen, closeServer } = require('../diff-harness');
const { STREAM_HWM_64K } = require('../node-version');

// node's measured transcript for the upstream scenario, so a change in either stack has
// to be a decision rather than a drift. The body's chunk count is deliberately not
// recorded: it depends on how the reads segment, not on the protocol.
const UPSTREAM = [
  'cli-close 0',
  'cli-data 60000',
  'cli-end',
  'cli-response 200 cl=120000',
  'srv-aborted',
  'srv-close 0',
  'srv-data 0',
  'write-returned true'
];

// One exchange, recorded as a sorted line set. `n` is the body length; the content-length
// promise is double it, as upstream writes it.
function arm(lib, n) {
  const { key, cert } = certificates();
  const content = Buffer.alloc(n, 0x44);
  const t = [];
  // Recording stops when the exchange is over: the graceful teardown below closes
  // sessions that a served exchange would otherwise report as lines.
  let recording = true;
  const push = (line) => {
    if (recording) {
      t.push(line);
    }
  };
  let client;
  let finished = false;
  let resolve;
  const done = new Promise((r) => {
    resolve = r;
  });
  const srv = lib.createSecureServer({ key, cert });
  const finish = () => {
    if (finished) {
      return;
    }
    finished = true;
    recording = false;
    clearTimeout(timer);
    if (client) {
      client.close();
    }
    closeServer(srv).then(() => resolve(t.sort()));
  };
  // A transcript that never arrives is a line, not a cancelled file.
  const timer = setTimeout(() => {
    push('HANG');
    finish();
  }, 8000);
  srv.on('error', () => {});
  srv.on('session', (s) => {
    s.on('error', (e) => push(`sess-error ${e.code}`));
    s.on('close', () => push(`sess-close ${s.goawayCode}`));
  });
  srv.on('stream', (stream) => {
    let data = 0;
    stream.on('data', () => {
      data++;
    });
    stream.on('error', (e) => push(`srv-error ${e.code}`));
    stream.on('aborted', () => push('srv-aborted'));
    stream.on('wantTrailers', () => push('srv-wantTrailers'));
    stream.on('close', () => {
      push(`srv-data ${data}`);
      push(`srv-close ${stream.rstCode}`);
    });
    stream.respond({
      'content-type': 'application/octet-stream',
      'content-length': String(content.length * 2),
      vary: 'Accept-Encoding'
    }, { waitForTrailers: true });
    push(`write-returned ${stream.write(content)}`);
    stream.destroy();
  });
  listen(srv).then((port) => {
    client = lib.connect(`https://127.0.0.1:${port}`, { rejectUnauthorized: false });
    client.on('error', () => {});
    client.on('close', () => push('cli-sess-close'));
    const req = client.request({ ':path': '/' });
    req.on('response', (h) => push(`cli-response ${h[':status']} cl=${h['content-length']}`));
    let bytes = 0;
    req.on('data', (c) => {
      bytes += c.length;
    });
    req.on('aborted', () => push('cli-aborted'));
    req.on('error', (e) => push(`cli-error ${e.code}`));
    req.on('end', () => push('cli-end'));
    req.on('close', () => {
      push(`cli-data ${bytes}`);
      push(`cli-close ${req.rstCode}`);
      setTimeout(finish, 150);
    });
    req.end();
    // Upstream's comment: otherwise close is not emitted while data is pending.
    req.resume();
  });
  return done;
}

test('upstream large-write-destroy: a 60000-octet write then destroy() is a clean reset', async () => {
  // 60000 octets fit the 65536 highWaterMark (node 22+, and h2lean on every version) so
  // write() returns true; node 20's 16384 mark reports false. Only the node-side line is
  // gated -- h2lean's own transcript stays the 65536 one (test/node-version.js).
  const nodeWant = STREAM_HWM_64K ? UPSTREAM
    : UPSTREAM.map((l) => l === 'write-returned true' ? 'write-returned false' : l);
  const native = await arm(http2, 60000);
  const ours = await arm(h2lean, 60000);
  assert.deepStrictEqual(native, nodeWant);
  assert.deepStrictEqual(ours, UPSTREAM);
});

test('a body over the send window goes out as far as the credit allows, then resets', async () => {
  // 200000 octets against the peer's 65535 initial window: write() reports backpressure
  // and only the credited part reaches the client before the reset ends the message.
  const want = [
    'cli-close 0',
    'cli-data 65535',
    'cli-end',
    'cli-response 200 cl=400000',
    'srv-aborted',
    'srv-close 0',
    'srv-data 0',
    'write-returned false'
  ];
  const native = await arm(http2, 200000);
  const ours = await arm(h2lean, 200000);
  assert.deepStrictEqual(native, want);
  assert.deepStrictEqual(ours, native);
});
