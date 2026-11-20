// Port of node's `test/parallel/test-http2-exceeds-server-trailer-size.js` (CONTRIBUTING.md
// §1: the upstream file is a scenario source, node's measured value is the expectation).
//
// The scenario answers a request with `respond(undefined, {waitForTrailers: true})`, then
// sends a 64 KiB trailer value from 'wantTrailers'. That block is over the outbound bound,
// and the point of the test is *how* the refusal is reported: not a throw out of
// sendTrailers(), but a 'frameError' on the server stream, an ERR_HTTP2_STREAM_ERROR naming
// NGHTTP2_FRAME_SIZE_ERROR on both halves, rstCode 6 on both, and a session close behind it.
// Upstream also pins the two events that must NOT fire: 'frameError' on the client stream
// and on the client session (the error is the server's, and reaches the client as a
// RST_STREAM), and 'data' on the server stream (the request carried no body).
//
// The bound itself is pinned beside the scenario, because the scenario only shows one side
// of it: nghttp2 compares an estimate -- 12 + 12*fields + sum(name.length + value.length),
// plus 5 for HEADERS -- against maxSendHeaderBlockLength (default 65536), so for a
// {'test-trailer': 'X'.repeat(n)} block the estimate is 41+n. Under a bound of 200, n=159 is
// served and n=160 refused; under the default, n=65495 is served and n=65496 refused. The
// estimate counts string lengths, not UTF-8 octets: 'é' and '€' values fall on the same line.
//
// KNOWN DIVERGENCE, not pinned here: with NO reader on the server stream (no 'data'
// listener, no resume(), no 'readable'), node's own unread-request auto-close wins the race
// against the deferred error close and the stream reports rstCode 0 with no 'error', while
// h2lean reports 6. The two stacks schedule the same pair in the opposite order -- node's
// writable 'finish' schedules the auto-close before the frame pump refuses the block,
// h2lean frames inside the send call and records the half-close after it. Same class as the
// graceful-close divergence in AGENTS.md §10. The upstream scenario attaches 'data', which
// is the shape both stacks agree on.
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const h2lean = require('../../index');
const { listen, closeServer } = require('../diff-harness');

const SIZE_ERROR = 'ERR_HTTP2_STREAM_ERROR:Stream closed with error code NGHTTP2_FRAME_SIZE_ERROR';

// One exchange, recorded as a sorted line set. `ch` repeats into the trailer value, so the
// multi-byte cases run through the same driver as the ASCII ones; `body` octets ahead of it
// stall the send window, which is what makes sendTrailers() stash the block.
function arm(lib, n, bound, ch = 'X', body = 0) {
  const opts = bound === undefined ? {} : { maxSendHeaderBlockLength: bound };
  const value = ch.repeat(n);
  const t = [];
  // Recording stops when the exchange is over: the teardown below (client.destroy,
  // closeServer) closes sessions that a served exchange would otherwise report as lines.
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
  const srv = lib.createServer(opts);
  const finish = () => {
    if (finished) {
      return;
    }
    finished = true;
    recording = false;
    clearTimeout(timer);
    if (client) {
      client.destroy();
    }
    closeServer(srv).then(() => resolve(t.sort()));
  };
  // A transcript that never arrives is a line, not a cancelled file.
  const timer = setTimeout(() => {
    push('HANG');
    finish();
  }, 5000);
  srv.on('error', () => {});
  srv.on('session', (s) => {
    s.on('error', () => {});
    s.on('close', () => push(`sess-close ${s.goawayCode}`));
  });
  srv.on('stream', (stream) => {
    let data = 0;
    stream.on('data', () => {
      data++;
    });
    stream.on('error', (e) => push(`srv-error ${e.code}:${e.message}`));
    stream.on('frameError', (type, code, id) => push(`srv-frameError ${type}/${code}/${id}`));
    stream.on('aborted', () => push('srv-aborted'));
    stream.on('close', () => {
      push(`srv-data ${data}`);
      push(`srv-close ${stream.rstCode}`);
    });
    stream.respond(undefined, { waitForTrailers: true });
    stream.on('wantTrailers', () => stream.sendTrailers({ 'test-trailer': value }));
    if (body > 0) {
      stream.write('x'.repeat(body));
    }
    stream.end();
  });
  listen(srv).then((port) => {
    client = lib.connect(`http://127.0.0.1:${port}`, opts);
    client.on('error', () => {});
    client.on('frameError', (type, code, id) => push(`cli-sess-frameError ${type}/${code}/${id}`));
    client.on('close', () => push('cli-sess-close'));
    const req = client.request();
    req.on('error', (e) => push(`cli-error ${e.code}:${e.message}`));
    req.on('frameError', (type, code, id) => push(`cli-frameError ${type}/${code}/${id}`));
    req.on('response', (h) => {
      push(`cli-response ${h[':status']}`);
      req.resume();
    });
    req.on('end', () => push('cli-end'));
    req.on('close', () => {
      push(`cli-close ${req.rstCode}`);
      setTimeout(finish, 150);
    });
    req.end();
  });
  return done;
}

// node's measured transcript for the upstream scenario, so a change in either stack has to
// be a decision rather than a drift.
const UPSTREAM = [
  'cli-close 6',
  `cli-error ${SIZE_ERROR}`,
  'cli-response 200',
  'cli-sess-close',
  'sess-close 0',
  'srv-close 6',
  'srv-data 0',
  `srv-error ${SIZE_ERROR}`,
  'srv-frameError 1/6/1'
];

// The same scenario with a value that fits: an ordinary exchange, no session close.
const SERVED = [
  'cli-close 0',
  'cli-end',
  'cli-response 200',
  'srv-close 0',
  'srv-data 0'
];

test('upstream exceeds-server-trailer-size: an oversize trailer is a frameError, not a throw', async () => {
  const native = await arm(http2, 64 * 1024);
  const ours = await arm(h2lean, 64 * 1024);
  assert.deepStrictEqual(native, UPSTREAM);
  assert.deepStrictEqual(ours, native);
});

test('the outbound bound is an estimate: 41+n for a single trailer field', async () => {
  for (const [n, bound, want] of [[159, 200, SERVED], [160, 200, UPSTREAM],
    [65495, undefined, SERVED], [65496, undefined, UPSTREAM]]) {
    const native = await arm(http2, n, bound);
    const ours = await arm(h2lean, n, bound);
    assert.deepStrictEqual(native, want, `node n=${n} bound=${bound}`);
    assert.deepStrictEqual(ours, native, `h2lean n=${n} bound=${bound}`);
  }
});

test('the bound counts string lengths, not UTF-8 octets', async () => {
  // 'é' is 2 octets per character and '€' is 3, and neither moves the line.
  for (const ch of ['\u00e9', '\u20ac']) {
    const native = await arm(http2, 160, 200, ch);
    const ours = await arm(h2lean, 160, 200, ch);
    assert.deepStrictEqual(native, UPSTREAM, `node ${ch}`);
    assert.deepStrictEqual(ours, native, `h2lean ${ch}`);
    const nativeFit = await arm(http2, 159, 200, ch);
    const oursFit = await arm(h2lean, 159, 200, ch);
    assert.deepStrictEqual(nativeFit, SERVED, `node ${ch} fit`);
    assert.deepStrictEqual(oursFit, nativeFit, `h2lean ${ch} fit`);
  }
});

test('maxSendHeaderBlockLength is taken as a number and coerced, not validated', async () => {
  // node's updateOptionsBuffer (internal/http2/util.js) writes the option into the
  // binding's Uint32Array on a `typeof === 'number'` test alone: 200.9 bounds like 200,
  // and -1 like 4294967295, which serves the 64 KiB trailer the default refuses.
  for (const [bound, want] of [[200.9, UPSTREAM], [-1, SERVED]]) {
    const native = await arm(http2, 160, bound);
    const ours = await arm(h2lean, 160, bound);
    assert.deepStrictEqual(native, want, `node bound=${bound}`);
    assert.deepStrictEqual(ours, native, `h2lean bound=${bound}`);
  }
});

test('the bound is checked when a stashed trailer block drains, not when it is stashed', async () => {
  // A body larger than the send window makes sendTrailers() stash the block; nghttp2
  // compares the estimate at prep time, whenever that happens, so the refusal is the same
  // one -- and the END_STREAM the trailers carried never goes out, so the client's 'end'
  // never fires even though the whole body arrived.
  const native = await arm(http2, 64 * 1024, undefined, 'X', 200000);
  const ours = await arm(h2lean, 64 * 1024, undefined, 'X', 200000);
  assert.deepStrictEqual(native, UPSTREAM);
  assert.deepStrictEqual(ours, native);
});
