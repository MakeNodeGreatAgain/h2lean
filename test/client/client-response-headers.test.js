// The response's pseudo-header rules, enforced client-side: a response block
// without exactly one :status, a request-only pseudo-header on a response, two
// :status, and a pseudo-header in a trailer block are all the same per-stream
// refusal -- ERR_HTTP2_STREAM_ERROR on the stream, RST_STREAM(PROTOCOL_ERROR)
// on the wire, no 'response'/'trailers' event, the session stays up (measured
// against node v26.12.1, tools/probe-client-status.js; node enforces the same
// in nghttp2's C layer, nghttp2_http.c nghttp2_http_on_response_headers).
//
// The legal trailers round-trip -- no :status in the trailer block at all --
// is pinned by test/client/client-stream.test.js.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const http2 = require('node:http2');
const h2lean = require('../../index');
const f = require('../../lib/frames');

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// HPACK literals (no huffman, no index): 0x00, len(name), name, len(value), value.
function lit(name, value) {
  const n = Buffer.from(name);
  const v = Buffer.from(value);
  return Buffer.concat([Buffer.from([0, n.length]), n, Buffer.from([v.length]), v]);
}

const OK = Buffer.from([0x88]); // :status 200, indexed

// A raw server that answers the request's HEADERS with the given frames and
// records the RST_STREAM / GOAWAY that arrive.
function rawServer(answerFrames) {
  return new Promise((resolve) => {
    const sockets = [];
    const got = [];
    const srv = net.createServer((sock) => {
      sockets.push(sock);
      sock.on('error', () => {});
      sock.write(frame(f.SETTINGS, 0, 0, Buffer.alloc(0)));
      let gotPreface = false;
      sock.on('data', (d) => {
        if (!gotPreface) {
          if (d.length < f.PREFACE_LEN) {
            return;
          }
          gotPreface = true;
          d = d.subarray(f.PREFACE_LEN);
        }
        let off = 0;
        while (off + f.HEADER_LEN <= d.length) {
          const len = (d[off] << 16) | (d[off + 1] << 8) | d[off + 2];
          const type = d[off + 3];
          if (off + f.HEADER_LEN + len > d.length) {
            break;
          }
          if (type === f.RST_STREAM) {
            got.push({ type, code: len >= 4 ? d.readUInt32BE(off + f.HEADER_LEN) : -1 });
          }
          if (type === f.GOAWAY) {
            got.push({ type, code: len >= 8 ? d.readUInt32BE(off + f.HEADER_LEN + 4) : -1 });
          }
          off += f.HEADER_LEN + len;
          if (type === f.HEADERS) {
            for (const af of answerFrames) {
              sock.write(af);
            }
          }
        }
      });
      setTimeout(() => sock.destroy(), 800);
    });
    srv.listen(0, '127.0.0.1', () => resolve({ srv, sockets, got }));
  });
}

// What the client made of the refused block. The raw server never hangs up
// early, so a client that accepted the block is still waiting for more when
// the deadline resolves -- the deadline is what separates refusal from
// acceptance on both arms.
async function run(impl, answerFrames) {
  const { srv, sockets, got } = await rawServer(answerFrames);
  try {
    const events = [];
    const sess = impl.connect(`http://127.0.0.1:${srv.address().port}`);
    sess.on('error', (e) => {
      events.push(`session error ${e.code}`);
    });
    sess.on('close', () => {
      events.push('session close');
    });
    const req = sess.request({ ':path': '/' });
    req.on('error', (e) => {
      events.push(`stream error ${e.code}`);
    });
    req.on('response', () => {
      events.push('response');
    });
    req.on('trailers', () => {
      events.push('trailers');
    });
    req.on('data', () => {});
    req.on('end', () => {
      events.push('end');
    });
    req.on('close', () => {
      events.push('stream close');
    });
    await new Promise((resolve) => {
      req.on('close', resolve);
      setTimeout(resolve, 400);
    });
    await new Promise((r) => setTimeout(r, 50));
    sess.destroy();
    return { events, got };
  } finally {
    for (const s of sockets) {
      s.destroy();
    }
    srv.close();
  }
}

async function assertRefusalParity(answerFrames, refuseName, expectResponse) {
  for (const [who, impl] of [['node', http2], ['h2lean', h2lean]]) {
    const { events, got } = await run(impl, answerFrames);
    const joined = events.join(' | ');
    const rst = got.find((g) => g.type === f.RST_STREAM);
    assert.ok(rst !== undefined, `${who}: no RST_STREAM on the wire (${joined})`);
    assert.strictEqual(rst.code, 1,
      `${who}: RST_STREAM code is not PROTOCOL_ERROR (${joined})`);
    assert.ok(!got.some((g) => g.type === f.GOAWAY),
      `${who}: the session was ended for a per-stream refusal (${joined})`);
    assert.ok(events.includes('stream error ERR_HTTP2_STREAM_ERROR'),
      `${who}: no ERR_HTTP2_STREAM_ERROR on the stream (${joined})`);
    // The trailer case emits the legal 'response' before the refusal; the
    // 'trailers' event must never fire.
    assert.ok(!events.includes('trailers'),
      `${who}: the refused ${refuseName} reached the application (${joined})`);
    assert.strictEqual(events.includes('response'), !!expectResponse,
      `${who}: 'response' fired unexpectedly or not at all (${joined})`);
    assert.ok(!events.includes('session error'),
      `${who}: the session errored for a per-stream refusal (${joined})`);
  }
}

test('a response without :status is refused per stream', async () => {
  await assertRefusalParity(
    [frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, lit('content-type', 'text/plain'))],
    'response without :status');
});

test('a request-only pseudo-header on a response is refused per stream', async () => {
  await assertRefusalParity(
    [frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1,
      Buffer.concat([OK, lit(':path', '/')]))],
    ':path on a response');
});

test('a second :status is refused per stream', async () => {
  await assertRefusalParity(
    [frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, Buffer.concat([OK, OK]))],
    'duplicate :status');
});

test('a pseudo-header in the trailer block is refused per stream', async () => {
  // The response first (no END_STREAM), then the trailer block that carries a
  // pseudo-header with END_STREAM. The response itself is legal and is
  // delivered before the refusal.
  await assertRefusalParity(
    [frame(f.HEADERS, f.F_END_HEADERS, 1, OK),
      frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, lit(':status', '200'))],
    'pseudo-header in trailers', true);
});
