// What the client does with a fatal inbound frame: node's client answers it with
// GOAWAY(PROTOCOL_ERROR) on the wire and reports ERR_HTTP2_ERROR on the stream
// and on the session; the teardown order is stream 'error', stream 'close',
// session 'error', session 'close' (measured, tools/probe-client-pad-bad.js and
// tools/probe-client-hdr-pad.js, node v26.12.1). These tests pin that parity for
// h2lite: the staged GOAWAY has to leave before the socket dies, and the two
// padded-HEADERS shapes RFC 9113 6.2 refuses (padding that does not fit, and a
// PADDED payload with no pad-length octet) have to end the session as node's do.
//
// The one shape that stays a divergence is documented in lib/client-session.js:
// a padded HEADERS whose padding exactly consumes the payload leaves an empty
// block, which is a legal frame -- node answers a stream error for the missing
// :status, h2lite reports a response with no headers.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const http2 = require('node:http2');
const h2lite = require('../../index');
const f = require('../../lib/frames');

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// A raw server that answers the request with the given frames and records what
// arrives, so the test can look for the GOAWAY the client was supposed to send.
// The socket is held open long enough for a client that ignores the frame to
// still be alive when the test looks.
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
        // The client opens with its 24-octet preface; the frames start after it.
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

// What the client made of the malformed frame. The deadline is the point: a
// client that accepts the frame keeps its stream open until the server hangs up,
// so the events are only complete when the frame was refused.
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
    req.on('data', () => {});
    req.on('end', () => {
      events.push('end');
    });
    req.on('close', () => {
      events.push('stream close');
    });
    await new Promise((resolve) => {
      // The session's 'close' is the last event of the teardown (node's order is
      // stream 'error', stream 'close', session 'error', session 'close'), so it
      // is the one to wait for: a client that ignored the frame is still waiting
      // for the server to hang up when the deadline expires.
      sess.on('close', resolve);
      setTimeout(resolve, 400);
    });
    // The GOAWAY leaves the client before its session 'close', but it crosses
    // the socket one tick later; a grace here so the raw server sees it before
    // the finally tears the socket down.
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

function padTooBigData() {
  // Four octets of payload -- the pad length and three more -- and a padding
  // that claims two hundred of them.
  return [frame(f.DATA, f.F_PADDED, 1, Buffer.concat([Buffer.from([200]), Buffer.alloc(3)]))];
}

function padTooBigHeaders() {
  // The malformed HEADERS is the response: a pad length of 200 over a block
  // that does not nearly cover it (tools/probe-client-hdr-pad.js).
  return [frame(f.HEADERS, f.F_PADDED | f.F_END_HEADERS, 1,
    Buffer.concat([Buffer.from([200]), Buffer.from([0x88])]))];
}

function emptyPaddedHeaders() {
  // A PADDED payload with no octet at all has no pad-length octet to read.
  return [frame(f.HEADERS, f.F_PADDED | f.F_END_HEADERS, 1, Buffer.alloc(0))];
}

function responseOnUnopenedSid() {
  // A valid response block on a stream id the client never opened: nghttp2
  // (nghttp2_session.c, nghttp2_session_on_request_headers_received after the
  // stream lookup in session_process_headers_frame misses) answers an idle
  // stream with a connection error of NGHTTP2_ERR_PROTO -- an odd id above
  // last_sent_stream_id and every even id alike (tools/probe-client-unopened-headers.js).
  return [frame(f.HEADERS, f.F_END_STREAM | f.F_END_HEADERS, 3, Buffer.from([0x88]))];
}

function responseOnEvenSid() {
  // The even-id half of the same rule: the client is never sent a request, so
  // an even sid (push territory) is idle by the same session_detect_idle_stream
  // branch and fatal the same way.
  return [frame(f.HEADERS, f.F_END_STREAM | f.F_END_HEADERS, 2, Buffer.from([0x88]))];
}

// `legacy` marks the two measured deviations of node 20/22's own client (out/
// probe-client-fatal-node20.js, v20.19.5 / v22.21.1 / v26.12.1): for the padded shapes the
// old bundled nghttp2 terminates the session at the C layer without surfacing anything on
// the JS client -- no error events, no closes, the wire GOAWAY is the only observable --
// and for a response on an unopened or even id its wire GOAWAY code is 2, not 1. h2lite
// pins the v24+ surface in both cases; in legacy mode the node arm only has to have sent
// a fatal GOAWAY.
async function assertFatalParity(answerFrames, legacy) {
  for (const [who, impl] of [['node', http2], ['h2lite', h2lite]]) {
    const { events, got } = await run(impl, answerFrames);
    const joined = events.join(' | ');
    const goaway = got.find((g) => g.type === f.GOAWAY);
    assert.ok(goaway !== undefined,
      `${who}: no GOAWAY on the wire (${joined})`);
    if (who === 'node' && legacy) {
      assert.ok(goaway.code === 1 || goaway.code === 2,
        `${who}: unexpected GOAWAY code ${goaway.code} (${joined})`);
      continue;
    }
    assert.strictEqual(goaway.code, 1,
      `${who}: GOAWAY code is not PROTOCOL_ERROR (${joined})`);
    assert.ok(events.includes('stream error ERR_HTTP2_ERROR'),
      `${who}: no ERR_HTTP2_ERROR on the stream (${joined})`);
    assert.ok(events.includes('session error ERR_HTTP2_ERROR'),
      `${who}: no ERR_HTTP2_ERROR on the session (${joined})`);
    assert.ok(events.includes('stream close') && events.includes('session close'),
      `${who}: the teardown did not finish (${joined})`);
    assert.ok(!events.includes('end'),
      `${who}: the answer was delivered as a complete body (${joined})`);
    // The documented order: stream error, stream close, session error, session
    // close -- session close last. The error events carry their code, so the
    // positions are matched by prefix.
    const at = (prefix) => events.findIndex((e) => e.startsWith(prefix));
    const close = events.lastIndexOf('session close');
    assert.ok(at('stream close') < at('session error') && at('session error') < close,
      `${who}: teardown order differs from node's (${joined})`);
  }
}

test('a padded DATA frame whose padding does not fit: GOAWAY + ERR_HTTP2_ERROR', async () => {
  await assertFatalParity(padTooBigData(), true);
});

test('a padded HEADERS whose padding does not fit: GOAWAY + ERR_HTTP2_ERROR', async () => {
  await assertFatalParity(padTooBigHeaders(), true);
});

test('a padded HEADERS with an empty payload: GOAWAY + ERR_HTTP2_ERROR', async () => {
  await assertFatalParity(emptyPaddedHeaders(), true);
});

test('response HEADERS on an unopened odd stream id: GOAWAY + ERR_HTTP2_ERROR', async () => {
  await assertFatalParity(responseOnUnopenedSid(), true);
});

test('response HEADERS on an even stream id: GOAWAY + ERR_HTTP2_ERROR', async () => {
  await assertFatalParity(responseOnEvenSid(), true);
});

test('response HEADERS again on a closed stream id: both stacks ignore it', async () => {
  // The other half of the nghttp2 rule (session_detect_idle_stream returns 0
  // for a sid at or below last_sent_stream_id): the block is dropped without a
  // verdict, the session stays up. Both stacks must be silent about it.
  const answer = frame(f.HEADERS, f.F_END_STREAM | f.F_END_HEADERS, 1, Buffer.from([0x88]));
  for (const [who, impl] of [['node', http2], ['h2lite', h2lite]]) {
    const { events, got } = await run(impl, [answer, answer]);
    const joined = events.join(' | ');
    assert.ok(!got.some((g) => g.type === f.GOAWAY),
      `${who}: a GOAWAY left the client for a closed-stream block (${joined})`);
    assert.ok(events.includes('end'),
      `${who}: the first response was not delivered (${joined})`);
    assert.ok(!events.some((e) => e.includes('error')),
      `${who}: the closed-stream block was answered with an error (${joined})`);
    assert.ok(!events.includes('session close'),
      `${who}: the session died on a closed-stream block (${joined})`);
  }
});
