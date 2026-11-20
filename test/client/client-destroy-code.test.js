// The code a client stream closes with when the application destroys it, and the error
// it reports. node derives both in Http2Stream._destroy (internal/http2/core.js
// 2494-2544) rather than fixing them:
//
//   sessionCode = session.goawayCode || session.destroyCode
//   code = stream.closed ? stream.rstCode : sessionCode
//   if (err != null) code = sessionCode || (err instanceof AbortError ? CANCEL
//                                                                  : INTERNAL_ERROR)
//   if (!closed) closeStream(stream, code, hasHandle ? kForceRstStream : kNoRstStream)
//   if (err == null && code !== NO_ERROR && code !== CANCEL)
//     err = new ERR_HTTP2_STREAM_ERROR(nameForErrorCode[code] || code)
//
// closeStream (core.js:2071) records rstCode, and finishes a writable half the
// application left open: 'aborted' then end(). The inbound-reset path reaches the same
// derivation from the other side -- onStreamClose (core.js:607) calls closeStream and
// then `stream.destroy()` with NO error, so the error the application sees is the one
// _destroy synthesizes from the peer's code.
//
// Measured on a live node v26.12.1 client against a raw TCP peer in the same session
// (node writes socket bytes from C++, so a socket.write wrapper captures nothing):
//   * destroy() with the request already ended frames RST_STREAM(0) and reports
//     rstCode 0, with no 'error';
//   * destroy(new Error('boom')) frames RST_STREAM(2), reports rstCode 2 and re-emits
//     the caller's own error unchanged;
//   * a plain Error merely *named* 'AbortError' is not an instance of node's internal
//     class, so it takes the INTERNAL_ERROR branch too -- RST_STREAM(2), rstCode 2;
//   * options.signal + controller.abort() destroys with that internal AbortError
//     (core.js:1920), so it frames RST_STREAM(8) and reports rstCode 8, and the
//     AbortError itself is the 'error' (code 8 suppresses only a *synthesized* error);
//   * a POST body still open is 'aborted' first. writableEnded afterwards is true for
//     destroy() and false for destroy(err) on both arms -- the same node:stream Duplex
//     machinery decides that, given end() is called at the same point;
//   * a destroy after the response ended and the stream closed puts nothing on the
//     wire and leaves rstCode at 0;
//   * session.destroy() derives the code the same way (core.js:1622-1637): a number IS
//     the code and builds ERR_HTTP2_SESSION_ERROR for a non-zero one, an error with no
//     code implies INTERNAL_ERROR, and an explicit second argument wins. The open
//     stream's rstCode is 0 / 2 / 8 / 1 for destroy(), destroy(err), destroy(8) and
//     destroy(err, 1), and its error is the session's.
//
// The session-destroy cases here compare the stream's code and error, not the wire; the
// GOAWAY(last=0, code) and the N-1 resets a destroy also submits are pinned by
// test/client/client-destroy-goaway.test.js.
//
// KNOWN DIVERGENCE, not pinned here: a non-zero inbound GOAWAY destroys node's
// session at once (onGoawayData, core.js:781: destroy(ERR_HTTP2_SESSION_ERROR(code),
// NO_ERROR)) while h2lean leaves it open, so a stream destroyed after one reports the
// caller's error and frames a reset where node reports ERR_HTTP2_SESSION_ERROR and
// frames nothing; the rstCode agrees (the goaway code) but the rest does not.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const http2 = require('node:http2');
const h2lean = require('../../index');
const f = require('../../lib/frames');

const PREFACE = 'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n';

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// A raw peer that completes the handshake and records the resets the client submits.
// `answer` makes it respond to the first request with an empty 200 carrying END_STREAM,
// so the stream can close before the destroy under test.
function peer(wire, answer) {
  return new Promise((resolve) => {
    const srv = net.createServer((sock) => {
      let buf = Buffer.alloc(0);
      let prefaceSeen = false;
      sock.on('data', (d) => {
        buf = Buffer.concat([buf, d]);
        if (!prefaceSeen) {
          const i = buf.indexOf(PREFACE);
          if (i < 0) {
            return;
          }
          prefaceSeen = true;
          buf = buf.subarray(i + PREFACE.length);
          sock.write(frame(f.SETTINGS, 0, 0, Buffer.alloc(0)));
        }
        while (buf.length >= f.HEADER_LEN) {
          const len = buf.readUIntBE(0, 3);
          if (buf.length < f.HEADER_LEN + len) {
            break;
          }
          const type = buf[3];
          const flags = buf[4];
          const sid = buf.readUInt32BE(5) & 0x7fffffff;
          const body = buf.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
          buf = buf.subarray(f.HEADER_LEN + len);
          if (type === f.SETTINGS && !(flags & f.F_ACK)) {
            sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
          } else if (type === f.RST_STREAM) {
            wire.push(`RST(${body.readUInt32BE(0)})`);
          } else if (type === f.HEADERS && answer) {
            // :status 200 as a literal with an indexed name, END_STREAM | END_HEADERS
            sock.write(frame(f.HEADERS, f.F_END_STREAM | f.F_END_HEADERS, sid,
              Buffer.from([0x48, 0x03, 0x32, 0x30, 0x30])));
          }
        }
      });
      sock.on('error', () => {});
    });
    srv.on('error', () => {});
    // srv.close() waits for open connections, so a client that never closes its socket
    // would hang the file rather than fail a test. Track them and end them explicitly.
    srv.sockets = [];
    srv.on('connection', (s) => {
      srv.sockets.push(s); s.on('error', () => {});
    });
    srv.listen(0, '127.0.0.1', () => resolve({ srv, port: srv.address().port }));
  });
}

function closePeer(srv) {
  return new Promise((resolve) => {
    for (const s of srv.sockets) {
      s.destroy();
    }
    srv.close(resolve);
  });
}

// One request, destroyed the way `opts` says, with the wire and the stream's own
// reports collected. The lines are sorted: the two arms defer the session-level
// teardown differently, and the order within that turn is not what is under test.
async function transcript(impl, opts) {
  const wire = [];
  const { srv, port } = await peer(wire, opts.answer);
  const out = [];
  try {
    const client = impl.connect(`http://127.0.0.1:${port}`);
    client.on('error', () => {});
    await new Promise((r) => client.once('connect', r));
    const ac = opts.signal ? new AbortController() : null;
    const req = client.request({ ':path': '/', ':method': opts.post ? 'POST' : 'GET' },
      ac ? { signal: ac.signal } : undefined);
    req.on('error', (e) => out.push(`error ${e.code}/${e.name}/${e.message}`));
    req.on('aborted', () => out.push('aborted'));
    req.on('finish', () => out.push('finish'));
    req.on('close', () => out.push(`close rstCode=${req.rstCode}`));
    req.resume();
    if (opts.post) {
      req.write('hello');
    }
    if (opts.answer) {
      await new Promise((r) => req.once('close', r));
    } else {
      await new Promise((r) => setTimeout(r, 60));
    }
    if (ac) {
      ac.abort();
    } else {
      req.destroy(opts.err);
    }
    await new Promise((r) => setTimeout(r, 120));
    out.push(`rstCode=${req.rstCode} writableEnded=${req.writableEnded}`
      + ` aborted=${req.aborted}`);
    out.push(`wire=${wire.join(' ') || '(none)'}`);
    client.destroy();
  } finally {
    await closePeer(srv);
  }
  return out.sort();
}

// A plain Error carrying AbortError's name and code. node tests `err instanceof
// AbortError`, so this is NOT the abort path -- it derives INTERNAL_ERROR like any
// other error, which is the distinction the class in lib/client-stream.js exists for.
function lookalikeAbort() {
  const err = new Error('The operation was aborted');
  err.name = 'AbortError';
  err.code = 'ABORT_ERR';
  return err;
}

// label, destroy shape, the reset node puts on the wire, and the stream state node
// reports afterwards. writableEnded differs between the two open-body cases on node
// itself: end() is called at the same point in both, and the Duplex's own error
// handling decides whether it takes effect.
const CASES = [
  ['no error', {}, 'RST(0)', 'rstCode=0 writableEnded=true aborted=false'],
  ['an error', { err: new Error('boom') }, 'RST(2)',
    'rstCode=2 writableEnded=true aborted=false'],
  ['an AbortError lookalike', { err: lookalikeAbort() }, 'RST(2)',
    'rstCode=2 writableEnded=true aborted=false'],
  ['an open request body', { post: true }, 'RST(0)',
    'rstCode=0 writableEnded=true aborted=true'],
  ['an open request body and an error', { post: true, err: new Error('boom') }, 'RST(2)',
    'rstCode=2 writableEnded=false aborted=true'],
  ['a closed stream', { answer: true }, '(none)',
    'rstCode=0 writableEnded=true aborted=false'],
  ['a closed stream and an error', { answer: true, err: new Error('boom') }, '(none)',
    'rstCode=0 writableEnded=true aborted=false']
];

test('an application destroy derives the reset code from the error', async () => {
  for (const [label, opts, wire, state] of CASES) {
    const native = await transcript(http2, opts);
    // node's own values first: a harness that answered nothing would otherwise pass.
    assert.ok(native.includes(`wire=${wire}`), `node wire, ${label}: ${native}`);
    assert.ok(native.includes(state), `node state, ${label}: ${native}`);
    const ours = await transcript(h2lean, opts);
    assert.deepStrictEqual(ours, native, label);
  }
});

test('an aborted signal derives CANCEL and keeps the AbortError', async () => {
  const native = await transcript(http2, { signal: true });
  assert.ok(native.includes('wire=RST(8)'), `node wire: ${native}`);
  assert.ok(native.includes('rstCode=8 writableEnded=true aborted=false'),
    `node rstCode: ${native}`);
  assert.ok(native.includes('error ABORT_ERR/AbortError/The operation was aborted'),
    `node error: ${native}`);
  const ours = await transcript(h2lean, { signal: true });
  assert.deepStrictEqual(ours, native);
});

// The session's code reaches an open stream through the same derivation. The wire is
// not compared: node submits GOAWAY here and h2lean does not (header).
async function sessionTranscript(impl, fn) {
  const { srv, port } = await peer([], false);
  const out = [];
  try {
    const client = impl.connect(`http://127.0.0.1:${port}`);
    client.on('error', (e) => out.push(`session error ${e.code}/${e.message}`));
    await new Promise((r) => client.once('connect', r));
    const req = client.request({ ':path': '/' });
    req.on('error', (e) => out.push(`error ${e.code}/${e.name}/${e.message}`));
    req.on('close', () => out.push(`close rstCode=${req.rstCode}`));
    req.resume();
    await new Promise((r) => setTimeout(r, 60));
    fn(client);
    await new Promise((r) => setTimeout(r, 120));
    out.push(`rstCode=${req.rstCode}`);
  } finally {
    await closePeer(srv);
  }
  return out.sort();
}

const SESSION_CASES = [
  ['no argument', (c) => c.destroy(), 0],
  ['an error', (c) => c.destroy(new Error('boom')), 2],
  ['a numeric code', (c) => c.destroy(8), 8],
  ['an error and a code', (c) => c.destroy(new Error('boom'), 1), 1]
];

test('the session destroy code reaches the open stream', async () => {
  for (const [label, fn, rstCode] of SESSION_CASES) {
    const native = await sessionTranscript(http2, fn);
    assert.ok(native.includes(`rstCode=${rstCode}`), `node rstCode, ${label}: ${native}`);
    assert.ok(native.includes(`close rstCode=${rstCode}`), `node close, ${label}: ${native}`);
    const ours = await sessionTranscript(h2lean, fn);
    assert.deepStrictEqual(ours, native, label);
  }
});
