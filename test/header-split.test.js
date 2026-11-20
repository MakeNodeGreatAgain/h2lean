// Outbound header-block framing: which frame carries END_STREAM when a block does not
// fit one HEADERS frame.
//
// RFC 9113 6.2: "a HEADERS frame with the END_STREAM flag set can be followed by
// CONTINUATION frames on the same stream. Logically, the CONTINUATION frames are part of
// the HEADERS frame." 5.1: "CONTINUATION frames do not result in state transitions."
// 8.1 says it outright for trailers: "a sequence starting with a HEADERS frame, followed
// by zero or more CONTINUATION frames, where the HEADERS frame bears an END_STREAM flag".
//
// Measured on a live node server (v26.12.1), a 20 000-octet trailer value:
//   HEADERS sid 1 flags 0x1 len 16384, CONTINUATION sid 1 flags 0x4 len ...
// h2lean used to put END_STREAM on the last CONTINUATION instead (flags 0x5 there, 0x0 on
// the HEADERS). No receiver that follows the RFC accepts that: node answers it with a
// stream error and RST_STREAM(PROTOCOL_ERROR), and h2lean's own receiver -- which reads
// END_STREAM off the HEADERS frame -- never sees the message end at all. The wire
// assertions below are the rule; the four-pairing round trip is the consequence.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const http2 = require('node:http2');
const h2lean = require('../index');
const f = require('../lib/frames');
const { listen, closeServer } = require('./diff-harness');

// Over the 16384 default maxFrameSize, under the block bound both stacks accept.
const BIG = 'z'.repeat(20000);
// :method GET, :path /, :scheme http, :authority localhost -- one complete request
const REQ = Buffer.from([0x82, 0x84, 0x86, 0x01, 9, ...Buffer.from('localhost', 'latin1')]);

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// Every frame the server sends, as {type, flags, sid}, in order. The request goes out
// once the server's SETTINGS has been answered, so no frame of ours can land between
// its HEADERS and CONTINUATION.
function drive(port, handler) {
  return new Promise((resolve) => {
    const seen = [];
    port.server.on('stream', handler);
    const sock = net.connect(port.port, '127.0.0.1');
    let acc = Buffer.alloc(0);
    let started = false;
    sock.on('error', () => {});
    sock.on('data', (d) => {
      acc = Buffer.concat([acc, d]);
      for (;;) {
        if (acc.length < f.HEADER_LEN) {
          break;
        }
        const len = (acc[0] << 16) | (acc[1] << 8) | acc[2];
        if (acc.length < f.HEADER_LEN + len) {
          break;
        }
        const type = acc[3], flags = acc[4];
        seen.push({ type, flags, sid: acc.readUInt32BE(5) & 0x7fffffff });
        acc = acc.subarray(f.HEADER_LEN + len);
        if (!started && type === f.SETTINGS && !(flags & f.F_ACK)) {
          started = true;
          sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
          sock.write(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, REQ));
        }
      }
    });
    sock.on('connect', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
    });
    setTimeout(() => {
      sock.destroy();
      resolve(seen);
    }, 400);
  });
}

async function wire(impl, handler) {
  const srv = impl.createServer();
  srv.on('error', () => {});
  const port = await listen(srv);
  try {
    const seen = await drive({ server: srv, port }, (stream) => {
      stream.on('error', () => {});
      handler(stream);
    });
    return seen.filter((s) => s.sid === 1).map((s) => `${s.type}/${s.flags}`);
  } finally {
    await closeServer(srv);
  }
}

function trailerHandler(stream) {
  stream.respond(undefined, { waitForTrailers: true });
  stream.on('wantTrailers', () => stream.sendTrailers({ 'x-big': BIG }));
  stream.end();
}

function responseHandler(stream) {
  stream.respond({ 'x-big': BIG }, { endStream: true });
}

// The rule, stated once and asserted on both arms: END_STREAM on the HEADERS frame that
// starts the block, END_HEADERS on the CONTINUATION that finishes it, and nothing else.
function assertPlacement(seq, headCount) {
  const headers = seq.filter((s) => s.startsWith(`${f.HEADERS}/`));
  const conts = seq.filter((s) => s.startsWith(`${f.CONTINUATION}/`));
  assert.strictEqual(headers.length, headCount);
  assert.ok(conts.length >= 1, `no CONTINUATION in ${seq.join(' ')}`);
  const last = headers[headers.length - 1];
  assert.strictEqual(Number(last.split('/')[1]) & f.F_END_STREAM, f.F_END_STREAM,
    `END_STREAM is not on the HEADERS frame: ${seq.join(' ')}`);
  assert.strictEqual(Number(last.split('/')[1]) & f.F_END_HEADERS, 0,
    `the split HEADERS claims END_HEADERS: ${seq.join(' ')}`);
  for (const c of conts.slice(0, -1)) {
    assert.strictEqual(Number(c.split('/')[1]), 0);
  }
  assert.strictEqual(Number(conts[conts.length - 1].split('/')[1]), f.F_END_HEADERS);
}

test('a split trailer block carries END_STREAM on its HEADERS frame', async () => {
  const native = await wire(http2, trailerHandler);
  const ours = await wire(h2lean, trailerHandler);
  assertPlacement(native, 2);
  assert.deepStrictEqual(ours, native);
});

test('a split response block carries END_STREAM on its HEADERS frame', async () => {
  const native = await wire(http2, responseHandler);
  const ours = await wire(h2lean, responseHandler);
  assertPlacement(native, 1);
  assert.deepStrictEqual(ours, native);
});

// The consequence, on all four stack pairings. A receiver that follows the RFC reads the
// message end off the HEADERS frame: node answers the misplaced flag with a stream error
// and RST_STREAM(PROTOCOL_ERROR), h2lean's own receiver waits for an END_STREAM that never
// comes. Measured with the flag on the CONTINUATION, a 20 000-octet trailer value:
//   node->node, node->h2lean   response, trailers, end, close:0
//   h2lean->node               response, error:ERR_HTTP2_STREAM_ERROR, close:1
//   h2lean->h2lean             response, trailers, then nothing -- no end, no close
function roundTrip(srvLib, cliLib, body) {
  return new Promise((resolve, reject) => {
    const srv = srvLib.createServer();
    srv.on('error', () => {});
    srv.on('stream', (stream) => {
      stream.on('error', () => {});
      stream.respond({ ':status': 200 }, { waitForTrailers: true });
      stream.on('wantTrailers', () => stream.sendTrailers({ 'x-big': BIG }));
      if (body !== undefined) {
        stream.write(body);
      }
      stream.end();
    });
    let client = null;
    const events = [];
    // The failure this pins is a stream that never closes, so the deadline has to release
    // the client too: an unref'd timer alone leaves its socket holding the runner open.
    const done = (err) => {
      clearTimeout(timer);
      if (client) {
        client.destroy();
      }
      closeServer(srv).then(() => (err ? reject(err) : resolve(events)), reject);
    };
    const timer = setTimeout(() => {
      events.push('TIMEOUT');
      done();
    }, 5000);
    listen(srv).then((port) => {
      client = cliLib.connect(`http://127.0.0.1:${port}`);
      client.on('error', () => {});
      const req = client.request({ ':path': '/' });
      req.on('error', (e) => events.push(`error:${e.code}`));
      let trailers = null;
      req.on('trailers', (h) => {
        trailers = h['x-big'];
      });
      req.on('response', () => {
        events.push('response');
        req.resume();
      });
      req.on('end', () => events.push('end'));
      req.on('close', () => {
        events.push(`trailers:${trailers === BIG ? 'full' : 'missing'}`, `close:${req.rstCode}`);
        done();
      });
      req.end();
    }, (e) => done(e));
  });
}

test('a split trailer block ends the stream on every stack pairing', async () => {
  const expected = ['response', 'end', 'trailers:full', 'close:0'];
  for (const srvLib of [http2, h2lean]) {
    for (const cliLib of [http2, h2lean]) {
      const seen = await roundTrip(srvLib, cliLib);
      assert.deepStrictEqual(seen, expected,
        `${srvLib === http2 ? 'node' : 'h2lean'} server -> ` +
        `${cliLib === http2 ? 'node' : 'h2lean'} client`);
    }
  }
});

// The same rule on the second path that frames a trailer block: one stashed behind a body
// that did not fit the send window is framed when the queue drains, not inside
// sendTrailers(). Measured with a 200 000-octet body ahead of the 20 000-octet trailer
// value: node serves the whole exchange, while h2lean framed the stash as a single
// oversized HEADERS and the peer answered ERR_HTTP2_ERROR with no 'end'.
test('a split trailer block stashed behind a stalled body ends the stream on every pairing', async () => {
  const expected = ['response', 'end', 'trailers:full', 'close:0'];
  const body = 'x'.repeat(200000);
  for (const srvLib of [http2, h2lean]) {
    for (const cliLib of [http2, h2lean]) {
      const seen = await roundTrip(srvLib, cliLib, body);
      assert.deepStrictEqual(seen, expected,
        `${srvLib === http2 ? 'node' : 'h2lean'} server -> ` +
        `${cliLib === http2 ? 'node' : 'h2lean'} client`);
    }
  }
});
