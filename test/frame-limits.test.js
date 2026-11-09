// Frame-level inbound rules: CONTINUATION assembly, its two bounds, and the SETTINGS
// entry bound. Every expectation in this file was measured against a live node:http2
// server (nghttp2), and the same expectation is asserted against both implementations, so
// a change in either one fails here instead of surfacing as an interop difference.
//
// Measured numbers:
//   a block split into HEADERS + CONTINUATION  -- node serves it. h2lite hung the stream
//     instead, because lib/frames.js had CONTINUATION = 10 where RFC 9113 6.10 says 9;
//     both ends of our own tests used the same wrong number, which is exactly why a
//     differential test is the only thing that could have caught it.
//   interruption of an unfinished block          -- node: GOAWAY PROTOCOL_ERROR for a
//     SETTINGS, PING, WINDOW_UPDATE, RST_STREAM and DATA frame alike (RFC 9113 6.10).
//   8 / 9 CONTINUATION frames for one block     -- node: served / GOAWAY INTERNAL_ERROR,
//     which is nghttp2's NGHTTP2_DEFAULT_MAX_CONTINUATIONS = 8 (CVE-2024-28182).
//   32 / 33 entries in one SETTINGS frame       -- node: served / GOAWAY
//     ENHANCE_YOUR_CALM 'SETTINGS: too many setting entries' (CVE-2020-11080). SETTINGS
//     is exempt from the frame size limit, so the entry count is the only bound.
//   a frame before the client's own SETTINGS   -- node: GOAWAY PROTOCOL_ERROR
//     'SETTINGS expected' for HEADERS, PING and a SETTINGS carrying ACK alike
//     (RFC 9113 3.4). h2lite used to accept all three and answer them normally.
//     Version-dependent on the node arm: node 24+ bundles an nghttp2 that answers
//     PROTOCOL_ERROR 'SETTINGS expected' (measured on v26.12.1), while node 20/22
//     bundle an older nghttp2 that answers INTERNAL_ERROR with no debug data --
//     measured on v20.19.5 and v22.21.1, all three violations, same answer.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const f = require('../lib/frames');
const h2lite = require('../index');
const { listen, closeServer } = require('./diff-harness');

const CODES = { 1: 'PROTOCOL_ERROR', 2: 'INTERNAL_ERROR', 11: 'ENHANCE_YOUR_CALM' };
// :method GET, :path /, :scheme http, :authority localhost -- one complete request
const REQ = Buffer.from([0x82, 0x84, 0x86, 0x01, 9,
  ...Buffer.from('localhost', 'latin1')]);
// one octet that is a valid field line on its own: indexed static 24, a header whose
// value is empty -- legal to repeat, and it does not disturb the request either way
const ONE = Buffer.from([0x80 | 24]);

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// Sends a sequence of frames after the connection is established, and returns what the
// server answered, in order, plus how many requests reached the application.
function drive(port, send) {
  return new Promise((resolve) => {
    const seen = [];
    let served = 0;
    const sock = net.connect(port, '127.0.0.1');
    let acc = Buffer.alloc(0);
    let started = false;
    sock.on('error', (e) => seen.push('sock:' + e.code));
    sock.on('data', (d) => {
      acc = Buffer.concat([acc, d]);
      for (;;) {
        if (acc.length < f.HEADER_LEN) {
          return;
        }
        const len = (acc[0] << 16) | (acc[1] << 8) | acc[2];
        if (acc.length < f.HEADER_LEN + len) {
          return;
        }
        const type = acc[3], flags = acc[4];
        const payload = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
        acc = acc.subarray(f.HEADER_LEN + len);
        if (type === f.SETTINGS && !(flags & f.F_ACK)) {
          // the ACK goes out before anything else: an ACK written between HEADERS and
          // CONTINUATION would itself be the interleaving this file is about
          sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
          if (!started) {
            started = true; send(sock);
          }
        } else if (type === f.HEADERS) {
          served++;
          seen.push('HEADERS');
        } else if (type === f.GOAWAY) {
          seen.push('GOAWAY:' + (CODES[payload.readUInt32BE(4)] || payload.readUInt32BE(4)));
        } else if (type === f.RST_STREAM) {
          // RST_STREAM(NO_ERROR) is how a server tears down a stream it has finished
          // answering while the request side is still open -- node does it, h2lite does
          // it, and when depends on timing. It says nothing about the verdict, so it is
          // not recorded. Any other code is a refusal and stays in the transcript.
          const code = payload.readUInt32BE(0);
          if (code !== 0) {
            seen.push(`RST:${code}`);
          }
        }
      }
    });
    sock.on('connect', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
    });
    setTimeout(() => {
      sock.destroy();
      // A GOAWAY is always followed by the peer or the server closing the socket, and
      // which side notices first is timing, not protocol: the reset is dropped once the
      // GOAWAY has been recorded, so what is compared is what the two said, not who hung
      // up first.
      const gone = seen.some((s) => s.startsWith('sock:'));
      const kept = gone && seen.some((s) => s.startsWith('GOAWAY:'))
        ? seen.filter((s) => !s.startsWith('sock:')) : seen;
      resolve({ seen: kept.join('+') || 'nothing', served });
    }, 500);
  });
}

// One server per scenario, both implementations, one assertion pair.
async function both(name, send, wantSeen, wantServed) {
  const fails = [];
  for (const impl of [h2lite, require('node:http2')]) {
    const srv = impl.createServer();
    srv.on('stream', (stream) => {
      // a body-less response: 'HEADERS' in the log means the request reached the app
      stream.respond({ ':status': 200 }, { endStream: true });
      stream.on('error', () => {});
    });
    srv.on('error', () => {});
    const port = await listen(srv);
    const got = await drive(port, send);
    if (wantSeen !== undefined && got.seen !== wantSeen) {
      fails.push(`${impl === h2lite ? 'h2lite' : 'node'}: wanted [${wantSeen}], got [${got.seen}]`);
    }
    if (wantServed !== undefined && got.served !== wantServed) {
      fails.push(`${impl === h2lite ? 'h2lite' : 'node'}: wanted served=${wantServed},`
        + ` got served=${got.served}`);
    }
    await closeServer(srv);
  }
  assert.deepStrictEqual(fails, [], name);
}

test('a header block split over HEADERS and CONTINUATION is served', async () => {
  // RFC 9113 6.10: the block below arrives as 2 octets and then the rest. CONTINUATION
  // is frame type 9 -- 10 is ALTSVC, which RFC 9113 5.5 says must be discarded, so a
  // wrong constant here means the block never finishes and the stream just hangs.
  // END_HEADERS only: END_STREAM is not defined for CONTINUATION (RFC 9113 6.10), so the
  // stream is closed by the empty DATA frame that follows it.
  await both('split block', (sock) => {
    sock.write(frame(f.HEADERS, 0, 1, REQ.subarray(0, 2)));
    sock.write(frame(f.CONTINUATION, f.F_END_HEADERS, 1, REQ.subarray(2)));
    sock.write(frame(f.DATA, f.F_END_STREAM, 1, Buffer.alloc(0)));
  }, 'HEADERS', 1);
});

test('a frame that interrupts an unfinished block kills the connection', async () => {
  // RFC 9113 6.10: while a header block is pending, the only frames allowed are
  // CONTINUATION frames of the same stream. All five of these were measured on node,
  // which answers GOAWAY PROTOCOL_ERROR for every one; h2lite used to serve the request.
  const interrupts = [
    ['SETTINGS', frame(f.SETTINGS, 0, 0, Buffer.alloc(0))],
    ['PING', frame(f.PING, 0, 0, Buffer.alloc(8))],
    ['WINDOW_UPDATE', frame(f.WINDOW_UPDATE, 0, 0, Buffer.from([0, 0, 0, 1]))],
    ['DATA', frame(f.DATA, 0, 1, Buffer.from('x'))],
    ['RST_STREAM', frame(f.RST_STREAM, 0, 1, Buffer.from([0, 0, 0, 8]))],
    ['HEADERS for another stream', frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM,
      3, REQ)]
  ];
  for (const [name, intr] of interrupts) {
    await both(name, (sock) => {
      sock.write(frame(f.HEADERS, 0, 1, REQ.subarray(0, 2)));
      sock.write(intr);
      // the block is completed afterwards, which is precisely what is not allowed
      sock.write(frame(f.CONTINUATION, f.F_END_HEADERS, 1, REQ.subarray(2)));
      sock.write(frame(f.DATA, f.F_END_STREAM, 1, Buffer.alloc(0)));
    }, 'GOAWAY:PROTOCOL_ERROR', 0);
  }
});

test('eight CONTINUATION frames are fine, nine are not', async () => {
  // CVE-2024-28182 and nghttp2's NGHTTP2_DEFAULT_MAX_CONTINUATIONS = 8. The frames are
  // one octet each, so the bound being tested is the number of frames and not the bytes.
  const split = (n) => (sock) => {
    // The whole request goes in the HEADERS frame and the filler frames come after it:
    // pseudo-headers have to precede regular fields (RFC 9113 8.1), so spreading REQ over
    // several frames and interleaving the filler would refuse the request for the wrong
    // reason. Each filler frame is one field line, so n counts CONTINUATION frames.
    sock.write(frame(f.HEADERS, 0, 1, REQ));
    for (let i = 0; i < n - 1; i++) {
      sock.write(frame(f.CONTINUATION, 0, 1, ONE));
    }
    sock.write(frame(f.CONTINUATION, f.F_END_HEADERS, 1, ONE));
    sock.write(frame(f.DATA, f.F_END_STREAM, 1, Buffer.alloc(0)));
  };
  await both('eight', split(8), 'HEADERS', 1);
  await both('nine', split(9), 'GOAWAY:INTERNAL_ERROR', 0);
});

test('a SETTINGS frame may carry 32 entries and not 33', async () => {
  // CVE-2020-11080. The entries use ids we do not know, which RFC 9113 6.5.2 says must be
  // ignored -- ignoring them is still parsing them, and that is the work being bounded.
  const settings = (n) => (sock) => {
    const body = Buffer.alloc(n * 6);
    for (let i = 0; i < n; i++) {
      body.writeUInt16BE(0x7f00 + (i & 0x3f), i * 6); // unknown ids are ignored
      body.writeUInt32BE(i, i * 6 + 2);
    }
    sock.write(frame(f.SETTINGS, 0, 0, body));
    sock.write(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, REQ));
  };
  await both('thirty-two', settings(32), 'HEADERS', 1);
  await both('thirty-three', settings(33), 'GOAWAY:ENHANCE_YOUR_CALM', 0);
});

test('the CONTINUATION counter restarts for the next block', async () => {
  // The bound is per block, not per connection: h2load and curl split large header sets
  // routinely, and a counter that was never reset would refuse a healthy connection after
  // a few responses. Guard for `this._hdrCont = 0` in _beginHeaders.
  const srv = h2lite.createServer();
  let served = 0;
  srv.on('stream', (stream) => {
    served++;
    stream.respond({ ':status': 200 }, { endStream: true });
  });
  srv.on('error', () => {});

  const splitRequest = (sid) => {
    // REQ in the HEADERS frame, then seven filler frames, then the eighth with
    // END_HEADERS: eight CONTINUATION frames per block, two blocks, sixteen in all
    const out = [frame(f.HEADERS, 0, sid, REQ)];
    for (let i = 0; i < 7; i++) {
      out.push(frame(f.CONTINUATION, 0, sid, ONE));
    }
    out.push(frame(f.CONTINUATION, f.F_END_HEADERS, sid, ONE));
    out.push(frame(f.DATA, f.F_END_STREAM, sid, Buffer.alloc(0)));
    return out;
  };

  const port = await listen(srv);
  const got = await drive(port, (sock) => {
    // two blocks of eight frames each, back to back: 16 CONTINUATION frames in total
    sock.write(Buffer.concat([...splitRequest(1), ...splitRequest(3)]));
  });
  await closeServer(srv);
  assert.strictEqual(got.seen, 'HEADERS+HEADERS');
  assert.strictEqual(served, 2);
});

// What a server answers when the first frame after the preface is not the client's
// SETTINGS. This needs a driver of its own: every other scenario in this file finishes the
// handshake first, and finishing it is precisely what is being withheld here. Nothing is
// ACKed and no request follows -- the question is only what the server says back.
function driveFirstFrame(port, first) {
  return new Promise((resolve) => {
    const seen = [];
    const sock = net.connect(port, '127.0.0.1');
    let acc = Buffer.alloc(0);
    sock.on('error', () => {}); // the socket is closed after a GOAWAY, both sides
    sock.on('data', (d) => {
      acc = Buffer.concat([acc, d]);
      for (;;) {
        if (acc.length < f.HEADER_LEN) {
          return;
        }
        const len = (acc[0] << 16) | (acc[1] << 8) | acc[2];
        if (acc.length < f.HEADER_LEN + len) {
          return;
        }
        const type = acc[3];
        const payload = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
        acc = acc.subarray(f.HEADER_LEN + len);
        if (type === f.GOAWAY && seen.length === 0) {
          seen.push(`GOAWAY:${CODES[payload.readUInt32BE(4)] || payload.readUInt32BE(4)}`
            + ` "${payload.subarray(8).toString('latin1')}"`);
        } else if (type === f.HEADERS) {
          seen.push('HEADERS');
        }
      }
    });
    sock.on('connect', () => sock.write(Buffer.concat([f.PREFACE, first])));
    setTimeout(() => {
      sock.destroy(); resolve(seen.join('+') || 'nothing');
    }, 500);
  });
}

test('the client\'s first frame has to be a SETTINGS without the ACK flag', async () => {
  // RFC 9113 3.4: "The client's first frame MUST be a SETTINGS frame". nghttp2 enforces it
  // in the state right after the preface (NGHTTP2_IB_READ_FIRST_SETTINGS): anything that is
  // not SETTINGS, or is SETTINGS with ACK, terminates the session with
  // GOAWAY(PROTOCOL_ERROR) and the debug string "SETTINGS expected".
  // The reason is not ceremony: the settings announce the window size, the header table
  // size and the concurrency limit, so a request answered before them is served against
  // numbers the peer has not committed to yet -- and a SETTINGS-with-ACK is the frame a
  // server sends, never a client, which is how a peer that has it backwards gets caught.
  const cases = [
    ['HEADERS instead', frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, REQ)],
    ['PING instead', frame(f.PING, 0, 0, Buffer.alloc(8))],
    ['SETTINGS with ACK', frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0))]
  ];
  for (const [label, first] of cases) {
    const answers = [];
    for (const impl of [h2lite, require('node:http2')]) {
      const srv = impl.createServer();
      srv.on('stream', (stream) => {
        stream.respond({ ':status': 200 }, { endStream: true });
        stream.on('error', () => {});
      });
      srv.on('error', () => {});
      const port = await listen(srv);
      answers.push(await driveFirstFrame(port, first));
      await closeServer(srv);
    }
    // The two stacks must agree, modulo the bundled-nghttp2 verdict: h2lite pins the
    // node 24+ answer; node 20/22 bundle an older nghttp2 that says INTERNAL_ERROR with
    // no debug data for the same violation (measured, see the header of this file).
    assert.strictEqual(answers[0], 'GOAWAY:PROTOCOL_ERROR "SETTINGS expected"', label);
    assert.ok(answers[1] === answers[0] || answers[1] === 'GOAWAY:INTERNAL_ERROR ""',
      `${label}: node answered [${answers[1]}], which is neither nghttp2 verdict`);
  }
});

// ---- SETTINGS advertisement -----------------------------------------------------------------

// Reads the first non-ACK SETTINGS frame the server sends and decodes its entries as
// "name=value" strings. A SET comparison avoids the order divergence that surfaces when
// updateSettings is used after construction (both stacks emit the same entries but may
// order them differently when the application added values through two separate calls).
async function firstServerSettings(impl, opts, mutate) {
  const srv = impl.createServer(opts);
  srv.on('stream', (s) => {
    s.on('error', () => {}); s.respond({ ':status': 200 }, { endStream: true });
  });
  srv.on('error', () => {});
  if (mutate) {
    srv.updateSettings(mutate);
  }
  const port = await listen(srv);
  return new Promise((resolve) => {
    const sock = net.connect(port, '127.0.0.1');
    sock.on('error', () => {});
    const NAMES = { 1: 'headerTableSize', 2: 'enablePush', 3: 'maxConcurrentStreams',
      4: 'initialWindowSize', 5: 'maxFrameSize', 6: 'maxHeaderListSize',
      8: 'enableConnectProtocol' };
    let acc = Buffer.alloc(0);
    sock.on('data', (chunk) => {
      acc = Buffer.concat([acc, chunk]);
      for (;;) {
        if (acc.length < f.HEADER_LEN) {
          return;
        }
        const len = (acc[0] << 16) | (acc[1] << 8) | acc[2];
        if (acc.length < f.HEADER_LEN + len) {
          return;
        }
        const type = acc[3], flags = acc[4];
        const payload = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
        acc = acc.subarray(f.HEADER_LEN + len);
        if (type === f.SETTINGS && !(flags & f.F_ACK)) {
          const entries = [];
          for (let i = 0; i + 6 <= payload.length; i += 6) {
            entries.push(`${NAMES[payload.readUInt16BE(i)] || 'unknown'}=${payload.readUInt32BE(i + 2)}`);
          }
          sock.destroy(); srv.close();
          resolve({ len, entries: entries.sort() });
          return;
        }
      }
    });
    sock.on('connect', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
    });
  });
}

test('a server with no settings advertised emits an empty SETTINGS frame', async () => {
  const [lite, node] = await Promise.all([
    firstServerSettings(h2lite, {}),
    firstServerSettings(require('node:http2'), {})
  ]);
  assert.deepStrictEqual(node.entries, [], 'node must emit no settings by default');
  assert.deepStrictEqual(lite.entries, node.entries,
    'h2lite must match node: advertise nothing the application did not set');
});

test('a server with configured settings advertises exactly those', async () => {
  const opts = { settings: { maxConcurrentStreams: 100, maxFrameSize: 32768 } };
  const [lite, node] = await Promise.all([
    firstServerSettings(h2lite, opts),
    firstServerSettings(require('node:http2'), opts)
  ]);
  assert.deepStrictEqual(node.entries, ['maxConcurrentStreams=100', 'maxFrameSize=32768'].sort());
  assert.deepStrictEqual(lite.entries, node.entries);
});

test('updateSettings on a bare server advertises the updated value', async () => {
  const [lite, node] = await Promise.all([
    firstServerSettings(h2lite, {}, { maxConcurrentStreams: 7 }),
    firstServerSettings(require('node:http2'), {}, { maxConcurrentStreams: 7 })
  ]);
  assert.deepStrictEqual(node.entries, ['maxConcurrentStreams=7']);
  assert.deepStrictEqual(lite.entries, node.entries);
});

test('configured + updateSettings advertises both values', async () => {
  const opts = { settings: { maxFrameSize: 32768 } };
  const mutate = { maxConcurrentStreams: 7 };
  const [lite, node] = await Promise.all([
    firstServerSettings(h2lite, opts, mutate),
    firstServerSettings(require('node:http2'), opts, mutate)
  ]);
  assert.deepStrictEqual(node.entries, ['maxConcurrentStreams=7', 'maxFrameSize=32768'].sort());
  assert.deepStrictEqual(lite.entries, node.entries,
    'same set, order not asserted: both stacks emit the same entries but may differ in the '
    + 'order they were added (constructor first vs updateSettings first)');
});
