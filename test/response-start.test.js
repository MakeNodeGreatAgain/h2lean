// A response the application never started: what starts it, and in what order. Every
// expectation here was measured against a live node:http2 server first with
// tools/probe-response-start.js, and both stacks then run the same script.
//
// The rules that came out of the measurement:
//   * the first write() of a server stream sends the response headers itself. Node does it in
//     the write path, not in end(), which is what keeps HEADERS in front of DATA -- and DATA on
//     a stream that has no headers yet is not a cosmetic ordering difference: RFC 9113 8.1 puts
//     the response HEADERS before any content, so a strict peer has nothing to attach the DATA
//     to. h2lean used to queue the body first and answer the headers after it.
//   * an end() with NO body at all is where the two stacks part company: node puts nothing on
//     the wire and leaves headersSent false, while h2lean answers 200 with an empty DATA/ES.
//     Pinned below as a KNOWN DIVERGENCE, with the reason in AGENTS.md section 10.
//   * node also closes a stream whose request body nobody read (RST_STREAM(NO_ERROR) after the
//     response). That is the second open item of section 10, so the transcripts here compare
//     the response frames and record the reset separately.
//   * a response STARTED and then destroyed in the same turn of the loop is the third: node
//     submits the HEADERS to nghttp2 but nothing flushes them before the destroy, so the
//     queued response is discarded and the peer sees only RST_STREAM(0). h2lean frames the
//     HEADERS as respond() runs and the batch leaves at the end of the read, so they reach
//     the peer. Measured with tools/probe-zero-write.js for write(''), respond(),
//     respond()+end() and respond()+close(); a NON-empty write flushes in the same turn on
//     both stacks, and so does a destroy a setImmediate later -- the trigger is the turn,
//     not the empty write. Same root cause as the refusal-batch item of section 10.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const f = require('../lib/frames');
const { Decoder } = require('../lib/hpack');
const h2lean = require('../index');
const nodeHttp2 = require('node:http2');
const { closeServer } = require('./diff-harness');
const { SAME_TURN_DESTROY_DISCARDS } = require('./node-version');

// :method GET, :path /, :scheme http, :authority localhost -- sent WITHOUT END_STREAM, so the
// request half stays open exactly the way an upload that the handler never reads does.
const REQ = Buffer.from([0x82, 0x84, 0x86, 0x01, 9, ...Buffer.from('localhost', 'latin1')]);

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// One request answered by `app`, returned as the response frames in the order they arrived,
// each tagged with its flags and (for HEADERS) its decoded fields minus the generated date,
// plus the stream's own state once the application is done with it.
function drive(impl, app) {
  return new Promise((resolve, reject) => {
    const srv = impl.createServer();
    let streamRef = null;
    srv.on('stream', (stream) => {
      streamRef = stream;
      stream.on('error', () => {}); // a late error must not fail the file
      app(stream);
    });
    srv.on('error', reject); // a listen that never binds must fail, not hang
    srv.listen(0, '127.0.0.1', () => {
      const port = srv.address().port;

      const seen = [];
      const decoder = new Decoder(4096); // per connection: the dynamic table is per peer
      const sock = net.connect(port, '127.0.0.1');
      let acc = Buffer.alloc(0);
      let settled = false;
      sock.on('error', () => {});

      const finish = () => {
        if (settled) {
          return;
        }
        settled = true;
        const state = streamRef === null ? 'no-stream' : JSON.stringify({
          headersSent: streamRef.headersSent, rst: streamRef.rstCode
        });
        sock.destroy();
        closeServer(srv).then(() =>
          resolve({ wire: seen.join(' ') || '(nothing)', state }));
      };
      // Both windows are idle confirmations, not verdicts: the transcript resolves the moment
      // the terminal RST_STREAM or GOAWAY arrives, and every green wire in this file ends on
      // one, so a longer window costs a passing run nothing -- it is paid only by a transcript
      // whose trailing frame is still in flight, which is exactly the state a window must not
      // cut short. The trailing RST of the unread-request rule is scheduled by the server's
      // loop after the response's END_STREAM, and under a parallel test runner's load one
      // round of that scheduling can land past a sub-second window. The assertions pin exact
      // frame sequences, so a transcript compared without its last frame fails as if a stack
      // verdict had changed -- the shape this file was seen failing with on the CI matrix,
      // where nothing reproduced on any binary here (20/22/24/26, isolated and loaded). Both
      // windows therefore take the suite's established multi-second idle waits
      // (test/trailers.test.js 1200, test/upstream/origin.test.js 5000) at 2000.
      const timer = setTimeout(finish, 2000);
      // END_STREAM ends the STREAM and not the connection: node follows its own last DATA with
      // the RST_STREAM of the unread-request rule, so the transcript gets a settling window
      // after it instead of resolving on it. A GOAWAY really is last and resolves at once.
      let settling = null;
      const settle = () => {
        if (settling !== null || settled) {
          return;
        }
        settling = setTimeout(finish, 2000);
      };
      sock.on('data', (d) => {
        if (settled) {
          return;
        }
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
          const body = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
          acc = acc.subarray(f.HEADER_LEN + len);
          if (type === f.SETTINGS) {
            continue;
          } // ours and the ACK, not part of the answer
          if (type === f.HEADERS) {
            const out = [];
            decoder.decode(body, 0, body.length, out);
            const fields = [];
            for (let i = 0; i < out.length; i += 2) {
              if (out[i] !== 'date') {
                fields.push(`${out[i]}=${out[i + 1]}`);
              }
            }
            seen.push(`HEADERS${flags & f.F_END_HEADERS ? '/EH' : ''}`
              + `${flags & f.F_END_STREAM ? '/ES' : ''}{${fields.join(',')}}`);
          } else if (type === f.DATA) {
            seen.push(`DATA(${body.toString('latin1')})${flags & f.F_END_STREAM ? '/ES' : ''}`);
            if (flags & f.F_END_STREAM) {
              clearTimeout(timer); settle();
            }
          } else if (type === f.RST_STREAM) {
            seen.push(`RST(${body.readUInt32BE(0)})`);
            if (settling !== null) {
              clearTimeout(settling);
            }
            clearTimeout(timer); finish(); return;
          } else if (type === f.GOAWAY) {
            seen.push('GOAWAY');
            if (settling !== null) {
              clearTimeout(settling);
            }
            clearTimeout(timer); finish(); return;
          }
        }
      });
      sock.on('connect', () => {
        sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
        // the request goes out after the server's SETTINGS, so the response is not reordered by
        // the handshake; one tick is enough on loopback and the deadline covers the rest
        setTimeout(() => sock.write(frame(f.HEADERS, f.F_END_HEADERS, 1, REQ)), 60);
      });
    });
  });
}

// Both stacks, the same application script.
async function both(app) {
  return [await drive(nodeHttp2, app), await drive(h2lean, app)];
}

test('the first write() sends the response headers ahead of the body', async () => {
  // The frame order is the whole point: HEADERS must precede every DATA. This is the shape
  // that h2lean got wrong -- with the auto-respond only in end(), the queued body left first.
  const got = await both((stream) => {
    stream.write('x');
  });
  for (let i = 0; i < 2; i++) {
    assert.strictEqual(got[i].wire, 'HEADERS/EH{:status=200} DATA(x)', `${i ? 'h2lean' : 'node'}: ${got[i].wire}`);
    assert.strictEqual(got[i].state, '{"headersSent":true,"rst":0}', got[i].state);
  }
});

test('write() then end() keeps the headers first and closes with END_STREAM', async () => {
  const got = await both((stream) => {
    stream.write('x'); stream.end('body');
  });
  // RST(0) on BOTH arms: the unread-request auto-close (lib/stream.js
  // _maybeAutoClose) -- node closes the stream the application finished whose
  // request nobody read, and h2lean does the same now.
  assert.strictEqual(got[0].wire,
    'HEADERS/EH{:status=200} DATA(x) DATA(body)/ES RST(0)', `node: ${got[0].wire}`);
  assert.strictEqual(got[1].wire, got[0].wire, `h2lean: ${got[1].wire}`);
});

test('an explicit respond() is left alone', async () => {
  // The control: the auto-respond may not fire, or fire twice, when the application started the
  // response itself.
  const got = await both(
    (stream) => {
      stream.respond({ ':status': 201 }); stream.end('body');
    });
  // RST(0) on both arms -- the unread-request auto-close, as above.
  assert.strictEqual(got[0].wire,
    'HEADERS/EH{:status=201} DATA(body)/ES RST(0)', `node: ${got[0].wire}`);
  assert.strictEqual(got[1].wire, got[0].wire, `h2lean: ${got[1].wire}`);
});

test('KNOWN DIVERGENCE: end() with no body answers 200 where node says nothing', async () => {
  // node finishes the writable side without the response ever starting: no HEADERS, no DATA,
  // no END_STREAM, and headersSent stays false -- the stream is let go only when the connection
  // is. h2lean answers a plain 200 with an empty DATA/ES instead, and the response that started
  // also arms the unread-request auto-close: RST(0) follows the empty answer, where node has
  // nothing to close and nothing on the wire at all. The divergence is the answer itself
  // (recorded in AGENTS.md §10); the RST(0) behind it is node's own auto-close rule.
  const got = await both((stream) => {
    stream.end();
  });
  assert.strictEqual(got[0].wire, '(nothing)', `node: ${got[0].wire}`);
  assert.strictEqual(got[0].state, '{"headersSent":false,"rst":0}', `node: ${got[0].state}`);
  assert.strictEqual(got[1].wire, 'HEADERS/EH{:status=200} DATA()/ES RST(0)', `h2lean: ${got[1].wire}`);
  assert.strictEqual(got[1].state, '{"headersSent":true,"rst":0}', `h2lean: ${got[1].state}`);
});

test('a non-empty write flushes the response even when the stream is destroyed at once', async () => {
  // The control for the divergence below: bytes actually submitted make node send in the
  // same turn, so the HEADERS and the DATA reach the peer ahead of the reset. h2lean agrees.
  const apps = [
    (stream) => {
      stream.write('x'); stream.destroy();
    },
    (stream) => {
      stream.respond({ ':status': 200 }); stream.write('x'); stream.destroy();
    }
  ];
  for (const app of apps) {
    const got = await both(app);
    assert.strictEqual(got[0].wire, 'HEADERS/EH{:status=200} DATA(x) RST(0)',
      `node: ${got[0].wire}`);
    assert.strictEqual(got[1].wire, got[0].wire, `h2lean: ${got[1].wire}`);
  }
});

test('a destroy a turn later finds the headers already out, on both stacks', async () => {
  // The other control: the trigger is the turn of the loop, not the empty write. Once a
  // turn has passed node has flushed the queued HEADERS and the two arms agree again.
  const apps = [
    (stream) => {
      stream.respond({ ':status': 200 }); setImmediate(() => stream.destroy());
    },
    (stream) => {
      stream.write(''); setTimeout(() => stream.destroy(), 30);
    }
  ];
  for (const app of apps) {
    const got = await both(app);
    assert.strictEqual(got[0].wire, 'HEADERS/EH{:status=200} RST(0)', `node: ${got[0].wire}`);
    assert.strictEqual(got[1].wire, got[0].wire, `h2lean: ${got[1].wire}`);
  }
});

test('KNOWN DIVERGENCE: a response started and destroyed in the same turn still goes out here',
  async () => {
    // node submits the HEADERS to nghttp2 and nothing sends them before the destroy: the
    // session teardown discards what was queued and the peer sees only the reset. h2lean
    // frames the HEADERS into the outbound batch as respond() runs and the batch leaves at
    // the end of the read, so they reach the peer. headersSent and rstCode agree on every
    // shape -- only whether a HEADERS (and an END_STREAM) reaches the peer differs.
    //
    // Same root cause as the refusal-batch item of AGENTS.md section 10: matching node
    // means discarding outbound state at destroy time, which the staging buffer has no
    // per-stream handle on. The last shape is the Duplex write queue rather than the
    // flush: the second write is still buffered when the destroy runs, so node submitted
    // nothing for it at all.
    // The discard is a node 26 verdict: 20/22/24 send the queued HEADERS (measured off each
    // binary, all three agree), so the node-side expectation is gated per version while
    // h2lean's own wire stays fixed (test/node-version.js). Each shape carries both node
    // wires -- the 26 one first, the pre-26 one second -- then h2lean's.
    const shapes = [
      ['write("")', (stream) => {
        stream.write(''); stream.destroy();
      }, 'RST(0)', 'HEADERS/EH{:status=200} RST(0)', 'HEADERS/EH{:status=200} RST(0)'],
      ['respond()', (stream) => {
        stream.respond({ ':status': 200 }); stream.destroy();
      }, 'RST(0)', 'HEADERS/EH{:status=200} RST(0)', 'HEADERS/EH{:status=200} RST(0)'],
      ['respond()+end()', (stream) => {
        stream.respond({ ':status': 200 }); stream.end(); stream.destroy();
      }, 'RST(0)', 'HEADERS/EH{:status=200} DATA()/ES RST(0)', 'HEADERS/EH{:status=200} DATA()/ES RST(0)'],
      ['respond()+end(body)', (stream) => {
        stream.respond({ ':status': 200 }); stream.end('body'); stream.destroy();
      }, 'HEADERS/EH{:status=200} DATA(body) RST(0)', 'HEADERS/EH{:status=200} DATA(body) RST(0)',
      'HEADERS/EH{:status=200} DATA(body)/ES RST(0)'],
      ['write("")+write(x)', (stream) => {
        stream.write(''); stream.write('x'); stream.destroy();
      }, 'RST(0)', 'HEADERS/EH{:status=200} RST(0)', 'HEADERS/EH{:status=200} DATA(x) RST(0)']
    ];
    for (const [label, app, nodeWire26, nodeWireOld, oursWire] of shapes) {
      const nodeWire = SAME_TURN_DESTROY_DISCARDS ? nodeWire26 : nodeWireOld;
      const got = await both(app);
      assert.strictEqual(got[0].wire, nodeWire, `node, ${label}: ${got[0].wire}`);
      assert.strictEqual(got[0].state, '{"headersSent":true,"rst":0}', `node, ${label}`);
      assert.strictEqual(got[1].wire, oursWire, `h2lean, ${label}: ${got[1].wire}`);
      assert.strictEqual(got[1].state, got[0].state, `h2lean state, ${label}`);
    }
  });
