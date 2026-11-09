// A response the application never started: what starts it, and in what order. Every
// expectation here was measured against a live node:http2 server first with
// tools/probe-response-start.js, and both stacks then run the same script.
//
// The rules that came out of the measurement:
//   * the first write() of a server stream sends the response headers itself. Node does it in
//     the write path, not in end(), which is what keeps HEADERS in front of DATA -- and DATA on
//     a stream that has no headers yet is not a cosmetic ordering difference: RFC 9113 8.1 puts
//     the response HEADERS before any content, so a strict peer has nothing to attach the DATA
//     to. h2lite used to queue the body first and answer the headers after it.
//   * an end() with NO body at all is where the two stacks part company: node puts nothing on
//     the wire and leaves headersSent false, while h2lite answers 200 with an empty DATA/ES.
//     Pinned below as a KNOWN DIVERGENCE, with the reason in AGENTS.md section 10.
//   * node also closes a stream whose request body nobody read (RST_STREAM(NO_ERROR) after the
//     response). That is the second open item of section 10, so the transcripts here compare
//     the response frames and record the reset separately.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const f = require('../lib/frames');
const { Decoder } = require('../lib/hpack');
const h2lite = require('../index');
const nodeHttp2 = require('node:http2');
const { closeServer } = require('./diff-harness');

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
      const timer = setTimeout(finish, 500); // the safety net; the frames usually decide it
      // END_STREAM ends the STREAM and not the connection: node follows its own last DATA with
      // the RST_STREAM of the unread-request rule, so the transcript gets a short settling window
      // after it instead of resolving on it. A GOAWAY really is last and resolves at once.
      let settling = null;
      const settle = () => {
        if (settling !== null || settled) {
          return;
        }
        settling = setTimeout(finish, 150);
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
  return [await drive(nodeHttp2, app), await drive(h2lite, app)];
}

test('the first write() sends the response headers ahead of the body', async () => {
  // The frame order is the whole point: HEADERS must precede every DATA. This is the shape
  // that h2lite got wrong -- with the auto-respond only in end(), the queued body left first.
  const got = await both((stream) => {
    stream.write('x');
  });
  for (let i = 0; i < 2; i++) {
    assert.strictEqual(got[i].wire, 'HEADERS/EH{:status=200} DATA(x)', `${i ? 'h2lite' : 'node'}: ${got[i].wire}`);
    assert.strictEqual(got[i].state, '{"headersSent":true,"rst":0}', got[i].state);
  }
});

test('write() then end() keeps the headers first and closes with END_STREAM', async () => {
  const got = await both((stream) => {
    stream.write('x'); stream.end('body');
  });
  // node adds RST(0) -- the auto-close of a stream whose request nobody read, AGENTS.md §10.
  assert.strictEqual(got[0].wire,
    'HEADERS/EH{:status=200} DATA(x) DATA(body)/ES RST(0)', `node: ${got[0].wire}`);
  assert.strictEqual(got[1].wire,
    'HEADERS/EH{:status=200} DATA(x) DATA(body)/ES',
    `h2lite ${got[1].wire} vs node ${got[0].wire}: the response frames must match`);
});

test('an explicit respond() is left alone', async () => {
  // The control: the auto-respond may not fire, or fire twice, when the application started the
  // response itself.
  const got = await both(
    (stream) => {
      stream.respond({ ':status': 201 }); stream.end('body');
    });
  assert.strictEqual(got[0].wire,
    'HEADERS/EH{:status=201} DATA(body)/ES RST(0)', `node: ${got[0].wire}`);
  assert.strictEqual(got[1].wire,
    'HEADERS/EH{:status=201} DATA(body)/ES', `h2lite: ${got[1].wire}`);
});

test('KNOWN DIVERGENCE: end() with no body answers 200 where node says nothing', async () => {
  // node finishes the writable side without the response ever starting: no HEADERS, no DATA,
  // no END_STREAM, and headersSent stays false -- the stream is let go only when the connection
  // is. h2lite answers a plain 200 with an empty DATA/ES instead, because a stream the peer
  // never gets an answer on is only released by the idle timeout. Changing that means the
  // local-half-only close going through _endLocalSide() with no frame at all, which the
  // teardown traces have never covered; recorded in AGENTS.md §10 rather than done blind.
  const got = await both((stream) => {
    stream.end();
  });
  assert.strictEqual(got[0].wire, '(nothing)', `node: ${got[0].wire}`);
  assert.strictEqual(got[0].state, '{"headersSent":false,"rst":0}', `node: ${got[0].state}`);
  assert.strictEqual(got[1].wire, 'HEADERS/EH{:status=200} DATA()/ES', `h2lite: ${got[1].wire}`);
  assert.strictEqual(got[1].state, '{"headersSent":true,"rst":0}', `h2lite: ${got[1].state}`);
});
