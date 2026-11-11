// Where END_STREAM goes when a response has trailers, measured frame by frame against
// node:http2 (lib/stream.js end()/sendTrailers(), lib/session.js _flushBlocked()).
//
// RFC 9113 6.1 and 8.1: a trailing HEADERS block is part of the message *before* the
// stream closes -- END_STREAM belongs on it, not on the last DATA frame. That ordering is
// not a preference: a peer that has seen END_STREAM has no stream left to write the
// trailers into, and node's client simply never reports them. Measured before the fix, on
// the exact shape below:
//
//   trailers + body        node: HEADERS/EH DATA HEADERS/ES/EH
//                          hl:   HEADERS/EH DATA/ES HEADERS/ES/EH   <- unreachable block
//   trailers, empty body   node: HEADERS/EH HEADERS/ES/EH
//                          hl:   HEADERS/EH DATA/ES HEADERS/ES/EH   <- plus a stray DATA
//
// The same rule decides respond({endStream:true, waitForTrailers:true}): the response
// headers already closed the stream, so node emits nothing else -- one HEADERS/ES/EH and no
// trailer block at all, and no 'wantTrailers' asking the application to send one.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const f = require('../lib/frames');
const h2lean = require('../index');
const { listen, connect, request } = require('./diff-harness');

const REQ = Buffer.from([0x82, 0x84, 0x86, 0x01, 9,
  ...Buffer.from('localhost', 'latin1')]);
const ID_INITIAL_WINDOW_SIZE = 4; // the SETTINGS id, not a size (RFC 9113 6.5.2)

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}
function setting(id, value) {
  const b = Buffer.alloc(6);
  b.writeUInt16BE(id, 0);
  b.writeUInt32BE(value, 2);
  return b;
}

// One request against `impl`, answered as a transcript of the frames that came back, each
// tagged with END_STREAM and END_HEADERS. `window` is the stream window the client
// advertises: make it small and a large body cannot go out at once, which is how the
// trailer-behind-a-queued-body path gets exercised; the client then returns the credit as
// the data arrives, so the transfer completes instead of stalling.
async function sequence(impl, handler, opts) {
  const window = opts.window === undefined ? 65536 : opts.window;
  const srv = impl.createServer();
  srv.on('stream', (stream) => {
    stream.on('error', () => {});
    handler(stream);
  });
  srv.on('error', () => {});
  const port = await listen(srv);

  const got = await new Promise((resolve) => {
    const tags = [];
    let bytes = 0;
    let ended = false; // a frame carrying END_STREAM has been seen
    const sock = net.connect(port, '127.0.0.1');
    let acc = Buffer.alloc(0);
    sock.on('error', () => {});

    function finish() {
      sock.destroy();
      // which frames carry END_STREAM is the whole question this file asks about, so it is
      // reported as indices rather than as a rendered string
      const ends = tags.map((t, i) => (t.includes('/ES') ? i : -1)).filter((i) => i >= 0);
      resolve({ tags: tags.join(' '), ends, bytes });
    }

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
        const sid = acc.readUInt32BE(5) & 0x7fffffff;
        const body = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
        acc = acc.subarray(f.HEADER_LEN + len);
        if (type === f.SETTINGS && !(flags & f.F_ACK)) {
          sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
          sock.write(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, REQ));
        } else if (type === f.DATA) {
          bytes += body.length;
          tags.push(`DATA${flags & f.F_END_STREAM ? '/ES' : ''}`);
          if (flags & f.F_END_STREAM) {
            ended = true;
          }
          if (opts.window) {
            const p = Buffer.alloc(4);
            p.writeUInt32BE(body.length, 0);
            // a reading client returns credit for both windows it was billed against:
            // the stream's and the connection's. Only the first one is what this test is
            // about, and without the second the transfer stops at 65535 bytes.
            sock.write(frame(f.WINDOW_UPDATE, 0, sid, p));
            sock.write(frame(f.WINDOW_UPDATE, 0, 0, p));
          }
        } else if (type === f.HEADERS) {
          tags.push(`HEADERS${flags & f.F_END_STREAM ? '/ES' : ''}`
            + `${flags & f.F_END_HEADERS ? '/EH' : ''}`);
          if (flags & f.F_END_STREAM) {
            ended = true;
          }
        } else if (type === f.GOAWAY) {
          tags.push('GOAWAY');
        }
        // frames the transcript does not care about (the SETTINGS ACK, a WINDOW_UPDATE of
        // our own coming back round) must not be treated as an end either
        if (opts.stopAtEnd && ended) {
          finish(); return;
        }
      }
    });
    sock.on('connect', () => {
      sock.write(Buffer.concat([f.PREFACE,
        frame(f.SETTINGS, 0, 0, setting(ID_INITIAL_WINDOW_SIZE, window))]));
    });
    setTimeout(finish, 1200);
  });
  await new Promise((resolve) => srv.close(resolve));
  return got;
}

const wantTrailers = (s) => {
  s.on('wantTrailers', () => s.sendTrailers({ 'x-tail': 'v' }));
};

// The shapes whose frame counts are fixed: one response block, at most one DATA frame
// (every body here is far below both stacks' max frame size), one trailer block.
const cases = [
  ['body then trailers', (s) => {
    s.respond({ ':status': 200 }, { waitForTrailers: true });
    wantTrailers(s);
    s.end('body');
  }, 'HEADERS/EH DATA HEADERS/ES/EH'],
  ['empty body then trailers', (s) => {
    s.respond({ ':status': 200 }, { waitForTrailers: true });
    wantTrailers(s);
    s.end();
  }, 'HEADERS/EH HEADERS/ES/EH'],
  ['endStream on respond closes the stream', (s) => {
    s.respond({ ':status': 200 }, { waitForTrailers: true, endStream: true });
    wantTrailers(s);
  }, 'HEADERS/ES/EH'],
  ['informational block then final', (s) => {
    s.additionalHeaders({ ':status': 103, link: '</a.css>; rel=preload' });
    s.respond({ ':status': 200 }, { endStream: true });
  }, 'HEADERS/EH HEADERS/ES/EH']
];

for (const [label, handler, want] of cases) {
  test(`${label}: the frame sequence matches node`, async () => {
    const n = await sequence(require('node:http2'), handler, { stopAtEnd: true });
    const h = await sequence(h2lean, handler, { stopAtEnd: true });
    assert.strictEqual(n.tags, want, `node itself answered [${n.tags}], expected [${want}]`);
    assert.strictEqual(h.tags, want, `h2lean answered [${h.tags}]`);
  });
}

test('trailers do not overtake a body that is still waiting for credit', async () => {
  // The part a two-frame example cannot reach: 32 KB of body behind a 4 KB stream window,
  // so end() queues most of it and 'wantTrailers' fires while those bytes have still not
  // gone out. Sending the block at that moment would put a HEADERS in front of DATA that
  // has not been written, so it waits for the queue instead.
  //
  // How many DATA frames each stack uses here is a property of its own window accounting
  // and frame size, not of the rule being tested, so the two implementations are compared
  // on the shape -- where END_STREAM appears -- rather than on the transcript.
  const handler = (s) => {
    s.respond({ ':status': 200 }, { waitForTrailers: true });
    wantTrailers(s);
    s.end(Buffer.alloc(32 * 1024, 0x61));
  };
  for (const impl of [h2lean, require('node:http2')]) {
    const name = impl === h2lean ? 'h2lean' : 'node';
    const got = await sequence(impl, handler, { window: 4096, stopAtEnd: true });
    const frames = got.tags.split(' ');
    assert.strictEqual(got.bytes, 32 * 1024, `${name}: delivered ${got.bytes} bytes`);
    assert.strictEqual(frames[0], 'HEADERS/EH', `${name}: first frame was ${frames[0]}`);
    assert.strictEqual(frames[frames.length - 1], 'HEADERS/ES/EH',
      `${name}: the stream did not close on a trailer block (${got.tags})`);
    assert.deepStrictEqual(got.ends, [frames.length - 1],
      `${name}: END_STREAM appeared before the end (${got.ends.join(',')})`);
    assert.ok(frames.length > 3, `${name}: the body never went through a queue`);
  }
});

test('a client that asks for trailers receives them', async () => {
  // The symptom this file exists for: with END_STREAM on the body, node's own client
  // decoded trailers=null -- the block went out on an already closed stream and was
  // discarded. Checked through the real client API, so it is the answer an application
  // gets, not just the frame layout.
  for (const impl of [h2lean, require('node:http2')]) {
    const name = impl === h2lean ? 'h2lean' : 'node';
    const srv = impl.createServer();
    srv.on('stream', (stream) => {
      stream.on('error', () => {});
      stream.respond({ ':status': 200 }, { waitForTrailers: true });
      stream.on('wantTrailers', () => stream.sendTrailers({ 'x-tail': 'v' }));
      stream.end('body');
    });
    srv.on('error', () => {});
    const port = await listen(srv);
    const client = connect(port);
    const r = await request(client, { ':method': 'GET', ':path': '/' });
    client.destroy();
    await new Promise((resolve) => srv.close(resolve));
    assert.strictEqual(r.body, 'body', `${name}: the body did not arrive`);
    // the shape an application reads: node's header objects also carry its internal
    // sensitive-header symbol, which is not part of what the server sent
    assert.strictEqual(r.trailers['x-tail'], 'v',
      `${name}: trailers never arrived (${JSON.stringify(r.trailers)})`);
  }
});

// The event side: 'wantTrailers' is the application's permission to call sendTrailers(),
// 'trailersSent' is the report that it did. Both are answered by the same call shape as the
// frame transcript above, on the same two implementations.
async function trailerEvents(impl, opts) {
  const srv = impl.createServer();
  const log = [];
  srv.on('stream', (stream) => {
    stream.on('error', () => {});
    stream.on('wantTrailers', () => {
      log.push('wantTrailers');
      stream.sendTrailers({ 'x-tail': 'v' });
    });
    stream.on('trailersSent', () => log.push('trailersSent'));
    stream.respond({ ':status': 200 }, opts);
    // a stream whose headers already carried END_STREAM has nothing left to end, so the
    // body is only written on the shapes that still owe it -- otherwise this test would be
    // measuring what an error on a closed stream reports
    if (!opts.endStream) {
      stream.end('body');
    }
  });
  srv.on('error', () => {});
  const port = await listen(srv);
  const client = connect(port);
  const r = await request(client, { ':method': 'GET', ':path': '/' });
  client.destroy();
  await new Promise((resolve) => srv.close(resolve));
  return { events: log.join(','), trailers: r.trailers === null ? 'none' : 'some' };
}

const eventCases = [
  // the ordinary request for trailers: the application is asked, sends, and the peer gets
  // the block. node does not report 'trailersSent' here, so the event list is exact.
  ['waitForTrailers', { waitForTrailers: true }, { events: 'wantTrailers', trailers: 'some' }],
  // the same call with END_STREAM already on the header block: node asks for nothing and
  // sends nothing, because there is no stream left to write a trailer block into
  ['waitForTrailers + endStream', { waitForTrailers: true, endStream: true },
    { events: '', trailers: 'none' }]
];

for (const [label, opts, want] of eventCases) {
  test(`${label}: the events match node`, async () => {
    const n = await trailerEvents(require('node:http2'), opts);
    const h = await trailerEvents(h2lean, opts);
    assert.deepStrictEqual(n, want, `node itself: ${JSON.stringify(n)}`);
    assert.deepStrictEqual(h, want, `h2lean: ${JSON.stringify(h)}, node: ${JSON.stringify(n)}`);
  });
}
