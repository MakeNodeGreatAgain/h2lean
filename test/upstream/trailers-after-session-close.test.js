// Port of node's `test/parallel/test-http2-trailers-after-session-close.js` (upstream
// nodejs/node#42713) to the differential harness used here (AGENTS.md §3, CONTRIBUTING.md §1:
// the upstream file is a scenario source, node's measured value is the expectation).
//
// The scenario is a server that closes the listening socket and the session from inside the
// stream handler, and only then writes the body and answers 'wantTrailers':
//
//   server.close(); stream.session.close();
//   respond({ ':status': 200 }, { waitForTrailers: true }); write('some data'); end();
//
// What it pins, all of it visible on the client:
//   1. GOAWAY is not disconnect. The message that was already in flight when the session began
//      to close still reaches the peer -- body and trailer block both -- so an answer queued
//      behind the close must not be dropped.
//   2. a trailer block sent *after* the GOAWAY still arrives and is still reported as
//      'trailers', because closing the local half does not tear down the streams already open.
//   3. the client's event order for a trailer block that carries the end of the message:
//      'trailers' fires when the block is decoded and 'end' after it, so the trailer event
//      precedes the readable-side EOF (measured on the compat path too -- see
//      test/upstream/trailers.test.js for the frame shape this order comes from).
//   4. the connection still terminates: the client's socket closes, so neither side is left
//      waiting on a session that decided to shut down.
//
// Adaptations: `common.mustCall()` counters become recorded transcript lines, the client
// pseudo-headers are written out, and the arm is bounded by a watchdog because "the answer that
// follows the close hangs" is exactly the failure this file was written against. Lines are
// compared as a sorted set because the socket close races the last response event; the client's
// own event order is carried as one line so rule 3 is still checked strictly.
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const h2lean = require('../../index');
const { listen, connect, closeServer } = require('../diff-harness');

// A hang is one of the answers this file is testing, so the arm is bounded and reports it.
function withTimeout(promise, label) {
  let timer;
  const guard = new Promise((resolve) => {
    timer = setTimeout(() => resolve([['HANG', label]]), 5000);
    timer.unref();
  });
  return Promise.race([promise, guard]).then((r) => {
    clearTimeout(timer);
    return r;
  });
}

function arm(lib) {
  return withTimeout(new Promise((resolve) => {
    const t = [];
    const order = [];
    let socketClosed = 0;
    const srv = lib.createServer();
    srv.on('stream', (stream) => {
      t.push(['srv-stream', 1]);
      srv.close();
      stream.session.close();
      stream.on('wantTrailers', () => {
        t.push(['srv-wantTrailers', 1]);
        stream.sendTrailers({ xyz: 'abc' });
      });
      stream.respond({ ':status': 200 }, { waitForTrailers: true });
      stream.write('some data');
      stream.end();
    });
    srv.on('error', () => {});
    listen(srv).then((port) => {
      const client = connect(port);
      // upstream asserts the client's socket closes; the server's session 'close' is the same
      // fact seen from the other end. The arm ends when both the stream and the transport are
      // done, so the close is counted rather than raced -- a stack that never shuts the
      // connection down shows up as HANG, which is the bug this file was written against.
      let streamDone = false;
      let transportDone = false;
      const finish = () => {
        if (!streamDone || !transportDone) {
          return;
        }
        t.push(['cli-socket-close', socketClosed]);
        t.push(['order', order.join('>')]);
        client.destroy();
        closeServer(srv).then(() => resolve(t));
      };
      client.socket.on('close', () => {
        socketClosed++;
        transportDone = true;
        finish();
      });
      const req = client.request({ ':path': '/', ':method': 'POST' });
      req.on('response', (headers) => {
        order.push('response');
        t.push(['cli-response', headers[':status']]);
      });
      let data = '';
      req.on('data', (chunk) => {
        if (order[order.length - 1] !== 'data') {
          order.push('data');
        }
        data += chunk;
      });
      req.on('end', () => {
        order.push('end');
        t.push(['body', data]);
      });
      req.on('trailers', (headers) => {
        order.push('trailers');
        t.push(['cli-trailers', headers.xyz]);
      });
      req.on('close', () => {
        order.push('close');
        streamDone = true;
        finish();
      });
    });
  }), 'afterclose');
}

function lineSet(t) {
  return t.map(([k, v]) => `${k} ${v}`).sort();
}

function expectedLines() {
  return lineSet([
    ['srv-stream', 1],
    ['srv-wantTrailers', 1],
    ['cli-response', 200],
    ['body', 'some data'],
    ['cli-trailers', 'abc'],
    ['cli-socket-close', 1],
    ['order', 'response>data>trailers>end>close']
  ]);
}

test('upstream trailers-after-session-close: the answer after a session close still arrives',
  async () => {
    const native = await arm(http2);
    const ours = await arm(h2lean);
    assert.deepStrictEqual(lineSet(ours), lineSet(native));
    // Anchors read off the live node run: the two mustCall counters upstream sets are one each,
    // the body and the trailer both arrive after the server closed its listening socket and its
    // session, and the client's socket closes.
    assert.deepStrictEqual(lineSet(native), expectedLines());
  });
