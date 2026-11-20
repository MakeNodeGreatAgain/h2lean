// The client's reading of an inbound RST_STREAM, measured against a raw peer so the
// frame sequence is under the test's control: any code, with or without END_STREAM ahead
// of it, and with the request body either ended or still open. A node:http2 server cannot
// produce every one of these shapes -- close(code) ends the writable half first, so
// END_STREAM always precedes the reset -- and the shapes are exactly where the two
// stacks differed.
//
// node's rule is two steps in this order (internal/http2/core.js):
//  * closeStream: `ending` is whether end() was *called*. A writable half still open is
//    'aborted' and then ended, so 'finish' fires and writableEnded is true. The guard is
//    the stream's `aborted` flag, so the event fires once.
//  * onStreamClose: a NO_ERROR reset with a still-readable half is a clean end, not a
//    discard -- "Preserve buffered reads after a clean reset" -- so node pushes null and
//    destroys on 'end'. Every other code destroys at once, and _destroy pushes null
//    unconditionally, which is why CANCEL (8) also reaches 'end' while an error code
//    does not: destroy(err) skips 'end', destroy() does not.
//
// KNOWN DIVERGENCE, not covered here: when END_STREAM precedes the reset AND the request
// body was never ended, h2lean's session has already dropped the stream from its map, so
// the reset never reaches it and the Duplex stays open forever. node keeps the stream
// until it is destroyed and reports aborted/finish/close. The same drop is why a reset
// after a completed response is ignored while an unread body is still buffered.
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

// The answer: :status 200 (static index 8 is a whole response block), 100 octets of body,
// then the reset. `es` puts END_STREAM on the body, so the message is complete before the
// reset arrives.
function answer(code, es) {
  const rst = Buffer.alloc(4);
  rst.writeUInt32BE(code, 0);
  return [
    frame(f.HEADERS, f.F_END_HEADERS, 1, Buffer.from([0x88])),
    frame(f.DATA, es ? f.F_END_STREAM : 0, 1, Buffer.alloc(100, 0x44)),
    frame(f.RST_STREAM, 0, 1, rst)
  ];
}

function rawServer(frames) {
  return new Promise((resolve) => {
    const sockets = [];
    const srv = net.createServer((sock) => {
      sockets.push(sock);
      sock.on('error', () => {});
      sock.write(frame(f.SETTINGS, 0, 0, Buffer.alloc(0)));
      let gotPreface = false;
      let rest = Buffer.alloc(0);
      sock.on('data', (d) => {
        rest = Buffer.concat([rest, d]);
        if (!gotPreface) {
          if (rest.length < f.PREFACE_LEN) {
            return;
          }
          gotPreface = true;
          rest = rest.subarray(f.PREFACE_LEN);
        }
        let off = 0;
        while (off + f.HEADER_LEN <= rest.length) {
          const len = (rest[off] << 16) | (rest[off + 1] << 8) | rest[off + 2];
          const type = rest[off + 3];
          if (off + f.HEADER_LEN + len > rest.length) {
            break;
          }
          off += f.HEADER_LEN + len;
          if (type === f.HEADERS) {
            for (const af of frames) {
              sock.write(af);
            }
          }
        }
        rest = rest.subarray(off);
      });
    });
    srv.listen(0, '127.0.0.1', () => resolve({ srv, sockets }));
  });
}

// The events in the order they arrive -- the order is the assertion, not just the set.
async function run(impl, { code, es, end }) {
  const { srv, sockets } = await rawServer(answer(code, es));
  try {
    const events = [];
    const sess = impl.connect(`http://127.0.0.1:${srv.address().port}`);
    sess.on('error', () => {});
    const req = sess.request({ ':path': '/', ':method': 'POST' });
    req.on('response', () => events.push('response'));
    let bytes = 0;
    req.on('data', (c) => {
      bytes += c.length;
    });
    req.on('aborted', () => events.push('aborted'));
    req.on('error', (e) => events.push(`error ${e.code}`));
    req.on('end', () => events.push('end'));
    req.on('finish', () => events.push('finish'));
    let closed = false;
    req.on('close', () => {
      closed = true;
      events.push(`data ${bytes}`);
      events.push(`close ${req.rstCode}`);
      events.push(`wEnded ${req.writableEnded}`);
    });
    req.write(Buffer.alloc(50, 0x41));
    if (end) {
      req.end();
    }
    await new Promise((resolve) => {
      const tick = setInterval(() => {
        if (closed) {
          clearInterval(tick);
          setTimeout(resolve, 30);
        }
      }, 5);
      setTimeout(() => {
        clearInterval(tick);
        resolve();
      }, 600);
    });
    sess.destroy();
    return events;
  } finally {
    for (const s of sockets) {
      s.destroy();
    }
    srv.close();
  }
}

test('a reset closes the writable half first: aborted, finish, then the readable verdict', async () => {
  // Every combination of code, END_STREAM and an ended request body, except the one the
  // header comment records as a divergence (END_STREAM ahead of the reset with the body
  // still open). node's measured transcript is the expectation in each case.
  for (const code of [0, 1, 8]) {
    for (const es of [false, true]) {
      for (const end of [false, true]) {
        if (es && !end) {
          continue;
        }
        const shape = { code, es, end };
        const native = await run(http2, shape);
        const ours = await run(h2lean, shape);
        assert.deepStrictEqual(native, expected(code, es, end),
          `node code=${code} es=${es} end=${end}`);
        assert.deepStrictEqual(ours, native, `h2lean code=${code} es=${es} end=${end}`);
      }
    }
  }
});

// node's measured transcripts. The writable-open cases carry 'aborted' and 'finish';
// NO_ERROR and CANCEL reach 'end' because their destroy carries no error, an error code
// does not. With END_STREAM ahead of the reset the message already ended, so 'end' is
// there for every code and the reset finds a destroyed stream: rstCode stays 0.
function expected(code, es, end) {
  const out = [];
  if (end) {
    out.push('finish');
  }
  out.push('response');
  if (es) {
    out.push('end');
  }
  if (!end) {
    out.push('aborted', 'finish');
  }
  if (code === 1 && !es) {
    out.push('error ERR_HTTP2_STREAM_ERROR');
  } else if (!es && code !== 1) {
    out.push('end');
  }
  out.push('data 100', `close ${es ? 0 : code}`, 'wEnded true');
  return out;
}

test('a clean reset preserves an unread body: neither stack closes until it is consumed', async () => {
  // The deferral is observable as a wait. With no consumer at all node reports no 'end'
  // and no 'close' -- the buffered body keeps the readable half open -- and consuming it
  // releases both. A client that destroyed immediately would close here instead.
  for (const impl of [http2, h2lean]) {
    const { srv, sockets } = await rawServer(answer(0, false));
    try {
      const events = [];
      const sess = impl.connect(`http://127.0.0.1:${srv.address().port}`);
      sess.on('error', () => {});
      const req = sess.request({ ':path': '/' });
      req.on('response', () => events.push('response'));
      req.on('end', () => events.push('end'));
      req.on('close', () => events.push(`close ${req.rstCode}`));
      req.end();
      await new Promise((r) => setTimeout(r, 250));
      assert.deepStrictEqual(events, ['response'], `${impl === http2 ? 'node' : 'h2lean'} waits`);
      let bytes = 0;
      req.on('data', (c) => {
        bytes += c.length;
      });
      await new Promise((resolve) => {
        req.on('close', resolve);
        setTimeout(resolve, 600);
      });
      assert.deepStrictEqual(events, ['response', 'end', 'close 0']);
      assert.strictEqual(bytes, 100);
      sess.destroy();
    } finally {
      for (const s of sockets) {
        s.destroy();
      }
      srv.close();
    }
  }
});
