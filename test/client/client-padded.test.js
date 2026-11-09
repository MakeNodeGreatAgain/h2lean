// What the client does with a padded DATA frame whose padding does not fit.
//
// RFC 9113 6.1: "If the length of the padding is the length of the frame payload
// or greater, the recipient MUST treat this as a connection error of type
// PROTOCOL_ERROR." A padded frame with an empty payload has no pad-length octet
// at all, so it is malformed before any of the padding rules apply to it.
//
// The server is raw and puts the malformed frame on the answer's stream. Both
// clients end the session on it, and neither delivers the answer as a complete
// body.
//
// What node adds on top is a wire and an event difference that h2lite's client
// does not have yet, and that the difference list in lib/client-session.js
// records: node puts GOAWAY(PROTOCOL_ERROR) on the wire and reports the failure
// as ERR_HTTP2_ERROR on the stream and on the session (measured,
// tools/probe-client-pad-bad.js), while h2lite's client ends the session
// without the GOAWAY and without the error events.
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

// A raw server that answers the request and then sends the malformed frame.
// The socket is held open long enough for a client that ignores the frame to
// still be alive when the test looks.
function rawServer(payload, flags) {
  return new Promise((resolve) => {
    const sockets = [];
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
          off += f.HEADER_LEN + len;
          if (type === f.HEADERS) {
            sock.write(frame(f.HEADERS, f.F_END_HEADERS, 1, Buffer.from([0x88])));
            sock.write(frame(f.DATA, flags, 1, payload));
          }
        }
      });
      setTimeout(() => sock.destroy(), 800);
    });
    srv.listen(0, '127.0.0.1', () => resolve({ srv, sockets }));
  });
}

// What the client made of the malformed frame. The deadline is the point: a
// client that accepts the frame keeps its stream open until the server hangs up,
// so the events are only complete when the frame was refused.
async function run(impl, payload, flags) {
  const { srv, sockets } = await rawServer(payload, flags);
  try {
    const events = [];
    const sess = impl.connect(`http://127.0.0.1:${srv.address().port}`);
    sess.on('error', () => {
      events.push('session error');
    });
    sess.on('close', () => {
      events.push('session close');
    });
    const req = sess.request({ ':path': '/' });
    req.on('error', () => {
      events.push('stream error');
    });
    req.on('data', () => {
      events.push('data');
    });
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
    sess.destroy();
    return events;
  } finally {
    for (const s of sockets) {
      s.destroy();
    }
    srv.close();
  }
}

test('a padded DATA frame with an empty payload ends the session', async () => {
  for (const [who, impl] of [['node', http2], ['h2lite', h2lite]]) {
    const events = await run(impl, Buffer.alloc(0), f.F_PADDED);
    assert.ok(events.includes('stream close'),
      `${who}: the stream stayed open on a malformed frame (${events.join(' | ')})`);
    assert.ok(events.includes('session close'),
      `${who}: the session stayed open on a malformed frame (${events.join(' | ')})`);
    assert.ok(!events.includes('end'),
      `${who}: the answer was delivered as a complete body (${events.join(' | ')})`);
  }
});

test('a padded DATA frame whose padding does not fit ends the session', async () => {
  // Four octets of payload -- the pad length and three more -- and a padding
  // that claims two hundred of them.
  for (const [who, impl] of [['node', http2], ['h2lite', h2lite]]) {
    const payload = Buffer.concat([Buffer.from([200]), Buffer.alloc(3)]);
    const events = await run(impl, payload, f.F_PADDED);
    assert.ok(events.includes('stream close'),
      `${who}: the stream stayed open on a malformed frame (${events.join(' | ')})`);
    assert.ok(events.includes('session close'),
      `${who}: the session stayed open on a malformed frame (${events.join(' | ')})`);
    assert.ok(!events.includes('end'),
      `${who}: the answer was delivered as a complete body (${events.join(' | ')})`);
  }
});
