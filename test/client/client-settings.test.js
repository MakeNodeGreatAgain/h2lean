// When the client sends its first request, and which window a stream runs on.
//
// Node's client does not wait for the peer's SETTINGS: internal/http2/core.js
// emits 'connect' from setupHandle with a process.nextTick as soon as the socket
// is up, and request() submits the request from there (requestOnConnect). The
// request therefore leaves in the first flight, before the peer's SETTINGS are
// known -- which is also what RFC 9113 6.5.2 asks for, since a peer that will
// not take the stream answers REFUSED_STREAM.
//
// h2lite's client used to hold the first request back until the peer's SETTINGS
// had been applied. That cost a round trip on every session's first request:
// measured against a raw server that delays its SETTINGS by 200 ms, node's
// request HEADERS arrived at 5 ms while h2lite's arrived at 225 ms
// (tools/probe-client-first.js).
//
// The window is the other half of the same rule. RFC 9113 6.9.2: a stream runs
// on the peer's SETTINGS_INITIAL_WINDOW_SIZE, and a later SETTINGS shifts the
// windows of the streams that are open by then. A stream that h2lite opens
// later -- one that waited for a concurrency slot -- is not among them, so the
// window it runs on is taken where it is opened, not where the stream object
// was built. Measured against a raw server that lowers the window to 1024 while
// the second request waits for a slot: node sends 1024 octets on that stream,
// and h2lite used to send 65 519 (out/probe-client-park.js).
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const http2 = require('node:http2');
const h2lite = require('../../index');
const f = require('../../lib/frames');

const SETTINGS_DELAY = 300; // how late the peer's SETTINGS arrive
const WINDOW = 1024; // the window the peer advertises
const BODY = 65536;

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

function settings(pairs) {
  const p = Buffer.alloc(6 * pairs.length);
  let o = 0;
  for (const [id, value] of pairs) {
    p.writeUInt16BE(id, o);
    p.writeUInt32BE(value, o + 2);
    o += 6;
  }
  return frame(f.SETTINGS, 0, 0, p);
}

// A raw server whose SETTINGS arrive late, and which records when the request's
// HEADERS reached it. The answer is sent when the request arrives, so the client
// has something to finish on.
function lateSettingsServer() {
  return new Promise((resolve) => {
    const sockets = [];
    const state = { headAt: null, settingsAt: null };
    const srv = net.createServer((sock) => {
      sockets.push(sock);
      sock.on('error', () => {});
      let buf = Buffer.alloc(0);
      let off = 0;
      let gotPreface = false;
      setTimeout(() => {
        state.settingsAt = Date.now();
        sock.write(frame(f.SETTINGS, 0, 0, Buffer.alloc(0)));
      }, SETTINGS_DELAY);
      sock.on('data', (d) => {
        if (!gotPreface) {
          if (d.length < f.PREFACE_LEN) {
            return;
          }
          gotPreface = true;
          d = d.subarray(f.PREFACE_LEN);
        }
        buf = Buffer.concat([buf, d]);
        while (off + f.HEADER_LEN <= buf.length) {
          const len = (buf[off] << 16) | (buf[off + 1] << 8) | buf[off + 2];
          const type = buf[off + 3];
          if (off + f.HEADER_LEN + len > buf.length) {
            break;
          }
          off += f.HEADER_LEN + len;
          if (type === f.HEADERS) {
            state.headAt = Date.now();
            sock.write(frame(f.HEADERS, f.F_END_HEADERS, 1, Buffer.from([0x88])));
            sock.write(frame(f.DATA, f.F_END_STREAM, 1, Buffer.from('ok')));
          }
        }
        buf = buf.subarray(off);
        off = 0;
      });
    });
    srv.listen(0, '127.0.0.1', () => resolve({ srv, sockets, state }));
  });
}

// When the request HEADERS reached the server, relative to the peer's SETTINGS.
async function firstFlight(impl) {
  const { srv, sockets, state } = await lateSettingsServer();
  try {
    const sess = impl.connect(`http://127.0.0.1:${srv.address().port}`);
    sess.on('error', () => {});
    await new Promise((resolve) => {
      const req = sess.request({ ':path': '/', ':method': 'POST' });
      req.on('error', () => {});
      req.on('close', resolve);
      req.end(Buffer.alloc(16, 0x61));
      setTimeout(resolve, SETTINGS_DELAY + 500);
    });
    sess.destroy();
    return state;
  } finally {
    for (const s of sockets) {
      s.destroy();
    }
    srv.close();
  }
}

// A raw server that allows one stream at a time and lowers the window while the
// second request is waiting for a slot. It never sends a WINDOW_UPDATE, so the
// octets that arrive on the second stream are the window the client ran on.
function parkedServer() {
  return new Promise((resolve) => {
    const sockets = [];
    const state = { data: {} };
    const srv = net.createServer((sock) => {
      sockets.push(sock);
      sock.on('error', () => {});
      let buf = Buffer.alloc(0);
      let off = 0;
      let gotPreface = false;
      let headers = 0;
      sock.write(settings([[3, 1]])); // SETTINGS_MAX_CONCURRENT_STREAMS = 1
      sock.on('data', (d) => {
        if (!gotPreface) {
          if (d.length < f.PREFACE_LEN) {
            return;
          }
          gotPreface = true;
          d = d.subarray(f.PREFACE_LEN);
        }
        buf = Buffer.concat([buf, d]);
        while (off + f.HEADER_LEN <= buf.length) {
          const len = (buf[off] << 16) | (buf[off + 1] << 8) | buf[off + 2];
          const type = buf[off + 3];
          const sid = buf.readUInt32BE(off + 5) & 0x7fffffff;
          if (off + f.HEADER_LEN + len > buf.length) {
            break;
          }
          off += f.HEADER_LEN + len;
          if (type === f.HEADERS) {
            headers++;
            if (headers === 1) {
              // The window is lowered while the second request still waits.
              sock.write(settings([[4, WINDOW]]));
              sock.write(frame(f.HEADERS, f.F_END_HEADERS, sid, Buffer.from([0x88])));
              sock.write(frame(f.DATA, f.F_END_STREAM, sid, Buffer.from('ok')));
            }
          } else if (type === f.DATA) {
            state.data[sid] = (state.data[sid] || 0) + len;
          }
        }
        buf = buf.subarray(off);
        off = 0;
      });
    });
    srv.listen(0, '127.0.0.1', () => resolve({ srv, sockets, state }));
  });
}

// The octets that reached the second stream.
async function parkedBody(impl) {
  const { srv, sockets, state } = await parkedServer();
  try {
    const sess = impl.connect(`http://127.0.0.1:${srv.address().port}`);
    sess.on('error', () => {});
    await new Promise((resolve) => {
      // The requests are issued once the peer's SETTINGS have landed, so the
      // concurrency limit is known and the second one has to wait for a slot.
      setTimeout(() => {
        const first = sess.request({ ':path': '/', ':method': 'POST' });
        const second = sess.request({ ':path': '/', ':method': 'POST' });
        first.on('error', () => {});
        second.on('error', () => {});
        first.on('response', () => {
          setTimeout(() => second.end(Buffer.alloc(BODY, 0x62)), 50);
        });
        first.end(Buffer.alloc(16, 0x61));
      }, 250);
      setTimeout(resolve, 900);
    });
    sess.destroy();
    return state.data;
  } finally {
    for (const s of sockets) {
      s.destroy();
    }
    srv.close();
  }
}

test('the first request leaves in the first flight, before the peer\'s SETTINGS', async () => {
  const node = await firstFlight(http2);
  const ours = await firstFlight(h2lite);
  for (const [who, st] of [['node', node], ['h2lite', ours]]) {
    assert.ok(st.headAt !== null, `${who}: the request never arrived`);
    assert.ok(st.settingsAt !== null, `${who}: the SETTINGS never arrived`);
    assert.ok(st.headAt < st.settingsAt,
      `${who}: the request arrived ${st.headAt - st.settingsAt} ms after the SETTINGS`);
  }
});

test('a stream opened after the peer\'s SETTINGS runs on the peer\'s window', async () => {
  const node = await parkedBody(http2);
  const ours = await parkedBody(h2lite);
  // Stream 1 carries the first request's body, stream 3 the second one. The
  // second one is the one that waited for a slot while the window was lowered.
  for (const [who, data] of [['node', node], ['h2lite', ours]]) {
    assert.strictEqual(data[1], 16, `${who}: the first request's body (${JSON.stringify(data)})`);
    assert.strictEqual(data[3], WINDOW,
      `${who}: the second request ran on a window of ${data[3]} octets (${JSON.stringify(data)})`);
  }
});
