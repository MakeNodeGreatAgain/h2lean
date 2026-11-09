// Which window does a client run a request on when the request waits for a
// concurrency slot and the peer lowers SETTINGS_INITIAL_WINDOW_SIZE in the meantime?
//
// The server is raw:
//   * it advertises SETTINGS_MAX_CONCURRENT_STREAMS=1, so the second request has
//     to wait for a slot;
//   * it lowers SETTINGS_INITIAL_WINDOW_SIZE to 1024 while the second request is
//     still waiting, and answers the first one;
//   * it never sends a WINDOW_UPDATE, so the octets that arrive on the second
//     stream are the window the client ran on.
//
// Usage: node tools/probe-client-park.js
'use strict';
const net = require('node:net');
const http2 = require('node:http2');
const h2lite = require('../index');
const f = require('../lib/frames');

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

function rawServer() {
  const state = { headers: [], data: {}, frames: [], t0: Date.now() };
  const sockets = [];
  const srv = net.createServer((sock) => {
    sockets.push(sock);
    sock.on('error', () => {});
    let buf = Buffer.alloc(0);
    let off = 0;
    let gotPreface = false;
    sock.write(settings([[3, 1]])); // MAX_CONCURRENT_STREAMS = 1
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
        state.frames.push({ t: Date.now() - state.t0, type, sid, len });
        if (type === f.HEADERS) {
          state.headers.push(sid);
          if (state.headers.length === 1) {
            // The window is lowered while the second request waits for a slot.
            sock.write(settings([[4, 1024]])); // INITIAL_WINDOW_SIZE = 1024
            sock.write(frame(f.HEADERS, f.F_END_HEADERS, sid, Buffer.from([0x88])));
            sock.write(frame(f.DATA, f.F_END_STREAM, sid, Buffer.from('ok')));
          }
        } else if (type === f.DATA) {
          state.data[sid] = (state.data[sid] || 0) + len;
        }
        off += f.HEADER_LEN + len;
      }
      buf = buf.subarray(off);
      off = 0;
    });
  });
  return { srv, state, sockets };
}

function run(impl) {
  return new Promise((resolve) => {
    const { srv, state, sockets } = rawServer();
    srv.listen(0, '127.0.0.1', () => {
      const url = `http://127.0.0.1:${srv.address().port}`;
      const session = impl === 'node' ? http2.connect(url) : h2lite.connect(url);
      session.on('error', (e) => {
        state.frames.push({ t: -1, type: 'session error', sid: 0, len: 0, msg: e.code || e.message });
      });
      // The requests are issued once the peer's SETTINGS have landed, so the
      // concurrency limit is known and the second one has to wait.
      setTimeout(() => {
        const a = session.request({ ':path': '/', ':method': 'POST' });
        const b = session.request({ ':path': '/', ':method': 'POST' });
        a.on('error', (e) => {
          state.frames.push({ t: -1, type: 'stream error', sid: a.id, len: 0, msg: e.code || e.message });
        });
        b.on('error', (e) => {
          state.frames.push({ t: -1, type: 'stream error', sid: b.id, len: 0, msg: e.code || e.message });
        });
        a.on('response', () => {
          setTimeout(() => b.end(Buffer.alloc(BODY, 0x62)), 50);
        });
        a.end(Buffer.alloc(16, 0x61));
      }, 300);
      setTimeout(() => {
        session.destroy();
        for (const s of sockets) {
          s.destroy();
        }
        srv.close();
        resolve(state);
      }, 1200);
    });
  });
}

(async () => {
  for (const impl of ['node', 'h2lite']) {
    const st = await run(impl);
    console.log(`--- ${impl}`);
    console.log(`    request HEADERS on streams: ${st.headers.join(', ')}`);
    console.log(`    octets per stream: ${JSON.stringify(st.data)}`);
    console.log(`    frames: ${st.frames.map((e) => `${e.t}:${e.type}/${e.sid}${e.msg ? '/' + e.msg : ''}`).join(' ')}`);
  }
})();
