// When does a client send its first request: as soon as the socket is up, or
// after the peer's SETTINGS have arrived?
//
// Node's client does not wait: internal/http2/core.js emits 'connect' from
// setupHandle with a process.nextTick as soon as the socket is up, and
// request() submits the request from there (requestOnConnect). This probe
// measures the consequence against a raw server that delays its SETTINGS.
//
// The server is raw so that the timeline is observable:
//   * it sends its SETTINGS either at once or after a delay;
//   * it counts the body octets that arrive, so a peer that advertises a small
//     window can be caught overrunning it.
//
// Usage: node tools/probe-client-first.js [delayMs] [initialWindow]
'use strict';
const net = require('node:net');
const http2 = require('node:http2');
const h2lean = require('../index');
const f = require('../lib/frames');

const DELAY = Number(process.argv[2] || 200);
const WINDOW = Number(process.argv[3] || 65535);
const BODY = 65536;

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

function settingsFrame(initialWindow) {
  const p = Buffer.alloc(6);
  p.writeUInt16BE(4, 0); // SETTINGS_INITIAL_WINDOW_SIZE
  p.writeUInt32BE(initialWindow, 2);
  return frame(f.SETTINGS, 0, 0, p);
}

function rawServer() {
  const state = { timeline: [], body: 0, headAt: null, t0: Date.now(), errors: [] };
  const sockets = [];
  const srv = net.createServer((sock) => {
    sockets.push(sock);
    sock.on('error', (e) => state.errors.push(String(e.code || e.message)));
    let buf = Buffer.alloc(0);
    let off = 0;
    let gotPreface = false;
    const timer = setTimeout(() => {
      sock.write(settingsFrame(WINDOW));
      state.timeline.push({ t: Date.now() - state.t0, what: 'settings sent' });
    }, DELAY);
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
        const at = Date.now() - state.t0;
        if (type === f.DATA) {
          state.body += len;
          state.timeline.push({ t: at, what: 'DATA', len });
        } else {
          state.timeline.push({ t: at, what: `type ${type}`, len });
          if (type === f.HEADERS) {
            state.headAt = at;
            clearTimeout(timer);
            if (!state.sentSettings) {
              state.sentSettings = true;
              sock.write(settingsFrame(WINDOW));
              state.timeline.push({ t: Date.now() - state.t0, what: 'settings sent' });
            }
            sock.write(frame(f.HEADERS, f.F_END_HEADERS, 1, Buffer.from([0x88])));
            sock.write(frame(f.DATA, f.F_END_STREAM, 1, Buffer.from('ok')));
          }
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
      const port = srv.address().port;
      const url = `http://127.0.0.1:${port}`;
      let session;
      let stream;
      const done = () => {
        setTimeout(() => {
          for (const s of sockets) {
            s.destroy();
          }
          srv.close();
          resolve(state);
        }, 300);
      };
      if (impl === 'node') {
        session = http2.connect(url);
        stream = session.request({ ':path': '/', ':method': 'POST' });
        stream.on('error', () => {});
        stream.end(Buffer.alloc(BODY, 0x61));
        stream.on('close', done);
      } else {
        session = h2lean.connect(url);
        stream = session.request({ ':path': '/', ':method': 'POST' });
        stream.on('error', () => {});
        stream.end(Buffer.alloc(BODY, 0x61));
        stream.on('close', done);
      }
      setTimeout(done, 3000);
    });
  });
}

(async () => {
  for (const impl of ['node', 'h2lean']) {
    const st = await run(impl);
    const head = st.timeline.find((e) => e.what === 'type 1');
    const settings = st.timeline.find((e) => e.what === 'settings sent');
    const data = st.timeline.filter((e) => e.what === 'DATA');
    console.log(`--- ${impl} (settings delay ${DELAY} ms, window ${WINDOW})`);
    console.log(`    request HEADERS at ${head ? head.t : 'never'} ms, ` +
      `server SETTINGS sent at ${settings ? settings.t : 'never'} ms`);
    console.log(`    body octets received: ${st.body}, DATA frames: ${data.length}, ` +
      `first DATA at ${data.length ? data[0].t : 'never'} ms`);
    console.log(`    timeline: ${st.timeline.map((e) => `${e.t}:${e.what}${e.len ? '/' + e.len : ''}`).join(' ')}`);
  }
})();
