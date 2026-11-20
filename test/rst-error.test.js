// What a stream reports when an RST_STREAM ends it, on both sides and for every code.
//
// node's rule is one line, internal/http2/core.js:2543:
//   if (err == null && code !== NGHTTP2_NO_ERROR && code !== NGHTTP2_CANCEL)
//     err = new ERR_HTTP2_STREAM_ERROR(nameForErrorCode[code] || code);
// and the message template is internal/errors.js:1344, 'Stream closed with error code %s'.
// `nameForErrorCode` comes from the C++ binding (core.js:249), so it is not reachable from
// node's JS: the names below were measured off a live node client and a live node server,
// one RST_STREAM per code, and the enum in nghttp2.h ends at NGHTTP2_HTTP_1_1_REQUIRED =
// 0x0D. A code outside the table names itself as a number (measured: 14, 100, 4294967295).
//
// Two things this pins that are easy to get wrong in opposite directions: NO_ERROR (0) and
// CANCEL (8) are NOT errors -- a graceful reset and a client abort report 'close' and
// nothing else -- and an inbound reset is not a 'frameError' on either the stream or the
// session, because nothing failed to parse.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const http2 = require('node:http2');
const h2lean = require('../index');
const f = require('../lib/frames');
const { listen, closeServer } = require('./diff-harness');

// The measured names, independent of lib/frames.js ERROR_NAMES so that a change there has
// to be a decision rather than something this test follows along with.
const NAMES = [
  'NGHTTP2_NO_ERROR', 'NGHTTP2_PROTOCOL_ERROR', 'NGHTTP2_INTERNAL_ERROR',
  'NGHTTP2_FLOW_CONTROL_ERROR', 'NGHTTP2_SETTINGS_TIMEOUT', 'NGHTTP2_STREAM_CLOSED',
  'NGHTTP2_FRAME_SIZE_ERROR', 'NGHTTP2_REFUSED_STREAM', 'NGHTTP2_CANCEL',
  'NGHTTP2_COMPRESSION_ERROR', 'NGHTTP2_CONNECT_ERROR', 'NGHTTP2_ENHANCE_YOUR_CALM',
  'NGHTTP2_INADEQUATE_SECURITY', 'NGHTTP2_HTTP_1_1_REQUIRED'
];
const CODES = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 4294967295];

// :method GET, :path /, :scheme http, :authority localhost
const REQ = Buffer.from([0x82, 0x84, 0x86, 0x01, 9, ...Buffer.from('localhost', 'latin1')]);

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

function rst(code) {
  const p = Buffer.alloc(4);
  p.writeUInt32BE(code, 0);
  return frame(f.RST_STREAM, 0, 1, p);
}

// The transcript node reports for a stream ended by RST_STREAM(code): no error at all for
// 0 and 8, the named error otherwise, and never a frameError.
function expected(code) {
  const name = NAMES[code] || code;
  const err = code === 0 || code === 8 ? null
    : `ERR_HTTP2_STREAM_ERROR:Stream closed with error code ${name}`;
  return { err, rstCode: code, frameError: null };
}

// A raw peer that completes the handshake and then resets stream 1. `lib` is the stack
// under test on the other end: 'client' means lib.connect() against this raw server,
// 'server' means lib.createServer() against this raw client.
function drive(lib, side, code) {
  return new Promise((resolve) => {
    const out = { err: null, rstCode: null, frameError: null };
    let sock = null;
    let peer = null;
    let srv = null;
    let done = false;
    const finish = () => {
      if (done) {
        return;
      }
      done = true;
      clearTimeout(timer);
      if (sock) {
        sock.destroy();
      }
      if (peer) {
        peer.destroy();
      }
      if (srv) {
        closeServer(srv).then(() => resolve(out));
      } else {
        resolve(out);
      }
    };
    const timer = setTimeout(finish, 5000);
    timer.unref();

    // The stream under test, whichever side it is on.
    const watch = (stream) => {
      stream.on('error', (e) => {
        out.err = `${e.code}:${e.message}`;
      });
      stream.on('frameError', (t, c, id) => {
        out.frameError = `${t}/${c}/${id}`;
      });
      stream.on('close', () => {
        out.rstCode = stream.rstCode;
        finish();
      });
    };

    if (side === 'client') {
      // lib is the client; this end is a raw server that answers SETTINGS and resets.
      srv = net.createServer((s) => {
        sock = s;
        let acc = Buffer.alloc(0);
        let preface = false;
        let sent = false;
        s.on('error', () => {});
        s.on('data', (d) => {
          acc = Buffer.concat([acc, d]);
          if (!preface) {
            if (acc.length < f.PREFACE_LEN) {
              return;
            }
            acc = acc.subarray(f.PREFACE_LEN);
            preface = true;
          }
          for (;;) {
            if (acc.length < f.HEADER_LEN) {
              break;
            }
            const len = (acc[0] << 16) | (acc[1] << 8) | acc[2];
            if (acc.length < f.HEADER_LEN + len) {
              break;
            }
            const type = acc[3], flags = acc[4];
            const sid = acc.readUInt32BE(5) & 0x7fffffff;
            acc = acc.subarray(f.HEADER_LEN + len);
            if (!sent && type === f.SETTINGS && !(flags & f.F_ACK)) {
              sent = true;
              s.write(frame(f.SETTINGS, 0, 0, Buffer.alloc(0)));
              s.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
            }
            if (type === f.HEADERS && sid === 1) {
              s.write(rst(code));
            }
          }
        });
      });
      srv.on('error', () => {});
      listen(srv).then((port) => {
        peer = lib.connect(`http://127.0.0.1:${port}`);
        peer.on('error', () => {});
        peer.on('frameError', (t, c, id) => {
          out.sessionFrameError = `${t}/${c}/${id}`;
        });
        const req = peer.request({ ':path': '/' });
        watch(req);
        req.end();
      }, finish);
    } else {
      // lib is the server; this end is a raw client that requests and then resets.
      srv = lib.createServer();
      srv.on('error', () => {});
      srv.on('stream', (stream) => {
        watch(stream);
        // No response: the reset is the only thing that ends this stream, so 'aborted'
        // is part of the shape both stacks report.
        stream.on('aborted', () => {
          out.aborted = true;
        });
      });
      listen(srv).then((port) => {
        sock = net.connect(port, '127.0.0.1');
        sock.on('error', () => {});
        let acc = Buffer.alloc(0);
        let sent = false;
        sock.on('data', (d) => {
          acc = Buffer.concat([acc, d]);
          for (;;) {
            if (acc.length < f.HEADER_LEN) {
              break;
            }
            const len = (acc[0] << 16) | (acc[1] << 8) | acc[2];
            if (acc.length < f.HEADER_LEN + len) {
              break;
            }
            const type = acc[3], flags = acc[4];
            acc = acc.subarray(f.HEADER_LEN + len);
            if (!sent && type === f.SETTINGS && !(flags & f.F_ACK)) {
              sent = true;
              sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
              sock.write(frame(f.HEADERS, f.F_END_HEADERS, 1, REQ));
              sock.write(rst(code));
            }
          }
        });
        sock.on('connect', () => {
          sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
        });
      }, finish);
    }
  });
}

test('a client stream names the code that reset it, and 0 and 8 are not errors', async () => {
  for (const code of CODES) {
    const want = expected(code);
    const native = await drive(http2, 'client', code);
    assert.strictEqual(native.err, want.err, `node client, code ${code}`);
    assert.strictEqual(native.rstCode, want.rstCode, `node client rstCode, code ${code}`);
    assert.strictEqual(native.frameError, null, `node client frameError, code ${code}`);
    assert.strictEqual(native.sessionFrameError, undefined,
      `node session frameError, code ${code}`);
    const ours = await drive(h2lean, 'client', code);
    assert.deepStrictEqual(ours, native, `h2lean client, code ${code}`);
  }
});

test('a server stream names the code that reset it, and 0 and 8 are not errors', async () => {
  for (const code of CODES) {
    const want = expected(code);
    const native = await drive(http2, 'server', code);
    assert.strictEqual(native.err, want.err, `node server, code ${code}`);
    assert.strictEqual(native.rstCode, want.rstCode, `node server rstCode, code ${code}`);
    assert.strictEqual(native.frameError, null, `node server frameError, code ${code}`);
    assert.strictEqual(native.aborted, true, `node server aborted, code ${code}`);
    const ours = await drive(h2lean, 'server', code);
    assert.deepStrictEqual(ours, native, `h2lean server, code ${code}`);
  }
});
