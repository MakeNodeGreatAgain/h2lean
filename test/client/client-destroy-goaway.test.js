// The goodbye a client session puts on the wire when it is destroyed, and the resets
// that go with it. node's closeSession (internal/http2/core.js:1241-1268) terminates the
// nghttp2 session with the derived code -- handle.destroy(code, socket.destroyed) --
// which submits GOAWAY(lastStreamID, code) before the socket goes, and destroys the open
// streams with the session's error first.
//
// Measured on a live node v26.12.1 client against a raw frame-decoding peer, in the same
// session, through the same peer for both arms:
//   * destroy() / destroy(err) / destroy(8) / destroy(err, 1) send
//     GOAWAY(last=0, code=0 / 2 / 8 / 1). last is 0 with no pushes: for a CLIENT the
//     processed streams are the server-initiated ones, so nghttp2's
//     get_last_proc_stream_id is the highest promised id, not the next request id;
//   * the goodbye is not conditional on the session still being open. A graceful
//     close() first sends GOAWAY(NO_ERROR), and a destroy afterwards sends a SECOND
//     GOAWAY carrying the destroy code -- both in the same tick and a tick apart;
//   * with no request at all the GOAWAY still goes out;
//   * a destroy while connecting puts nothing on the wire;
//   * the open streams are reset with the session's code, ALL BUT THE LAST of them:
//     one open stream produces no RST_STREAM, two produce RST(1), three produce
//     RST(1) RST(3), four produce RST(1) RST(3) RST(5), each in ascending id order and
//     ahead of the GOAWAY. The codes track the session's (0, 2 and 8 for the three
//     destroy shapes). A stream the application already destroyed is not counted: with
//     two requests and the second one destroyed first, the session destroy sends the
//     GOAWAY alone. Why the last open stream is exempt lives in the C++ layer
//     (node_http2.cc / nghttp2), which was not read -- the rule pinned here is the wire
//     measurement, repeated across four stream counts and three codes, not a derivation.
//
// Not pinned, both measured and below the HTTP/2 surface: whether the peer sees a clean
// socket close or ECONNRESET after the goodbye (node's finishSessionClose ends the socket
// and then destroys it a tick later, so which one the peer observes is a race -- the
// CONTRIBUTING.md 5 GOAWAY-echo family), and whether the TCP connection of a
// destroy-while-connecting completes before it goes (node's does, h2lean's does not;
// neither puts a frame on the wire and both report the same session error and 'close').
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const http2 = require('node:http2');
const h2lean = require('../../index');
const f = require('../../lib/frames');

const PREFACE = 'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n';

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// A raw peer that completes the handshake and records every frame the client sends, in
// order. It never answers and never closes its own side, so the only frames on the wire
// are the ones the destroy under test produced. Socket events are deliberately not
// recorded: see the header.
function peer(wire) {
  return new Promise((resolve) => {
    const srv = net.createServer((sock) => {
      let buf = Buffer.alloc(0);
      let prefaceSeen = false;
      sock.on('data', (d) => {
        buf = Buffer.concat([buf, d]);
        if (!prefaceSeen) {
          const i = buf.indexOf(PREFACE);
          if (i < 0) {
            return;
          }
          prefaceSeen = true;
          buf = buf.subarray(i + PREFACE.length);
          sock.write(frame(f.SETTINGS, 0, 0, Buffer.alloc(0)));
        }
        while (buf.length >= f.HEADER_LEN) {
          const len = buf.readUIntBE(0, 3);
          if (buf.length < f.HEADER_LEN + len) {
            break;
          }
          const type = buf[3];
          const flags = buf[4];
          const sid = buf.readUInt32BE(5) & 0x7fffffff;
          const body = buf.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
          buf = buf.subarray(f.HEADER_LEN + len);
          if (type === f.SETTINGS && !(flags & f.F_ACK)) {
            sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
          } else if (type === f.RST_STREAM) {
            wire.push(`RST(${sid},${body.readUInt32BE(0)})`);
          } else if (type === f.GOAWAY) {
            wire.push(`GOAWAY(${body.readUInt32BE(0)},${body.readUInt32BE(4)})`);
          } else if (type === f.HEADERS) {
            wire.push(`HEADERS(${sid})`);
          }
        }
      });
      sock.on('error', () => {});
    });
    srv.on('error', () => {});
    // srv.close() waits for open connections, so a client that never closes its socket
    // would hang the file rather than fail a test. Track them and end them explicitly.
    srv.sockets = [];
    srv.on('connection', (s) => {
      srv.sockets.push(s); s.on('error', () => {});
    });
    srv.listen(0, '127.0.0.1', () => resolve({ srv, port: srv.address().port }));
  });
}

function closePeer(srv) {
  return new Promise((resolve) => {
    for (const s of srv.sockets) {
      s.destroy();
    }
    srv.close(resolve);
  });
}

// `count` requests are made and left open; `pre` destroys the last of them before the
// session destroy; `closeFirst` runs a graceful close() first; `connecting` destroys
// before the 'connect' event. The wire is returned in order -- frame order is the point
// -- and the session's own reports sorted, since the teardown turn's internal order is
// not what is under test.
async function transcript(impl, opts) {
  const wire = [];
  const { srv, port } = await peer(wire);
  const out = [];
  try {
    const client = impl.connect(`http://127.0.0.1:${port}`);
    client.on('error', (e) => out.push(`session error ${e.code}/${e.message}`));
    client.on('close', () => out.push('session close'));
    if (!opts.connecting) {
      await new Promise((r) => client.once('connect', r));
      const reqs = [];
      for (let i = 0; i < (opts.count || 0); i++) {
        const req = client.request({ ':path': `/${i}` });
        req.on('error', () => {});
        req.on('close', () => out.push(`close ${i} rstCode=${req.rstCode}`));
        req.resume();
        reqs.push(req);
      }
      if (opts.count) {
        await new Promise((r) => setTimeout(r, 60));
      }
      if (opts.pre) {
        reqs[reqs.length - 1].destroy();
        await new Promise((r) => setTimeout(r, 60));
        wire.length = 0;
      }
      if (opts.closeFirst) {
        client.close();
        if (opts.tick) {
          await new Promise((r) => setImmediate(r));
        }
      }
    }
    opts.destroy(client);
    await new Promise((r) => setTimeout(r, 150));
  } finally {
    await closePeer(srv);
  }
  return { wire, events: out.sort() };
}

const SHAPES = [
  ['no argument', (c) => c.destroy(), 0],
  ['an error', (c) => c.destroy(new Error('boom')), 2],
  ['a numeric code', (c) => c.destroy(8), 8],
  ['an error and a code', (c) => c.destroy(new Error('boom'), 1), 1]
];

test('a destroy says goodbye with the code it derived', async () => {
  for (const [label, destroy, code] of SHAPES) {
    const native = await transcript(http2, { count: 1, destroy });
    assert.deepStrictEqual(native.wire, ['HEADERS(1)', `GOAWAY(0,${code})`],
      `node wire, ${label}`);
    const ours = await transcript(h2lean, { count: 1, destroy });
    assert.deepStrictEqual(ours, native, label);
  }
});

test('the goodbye goes out with no stream, and after a graceful close', async () => {
  const shapes = [
    ['no request', { count: 0, destroy: (c) => c.destroy() }, ['GOAWAY(0,0)']],
    ['a close first', { count: 1, closeFirst: true, destroy: (c) => c.destroy(2) },
      ['HEADERS(1)', 'GOAWAY(0,0)', 'GOAWAY(0,2)']],
    ['a close a tick earlier', { count: 2, closeFirst: true, tick: true,
      destroy: (c) => c.destroy(new Error('boom')) },
    ['HEADERS(1)', 'HEADERS(3)', 'GOAWAY(0,0)', 'RST(1,2)', 'GOAWAY(0,2)']]
  ];
  for (const [label, opts, wire] of shapes) {
    const native = await transcript(http2, opts);
    assert.deepStrictEqual(native.wire, wire, `node wire, ${label}`);
    const ours = await transcript(h2lean, opts);
    assert.deepStrictEqual(ours, native, label);
  }
});

test('every open stream but the last is reset with the session code', async () => {
  for (const count of [1, 2, 3, 4]) {
    const expected = [];
    for (let i = 0; i < count; i++) {
      expected.push(`HEADERS(${i * 2 + 1})`);
    }
    for (let i = 0; i < count - 1; i++) {
      expected.push(`RST(${i * 2 + 1},2)`);
    }
    expected.push('GOAWAY(0,2)');
    const opts = { count, destroy: (c) => c.destroy(new Error('boom')) };
    const native = await transcript(http2, opts);
    assert.deepStrictEqual(native.wire, expected, `node wire, ${count} streams`);
    const ours = await transcript(h2lean, opts);
    assert.deepStrictEqual(ours, native, `${count} streams`);
    // The code reaches every stream, the one that was reset and the one that was not.
    for (let i = 0; i < count; i++) {
      assert.ok(native.events.includes(`close ${i} rstCode=2`), `node rstCode ${i}`);
    }
  }
});

test('a stream already destroyed is not counted, and a connecting session sends nothing',
  async () => {
    const shapes = [
      ['the second request destroyed first',
        { count: 2, pre: true, destroy: (c) => c.destroy() }, ['GOAWAY(0,0)']],
      ['destroyed while connecting',
        { connecting: true, destroy: (c) => c.destroy(new Error('boom')) }, []]
    ];
    for (const [label, opts, wire] of shapes) {
      const native = await transcript(http2, opts);
      assert.deepStrictEqual(native.wire, wire, `node wire, ${label}`);
      const ours = await transcript(h2lean, opts);
      assert.deepStrictEqual(ours.wire, native.wire, label);
      assert.deepStrictEqual(ours.events, native.events, `${label} events`);
    }
  });
