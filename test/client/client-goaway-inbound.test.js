// The branch an inbound GOAWAY takes, and what the client reports and puts on the wire
// when the peer's code is not NO_ERROR. node's onGoawayData (internal/http2/core.js:758-783)
// records the code and lastStreamID, emits 'goaway' synchronously -- the comment above it
// claiming next tick is stale -- and then branches: NO_ERROR calls session.close(), anything
// else calls session.destroy(new ERR_HTTP2_SESSION_ERROR(code), NGHTTP2_NO_ERROR). The
// second argument is why the answer on the wire carries 0 and not the peer's code; node's
// own comment gives the reason, "there was no error condition on this side of the session
// that caused the shutdown".
//
// Measured on a live node v26.12.1 client against a raw frame-decoding peer, in the same
// session, through the same peer for both arms:
//   * codes 1, 2, 7 and 8 all destroy at once. The session and every open stream report
//     ERR_HTTP2_SESSION_ERROR naming the peer's code, each stream's rstCode IS that code,
//     and the event order is goaway, then per stream error followed by close in ascending
//     id order, then the session's error and close. Repeated runs agree on the order;
//   * the wire is the request HEADERS and GOAWAY(last=0, code=0) ALONE -- no RST_STREAM
//     for any stream count. nghttp2 has already closed the streams when closeSession runs,
//     so its stream.destroy finds no handle and submits nothing (core.js:2526,
//     `hasHandle ? kForceRstStream : kNoRstStream`). The nghttp2 half of that was not read;
//     the rule pinned here is the wire measurement across three stream counts and four
//     lastStreamID values, not a derivation from it;
//   * lastStreamID does not change the non-zero branch: 0, 1, 3 and 5 all give the same
//     transcript. It is the NO_ERROR branch that reads it;
//   * destroyed is true and closed stays FALSE. node's destroy() never sets
//     SESSION_FLAGS_CLOSED -- only close() does (core.js:1657);
//   * a request made afterwards is accepted, not thrown, and errors
//     ERR_HTTP2_INVALID_SESSION with the goaway code as its rstCode;
//   * the third 'goaway' argument is a Buffer when the peer sent debug data and undefined
//     when it sent none, not an empty Buffer.
//
// The NO_ERROR control below is the other half of the branch: a lastStreamID that covers
// every open stream leaves them alive (rstCode 0, not destroyed) and the session closed
// but not destroyed, and a later request is refused ERR_HTTP2_GOAWAY_SESSION. Both arms
// agree on it.
//
// KNOWN DIVERGENCE, documented in lib/client-session.js and not pinned here: a NO_ERROR
// GOAWAY whose lastStreamID is BELOW the open ids makes node refuse the unprocessed
// streams itself -- rstCode 7, ERR_HTTP2_STREAM_ERROR naming NGHTTP2_REFUSED_STREAM, and
// the session destroys once they are gone. h2lean does not refuse them: they stay open
// with rstCode 0 until the connection goes. Pinning it would also pin node's second
// GOAWAY(0,0) on that path, which is the separate graceful-close echo item.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const http2 = require('node:http2');
const h2lean = require('../../index');
const f = require('../../lib/frames');

const PREFACE = 'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n';
const SESSION_ERROR = 'ERR_HTTP2_SESSION_ERROR/Session closed with error code';

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// A raw peer that completes the handshake, records every frame the client sends in order,
// and answers nothing -- so the only frames on the wire are the ones the GOAWAY under test
// produced. Once `count` request HEADERS have arrived it sends GOAWAY(last, code) with the
// optional debug tail; count 0 sends it as soon as the preface is seen. Socket events are
// deliberately not recorded: whether the peer sees a clean close or ECONNRESET after the
// goodbye is the CONTRIBUTING.md 5 GOAWAY-echo race, below the HTTP/2 surface.
function peer(wire, opts) {
  return new Promise((resolve) => {
    const payload = Buffer.alloc(8 + (opts.debug === undefined ? 0 : opts.debug.length));
    payload.writeUInt32BE(opts.last, 0);
    payload.writeUInt32BE(opts.code, 4);
    if (opts.debug !== undefined) {
      Buffer.from(opts.debug).copy(payload, 8);
    }
    const goodbye = frame(f.GOAWAY, 0, 0, payload);
    const srv = net.createServer((sock) => {
      let buf = Buffer.alloc(0);
      let prefaceSeen = false;
      let sent = false;
      let headers = 0;
      const maybeSend = () => {
        if (!sent && headers >= opts.count) {
          sent = true;
          sock.write(goodbye);
        }
      };
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
          maybeSend();
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
            headers++;
            maybeSend();
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

// The events in the order they fired -- the teardown order is part of what node's
// onGoawayData fixes, since the emit is synchronous and the destroy follows it in the
// same call. `state` and `streams` are snapshotted once the exchange has settled, and
// `late` is what a request made after the goodbye reports.
async function transcript(impl, opts) {
  const wire = [];
  const { srv, port } = await peer(wire, opts);
  const events = [];
  const late = [];
  let state;
  let streams;
  // The peer teardown below closes the socket, and node answers that with a CANCEL on
  // every live stream -- after the exchange under test is over. Snapshot before it.
  let seen;
  try {
    const client = impl.connect(`http://127.0.0.1:${port}`);
    client.on('goaway', (code, last, debug) => events.push(
      `goaway ${code} ${last} ${debug === undefined ? 'undefined' : debug.toString()}`));
    client.on('error', (e) => events.push(`session error ${e.code}/${e.message}`));
    client.on('close', () => events.push('session close'));
    await new Promise((r) => client.once('connect', r));
    const reqs = [];
    for (let i = 0; i < opts.count; i++) {
      const req = client.request({ ':path': `/${i}` });
      req.on('error', (e) => events.push(`req${i} error ${e.code}/${e.message}`));
      req.on('close', () => events.push(`req${i} close ${req.rstCode}`));
      req.resume();
      reqs.push(req);
    }
    await new Promise((r) => setTimeout(r, 200));
    state = `destroyed=${client.destroyed} closed=${client.closed}`;
    streams = reqs.map((r) => `${r.rstCode}/${r.destroyed}`);
    try {
      const req = client.request({ ':path': '/late' });
      late.push('accepted');
      req.on('error', (e) => late.push(`error ${e.code}/${e.message}`));
      req.on('close', () => late.push(`close ${req.rstCode}`));
      req.resume();
    } catch (e) {
      late.push(`threw ${e.code}`);
    }
    await new Promise((r) => setTimeout(r, 150));
    seen = { wire: wire.slice(), events: events.slice(), late: late.slice() };
  } finally {
    await closePeer(srv);
  }
  return { wire: seen.wire, events: seen.events, state, streams, late: seen.late };
}

// The transcript a non-zero code produces, derived from the measured rule rather than
// copied per case: the goaway event, every stream's error and close in ascending id
// order, then the session's own.
function destroyedTranscript(code, count) {
  const events = [`goaway ${code} 0 undefined`];
  for (let i = 0; i < count; i++) {
    events.push(`req${i} error ${SESSION_ERROR} ${code}`);
    events.push(`req${i} close ${code}`);
  }
  events.push(`session error ${SESSION_ERROR} ${code}`);
  events.push('session close');
  const wire = [];
  for (let i = 0; i < count; i++) {
    wire.push(`HEADERS(${i * 2 + 1})`);
  }
  wire.push('GOAWAY(0,0)');
  return {
    wire,
    events,
    state: 'destroyed=true closed=false',
    streams: new Array(count).fill(`${code}/true`),
    late: ['accepted', 'error ERR_HTTP2_INVALID_SESSION/The session has been destroyed',
      `close ${code}`]
  };
}

test('a non-zero inbound GOAWAY destroys the session and answers NO_ERROR', async () => {
  for (const code of [1, 2, 7, 8]) {
    const want = destroyedTranscript(code, 1);
    const native = await transcript(http2, { code, last: 0, count: 1 });
    assert.deepStrictEqual(native, want, `node, code ${code}`);
    const ours = await transcript(h2lean, { code, last: 0, count: 1 });
    assert.deepStrictEqual(ours, native, `code ${code}`);
  }
});

test('no stream is reset, whatever the count and whatever lastStreamID said', async () => {
  // lastStreamID is the NO_ERROR branch's business: on a non-zero code node destroys
  // without consulting it, so a mark below, inside and above the open ids all give the
  // same transcript. The absence of RST_STREAM is the point -- an application destroy
  // resets all but the last open stream (client-destroy-goaway.test.js), this path
  // resets none.
  const shapes = [[1, 0], [2, 0], [3, 0], [3, 1], [3, 3], [3, 5]];
  for (const [count, last] of shapes) {
    const want = destroyedTranscript(2, count);
    want.events[0] = `goaway 2 ${last} undefined`;
    const opts = { code: 2, last, count };
    const native = await transcript(http2, opts);
    assert.deepStrictEqual(native, want, `node, ${count} streams last=${last}`);
    const ours = await transcript(h2lean, opts);
    assert.deepStrictEqual(ours, native, `${count} streams last=${last}`);
  }
});

test('the debug tail arrives as a Buffer, and is undefined when there is none', async () => {
  const withDebug = await transcript(http2, { code: 8, last: 0, count: 1, debug: 'bye' });
  assert.strictEqual(withDebug.events[0], 'goaway 8 0 bye', 'node debug');
  const ours = await transcript(h2lean, { code: 8, last: 0, count: 1, debug: 'bye' });
  assert.deepStrictEqual(ours, withDebug, 'debug tail');
  const without = await transcript(h2lean, { code: 8, last: 0, count: 1 });
  assert.strictEqual(without.events[0], 'goaway 8 0 undefined', 'no debug tail');
});

test('a NO_ERROR GOAWAY that covers the open streams leaves them alive', async () => {
  // The control for the branch: same frame, code 0, lastStreamID above every open id.
  // node begins the graceful close and the streams finish naturally, so nothing is
  // refused, nothing is reset and the session is closed but not destroyed.
  const want = {
    wire: ['HEADERS(1)', 'HEADERS(3)', 'GOAWAY(0,0)'],
    events: ['goaway 0 5 undefined'],
    state: 'destroyed=false closed=true',
    streams: ['0/false', '0/false'],
    late: ['accepted',
      'error ERR_HTTP2_GOAWAY_SESSION/New streams cannot be created after receiving a GOAWAY',
      'close 2']
  };
  const opts = { code: 0, last: 5, count: 2 };
  const native = await transcript(http2, opts);
  assert.deepStrictEqual(native, want, 'node');
  const ours = await transcript(h2lean, opts);
  assert.deepStrictEqual(ours, native, 'NO_ERROR control');
});
