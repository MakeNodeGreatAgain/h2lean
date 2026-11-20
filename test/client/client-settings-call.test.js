// session.settings() on the client: the argument shape, the wire payload and the
// callback, all read off node's own bundled internal/http2/core.js (settings() ->
// submitSettings() -> settingsCallback()) and internal/http2/util.js
// (validateSettings' ranges live in core.js, updateSettingsBuffer/getSettings in util)
// and measured against a live node v26.12.1 client in the same session.
//
// The peer here is a raw TCP server that decodes frames, because node writes socket
// bytes from C++: a socket.write wrapper on a node client captures nothing. Both arms
// are observed through the same peer, and every expectation below is node's transcript,
// not h2lean's -- the two are compared, so a change that moves h2lean off node fails.
//
// What the measurement pinned:
//   * settings({headerTableSize: 4096}) submits SETTINGS{headerTableSize=4096} -- only
//     the named entries, not the whole local set;
//   * several entries leave in ascending SETTING-ID order, not the object's key order:
//     {headerTableSize, maxConcurrentStreams, enablePush} arrived as ids 1, 2, 3;
//   * the callback is deferred to the matching SETTINGS|ACK and receives
//     (null, localSettings, duration); 'localSettings' is emitted on EVERY ack,
//     including the one for the connection's own first SETTINGS frame, which has no
//     callback. Two acks therefore produce event, callback, event -- the first ack
//     answers the connection's own submit, not the application's;
//   * duration is NOT an elapsed time. node reports its monotonic clock reading in
//     milliseconds: a submit 120 ms earlier answered with 524545981.423 where
//     Number(process.hrtime.bigint())/1e6 read 524545981.445, and a second call 200 ms
//     later tracked the clock again rather than the interval. Only the type is compared
//     here, since the reading itself is a clock value no two runs share;
//   * localSettings reports eight keys in getSettings()' order, with maxHeaderListSize
//     and its alias maxHeaderSize at 4294967295 -- what nghttp2 has in effect, not the
//     65535 getDefaultSettings() prints;
//   * a submitted value reaches localSettings when ITS OWN ack arrives, not at submit.
//     Measured for all seven settings: submitting headerTableSize 8192 produced events
//     [4096, 8192] and maxHeaderListSize 1234 produced [4294967295, 1234] -- the first
//     event is the ack for the connection's own initial frame and still reports the old
//     value;
//   * undefined is a legal argument and submits an EMPTY SETTINGS frame; a Buffer is an
//     object whose setting properties are all undefined, so it passes and submits
//     nothing; null, a string and an array are ERR_INVALID_ARG_TYPE; an out-of-range
//     value is ERR_HTTP2_INVALID_SETTING_VALUE (a TypeError, not ERR_OUT_OF_RANGE);
//   * while connecting, the submit is deferred to 'connect' rather than refused;
//   * a peer that never ACKs: the connection's own SETTINGS plus nine settings() calls
//     go out (ten frames), the tenth call submits nothing and destroys the session with
//     ERR_HTTP2_MAX_PENDING_SETTINGS_ACK, and the call after it throws
//     ERR_HTTP2_INVALID_SESSION because the session is already gone.
//
// KNOWN DIVERGENCE, not pinned here: node also accepts a `customSettings` object of raw
// id/value pairs and puts those entries on the wire; h2lean submits the seven standard
// settings only (lib/client-session.js header). The destroy() GOAWAY that node sends and
// h2lean does not is a separate gap, so the never-ACK case compares the SETTINGS frames
// and the session error, not the whole wire.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const http2 = require('node:http2');
const h2lean = require('../../index');
const { INITIAL_WINDOW_CEILING_2_31 } = require('../node-version');

const NAMES = {
  1: 'headerTableSize', 2: 'enablePush', 3: 'maxConcurrentStreams',
  4: 'initialWindowSize', 5: 'maxFrameSize', 6: 'maxHeaderListSize',
  8: 'enableConnectProtocol'
};
const PREFACE = 'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n';

// A raw peer that records the SETTINGS entries the client submits and ACKs each one,
// unless `noAck` asks it to stay silent. It sends its own empty SETTINGS as the server
// preface so the client's 'connect' fires.
function peer(wire, noAck) {
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
          sock.write(Buffer.from([0, 0, 0, 4, 0, 0, 0, 0, 0]));
        }
        while (buf.length >= 9) {
          const len = buf.readUIntBE(0, 3);
          if (buf.length < 9 + len) {
            break;
          }
          const type = buf[3];
          const flags = buf[4];
          const body = buf.subarray(9, 9 + len);
          buf = buf.subarray(9 + len);
          if (type === 4 && !(flags & 1)) {
            const entries = [];
            for (let at = 0; at + 6 <= body.length; at += 6) {
              const id = body.readUInt16BE(at);
              entries.push(`${NAMES[id] || id}=${body.readUInt32BE(at + 2)}`);
            }
            wire.push(`SETTINGS{${entries.join(',')}}`);
            if (!noAck) {
              sock.write(Buffer.from([0, 0, 0, 4, 1, 0, 0, 0, 0]));
            }
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

// One settings() call answered by the peer, with the callback arguments and the
// 'localSettings' events recorded in the order they happened.
async function callTranscript(impl, settingsArg) {
  const wire = [];
  const { srv, port } = await peer(wire, false);
  const out = [];
  try {
    const client = impl.connect(`http://127.0.0.1:${port}`);
    client.on('error', () => {});
    client.on('localSettings', (s) => out.push(`event ${JSON.stringify(s)}`));
    await new Promise((r) => client.once('connect', r));
    await new Promise((r) => {
      client.settings(settingsArg, (err, s, duration) => {
        out.push(`cb err=${err && err.code} settings=${JSON.stringify(s)} duration=${typeof duration}`);
        r();
      });
    });
    out.push(`wire=${wire.join(' | ')}`);
    client.destroy();
  } finally {
    await closePeer(srv);
  }
  return out;
}

// The synchronous argument checks, as code plus message.
async function validationTranscript(impl) {
  const wire = [];
  const { srv, port } = await peer(wire, false);
  const out = [];
  try {
    const client = impl.connect(`http://127.0.0.1:${port}`);
    client.on('error', () => {});
    await new Promise((r) => client.once('connect', r));
    const attempt = (label, fn) => {
      try {
        const v = fn();
        out.push(`${label} -> ok ${v === undefined ? 'undefined' : typeof v}`);
      } catch (e) {
        out.push(`${label} -> ${e.code}: ${e.message}`);
      }
    };
    attempt('no args', () => client.settings());
    attempt('null', () => client.settings(null));
    attempt('empty object', () => client.settings({}));
    attempt('string', () => client.settings('x'));
    attempt('buffer', () => client.settings(Buffer.alloc(6)));
    attempt('array', () => client.settings([]));
    attempt('function', () => client.settings(() => {}));
    attempt('bad callback', () => client.settings({}, 'nope'));
    attempt('null callback', () => client.settings({}, null));
    attempt('maxFrameSize too small', () => client.settings({ maxFrameSize: 100 }));
    attempt('maxFrameSize too large', () => client.settings({ maxFrameSize: 2 ** 24 }));
    // 2^31 is over the node 24+ ceiling (refused in JS); on 20/22 it passes JS validation,
    // nghttp2 rejects it and Http2Settings::Send() CHECKs the submit, aborting the process.
    // Omitted from both arms there so the suite survives (test/node-version.js).
    if (INITIAL_WINDOW_CEILING_2_31) {
      attempt('initialWindowSize too large', () => client.settings({ initialWindowSize: 2 ** 31 }));
    }
    attempt('enablePush not boolean', () => client.settings({ enablePush: 1 }));
    attempt('enableConnectProtocol not boolean', () => client.settings({ enableConnectProtocol: 'yes' }));
    attempt('unknown key', () => client.settings({ nonsense: 5 }));
    attempt('negative', () => client.settings({ headerTableSize: -1 }));
    attempt('non-number', () => client.settings({ maxConcurrentStreams: '4' }));
    client.destroy();
  } finally {
    await closePeer(srv);
  }
  return out;
}

test('settings() submits only the named entries and answers on the matching ACK', async () => {
  const expected = await callTranscript(http2, { headerTableSize: 4096 });
  const actual = await callTranscript(h2lean, { headerTableSize: 4096 });
  assert.deepStrictEqual(actual, expected);
  // node's own values, pinned so a regression in either arm is visible in the failure
  assert.ok(expected.some((l) => l.startsWith('wire=SETTINGS{} | SETTINGS{headerTableSize=4096}')));
  assert.ok(expected.some((l) => l.startsWith('cb err=null')));
  assert.strictEqual(expected.filter((l) => l.startsWith('event ')).length, 2);
});

test('several entries leave in setting-id order, not key order', async () => {
  const arg = { headerTableSize: 8192, maxConcurrentStreams: 7, enablePush: false };
  const expected = await callTranscript(http2, arg);
  const actual = await callTranscript(h2lean, arg);
  assert.deepStrictEqual(actual, expected);
  assert.ok(expected.some((l) => l.includes(
    'SETTINGS{headerTableSize=8192,enablePush=0,maxConcurrentStreams=7}')));
});

test('an undefined argument submits an empty SETTINGS frame', async () => {
  const expected = await callTranscript(http2, undefined);
  const actual = await callTranscript(h2lean, undefined);
  assert.deepStrictEqual(actual, expected);
  assert.ok(expected.some((l) => l.startsWith('wire=SETTINGS{} | SETTINGS{}')));
});

test('maxHeaderSize is the deprecated alias of maxHeaderListSize', async () => {
  const expected = await callTranscript(http2, { maxHeaderSize: 1234 });
  const actual = await callTranscript(h2lean, { maxHeaderSize: 1234 });
  assert.deepStrictEqual(actual, expected);
  assert.ok(expected.some((l) => l.includes('SETTINGS{maxHeaderListSize=1234}')));
  assert.ok(expected.some((l) => l.includes('"maxHeaderListSize":1234,"maxHeaderSize":1234')));
});

test('the argument checks report node\'s codes and messages', async () => {
  const expected = await validationTranscript(http2);
  const actual = await validationTranscript(h2lean);
  assert.deepStrictEqual(actual, expected);
  assert.ok(expected.includes('no args -> ok undefined'));
  assert.ok(expected.includes('null -> ERR_INVALID_ARG_TYPE: The "settings" argument must be of type object. Received null'));
  assert.ok(expected.includes('maxFrameSize too small -> ERR_HTTP2_INVALID_SETTING_VALUE: Invalid value for setting "maxFrameSize": 100'));
  assert.ok(expected.includes('enablePush not boolean -> ERR_HTTP2_INVALID_SETTING_VALUE: Invalid value for setting "enablePush": 1'));
});

test('a submit while connecting is deferred to connect, not refused', async () => {
  const transcripts = [];
  for (const impl of [http2, h2lean]) {
    const wire = [];
    const { srv, port } = await peer(wire, false);
    const out = [];
    try {
      const client = impl.connect(`http://127.0.0.1:${port}`);
      client.on('error', () => {});
      out.push(`connecting=${client.connecting}`);
      client.settings({ maxConcurrentStreams: 11 }, (err) => out.push(`cb err=${err && err.code}`));
      out.push('returned while connecting');
      await new Promise((r) => setTimeout(r, 300));
      out.push(`wire=${wire.join(' | ')}`);
      client.destroy();
    } finally {
      await closePeer(srv);
    }
    transcripts.push(out);
  }
  assert.deepStrictEqual(transcripts[1], transcripts[0]);
  assert.ok(transcripts[0].includes('connecting=true'));
  assert.ok(transcripts[0].includes('cb err=null'));
  assert.ok(transcripts[0].some((l) => l.includes('SETTINGS{maxConcurrentStreams=11}')));
});

test('ten unacked SETTINGS frames are the limit; the next submit destroys the session', async () => {
  const transcripts = [];
  for (const impl of [http2, h2lean]) {
    const wire = [];
    const { srv, port } = await peer(wire, true);
    const out = [];
    try {
      const client = impl.connect(`http://127.0.0.1:${port}`);
      client.on('error', (e) => out.push(`session error ${e.code}`));
      await new Promise((r) => client.once('connect', r));
      for (let i = 0; i < 11; i++) {
        try {
          client.settings({ maxConcurrentStreams: i });
        } catch (e) {
          out.push(`call ${i} threw ${e.code}`); break;
        }
        // Yield so each submit's frame actually leaves. h2lean stages output into one
        // batch per tick and node writes through nghttp2, so a synchronous loop of
        // submits compares two flush schedules rather than the cap under test here.
        await new Promise((r) => setImmediate(r));
      }
      await new Promise((r) => setTimeout(r, 250));
      out.push(`destroyed=${client.destroyed}`);
      // the GOAWAY node's destroy() submits is a separate gap, so only the SETTINGS
      // frames are compared here
      out.push(`frames=${wire.length}`);
      client.destroy();
    } finally {
      await closePeer(srv);
    }
    transcripts.push(out.sort());
  }
  // Sorted because the session 'error' and the synchronous throw from the next call
  // land in a different order within the turn: node defers the 'error' emission, h2lean
  // raises it inside destroy(). That is the destroy path's timing, not the cap under
  // test here, and the same allowance the other client ports make.
  assert.deepStrictEqual(transcripts[1], transcripts[0]);
  assert.deepStrictEqual(transcripts[0], [
    'call 10 threw ERR_HTTP2_INVALID_SESSION',
    'destroyed=true',
    'frames=10',
    'session error ERR_HTTP2_MAX_PENDING_SETTINGS_ACK'
  ]);
});
