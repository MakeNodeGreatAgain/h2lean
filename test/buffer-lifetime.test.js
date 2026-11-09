// A buffer the application keeps must not change under it.
//
// A frame that arrives in pieces is put in a buffer of its own for the parse, and
// that buffer is not reused -- so a view into it does not move when the next frame
// lands. AGENTS.md 5 ("issued buffers stay valid") is the rule this file pins: a
// buffer handed to the application is either a view of the chunk read off the socket
// or a buffer built for that frame alone, and neither is ever recycled underneath it.
//
// Node hands the PING payload over from the C++ layer as a fresh buffer
// (internal/http2/core.js, onPing(): session.emit('ping', payload)), so it does not
// move there. The shape that shows the difference: four PINGs, of which the second
// and the fourth are split so that the server has to parse them out of the views
// they arrived in -- and the fourth would land on the bytes the second left behind
// if that buffer were reused.
//
// The split is not a race with the clock: the second half of a split PING goes out
// only after the server has answered the PING before it, which proves the server has
// read the first half. A read that coalesced both halves would leave the frame whole
// and the test would pass without testing anything -- it can never fail spuriously.
//
// The event shape differs between the stacks -- node emits the payload alone, h2lite
// emits (ack, payload) -- so the reader is per stack and what is compared is the bytes.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const f = require('../lib/frames');
const h2lite = require('../index');
const nodeHttp2 = require('node:http2');
const { listen, closeServer } = require('./diff-harness');

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// The four payloads. The second and the fourth are the split ones.
const PINGS = ['11111111', 'ABCDEFGH', '33333333', 'IJKLMNOP'];

// The payload out of a 'ping' event. node hands it over alone (internal/http2/core.js,
// onPing()); h2lite hands it over after the ack flag.
const readPing = (impl) => (impl === nodeHttp2 ? (p) => p : (ack, p) => p);

// ---- server side ----------------------------------------------------------------

// The raw client: four PINGs, of which the second and the fourth are split across
// two writes. Each half goes out only after the server has answered the PING before
// it, so the server has provably read the first half before the second one arrives.
function drive(port) {
  return new Promise((resolve, reject) => {
    const sock = net.connect(port, '127.0.0.1');
    sock.on('error', () => {});
    const timer = setTimeout(() => {
      sock.destroy();
      reject(new Error('the PING exchange did not finish'));
    }, 5000);
    let acc = Buffer.alloc(0);
    const ping = (n) => frame(f.PING, 0, 0, Buffer.from(PINGS[n], 'latin1'));
    sock.on('data', (d) => {
      acc = Buffer.concat([acc, d]);
      for (;;) {
        if (acc.length < f.HEADER_LEN) {
          return;
        }
        const len = (acc[0] << 16) | (acc[1] << 8) | acc[2];
        if (acc.length < f.HEADER_LEN + len) {
          return;
        }
        const type = acc[3], flags = acc[4];
        const payload = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
        acc = acc.subarray(f.HEADER_LEN + len);
        if (type === f.SETTINGS && !(flags & f.F_ACK)) {
          sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
          // the server has read our SETTINGS, so the PINGs may go out
          sock.write(Buffer.concat([ping(0), ping(1).subarray(0, 6)]));
        } else if (type === f.PING && (flags & f.F_ACK)) {
          const which = PINGS.indexOf(payload.toString('latin1'));
          if (which === 0) {
            sock.write(ping(1).subarray(6));
          } else if (which === 1) {
            sock.write(Buffer.concat([ping(2), ping(3).subarray(0, 6)]));
          } else if (which === 2) {
            sock.write(ping(3).subarray(6));
          } else if (which === 3) {
            clearTimeout(timer); sock.destroy(); resolve();
          }
        }
      }
    });
    sock.on('connect', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
    });
  });
}

async function serverRun(impl) {
  const kept = [];
  const srv = impl.createServer();
  const read = readPing(impl);
  srv.on('session', (session) => {
    session.on('ping', (...args) => kept.push(read(...args)));
  });
  srv.on('error', () => {});
  const port = await listen(srv);
  try {
    await drive(port);
    return kept;
  } finally {
    await closeServer(srv);
  }
}

test('a PING payload the application keeps does not change when the next frame is assembled',
  async () => {
    const node = await serverRun(nodeHttp2);
    const ours = await serverRun(h2lite);
    assert.strictEqual(node.length, 4, 'node did not see the four PINGs');
    assert.strictEqual(ours.length, 4, 'h2lite did not see the four PINGs');
    assert.ok(Buffer.isBuffer(ours[1]), 'the payload is not a Buffer');
    // The second PING is the one that was assembled, and the fourth is the one that
    // landed on its bytes: without a copy of its own, the payload the application
    // kept would read back as the fourth PING's payload.
    assert.strictEqual(ours[1].toString('latin1'), 'ABCDEFGH',
      'the payload the application kept changed under it');
    assert.strictEqual(ours[1].toString('latin1'), node[1].toString('latin1'),
      'the payload differs from the one node delivers');
  });

// ---- the GOAWAY debug data -------------------------------------------------------

// The debug data of a GOAWAY goes to the application the same way a PING payload
// does. A GOAWAY does not end the session right away -- a stream that is still open
// keeps it alive -- so the bytes the application was handed have to survive the
// frames that follow.
//
// The frame that follows has to be longer than the debug data's place in the
// assembled frame: the debug data starts after the frame header and the
// eight octets of last-stream-id and error code, so a SETTINGS frame with thirty
// entries (189 octets) reaches past a hundred octets of debug data.
const DEBUG = 'x'.repeat(100);

async function goawayRun(impl) {
  let debug = null;
  const srv = impl.createServer(() => { /* the request is never answered: the stream stays live */ });
  srv.on('session', (session) => {
    session.on('goaway', (code, lastStreamID, data) => {
      debug = data;
    });
  });
  srv.on('error', () => {});
  const port = await listen(srv);
  try {
    await new Promise((resolve, reject) => {
      const sock = net.connect(port, '127.0.0.1');
      sock.on('error', () => {});
      const timer = setTimeout(() => {
        sock.destroy();
        reject(new Error('the GOAWAY never arrived'));
      }, 5000);
      sock.on('connect', () => {
        sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
        // a request that is never answered, so the session outlives the GOAWAY
        sock.write(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1,
          Buffer.from([0x82, 0x84, 0x86, 0x01, 9, ...Buffer.from('localhost', 'latin1')])));
        // the GOAWAY, in two pieces: the debug data is put in a buffer of its own
        const goaway = frame(f.GOAWAY, 0, 0,
          Buffer.concat([Buffer.alloc(8), Buffer.from(DEBUG, 'latin1')]));
        setTimeout(() => {
          sock.write(goaway.subarray(0, 6));
          setTimeout(() => sock.write(goaway.subarray(6)), 30);
        }, 50);
        // and then a frame long enough to reach past a hundred octets of debug data
        const entries = Buffer.alloc(30 * 6);
        for (let i = 0; i < 30; i++) {
          entries.writeUInt16BE(5, i * 6); // SETTINGS_MAX_FRAME_SIZE
          entries.writeUInt32BE(16384, i * 6 + 2);
        }
        const settings = frame(f.SETTINGS, 0, 0, entries);
        setTimeout(() => {
          sock.write(settings.subarray(0, 6));
          setTimeout(() => {
            sock.write(settings.subarray(6));
            // the frame is out: the server has read it by the time the next poll
            // returns, and the debug data it would have overwritten is read already
            setTimeout(() => {
              clearTimeout(timer); sock.destroy(); resolve();
            }, 60);
          }, 30);
        }, 200);
      });
    });
    return debug;
  } finally {
    await closeServer(srv);
  }
}

test('the GOAWAY debug data the application keeps does not change', async () => {
  const node = await goawayRun(nodeHttp2);
  const ours = await goawayRun(h2lite);
  assert.ok(node, 'node did not report the GOAWAY');
  assert.ok(ours, 'h2lite did not report the GOAWAY');
  assert.strictEqual(ours.toString('latin1'), DEBUG,
    'the debug data the application kept changed under it');
  assert.strictEqual(ours.toString('latin1'), node.toString('latin1'),
    'the debug data differs from the one node delivers');
});

// ---- client side ----------------------------------------------------------------

// A raw server that answers a PING with an ACK carrying the same payload, split
// across two writes so that the client has to assemble it. It sends no magic: the
// magic preface is the CLIENT's (RFC 9113 3.4 -- a server connection preface is a
// SETTINGS frame and nothing else), and node's client refuses a server that sends
// one with GOAWAY(PROTOCOL_ERROR, "SETTINGS expected").
function rawPinger() {
  return new Promise((resolve) => {
    const sockets = [];
    const srv = net.createServer((sock) => {
      sockets.push(sock);
      sock.on('error', () => {});
      sock.write(frame(f.SETTINGS, 0, 0, Buffer.alloc(0)));
      let acc = Buffer.alloc(0);
      let sawPreface = false;
      sock.on('data', (d) => {
        if (!sawPreface) {
          // the client opens with its 24-octet preface; the frames start after it
          if (d.length < f.PREFACE_LEN) {
            return;
          }
          sawPreface = true;
          d = d.subarray(f.PREFACE_LEN);
        }
        acc = Buffer.concat([acc, d]);
        for (;;) {
          if (acc.length < f.HEADER_LEN) {
            return;
          }
          const len = (acc[0] << 16) | (acc[1] << 8) | acc[2];
          if (acc.length < f.HEADER_LEN + len) {
            return;
          }
          const type = acc[3], flags = acc[4];
          const payload = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
          acc = acc.subarray(f.HEADER_LEN + len);
          if (type === f.SETTINGS && !(flags & f.F_ACK)) {
            sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
          } else if (type === f.PING && !(flags & f.F_ACK)) {
            const ack = frame(f.PING, f.F_ACK, 0, payload);
            sock.write(ack.subarray(0, 6));
            setTimeout(() => sock.write(ack.subarray(6)), 30);
          }
        }
      });
      setTimeout(() => sock.destroy(), 2000);
    });
    srv.listen(0, '127.0.0.1', () => resolve({ srv, sockets }));
  });
}

async function clientRun(impl) {
  const { srv, sockets } = await rawPinger();
  try {
    const pongs = [];
    const sess = impl.connect(`http://127.0.0.1:${srv.address().port}`);
    sess.on('error', () => {});
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('the ping callback never fired')), 5000);
      // node calls the callback of a ping sent while the session is still connecting
      // with a cancellation error (internal/http2/core.js, ping()), so the ping waits
      // for the connection.
      sess.on('connect', () => {
        sess.ping(Buffer.from('ABCDEFGH', 'latin1'), (err, duration, pong) => {
          if (err) {
            clearTimeout(timer); reject(err); return;
          }
          pongs.push(pong);
          // the second ping is the one that makes the client parse a frame that
          // arrives in pieces, while the first pong is still kept
          sess.ping(Buffer.from('IJKLMNOP', 'latin1'), (err2, duration2, pong2) => {
            if (err2) {
              clearTimeout(timer); reject(err2); return;
            }
            pongs.push(pong2);
            clearTimeout(timer);
            resolve();
          });
        });
      });
    });
    sess.destroy();
    return pongs;
  } finally {
    for (const s of sockets) {
      s.destroy();
    }
    srv.close();
  }
}

test('the pong a ping callback keeps does not change when the next frame is assembled',
  async () => {
    const node = await clientRun(nodeHttp2);
    const ours = await clientRun(h2lite);
    assert.strictEqual(node.length, 2, 'node did not answer the two pings');
    assert.strictEqual(ours.length, 2, 'h2lite did not answer the two pings');
    assert.ok(Buffer.isBuffer(ours[0]), 'the pong is not a Buffer');
    assert.strictEqual(ours[0].toString('latin1'), 'ABCDEFGH',
      'the pong the callback kept changed under it');
    assert.strictEqual(ours[0].toString('latin1'), node[0].toString('latin1'),
      'the pong differs from the one node delivers');
  });

// ---- the client's GOAWAY debug data ----------------------------------------------

// The same rule on the client's side. A client that still has a stream open keeps
// the session alive after a GOAWAY, so the bytes the application was handed have to
// survive the frames that follow.
function rawGoaway(debug) {
  return new Promise((resolve) => {
    const sockets = [];
    const srv = net.createServer((sock) => {
      sockets.push(sock);
      sock.on('error', () => {});
      sock.write(frame(f.SETTINGS, 0, 0, Buffer.alloc(0)));
      let acc = Buffer.alloc(0);
      let sawPreface = false;
      let gotRequest = false;
      sock.on('data', (d) => {
        if (!sawPreface) {
          if (d.length < f.PREFACE_LEN) {
            return;
          }
          sawPreface = true;
          d = d.subarray(f.PREFACE_LEN);
        }
        acc = Buffer.concat([acc, d]);
        for (;;) {
          if (acc.length < f.HEADER_LEN) {
            return;
          }
          const len = (acc[0] << 16) | (acc[1] << 8) | acc[2];
          if (acc.length < f.HEADER_LEN + len) {
            return;
          }
          const type = acc[3], flags = acc[4];
          acc = acc.subarray(f.HEADER_LEN + len);
          if (type === f.SETTINGS && !(flags & f.F_ACK)) {
            sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
          } else if (type === f.HEADERS && !gotRequest) {
            gotRequest = true;
            // the request is never answered: the stream stays open, so the client
            // keeps the session alive after the GOAWAY
            const goaway = frame(f.GOAWAY, 0, 0,
              Buffer.concat([Buffer.alloc(8), Buffer.from(debug, 'latin1')]));
            sock.write(goaway.subarray(0, 6));
            setTimeout(() => sock.write(goaway.subarray(6)), 30);
            // and then a frame long enough to reach past a hundred octets of debug data
            const entries = Buffer.alloc(30 * 6);
            for (let i = 0; i < 30; i++) {
              entries.writeUInt16BE(5, i * 6); // SETTINGS_MAX_FRAME_SIZE
              entries.writeUInt32BE(16384, i * 6 + 2);
            }
            const settings = frame(f.SETTINGS, 0, 0, entries);
            setTimeout(() => {
              sock.write(settings.subarray(0, 6));
              setTimeout(() => sock.write(settings.subarray(6)), 30);
            }, 200);
          }
        }
      });
      setTimeout(() => sock.destroy(), 3000);
    });
    srv.listen(0, '127.0.0.1', () => resolve({ srv, sockets }));
  });
}

async function clientGoaway(impl) {
  const { srv, sockets } = await rawGoaway(DEBUG);
  try {
    let debug = null;
    const sess = impl.connect(`http://127.0.0.1:${srv.address().port}`);
    sess.on('error', () => {});
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('the GOAWAY never arrived')), 5000);
      sess.on('goaway', (code, lastStreamID, data) => {
        debug = data;
        clearTimeout(timer);
        // the frame that overwrites the bytes comes later: wait for it
        setTimeout(resolve, 400);
      });
      sess.on('connect', () => {
        const req = sess.request({ ':path': '/' });
        req.on('error', () => {});
        req.resume();
      });
    });
    sess.destroy();
    return debug;
  } finally {
    for (const s of sockets) {
      s.destroy();
    }
    srv.close();
  }
}

test('the GOAWAY debug data a client keeps does not change', async () => {
  const node = await clientGoaway(nodeHttp2);
  const ours = await clientGoaway(h2lite);
  assert.ok(node, 'node did not report the GOAWAY');
  assert.ok(ours, 'h2lite did not report the GOAWAY');
  assert.strictEqual(ours.toString('latin1'), DEBUG,
    'the debug data the application kept changed under it');
  assert.strictEqual(ours.toString('latin1'), node.toString('latin1'),
    'the debug data differs from the one node delivers');
});
