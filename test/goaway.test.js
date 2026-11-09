// What the session does when the peer says goodbye. Every expectation here was measured against
// a live node 26 server first (the traces and the state snapshots below are what node reported),
// and both stacks are then run through the same script.
//
// Measured rules:
//   * GOAWAY(NO_ERROR) begins a graceful shutdown: `session.close()` -- the session is closed,
//     NOT destroyed, and the stream already in front of this side keeps its place. node's own
//     comment on that branch is "No new streams permitted, but existing streams may close
//     naturally on their own". h2lite destroyed every stream above the peer's lastStreamID,
//     which closed a live request the moment the peer said goodbye.
//   * close() does not end the socket while something is still being carried. Only once the
//     session has nothing left to do does node let the connection go.
//   * any other code destroys the session at once, and hands the application
//     ERR_HTTP2_SESSION_ERROR -- on every open stream first, then on the session -- while the
//     GOAWAY this side writes carries NO_ERROR, because this side did not fail. `closed` stays
//     false: only close() sets it.
//   * a GOAWAY that arrives is answered with a GOAWAY of our own, even though the peer's has
//     already been received.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const f = require('../lib/frames');
const h2lite = require('../index');
const nodeHttp2 = require('node:http2');
const { listen, connect, closeServer } = require('./diff-harness');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const GET = { ':method': 'GET', ':path': '/', ':scheme': 'http', ':authority': 'localhost' };

// One server, one node client, and a snapshot of what the server side looked like after the
// peer's GOAWAY arrived: the events in order, the two public flags, and what the client was
// told. `hold` leaves the request unanswered, which is the only way to see whether the goodbye
// was allowed to disturb a stream that was still open.
async function goodbye(impl, code) {
  const ev = [];
  const peer = [];
  const srv = impl.createServer();
  let session = null;
  srv.on('session', (s) => {
    session = s;
    s.on('goaway', (c, last) => ev.push(`goaway:${c}/${last}`));
    s.on('error', (e) => ev.push('error:' + e.code));
    s.on('close', () => ev.push('session.close'));
  });
  srv.on('stream', (stream) => {
    ev.push('stream');
    stream.on('error', (e) => ev.push('stream.error:' + e.code));
    stream.on('close', () => ev.push('stream.close'));
  });
  srv.on('error', () => {});
  const port = await listen(srv);
  const client = connect(port);
  client.on('goaway', (c, last) => peer.push(`goaway:${c}/${last}`));
  client.on('error', (e) => peer.push('error:' + e.code));
  const req = client.request(GET);
  req.resume();
  req.on('error', () => {});
  req.end();
  await sleep(150);
  const open = session !== null;
  client.goaway(code);
  await sleep(400);
  const got = {
    open,
    events: ev.join(','),
    closed: session.closed,
    destroyed: session.destroyed,
    goawayCode: session.goawayCode,
    goawayLastStreamID: session.goawayLastStreamID,
    peer: peer.join(',')
  };
  client.destroy();
  await sleep(80);
  srv.close();
  return got;
}

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// :method GET, :path /, :scheme http, :authority localhost -- the same block as GET above,
// encoded by hand so that it can be split across two reads.
const REQ = Buffer.from([0x82, 0x84, 0x86, 0x01, 9, ...Buffer.from('localhost', 'latin1')]);

test('GOAWAY(NO_ERROR) closes the session and leaves the open stream alone', async () => {
  const got = await Promise.all([goodbye(nodeHttp2, 0), goodbye(h2lite, 0)]);
  // node, measured: the goodbye is reported, then reported again when the peer answers ours,
  // and nothing in between is torn down.
  const want = {
    open: true,
    events: 'stream,goaway:0/0,goaway:0/0',
    closed: true,
    destroyed: false,
    goawayCode: 0,
    goawayLastStreamID: 0,
    peer: 'goaway:0/1'
  };
  assert.deepStrictEqual(got[0], want, `node: ${JSON.stringify(got[0])}`);
  assert.deepStrictEqual(got[1], want, `h2lite: ${JSON.stringify(got[1])} vs node ${JSON.stringify(got[0])}`);
});

test('a GOAWAY with an error code is reported as an error and answered with NO_ERROR', async () => {
  for (const code of [2, 11]) {
    const got = await Promise.all([goodbye(nodeHttp2, code), goodbye(h2lite, code)]);
    // node, measured: stream 'error' then 'close', the session's own 'error' after them, the
    // session destroyed but never marked closed, and the peer given GOAWAY(NO_ERROR, 1).
    const want = {
      open: true,
      events: 'stream,goaway:' + code + '/0,stream.error:ERR_HTTP2_SESSION_ERROR,'
            + 'stream.close,error:ERR_HTTP2_SESSION_ERROR,session.close',
      closed: false,
      destroyed: true,
      goawayCode: code,
      goawayLastStreamID: 0,
      peer: 'goaway:0/1'
    };
    assert.deepStrictEqual(got[0], want, `node(code=${code}): ${JSON.stringify(got[0])}`);
    assert.deepStrictEqual(got[1], want,
      `h2lite(code=${code}): ${JSON.stringify(got[1])} vs node ${JSON.stringify(got[0])}`);
  }
});

// A session that was closed while a request was still open has to be finished by that request's
// own teardown: node's close() asks for a graceful close and [kMaybeDestroy] runs again when the
// last stream goes. Compared rather than hard-coded, because what is being pinned is that the
// session really ends -- a session left closed-but-alive holds its connection open forever.
async function drainedAfterClose(impl) {
  const srv = impl.createServer();
  let session = null;
  let held = null;
  srv.on('session', (s) => {
    session = s;
  });
  srv.on('stream', (stream) => {
    held = stream; stream.on('error', () => {});
  });
  srv.on('error', () => {});
  const port = await listen(srv);
  const client = connect(port);
  // What the peer sees is the point of the guard, and {closed, destroyed} alone does not notice
  // it: node's graceful close ENDS the socket, so the client reads a FIN. A closing session that
  // is never let go leaves the connection half-open, which is invisible in the flags -- the
  // session still reports closed=true -- and fatal for a client waiting to reconnect.
  const peer = [];
  client.socket.on('end', () => peer.push('fin'));
  client.socket.on('error', (e) => peer.push('err:' + e.code));
  const req = client.request(GET);
  req.resume();
  req.on('error', () => {});
  req.end();
  await sleep(150);
  session.close();
  await sleep(80);
  const afterClose = { closed: session.closed, destroyed: session.destroyed, peer: [...peer] };
  held.destroy(); // the application gives up on the request
  await sleep(250);
  const afterStream = { closed: session.closed, destroyed: session.destroyed, peer: [...peer] };
  client.destroy();
  srv.close();
  await sleep(80);
  return { afterClose, afterStream };
}

test('a session closed with a stream open ends when that stream does', async () => {
  const got = await Promise.all([drainedAfterClose(nodeHttp2), drainedAfterClose(h2lite)]);
  // node's own measured shape: while the stream lives the connection is held open and the peer
  // sees nothing; when it goes, the session is destroyed AND the peer gets its FIN.
  const want = {
    afterClose: { closed: true, destroyed: false, peer: [] },
    afterStream: { closed: true, destroyed: true, peer: ['fin'] }
  };
  assert.deepStrictEqual(got[0], want, `node: ${JSON.stringify(got[0])}`);
  assert.deepStrictEqual(got[1], want,
    `h2lite ${JSON.stringify(got[1])} vs node ${JSON.stringify(got[0])}`);
});

// The same close-then-drain with a peer that answers nothing, and the last stream finished
// NORMALLY rather than given up on. The echoing client above sends a GOAWAY of its own back, and
// receiving one re-runs this side's closing predicate -- which is why that shape cannot show what
// happens when no answer ever arrives. Nothing is echoed here.
async function silentPeerDrain(impl) {
  const srv = impl.createServer();
  let session = null;
  let held = null;
  srv.on('session', (s) => {
    session = s;
  });
  srv.on('stream', (s) => {
    held = s; s.on('error', () => {});
  });
  srv.on('error', () => {});
  const port = await listen(srv);
  const peer = [];
  const sock = net.connect(port, '127.0.0.1');
  sock.on('error', (e) => peer.push('err:' + e.code));
  sock.on('end', () => peer.push('fin'));
  // the socket has to be READ for its 'end' (the peer's FIN) to be delivered at all: a paused
  // stream that nobody drains reports no FIN even after it arrived. The frames themselves are
  // not needed here -- the question is only whether the connection went away.
  sock.resume();
  await new Promise((r) => sock.on('connect', r));
  sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
  await sleep(120);
  // END_STREAM on the request: it is complete, so answering it closes the stream for good
  sock.write(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, REQ));
  await sleep(150);
  session.close();
  await sleep(150); // our GOAWAY is on the wire; nothing comes back
  const afterClose = { closed: session.closed, destroyed: session.destroyed, peer: [...peer] };
  held.end('body'); // the application finishes the last stream
  await sleep(400);
  const got = {
    afterClose,
    afterStream: { closed: session.closed, destroyed: session.destroyed, peer: [...peer] }
  };
  sock.destroy();
  srv.close();
  await closeServer(srv);
  return got;
}

// KNOWN DIVERGENCE, not an endorsement: this is the silent-peer case of AGENTS.md §10
// ("Graceful close: who is allowed to finish it"). The request is complete, the session is
// closed, the last stream is answered normally, and the peer says nothing back. h2lite then
// sits with nothing left to carry -- live streams 0, queued bytes 0, write callback delivered
// -- and is never let go, because the only events that re-run the closing predicate are a
// stream teardown that has already happened while bytes were still owed, and the peer's answer
// that is never coming. node destroys it within milliseconds of the answer and does so on
// every run, so this is a state the two stacks reach rather than a race;
// `tools/probe-stream-teardown.js`, case `request-ended` with `session.close()` after the
// response, prints it for both stacks.
//
// Pinned in this shape rather than as a passing parity test on purpose: the fix is the decision
// recorded in §10 (re-check the close once our own write has landed, or separate the `destroyed`
// flag from the socket teardown the way node's finishSessionClose does), and both cost event-order
// traces that agree with node today. When that decision is taken, the h2lite arm below becomes
// node's shape and the second assertion has to be rewritten to want it.
test('a closing session whose last stream answered is not let go when the peer says nothing', async () => {
  const got = await Promise.all([silentPeerDrain(nodeHttp2), silentPeerDrain(h2lite)]);
  assert.deepStrictEqual(got[0], {
    afterClose: { closed: true, destroyed: false, peer: [] },
    afterStream: { closed: true, destroyed: true, peer: ['fin'] }
  }, `node: ${JSON.stringify(got[0])}`);
  assert.deepStrictEqual(got[1], {
    afterClose: { closed: true, destroyed: false, peer: [] },
    afterStream: { closed: true, destroyed: false, peer: [] }
  }, `h2lite, as it behaves today: ${JSON.stringify(got[1])}`);
});

// One live server session, handed to `act` after a request has arrived on it. The request is
// left unanswered so the session is neither idle nor destroyed when the script runs.
async function withSession(impl, act) {
  const srv = impl.createServer();
  let session = null;
  srv.on('session', (s) => {
    session = s;
  });
  srv.on('stream', (stream) => {
    stream.on('error', () => {});
  });
  srv.on('error', () => {});
  const port = await listen(srv);
  const client = connect(port);
  try {
    const req = client.request(GET);
    req.resume();
    req.on('error', () => {});
    req.end();
    await sleep(120);
    return await act(session);
  } finally {
    client.destroy();
    srv.close();
    await sleep(60);
  }
}

test('goawayCode and goawayLastStreamID read NO_ERROR/0 before a goodbye arrives', async () => {
  // node's getters are `state.goawayCode || NGHTTP2_NO_ERROR` and `... || 0`: the state field
  // starts as null and the property does not. An application asking a live session what it heard
  // must get 0, not null -- measured on a node server session with nothing received yet.
  for (const impl of [nodeHttp2, h2lite]) {
    const name = impl === h2lite ? 'h2lite' : 'node';
    const got = await withSession(impl, (s) => [s.goawayCode, s.goawayLastStreamID]);
    assert.deepStrictEqual(got, [0, 0], `${name}: got ${got}`);
  }
});

test('close() on a destroyed session does not turn it back into a closed one', async () => {
  // Measured on a live node session: destroy() then close() leaves destroyed=true and
  // closed=FALSE. close() bails when the session is already destroyed, so a graceful close is
  // not a way to undo a destroy -- and the flag stays out of reach of the code that writes the
  // GOAWAY and marks the session closed.
  for (const impl of [nodeHttp2, h2lite]) {
    const name = impl === h2lite ? 'h2lite' : 'node';
    const got = await withSession(impl, (s) => {
      s.destroy();
      s.close();
      return [s.destroyed, s.closed];
    });
    assert.deepStrictEqual(got, [true, false], `${name}: got ${got}`);
  }
});

// The one shape in which node's `if (session.destroyed) return;` at the top of onGoawayData
// matters: a frame that arrives in the same read as the one that ended the session. Here the
// request is split across two writes and the application destroys the session from inside the
// HEADERS handler, so the GOAWAY is already sitting in the buffer behind it.
//
// h2lite does not need node's guard, because the parse loop does not get that far: _dispatch()
// answers with `this._open`, and the batch stops at the frame that destroyed the session. This
// test pins that invariant -- which is why removing it is a caught mutation and the redundant
// guard is not (an unreachable line cannot have a failing test).
test('the read batch stops at the frame that destroyed the session', async () => {
  const seen = [];
  const srv = h2lite.createServer();
  srv.on('session', (session) => {
    session.on('goaway', () => seen.push('goaway'));
    session.on('stream', (stream) => {
      seen.push('stream');
      stream.on('error', () => {});
      session.destroy(); // inside the HEADERS dispatch
    });
  });
  srv.on('error', () => {});
  const port = await listen(srv);

  const hdr = frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, REQ);
  const goaway = frame(f.GOAWAY, 0, 0, Buffer.alloc(8));
  await new Promise((resolve) => {
    const sock = net.connect(port, '127.0.0.1', () => {
      sock.setNoDelay(true);
      // one write for the opening and the first half of the request: the frame is left
      // incomplete on purpose, so that its tail is parsed out of the NEXT chunk
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0)),
        hdr.subarray(0, 12)]));
      setTimeout(() => {
        sock.cork(); // the tail and the goodbye travel as one segment
        sock.write(hdr.subarray(12));
        sock.write(goaway);
        sock.uncork();
      }, 80);
    });
    sock.on('error', () => {});
    setTimeout(resolve, 700);
  });
  srv.close();
  assert.deepStrictEqual(seen, ['stream'],
    `expected the HEADERS handler and nothing after it, got ${seen.join(',')}`);
});

// The two frames h2spec writes for its http2/7 check: a GOAWAY carrying an error code no
// version of the specification defines, and a PING behind it. The check accepts a PING ACK or a
// closed connection, and fails only on a RESET -- a peer that reads a reset out of a write it
// had in flight calls it a protocol failure.
//
// node ends the socket before it destroys it (internal/http2/core.js, finishSessionClose), so
// the peer reads a FIN. h2lite used to destroy the socket outright, which threw the goodbye
// away and closed the connection with a reset whenever the write had not reached the OS yet --
// under load that is what h2spec's http2/7 read as a failure, and the check passed on its own
// but failed in the full conformance run (out/spec-current.txt against out/spec-teardown.txt).
async function goodbyeWithUnknownCode(impl) {
  const srv = impl.createServer();
  srv.on('error', () => {});
  const port = await listen(srv);
  const seen = [];
  const payload = Buffer.alloc(8);
  payload.writeUInt32BE(255, 4); // an error code no version of the spec defines
  await new Promise((resolve) => {
    const sock = net.connect(port, '127.0.0.1', () => {
      sock.setNoDelay(true);
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
      setTimeout(() => {
        // one write, so both frames land in one read on the server
        sock.write(Buffer.concat([frame(f.GOAWAY, 0, 0, payload),
          frame(f.PING, 0, 0, Buffer.from('h2spec00', 'latin1'))]));
      }, 80);
    });
    // the socket has to be READ for the peer's FIN to be delivered at all
    sock.resume();
    // the terminal event is the end of the connection: a FIN, or the reset of one
    sock.on('end', () => {
      seen.push('fin'); resolve();
    });
    sock.on('error', (e) => {
      seen.push('err:' + e.code); resolve();
    });
    setTimeout(resolve, 800);
  });
  srv.close();
  await closeServer(srv);
  return seen;
}

test("a peer's GOAWAY with an unknown error code ends the connection with a FIN, like node", async () => {
  const got = await Promise.all([goodbyeWithUnknownCode(nodeHttp2), goodbyeWithUnknownCode(h2lite)]);
  assert.deepStrictEqual(got[0], ['fin'], `node: ${JSON.stringify(got[0])}`);
  assert.deepStrictEqual(got[1], ['fin'],
    `h2lite: ${JSON.stringify(got[1])} vs node ${JSON.stringify(got[0])}`);
});

// A fresh TCP connection to a port the server has promised to give up: true if it is accepted,
// false if it is refused, null if nothing answered in time -- the three outcomes the assertion
// can tell apart.
function tryConnect(port) {
  return new Promise((resolve) => {
    const sock = net.connect(port, '127.0.0.1');
    const finish = (v) => {
      sock.destroy(); clearTimeout(timer); resolve(v);
    };
    const timer = setTimeout(() => finish(null), 800);
    sock.on('connect', () => finish(true));
    sock.on('error', () => finish(false));
  });
}

// node's Http2Server.close() is `ReflectApply(NETServer.prototype.close, this, arguments)` and
// only then `closeAllSessions(this)` (internal/http2/core): the listening socket goes FIRST and
// the sessions are told goodbye after it, so a server that has been closed no longer answers new
// connections even while a held request is still being carried. The opposite order -- sessions
// first, the transport only once the set has emptied -- keeps accepting connections for as long
// as one unanswered stream refuses to finish, which is a promise the application has already been
// told it can rely on.
test('server.close() stops accepting connections while a busy session drains', async () => {
  const probe = async (impl) => {
    const srv = impl.createServer();
    let held = null;
    srv.on('stream', (s) => {
      held = s; s.on('error', () => {});
    });
    srv.on('error', () => {});
    const port = await listen(srv);
    const client = connect(port);
    const req = client.request(GET);
    req.resume();
    req.on('error', () => {});
    req.end();
    await sleep(150); // the server holds an unanswered request
    let calledBack = false;
    srv.close(() => {
      calledBack = true;
    });
    await sleep(150);
    const got = { acceptedAfterClose: await tryConnect(port), closeCbWhileBusy: calledBack };
    held.end('body'); // the application finishes what it was holding
    await sleep(250);
    got.closeCbAfterDrain = calledBack;
    client.destroy();
    await closeServer(srv);
    got.closeCbAfterClientGone = calledBack;
    return got;
  };
    // measured on node: the port stops answering at once, the callback does not arrive while the
    // stream is still held, and it arrives as soon as the application finishes it -- the ended
    // socket is what the listening server counts as its last connection, so the client does not
    // have to be destroyed for the close to complete.
  const want = { acceptedAfterClose: false, closeCbWhileBusy: false,
    closeCbAfterDrain: true, closeCbAfterClientGone: true };
  // node 20/22 complete the close only once the client is gone as well: there the ended
  // request socket is not counted as the last connection (measured on v20.19.5 and
  // v22.21.1 -- the callback arrives right after client.destroy()). h2lite follows the
  // newer accounting; the node arm accepts either its version produces.
  const same = (a, b) => {
    try {
      assert.deepStrictEqual(a, b);
      return true;
    } catch {
      return false;
    }
  };
  const nodeGot = await probe(nodeHttp2);
  assert.ok(same(nodeGot, want) || same(nodeGot, { ...want, closeCbAfterDrain: false }),
    `node: ${JSON.stringify(nodeGot)} matches neither the v24+ nor the v20/22 accounting`);
  assert.deepStrictEqual(await probe(h2lite), want, 'h2lite');
});
