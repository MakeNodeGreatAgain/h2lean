// M8: the temporal contract. The wire is already proven identical (tools/
// probe-wire.js) and so is the decoded header set; what is left is the ORDER of
// events an application observes on server / session / stream / req / res.
//
// Each scenario is run twice, once against node:http2 and once against h2lean,
// with the same recorder, and the two traces must match after removing the
// documented differences below. Every whitelisted entry needs a reason: it is a
// deliberate incompatibility, not an unknown one.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const nodeHttp2 = require('node:http2');
const h2lean = require('../index');

const SESSION_EVENTS = ['connect', 'close', 'error', 'frameError', 'goaway', 'aborted',
  'streamClosed', 'streamError', 'socketError', 'timeout', 'remoteSettings'];
const STREAM_EVENTS = ['aborted', 'close', 'error', 'end', 'data', 'wantTrailers', 'trailers'];
// The same list without 'data'. Recording 'data' attaches a listener, and attaching one is
// what starts consumption -- on h2lean that releases the flow-control credit and replays the
// buffered EOF, on node it switches the stream to flowing mode. The eof-* scenarios are about
// the shape where nobody reads the request, so observing them must not make the test a reader.
const STREAM_LIFECYCLE = ['aborted', 'close', 'error', 'end', 'wantTrailers', 'trailers'];
// 'request' and 'checkContinue' are deliberately absent: attaching them arms the
// compat layer on both stacks, which would take over the stream-API scenarios.
// The compat scenarios register the handler through createServer(fn) instead, so
// both stacks arm the same way.
const SERVER_EVENTS = ['session', 'stream', 'close', 'sessionError',
  'connectionError', 'error'];
// 'readable' is deliberately not recorded: attaching a 'readable' listener puts
// the Readable into the mode where 'end' never fires, on node and on h2lean
// alike, so observing it here would change the thing being observed.
const REQ_EVENTS = ['aborted', 'close', 'end', 'data', 'error', 'trailers'];
const RES_EVENTS = ['finish', 'close', 'error', 'drain'];

// Differences we accept, keyed by "<label>.<event>". Kept empty: the two teardown quirks the
// comparison forgives are handled in normalize() below, where they can be described precisely
// enough not to blind the rest of the trace.
const WHITELIST = new Set();

// In the compat scenarios everything after the 'stream' handoff belongs to the
// teardown of a request, and node's order there is not reproduced: see the
// difference list at the top of lib/compat.js. The hand-off order is compared
// strictly, the teardown as a set. The set is empty: req 'end' used to be excluded
// from it because a request nobody read did not report it at all -- that was the bug
// the eof-* scenarios above pin down, and it is now compared like every other event.
const TEARDOWN_IGNORED = new Set();
const TEARDOWN_CUT = 'server.stream';

// The one difference the array comparison still has to forgive, and it is about liveness rather
// than about the event itself: on the 'goaway' scenario the peer answers our GOAWAY with one of
// its own, and by then node's session is already destroyed so it never reports that echo, while
// ours is still reading (it waits for the peer's FIN) and reports what actually arrived. Only a
// repeated event is collapsed -- a stack that reported none, or invented one out of nowhere, still
// fails the comparison. Everything else, including the teardown order of close() and 'close', is
// compared strictly: the pair swap that used to be here became unnecessary when server.close()
// took node's shape (net.Server's close runs first, sessions afterwards).
//
// The same echo ends every OTHER scenario -- each of them closes the peer and the server in the
// same tick -- and there it is teardown noise, so it is dropped instead of compared: whether the
// answer is read before the session is torn down is a race on node's side too. Measured on the
// 'compat' shape (tools/probe-goaway-echo.js): node reported the echo in 40 of 40 rounds on an
// idle machine and in 26 of 40 and 29 of 30 in two loaded runs, h2lean in every round of every
// run -- node's session is destroyed when its graceful close completes, ours when the GOAWAY is
// written, and the answer lands in the window between. The 'goaway' scenario keeps the check,
// because there the event IS the subject.
function normalize(log, scenario) {
  const out = log.filter((line) => !WHITELIST.has(line));
  if (scenario !== 'goaway') {
    return out.filter((line) => line !== 'session.goaway');
  }
  return out.filter((line, i) =>
    !(line === 'session.goaway' && i > 0 && out[i - 1] === 'session.goaway'));
}

function compare(ours, native, scenario) {
  if (scenario !== 'compat' && scenario !== 'compat-post') {
    assert.deepStrictEqual(ours, native, `trace mismatch for '${scenario}'`);
    return;
  }
  const cut = (log) => {
    const i = log.lastIndexOf(TEARDOWN_CUT);
    if (i < 0) {
      return [log, []];
    }
    return [log.slice(0, i + 1),
      log.slice(i + 1).filter((line) => !TEARDOWN_IGNORED.has(line)).sort()];
  };
  const [oursHead, oursTail] = cut(ours);
  const [nativeHead, nativeTail] = cut(native);
  assert.deepStrictEqual(oursHead, nativeHead,
    `handler-visible order differs for '${scenario}'`);
  assert.deepStrictEqual(oursTail, nativeTail, `teardown events differ for '${scenario}'`);
}

function rec(log, label, emitter, names) {
  for (const name of names) {
    emitter.on(name, () => log.push(`${label}.${name}`));
  }
}

// Runs one scenario and returns the recorded event sequence.
function traceOf(impl, scenario) {
  const log = [];
  const compat = scenario === 'compat' || scenario === 'compat-post';
  const eof = scenario.startsWith('eof');
  const server = compat
    ? impl.createServer((req, res) => {
      rec(log, 'req', req, REQ_EVENTS);
      rec(log, 'res', res, RES_EVENTS);
      if (scenario === 'compat-post') {
        req.resume();
        req.on('end', () => {
          res.writeHead(200, { 'x-a': '1' }); res.end('ok');
        });
        return;
      }
      res.writeHead(200, { 'x-a': '1' });
      res.end('ok');
    })
    : impl.createServer();
  rec(log, 'server', server, SERVER_EVENTS);
  if (compat) {
    rec(log, 'server', server, ['request']);
  }
  server.on('session', (session) => rec(log, 'session', session, SESSION_EVENTS));

  if (compat) {
    return runScenario(server, log, scenario);
  }

  server.on('stream', (stream) => {
    rec(log, 'stream', stream, eof ? STREAM_LIFECYCLE : STREAM_EVENTS);
    if (eof) {
      // the handler answers without ever touching the read side
      stream.respond({ ':status': 200 }, scenario === 'eof-wait'
      // END_STREAM is already on the header block, so the trailer request is dropped and
      // nothing asks for trailers: what is checked is that 'end' still fires in that shape
        ? { waitForTrailers: true, endStream: true } : { endStream: true });
      return;
    }
    switch (scenario) {
      case 'reset':
        stream.respond({ ':status': 200 });
        stream.close(8);
        return;
      case 'trailers':
        stream.on('data', () => {});
        stream.on('end', () => {
          stream.respond({ ':status': 200 }, { waitForTrailers: true });
          stream.on('wantTrailers', () => stream.sendTrailers({ 'x-tail': 't' }));
          stream.end('body');
        });
        return;
      default:
        stream.on('data', () => {});
        stream.on('end', () => {
          stream.respond({ ':status': 200 });
          stream.end('ok');
        });
    }
  });
  return runScenario(server, log, scenario);
}

function runScenario(server, log, scenario) {
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const client = nodeHttp2.connect(`http://127.0.0.1:${server.address().port}`);
      client.on('error', () => {});
      const headers = {
        ':method': scenario === 'post' || scenario === 'compat-post' || scenario === 'eof-body'
          ? 'POST' : 'GET',
        ':path': '/x', ':scheme': 'http', ':authority': 'localhost'
      };
      // eof-* sends END_STREAM on the HEADERS frame itself, which is the shape a GET
      // request takes and the one an unread request body never reaches. The other scenarios
      // keep the call exactly as it was: passing an explicit { endStream: false } turns out
      // to change what the client writes, so the default is left alone where it is not the
      // subject of the test.
      const endStreamOnHeaders = scenario.startsWith('eof') && scenario !== 'eof-body';
      const req = endStreamOnHeaders
        ? client.request(headers, { endStream: true })
        : client.request(headers);
      req.resume();
      req.on('error', () => {});
      const body = headers[':method'] === 'POST' ? Buffer.alloc(4096, 0x61) : null;

      let collected = false;
      const collect = () => {
        if (collected) {
          return;
        }
        collected = true;
        setTimeout(() => {
          client.destroy();
          server.close();
          setTimeout(() => resolve(normalize(log, scenario)), 150);
        }, 120);
      };

      if (scenario === 'clientReset') {
        req.on('response', () => req.close(8));
        req.on('close', collect);
      } else if (scenario === 'goaway') {
        req.on('end', () => {
          client.goaway(); collect();
        });
      } else if (scenario === 'destroy') {
        req.on('response', () => {
          client.destroy(); collect();
        });
      } else {
        req.on('end', collect);
      }

      if (!endStreamOnHeaders) {
        setTimeout(() => req.end(body), 20);
      }
    });
  });
}

const SCENARIOS = ['simple', 'post', 'reset', 'clientReset', 'destroy', 'goaway',
  'trailers', 'compat', 'compat-post',
  // the read side, observed without a reader: where the request ends on its HEADERS frame,
  // and where it ends on a DATA frame the application never consumes
  'eof', 'eof-wait', 'eof-body'];

for (const scenario of SCENARIOS) {
  test(`event order matches node:http2 (${scenario})`, async () => {
    const native = await traceOf(nodeHttp2, scenario);
    const ours = await traceOf(h2lean, scenario);
    compare(ours, native, scenario);
  });
}
