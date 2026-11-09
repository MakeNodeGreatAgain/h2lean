// Which :status values the two response APIs accept, and what they answer when they do
// not (lib/headers.js parse()). node's rules are two different ones and both are pinned
// here from its source and from measurements against a live node server:
//
//   respond():  prepareResponseHeadersObject() coerces with `statusCode | 0 || 200`
//               (core.js:2700) and validatePreparedResponseHeaders() then rejects
//               anything outside 200..599 with ERR_HTTP2_STATUS_INVALID (core.js:2747).
//               The comment there is explicit that this is stricter than HTTP/1 on
//               purpose. So '200.5' is 200 and 'abc' is 200, while 1.9 becomes 1 and is
//               refused, and 600 is refused although a 3-digit number.
//   additionalHeaders():  a :status, if present, is coerced with `|= 0`; 101 gets
//               ERR_HTTP2_STATUS_101 and every other value outside 100..199 gets
//               ERR_HTTP2_INVALID_INFO_STATUS (core.js:3268-3274). There is no default
//               status and no generated date for these blocks.
//
// The compat layer (res.writeHead / res.statusCode / res.writeInformation) has its OWN
// rules that run first, so the same status can produce a different error code depending on
// the API the application used -- the compatCases table at the bottom pins that too.
//
// Before this file, h2lite accepted 100..999 in respond() and reported 101 with a code
// node only ever uses on the informational path -- so the two stacks disagreed twice over
// on the same call.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const h2lite = require('../index');
const nodeHttp2 = require('node:http2');
const { listen, connect, request } = require('./diff-harness');

// One stream-API call, and what the client ends up seeing: the error code if the API
// refused the headers, otherwise the status that arrived on the wire. The final
// respond(200) after a refused call is deliberate: it keeps every case answerable, so a
// mistake in the API shows up as a wrong verdict rather than as a hung client.
async function callStreamApi(impl, act) {
  const srv = impl.createServer();
  let thrown = null;
  srv.on('stream', (stream) => {
    stream.on('error', () => {});
    try {
      act(stream);
    } catch (e) {
      thrown = e.code;
    }
    // answer every case, so a mistake in the API surfaces as a wrong verdict and not as a
    // client that hangs until the test timeout: an informational block by itself does not
    // complete a response, and a refused call completes nothing at all
    if (!stream.headersSent) {
      try {
        stream.respond({ ':status': 200 }, { endStream: true });
      } catch { /* sent */ }
    }
  });
  srv.on('error', () => {});
  const port = await listen(srv);
  const client = connect(port);
  let status = null;
  try {
    const r = await request(client, { ':method': 'GET', ':path': '/' });
    status = r.headers[':status'];
  } catch { /* the peer refused the block: that is a verdict too */ }
  // teardown on the failure path as well: a listening server left open keeps the process
  // alive after the test has reported, which looks like a hang rather than like a result
  let outcome;
  try {
    outcome = thrown === null ? `ok ${status}` : `throw ${thrown}`;
  } finally {
    client.destroy();
    await new Promise((resolve) => srv.close(resolve));
  }
  return outcome;
}

// The expected answers, every one of them measured against node:http2 first. Anything the
// two implementations disagree on is reported by name, so a new divergence cannot hide
// behind a passing agreement.
const responseCases = [
  [200, 'ok 200'],
  [204, 'ok 204'],
  [304, 'ok 304'],
  [599, 'ok 599'],
  [600, 'throw ERR_HTTP2_STATUS_INVALID'],
  [999, 'throw ERR_HTTP2_STATUS_INVALID'],
  [103, 'throw ERR_HTTP2_STATUS_INVALID'],
  [101, 'throw ERR_HTTP2_STATUS_INVALID'],
  [0, 'ok 200'],
  [1.9, 'throw ERR_HTTP2_STATUS_INVALID'],
  ['200.5', 'ok 200'],
  [' 200 ', 'ok 200'],
  ['abc', 'ok 200']
];

for (const value of responseCases) {
  const [status, want] = value;
  test(`respond({':status':${JSON.stringify(status)}}) -> ${want}`, async () => {
    const got = [];
    for (const impl of [h2lite, nodeHttp2]) {
      got.push(await callStreamApi(impl, (s) =>
        s.respond({ ':status': status }, { endStream: true })));
    }
    assert.strictEqual(got[1], want, `node itself answered ${got[1]}`);
    assert.strictEqual(got[0], want, `h2lite answered ${got[0]}, node answered ${got[1]}`);
  });
}

const infoCases = [
  [100, 'ok 200'],
  [103, 'ok 200'],
  [199, 'ok 200'],
  [101, 'throw ERR_HTTP2_STATUS_101'],
  [200, 'throw ERR_HTTP2_INVALID_INFO_STATUS'],
  [99, 'throw ERR_HTTP2_INVALID_INFO_STATUS'],
  [600, 'throw ERR_HTTP2_INVALID_INFO_STATUS']
];

for (const [status, want] of infoCases) {
  test(`additionalHeaders({':status':${status}}) -> ${want}`, async () => {
    const got = [];
    for (const impl of [h2lite, nodeHttp2]) {
      got.push(await callStreamApi(impl, (s) =>
        s.additionalHeaders({ ':status': status, link: '</a.css>; rel=preload' })));
    }
    assert.strictEqual(got[1], want, `node itself answered ${got[1]}`);
    assert.strictEqual(got[0], want, `h2lite answered ${got[0]}, node answered ${got[1]}`);
  });
}

test('an informational block without a :status is the application\'s error, not ours', async () => {
  // node validates a :status on this path only if the application put one there
  // (`headers[HTTP2_HEADER_STATUS] != null`), so calling additionalHeaders({link: ...})
  // without a status does not throw -- and the block that reaches the wire is invalid,
  // because RFC 9113 8.1 makes :status part of *every* response, informational ones
  // included. Measured on a live node server: its own client answers such a block with
  // NGHTTP2_PROTOCOL_ERROR. What is pinned here is that both stacks make the same promise
  // to the application (no local refusal) and get the same answer from the peer.
  const got = [];
  for (const impl of [h2lite, nodeHttp2]) {
    got.push(await callStreamApi(impl, (s) => {
      s.additionalHeaders({ link: '</a.css>; rel=preload' });
      s.respond({ ':status': 200 }, { endStream: true });
    }));
  }
  // `ok null` is the measured answer on a live node server: the API call itself does not
  // throw, and the peer's reset means no status ever arrives to report -- both stacks have
  // to land in the same place, because an application cannot tell them apart from here.
  assert.strictEqual(got[1], 'ok null', `node: ${got[1]}`);
  assert.strictEqual(got[0], got[1], `h2lite ${got[0]} vs node ${got[1]}`);
});

test('a coerced status is what reaches the wire', async () => {
  // Not a throw, and not the application's string either: node writes the coerced number
  // back into the headers, so '200.5' must arrive as 200 rather than as '200.5'.
  for (const impl of [h2lite, nodeHttp2]) {
    const name = impl === h2lite ? 'h2lite' : 'node';
    const srv = impl.createServer();
    srv.on('stream', (stream) => {
      stream.on('error', () => {});
      stream.respond({ ':status': '200.5' }, { endStream: true });
    });
    srv.on('error', () => {});
    const port = await listen(srv);
    const client = connect(port);
    const r = await request(client, { ':method': 'GET', ':path': '/' });
    client.destroy();
    await new Promise((resolve) => srv.close(resolve));
    assert.strictEqual(r.headers[':status'], 200, `${name}: got ${r.headers[':status']}`);
  }
});

// ---- the compat layer ---------------------------------------------------------------
//
// node's Http2ServerResponse validates a status in its own code, before the stream API
// above ever sees it (internal/http2/compat). Two of those rules differ from the stream
// API, and one of them is why the check cannot live in the stream API alone: putting
// writeInformation(101) through additionalHeaders() gives ERR_HTTP2_STATUS_101, while node
// refuses 101 inside compat with ERR_HTTP2_STATUS_INVALID (its own `statusCode === 101`
// check). res.writeHead() writes state.statusCode directly, so it bypasses the setter and
// lands on respond()'s rule instead -- which is why writeHead(101) reports STATUS_INVALID
// and statusCode = 101 reports INFO_STATUS_NOT_ALLOWED.
//
// Every expectation is the whole observed line: what the server-side call did, plus what
// the client managed to see. A case with no answer on the wire is legitimate here, because
// a refused writeHead() leaves the response unstarted.
async function callCompat(impl, act) {
  const srv = impl.createServer();
  const seen = [];
  srv.on('request', (req, res) => {
    try {
      seen.push('sync:' + act(res));
    } catch (e) {
      seen.push('throw:' + e.code);
    }
    if (res.headersSent === false) {
      try {
        res.end('body');
      } catch (e) {
        seen.push('end:' + e.code);
      }
    }
  });
  srv.on('error', () => {});
  const port = await listen(srv);
  const client = connect(port);
  const wire = [];
  let outcome;
  try {
    const req = client.request({ ':method': 'GET', ':path': '/' });
    req.on('information', (h) => wire.push('info:' + h[':status']));
    req.on('continue', () => wire.push('continue'));
    req.on('response', (h) => wire.push('status:' + h[':status']));
    req.on('error', (e) => wire.push('clientErr:' + e.code));
    req.end();
    // consume the body: 'end' then fires as soon as the answer is in, so only a case that
    // genuinely has no answer on the wire pays for the deadline
    req.resume();
    await Promise.race([
      new Promise((resolve) => setTimeout(resolve, 400)),
      new Promise((resolve) => req.on('end', resolve))
    ]);
    outcome = `server[${seen.join(' ')}] client[${wire.join(',')}]`;
  } finally {
    client.destroy();
    srv.close();
  }
  return outcome;
}

// The four writeInformation cases carry a second expectation: node before v24 throws a
// code-less error out of writeInformation() for every status and never puts an informational
// response on the wire -- measured on v20.19.5 and v22.21.1, all four statuses identical
// ('server[throw:undefined] client[status:200]'). h2lite pins the v24+ behavior; the node
// arm accepts whichever its version produces.
const LEGACY_WRITE_INFORMATION = 'server[throw:undefined] client[status:200]';
const compatCases = [
  ['writeHead(101)', (res) => {
    res.writeHead(101);
  },
  'server[throw:ERR_HTTP2_STATUS_INVALID end:ERR_HTTP2_STATUS_INVALID] client[]'],
  ['writeHead(103)', (res) => {
    res.writeHead(103);
  },
  'server[throw:ERR_HTTP2_STATUS_INVALID end:ERR_HTTP2_STATUS_INVALID] client[]'],
  ['writeHead(600)', (res) => {
    res.writeHead(600);
  },
  'server[throw:ERR_HTTP2_STATUS_INVALID end:ERR_HTTP2_STATUS_INVALID] client[]'],
  ['writeHead(99)', (res) => {
    res.writeHead(99);
  },
  'server[throw:ERR_HTTP2_STATUS_INVALID end:ERR_HTTP2_STATUS_INVALID] client[]'],
  ['writeHead(201)', (res) => {
    res.writeHead(201);
  }, 'server[sync:undefined] client[status:201]'],
  ['statusCode=101', (res) => {
    res.statusCode = 101;
  },
  'server[throw:ERR_HTTP2_INFO_STATUS_NOT_ALLOWED] client[status:200]'],
  ['statusCode=600', (res) => {
    res.statusCode = 600;
  },
  'server[throw:ERR_HTTP2_STATUS_INVALID] client[status:200]'],
  ['writeContinue()', (res) => res.writeContinue(),
    'server[sync:true] client[continue,status:200]'],
  ['writeInformation(100)', (res) => res.writeInformation(100),
    'server[sync:true] client[continue,status:200]', LEGACY_WRITE_INFORMATION],
  ['writeInformation(101)', (res) => res.writeInformation(101),
    'server[throw:ERR_HTTP2_STATUS_INVALID] client[status:200]', LEGACY_WRITE_INFORMATION],
  ['writeInformation(103)', (res) => res.writeInformation(103),
    'server[sync:true] client[status:200]', LEGACY_WRITE_INFORMATION],
  ['writeInformation(204)', (res) => res.writeInformation(204),
    'server[throw:ERR_HTTP2_STATUS_INVALID] client[status:200]', LEGACY_WRITE_INFORMATION]
];

for (const [name, act, want, nodeWant] of compatCases) {
  test(`compat ${name} -> ${want}`, async () => {
    const got = [];
    for (const impl of [h2lite, nodeHttp2]) {
      got.push(await callCompat(impl, act));
    }
    assert.ok(got[1] === want || got[1] === nodeWant, `node itself produced ${got[1]}`);
    assert.strictEqual(got[0], want, `h2lite produced ${got[0]}, node produced ${got[1]}`);
  });
}
