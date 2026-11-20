// Port of node's `test/parallel/test-http2-sent-headers.js` (CONTRIBUTING.md §1: the
// upstream file is a scenario source, node's measured value is the expectation).
//
// The scenario reads the three snapshots a stream publishes about what it has already put
// on the wire, and the point of it is that they are not the caller's objects:
//   1. `sentInfoHeaders` after `additionalHeaders()` -- the informational block as sent.
//   2. `sentHeaders` after `respond()` -- including the two fields the stack fills in
//      itself, `:status` (a number, even when respond() was given none) and `date`, and
//      laid out as a null-prototype map. On the client the same getter resolves the four
//      pseudo-headers `request()` added to what the application passed.
//   3. `sentTrailers` after `sendTrailers()` -- a null-prototype copy of the trailer map,
//      readable once the stream has closed.
// Upstream also pins the second argument of the client's informational 'headers' event:
// the raw HEADERS frame flags, a number.
//
// Adaptations: `common.mustCall()` becomes recorded transcript lines; the assertions run
// synchronously where upstream runs them (right after the send call, and in 'close'), so a
// snapshot that is only filled in later is a difference the transcript shows. The port is
// same-stack on both halves -- node's client against node's server, h2lean's against
// h2lean's -- because the client getter is part of what is being compared. `:authority` is
// recorded as a host:port predicate rather than a value, since each arm binds its own port.
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const h2lean = require('../../index');
const { listen, closeServer } = require('../diff-harness');

function kind(v) {
  if (v === undefined) {
    return 'undefined';
  }
  return Object.getPrototypeOf(v) === null ? 'null-proto' : 'obj';
}

// A snapshot that never arrives leaves the stream open, so each arm is bounded and a hang
// is reported as a line rather than as a cancelled file.
function withTimeout(promise, label) {
  let timer;
  const guard = new Promise((resolve) => {
    timer = setTimeout(() => resolve([['HANG', label]]), 5000);
    timer.unref();
  });
  return Promise.race([promise, guard]).then((r) => {
    clearTimeout(timer);
    return r;
  });
}

function arm(lib) {
  return withTimeout(new Promise((resolve) => {
    const t = [];
    const srv = lib.createServer();
    srv.on('stream', (stream) => {
      stream.on('error', () => {});
      t.push(['srv-before-respond', kind(stream.sentHeaders)]);
      stream.additionalHeaders({ ':status': 102 });
      t.push(['srv-info-status', stream.sentInfoHeaders[0][':status']]);
      stream.respond({ abc: 'xyz' }, { waitForTrailers: true });
      const sent = stream.sentHeaders;
      t.push(['srv-sent-proto', kind(sent)]);
      t.push(['srv-sent-keys', Object.keys(sent).join(',')]);
      t.push(['srv-abc', sent.abc]);
      t.push(['srv-status', sent[':status']]);
      t.push(['srv-status-type', typeof sent[':status']]);
      t.push(['srv-date-type', typeof sent.date]);
      t.push(['srv-sent-stable', stream.sentHeaders === sent]);
      stream.on('wantTrailers', () => {
        stream.sendTrailers({ xyz: 'abc' });
      });
      stream.end();
      stream.on('close', () => {
        t.push(['srv-close', stream.rstCode]);
        t.push(['srv-trailers-proto', kind(stream.sentTrailers)]);
        t.push(['srv-trailer-xyz', stream.sentTrailers.xyz]);
      });
    });
    listen(srv).then((port) => {
      const client = lib.connect(`http://127.0.0.1:${port}`);
      client.on('error', () => {});
      const req = client.request();
      req.on('error', () => {});
      const sent = req.sentHeaders;
      t.push(['cli-sent-proto', kind(sent)]);
      t.push(['cli-sent-keys', Object.keys(sent).join(',')]);
      t.push(['cli-method', sent[':method']]);
      t.push(['cli-scheme', sent[':scheme']]);
      t.push(['cli-path', sent[':path']]);
      t.push(['cli-authority-is-hostport', sent[':authority'] === `127.0.0.1:${port}`]);
      t.push(['cli-before-trailers', kind(req.sentTrailers)]);
      req.on('headers', (headers, flags) => {
        t.push(['cli-info-status', headers[':status']]);
        t.push(['cli-flags-type', typeof flags]);
      });
      req.resume();
      req.on('close', () => {
        t.push(['cli-close', req.rstCode]);
        client.destroy();
        closeServer(srv).then(() => resolve(t));
      });
    });
  }), 'sent-headers');
}

function lineSet(t) {
  return t.map(([k, v]) => `${k} ${v}`).sort();
}

// node's measured values, so a change in either stack has to be a decision.
function expectedLines() {
  return lineSet([
    ['srv-before-respond', 'undefined'],
    ['srv-info-status', 102],
    ['srv-sent-proto', 'null-proto'],
    ['srv-sent-keys', 'abc,:status,date'],
    ['srv-abc', 'xyz'],
    ['srv-status', 200],
    ['srv-status-type', 'number'],
    ['srv-date-type', 'string'],
    ['srv-sent-stable', true],
    ['srv-close', 0],
    ['srv-trailers-proto', 'null-proto'],
    ['srv-trailer-xyz', 'abc'],
    ['cli-sent-proto', 'null-proto'],
    ['cli-sent-keys', ':method,:authority,:scheme,:path'],
    ['cli-method', 'GET'],
    ['cli-scheme', 'http'],
    ['cli-path', '/'],
    ['cli-authority-is-hostport', true],
    ['cli-before-trailers', 'undefined'],
    ['cli-info-status', 102],
    ['cli-flags-type', 'number'],
    ['cli-close', 0]
  ]);
}

test('upstream sent-headers: a stream publishes what it actually sent', async () => {
  const native = await arm(http2);
  const ours = await arm(h2lean);
  assert.deepStrictEqual(lineSet(native), expectedLines());
  assert.deepStrictEqual(lineSet(ours), lineSet(native));
});
