// Port of node's `test/parallel/test-http2-altsvc.js` (RFC 7807 ALTSVC) to the differential
// shape used here (CONTRIBUTING.md §1 — the upstream file is a scenario source, and node's
// measured value is the expectation, never h2lean's own behaviour).
//
// Upstream does two things in one file, and both are reproduced:
//   1. the argument matrix of `session.altsvc(alt, originOrStream)`. The origin may be given
//      as a string, as a URL, or as an object with an `origin` property — for the first two
//      node guarantees an ASCII-serialised origin, for the third it takes the user's word.
//      Everything else is a rejection, and upstream asserts the *message* as well as the
//      code, so the port records `name/code/message` for every call and compares the whole
//      string. node's wording is `determineSpecificType`/`addNumericalSeparator` from
//      internal/errors.js, which is why lib/session.js carries ports of both.
//   2. what the client receives: four `'altsvc'` events — the three session-level calls above
//      arrive with the serialised origin on stream 0, the call against a live stream id
//      arrives with an empty origin on that stream.
//
// One upstream-shaped call is deliberately absent: `altsvc(alt)` with neither an origin nor a
// stream id. node aborts there in C++ (a `CHECK` in node_http2.cc) and a probe that survived
// it would have to fork, so AGENTS.md §10 records h2lean's documented
// ERR_HTTP2_ALTSVC_INVALID_ORIGIN for that one degenerate call instead. `altsvc(alt, 3)` —
// accepted, naming a stream that does not exist, sending nothing — *is* reproduced, because
// the absence of an event is observable.
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const h2lean = require('../../index');

const ALT = 'h2=":8000"';

// Upstream's matrix, in upstream's order. The three origin forms come first, then the
// accepted-but-silent stream id, then each rejection class.
const CALLS = [
  [ALT, 'https://example.org:8111/this'],
  [ALT, new URL('https://example.org:8111/this')],
  [ALT, { origin: 'https://example.org:8111' }],
  [ALT, 3],
  // a numeric stream id out of the valid range
  ...[0, -1, 1.1, 0xFFFFFFFF + 1, Infinity, -Infinity].map((input) => [ALT, input]),
  // the first argument must be a string
  ...[0, {}, [], null, Infinity].map((input) => [input, undefined]),
  // characters that may not appear in the alt field-value: a control octet, a fullwidth
  // commercial at, and a non-BMP code point
  [String.fromCharCode(1), undefined],
  [`h2="${String.fromCharCode(0xFF20)}"`, undefined],
  [String.fromCodePoint(0x1F440), undefined],
  // an originOrStream that is neither an origin form nor a stream id
  ...[{}, [], true].map((input) => ['clear', input]),
  // an origin that is not an origin at all
  ['h2=":8000', 'abc:'],
  ['h2=":8000', new URL('abc:')],
  ['h2=":8000', { origin: 'null' }],
  ['h2=":8000', { origin: '' }],
  // arguments plus origin beyond the frame budget
  [ALT, `http://example.${'a'.repeat(17000)}.org:8000`]
];

// node's exact wording for the rejection classes, measured off the live binary in the same
// run as the comparison below and pinned here so the comparison cannot silently drift.
const OUT_OF_RANGE = (input) =>
  'The value of "originOrStream" is out of range. ' +
  `It must be > 0 && < 4294967296. Received ${input}`;

function record(lib) {
  return new Promise((resolve) => {
    const out = { throws: [], events: [] };
    const server = lib.createServer();
    server.on('session', (session) => {
      out.throws = CALLS.map((args) => {
        try {
          session.altsvc(...args);
          return 'ok';
        } catch (e) {
          return `${e.constructor.name}/${e.code}/${e.message}`;
        }
      });
    });
    server.on('stream', (stream) => {
      stream.session.altsvc(ALT, stream.id);
      stream.respond();
      stream.end('ok');
    });
    server.on('error', () => {});
    server.listen(0, '127.0.0.1', () => {
      const port = server.address().port;
      const client = http2.connect(`http://127.0.0.1:${port}`);
      client.on('error', () => {});
      client.on('altsvc', (alt, origin, sid) => out.events.push([alt, origin, sid]));
      const req = client.request();
      req.resume();
      req.on('close', () => {
        setTimeout(() => {
          client.destroy();
          server.close();
          resolve(out);
        }, 150);
      });
      setTimeout(() => {
        client.destroy(); server.close(); resolve(out);
      }, 5000);
    });
  });
}

test('upstream altsvc: the argument matrix rejects with node\'s exact strings', async () => {
  const a = await record(http2);
  const b = await record(h2lean);
  assert.deepStrictEqual(b.throws, a.throws,
    'h2lean must reject each altsvc() call exactly as node does');
  // Anchors read off node, one per rejection class, so a shared drift in both arms of the
  // comparison still shows up as a failure here.
  assert.strictEqual(a.throws[4], `RangeError/ERR_OUT_OF_RANGE/${OUT_OF_RANGE(0)}`);
  assert.strictEqual(a.throws[7], `RangeError/ERR_OUT_OF_RANGE/${OUT_OF_RANGE(4294967296)}`);
  assert.strictEqual(a.throws[10], 'TypeError/ERR_INVALID_ARG_TYPE/'
    + 'The "alt" argument must be of type string. Received type number (0)');
  assert.strictEqual(a.throws[12], 'TypeError/ERR_INVALID_ARG_TYPE/'
    + 'The "alt" argument must be of type string. Received an instance of Array');
  assert.strictEqual(a.throws[13], 'TypeError/ERR_INVALID_ARG_TYPE/'
    + 'The "alt" argument must be of type string. Received null');
  assert.strictEqual(a.throws[15], 'TypeError/ERR_INVALID_CHAR/Invalid character in alt');
  assert.strictEqual(a.throws[18], 'TypeError/ERR_INVALID_ARG_TYPE/'
    + 'The "originOrStream" argument must be one of type string or number '
    + 'or an instance of URL or Object. Received an instance of Object');
  assert.strictEqual(a.throws[22], 'TypeError/ERR_HTTP2_ALTSVC_INVALID_ORIGIN/'
    + 'HTTP/2 ALTSVC frames require a valid origin');
  assert.strictEqual(a.throws[CALLS.length - 1], 'TypeError/ERR_HTTP2_ALTSVC_LENGTH/'
    + 'HTTP/2 ALTSVC frames are limited to 16382 bytes');
});

test('upstream altsvc: the client sees four events, three on stream 0 and one on the stream',
  async () => {
    const a = await record(http2);
    const b = await record(h2lean);
    assert.deepStrictEqual(b.events, a.events,
      'h2lean must put the same ALTSVC frames on the wire as node');
    // Upstream's shape: the three origin forms serialise to one origin string on stream 0,
    // the numeric call carries no origin and names the live stream, and altsvc(alt, 3) —
    // a stream that does not exist — contributes no event at all.
    assert.deepStrictEqual(a.events, [
      [ALT, 'https://example.org:8111', 0],
      [ALT, 'https://example.org:8111', 0],
      [ALT, 'https://example.org:8111', 0],
      [ALT, '', 1]
    ]);
  });
