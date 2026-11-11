// Field-name validation and the `date` header, checked where they live instead of
// through a server: both are pure functions of the header object, and both are now
// cached, which is the kind of thing a differential test alone does not cover.
//
// The caches are what could go wrong here. A validated-name Set speeds up the common
// case by skipping the token regex, so the risk is that it also skips the regex for a
// name that should have been rejected -- including after the Set reaches its bound.
// The date string is cached across a second and invalidated by a timer, so the risk is
// a header that freezes at the moment the server started.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const { parse } = require('../lib/headers');

// The `date` field a response would carry, or undefined when it must not carry one.
// The parsed fields are flat: name, value, never, name, value, never, ...
const sentDate = (headers, opts) => {
  const fields = parse(headers, undefined, opts).fields;
  for (let i = 0; i < fields.length; i += 3) {
    if (fields[i] === 'date') {
      return fields[i + 1];
    }
  }
  return undefined;
};

test('a field name is validated once, and rejection survives the cache', () => {
  const GOOD = ['content-type', 'x-request-id', 'etag', 'set-cookie'];
  for (const name of GOOD) {
    assert.doesNotThrow(() => parse({ ':status': 200, [name]: 'v' }));
    // second call goes through the cache instead of the regex
    assert.doesNotThrow(() => parse({ ':status': 200, [name]: 'v' }));
  }
  const BAD = ['bad name', 'x{y', 'a\nb', 'trailing ', ''];
  for (const name of BAD) {
    assert.throws(() => parse({ ':status': 200, [name]: 'v' }),
      (err) => err.code === 'ERR_INVALID_HTTP_TOKEN',
      `${JSON.stringify(name)} should not be a legal token`);
    // and again with the cache warm, so a remembered name cannot mask a new bad one
    assert.throws(() => parse({ ':status': 200, [name]: 'v' }),
      (err) => err.code === 'ERR_INVALID_HTTP_TOKEN');
  }
});

test('the name cache stays correct past its bound', () => {
  // Fill it: names are remembered only up to VALID_NAMES_MAX, so push well past that
  // and check both halves of the promise afterwards -- a new legal name is still legal,
  // a new illegal one is still rejected, and neither answer depends on room being left.
  for (let i = 0; i < 2500; i++) {
    parse({ ':status': 200, [`x-pad-${i}`]: 'v' });
  }
  assert.doesNotThrow(() => parse({ ':status': 200, 'x-after-full-cache': 'v' }));
  assert.throws(() => parse({ ':status': 200, 'bad after full cache': 'v' }),
    (err) => err.code === 'ERR_INVALID_HTTP_TOKEN');
});

test('connection-specific field names are rejected by the lookup, not by luck', () => {
  for (const name of ['connection', 'keep-alive', 'proxy-connection',
    'transfer-encoding', 'upgrade']) {
    assert.throws(() => parse({ ':status': 200, [name]: 'v' }),
      (err) => err.code === 'ERR_HTTP2_INVALID_CONNECTION_HEADERS', name);
    // looked up again from the cache path of the name check above
    assert.throws(() => parse({ ':status': 200, [name]: 'v' }),
      (err) => err.code === 'ERR_HTTP2_INVALID_CONNECTION_HEADERS', name);
  }
  // the names that merely look neighbourly must pass
  for (const name of ['content-type', 'encoding', 'x-connection']) {
    assert.doesNotThrow(() => parse({ ':status': 200, [name]: 'v' }), name);
  }
});

test('a field value may not carry a control character', () => {
  // The class is the one nghttp2 applies to an INBOUND value, measured character by
  // character against a live node server (out/probe-value-matrix.js): every control
  // character but HTAB, and DEL. The outbound check is h2lean's own and stricter than
  // node's -- node does not look at an outbound value at all and puts a CR on the wire
  // (out/probe-out-value-matrix.js) -- so this rule is the RFC's, not node's behaviour.
  // A CR is the one that matters: it is what turns one field into two lines for a
  // translator that splits on it (CVE-2020-8287's class).
  const BAD = ['a\u0000b', 'a\u0001b', 'a\u0008b', 'a\u000ab', 'a\u000bb', 'a\u000cb',
    'a\u000db', 'a\u000eb', 'a\u001fb', 'a\u007fb'];
  for (const value of BAD) {
    assert.throws(() => parse({ ':status': 200, 'x-a': value }),
      (err) => err.code === 'ERR_HTTP2_INVALID_HEADER_VALUE',
      `${JSON.stringify(value)} should not be a legal field value`);
  }
  // and the ones that may be there
  for (const value of ['a\tb', 'a b', 'a~b', 'a\u0080b', 'a\u00ffb']) {
    assert.doesNotThrow(() => parse({ ':status': 200, 'x-a': value }),
      JSON.stringify(value));
  }
});

test('the date header holds within a second and moves across the boundary', (t) => {
  // Both the clock and the invalidating timer are mocked, so this tests the arithmetic
  // of the refresh -- 1000 - getMilliseconds() -- rather than waiting on a real second.
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'] });
  t.mock.timers.setTime(Date.parse('2026-11-01T12:00:00.400Z'));

  const first = sentDate({ ':status': 200 }, { sendDate: true });
  assert.strictEqual(first, 'Sun, 01 Nov 2026 12:00:00 GMT');
  assert.strictEqual(sentDate({ ':status': 200 }, { sendDate: true }), first,
    'the same second must produce the identical string, not a rebuilt one');

  // still inside the second: nothing to invalidate yet
  t.mock.timers.setTime(Date.parse('2026-11-01T12:00:00.900Z'));
  assert.strictEqual(sentDate({ ':status': 200 }, { sendDate: true }), first);

  // past 12:00:01.000 the timer has fired, so the next call rebuilds the string
  t.mock.timers.tick(200);
  assert.strictEqual(sentDate({ ':status': 200 }, { sendDate: true }),
    'Sun, 01 Nov 2026 12:00:01 GMT');
  t.mock.timers.tick(1000);
  assert.strictEqual(sentDate({ ':status': 200 }, { sendDate: true }),
    'Sun, 01 Nov 2026 12:00:02 GMT');

  // and the switches that must keep it absent
  assert.strictEqual(sentDate({ ':status': 200 }, { sendDate: false }), undefined,
    'sendDate off sends no date');
  assert.strictEqual(sentDate({ ':status': 200, date: 'Mon, 02 Nov 2026 00:00:00 GMT' },
    { sendDate: true }), 'Mon, 02 Nov 2026 00:00:00 GMT',
  "the application's own date wins");
  assert.strictEqual(sentDate({ 'x-trailer': 'v' }, { trailer: true, sendDate: true }),
    undefined, 'a trailer block carries no date');
});
