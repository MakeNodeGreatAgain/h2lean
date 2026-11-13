'use strict';
// Cost of the strict-single-value check inside parse() (lib/headers.js): the
// validation that runs over every non-frozen respond()/additionalHeaders()
// header list, because a non-frozen object cannot come from the block cache.
// The bench calls parse() directly -- block() adds HPACK encode noise that
// would bury the check.
//   node bench/micro/parse-singles.js
const { parse } = require('../../lib/headers');

// Eight fields, two of them single-value (content-type, date) -- the same mix
// the client-side bench (bench/micro/req-singles.js) measures on request().
const RESP_HEADERS = {
  ':status': '200',
  'content-type': 'application/json',
  date: 'Fri, 13 Nov 2026 00:00:00 GMT',
  server: 'h2lean',
  'x-trace': '0123456789abcdef',
  'cache-control': 'no-cache',
  'x-a': '1',
  'x-b': '2'
};

function bench(label, fn, n = 500_000) {
  for (let i = 0; i < 50_000; i++) {
    fn();
  } // warm up / let V8 tier up
  globalThis.sink = null;
  const t0 = Number(process.hrtime.bigint());
  for (let i = 0; i < n; i++) {
    fn();
  }
  const ns = (Number(process.hrtime.bigint()) - t0) / n;
  console.log(`${ns.toFixed(0).padStart(7)} ns/op   ${label}`);
  return ns;
}

bench('parse(), strict on (default)', () => {
  globalThis.sink = parse(RESP_HEADERS, undefined, { sendDate: false });
});

bench('parse(), strict off', () => {
  globalThis.sink = parse(RESP_HEADERS, undefined,
    { sendDate: false, strict: false });
});

// The sensitive path: neverList is built only here, so this arm is the one the
// neverList construction can move. A fresh array per call is what a caller that
// passes options.sensitive per response does.
const SENSITIVE = ['authorization', 'set-cookie', 'x-secret'];
bench('parse(), sensitive (3 names, fresh array)', () => {
  globalThis.sink = parse(RESP_HEADERS, ['authorization', 'set-cookie', 'x-secret'],
    { sendDate: false });
});
bench('parse(), sensitive (same array identity)', () => {
  globalThis.sink = parse(RESP_HEADERS, SENSITIVE, { sendDate: false });
});
