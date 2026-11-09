// What one call of the inbound request validator costs, measured directly instead of
// through a benchmark where OS noise swamps it.
//
// The point of the numbers: the validator runs when a request *shape* is first seen, and
// its verdict is cached with the shape (lib/session.js _processHeaders), so steady-state
// traffic pays a property read. The arm that matters is therefore the one where the shape
// cache cannot help -- a client that varies a header on every request -- and that is what
// this measures: one call per request, over a field list of the size a real GET carries.
'use strict';
const { Http2Session } = require('../lib/session');

// A session object is not needed to validate fields: _requestProblem only reads
// localSettings. Building one would drag the whole frame layer in here.
const fake = { localSettings: { enableConnectProtocol: false } };
const validate = Http2Session.prototype._requestProblem;

// h2load's GET: three pseudo-headers, an authority, and the fields a browser-ish client
// adds. Names arrive interned and lower case, which is what the scan sees.
const SMALL = [':method', 'GET', ':path', '/foo', ':scheme', 'http', ':authority', 'x'];
const TYPICAL = [
  ':method', 'GET', ':path', '/foo', ':scheme', 'http', ':authority', 'localhost:8080',
  'accept', '*/*', 'user-agent', 'h2load/1.41.0', 'via', 'HTTP/2.0 client',
  'x-request-id', '0123456789abcdef0123456789abcdef'
];
const BIG = TYPICAL.concat(Array.from({ length: 32 }, (_, i) => `x-extra-${i}`)
  .reduce((a, n) => {
    a.push(n, 'v'); return a;
  }, []));

function bench(label, fields, iters) {
  // warm up, then time
  for (let i = 0; i < 20000; i++) {
    validate.call(fake, fields, false);
  }
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < iters; i++) {
    validate.call(fake, fields, false);
  }
  const ns = Number(process.hrtime.bigint() - t0) / iters;
  console.log(`${label.padEnd(28)} ${fields.length / 2 | 0} fields  ${ns.toFixed(0)} ns/request`);
  return ns;
}

const rounds = 300000;
const small = bench('h2load GET (no cache hit)', SMALL, rounds);
const typical = bench('browser-sized GET', TYPICAL, rounds);
const big = bench('40 fields', BIG, rounds);

// The steady-state path: what a shape-cache hit pays instead of the call above.
const cached = { problem: undefined };
function hitPath(fields, verdict) {
  // this is the branch in _processHeaders: take the stored verdict, check it
  const v = verdict;
  if (v !== null && v.problem !== undefined) {
    return 'refused';
  }
  return fields.length;
}
for (let i = 0; i < 200000; i++) {
  hitPath(SMALL, cached);
}
const c0 = process.hrtime.bigint();
for (let i = 0; i < rounds; i++) {
  hitPath(SMALL, cached);
}
const cachedNs = Number(process.hrtime.bigint() - c0) / rounds;
console.log('shape-cache hit (what traffic'
  + '\n  actually pays today)             ' + cachedNs.toFixed(0) + ' ns/request');
console.log('\nA client that varies one header per request costs the server the per-call'
  + ` number above on top of everything else: ${small.toFixed(0)} ns on the h2load GET,`
  + ` or ${(100 * small / 1300).toFixed(0)}% of the ~1.3 us that request already costs.`);
console.log(`Typical browser request: ${typical.toFixed(0)} ns`
  + ` (${(100 * typical / 1300).toFixed(0)}%), 40 fields: ${big.toFixed(0)} ns.`);
