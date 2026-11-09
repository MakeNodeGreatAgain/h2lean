// Cost of the API obligations a drop-in node:http2 clone must keep.
// Measures the exact functions the CPU profile flagged, in isolation.
//   node --expose-internals bench/api-cost.js
const { Duplex } = require('node:stream');
const { EventEmitter } = require('node:events');
const util = require('node:internal/http2/util');
const asyncHooks = require('node:async_hooks');

const { buildNgHeaderString, toHeaderObject } = util;

const RESP_HEADERS = Object.freeze({
  ':status': '200',
  'content-type': 'text/plain',
  server: 'prof',
  'x-trace': '0123456789abcdef0123456789abcdef'
});
// what the C++ layer hands JS after HPACK decode: flat array of Latin1 strings
const FLAT_REQ = [
  ':method', 'GET', ':path', '/abc/def/ghij?x=1', ':scheme', 'http',
  'accept', '*/*', 'user-agent', 'probe/1.0'
];

// the shape-cache alternative: same frozen header object, reused across requests
const CACHE = new Map([['8286835c0123456789abcdef', Object.freeze({
  ':method': 'GET', ':path': '/abc/def/ghij?x=1', ':scheme': 'http',
  accept: '*/*', 'user-agent': 'probe/1.0'
})]]);

function bench(label, fn, n = 200_000) {
  for (let i = 0; i < 20_000; i++) {
    fn(i);
  } // warm up / let V8 tier up
  globalThis.sink = null;
  const t0 = Number(process.hrtime.bigint());
  for (let i = 0; i < n; i++) {
    fn(i);
  }
  const ns = (Number(process.hrtime.bigint()) - t0) / n;
  console.log(`${ns.toFixed(0).padStart(7)} ns/op   ${label}`);
  return ns;
}

const noErr = () => undefined;
console.log('--- header marshalling (the 31%-of-busy-CPU line in the profile) ---');
const a = bench('buildNgHeaderString(respObj)  [send path]', () => {
  globalThis.sink = buildNgHeaderString(RESP_HEADERS, noErr, false);
});
const b = bench('toHeaderObject(flatArray)     [recv path]', () => {
  globalThis.sink = toHeaderObject(FLAT_REQ, undefined);
});
bench('shape-cache hit, returns FROZEN object (not API-identical!)', () => {
  globalThis.sink = CACHE.get('8286835c0123456789abcdef');
});
// node hands out a fresh MUTABLE null-proto object per request (user code does
// `delete req.headers.cookie`), so a frozen cache entry is a behaviour change.
// These are the API-identical ways to reuse a decoded shape:
const d2 = bench('shape-cache + Object.assign(nullProto, cached) [fresh, mutable]', () => {
  globalThis.sink = Object.assign(Object.create(null), CACHE.get('8286835c0123456789abcdef'));
});
const e2 = bench('shape-cache + spread into null-proto literal', () => {
  const src = CACHE.get('8286835c0123456789abcdef');
  globalThis.sink = { __proto__: null, ...src };
});
const g = bench('hand-built fresh object from cached flat k/v array', () => {
  const o = { __proto__: null };
  o[':method'] = 'GET'; o[':path'] = '/abc/def/ghij?x=1'; o[':scheme'] = 'http';
  o.accept = '*/*'; o['user-agent'] = 'probe/1.0';
  globalThis.sink = o;
});

console.log('\n--- per-stream object graph ---');
class LiteStream extends EventEmitter {
  constructor(id) {
    super(); this.id = id; this.headers = null; this.sentHeaders = null;
  }
  respond(h) {
    this.sentHeaders = h;
  }
  end() {
    this.emit('end'); this.emit('close');
  }
}
const d = bench('LiteStream: EventEmitter + 3 listeners + 3 emits', () => {
  const s = new LiteStream(1);
  s.on('error', noop); s.on('close', noop); s.on('end', noop);
  s.respond(RESP_HEADERS);
  s.end();
});
const e = bench('new Duplex({}) + 2 listeners            [node stream machinery]', () => {
  const s = new Duplex({ read() {}, write(c, _e, cb) {
    cb();
  } });
  s.on('error', noop); s.on('close', noop);
});
bench('new Duplex + AsyncResource (async_hooks)', () => {
  const s = new Duplex({ read() {}, write(c, _e, cb) {
    cb();
  } });
  const r = new asyncHooks.AsyncResource('HTTP2STREAM');
  s.on('error', noop);
  globalThis.sink = r;
});

console.log('\n--- projection for one request on a drop-in pure-JS server ---');
const parserAndSocket = 770; // measured: lite-h2.js CPU/req at body=0, no API
console.log(`   lite parser+socket (measured) ......... ${parserAndSocket} ns`);
console.log(`   + recv headers: node toHeaderObject ... ${b.toFixed(0)} ns`);
console.log(`   + recv headers: cached shape, fresh    ${d2.toFixed(0)} ns  <- API-identical`);
console.log(`   + send headers: buildNgHeaderString ... ${a.toFixed(0)} ns`);
console.log('   + send headers: precomputed HPACK bytes ~0 ns    <- API-identical');
console.log(`   + stream object graph (EventEmitter) .. ${d.toFixed(0)} ns`);
console.log(`   + stream object graph (real Duplex) ... ${e.toFixed(0)} ns`);
console.log(`   => drop-in pure JS, cached shapes ..... ~${(parserAndSocket + d2 + d).toFixed(0)} ns/req`);
console.log(`   => drop-in pure JS, no cache .......... ~${(parserAndSocket + b + d).toFixed(0)} ns/req`);
console.log('   native node:http2 (measured) .......... 7700 ns/req');
console.log(`   hand-built fresh object (alt) ......... ${g.toFixed(0)} ns`);
console.log(`   spread into null-proto (alt) .......... ${e2.toFixed(0)} ns`);

function noop() {}
