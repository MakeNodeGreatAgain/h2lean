// Which construction is the cheapest way to hand out a *fresh mutable* header
// object that looks exactly like node's req.headers?
// The first measurement showed that cloning an object built by dynamic key
// insertion is slower than building from scratch -- because that object is in
// V8 dictionary mode. This finds the fast path (and tests code generation).
//   node --expose-internals bench/headers-cost.js
const util = require('node:internal/http2/util');
const { toHeaderObject } = util;

const FLAT = [
  ':method', 'GET', ':path', '/abc/def/ghij?x=1', ':scheme', 'http',
  ':authority', '127.0.0.1:53910', 'user-agent', 'probe/1.0',
  'accept', '*/*', 'x-trace', '0123456789abcdef0123456789abcdef',
  'cookie', 'a=1; b=2; c=3'
];

function time(label, fn, n = 300_000) {
  for (let i = 0; i < 50_000; i++) {
    fn();
  }
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < n; i++) {
    fn();
  }
  const ns = Number(process.hrtime.bigint() - t0) / n;
  console.log(`${ns.toFixed(0).padStart(6)} ns/op  ${label}`);
  return ns;
}

// A: dictionary-mode cache (what lib/hpack's decode loop naturally produces)
const dictShape = (() => {
  const o = { __proto__: null };
  for (let i = 0; i < FLAT.length; i += 2) {
    o[FLAT[i]] = FLAT[i + 1];
  }
  return Object.freeze(o);
})();

// B: literal-mode cache, same keys, written out
const litShape = Object.freeze({
  __proto__: null,
  ':method': 'GET', ':path': '/abc/def/ghij?x=1', ':scheme': 'http',
  ':authority': '127.0.0.1:53910', 'user-agent': 'probe/1.0', accept: '*/*',
  'x-trace': '0123456789abcdef0123456789abcdef', cookie: 'a=1; b=2; c=3'
});

// C: code-generated builder, one per observed shape
const makeBuilder = (keys) => {
  const src = `return { __proto__: null, ${keys.map((k, i) =>
    `${JSON.stringify(k)}: v[${i}]`).join(', ')} };`;
  return new Function('v', src);
};
const builder = makeBuilder(Object.keys(dictShape));
const values = Object.values(dictShape);

console.log('--- baseline ---');
time('node toHeaderObject(flat)                 ', () => {
  globalThis.h = toHeaderObject(FLAT, undefined);
});
time('clone dictionary-mode cache               ', () => {
  globalThis.h = Object.assign(Object.create(null), dictShape);
});
time('clone literal-mode cache (Object.assign)  ', () => {
  globalThis.h = Object.assign(Object.create(null), litShape);
});
time('clone literal-mode cache (spread)         ', () => {
  globalThis.h = { __proto__: null, ...litShape };
});
time('code-generated builder from value array   ', () => {
  globalThis.h = builder(values);
});

console.log('\n--- hidden class sanity: is the result fast-mode? ---');
for (const [label, o] of [
  ['toHeaderObject', toHeaderObject(FLAT, undefined)],
  ['dict clone', Object.assign(Object.create(null), dictShape)],
  ['generated', builder(values)]
]) {
  // fast-mode objects iterate keys in insertion order via a hidden class; the
  // cheapest observable proxy is repeated property access after 10k warmups
  let acc = 0;
  for (let i = 0; i < 100_000; i++) {
    acc += o.cookie.length;
  }
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < 300_000; i++) {
    acc += o['cookie'].length;
  }
  console.log(`${(Number(process.hrtime.bigint() - t0) / 300_000).toFixed(1).padStart(6)} ns    key access on ${label} (acc=${acc & 7})`);
}

console.log('\n--- and the full request path: lookup + construction ---');
const BLOCK_KEYS = new Map([['someblock', values]]);
time('Map.get(blockKey) + generated builder     ', () => {
  globalThis.h = builder(BLOCK_KEYS.get('someblock'));
});

console.log('\n--- request shape lookup key cost (bytes -> Map) ---');
const block = Buffer.from([0x82, 0x86, 0x83, 0x89, 0x10, 0x0a, 0x61, 0x2d, 0x31, 0x1f]);
time('latin1Slice(block) + Map.get              ', () => {
  globalThis.h = BLOCK_KEYS.get(block.latin1Slice(0, block.length));
});
time('latin1Slice(block) only                   ', () => {
  globalThis.h = block.latin1Slice(0, block.length);
});

console.log('\n--- response path: cache the whole encoded block by object identity ---');
const RESP_HEADERS = Object.freeze({
  ':status': '200', 'content-type': 'text/plain', server: 'h2lean',
  'x-trace': '0123456789abcdef0123456789abcdef'
});
const BYTES = Buffer.from([0x88, 0x10, 0x0a, ...Buffer.from('text/plain'), 0x00, 0x06,
  ...Buffer.from('server'), 0x06, ...Buffer.from('h2lean')]);
const WM = new WeakMap([[RESP_HEADERS, BYTES]]);
const outBuf = Buffer.allocUnsafe(1024);
time('WeakMap.get(userHeadersObj) + memcpy      ', () => {
  const b = WM.get(RESP_HEADERS);
  globalThis.off = b.copy(outBuf, 0);
});
time('same, and also store sentHeaders ref only ', () => {
  const b = WM.get(RESP_HEADERS);
  globalThis.off = b.copy(outBuf, 0);
  globalThis.sent = RESP_HEADERS;
});
