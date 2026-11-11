// How do you make a null-prototype object that is ALSO in fast-properties mode,
// and what does it cost to make one?
//
//   node --allow-natives-syntax bench/micro/nullproto-alloc.js
//
// The shape matters: V8 puts any object whose prototype is null into
// dictionary mode, because a null prototype means "__proto__" is an ordinary
// field and the map can be poisoned. Taking the prototype away AFTER the
// object is built keeps the fast-properties layout -- but Object.setPrototypeOf
// is a runtime call and costs ~59 ns.
//
// A constructor whose prototype is null is the other way: the initial map of
// the constructor already carries the null prototype, so `new` is a plain
// allocation and the object stays fast.
const N = 1000000;

function bench(name, fn) {
  for (let i = 0; i < 50000; i++) fn();
  globalThis.sink = null;
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < N; i++) fn();
  const t1 = process.hrtime.bigint();
  const ns = Number(t1 - t0) / N;
  console.log(`${name.padEnd(46)} ${ns.toFixed(1)} ns`);
  return ns;
}

// --- the three ways to make an empty null-prototype object ---
const literal = bench('{ __proto__: null }', () => ({ __proto__: null }));
const stripped = bench('Object.setPrototypeOf({}, null)', () => Object.setPrototypeOf({}, null));
const created = bench('Object.create(null)', () => Object.create(null));

function NullMap() {}
NullMap.prototype = null;
const ctor = bench('new NullMap()', () => new NullMap());

// --- and their modes ---
const a = { __proto__: null };
const b = Object.setPrototypeOf({}, null);
const c = Object.create(null);
const d = new NullMap();
console.log('\nthe modes');
console.log('literal            ', %HasFastProperties(a) ? 'fast' : 'dictionary');
console.log('setPrototypeOf     ', %HasFastProperties(b) ? 'fast' : 'dictionary');
console.log('Object.create(null)', %HasFastProperties(c) ? 'fast' : 'dictionary');
console.log('new NullMap()      ', %HasFastProperties(d) ? 'fast' : 'dictionary');

// --- and the mode after the stores a header map gets ---
function fill(o, n) {
  for (let i = 0; i < n; i++) o['x-field-' + i] = 'value-' + i;
  return o;
}
fill(b, 6);
fill(d, 6);
console.log('\nafter six stores');
console.log('setPrototypeOf     ', %HasFastProperties(b) ? 'fast' : 'dictionary');
console.log('new NullMap()      ', %HasFastProperties(d) ? 'fast' : 'dictionary');
console.log('prototype of new   ', Object.getPrototypeOf(d));

// --- a "new" object that keeps the null prototype after the stores ---
console.log('"__proto__" store  ', (() => {
  const o = new NullMap();
  o['__proto__'] = 'x';
  return Object.prototype.hasOwnProperty.call(o, '__proto__') && Object.getPrototypeOf(o) === null;
})());

// --- and the cost of the whole map, built the way the compat layer builds it ---
console.log('\nthe whole map (four fields, dynamic keys)');
function buildLiteral() {
  const o = { __proto__: null };
  o['content-type'] = 'text/plain';
  o.server = 'h2lean';
  o['x-trace'] = '0123456789abcdef';
  o[':status'] = 200;
  globalThis.sink = o;
}
function buildStripped() {
  const o = Object.setPrototypeOf({}, null);
  o['content-type'] = 'text/plain';
  o.server = 'h2lean';
  o['x-trace'] = '0123456789abcdef';
  o[':status'] = 200;
  globalThis.sink = o;
}
function buildCtor() {
  const o = new NullMap();
  o['content-type'] = 'text/plain';
  o.server = 'h2lean';
  o['x-trace'] = '0123456789abcdef';
  o[':status'] = 200;
  globalThis.sink = o;
}
const mLit = bench('literal', buildLiteral);
const mStr = bench('setPrototypeOf', buildStripped);
const mCtor = bench('new NullMap()', buildCtor);

// --- and the read back ---
console.log('\nthe same map, Object.keys() and a keyed read per field');
function readLit() {
  const o = { __proto__: null };
  o['content-type'] = 'text/plain'; o.server = 'h2lean';
  o['x-trace'] = '0123456789abcdef'; o[':status'] = 200;
  const keys = Object.keys(o);
  let n = 0;
  for (let i = 0; i < keys.length; i++) n += o[keys[i]].length;
  globalThis.sink = o;
}
function readStr() {
  const o = Object.setPrototypeOf({}, null);
  o['content-type'] = 'text/plain'; o.server = 'h2lean';
  o['x-trace'] = '0123456789abcdef'; o[':status'] = 200;
  const keys = Object.keys(o);
  let n = 0;
  for (let i = 0; i < keys.length; i++) n += o[keys[i]].length;
  globalThis.sink = o;
}
function readCtor() {
  const o = new NullMap();
  o['content-type'] = 'text/plain'; o.server = 'h2lean';
  o['x-trace'] = '0123456789abcdef'; o[':status'] = 200;
  const keys = Object.keys(o);
  let n = 0;
  for (let i = 0; i < keys.length; i++) n += o[keys[i]].length;
  globalThis.sink = o;
}
const rLit = bench('literal', readLit);
const rStr = bench('setPrototypeOf', readStr);
const rCtor = bench('new NullMap()', readCtor);

console.log(`\nthe empty object: literal ${literal.toFixed(1)} ns, ` +
  `setPrototypeOf ${stripped.toFixed(1)} ns, Object.create ${created.toFixed(1)} ns, ` +
  `new NullMap ${ctor.toFixed(1)} ns`);
console.log(`the map:           literal ${mLit.toFixed(1)} ns, ` +
  `setPrototypeOf ${mStr.toFixed(1)} ns, new NullMap ${mCtor.toFixed(1)} ns`);
console.log(`read back:         literal ${rLit.toFixed(1)} ns, ` +
  `setPrototypeOf ${rStr.toFixed(1)} ns, new NullMap ${rCtor.toFixed(1)} ns`);