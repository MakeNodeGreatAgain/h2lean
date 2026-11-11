// Are the two shapes really interchangeable?
//
//   node bench/micro/nullproto-semantics.js
//
// The change swaps { __proto__: null } for Object.setPrototypeOf({}, null) so
// that the map stays in V8's fast-properties mode. Everything the two shapes
// differ on has to be checked, because the map carries untrusted field names:
// a field named "__proto__" must stay an ordinary field (prototype pollution),
// the enumeration order must not change (it is visible on the wire), and a
// frozen map must stay frozen.
'use strict';

const dict = { __proto__: null };
const fast = Object.setPrototypeOf({}, null);

let failed = 0;
function eq(label, a, b) {
  const ok = a === b;
  if (!ok) {
    failed++;
  }
  console.log(`${ok ? 'ok  ' : 'FAIL'}  ${label.padEnd(46)} ${JSON.stringify(a)} ${ok ? '' : '!== ' + JSON.stringify(b)}`);
}

// --- prototype ---
eq('prototype is null', Object.getPrototypeOf(dict), Object.getPrototypeOf(fast));
eq('prototype is null (value)', Object.getPrototypeOf(fast), null);
eq('"toString" in map', 'toString' in fast, false);
eq('"constructor" in map', 'constructor' in fast, false);
eq('hasOwnProperty is absent', fast.hasOwnProperty, undefined);
eq('own keys before any store', Object.keys(fast).length, 0);

// --- a field named "__proto__" ---
dict['__proto__'] = 'polluted';
fast['__proto__'] = 'polluted';
eq('"__proto__" is an own field', Object.prototype.hasOwnProperty.call(fast, '__proto__'), true);
eq('"__proto__" value', fast['__proto__'], 'polluted');
eq('Object.prototype is untouched', ({}).polluted, undefined);
eq('the two shapes agree', fast['__proto__'], dict['__proto__']);

// --- the dot form must not reach the prototype either ---
const dot = Object.setPrototypeOf({}, null);
dot.__proto__ = 'x';
eq('dot form is an own field too', Object.prototype.hasOwnProperty.call(dot, '__proto__'), true);
eq('dot form did not set a prototype', Object.getPrototypeOf(dot), null);

// --- enumeration order, including an integer-like name ---
function filled(make) {
  const o = make();
  o['content-type'] = 'text/plain';
  o.server = 'h2lean';
  o['0'] = 'zero';
  o[':status'] = 200;
  o['10'] = 'ten';
  return o;
}
eq('enumeration order', Object.keys(filled(() => Object.setPrototypeOf({}, null))).join(','),
  Object.keys(filled(() => ({ __proto__: null }))).join(','));

// --- JSON and valueOf behave the same ---
eq('JSON.stringify', JSON.stringify(fast), JSON.stringify(dict));

// --- freezing ---
const frozenFast = Object.freeze(Object.setPrototypeOf({}, null));
const frozenDict = Object.freeze({ __proto__: null });
eq('isFrozen (fast)', Object.isFrozen(frozenFast), true);
eq('isFrozen (dictionary)', Object.isFrozen(frozenDict), true);
eq('a frozen fast map is still readable', frozenFast.server, undefined);
// A write to a frozen map throws in strict mode -- and it must throw the same
// way for both shapes, because lib/compat.js writes into req.headers.
let fastThrow = 'none';
let dictThrow = 'none';
try {
  frozenFast.server = 'x';
} catch (e) {
  fastThrow = e.constructor.name;
}
try {
  frozenDict.server = 'x';
} catch (e) {
  dictThrow = e.constructor.name;
}
eq('a frozen fast map rejects writes', fastThrow, dictThrow);
eq('and it rejects them loudly', fastThrow, 'TypeError');

// --- set-cookie arrays and the shared-object path ---
const arr = [];
const shared = Object.setPrototypeOf({}, null);
shared['set-cookie'] = arr;
Object.freeze(arr);
eq('a frozen array inside the map', Object.isFrozen(shared['set-cookie']), true);

// --- the map survives a store of a name the map has never seen ---
const grow = Object.setPrototypeOf({}, null);
grow.a = 1; grow.b = 2; grow.c = 3;
eq('three stores are readable', grow.a + grow.b + grow.c, 6);
eq('the keys are in insertion order', Object.keys(grow).join(','), 'a,b,c');

// --- and the prototype is still null after the stores ---
eq('prototype is null after the stores', Object.getPrototypeOf(grow), null);

console.log(failed === 0 ? '\nall the same' : `\n${failed} DIFFERENCE(S)`);
process.exitCode = failed === 0 ? 0 : 1;
