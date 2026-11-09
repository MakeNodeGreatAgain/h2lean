// What does it cost to turn a decoded field list into the headers object the
// application sees? The post-leak-fix profile put _toObject at 13.6% of the
// server's busy CPU, so this is the biggest single JS cost left.
//
// Variants, all producing the same visible object:
//   a) today's loop: read name, compare against ':status'/'set-cookie', look the
//      name up in the object to detect repeats
//   b) simple loop from a precomputed keys array (only legal for shapes with no
//      pseudo/cookie special cases and no duplicate names)
//   c) a code-generated builder per shape (new Function)
//   d) spread/clone of a cached object, for reference
//   e) node's own cost, for reference
'use strict';

const FIELD_NAMES = [':method', ':path', ':scheme', ':authority', 'accept', 'user-agent'];
const FIELD_VALUES = ['GET', '/abc/def/ghij?x=1', 'http', 'localhost', '*/*', 'probe/1.0'];
const FLAT = [];
for (let i = 0; i < FIELD_NAMES.length; i++) {
  FLAT.push(FIELD_NAMES[i], FIELD_VALUES[i]);
}
const N = 200000;

function toObjectToday(flat) { // (a) the current implementation
  const o = { __proto__: null };
  for (let i = 0; i < flat.length; i += 2) {
    const name = flat[i];
    let value = flat[i + 1];
    if (name === ':status') {
      value = value | 0;
    }
    const have = o[name];
    if (have === undefined) {
      o[name] = name === 'set-cookie' ? [value] : value;
    } else if (name === 'cookie') {
      o[name] = `${have}; ${value}`;
    } else if (name === 'set-cookie') {
      have.push(value);
    } else {
      o[name] = `${have}, ${value}`;
    }
  }
  return o;
}

function toObjectSimple(keys, values) { // (b) precomputed key list
  const o = { __proto__: null };
  for (let i = 0; i < keys.length; i++) {
    o[keys[i]] = values[i];
  }
  return o;
}

const codegen = new Function('v', 'return { __proto__: null, ' +
  FIELD_NAMES.map((n, i) => `${JSON.stringify(n)}: v[${i}]`).join(', ') + ' };');

const cached = toObjectToday(FLAT);
function time(label, fn) {
  fn(); fn(); // warm up, then measure
  const t0 = process.hrtime.bigint();
  let sink = null;
  for (let i = 0; i < N; i++) {
    sink = fn();
  }
  const ns = Number(process.hrtime.bigint() - t0) / N;
  if (!sink) {
    throw new Error('no result');
  }
  console.log(`${label.padEnd(34)} ${ns.toFixed(1)} ns/request`);
}

time('a) today: loop with compares', () => toObjectToday(FLAT));
time('b) simple loop over keys', () => toObjectSimple(FIELD_NAMES, FIELD_VALUES));
time('c) code-generated builder', () => codegen(FIELD_VALUES));
time('d) spread clone of a cached object', () => ({ ...cached }));
time('e) Object.assign into null proto', () => Object.assign({ __proto__: null }, cached));
