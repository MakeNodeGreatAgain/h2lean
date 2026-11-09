// The REAL _toObject, measured both ways in one process.
//
//   node bench/micro/toobject-real.js
//
// ../out/map-shape-real.js (local scratch) measured a COPY of the method body. This one calls the
// method on the real class, so the store site, the ICs and the map shape are
// the ones the server runs.
//
// The second arm is the same body with the dictionary literal, kept here as a
// local function: the two are measured in one process, interleaved, so the
// machine's state cannot separate them.
'use strict';

const { Http2Session } = require('../../lib/session');

// The flat decoded header block, as the session hands it to _toObject.
const SETS = [
  [':method', 'GET', ':path', '/a/b/c?x=1', ':scheme', 'http', ':authority', 'localhost'],
  [':method', 'POST', ':path', '/upload/0123456789', ':scheme', 'https', ':authority', 'example.com',
    'content-type', 'application/json', 'user-agent', 'probe/1.0'],
  [':method', 'GET', ':path', '/', ':scheme', 'http', ':authority', 'localhost',
    'accept', '*/*', 'accept-encoding', 'gzip, deflate', 'x-request-id', '9f2c1d4e']
];

// The dictionary version of the same body.
function toObjectDict(flat) {
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

const real = Http2Session.prototype._toObject;
let sink = null;
let sinkN = 0; // reads the length sums the read-back arms compute; see void sink

function bench(name, fn) {
  for (let i = 0; i < 30000; i++) {
    fn(i);
  }
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < 200000; i++) {
    fn(i);
  }
  const t1 = process.hrtime.bigint();
  const ns = Number(t1 - t0) / 200000;
  console.log(`${name.padEnd(40)} ${ns.toFixed(1)} ns`);
  return ns;
}

// The real method, called on a bare object -- it touches no session state.
function runReal(i) {
  sink = real.call({}, SETS[i % SETS.length]);
}
function runDict(i) {
  sink = toObjectDict(SETS[i % SETS.length]);
}

// Interleaved: three passes each, so a drift in the machine shows up on both.
const realNs = [];
const dictNs = [];
for (let pass = 0; pass < 3; pass++) {
  dictNs.push(bench(`pass ${pass + 1}: dictionary`, runDict));
  realNs.push(bench(`pass ${pass + 1}: the real method`, runReal));
}

const med = (a) => a.slice().sort((x, y) => x - y)[Math.floor(a.length / 2)];
console.log(`\nthe real method: ${med(realNs).toFixed(1)} ns`);
console.log(`the dictionary:   ${med(dictNs).toFixed(1)} ns`);
console.log(`the difference:   ${(med(dictNs) - med(realNs)).toFixed(1)} ns ` +
  `(${(((med(dictNs) - med(realNs)) / med(dictNs)) * 100).toFixed(0)}%)`);

// And what the map costs to read back, which is what the application pays.
console.log('\n--- and the read back');
function readReal(i) {
  const o = real.call({}, SETS[i % SETS.length]);
  const keys = Object.keys(o);
  let n = 0;
  for (let k = 0; k < keys.length; k++) {
    n += o[keys[k]].length;
  }
  sink = o; sinkN = n;
}
function readDict(i) {
  const o = toObjectDict(SETS[i % SETS.length]);
  const keys = Object.keys(o);
  let n = 0;
  for (let k = 0; k < keys.length; k++) {
    n += o[keys[k]].length;
  }
  sink = o; sinkN = n;
}
const rReal = [];
const rDict = [];
for (let pass = 0; pass < 3; pass++) {
  rDict.push(bench(`pass ${pass + 1}: dictionary`, readDict));
  rReal.push(bench(`pass ${pass + 1}: the real method`, readReal));
}
console.log(`\nthe read back:   ${med(rReal).toFixed(1)} ns (fast) vs ` +
  `${med(rDict).toFixed(1)} ns (dictionary)`);
console.log(`the difference:   ${(med(rDict) - med(rReal)).toFixed(1)} ns`);
// sink keeps every built object reachable until here; without it V8 can hoist
// the construction out of a loop whose result nobody reads.
void sink; void sinkN;
