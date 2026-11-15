'use strict';
// Item-39 integration probe: the same encoded header block decoded through
// lib/hpack.js into (A) a reused plain array -- the shape the pre-change code had,
// a right-trimmed store per block (OPTIMIZATION.md item 38) -- and (B) a FlatList
// scratch (lib/flat-list.js), filled through out.push(). The block is encoded by a
// zero-table encoder (no dynamic indices, no insertions), so one warm decoder can
// decode the identical bytes forever and the only difference between the arms is
// the container the fill lands in.
//   node bench/micro/probe-decode-flat.js
const HPack = require('../../lib/hpack');
const FlatList = require('../../lib/flat-list');

// A typical request block: 8 field lines, values long enough to be realistic.
const PAIRS = [
  ':method', 'GET',
  ':path', '/search?q=http%2F2&lang=en&page=3',
  ':scheme', 'https',
  ':authority', 'example.org',
  'user-agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
  'accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'cookie', 'session=0123456789abcdef; theme=dark; consent=1',
  'x-request-id', '9f86d081884c7d659a2feaa0c55ad015'
];
// peerSize 0 -> maxSize 0 -> every line is a literal without indexing and nothing is
// inserted: the bytes name only static/full-literal forms, valid in any decoder state.
const enc = new HPack.Encoder(0, 0);
const scratch = Buffer.allocUnsafe(PAIRS.length * HPack.Encoder.maxBytesOf('user-agent', PAIRS[9]));
let boff = 0;
for (let i = 0; i < PAIRS.length; i += 2) {
  boff = enc.write(scratch, boff, PAIRS[i], PAIRS[i + 1], false);
}
const block = scratch.subarray(0, boff);

const ITERS = 300000;
const ROUNDS = 5;

function arm(dec, out, isFlat) {
  // Warm-up.
  for (let i = 0; i < 5000; i++) {
    dec.decode(block, 0, block.length, out);
  }
  const t0 = process.hrtime.bigint();
  let sink = 0;
  for (let i = 0; i < ITERS; i++) {
    dec.decode(block, 0, block.length, out);
    sink += isFlat ? out.count : out.length;
  }
  const t1 = process.hrtime.bigint();
  if (sink === -1) {
    console.log('unreachable');
  }
  return Number(t1 - t0) / ITERS; // ns per decode
}

const arr = [];
const flat = new FlatList(64);
const decA = new HPack.Decoder(4096);
const decB = new HPack.Decoder(4096);

// ABBA interleaving, 5 rounds, to average out the loop's drift.
const aRounds = [], bRounds = [];
for (let r = 0; r < ROUNDS; r++) {
  const a1 = arm(decA, arr, false); const b1 = arm(decB, flat, true);
  const b2 = arm(decB, flat, true); const a2 = arm(decA, arr, false);
  aRounds.push((a1 + a2) / 2); bRounds.push((b1 + b2) / 2);
  console.log(`round ${r + 1}: plain ${((a1 + a2) / 2).toFixed(0)}ns  flat ${((b1 + b2) / 2).toFixed(0)}ns`);
}
const mean = (xs) => xs.reduce((s, x) => s + x, 0) / xs.length;
console.log(`mean: plain ${mean(aRounds).toFixed(0)}ns/decode  flat ${mean(bRounds).toFixed(0)}ns/decode`);
console.log(`block: ${block.length} octets, ${PAIRS.length / 2} field lines, decoder ${decA.constructor.name}`);
