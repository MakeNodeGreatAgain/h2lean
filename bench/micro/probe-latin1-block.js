// Scratch probe: the latin1() widening past four octets, at real-block
// granularity. Item 13 originally measured the eight-case switch 6% SLOWER on
// a real block; the raw probe (probe-latin1-concat.js) measured switch8
// -60..-65% at lengths 5-8 on node v26.12.1, contradicting it, so the
// block-level number was re-measured here, ABBA, min of rounds, on library
// variants built from lib/hpack.js. The committed switch-4 + recursive-chunk
// (up to 12) won every block and is in lib since then; the arms below are
// alternatives against the committed code, so the probe stays re-runnable:
//   committed  whatever latin1() is in lib/hpack.js right now (the baseline)
//   old4       the original switch-4, toString beyond
//   wide       cases 0-8, toString beyond
//   rec8       switch-4 + recursive chunk4 up to 8 only
// The blocks are literal-without-indexing (0x10) so every decode re-decodes;
// the huff blocks carry Huffman-coded values whose DECODED length is 5-8 or
// 9-12, the shape the scratch path at line ~669 sees.
'use strict';

const fs = require('node:fs');
const { HUFF_BITS, HUFF_CODES } = require('../../lib/hpack-data');

function latin1Src(maxCase, chunkMax, rec) {
  const cases = [];
  for (let n = 1; n <= maxCase; n++) {
    const args = Array.from({ length: n }, (_, i) => i === 0 ? 'buf[start]' : `buf[start + ${i}]`).join(', ');
    cases.push(`    case ${n}: return String.fromCharCode(${args});`);
  }
  let tail = '    default: return buf.toString(\'latin1\', start, end);';
  if (rec) {
    // chunk4 + recursion: the rest is <= 8 octets, so it chunks again at most once
    tail = `    default: {
      if (len <= ${chunkMax}) {
        return String.fromCharCode(buf[start], buf[start + 1], buf[start + 2], buf[start + 3])
          + latin1(buf, start + 4, end);
      }
      return buf.toString('latin1', start, end);
    }`;
  } else if (chunkMax > maxCase) {
    tail = `    default: {
      if (len <= ${chunkMax}) {
        let s = '';
        let p = start;
        while (end - p > 4) {
          s += String.fromCharCode(buf[p], buf[p + 1], buf[p + 2], buf[p + 3]);
          p += 4;
        }
        const rest = [];
        for (; p < end; p++) {
          rest.push(buf[p]);
        }
        return s + String.fromCharCode(...rest);
      }
      return buf.toString('latin1', start, end);
    }`;
  }
  return `function latin1(buf, start, end) {
  const len = end - start;
  switch (len) {
    case 0: return '';
${cases.join('\n')}
${tail}
  }
}`;
}

const src = fs.readFileSync('lib/hpack.js', 'utf8');
const START_FN = src.indexOf('function latin1(');
const END_FN = src.indexOf('\n}\n', START_FN) + 3;
const COMMITTED = src.slice(START_FN, END_FN);
if (!COMMITTED.startsWith('function latin1(') || !COMMITTED.endsWith('}\n')) {
  throw new Error('latin1() extraction failed');
}

function build(name, code) {
  fs.writeFileSync(`out/hpack-${name}.js`,
    code.replace("require('./hpack-data')", "require('../lib/hpack-data')"));
  return require(`../../out/hpack-${name}.js`);
}

const committed = build('l1-committed', src);
const old4 = build('l1-old4', src.replace(COMMITTED, latin1Src(4, 4)));
const wide = build('l1-wide', src.replace(COMMITTED, latin1Src(8, 8)));
const rec8 = build('l1-rec8', src.replace(COMMITTED, latin1Src(4, 8, true)));

// --- block construction ---------------------------------------------------------

// never-indexed literal field line, optional Huffman value coding
function line(name, value, huff) {
  const parts = [Buffer.from([0x10])];
  const n = Buffer.from(name, 'latin1');
  if (n.length > 126) {
    throw new Error('name too long for one prefix octet');
  }
  parts.push(Buffer.from([n.length]), n);
  let v = Buffer.from(value, 'latin1');
  if (huff) {
    let acc = 0;
    let bits = 0;
    const out = [];
    for (let i = 0; i < value.length; i++) {
      const c = value.charCodeAt(i);
      acc = (acc << HUFF_BITS[c]) | HUFF_CODES[c];
      bits += HUFF_BITS[c];
      while (bits >= 8) {
        out.push((acc >> (bits - 8)) & 0xff);
        bits -= 8;
      }
    }
    if (bits > 0) {
      // the padding is the EOS prefix: all ones
      out.push((acc << (8 - bits)) | ((1 << (8 - bits)) - 1));
    }
    v = Buffer.from(out);
  }
  parts.push(Buffer.from([(huff ? 0x80 : 0) | v.length]), v);
  return Buffer.concat(parts);
}

function block(pairs, huff) {
  return Buffer.concat(pairs.map(([n, v]) => line(n, v, huff)));
}

const SHORT = block([
  ['x-a', 'v1'], ['x-b', 'v2'], ['x-c', 'v3'], ['x-d', 'v4'],
  ['x-e', 'v5'], ['x-f', 'v6'], ['x-g', 'v7'], ['x-h', 'v8']
]);
// values of exactly 5-8 octets
const MID58 = block([
  ['x-a', 'value1'], ['x-b', 'value2'], ['x-c', 'value3'], ['x-d', 'value4'],
  ['x-e', 'value5'], ['x-f', 'value6'], ['x-g', 'value7'], ['x-h', 'value8']
]);
// values of exactly 9-12 octets
const MID912 = block([
  ['x-a', 'value12345'], ['x-b', 'value67890'], ['x-c', 'valueabcde'],
  ['x-d', 'valuefghij'], ['x-e', 'valueklmno'], ['x-f', 'valuepqrst'],
  ['x-g', 'valueuvwxy'], ['x-h', 'valuez1234']
]);
// the same 5-8 / 9-12 shapes, Huffman-coded on the wire
const HUFF58 = block([
  ['x-a', 'value1'], ['x-b', 'value2'], ['x-c', 'value3'], ['x-d', 'value4'],
  ['x-e', 'value5'], ['x-f', 'value6'], ['x-g', 'value7'], ['x-h', 'value8']
], true);
const HUFF912 = block([
  ['x-a', 'value12345'], ['x-b', 'value67890'], ['x-c', 'valueabcde'],
  ['x-d', 'valuefghij'], ['x-e', 'valueklmno'], ['x-f', 'valuepqrst'],
  ['x-g', 'valueuvwxy'], ['x-h', 'valuez1234']
], true);
// realistic long values -- the shapes the widening must not regress
const LONG = block([
  ['x-request-id', '0123456789abcdef0123456789abcdef'],
  ['user-agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'],
  ['content-type', 'application/json'],
  ['referer', 'https://example.com/some/page?with=query'],
  ['cookie', 'a=1; b=2; c=3; d=4; e=5; f=6'],
  ['accept-language', 'en-US,en;q=0.9'],
  ['sec-ch-ua-platform', '"macOS"'],
  ['x-forwarded-for', '192.0.2.1, 198.51.100.2']
]);

// sanity: every variant decodes every block to the same field count
for (const mod of [committed, old4, wide, rec8]) {
  const d = new mod.Decoder(4096);
  const sink = [];
  for (const blk of [SHORT, MID58, MID912, HUFF58, HUFF912, LONG]) {
    if (d.decode(blk, 0, blk.length, sink) !== 8) {
      throw new Error('decode sanity failed');
    }
  }
}

function time(fn, n) {
  for (let i = 0; i < 20_000; i++) {
    fn();
  }
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < n; i++) {
    fn();
  }
  return Number(process.hrtime.bigint() - t0) / n;
}

const ARMS = [['committed', committed], ['old4', old4], ['wide', wide], ['rec8', rec8]];

function run(name, blk, n) {
  const res = new Map();
  for (let round = 0; round < 4; round++) {
    const order = round % 2 === 0 ? ARMS : ARMS.slice().reverse();
    for (const [label, mod] of order) {
      const dec = new mod.Decoder(4096);
      const sink = [];
      const t = time(() => dec.decode(blk, 0, blk.length, sink), n);
      if (!res.has(label)) {
        res.set(label, []);
      }
      res.get(label).push(t);
    }
  }
  const b = Math.min(...res.get('committed'));
  const verdict = [`${name} (${blk.length}B): committed ${b.toFixed(1)} ns/block`];
  for (const [label] of ARMS) {
    if (label === 'committed') {
      continue;
    }
    const m = Math.min(...res.get(label));
    verdict.push(`${label} ${m.toFixed(1)} (${((m / b - 1) * 100).toFixed(1)}%)`);
  }
  console.log(verdict.join(', '));
  for (const [label] of ARMS) {
    console.log(`   ${label}: ${res.get(label).map((x) => x.toFixed(0)).join(' ')}`);
  }
}

run('short 1-4   ', SHORT, 400_000);
run('mid 5-8     ', MID58, 400_000);
run('mid 9-12    ', MID912, 400_000);
run('huff 5-8    ', HUFF58, 400_000);
run('huff 9-12   ', HUFF912, 400_000);
run('long values ', LONG, 400_000);
process.exit(0);
