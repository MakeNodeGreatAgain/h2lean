// Scratch probe: the upper bound of the msgpackr-style latin1 conversion in
// the HPACK decoder. The block below is 8 literal field lines whose names and
// values are 1-4 octets long -- the shape where String.fromCharCode is
// fastest and Buffer.toString('latin1') is at its most wasteful.
'use strict';

const fs = require('node:fs');

const HELPER = `
const FCC = String.fromCharCode;
function latin1(buf, start, end) {
  const len = end - start;
  switch (len) {
    case 0: return '';
    case 1: return FCC(buf[start]);
    case 2: return FCC(buf[start], buf[start + 1]);
    case 3: return FCC(buf[start], buf[start + 1], buf[start + 2]);
    case 4: return FCC(buf[start], buf[start + 1], buf[start + 2], buf[start + 3]);
    default: return buf.toString('latin1', start, end);
  }
}
`;

const src = fs.readFileSync('lib/hpack.js', 'utf8');

function build(name, code) {
  fs.writeFileSync(`out/hpack-${name}.js`,
    code.replace("require('./hpack-data')", "require('../../lib/hpack-data')"));
  return require(`./hpack-${name}.js`);
}

const base = build('base', src);
const fast = build('fast', src.replace('class Decoder {', `${HELPER}\nclass Decoder {`)
  .replace("buf.toString('latin1', p, p + len)", 'latin1(buf, p, p + len)')
  .replace("this.scratch.toString('latin1', 0, n)", 'latin1(this.scratch, 0, n)'));

// a block of literal-without-indexing field lines (0x10 prefix), 2-3 octet
// names and 1-4 octet values
function block(pairs) {
  const parts = [];
  for (const [n, v] of pairs) {
    const nb = Buffer.from(n, 'latin1');
    const vb = Buffer.from(v, 'latin1');
    parts.push(Buffer.from([0x10, nb.length]), nb, Buffer.from([vb.length]), vb);
  }
  return Buffer.concat(parts);
}

const SHORT = block([
  ['x-a', 'v1'], ['x-b', 'v2'], ['x-c', 'v3'], ['x-d', 'v4'],
  ['x-e', 'v5'], ['x-f', 'v6'], ['x-g', 'v7'], ['x-h', 'v8']
]);
const LONG = block([
  ['x-request-id', '0123456789abcdef0123456789abcdef'],
  ['user-agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'],
  ['content-type', 'application/json'],
  ['referer', 'https://example.com/some/page?with=query'],
  ['cookie', 'a=1; b=2; c=3'],
  ['accept-language', 'en-US,en;q=0.9'],
  ['sec-ch-ua-platform', '"macOS"'],
  ['x-forwarded-for', '192.0.2.1, 198.51.100.2']
]);

function time(label, fn, n) {
  for (let i = 0; i < 20_000; i++) {
    fn();
  }
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < n; i++) {
    fn();
  }
  const ns = Number(process.hrtime.bigint() - t0) / n;
  return ns;
}

function run(name, blk, n) {
  const res = {};
  const arms = [['base', base], ['fast', fast]];
  for (let round = 0; round < 4; round++) {
    const order = round % 2 === 0 ? arms : arms.slice().reverse();
    for (const [label, mod] of order) {
      const dec = new mod.Decoder(4096);
      const sink = [];
      const t = time(label, () => dec.decode(blk, 0, blk.length, sink), n);
      (res[label] = res[label] || []).push(t);
    }
  }
  const b = Math.min(...res.base);
  const f = Math.min(...res.fast);
  console.log(`${name} (${blk.length}B): base ${b.toFixed(1)} ns/op, fast ${f.toFixed(1)} ns/op` +
              ` -> ${((f / b - 1) * 100).toFixed(1)}% (${(b - f).toFixed(0)} ns/block)`);
  console.log(`   base rounds: ${res.base.map((x) => x.toFixed(0)).join(' ')}`);
  console.log(`   fast rounds: ${res.fast.map((x) => x.toFixed(0)).join(' ')}`);
}

run('short literals', SHORT, 300_000);
run('long literals ', LONG, 300_000);
process.exit(0);
