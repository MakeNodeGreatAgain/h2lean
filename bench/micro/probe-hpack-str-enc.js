// Scratch probe: the msgpackr-style latin1 conversion on the encoder side
// (OPTIMIZATION.md item 35, lib/hpack.js latin1Into()). The threshold arm is the
// library as committed; the write arm forces `Buffer#write('latin1')` for every
// length, so the sweep shows what the threshold buys per value length.
'use strict';

const fs = require('node:fs');

const ALWAYS_WRITE = `
  const len = s.length;
  dst.write(s, off, 'latin1');
  return off + len;`;

const src = fs.readFileSync('lib/hpack.js', 'utf8');

function build(name, code) {
  fs.writeFileSync(`out/hpack-enc-${name}.js`,
    code.replace("require('./hpack-data')", "require('../lib/hpack-data')"));
  return require(`../../out/hpack-enc-${name}.js`);
}

const loop = build('loop', src); // the committed threshold code
const writeSrc = src.replace(
  `  const len = s.length;
  if (len <= RAW_WRITE_MAX) {
    for (let i = 0; i < len; i++) {
      dst[off + i] = s.charCodeAt(i);
    }
  } else {
    dst.write(s, off, 'latin1');
  }
  return off + len;`,
  ALWAYS_WRITE);
if (writeSrc === src) {
  throw new Error('the write-arm replacement did not apply');
}
const write = build('write', writeSrc);

// One literal line, name in full (not in either table), value of a given shape.
// The raw (non-Huffman) branch needs a value whose Huffman form is not shorter,
// so the sweep uses characters Huffman does not compress well. The encoder's
// slots make literalInto stateless, so the same call repeats cleanly.
const dst = Buffer.alloc(4096);
const encs = new Map(); // arm label -> its own Encoder

function enc(label, mod, name, value) {
  let e = encs.get(label);
  if (e === undefined) {
    e = new mod.Encoder(4096);
    encs.set(label, e);
  }
  return e.literalInto(dst, 0, name, value, 0x00);
}

function time(label, fn, n) {
  for (let i = 0; i < 20_000; i++) {
    fn();
  }
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < n; i++) {
    fn();
  }
  return Number(process.hrtime.bigint() - t0) / n;
}

function sweep(label, mkValue, lengths, n) {
  console.log(label);
  for (const len of lengths) {
    const name = 'x-request-id';
    const value = mkValue(len);
    const arms = [['loop', loop], ['write', write]];
    const res = {};
    for (let round = 0; round < 4; round++) {
      const order = round % 2 === 0 ? arms : arms.slice().reverse();
      for (const [k, mod] of order) {
        const t = time(k, () => enc(k, mod, name, value), n);
        (res[k] = res[k] || []).push(t);
      }
    }
    const b = Math.min(...res.write);
    const f = Math.min(...res.loop);
    console.log(`  ${String(len).padStart(4)} ch: write ${b.toFixed(1)} ns, loop ${f.toFixed(1)} ns` +
      ` -> ${((f / b - 1) * 100).toFixed(1)}%  (write ${res.write.map((x) => x.toFixed(0)).join(' ')}` +
      ` | loop ${res.loop.map((x) => x.toFixed(0)).join(' ')})`);
  }
}

// Typical header values: flat seq-one-byte strings whose Huffman form is NOT
// shorter (the raw branch -- chars with >=11-bit Huffman codes, see
// probe-str-write.js for the per-representation crossover: flat 1B ~16 chars,
// rope ~8, two-byte ~30), plus a rope and a two-byte shape.
const RAWCH = [0x7e, 0x40, 0x5b, 0x7b, 0x7c]; // ~ @ [ { | -- all >=11-bit
sweep('flat 1B values (raw branch)', (n) => String.fromCharCode(...Array.from({ length: n }, (_, i) => RAWCH[i % RAWCH.length])),
  [4, 8, 16, 32, 64], 400_000);
sweep('rope values', (n) => 'abcdefgh'.repeat(Math.ceil(n / 8)).slice(0, n) + '',
  [8, 16, 32], 400_000);
sweep('two-byte values', (n) => String.fromCharCode(0xfc).repeat(n),
  [8, 16, 32], 400_000);
process.exit(0);
