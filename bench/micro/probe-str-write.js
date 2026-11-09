// Scratch probe: raw `dst.write(s, off, 'latin1')` cost by the string's V8
// representation, and the charCodeAt loop next to it. The sweep in
// probe-hpack-str-enc.js scaled at ~14 ns/char in both arms, which is not a
// C++ writer's cost -- the value strings there were built with replace() and
// are likely rope (cons) strings. Here each shape is measured separately so
// the encoder's numbers can be read against the shape the application sends.
'use strict';

const dst = Buffer.alloc(8192);
const N = 2_000_000;

function time(fn, warm = 50_000) {
  for (let i = 0; i < warm; i++) {
    fn();
  }
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < N; i++) {
    fn();
  }
  return Number(process.hrtime.bigint() - t0) / N;
}

const LIT32 = 'abcdefghijklmnopqrstuvwxyz012345'; // a literal, flat seq-one-byte
function lit(len) {
  return LIT32.slice(0, len);
} // NOTE: sliced string

function fcc(len) { // flat seq-one-byte via fromCharCode
  return String.fromCharCode(...Array.from({ length: len }, (_, i) => 0x61 + (i % 26)));
}
function fcc2(len) { // flat seq-two-byte
  return String.fromCharCode(...Array.from({ length: len }, () => 0xfcfd));
}
function rope(len) { // cons string, like replace()/concat products
  const a = 'abcdefgh';
  const b = 'ijklmnop';
  let s = '';
  for (let i = 0; i < len; i += 8) {
    s += (i % 16 === 0 ? a : b);
  }
  return s;
}

function loop(s) {
  const off = 0;
  const rawLen = s.length;
  for (let i = 0; i < rawLen; i++) {
    dst[off + i] = s.charCodeAt(i);
  }
  return off + rawLen;
}
function write(s) {
  return dst.write(s, 0, 'latin1');
}

const shapes = [
  ['literal .slice (sliced str)', (n) => lit(n)],
  ['String.fromCharCode (flat 1B)', (n) => fcc(n)],
  ['rope (a += b)', (n) => rope(n)],
  ['two-byte (flat 2B)', (n) => fcc2(n)]
];

for (const len of [8, 32, 128]) {
  console.log(`--- ${len} chars ---`);
  for (const [label, mk] of shapes) {
    const s = mk(len);
    const w = time(() => write(s));
    const l = time(() => loop(s));
    console.log(`  ${label.padEnd(30)} write ${w.toFixed(1)} ns, loop ${l.toFixed(1)} ns -> ${((l / w - 1) * 100).toFixed(1)}%`);
  }
}
process.exit(0);
