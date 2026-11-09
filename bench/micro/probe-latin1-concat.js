// Scratch probe: does the decoder's latin1() win extend past four octets?
// Item 13 rejected the eight-case switch once ("6% slower on a real block");
// the concat idea is a different strategy -- build a longer string out of
// several short fromCharCode results instead of one Buffer.toString call.
// Arms per length:
//   toString   buf.toString('latin1', s, e)          -- the current >4 path
//   switch8    fromCharCode cases up to 8            -- the rejected widening
//   js+native  latin1(0..4) + toString(4..L)         -- one JS head, one C++ tail
//   native+js  toString(0..L-4) + latin1(L-4..L)     -- the mirror order
//   chunk4     s += latin1() per <=4-octet chunk     -- full chunking (PieceCursor.str() shape)
'use strict';

const dst = Buffer.alloc(64);

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
function latin1s8(buf, start, end) {
  const len = end - start;
  switch (len) {
    case 0: return '';
    case 1: return FCC(buf[start]);
    case 2: return FCC(buf[start], buf[start + 1]);
    case 3: return FCC(buf[start], buf[start + 1], buf[start + 2]);
    case 4: return FCC(buf[start], buf[start + 1], buf[start + 2], buf[start + 3]);
    case 5: return FCC(buf[start], buf[start + 1], buf[start + 2], buf[start + 3], buf[start + 4]);
    case 6: return FCC(buf[start], buf[start + 1], buf[start + 2], buf[start + 3], buf[start + 4], buf[start + 5]);
    case 7: return FCC(buf[start], buf[start + 1], buf[start + 2], buf[start + 3], buf[start + 4], buf[start + 5], buf[start + 6]);
    case 8: return FCC(buf[start], buf[start + 1], buf[start + 2], buf[start + 3], buf[start + 4], buf[start + 5], buf[start + 6], buf[start + 7]);
    default: return buf.toString('latin1', start, end);
  }
}

const ARMS = [
  ['toString ', (s, e) => dst.toString('latin1', s, e)],
  ['switch8  ', (s, e) => latin1s8(dst, s, e)],
  ['js+native', (s, e) => latin1(dst, s, s + 4) + dst.toString('latin1', s + 4, e)],
  ['native+js', (s, e) => dst.toString('latin1', s, e - 4) + latin1(dst, e - 4, e)],
  ['chunk4   ', (s, e) => {
    let acc = '';
    let p = s;
    while (e - p > 4) {
      acc += latin1(dst, p, p + 4);
      p += 4;
    }
    return acc + latin1(dst, p, e);
  }]
];

// The sinks keep the result strings alive so V8 cannot fold the whole loop
// into nothing; reading .length on a cons string also forces nothing -- the
// flatten cost stays where the application would pay it.
const sink = [];
for (const [, fn] of ARMS) {
  sink.push(fn(0, 5));
}

function time(fn, n) {
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < n; i++) {
    sink[i & 15] = fn();
  }
  return Number(process.hrtime.bigint() - t0) / n;
}

for (const L of [5, 6, 7, 8, 10, 12, 16, 24, 32, 64]) {
  const n = L <= 12 ? 5_000_000 : L <= 32 ? 2_000_000 : 1_000_000;
  const res = new Map();
  for (let round = 0; round < 4; round++) {
    // ABBA-ish: reverse the order every other round
    const order = round % 2 === 0 ? ARMS : ARMS.slice().reverse();
    for (const [label, fn] of order) {
      const t = time(() => fn(0, L), n);
      if (!res.has(label)) {
        res.set(label, []);
      }
      res.get(label).push(t);
    }
  }
  const base = Math.min(...res.get('toString '));
  const line = [`len ${String(L).padStart(2)}: toString ${base.toFixed(1)} ns`];
  for (const [label] of ARMS) {
    if (label === 'toString ') {
      continue;
    }
    const m = Math.min(...res.get(label));
    line.push(`${label.trim()} ${m.toFixed(1)} (${((m / base - 1) * 100).toFixed(0)}%)`);
  }
  console.log(line.join(', '));
}
