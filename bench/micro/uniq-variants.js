'use strict';
// Where the ~180 ns of emit() -- the cold path of one unique field line -- go.
//
// Every variant is warmed up with the ORIGINAL encoder, so the two repeating
// lines are already in the byte cache and the table is settled; the patch is
// applied afterwards and only ever sees the unique line, whose value is new
// every time.
//
//   base        -- the code in lib/hpack.js
//   no-add      -- emit() without the Set insert of the new value (the hash of
//                  a fresh string, the insert and the growth of the Set)
//   once-64     -- base, but the "seen once" set is emptied every 64 responses
//                  instead of every 8192: the same policy, a smaller memory
//   no-scan     -- emit() without the fullIndex()/findName() scans of the
//                  dynamic table
//   cached-nm   -- the encoded NAME bytes cached per name, so a new value does
//                  not Huffman-code the name again
//
//   node bench/micro/uniq-variants.js [iters]
const { Encoder, NAME_INDEX } = require('../../lib/hpack');
const { HUFF_BITS, HUFF_CODES } = require('../../lib/hpack-data');
const { block } = require('../../lib/headers');

const ITERS = Number(process.argv[2] || 200000);
const NEVER_INSERT = new Set(['set-cookie', 'www-authenticate', 'proxy-authenticate',
  'authorization', 'cookie']);
const POW2 = new Float64Array(53);
for (let i = 0; i < 53; i++) {
  POW2[i] = 2 ** i;
}

function newSession() {
  return { _encoder: new Encoder(4096), _blocks: new WeakMap() };
}

function makeHeaders(seq) {
  return {
    'content-type': 'text/plain',
    server: 'prof',
    'x-trace': '0123456789abcdef0123456789abcdef',
    'x-req': String(seq)
  };
}

function measure(name, session, patch, iters) {
  const enc = session._encoder;
  let seq = 0;
  // warm up with the original encoder: the repeating lines get cached
  for (let i = 0; i < 2000; i++) {
    block(makeHeaders(++seq), undefined, {}, session);
  }
  if (patch) {
    if (typeof patch === 'function') {
      patch(enc);
    } else if (patch.patch) {
      patch.patch(enc);
    }
  }
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < iters; i++) {
    block(makeHeaders(++seq), undefined, {}, session);
    if (patch && patch.tick) {
      patch.tick(enc, i);
    }
  }
  const ns = Number(process.hrtime.bigint() - t0) / iters;
  console.log(`${name.padEnd(10)} ${ns.toFixed(1)} ns/block`);
  return ns;
}

// The original emit() with the Set insert of the new value left out.
function patchNoAdd(enc) {
  const orig = enc.emit;
  enc.emit = function (dst, off, name, value, neverIndex) {
    if (neverIndex) {
      return orig.call(this, dst, off, name, value, neverIndex);
    }
    const hit = this.fullIndex(name, value);
    if (hit !== undefined) {
      return this.put(dst, off, this.indexed(hit));
    }
    if (NEVER_INSERT.has(name)) {
      return this.put(dst, off, this.literal(name, value, 0x00));
    }
    if (this.maxSize === 0 || this.noInsert) {
      return this.put(dst, off, this.literal(name, value, 0x00));
    }
    if (name.length + value.length + 32 > this.maxSize - (this.maxSize >> 2)) {
      return this.put(dst, off, this.literal(name, value, 0x00));
    }
    let seen = this.once.get(name);
    if (seen === undefined) {
      seen = new Set();
      this.once.set(name, seen);
    }
    if (!seen.has(value)) {
      this.unsettled = true;
      return this.literalInto(dst, off, name, value, 0x00);
    }
    seen.delete(value);
    this.onceCount--;
    const end = this.literalInto(dst, off, name, value, 0x40);
    this.insert(name, value);
    return end;
  };
}

// The original emit() without the scans of the dynamic table.
function patchNoScan(enc) {
  enc.emit = function (dst, off, name, value, neverIndex) {
    if (neverIndex) {
      return this.put(dst, off, this.literal(name, value, 0x10));
    }
    if (NEVER_INSERT.has(name)) {
      return this.put(dst, off, this.literal(name, value, 0x00));
    }
    if (this.maxSize === 0 || this.noInsert) {
      return this.put(dst, off, this.literal(name, value, 0x00));
    }
    if (name.length + value.length + 32 > this.maxSize - (this.maxSize >> 2)) {
      return this.put(dst, off, this.literal(name, value, 0x00));
    }
    let seen = this.once.get(name);
    if (seen === undefined) {
      seen = new Set();
      this.once.set(name, seen);
    }
    if (!seen.has(value)) {
      seen.add(value);
      this.onceCount++;
      this.unsettled = true;
      return this.literalInto(dst, off, name, value, 0x00);
    }
    seen.delete(value);
    this.onceCount--;
    const end = this.literalInto(dst, off, name, value, 0x40);
    this.insert(name, value);
    return end;
  };
}

// The encoded name bytes, cached per name: the length prefix and the
// Huffman-coded name, which every new value pays for again otherwise.
const NAME_BYTES = new Map();
function encodeName(name) {
  let bits = 0;
  for (let i = 0; i < name.length; i++) {
    bits += HUFF_BITS[name.charCodeAt(i) & 0xff];
  }
  const hlen = (bits + 7) >> 3;
  const b = Buffer.allocUnsafe(1 + (hlen < name.length ? hlen : name.length));
  if (hlen >= name.length) {
    b[0] = name.length;
    b.write(name, 1, 'latin1');
    return b;
  }
  b[0] = 0x80 | hlen;
  let acc = 0;
  let nbits = 0;
  let off = 1;
  for (let i = 0; i < name.length; i++) {
    const sym = name.charCodeAt(i) & 0xff;
    const nb = HUFF_BITS[sym];
    acc = acc * POW2[nb] + HUFF_CODES[sym];
    nbits += nb;
    while (nbits >= 8) {
      nbits -= 8;
      const byte = Math.floor(acc / POW2[nbits]);
      b[off++] = byte;
      acc -= byte * POW2[nbits];
    }
  }
  if (nbits > 0) {
    // this arm mirrors the shipped encoder's Huffman body (lib/hpack.js,
    // huffEncode); rewriting the tail would change the code under measurement.
    // eslint-disable-next-line no-useless-assignment
    b[off++] = acc * POW2[8 - nbits] + (POW2[8 - nbits] - 1);
  }
  return b;
}

function patchCachedName(enc) {
  const orig = enc.literalInto;
  enc.literalInto = function (dst, off, name, value, prefix) {
    if (prefix !== 0x40 && !NAME_INDEX.has(name) && this.findName(name) === 0) {
      let nb = NAME_BYTES.get(name);
      if (nb === undefined) {
        nb = encodeName(name);
        NAME_BYTES.set(name, nb);
      }
      dst[off] = 0x00;
      off += 1;
      nb.copy(dst, off);
      return this.literalStrInto(dst, off + nb.length, value, huffBitsOf(value));
    }
    return orig.call(this, dst, off, name, value, prefix);
  };
}

function huffBitsOf(s) {
  let bits = 0;
  for (let i = 0; i < s.length; i++) {
    bits += HUFF_BITS[s.charCodeAt(i) & 0xff];
  }
  return bits;
}

const base = measure('base', newSession(), null, ITERS);
const noAdd = measure('no-add', newSession(), patchNoAdd, ITERS);
const once64 = measure('once-64', newSession(), {
  tick(enc, i) {
    if ((i & 63) === 63) {
      enc.once.clear(); enc.onceCount = 0;
    }
  }
}, ITERS);
const noScan = measure('no-scan', newSession(), patchNoScan, ITERS);
const cachedNm = measure('cached-nm', newSession(), patchCachedName, ITERS);

console.log('');
console.log(`the Set insert of the new value costs ${(base - noAdd).toFixed(1)} ns`);
console.log(`a smaller "seen once" set saves      ${(base - once64).toFixed(1)} ns`);
console.log(`the scans of the dynamic table cost   ${(base - noScan).toFixed(1)} ns`);
console.log(`re-Huffman-coding the name costs      ${(base - cachedNm).toFixed(1)} ns`);
