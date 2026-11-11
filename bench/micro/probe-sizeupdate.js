// Does the concat-free size-update prefix produce the same octets as before, and does
// a peer decoder read it correctly? Builds the same block twice for one encoder --
// once the way _encodeRequest used to (takeSizeUpdate + Buffer.concat), once the way
// it does now (reserve 5, sizeUpdate in place) -- and feeds both to a fresh Decoder.
//   node bench/micro/probe-sizeupdate.js
'use strict';

const { Decoder, Encoder, STATIC } = require('../../lib/hpack');

const LIST = [
  ':method', 'GET', ':path', '/index.html', ':scheme', 'http', ':authority', 'localhost',
  'user-agent', 'h2lean/0.1', 'accept', '*/*', 'accept-encoding', 'gzip, deflate',
  'x-request-id', '0123456789abcdef'
];

function cap(list) {
  let n = 0;
  for (let i = 0; i < list.length; i += 2) {
    n += Encoder.maxBytesOf(list[i], list[i + 1]);
  }
  return n;
}

// old: take + concat
function encodeOld(enc, list) {
  const update = enc.takeSizeUpdate();
  const buf = Buffer.allocUnsafe(cap(list));
  enc.unsettled = false;
  let off = 0;
  for (let i = 0; i < list.length; i += 2) {
    off = enc.write(buf, off, list[i], list[i + 1], false);
  }
  return update === null ? buf.subarray(0, off) : Buffer.concat([update, buf.subarray(0, off)]);
}

// new: take first, reserve its length, encode behind it, copy the update to the front
function encodeNew(enc, list) {
  const update = enc.takeSizeUpdate();
  const ul = update === null ? 0 : update.length;
  const buf = Buffer.allocUnsafe(ul + cap(list));
  enc.unsettled = false;
  let off = ul;
  for (let i = 0; i < list.length; i += 2) {
    off = enc.write(buf, off, list[i], list[i + 1], false);
  }
  if (ul === 0) {
    return buf.subarray(0, off);
  }
  update.copy(buf, 0);
  return buf.subarray(0, off);
}

// Two sizes matter: the one an Encoder is constructed with when our cap differs from
// the peer's (pendingSize set at birth, RFC 7541 6.3 has to announce it before the
// first insertion), and the one setPeerTableSize() leaves behind after a SETTINGS.
function round(label, mutate) {
  const a = new Encoder(4096, 4096);
  const b = new Encoder(4096, 4096);
  mutate(a);
  mutate(b);
  const oldBytes = encodeOld(a, LIST);
  const newBytes = encodeNew(b, LIST);
  const same = oldBytes.equals(newBytes);
  console.log(`${label.padEnd(34)} bytes equal: ${same}`
    + `  len(old)=${oldBytes.length} len(new)=${newBytes.length}`);
  console.log('  old:', oldBytes.toString('hex'));
  console.log('  new:', newBytes.toString('hex'));
  const d = new Decoder(4096);
  const out = [];
  let failure = null;
  try {
    d.decode(newBytes, 0, newBytes.length, out);
  } catch (e) {
    failure = e.message;
  }
  const fields = [];
  for (let i = 0; i < out.length; i += 2) {
    fields.push(`${out[i]}=${out[i + 1]}`);
  }
  console.log(failure === null
    ? `  table=${d.size}/${d.maxSize} decoded ${out.length / 2} lines: ${fields.join(' | ')}`
    : `  DECODE FAILED: ${failure}`);
}

round('no update pending', () => {});
round('pending at construction (cap 2048)', (e) => {
  e.maxSize = 2048; e.pendingSize = 2048;
});
round('pending after setPeerTableSize', (e) => e.setPeerTableSize(1024));
round('pending, tiny size (1 byte prefix)', (e) => {
  e.pendingSize = 20;
});
round('pending, size 31 (prefix boundary)', (e) => {
  e.pendingSize = 31;
});
round('pending, size 32 (continuation)', (e) => {
  e.pendingSize = 32;
});
round('pending, huge size (4-byte prefix)', (e) => {
  e.pendingSize = 200000000;
});
console.log(`\nstatic table entries: ${STATIC.length}`);
