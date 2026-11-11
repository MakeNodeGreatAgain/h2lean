// A measurement arm, not a feature: puts lib/hpack's Encoder back to the behaviour from
// before the dynamic table existed -- a static index when the pair is known, a literal
// otherwise, nothing ever inserted, every field line cached from its first use. Run the
// server through it to get a before/after pair from the same server script:
//
//   PRELOAD=bench/no-table.js node bench/h2load.js --server=h2lean --api=stream
'use strict';
const path = require('node:path');
const hpack = require(path.join(__dirname, '..', 'lib', 'hpack'));

// The encoder's cold path: the decision that picks a form for a field line. It is
// given the destination and writes into it, so the arm below writes into it too.
hpack.Encoder.prototype.emit = function (dst, off, name, value, neverIndex) {
  const full = neverIndex ? undefined : hpack.FULL_INDEX.get(`${name}\0${value}`);
  let out;
  if (full !== undefined) {
    const b = Buffer.allocUnsafe(4);
    out = b.subarray(0, hpack.writeInt(b, 0, full, 7, 0x80));
  } else {
    out = this.literal(name, value, neverIndex ? 0x10 : 0x00);
  }
  this.remember(name, value, neverIndex, out);
  return this.put(dst, off, out);
};
// So a bench script can say which arm it is in without guessing from the environment.
hpack.noTable = true;
