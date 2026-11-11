// HPACK codec (RFC 7541) for h2lean.
//
// Deliberate choices, all CPU-driven:
//  * field names are interned, so a repeated name costs no new string and the
//    header object keeps a stable hidden class
//  * the encoder keeps a dynamic table (RFC 7541 2.3.2), so a repeated field line
//    costs one indexed octet on the wire instead of its full literal bytes -- and it
//    inserts a value only once it has seen it twice, so a header that is unique to
//    every response never enters the table and never disturbs the caches
//  * the encoder caches the *encoded bytes* of every (name, value, neverIndex)
//    triple it has produced: a repeated header line becomes a Map lookup plus a
//    memcpy, with no integer or Huffman work at all. Those bytes can name a dynamic
//    index, so the cache is dropped whenever the table changes -- after warm-up the
//    table stops changing and the cache stops being cleared
//  * the cache is keyed in two levels -- the name, then the value -- and not by one
//    composite 'name\0value' string. The composite key would have to be built for
//    every field line of every response and hashed by the Map on the way in
//    (OPTIMIZATION.md item 11). That is what a header block that cannot be cached as
//    a whole costs per field line, and a block cannot be cached whenever the response
//    headers are not the same object twice -- the compat API, and any response that
//    echoes a request id
//  * the two forms that are deliberately NOT cached -- the first sighting of a pair
//    and the insertion that follows it -- are written straight into the destination
//    buffer. They used to be built in a buffer of their own and copied in, which is
//    one allocation and one memcpy per line, and the lines that take those forms are
//    exactly the ones a server with a unique value in every response pays for
//  * the encoded form of a NAME is cached, separately from the pair: a name that is
//    in neither table is written in full, and a server that echoes a request id
//    writes the same name with a new value every response, so the name's octets
//    were Huffman-coded again for every one of those values
//  * the "seen exactly once" set is bounded per name (ONCE_PER_NAME = 64): a name
//    whose values never repeat -- the request id a server echoes in every response --
//    stops being remembered, because the check costs a hash of a fresh value string
//    on every response and the second sighting the set exists for never comes
//
// The encoder therefore holds per-connection state and must not be shared between
// sessions: the table has to match the one the peer's decoder builds from the same
// stream. lib/session.js owns one Encoder per session, exactly as it owns a Decoder.
//
// Where this differs from nghttp2, which is what node:http2 embeds, and why. Read from
// lib/nghttp2_hd.c v1.70.0 (hd_deflate_decide_indexing(), deflate_nv()); the differences
// were decided on measurements in bench/hpack-churn.js, not on preference:
//  * insert on the second sighting (nghttp2 inserts on the first). A value that appears
//    once per response is table poison -- it evicts a stable entry to be referenced by a
//    block that will never use it again. Cost: one response warms up later than node's;
//    in steady state the wire cost matches node's exactly (OPTIMIZATION.md item 11).
//  * a name-based never-index list is NOT copied (:path, etag, location, age,
//    content-length, if-*). An ETag is per-resource, not per-response, so indexing it is
//    the whole point; copying that list measured worse on both axes (OPTIMIZATION.md
//    item 27). The size half of the same rule is copied: nothing is inserted that needs
//    more than three quarters of the table.
//  * REINSERT_MAX: if the working set keeps evicting entries that are still wanted, the
//    session stops inserting altogether. nghttp2 has no such thing because an eviction
//    there costs a memcpy; here it costs both byte caches on every response.
//  * ONCE_PER_NAME: a name that has produced 64 distinct values without any of them
//    coming back stops being remembered, so it never inserts at all. The rule above
//    would have inserted the pair on a second sighting, and for a header that is unique
//    to every response that sighting never comes -- but a name whose values do repeat
//    after 64 distinct ones pays for it: it stays a literal instead of being indexed.
//    The per-line cost of the check: OPTIMIZATION.md item 11.
'use strict';

const { STATIC, HUFF_BITS, HUFF_CODES } = require('./hpack-data');

const STATIC_LEN = STATIC.length;
const EOS = 256;
const MAX_LITERAL = 1 << 20; // hard cap on one field name or value
const CACHE_LIMIT = 8192;

class HPackError extends Error {
  constructor(msg) {
    super(msg);
    this.name = 'HPackError';
  }
}

// ---- static table lookups -----------------------------------------------------
const FULL_INDEX = new Map(); // 'name\0value' -> 1-based static index
const NAME_INDEX = new Map(); // 'name'        -> first 1-based index with that name
// The same lookup in two levels: the name, then the value. The encoder asks it on
// every field line it has to build, and the composite key it would otherwise build
// for the one-level Map is what this table exists to avoid (see the header).
const STATIC_FULL = new Map(); // name -> Map<value, 1-based static index>
for (let i = 0; i < STATIC_LEN; i++) {
  const [n, v] = STATIC[i];
  const k = `${n}\0${v}`;
  if (!FULL_INDEX.has(k)) {
    FULL_INDEX.set(k, i + 1);
  }
  if (!NAME_INDEX.has(n)) {
    NAME_INDEX.set(n, i + 1);
  }
  let inner = STATIC_FULL.get(n);
  if (inner === undefined) {
    inner = new Map();
    STATIC_FULL.set(n, inner);
  }
  if (!inner.has(v)) {
    inner.set(v, i + 1);
  }
}

// ---- Huffman decode trie ------------------------------------------------------
const TR_LEAF = new Int32Array(1 + 257 * 30).fill(-1);
const TR_LEFT = new Int32Array(1 + 257 * 30);
const TR_RIGHT = new Int32Array(1 + 257 * 30);
{
  let nodes = 1;
  for (let s = 0; s <= EOS; s++) {
    let node = 0;
    for (let i = HUFF_BITS[s] - 1; i >= 0; i--) {
      if ((HUFF_CODES[s] >>> i) & 1) {
        if (TR_RIGHT[node] === 0) {
          TR_RIGHT[node] = nodes++;
        }
        node = TR_RIGHT[node];
      } else {
        if (TR_LEFT[node] === 0) {
          TR_LEFT[node] = nodes++;
        }
        node = TR_LEFT[node];
      }
    }
    TR_LEAF[node] = s;
  }
}

// ---- integer codec (RFC 7541 §5.1) --------------------------------------------
// The result lands in RI/RP so that no tuple is allocated per call.
let RI = 0;
let RP = 0;

function readInt(buf, p, bits, end) {
  const mask = (1 << bits) - 1;
  if (p >= end) {
    throw new HPackError('hpack: truncated integer');
  }
  let v = buf[p++] & mask;
  if (v < mask) {
    RI = v; RP = p; return;
  }
  // RFC 7541 5.1: the first continuation octet carries the low 7 bits (weight 1),
  // each following one weighs 128 times more.
  let mul = 1;
  while (p < end) {
    const b = buf[p++];
    v += (b & 0x7f) * mul;
    if (v > 0x7fffffff) {
      throw new HPackError('hpack: integer too large');
    }
    mul *= 128;
    if ((b & 0x80) === 0) {
      RI = v; RP = p; return;
    }
  }
  throw new HPackError('hpack: truncated integer');
}

function writeInt(dst, off, value, bits, firstHigh) {
  const max = (1 << bits) - 1;
  if (value < max) {
    dst[off++] = firstHigh | value;
    return off;
  }
  dst[off++] = firstHigh | max;
  let v = value - max;
  while (v >= 128) {
    dst[off++] = (v & 0x7f) | 0x80;
    v >>>= 7;
  }
  dst[off++] = v;
  return off;
}

// ---- Huffman ------------------------------------------------------------------

function huffBitLen(buf, start, end) {
  let bits = 0;
  for (let i = start; i < end; i++) {
    bits += HUFF_BITS[buf[i]];
  }
  return bits;
}

// The same sum for a string, without building its bytes: latin1 truncates every
// code unit to its low byte, so the byte of a character is its code unit & 0xff.
// Building the bytes first is the allocation this avoids (OPTIMIZATION.md item 12).
function huffBitsOf(s) {
  let bits = 0;
  for (let i = 0; i < s.length; i++) {
    bits += HUFF_BITS[s.charCodeAt(i) & 0xff];
  }
  return bits;
}

// The octets an integer takes in the HPACK integer encoding (RFC 7541 5.1).
function intLen(value, bits) {
  const max = (1 << bits) - 1;
  if (value < max) {
    return 1;
  }
  let v = value - max;
  let n = 2;
  while (v >= 128) {
    v >>>= 7;
    n++;
  }
  return n;
}

// The octets one literal string takes on the wire: its length prefix and the
// string itself, Huffman-coded when that is shorter.
function strLen(s, huffBits) {
  const hlen = (huffBits + 7) >> 3;
  const len = hlen < s.length ? hlen : s.length;
  return intLen(len, 7) + len;
}


const POW2 = new Float64Array(53);
for (let i = 0; i < 53; i++) {
  POW2[i] = 2 ** i;
}

// Bit accumulation must not use 32-bit operators: after adding a 30-bit code the
// buffer can hold up to 37 bits. Doubles are exact below 2^53, so multiply/divide.
function huffEncode(src, start, end, dst, off) {
  let acc = 0;
  let nbits = 0;
  for (let i = start; i < end; i++) {
    const b = HUFF_BITS[src[i]];
    acc = acc * POW2[b] + HUFF_CODES[src[i]];
    nbits += b;
    while (nbits >= 8) {
      nbits -= 8;
      const byte = Math.floor(acc / POW2[nbits]);
      dst[off++] = byte;
      acc -= byte * POW2[nbits];
    }
  }
  // EOS is the all-ones code, so padding the last byte with ones *is* EOS
  if (nbits > 0) {
    const pad = POW2[8 - nbits] - 1;
    dst[off++] = acc * POW2[8 - nbits] + pad;
  }
  return off;
}

// The same encoder for a string: latin1 truncates every code unit to its low byte,
// so the byte of a character is its code unit & 0xff -- the byte the buffer version
// above would have been handed. Coding straight from the string saves the copy into
// a scratch buffer and the view of it that the copy needed (OPTIMIZATION.md item 12).
function huffEncodeStr(s, dst, off) {
  let acc = 0;
  let nbits = 0;
  for (let i = 0; i < s.length; i++) {
    const sym = s.charCodeAt(i) & 0xff;
    const b = HUFF_BITS[sym];
    acc = acc * POW2[b] + HUFF_CODES[sym];
    nbits += b;
    while (nbits >= 8) {
      nbits -= 8;
      const byte = Math.floor(acc / POW2[nbits]);
      dst[off++] = byte;
      acc -= byte * POW2[nbits];
    }
  }
  // EOS is the all-ones code, so padding the last byte with ones *is* EOS
  if (nbits > 0) {
    const pad = POW2[8 - nbits] - 1;
    dst[off++] = acc * POW2[8 - nbits] + pad;
  }
  return off;
}

// The octets of a string into a buffer at `off`: latin1, one byte per code unit.
// Short strings go straight from charCodeAt, long through Buffer#write('latin1')
// -- the encode-side mirror of the latin1() helper below. Threshold and
// measurement: OPTIMIZATION.md item 35.
function latin1Into(dst, off, s) {
  const len = s.length;
  if (len <= RAW_WRITE_MAX) {
    for (let i = 0; i < len; i++) {
      dst[off + i] = s.charCodeAt(i);
    }
  } else {
    dst.write(s, off, 'latin1');
  }
  return off + len;
}

// Returns the decoded byte count, or -1 if the input is not a valid sequence of
// complete codes (an explicit EOS, or a tail that is neither empty nor all ones).
function huffDecode(src, start, end, dst) {
  let node = 0;
  let out = 0;
  let tail = 0;
  for (let i = start; i < end; i++) {
    const byte = src[i];
    for (let k = 7; k >= 0; k--) {
      node = ((byte >>> k) & 1) ? TR_RIGHT[node] : TR_LEFT[node];
      const sym = TR_LEAF[node];
      if (sym !== -1) {
        if (sym === EOS) {
          return -1;
        }
        dst[out++] = sym;
        node = 0;
        tail = 0;
      } else {
        tail++;
      }
    }
  }
  // the unwalked tail is the last `tail` bits of the stream; a valid padding is
  // a prefix of EOS, i.e. at most 7 bits, all of them ones
  if (tail > 7) {
    return -1;
  }
  if (tail > 0 && (src[end - 1] & ((1 << tail) - 1)) !== (1 << tail) - 1) {
    return -1;
  }
  return out;
}

// ---- decoder ------------------------------------------------------------------

// The stand-in for a view past the end of the range: reading from it is a bug
// the range check above catches first.
const EMPTY = Buffer.alloc(0);

// A latin1 string out of a buffer: the octets become code points, which is what
// latin1 decoding is -- the library keeps a field value's octets one to one
// (RFC 9113 8.2). Switch, chunked default and the measurement that shaped
// both: OPTIMIZATION.md item 13.
function latin1(buf, start, end) {
  const len = end - start;
  switch (len) {
    case 0: return '';
    case 1: return String.fromCharCode(buf[start]);
    case 2: return String.fromCharCode(buf[start], buf[start + 1]);
    case 3: return String.fromCharCode(buf[start], buf[start + 1], buf[start + 2]);
    case 4: return String.fromCharCode(buf[start], buf[start + 1], buf[start + 2], buf[start + 3]);
    default: {
      // the rest is at most 8 octets, so the recursion is at most two calls deep
      if (len <= 12) {
        return String.fromCharCode(buf[start], buf[start + 1], buf[start + 2], buf[start + 3])
          + latin1(buf, start + 4, end);
      }
      return buf.toString('latin1', start, end);
    }
  }
}

// A cursor over the octets of a header block that arrived in pieces.
//
// The block is a list of views of the socket chunks it came in, and the octets
// of a field line -- its prefix, the length of a literal, the literal itself --
// may straddle the boundary between two of them. The cursor walks the views in
// order and is the piece-aware twin of the module-level slots the contiguous
// decoder reads into (RI/RP): same rules, same order, same table updates, only
// the place the octets come from is different.
//
// test/hpack.test.js pins the two decoders against each other over the same
// blocks, so the twin cannot drift away from the original.
class PieceCursor {
  // `pieces` are the views in the order they arrived; `start` and `end` are
  // offsets into their concatenation.
  constructor(pieces, start, end) {
    this.pieces = pieces;
    this.i = -1; // the view the cursor is in
    this.o = 0; // the offset inside it
    this.left = end - start; // the octets left in the range
    this.value = 0; // the last integer read
    this.cur = EMPTY;
    this.pieceEnd = 0;
    this.next();
    let skip = start;
    while (skip > 0) {
      const avail = this.pieceEnd - this.o;
      if (skip < avail) {
        this.o += skip; skip = 0;
      } else {
        skip -= avail; this.next();
      }
    }
  }

  // The next view with octets in it. An empty one is skipped: a frame whose
  // payload starts exactly at the end of a read arrives as an empty view and
  // the octets that follow, and the cursor may not sit on an empty one.
  next() {
    this.i++;
    this.o = 0;
    while (this.i < this.pieces.length && this.pieces[this.i].length === 0) {
      this.i++;
    }
    if (this.i >= this.pieces.length) {
      // past the last view: the range is over, and nothing may be read from it
      this.cur = EMPTY;
      this.pieceEnd = 0;
      return;
    }
    this.cur = this.pieces[this.i];
    this.pieceEnd = this.cur.length;
  }

  // The octet at the cursor, without moving it.
  peek() {
    return this.cur[this.o];
  }

  // The octet at the cursor, moving it on.
  byte() {
    const b = this.cur[this.o++];
    this.left--;
    if (this.o === this.pieceEnd) {
      this.next();
    }
    return b;
  }

  // RFC 7541 5.1: the first octet carries `bits` bits of the value, and each
  // continuation octet carries seven bits more, the first of them weighing 1.
  readInt(bits) {
    const mask = (1 << bits) - 1;
    if (this.left <= 0) {
      throw new HPackError('hpack: truncated integer');
    }
    let v = this.byte() & mask;
    if (v < mask) {
      this.value = v; return;
    }
    let mul = 1;
    while (this.left > 0) {
      const b = this.byte();
      v += (b & 0x7f) * mul;
      if (v > 0x7fffffff) {
        throw new HPackError('hpack: integer too large');
      }
      mul *= 128;
      if ((b & 0x80) === 0) {
        this.value = v; return;
      }
    }
    throw new HPackError('hpack: truncated integer');
  }

  // The next `len` octets as a latin1 string. Inside one view: the native
  // conversion and nothing in between. Across several: one conversion per view,
  // which is what a string that a socket read split costs.
  str(len) {
    if (this.o + len <= this.pieceEnd) {
      const whole = latin1(this.cur, this.o, this.o + len);
      this.o += len;
      this.left -= len;
      if (this.o === this.pieceEnd) {
        this.next();
      }
      return whole;
    }
    let s = '';
    let left = len;
    while (left > 0) {
      const avail = this.pieceEnd - this.o;
      if (avail === 0) {
        throw new HPackError('hpack: truncated literal');
      }
      const take = avail < left ? avail : left;
      s += latin1(this.cur, this.o, this.o + take);
      this.o += take;
      this.left -= take;
      left -= take;
      if (this.o === this.pieceEnd) {
        this.next();
      }
    }
    return s;
  }
}

// The Huffman decoder over a run of octets that arrived in pieces: the same
// walk as huffDecode(), with the octets coming from the cursor.
function huffDecodePieces(cur, len, dst) {
  let node = 0;
  let out = 0;
  let tail = 0;
  let last = 0;
  for (let i = 0; i < len; i++) {
    const byte = cur.byte();
    last = byte;
    for (let k = 7; k >= 0; k--) {
      node = ((byte >>> k) & 1) ? TR_RIGHT[node] : TR_LEFT[node];
      const sym = TR_LEAF[node];
      if (sym !== -1) {
        if (sym === EOS) {
          return -1;
        }
        dst[out++] = sym;
        node = 0;
        tail = 0;
      } else {
        tail++;
      }
    }
  }
  // the unwalked tail is the last `tail` bits of the stream; a valid padding is
  // a prefix of EOS, i.e. at most 7 bits, all of them ones
  if (tail > 7) {
    return -1;
  }
  if (tail > 0 && (last & ((1 << tail) - 1)) !== (1 << tail) - 1) {
    return -1;
  }
  return out;
}

class Decoder {
  // `capacity` = what we advertised in our SETTINGS_HEADER_TABLE_SIZE
  constructor(capacity = 4096) {
    this.capacity = capacity;
    this.maxSize = capacity;
    this.size = 0;
    this.dyn = []; // oldest first; entries below `base` are evicted
    this.base = 0;
    this.epoch = 0; // bumped on every table change (see §4.4, 4.3)
    this.names = new Map(); // interned field names
    this.scratch = Buffer.allocUnsafe(4096);
  }

  // HPACK index (1-based, newest dynamic entry first) -> [name, value]
  at(index) {
    if (index >= 1 && index <= STATIC_LEN) {
      return STATIC[index - 1];
    }
    const j = index - STATIC_LEN - 1;
    if (j < 0 || j >= this.dyn.length - this.base) {
      return undefined;
    }
    return this.dyn[this.dyn.length - 1 - j];
  }

  insert(name, value) {
    this.epoch++;
    const sz = name.length + value.length + 32;
    if (sz > this.maxSize) { // RFC 7541 §4.4: table ends up empty
      this.dyn.length = this.base = 0;
      this.size = 0;
      return;
    }
    this.dyn.push([name, value]);
    this.size += sz;
    while (this.size > this.maxSize && this.dyn.length > this.base + 1) {
      const old = this.dyn[this.base++];
      this.size -= old[0].length + old[1].length + 32;
    }
    if (this.base > 64) { // amortised compaction
      this.dyn.copyWithin(0, this.base);
      this.dyn.length -= this.base;
      this.base = 0;
      this.size = 0;
      for (const e of this.dyn) {
        this.size += e[0].length + e[1].length + 32;
      }
    }
  }

  setMaxSize(n) {
    if (n > this.capacity) {
      throw new HPackError('hpack: table size update exceeds advertised capacity');
    }
    if (this.maxSize !== n) {
      this.epoch++;
    }
    this.maxSize = n;
    while (this.size > n && this.dyn.length > this.base) {
      const old = this.dyn[this.base++];
      this.size -= old[0].length + old[1].length + 32;
    }
  }

  // Refills the reusable flat array `out` with name, value, name, value, ...
  // and returns the number of field lines.
  decode(buf, start, end, out) {
    out.length = 0;
    let p = start;
    while (p < end) {
      const b = buf[p];
      if (b & 0x80) { // 1xxxxxxx  indexed
        readInt(buf, p, 7, end);
        const e = this.at(RI);
        if (e === undefined) {
          throw new HPackError(`hpack: invalid index ${RI}`);
        }
        out.push(e[0], e[1]);
        p = RP;
      } else if ((b & 0xc0) === 0x40) { // 01xxxxxx  incremental
        p = this.literal(buf, p, end, out, 6);
        this.insert(out[out.length - 2], out[out.length - 1]);
      } else if ((b & 0xe0) === 0x20) { // 001xxxxx  size update
        // RFC 7541 6.3: a size update may only appear at the beginning of a header block --
        // before its first field line, and `out.length === 0` is exactly that. Measured on node
        // (h2spec hpack/4.2), one that follows a field line is GOAWAY(COMPRESSION_ERROR) whether
        // or not it changes anything: the indices of the lines already decoded name a table that
        // the update says was different, so a second decoder that reads the block in the order
        // the octets arrive would rebuild a different context from ours.
        if (out.length !== 0) {
          throw new HPackError('hpack: table size update after a field line');
        }
        readInt(buf, p, 5, end);
        this.setMaxSize(RI);
        p = RP;
      } else { // 0000/0001 literal
        p = this.literal(buf, p, end, out, 4);
      }
    }
    return out.length >> 1;
  }

  literal(buf, p, end, out, bits) {
    readInt(buf, p, bits, end);
    let name;
    if (RI === 0) {
      const s = this.str(buf, RP, end);
      name = this.intern(s.value);
      p = s.p;
    } else {
      const e = this.at(RI);
      if (e === undefined) {
        throw new HPackError(`hpack: invalid name index ${RI}`);
      }
      name = e[0];
      p = RP;
    }
    const v = this.str(buf, p, end);
    out.push(name, v.value);
    return v.p;
  }

  str(buf, p, end) {
    if (p >= end) {
      throw new HPackError('hpack: truncated string');
    }
    const huff = (buf[p] & 0x80) !== 0;
    readInt(buf, p, 7, end);
    const len = RI;
    p = RP;
    if (len > MAX_LITERAL) {
      throw new HPackError('hpack: literal too long');
    }
    if (p + len > end) {
      throw new HPackError('hpack: truncated literal');
    }
    if (!huff) {
      return { value: latin1(buf, p, p + len), p: p + len };
    }
    // the shortest code is 5 bits, so `len` encoded bytes decode to <= len*8/5
    const need = (len * 8) / 5 + 8 | 0;
    if (this.scratch.length < need) {
      this.scratch = Buffer.allocUnsafe(need);
    }
    const n = huffDecode(buf, p, p + len, this.scratch);
    if (n < 0) {
      throw new HPackError('hpack: invalid huffman encoding');
    }
    return { value: latin1(this.scratch, 0, n), p: p + len };
  }

  // The same decoder over a block that arrived in pieces: the octets are read
  // through a cursor that walks the views the block arrived in, instead of out
  // of one buffer. The rules, the order and the table updates are the ones
  // decode() applies -- this is the same decoder, not a second set of rules.
  //
  // Only a block that a socket read split takes this path. A block that arrived
  // whole is decoded out of the chunk it came in, with no cursor and no copy.
  decodePieces(pieces, start, end, out) {
    out.length = 0;
    const cur = new PieceCursor(pieces, start, end);
    while (cur.left > 0) {
      const b = cur.peek();
      if (b & 0x80) { // 1xxxxxxx  indexed
        cur.readInt(7);
        const e = this.at(cur.value);
        if (e === undefined) {
          throw new HPackError(`hpack: invalid index ${cur.value}`);
        }
        out.push(e[0], e[1]);
      } else if ((b & 0xc0) === 0x40) { // 01xxxxxx  incremental
        this.literalPieces(cur, out, 6);
        this.insert(out[out.length - 2], out[out.length - 1]);
      } else if ((b & 0xe0) === 0x20) { // 001xxxxx  size update
        // RFC 7541 6.3: a size update may only appear at the beginning of a header
        // block -- before its first field line. See decode() for the measurement.
        if (out.length !== 0) {
          throw new HPackError('hpack: table size update after a field line');
        }
        cur.readInt(5);
        this.setMaxSize(cur.value);
      } else { // 0000/0001 literal
        this.literalPieces(cur, out, 4);
      }
    }
    return out.length >> 1;
  }

  literalPieces(cur, out, bits) {
    cur.readInt(bits);
    let name;
    if (cur.value === 0) {
      name = this.intern(this.strPieces(cur));
    } else {
      const e = this.at(cur.value);
      if (e === undefined) {
        throw new HPackError(`hpack: invalid name index ${cur.value}`);
      }
      name = e[0];
    }
    const v = this.strPieces(cur);
    out.push(name, v);
  }

  // A literal string out of the views: the length prefix, then the octets. The
  // prefix's top bit says whether the octets are Huffman-coded.
  strPieces(cur) {
    const huff = (cur.peek() & 0x80) !== 0;
    cur.readInt(7);
    const len = cur.value;
    if (len > MAX_LITERAL) {
      throw new HPackError('hpack: literal too long');
    }
    if (cur.left < len) {
      throw new HPackError('hpack: truncated literal');
    }
    if (!huff) {
      return cur.str(len);
    }
    // the shortest code is 5 bits, so `len` encoded octets decode to <= len*8/5
    const need = (len * 8) / 5 + 8 | 0;
    if (this.scratch.length < need) {
      this.scratch = Buffer.allocUnsafe(need);
    }
    const n = huffDecodePieces(cur, len, this.scratch);
    if (n < 0) {
      throw new HPackError('hpack: invalid huffman encoding');
    }
    return latin1(this.scratch, 0, n);
  }

  intern(name) {
    const hit = this.names.get(name);
    if (hit !== undefined) {
      return hit;
    }
    if (this.names.size > 4096) {
      this.names.clear();
    }
    this.names.set(name, name);
    return name;
  }
}

// ---- encoder ------------------------------------------------------------------

// Names whose values are not to be remembered in a table on either side: the five that
// RFC 7541 7.1.3 names and its A.2 example list carries -- credentials and session
// tokens. The application's own sensitiveHeaders path (the neverIndex flag, 0x10) is the
// per-call mechanism; this is the default for the headers an application did not mark.
//
// nghttp2 refuses to index a longer list (lib/nghttp2_hd.c v1.70.0,
// hd_deflate_decide_indexing(): :path, age, content-length, etag, if-modified-since,
// if-none-match, location, set-cookie) on the theory that a per-response value is not
// worth a table entry. That list was tried here, against this encoder's second-sighting
// rule, and it lost on both axes: an ETag is per-*resource*, so it repeats, and
// indexing it is what makes a cached response cheap (OPTIMIZATION.md item 27). The
// churn the name list protects against is already handled by REINSERT_MAX below, which
// reacts to the eviction pattern instead of guessing from the name. Only the size half
// of that rule is kept (see compile()).
const NEVER_INSERT = new Set(['set-cookie', 'www-authenticate', 'proxy-authenticate',
  'authorization', 'cookie']);

// How often a table entry has to come back after being evicted before this session
// gives up on its dynamic table. See insert().
const REINSERT_MAX = 8;

// Raw literal strings at or below this length are written straight from charCodeAt
// instead of through Buffer#write('latin1'); see latin1Into() and OPTIMIZATION.md
// item 35 for the measurement that set the threshold.
const RAW_WRITE_MAX = 16;

// How many distinct values one name may have before the encoder stops remembering
// them. The "seen exactly once" set is what makes the second-sighting rule work,
// and the set is not free: the value is a fresh string, so every check hashes it
// and every insert grows a Set that reaches 8192 entries per session. A name whose
// values never repeat -- the request id a server echoes in every response -- pays
// that for every response and gets nothing for it, because a pair that is
// remembered here is a pair that will be inserted on its second sighting, and that
// second sighting never comes.
//
// Past this many distinct values the name is treated as one whose values are
// unique to the response: the check is skipped and the line stays a literal.
// A name whose values do repeat keeps its entries -- the set holds the values seen
// exactly once, and a value that comes back is removed from it -- so the bound only
// bites on a name that has already produced 64 values nobody asked for twice
// (the cost of the check: OPTIMIZATION.md item 11).
const ONCE_PER_NAME = 64;

// The encoded form of a field name: the length prefix and the octets.
//
// A name is written in full whenever it is in neither the static nor the dynamic
// table, and a response whose header block cannot be cached as a whole pays that
// for every response -- a server that echoes a request id writes the same name
// with a new value every time. Huffman-coding the name again for every one of
// those values is the waste this cache removes (OPTIMIZATION.md item 11).
//
// The bytes depend on the name alone -- the length prefix and the octets, not the
// representation prefix in front of them -- so the cache is process-wide and not
// per session. Bounded like VALID_NAMES in lib/headers.js: the names come from
// the application, and a name that is new to a full cache is encoded as before.
const NAME_BYTES = new Map();
const NAME_BYTES_MAX = 1024;

// The name's own encoding, the same octets literalStrInto() would write.
function nameBytes(name) {
  let b = NAME_BYTES.get(name);
  if (b === undefined) {
    const huffBits = huffBitsOf(name);
    const hlen = (huffBits + 7) >> 3;
    const huff = hlen < name.length;
    const len = huff ? hlen : name.length;
    b = Buffer.allocUnsafe(intLen(len, 7) + len);
    const off = writeInt(b, 0, len, 7, huff ? 0x80 : 0x00);
    if (huff) {
      huffEncodeStr(name, b, off);
    } else {
      b.write(name, off, 'latin1');
    }
    if (NAME_BYTES.size < NAME_BYTES_MAX) {
      NAME_BYTES.set(name, b);
    }
  }
  return b;
}

// The size of the name cache, for the test that pins the bound.
function nameBytesSize() {
  return NAME_BYTES.size;
}

// The shape of a literal, in module-level slots: the name it uses (an index, or 0
// for the full name), the Huffman bits of its two strings, and the octets the whole
// line takes. Asking for it costs no allocation, which is why it is not an object.
let L_BITS = 0;
let L_NAME_IDX = 0;
let L_NAME_BYTES = null;
let L_VALUE_BITS = 0;
let L_NEED = 0;

class Encoder {
  // `peerSize` is the peer's SETTINGS_HEADER_TABLE_SIZE -- the size it assumes the table
  // we build for it has -- and `cap` is the ceiling we are willing to spend on it. The
  // two are normally the same number, and when they are not we have to announce ours, or
  // the peer would evict at a different point than we do and the tables would drift.
  constructor(peerSize = 4096, cap = peerSize) {
    this.cap = cap;
    this.peerSize = peerSize;
    this.maxSize = Math.min(peerSize, cap);
    this.size = 0;
    this.dyn = []; // oldest first; everything below `base` is evicted
    this.base = 0;
    this.epoch = 0; // bumped on every change the peer's table mirrors
    this.pendingSize = this.maxSize === peerSize ? null : this.maxSize;
    // The encoded bytes of a field line, in two levels: the name, then the value.
    // The never-indexed form is a map of its own, because the same pair can be sent
    // both ways and the two forms are different bytes.
    this.cache = new Map(); // name -> Map<value, Buffer>
    this.cacheNever = new Map(); // name -> Map<value, Buffer>
    this.cacheCount = 0; // field lines in both maps
    // The pairs seen exactly once: not worth an insertion yet. And the pairs whose
    // entry left the table, oldest signal first. Both in two levels, for the same
    // reason as the byte cache above.
    this.once = new Map(); // name -> Set<value>
    this.onceCount = 0;
    this.evicted = new Map(); // name -> Set<value>
    this.evictedCount = 0;
    this.reinserts = 0; // of them, how often we had to put one back
    this.noInsert = false; // the working set does not fit; stop inserting
    this.unsettled = false; // set by a line whose form will change next time
  }

  // Newest dynamic index holding exactly this pair, 0 when absent. Only reached on a
  // byte-cache miss, so the scan of a table of at most ~100 entries is cold-path work.
  findFull(name, value) {
    for (let k = this.dyn.length - 1; k >= this.base; k--) {
      const e = this.dyn[k];
      if (e[0] === name && e[1] === value) {
        return STATIC_LEN + this.dyn.length - k;
      }
    }
    return 0;
  }

  findName(name) {
    for (let k = this.dyn.length - 1; k >= this.base; k--) {
      if (this.dyn[k][0] === name) {
        return STATIC_LEN + this.dyn.length - k;
      }
    }
    return 0;
  }

  // Adds an entry and evicts to fit. Every cached byte sequence may name an index that
  // just moved, so the cache cannot survive the change -- which is why insertion is
  // reserved for values that repeat.
  insert(name, value) {
    const sz = name.length + value.length + 32;
    let dropped = 0;
    if (sz > this.maxSize) { // RFC 7541 4.4: the table ends up empty
      this.rememberEvicted(this.dyn, this.base, this.dyn.length);
      this.dyn.length = this.base = 0;
      this.size = 0;
    } else {
      this.dyn.push([name, value]);
      this.size += sz;
      const from = this.base;
      while (this.size > this.maxSize && this.dyn.length > this.base + 1) {
        const old = this.dyn[this.base++];
        this.size -= old[0].length + old[1].length + 32;
        dropped++;
      }
      if (dropped > 0) {
        this.rememberEvicted(this.dyn, from, this.base);
      }
      if (this.base > 64) { // amortised compaction
        this.dyn.copyWithin(0, this.base);
        this.dyn.length -= this.base;
        this.base = 0;
        this.size = 0;
        for (const e of this.dyn) {
          this.size += e[0].length + e[1].length + 32;
        }
      }
    }
    this.touch();
    // The signal that matters is not the number of evictions but whether an evicted
    // entry was still wanted: if the same pair comes back and has to be inserted again,
    // the working set is bigger than the table. A workload that fits never re-inserts
    // anything, so this cannot fire during its warm-up.
    if (this.evictedDelete(name, value) && ++this.reinserts >= REINSERT_MAX) {
      // Measured (OPTIMIZATION.md item 11): a working set that
      // keeps overflowing the table spends a multiple of the no-table cost on every
      // block, because each eviction invalidates both byte caches -- and inserting stops
      // returns it within a few hundred responses. Nothing has to be announced for
      // that: the table keeps what is in it, stops moving, and so do the caches --
      // the peer's decoder sees exactly the same indices, because we never touch
      // them again.
      this.noInsert = true;
    }
  }

  rememberEvicted(dyn, from, to) {
    if (this.evictedCount >= CACHE_LIMIT) {
      this.evicted.clear();
      this.evictedCount = 0;
    }
    for (let k = from; k < to; k++) {
      const e = dyn[k];
      let inner = this.evicted.get(e[0]);
      if (inner === undefined) {
        inner = new Set();
        this.evicted.set(e[0], inner);
      }
      if (!inner.has(e[1])) {
        inner.add(e[1]);
        this.evictedCount++;
      }
    }
  }

  // Whether this pair was evicted before, and forgets it: it is back, so the entry
  // it had is wanted again.
  evictedDelete(name, value) {
    const inner = this.evicted.get(name);
    if (inner === undefined) {
      return false;
    }
    const had = inner.delete(value);
    if (had) {
      this.evictedCount--;
    }
    if (inner.size === 0) {
      this.evicted.delete(name);
    }
    return had;
  }

  // The peer changed the table size it advertises for its own decoder, which is the
  // bound on ours. Any change has to be restated to it with a size update -- that is what
  // nghttp2 does on receiving this setting -- because its decompression context is sized
  // by that number and it has no other way of learning where ours ended up. An unchanged
  // value, which is every normal SETTINGS, costs nothing.
  setPeerTableSize(peerSize) {
    const want = Math.min(peerSize, this.cap);
    if (peerSize !== this.peerSize || want !== this.maxSize) {
      this.pendingSize = want;
    }
    this.peerSize = peerSize;
    if (want === this.maxSize) {
      return;
    }
    this.maxSize = want;
    let moved = false;
    while (this.size > want && this.dyn.length > this.base) {
      const old = this.dyn[this.base++];
      this.size -= old[0].length + old[1].length + 32;
      moved = true;
    }
    if (moved) {
      this.touch();
    } // the indices the caches name just shifted
  }

  // The size update octets for the next field block, or null. Consumed here so a block
  // never announces the same change twice.
  takeSizeUpdate() {
    const n = this.pendingSize;
    if (n === null) {
      return null;
    }
    this.pendingSize = null;
    const b = Buffer.allocUnsafe(5);
    return b.subarray(0, writeInt(b, 0, n, 5, 0x20));
  }

  touch() {
    this.epoch++;
    this.cache.clear();
    this.cacheNever.clear();
    this.cacheCount = 0;
  }

  // The bytes of one field line, or undefined when they have to be built. Two
  // lookups, no key to build and no key to hash.
  lookup(name, value, neverIndex) {
    const inner = (neverIndex ? this.cacheNever : this.cache).get(name);
    return inner === undefined ? undefined : inner.get(value);
  }

  // Appends one field line to `dst` at `off`, returns the new offset.
  // Cached bytes are only valid for the table state that produced them: see touch().
  write(dst, off, name, value, neverIndex) {
    const bytes = this.lookup(name, value, neverIndex);
    if (bytes !== undefined) {
      return this.put(dst, off, bytes);
    }
    return this.emit(dst, off, name, value, neverIndex);
  }

  // Copies bytes into the destination and returns the new offset. The check belongs
  // here because the destination is a shared scratch block sized by an upper bound,
  // so a line that does not fit is a bug in that bound, not in the peer.
  put(dst, off, bytes) {
    if (off + bytes.length > dst.length) {
      throw new HPackError('hpack: output buffer too small');
    }
    // set() against copy(): the same memcpy with less argument juggling, and the
    // difference is real at the size of a field line (OPTIMIZATION.md item 12).
    dst.set(bytes, off);
    return off + bytes.length;
  }

  // One field line, cold: choose the representation, and the table's fate with it.
  // Appends the line to `dst` at `off` and returns the new offset. The forms that are
  // cached (table-independent, or valid for this epoch) are built in a buffer of their
  // own and copied in; the two that are deliberately not cached -- the first sighting
  // of a pair, and the insertion that follows it -- are written straight into `dst`,
  // so a line whose value is new to the response costs no buffer at all.
  emit(dst, off, name, value, neverIndex) {
    // 11xxxxxx: the value is named in full and neither table stores it. Emitted as a
    // literal even when the pair is in the static table, which is what node's
    // nghttp2 does for sensitive values too -- the point is that nothing is remembered.
    if (neverIndex) {
      const bytes = this.literal(name, value, 0x10);
      this.remember(name, value, neverIndex, bytes);
      return this.put(dst, off, bytes);
    }
    const hit = this.fullIndex(name, value);
    if (hit !== undefined) {
      const bytes = this.indexed(hit);
      this.remember(name, value, neverIndex, bytes);
      return this.put(dst, off, bytes);
    }
    // Its name can never reach the table -- insert() is only reached past this line --
    // so the bytes reference nothing but static indices and are safe to cache despite
    // being literals.
    if (NEVER_INSERT.has(name)) {
      const bytes = this.literal(name, value, 0x00);
      this.remember(name, value, neverIndex, bytes);
      return this.put(dst, off, bytes);
    }
    // Nothing may be added to the table: either the peer has no room for it (inserting
    // would still be correct on the wire, since both tables empty at once, but it would
    // bump the epoch and clear the caches on every response), or this session gave up on
    // its table because its working set does not fit. Entries already in it keep their
    // one-octet references above -- only new ones stop. From here the table stands still,
    // so these bytes, which may name a dynamic *name* index, stay valid for the session.
    if (this.maxSize === 0 || this.noInsert) {
      const bytes = this.literal(name, value, 0x00);
      this.remember(name, value, neverIndex, bytes);
      return this.put(dst, off, bytes);
    }
    // One more rule taken from hd_deflate_decide_indexing(): an entry bigger than three
    // quarters of the table is not inserted at all. It would evict nearly the whole table
    // to make room for itself, and a single reference is worth less than the stable
    // entries it displaces -- which is the same reasoning as NEVER_INSERT above, applied
    // by size instead of by name.
    if (name.length + value.length + 32 > this.maxSize - (this.maxSize >> 2)) {
      const bytes = this.literal(name, value, 0x00);
      this.remember(name, value, neverIndex, bytes);
      return this.put(dst, off, bytes);
    }
    // Insert on the second sighting. The first one pays a literal and is only
    // remembered, so a value that is unique to every response never enters the table,
    // never evicts, and never clears the cache. Not cached: caching it here is what
    // would keep it a literal forever. Written straight into the destination, because
    // this line is thrown away with the response anyway.
    if (this.onceCount >= CACHE_LIMIT) {
      this.once.clear();
      this.onceCount = 0;
    }
    let seen = this.once.get(name);
    if (seen === undefined) {
      seen = new Set();
      this.once.set(name, seen);
    }
    if (seen.size >= ONCE_PER_NAME) {
      // This name has produced ONCE_PER_NAME distinct values and none of them came
      // back, so its values are unique to the response. Stop remembering them: the
      // check is the cost (the hash of a fresh value string) and the pair would not
      // be inserted on a second sighting anyway -- that is the rule this name wants.
      this.unsettled = true;
      return this.literalInto(dst, off, name, value, 0x00);
    }
    if (!seen.has(value)) {
      seen.add(value);
      this.onceCount++;
      this.unsettled = true;
      return this.literalInto(dst, off, name, value, 0x00);
    }
    seen.delete(value);
    this.onceCount--;
    // 01xxxxxx: both tables insert. The octets name the table as it is now, so they
    // are written before insert() moves it.
    const end = this.literalInto(dst, off, name, value, 0x40);
    this.insert(name, value);
    return end; // not cached, on purpose
  }

  // Static table first, then ours: the index a peer can resolve without any literal.
  fullIndex(name, value) {
    const inner = STATIC_FULL.get(name);
    const stat = inner === undefined ? undefined : inner.get(value);
    if (stat !== undefined) {
      return stat;
    }
    const dyn = this.findFull(name, value);
    return dyn === 0 ? undefined : dyn;
  }

  remember(name, value, neverIndex, bytes) {
    if (bytes === undefined) {
      return bytes;
    }
    const cache = neverIndex ? this.cacheNever : this.cache;
    let inner = cache.get(name);
    if (inner === undefined) {
      inner = new Map();
      cache.set(name, inner);
    }
    if (!inner.has(value)) {
      inner.set(value, bytes);
      if (++this.cacheCount >= CACHE_LIMIT) {
        this.cache.clear();
        this.cacheNever.clear();
        this.cacheCount = 0;
      }
    }
    return bytes;
  }


  indexed(index) {
    const b = Buffer.allocUnsafe(4);
    return b.subarray(0, writeInt(b, 0, index, 7, 0x80));
  }

  // prefix 0x00 = without indexing (4-bit name field), 0x10 = never indexed,
  // 0x40 = with incremental indexing (6-bit name field). A never-indexed line names
  // its field in full as well: nothing about the field may be left in a table.
  literal(name, value, prefix) {
    this.plan(name, value, prefix);
    const out = Buffer.allocUnsafe(L_NEED);
    let off = writeInt(out, 0, L_NAME_IDX, L_BITS, prefix);
    if (L_NAME_IDX === 0) {
      out.set(L_NAME_BYTES, off);
      off += L_NAME_BYTES.length;
    }
    this.literalStrInto(out, off, value, L_VALUE_BITS);
    return out; // the buffer is exactly as long as the line
  }

  // The same line, written straight into the destination: no buffer of the line's
  // own, and the size is known before a single octet is written, so the destination
  // can be checked first. Used for the two forms that are not cached.
  literalInto(dst, off, name, value, prefix) {
    this.plan(name, value, prefix);
    if (off + L_NEED > dst.length) {
      throw new HPackError('hpack: output buffer too small');
    }
    let o = writeInt(dst, off, L_NAME_IDX, L_BITS, prefix);
    if (L_NAME_IDX === 0) {
      dst.set(L_NAME_BYTES, o);
      o += L_NAME_BYTES.length;
    }
    o = this.literalStrInto(dst, o, value, L_VALUE_BITS);
    return o;
  }

  // The shape of the line, in the module-level slots above.
  plan(name, value, prefix) {
    L_BITS = prefix === 0x40 ? 6 : 4;
    let nameIdx = 0;
    if (prefix !== 0x10) {
      const stat = NAME_INDEX.get(name);
      nameIdx = stat !== undefined ? stat : this.findName(name);
    }
    L_NAME_IDX = nameIdx;
    L_VALUE_BITS = huffBitsOf(value);
    if (nameIdx === 0) {
      // the name is written in full: its octets are the cached ones
      L_NAME_BYTES = nameBytes(name);
      L_NEED = intLen(0, L_BITS) + L_NAME_BYTES.length + strLen(value, L_VALUE_BITS);
    } else {
      L_NAME_BYTES = null;
      L_NEED = intLen(nameIdx, L_BITS) + strLen(value, L_VALUE_BITS);
    }
  }

  // One literal string: its length prefix and the string itself, Huffman-coded when
  // that is shorter. The octets of a string are its latin1 bytes -- one per code
  // unit, the low byte of each -- so the Huffman form is coded straight from the
  // string and the raw form goes through latin1Into().
  literalStrInto(dst, off, s, huffBits) {
    const rawLen = s.length;
    const hlen = (huffBits + 7) >> 3;
    if (hlen < rawLen) { // Huffman wins
      off = writeInt(dst, off, hlen, 7, 0x80);
      return huffEncodeStr(s, dst, off);
    }
    off = writeInt(dst, off, rawLen, 7, 0x00);
    return latin1Into(dst, off, s);
  }


  sizeUpdate(dst, off, size) {
    return writeInt(dst, off, size, 5, 0x20);
  }

  // Upper bound on the bytes one field line can produce, for capacity planning.
  static maxBytesOf(name, value) {
    return 1 + 4 + (name.length + 5) * 2 + (value.length + 5) * 2;
  }
}

module.exports = {
  Decoder,
  Encoder,
  HPackError,
  STATIC,
  FULL_INDEX,
  NAME_INDEX,
  nameBytesSize,
  huffEncode,
  huffEncodeStr,
  huffDecode,
  huffBitLen,
  writeInt,
  readInt
};
