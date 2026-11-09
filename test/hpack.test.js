// Unit tests for the HPACK codec: RFC vectors, round-trips, table mechanics.
const test = require('node:test');
const assert = require('node:assert');
const {
  Decoder, Encoder, huffEncode, huffEncodeStr, huffDecode, huffBitLen, HPackError, STATIC,
  nameBytesSize
} = require('../lib/hpack');

test('huffman: encoding of www.example.com matches RFC 7541 C.3', () => {
  const src = Buffer.from('www.example.com', 'latin1');
  const dst = Buffer.allocUnsafe(64);
  const n = huffEncode(src, 0, src.length, dst, 0);
  assert.strictEqual(dst.subarray(0, n).toString('hex'), 'f1e3c2e5f23a6ba0ab90f4ff');
});

test('huffman: decode is the exact inverse of encode', () => {
  const enc = Buffer.allocUnsafe(4096);
  const dec = Buffer.allocUnsafe(4096);
  const cases = ['', 'a', '/s', '309', 'gzip', 'Mozilla/5.0 (Macintosh)',
    'text/plain;charset=iso-8859-1', 'a'.repeat(300)];
  for (const s of cases) {
    const src = Buffer.from(s, 'latin1');
    const n = huffEncode(src, 0, src.length, enc, 0);
    assert.strictEqual(huffDecode(enc, 0, n, dec), src.length, `case ${JSON.stringify(s)}`);
    assert.strictEqual(dec.toString('latin1', 0, src.length), s);
  }
});

test('huffman: all 256 octets round-trip', () => {
  const src = Buffer.allocUnsafe(256);
  for (let i = 0; i < 256; i++) {
    src[i] = i;
  }
  const enc = Buffer.allocUnsafe(2048);
  const n = huffEncode(src, 0, 256, enc, 0);
  const dec = Buffer.allocUnsafe(2048);
  assert.strictEqual(huffDecode(enc, 0, n, dec), 256);
  assert.deepStrictEqual([...dec.subarray(0, 256)], [...src]);
});

test('huffman: a tail that is not all ones is rejected', () => {
  const src = Buffer.from('a', 'latin1');
  const enc = Buffer.allocUnsafe(8);
  const n = huffEncode(src, 0, src.length, enc, 0);
  assert.strictEqual(n, 1);
  const tail = (8 - huffBitLen(src, 0, src.length) % 8) % 8; // padding width
  assert.ok(tail > 0, 'this code needs padding');
  const good = Buffer.allocUnsafe(8);
  assert.strictEqual(huffDecode(enc, 0, n, good), 1);
  const bad = Buffer.from([enc[0] & ~((1 << tail) - 1)]); // break the ones
  assert.strictEqual(huffDecode(bad, 0, 1, Buffer.allocUnsafe(8)), -1);
});

test('huffman: the string encoder and the buffer encoder agree', () => {
  // The encoder codes a literal straight from the string (huffEncodeStr), while the
  // tests and the tools use the form that takes a buffer (huffEncode). One table, two
  // implementations: they have to stay in step, so they are compared here over the
  // shapes a header value can take -- empty, longer than 127 octets, and outside ASCII.
  const fromBuf = Buffer.allocUnsafe(65536);
  const fromStr = Buffer.allocUnsafe(65536);
  const cases = ['', 'a', 'x'.repeat(300), 'www.example.com', 'text/plain;charset=utf-8',
    'caf\u00e9', '\u00ff\u00fe\u00fd', '\u{1f600}', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    '0123456789abcdef0123456789abcdef'];
  for (const s of cases) {
    const src = Buffer.from(s, 'latin1');
    const a = huffEncode(src, 0, src.length, fromBuf, 0);
    const b = huffEncodeStr(s, fromStr, 0);
    assert.strictEqual(b, a, `length differs for ${JSON.stringify(s)}`);
    assert.ok(fromBuf.subarray(0, a).equals(fromStr.subarray(0, b)),
      `bytes differ for ${JSON.stringify(s)}`);
  }
});

test('integer codec: multi-byte length prefixes, huffman and raw branches', () => {
  const enc = new Encoder();
  const dec = new Decoder(4096);
  const dst = Buffer.allocUnsafe(4096);
  // 'x' has a 6-bit code: huffman wins and the encoded length exceeds 127,
  // so the 7-bit string prefix needs a continuation byte.
  const huffCase = 'x'.repeat(300);
  // NUL has a 13-bit code: huffman loses, so the raw branch is taken, again
  // with a length above 127.
  const rawCase = '\0'.repeat(130);
  for (const value of [huffCase, rawCase]) {
    const off = enc.write(dst, 0, 'x-long-name', value, false);
    const out = [];
    dec.decode(dst, 0, off, out);
    assert.strictEqual(out[0], 'x-long-name');
    assert.strictEqual(out[1], value);
  }
});

test('integer codec: round-trips at every prefix boundary', () => {
  const enc = new Encoder();
  const dec = new Decoder(4096);
  const dst = Buffer.allocUnsafe(64 * 1024);
  // NUL values keep the encoder on the raw branch, so the declared length is
  // exactly what the integer codec has to encode.
  for (const len of [1, 126, 127, 128, 129, 254, 255, 256, 16383, 16384, 20000]) {
    const value = '\0'.repeat(len);
    const off = enc.write(dst, 0, 'n', value, false);
    const out = [];
    dec.decode(dst, 0, off, out);
    assert.strictEqual(out[1].length, len, `length ${len}`);
  }
});

test('encoder: static full match becomes a one-byte indexed line', () => {
  const enc = new Encoder();
  const dst = Buffer.allocUnsafe(64);
  const off = enc.write(dst, 0, ':status', '200', false);
  assert.strictEqual(off, 1);
  assert.strictEqual(dst[0], 0x88); // index 8, 7-bit prefix
});

test('encoder: neverIndex keeps sensitive values out of any table', () => {
  const enc = new Encoder();
  const dst = Buffer.allocUnsafe(64);
  const off = enc.write(dst, 0, ':status', '200', true);
  assert.notStrictEqual(dst[0] & 0xf0, 0x80);
  const out = [];
  new Decoder(4096).decode(dst, 0, off, out);
  assert.deepStrictEqual(out, [':status', '200']);
});

test('encoder: a repeated field line settles into one indexed octet', () => {
  const enc = new Encoder(4096);
  const dec = new Decoder(4096); // the peer's half, fed only what we emit
  const dst = Buffer.allocUnsafe(4096);
  const sizes = [];
  for (let i = 0; i < 4; i++) {
    const off = enc.write(dst, 0, 'x-flow', 'abcdefgh', false);
    sizes.push(off);
    const out = [];
    dec.decode(dst, 0, off, out);
    assert.deepStrictEqual(out, ['x-flow', 'abcdefgh'], `response ${i} did not decode back`);
    assert.deepStrictEqual(live(dec), live(enc), `the peer's table diverged at ${i}`);
  }
  // literal, then the insertion, then references from then on
  assert.ok(sizes[0] > 10, `first block was ${sizes[0]} bytes`);
  assert.strictEqual(sizes[1] > sizes[2], true, 'the insertion is smaller than the literal');
  assert.strictEqual(sizes[2], 1, 'a table entry is one indexed octet');
  assert.strictEqual(sizes[3], 1);
});

test('encoder: a value that appears once per response never enters the table', () => {
  // The reason the whole design is safe to cache: unique values cost a literal and
  // change nothing, so the repeating lines keep their one-byte form and the byte cache
  // is never cleared.
  const enc = new Encoder(4096);
  const dec = new Decoder(4096);
  const dst = Buffer.allocUnsafe(4096);
  for (let i = 0; i < 50; i++) {
    const id = `req-${i}-${Math.floor(Math.random() * 1e9)}`;
    let off = enc.write(dst, 0, 'content-type', 'text/plain', false);
    off = enc.write(dst, off, 'x-request-id', id, false);
    const out = [];
    dec.decode(dst, 0, off, out);
    assert.deepStrictEqual(out, ['content-type', 'text/plain', 'x-request-id', id]);
  }
  assert.strictEqual(live(enc).length, 1, `table holds ${live(enc).length} entries`);
  assert.deepStrictEqual(live(enc), [['content-type', 'text/plain']]);
  assert.deepStrictEqual(live(dec), live(enc));
});

test('encoder: a name whose values never come back stops being remembered', () => {
  // The second-sighting rule needs the set of values seen exactly once, and the set is
  // not free: a fresh value string is hashed on every response, and the set grows until
  // it is cleared. A name whose values never repeat -- the request id a server echoes
  // in every response -- pays that for every response and gets nothing for it, since
  // the second sighting that would insert the pair never comes. Past ONCE_PER_NAME
  // distinct values (64, lib/hpack.js) the encoder stops remembering them.
  const enc = new Encoder(4096);
  const dst = Buffer.allocUnsafe(4096);
  for (let i = 0; i < 200; i++) {
    enc.write(dst, 0, 'x-request-id', `req-${i}`, false);
  }
  const seen = enc.once.get('x-request-id');
  assert.ok(seen !== undefined, 'the name was remembered');
  assert.ok(seen.size <= 64, `the set grew to ${seen.size} values`);
  assert.strictEqual(live(enc).length, 0,
    'a value that never comes back enters no table');
  // and the line still decodes back to what was written
  const dec = new Decoder(4096);
  const off = enc.write(dst, 0, 'x-request-id', 'req-final', false);
  const out = [];
  dec.decode(dst, 0, off, out);
  assert.deepStrictEqual(out, ['x-request-id', 'req-final']);
});

test('encoder: the encoded form of a name is cached, and the cache is bounded', () => {
  // A name that is in neither table is written in full, and a response whose block
  // cannot be cached as a whole pays that for every response: the same name is
  // Huffman-coded again for every new value of it. The encoded form is cached instead
  // -- the bytes depend on the name alone, so the cache is process-wide (lib/hpack.js).
  const enc = new Encoder(4096);
  const dst = Buffer.allocUnsafe(4096);
  const before = nameBytesSize();
  enc.write(dst, 0, 'x-a-name-of-its-own', '1', false);
  assert.strictEqual(nameBytesSize(), before + 1, 'the name was cached');
  // and the bound holds: a server that reflects untrusted input into header names
  // cannot grow the cache for the life of the process
  for (let i = 0; i < 2000; i++) {
    enc.write(dst, 0, `x-pad-${i}`, 'v', false);
  }
  assert.ok(nameBytesSize() <= 1024, `the cache holds ${nameBytesSize()} names`);
  // a name past the bound is still encoded correctly
  const dec = new Decoder(4096);
  const off = enc.write(dst, 0, 'x-after-the-bound', 'v', false);
  const out = [];
  dec.decode(dst, 0, off, out);
  assert.deepStrictEqual(out, ['x-after-the-bound', 'v']);
});

test('encoder: eviction leaves both tables identical', () => {
  // Capacity for about three of these entries, so every insertion evicts.
  const enc = new Encoder(160);
  const dec = new Decoder(160);
  const dst = Buffer.allocUnsafe(4096);
  const rounds = 6;
  for (let r = 0; r < rounds; r++) {
    for (let i = 0; i < 8; i++) {
      const off = enc.write(dst, 0, `n${i}`, `value-${i}`.repeat(3), false);
      const out = [];
      dec.decode(dst, 0, off, out);
      assert.strictEqual(out[0], `n${i}`);
      assert.strictEqual(out[1], `value-${i}`.repeat(3));
    }
    assert.deepStrictEqual(live(dec), live(enc), `round ${r}`);
    assert.ok(enc.size <= 160 && dec.size <= 160, `table grew to ${enc.size}`);
  }
  assert.ok(live(enc).length > 0 && live(enc).length < 8 * rounds, 'it did evict');
});

test('encoder: neverIndex stores nothing on either side', () => {
  const enc = new Encoder(4096);
  const dec = new Decoder(4096);
  const dst = Buffer.allocUnsafe(4096);
  for (let i = 0; i < 5; i++) {
    const off = enc.write(dst, 0, 'set-cookie', 'sid=secret', true);
    const out = [];
    dec.decode(dst, 0, off, out);
    assert.deepStrictEqual(out, ['set-cookie', 'sid=secret']);
  }
  assert.strictEqual(enc.size, 0);
  assert.strictEqual(dec.size, 0);
  // The same value without the flag is a table entry: the flag has to be what stops it
  const off = enc.write(dst, 0, 'set-cookie', 'sid=secret', false);
  const out = [];
  dec.decode(dst, 0, off, out);
  assert.strictEqual(enc.size, 0, 'a never-seen value is only remembered, not inserted');
  enc.write(dst, 0, 'set-cookie', 'sid=secret', false);
  assert.strictEqual(enc.size, 0, 'set-cookie is on the never-insert list');
  enc.write(dst, 0, 'x-cache-tag', 'private', false);
  enc.write(dst, 0, 'x-cache-tag', 'private', false);
  assert.ok(enc.size > 0, 'an ordinary value does get inserted');
});

test('encoder: a table size change is restated, an unchanged one is not', () => {
  const enc = new Encoder(4096);
  const dec = new Decoder(4096);
  const dst = Buffer.allocUnsafe(4096);
  for (let r = 0; r < 3; r++) {
    for (const [n, v] of [['content-type', 'text/plain'], ['server', 'h2lite']]) {
      const off = enc.write(dst, 0, n, v, false);
      dec.decode(dst, 0, off, []);
    }
  }
  assert.ok(enc.size > 0);
  assert.deepStrictEqual(live(dec), live(enc), 'warm-up drifted');
  assert.strictEqual(enc.takeSizeUpdate(), null, 'nothing pending before the change');

  // The same number the peer already advertised: no announcement, no eviction.
  enc.setPeerTableSize(4096);
  assert.strictEqual(enc.takeSizeUpdate(), null, 'an unchanged setting says nothing new');

  // A smaller one: our table shrinks, and the peer's context has to be told where.
  enc.setPeerTableSize(64);
  const update = enc.takeSizeUpdate();
  assert.ok(update !== null && (update[0] & 0xe0) === 0x20, 'a 001xxxxx size update');
  assert.strictEqual(enc.takeSizeUpdate(), null, 'and it is announced once');
  assert.strictEqual(enc.cache.size, 0, 'cached bytes may name an index that just moved');

  // the same peer decoder that has been tracking us: a fresh one would have an empty
  // table and could not be compared against ours
  dec.decode(update, 0, update.length, []);
  assert.strictEqual(dec.maxSize, 64, 'the peer shrank its table too');
  const off = enc.write(dst, 0, 'x-fit', 'nothing fits in 64 bytes with a name', false);
  const out = [];
  dec.decode(dst, 0, off, out);
  assert.strictEqual(out[1], 'nothing fits in 64 bytes with a name');
  assert.deepStrictEqual(live(dec), live(enc), 'a resized table drifted');
  assert.ok(enc.size <= 64 && dec.size <= 64, `${enc.size}/${dec.size} above the new cap`);

  // And a peer that raises its bound above our cap: our table does not move, but its
  // assumption of it does, so that has to be corrected too.
  const wide = new Encoder(4096, 1024);
  wide.setPeerTableSize(8192);
  assert.strictEqual(wide.maxSize, 1024, 'the cap holds');
  const w = wide.takeSizeUpdate();
  assert.ok(w !== null && (w[0] & 0xe0) === 0x20, 'we announce the smaller table we keep');
});

// The live part of either table, oldest first, as plain pairs: comparing these is what
// catches an encoder that inserted something its own output never announced.
function live(table) {
  return table.dyn.slice(table.base).map(([n, v]) => [n, v]);
}

test('encoder: a working set that overflows the table stops inserting', () => {
  // Forty distinct repeating lines of ~66 bytes do not fit in 160 bytes of table, so
  // every insertion throws an older one out -- and each eviction costs this session both
  // of its caches. It has to notice. The peer never hears about it, so the two tables
  // must stay identical anyway: nothing is inserted any more, and nothing is removed.
  const enc = new Encoder(160);
  const dec = new Decoder(160);
  const dst = Buffer.allocUnsafe(4096);
  const lines = [];
  for (let i = 0; i < 40; i++) {
    lines.push(['x-item', `value-${i}-${'z'.repeat(20)}`]);
  }
  let epochAtTrip = -1;
  for (let pass = 0; pass < 12; pass++) {
    for (const [n, v] of lines) {
      const off = enc.write(dst, 0, n, v, false);
      const out = [];
      dec.decode(dst, 0, off, out);
      assert.deepStrictEqual(out, [n, v], `pass ${pass}`);
      assert.deepStrictEqual(live(dec), live(enc), `pass ${pass}: the tables drifted`);
    }
    if (enc.noInsert && epochAtTrip < 0) {
      epochAtTrip = enc.epoch;
    }
  }
  assert.strictEqual(enc.noInsert, true, 'it never noticed the overflow');
  assert.strictEqual(enc.epoch, epochAtTrip, 'after stopping, nothing moved any more');
  assert.ok(live(enc).length > 0, 'the entries that fit were thrown away too');
});

test('encoder: a value bigger than three quarters of the table is not inserted', () => {
  // The size half of nghttp2's hd_deflate_decide_indexing() (lib/nghttp2_hd.c v1.70.0).
  // An entry that needs most of the table to itself would evict the stable lines to earn
  // one reference, so it stays a literal -- and since nothing is inserted, the epoch and
  // both caches stand still. 'x-big' plus a 163-octet value is 5 + 163 + 32 = 200 bytes
  // against a 256-byte table: it fits (RFC 7541 4.4 empties the table only above that),
  // but it is over three quarters of it.
  const enc = new Encoder(256);
  const dec = new Decoder(256);
  const dst = Buffer.allocUnsafe(4096);
  const value = 'q'.repeat(163);
  let first = null;
  for (let i = 0; i < 4; i++) {
    const off = enc.write(dst, 0, 'x-big', value, false);
    const bytes = Buffer.from(dst.subarray(0, off));
    if (first === null) {
      first = bytes;
    } else {
      assert.ok(bytes.equals(first), `pass ${i}: the bytes changed under a warm cache`);
    }
    const out = [];
    dec.decode(dst, 0, off, out);
    assert.deepStrictEqual(out, ['x-big', value]);
    assert.deepStrictEqual(live(dec), live(enc), `pass ${i}: the tables drifted`);
  }
  assert.deepStrictEqual(live(enc), [], 'the oversized entry reached the table');
  assert.strictEqual(enc.epoch, 0,
    'inserting it moved the table, which is the cost this rule avoids');
});

test('decoder: every static entry decodes through its own index', () => {
  const enc = new Encoder();
  const dec = new Decoder(65536);
  const dst = Buffer.allocUnsafe(4096);
  for (let i = 0; i < STATIC.length; i++) {
    const [n, v] = STATIC[i];
    const off = enc.write(dst, 0, n, v, false);
    const out = [];
    dec.decode(dst, 0, off, out);
    assert.deepStrictEqual(out, [n, v], `static index ${i + 1}`);
  }
});

test('dynamic table: insertion, index shift and eviction', () => {
  const one = Buffer.from([
    0x40, 0x0a, ...Buffer.from('custom-key', 'latin1'), // 01xxxxxx, new name
    0x0c, ...Buffer.from('custom-value', 'latin1')
  ]);
  const dec = new Decoder(4096);
  const out = [];
  dec.decode(one, 0, one.length, out);
  assert.deepStrictEqual(out, ['custom-key', 'custom-value']);
  // now it must be reachable as index 62 (newest dynamic entry)
  const indexed = Buffer.from([0x80 | 62]);
  const out2 = [];
  dec.decode(indexed, 0, 1, out2);
  assert.deepStrictEqual(out2, ['custom-key', 'custom-value']);
  // a second insert shifts the first one to 63
  const ab = Buffer.from([0x40, 0x01, 0x61, 0x01, 0x62]); // 'a' -> 'b'
  const more = Buffer.concat([one, ab]);
  const dec2 = new Decoder(4096);
  const out3 = [];
  dec2.decode(more, 0, more.length, out3);
  assert.deepStrictEqual(out3, ['custom-key', 'custom-value', 'a', 'b']);
  assert.deepStrictEqual(dec2.at(62), ['a', 'b']);
  assert.deepStrictEqual(dec2.at(63), ['custom-key', 'custom-value']);
  assert.strictEqual(dec2.at(64), undefined);
});

test('dynamic table: size update shrinks and evicts', () => {
  const dec = new Decoder(4096);
  const one = Buffer.from([0x40, 0x01, 0x61, 0x01, 0x62]); // 'a' -> 'b', 34 bytes
  const o = [];
  dec.decode(one, 0, one.length, o);
  assert.strictEqual(dec.dyn.length - dec.base, 1);
  dec.decode(Buffer.from([0x20]), 0, 1, o); // 001xxxxx size = 0
  assert.strictEqual(dec.size, 0);
  assert.strictEqual(dec.dyn.length - dec.base, 0);
  assert.throws(() => dec.decode(Buffer.from([0x3f, 0xff, 0xff, 0x01]), 0, 4, o),
    HPackError, 'a size update above our advertised capacity is fatal');
});

test('decoder: rejects invalid indices and truncated input', () => {
  const dec = new Decoder(4096);
  const out = [];
  assert.throws(() => dec.decode(Buffer.from([0x80]), 0, 1, out), HPackError); // index 0
  assert.throws(() => dec.decode(Buffer.from([0xbf]), 0, 1, out), HPackError); // 63: empty table
  assert.throws(() => dec.decode(Buffer.from([0x00, 0x05, 0x61]), 0, 3, out), HPackError);
  assert.throws(() => dec.decode(Buffer.from([0x01]), 0, 1, out), HPackError);
});

test('decoder: field names are interned', () => {
  const dec = new Decoder(4096);
  const enc = new Encoder();
  const dst = Buffer.allocUnsafe(256);
  const o1 = []; const o2 = [];
  let off = enc.write(dst, 0, 'x-custom-name', 'v1', false);
  dec.decode(dst, 0, off, o1);
  off = enc.write(dst, 0, 'x-custom-name', 'v2', false);
  dec.decode(dst, 0, off, o2);
  assert.strictEqual(o1[0], o2[0]);
  assert.notStrictEqual(o1[1], o2[1]);
});

// ---- the two decoders ------------------------------------------------------------
//
// A header block that arrived whole is decoded out of the chunk it came in; one
// that a socket read split is decoded out of the views it arrived in. The peer
// cannot tell which was which -- the split is the socket's business, not the
// protocol's -- so the two decoders have to agree: on the fields, and on the
// table they leave behind.
test('decoder: the piece decoder agrees with the contiguous one at every split', () => {
  const blocks = headerBlocks();
  for (let b = 0; b < blocks.length; b++) {
    const block = blocks[b];
    for (let k = 0; k <= block.length; k++) {
      const whole = [];
      const dw = new Decoder(4096);
      dw.decode(block, 0, block.length, whole);

      const split = [];
      const ds = new Decoder(4096);
      ds.decodePieces([block.subarray(0, k), block.subarray(k)], 0, block.length, split);

      assert.deepStrictEqual(split, whole, `block ${b} split at ${k}`);
      assert.deepStrictEqual(live(ds), live(dw), `block ${b} split at ${k}: the tables differ`);
    }
  }
});

test('decoder: an empty view between two others does not stop the piece decoder', () => {
  // A frame whose payload starts exactly at the end of a read arrives as an
  // empty view and the octets that follow it, so the cursor may not sit on an
  // empty one -- and an empty view in the middle of a block has to be skipped
  // the same way.
  const block = Buffer.from([0x82, 0x87, 0x84, 0x41, 0x09,
    ...Buffer.from('localhost', 'latin1')]);
  const whole = [];
  new Decoder(4096).decode(block, 0, block.length, whole);
  const split = [];
  new Decoder(4096).decodePieces(
    [block.subarray(0, 0), block.subarray(0, 3), Buffer.alloc(0), block.subarray(3)],
    0, block.length, split);
  assert.deepStrictEqual(split, whole);
  assert.strictEqual(whole.length, 8);
});

test('decoder: the piece decoder refuses what the contiguous one refuses', () => {
  const bad = [
    Buffer.from([0x80]), // index 0 does not exist
    Buffer.from([0xbf]), // 63: the dynamic table is empty
    Buffer.from([0x00, 0x05, 0x61]), // a literal that is cut short
    Buffer.from([0x01]), // a truncated integer
    Buffer.from([0x82, 0x20]) // a size update after a field line
  ];
  for (const block of bad) {
    for (let k = 0; k <= block.length; k++) {
      assert.throws(() => new Decoder(4096).decode(block, 0, block.length, []), HPackError,
        `block ${block.toString('hex')} split at ${k}`);
      assert.throws(() => new Decoder(4096).decodePieces(
        [block.subarray(0, k), block.subarray(k)], 0, block.length, []), HPackError,
      `block ${block.toString('hex')} split at ${k}`);
    }
  }
});

// The blocks the two decoders are compared over. Between them they hold every
// form a header block can take: a static index, a literal with a name in full,
// a literal with a name from the dynamic table, a Huffman-coded value, a length
// prefix that needs a continuation octet, an incremental indexing line (which
// moves the table), a never-indexed line and a size update.
function headerBlocks() {
  const enc = new Encoder(4096);
  const dst = Buffer.allocUnsafe(64 * 1024);
  const blocks = [];

  // A request as a peer sends it: three static indices and a literal
  // :authority that the table remembers.
  blocks.push(Buffer.from([0x82, 0x87, 0x84, 0x41, 0x09,
    ...Buffer.from('localhost', 'latin1')]));

  // The same shapes, written by the encoder: a Huffman-coded value, the line
  // that indexes it, a second line that names it through the dynamic table, and
  // a literal long enough to need a continuation octet in its length prefix.
  let off = enc.write(dst, 0, 'x-huff', 'www.example.com', false);
  off = enc.write(dst, off, 'x-huff', 'www.example.com', false);
  off = enc.write(dst, off, 'x-huff', 'other.example.org', false);
  off = enc.write(dst, off, 'x-long', 'z'.repeat(300), false);
  blocks.push(Buffer.from(dst.subarray(0, off)));

  // A size update at the front of a block (RFC 7541 6.3) and a never-indexed
  // line (the sensitive values' form).
  const enc2 = new Encoder(4096);
  const dst2 = Buffer.allocUnsafe(64 * 1024);
  let off2 = 0;
  off2 = enc2.write(dst2, off2, 'set-cookie', 'sid=secret', true);
  off2 = enc2.write(dst2, off2, 'x-after', 'v', false);
  blocks.push(Buffer.from(Buffer.concat([Buffer.from([0x3f, 0xe1, 0x1f]),
    dst2.subarray(0, off2)])));
  return blocks;
}
