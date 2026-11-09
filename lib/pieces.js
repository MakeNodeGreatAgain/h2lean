// The octets of a frame that arrived in pieces.
//
// A frame whose bytes do not fall inside one socket read is assembled out of the
// views it arrived in rather than copied into a buffer of its own: a DATA
// payload reaches the application as the views it arrived in (node hands over
// the same shape -- tools/probe-chunking.js), and a header block that a read
// split is decoded out of the views. This file is the machinery that needs:
// how long the views are together, how to read an octet or a 32-bit value out
// of them, how to cut a range out of them, and how to compare them with a
// buffer.
//
// The views are of the chunks node read off the socket, and node never writes
// into a chunk again, so a view stays valid for as long as anybody holds it.
// That is the rule the slab this replaced broke: the slab was reused for the
// next frame that arrived in pieces, so the bytes an application kept moved
// under it (AGENTS.md 5: issued buffers stay valid).
//
// Every function here takes the views and an offset into their concatenation.
// The offsets are into the *payload* of a frame, so a caller that has trimmed
// the padding off a frame passes the trimmed offsets.
'use strict';

// How many octets the views hold together.
function piecesLength(pieces) {
  let n = 0;
  for (let i = 0; i < pieces.length; i++) {
    n += pieces[i].length;
  }
  return n;
}

// The octet at `off` in the concatenation of the views, or -1 past the end.
function pieceByteAt(pieces, off) {
  for (let i = 0; i < pieces.length; i++) {
    const p = pieces[i];
    if (off < p.length) {
      return p[off];
    }
    off -= p.length;
  }
  return -1;
}

// A 32-bit value at `off`, big endian -- the frame header's stream id, the
// SETTINGS entries' values, the flow-control increments.
function pieceU32At(pieces, off) {
  return ((pieceByteAt(pieces, off) * 0x1000000)
    + (pieceByteAt(pieces, off + 1) << 16)
    + (pieceByteAt(pieces, off + 2) << 8)
    + pieceByteAt(pieces, off + 3)) >>> 0;
}

// The views that cover [from, to) of the concatenation. The parts that are not
// whole views are trimmed, so the result is the range and nothing else.
function pieceSlice(pieces, from, to) {
  const out = [];
  let off = 0;
  for (let i = 0; i < pieces.length; i++) {
    const p = pieces[i];
    const end = off + p.length;
    if (end > from && off < to) {
      const a = from > off ? from - off : 0;
      const b = to < end ? to - off : p.length;
      out.push(p.subarray(a, b));
    }
    off = end;
    if (off >= to) {
      break;
    }
  }
  return out;
}

// The octets of [from, to) copied into `dst` at `dstOff`. The copy a frame's
// first octets need when they are spread over several views: the pad length
// and the priority block of a HEADERS payload are at its front and are at most
// six octets.
function pieceCopyTo(dst, dstOff, pieces, from, to) {
  let off = 0;
  let written = 0;
  for (let i = 0; i < pieces.length && written < to - from; i++) {
    const p = pieces[i];
    const end = off + p.length;
    if (end > from && off < to) {
      const a = from > off ? from - off : 0;
      const b = to < end ? to - off : p.length;
      p.copy(dst, dstOff + written, a, b);
      written += b - a;
    }
    off = end;
  }
  return written;
}

// A fresh buffer with the octets of [from, to). This is the copy an application
// keeps, and the copy the shape cache stores -- the views themselves are not
// kept, because a view pins the socket chunk it came from.
function pieceConcat(pieces, from, to) {
  const parts = pieceSlice(pieces, from, to);
  if (parts.length === 1) {
    return Buffer.from(parts[0]);
  }
  return Buffer.concat(parts, to - from);
}

// Are the octets of [from, to) the same as the buffer's? The shape cache's
// answer to "was this exact block seen before".
function pieceEquals(buf, pieces, from, to) {
  if (buf.length !== to - from) {
    return false;
  }
  let off = 0;
  for (let i = 0; i < pieces.length; i++) {
    const p = pieces[i];
    const end = off + p.length;
    if (end > from && off < to) {
      const a = from > off ? from - off : 0;
      const b = to < end ? to - off : p.length;
      if (buf.compare(p, a, b, off - from + a, off - from + b) !== 0) {
        return false;
      }
    }
    off = end;
    if (off >= to) {
      break;
    }
  }
  return true;
}

module.exports = {
  piecesLength, pieceByteAt, pieceU32At, pieceSlice, pieceCopyTo, pieceConcat,
  pieceEquals
};
