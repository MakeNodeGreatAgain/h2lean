// Frame layer: constants and in-place header read/write. Deliberately tiny --
// a frame is (offset, length) into a caller-provided Buffer, never an object.
'use strict';

const PREFACE = Buffer.from('PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n');
const PREFACE_LEN = PREFACE.length;
const HEADER_LEN = 9;

// Frame types, RFC 9113 §6 and the IANA HTTP/2 frame registry. The numbers are the
// wire format, not an internal enumeration: a peer sends 9 when it splits a header
// block, and 10 is ALTSVC (RFC 7807, and RFC 9113 §5.5 says a frame of unknown or
// unsupported type MUST be discarded). Getting one of these wrong is invisible to our own
// tests -- both ends would use the same wrong number -- and looks to every other
// implementation like a connection error, so each value here was checked against a
// live node:http2 endpoint rather than recalled.
const DATA = 0, HEADERS = 1, PRIORITY = 2, RST_STREAM = 3, SETTINGS = 4;
const PUSH_PROMISE = 5, PING = 6, GOAWAY = 7, WINDOW_UPDATE = 8, CONTINUATION = 9;

const F_END_STREAM = 0x1;
const F_ACK = 0x1;
const F_END_HEADERS = 0x4;
const F_PADDED = 0x8;
const F_PRIORITY = 0x20;

// Wire error codes, RFC 9113 §7. Values verified against http2.constants
// (NGHTTP2_*), because rstCode/goawayCode are part of the observable API.
const NO_ERROR = 0, PROTOCOL_ERROR = 1, INTERNAL_ERROR = 2, FLOW_CONTROL_ERROR = 3;
const SETTINGS_TIMEOUT = 4, STREAM_CLOSED = 5, FRAME_SIZE_ERROR = 6;
const REFUSED_STREAM = 7, CANCEL = 8, COMPRESSION_ERROR = 9, CONNECT_ERROR = 10;
const ENHANCE_YOUR_CALM = 11, INADEQUATE_SECURITY = 12, HTTP_1_1_REQUIRED = 13;

// Stream state numbers as node reports them in stream.state.state.
const ST_IDLE = 1, ST_OPEN = 2, ST_RESERVED_LOCAL = 3, ST_RESERVED_REMOTE = 4;
const ST_HALF_CLOSED_LOCAL = 5, ST_HALF_CLOSED_REMOTE = 6, ST_CLOSED = 7;

// Results of readHeader land in these module-level slots instead of in a
// returned object: one allocation avoided per frame.
let FH_LEN = 0, FH_TYPE = 0, FH_FLAGS = 0, FH_SID = 0;

function readHeader(buf, off) {
  FH_LEN = (buf[off] << 16) | (buf[off + 1] << 8) | buf[off + 2];
  FH_TYPE = buf[off + 3];
  FH_FLAGS = buf[off + 4];
  FH_SID = buf.readUInt32BE(off + 5) & 0x7fffffff;
}

function writeHeader(dst, off, len, type, flags, streamId) {
  dst[off] = len >> 16;
  dst[off + 1] = len >> 8;
  dst[off + 2] = len;
  dst[off + 3] = type;
  dst[off + 4] = flags;
  dst.writeUInt32BE(streamId >>> 0, off + 5); // reserved bit stays 0
}

// RFC 9113 6.1: a padded DATA frame's payload is one octet of pad length, then
// the data, then that many octets of padding. The views a payload arrived in are
// therefore not the payload: the pad length has to come off the front and the
// padding off the back. The pad length is the payload's first octet, so it is
// known as soon as the payload starts -- there is nothing to wait for.
//
// The views are edited in place: the pad length octet is dropped from the first
// one, the padding from the last one, and the pieces that are left empty are
// dropped. A frame that arrived in pieces is handed to the application in
// pieces, and a copy made to trim it would undo that.
//
// Returns the number of data octets, or -1 when the frame is malformed: a padded
// frame with an empty payload has no pad length to read at all, and padding that
// does not fit in the payload is what RFC 9113 6.1 calls a connection error of
// type PROTOCOL_ERROR. Node answers both with GOAWAY(PROTOCOL_ERROR) (measured
// against a live node server, tools/probe-pad-bad.js).
function trimPadding(pieces) {
  let first = 0;
  while (first < pieces.length && pieces[first].length === 0) {
    first++;
  }
  if (first === pieces.length) {
    return -1;
  } // no pad length octet
  const pad = pieces[first][0];
  pieces[first] = pieces[first].subarray(1); // the pad length is not data
  let left = pad;
  for (let i = pieces.length - 1; i >= 0 && left > 0; i--) {
    const p = pieces[i];
    if (left >= p.length) {
      left -= p.length; pieces[i] = null;
    } else {
      pieces[i] = p.subarray(0, p.length - left); left = 0;
    }
  }
  if (left > 0) {
    return -1;
  } // the padding does not fit
  let total = 0;
  for (let i = 0; i < pieces.length; i++) {
    if (pieces[i] === null || pieces[i].length === 0) {
      pieces[i] = null; continue;
    }
    total += pieces[i].length;
  }
  return total;
}

module.exports = {
  PREFACE, PREFACE_LEN, HEADER_LEN,
  DATA, HEADERS, PRIORITY, RST_STREAM, SETTINGS, PUSH_PROMISE, PING, GOAWAY,
  WINDOW_UPDATE, CONTINUATION,
  F_END_STREAM, F_ACK, F_END_HEADERS, F_PADDED, F_PRIORITY,
  NO_ERROR, PROTOCOL_ERROR, INTERNAL_ERROR, FLOW_CONTROL_ERROR, SETTINGS_TIMEOUT,
  STREAM_CLOSED, FRAME_SIZE_ERROR, REFUSED_STREAM, CANCEL, COMPRESSION_ERROR,
  CONNECT_ERROR, ENHANCE_YOUR_CALM, INADEQUATE_SECURITY, HTTP_1_1_REQUIRED,
  ST_IDLE, ST_OPEN, ST_RESERVED_LOCAL, ST_RESERVED_REMOTE,
  ST_HALF_CLOSED_LOCAL, ST_HALF_CLOSED_REMOTE, ST_CLOSED,
  readHeader, writeHeader, trimPadding,
  get FH_LEN() {
    return FH_LEN;
  },
  get FH_TYPE() {
    return FH_TYPE;
  },
  get FH_FLAGS() {
    return FH_FLAGS;
  },
  get FH_SID() {
    return FH_SID;
  }
};
