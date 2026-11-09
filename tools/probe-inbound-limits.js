// What node:http2 answers to the inbound frames whose bounds test/inbound-limits.test.js
// pins. One raw client per case, the same case run against node and against h2lite, the two
// transcripts printed together -- so every expectation in the test file was measured here
// first rather than recalled.
//
//   node tools/probe-inbound-limits.js          -> both stacks
//   node tools/probe-inbound-limits.js node     -> node only
'use strict';
const net = require('node:net');
const f = require('../lib/frames');
const h2lite = require('../index');
const nodeHttp2 = require('node:http2');
const { listen } = require('../test/diff-harness');

const CODES = { 0: 'NO_ERROR', 1: 'PROTOCOL_ERROR', 2: 'INTERNAL_ERROR', 3: 'FLOW_CONTROL_ERROR',
  5: 'STREAM_CLOSED', 6: 'FRAME_SIZE_ERROR', 7: 'REFUSED_STREAM', 8: 'CANCEL',
  9: 'COMPRESSION_ERROR', 11: 'ENHANCE_YOUR_CALM' };
const code = (n) => CODES[n] !== undefined ? `${n}:${CODES[n]}` : String(n);

// :method GET, :path /, :scheme http, :authority localhost -- 14 octets on the wire, and
// 7+3 + 5+1 + 7+4 + 10+9 = 46 octets once decoded. The gap between those two numbers is what
// cases B1-B3 use to tell the two readings of maxHeaderListSize apart.
const REQ = Buffer.from([0x82, 0x84, 0x86, 0x01, 9, ...Buffer.from('localhost', 'latin1')]);
// one octet on the wire, 15+13 = 28 decoded: the static line 'accept-encoding: gzip, deflate'
const STATIC_LINE = Buffer.from([0x80 | 16]);

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// RFC 7541 5.1, the integer representation (here with the 7-bit prefix 6.1 uses for an index).
// Writing `0x00 | length` into one octet instead
// announces a 1 400-octet value as 120, and the server then answers COMPRESSION_ERROR for a bug
// in the probe rather than in itself.
function int7(prefix, n) {
  if (n < 0x80 - 1) {
    return Buffer.from([prefix | n]);
  }
  const bytes = [prefix | 0x7f];
  n -= 0x80 - 1;
  while (n >= 0x80) {
    bytes.push((n & 0x7f) | 0x80); n >>= 7;
  }
  bytes.push(n);
  return Buffer.from(bytes);
}

// RFC 7541 5.1 with an explicit prefix width. int7 above hard-codes the 7-bit case; a 5-bit
// integer (an HPACK table size update) carries only 31 in the first octet, so reusing it writes a
// value 96 lower than intended and the server then errors on the probe rather than on itself.
function intN(prefix, bits, n) {
  const max = (1 << bits) - 1;
  if (n < max) {
    return Buffer.from([prefix | n]);
  }
  const bytes = [prefix | max];
  n -= max;
  while (n >= 0x80) {
    bytes.push((n & 0x7f) | 0x80); n >>= 7;
  }
  bytes.push(n);
  return Buffer.from(bytes);
}

// A literal field line, never indexed, no Huffman: `nameLen` + `valueLen` decoded octets and
// 2 + nameLen + the value's length prefix + valueLen on the wire.
function lit(nameLen, valueLen) {
  return Buffer.concat([
    Buffer.from([0x00]), int7(0x00, nameLen), Buffer.from('x'.repeat(nameLen), 'latin1'),
    int7(0x00, valueLen), Buffer.from('y'.repeat(valueLen), 'latin1')
  ]);
}

// A field line whose value length prefix claims `claimed` octets and sends `sent` of them.
function lyingLength(claimed, sent) {
  return Buffer.concat([
    Buffer.from([0x00]), int7(0x00, 1), Buffer.from('n', 'latin1'),
    int7(0x00, claimed), Buffer.from('z'.repeat(sent), 'latin1')
  ]);
}

const EOH = f.F_END_HEADERS, EOS = f.F_END_STREAM;
const req = (block, endStream) => frame(f.HEADERS, endStream === false ? 0 : EOH, 1, block);
const big = (n) => Buffer.alloc(n, 0x61);

// Frame payloads for the state-validation cases below: a WINDOW_UPDATE / RST_STREAM / GOAWAY /
// PRIORITY block built the way RFC 9113 6 lays it out, and one SETTINGS entry.
const wu = (sid, inc) => {
  const p = Buffer.alloc(4); p.writeUInt32BE(inc, 0); return frame(f.WINDOW_UPDATE, 0, sid, p);
};
const rst = (sid, c) => {
  const p = Buffer.alloc(4); p.writeUInt32BE(c, 0); return frame(f.RST_STREAM, 0, sid, p);
};
const pri = (sid, dep, weight) => frame(f.PRIORITY, 0, sid, dep32(dep, weight));
const dep32 = (dep, weight) => {
  const p = Buffer.alloc(5); p.writeUInt32BE(dep, 0); p[4] = weight; return p;
};
const goaway = (c) => {
  const p = Buffer.alloc(8); p.writeUInt32BE(0, 0); p.writeUInt32BE(c >>> 0, 4); return frame(f.GOAWAY, 0, 0, p);
};
const pingData = () => Buffer.concat([Buffer.from('h2spec', 'latin1'), Buffer.alloc(2)]);
const setting = (id, value) => {
  const p = Buffer.alloc(6); p.writeUInt16BE(id, 0); p.writeUInt32BE(value, 2); return p;
};

// A literal field line with a real name, never indexed: HPACK 6.2.2, name index 0 means "the
// name is a string literal that follows".
function litField(name, value) {
  const n = Buffer.from(name, 'latin1'), v = Buffer.from(value, 'latin1');
  return Buffer.concat([
    Buffer.from([0x00]), int7(0x00, n.length), n, int7(0x00, v.length), v
  ]);
}

// [label, server settings, frames to send]
const cases = [
  ['A1 DATA 16384 == maxFrameSize, stream left open for it', undefined, (sock) => {
    sock.write(req(REQ));
    sock.write(frame(f.DATA, 0, 1, big(16384)));
    sock.write(frame(f.DATA, EOS, 1, Buffer.alloc(0)));
  }],
  ['A2 DATA 16385 > maxFrameSize', undefined, (sock) => {
    sock.write(req(REQ));
    sock.write(frame(f.DATA, EOS, 1, big(16385)));
  }],
  ['A2b CONTINUATION 16385 finishing a block', undefined, (sock) => {
    sock.write(frame(f.HEADERS, 0, 1, REQ));
    sock.write(frame(f.CONTINUATION, EOH, 1, big(16385)));
  }],
  ['A2c DATA 16385 on stream 0 (a protocol error of another kind)', undefined, (sock) => {
    sock.write(frame(f.DATA, EOS, 0, big(16385)));
  }],
  ['A3 HEADERS payload 16385', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 1, Buffer.concat([REQ, big(16370)])));
  }],
  ['A4 WINDOW_UPDATE payload 16385', undefined, (sock) => {
    const p = Buffer.alloc(16385); p.writeUInt32BE(1, 0);
    sock.write(frame(f.WINDOW_UPDATE, 0, 0, p));
    sock.write(req(REQ));
  }],
  // Is HEADERS exempt from the frame-size limit the way SETTINGS and PING are, or does the
  // COMPRESSION_ERROR of A3/A2b only win because that oversized payload was not valid HPACK?
  // Here the payload IS valid: 5 000 literal lines of one-octet name and one-octet value
  // (20 000 octets, over the 16 384 limit), and maxHeaderListSize is raised to 1 000 000 so
  // nothing but a size check can refuse it. FRAME_SIZE_ERROR means HEADERS is checked; an
  // answer means it is not, and the header block limit is what bounds header frames.
  ['A5 HEADERS 20 000 octets of VALID HPACK, limit 1 000 000', { maxHeaderListSize: 1000000 },
    (sock) => {
      const line = lit(1, 1);
      const parts = [REQ];
      for (let i = 0; i < 5000; i++) {
        parts.push(line);
      }
      sock.write(frame(f.HEADERS, EOH | EOS, 1, Buffer.concat(parts)));
    }],
  ['B1 limit 48: block 15 wire / 46 decoded  (both under)', { maxHeaderListSize: 48 },
    (sock) => {
      sock.write(req(REQ));
    }],
  ['B2 limit 48: block 16 wire / 74 decoded  (only decoded over)', { maxHeaderListSize: 48 },
    (sock) => {
      sock.write(req(Buffer.concat([REQ, STATIC_LINE])));
    }],
  ['B3 limit 48: block 88 wire / 114 decoded (both over)', { maxHeaderListSize: 48 },
    (sock) => {
      sock.write(req(Buffer.concat([REQ, lit(45, 23)])));
    }],
  ['B4 limit 48: 160 wire / 102 decoded, second request must still work',
    { maxHeaderListSize: 48 }, (sock) => {
      sock.write(req(Buffer.concat([REQ, lit(120, 36)])));
      sock.write(frame(f.HEADERS, EOH | EOS, 3, REQ));
    }],
  ['C1 default limit: block 88 wire / 114 decoded', undefined, (sock) => {
    sock.write(req(Buffer.concat([REQ, lit(45, 23)])));
  }],
  // What maxHeaderListSize is compared against. B1 says it is not the number of octets on the
  // wire (14 of them, refused under a limit of 48), so the candidate is the decoded size plus a
  // cost per field. E1 (5 fields, 87 decoded) served and E2 (5 fields, 93) refused put the
  // boundary at 90 = 250 - 5*32, which is the 32-octet entry overhead of RFC 7541 4.1 (and the
  // same computation RFC 9113 6.5.2 puts the setting in terms of); E4 (8
  // fields, 158 decoded, 414 with the overhead) refused where a model without overhead would
  // have served it. Measured on node v26.12.1 with this probe.
  ['E1 limit 250: 5 fields, decoded 87   (+32 each = 247, under)',
    { maxHeaderListSize: 250 }, (sock) => {
      sock.write(req(Buffer.concat([REQ, lit(1, 40)])));
    }],
  ['E2 limit 250: 5 fields, decoded 93   (+32 each = 253, over)',
    { maxHeaderListSize: 250 }, (sock) => {
      sock.write(req(Buffer.concat([REQ, lit(1, 46)])));
    }],
  ['E3 limit 250: 4 fields, decoded 46   (+32 each = 174, under)',
    { maxHeaderListSize: 250 }, (sock) => {
      sock.write(req(REQ));
    }],
  ['E4 limit 250: 8 fields, decoded 158  (+32 each = 414: model refuses, plain serves)',
    { maxHeaderListSize: 250 }, (sock) => {
      sock.write(req(Buffer.concat([REQ, STATIC_LINE, STATIC_LINE, STATIC_LINE, STATIC_LINE,
        STATIC_LINE, STATIC_LINE, STATIC_LINE])));
    }],
  // Does a refused header list end the connection? node refuses stream 1 and answers stream 3
  // on the same connection -- so an application that sends one oversized request must not
  // lose every other client with it.
  ['F1 limit 250: oversized stream 1, then a legal stream 3', { maxHeaderListSize: 250 },
    (sock) => {
      sock.write(req(Buffer.concat([REQ, lit(1, 46)])));
      sock.write(frame(f.HEADERS, EOH | EOS, 3, REQ));
    }],
  // After a per-stream refusal the peer is entitled to keep sending the body it had already
  // put on the wire. Does the refusal make the stream's later DATA an error of the connection?
  ['H1 limit 250: oversized stream 1, then DATA for it, then a legal stream 3',
    { maxHeaderListSize: 250 }, (sock) => {
      sock.write(req(Buffer.concat([REQ, lit(1, 46)])));
      sock.write(frame(f.DATA, EOS, 1, Buffer.from('body', 'ascii')));
      sock.write(frame(f.HEADERS, EOH | EOS, 3, REQ));
    }],
  // Is there a cap on ONE literal at all, apart from the frame size and the list size? The
  // two cases below differ only in how big the value is, and maxFrameSize is raised so the
  // frames are legal while maxHeaderListSize is raised so the list is not what refuses them.
  // nghttp2 had a NGHTTP2_HD_MAX_LITERAL of 4096 in older releases; what node 26 does is
  // measured here.
  ['G1 a 6 000-octet literal value, maxFrameSize 2 MiB',
    { maxFrameSize: 2097152, maxHeaderListSize: 10000000 }, (sock) => {
      sock.write(req(Buffer.concat([REQ, lit(1, 6000)])));
    }],
  ['G2 a 1 500 000-octet literal value, maxFrameSize 2 MiB',
    { maxFrameSize: 2097152, maxHeaderListSize: 10000000 }, (sock) => {
      sock.write(req(Buffer.concat([REQ, lit(1, 1500000)])));
    }],
  ['C2 literal claims 2 000 000 octets, sends 4', undefined, (sock) => {
    sock.write(req(Buffer.concat([REQ, lyingLength(2000000, 4)])));
  }],
  ['C3 literal claims 2 000 000 000, sends 4', undefined, (sock) => {
    sock.write(req(Buffer.concat([REQ, lyingLength(2000000000, 4)])));
  }],
  ['D1 unknown frame type 10 (ALTSVC) on stream 0, then a request', undefined, (sock) => {
    sock.write(frame(10, 0, 0, Buffer.from('localhost', 'ascii')));
    sock.write(req(REQ));
  }],
  ['D2 unknown frame type 99 on stream 0, then a request', undefined, (sock) => {
    sock.write(frame(99, 0, 0, Buffer.from('x', 'ascii')));
    sock.write(req(REQ));
  }],
  ['D3 unknown frame type 0xff with a 4 KiB payload, then a request', undefined, (sock) => {
    sock.write(frame(0xff, 0, 0, big(4096)));
    sock.write(req(REQ));
  }],

  // ---- frame-state validation ------------------------------------------------─
  // The shapes below are the ones h2spec sends for the checks it names in its own words: idle-
  // stream RST_STREAM and WINDOW_UPDATE (http2/5.1, 6.4), SETTINGS answered wrongly (6.5, 6.5.2),
  // PING on a stream (6.7), PRIORITY (6.3), a self-dependency (5.3.1), a peer GOAWAY with a code
  // no version defined (7), a second HEADERS block (8.1), a content-length that does not add up
  // (8.1.2.6) and an HPACK size update in the wrong place (hpack/4.2). Each case ends with a legal
  // request on stream 1 (or 3) so "the frame was ignored" and "the connection died" cannot be read
  // from the same transcript: an ANSWERED means the session stayed up.
  ['I1 WINDOW_UPDATE on an idle stream, then a request', undefined, (sock) => {
    sock.write(wu(3, 1024));
    sock.write(req(REQ));
  }],
  ['I2 RST_STREAM on an idle stream, then a request', undefined, (sock) => {
    sock.write(rst(3, 0));
    sock.write(req(REQ));
  }],
  ['I3 RST_STREAM with stream id 0, then a request', undefined, (sock) => {
    sock.write(rst(0, 0));
    sock.write(req(REQ));
  }],
  ['J1 SETTINGS with the ACK flag and a payload, then a request', undefined, (sock) => {
    sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(1)));
    sock.write(req(REQ));
  }],
  ['J2 SETTINGS on a stream id other than 0', undefined, (sock) => {
    sock.write(frame(f.SETTINGS, 0, 1, Buffer.alloc(0)));
    sock.write(req(REQ));
  }],
  ['J3 SETTINGS answered on a stream id other than 0', undefined, (sock) => {
    sock.write(frame(f.SETTINGS, 0, 1, setting(0x2, 1)));
    sock.write(req(REQ));
  }],
  ['J4 SETTINGS_ENABLE_PUSH with a value other than 0 or 1', undefined, (sock) => {
    sock.write(frame(f.SETTINGS, 0, 0, setting(0x2, 2)));
    sock.write(req(REQ));
  }],
  ['J5 PING on a stream id other than 0, then a request', undefined, (sock) => {
    sock.write(frame(f.PING, 0, 3, Buffer.alloc(8)));
    sock.write(req(REQ));
  }],
  ['K1 PRIORITY with a 4-octet payload, then a request', undefined, (sock) => {
    sock.write(frame(f.PRIORITY, 0, 1, Buffer.alloc(4)));
    sock.write(req(REQ));
  }],
  ['K2 PRIORITY on stream id 0, then a request', undefined, (sock) => {
    sock.write(pri(0, 1, 16));
    sock.write(req(REQ));
  }],
  ['K3 HEADERS whose priority block names itself as the dependency', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS | f.F_PRIORITY, 1,
      Buffer.concat([dep32(1, 255), REQ])));
  }],
  ['K4 PRIORITY that names itself as the dependency, after the stream is open', undefined, (sock) => {
    sock.write(req(REQ));
    sock.write(pri(1, 1, 16));
  }],
  ['L1 GOAWAY with an unassigned error code (17), then PING', undefined, (sock) => {
    sock.write(goaway(17));
    sock.write(frame(f.PING, 0, 0, Buffer.from('h2spec00', 'ascii')));
  }],
  ['L2 GOAWAY with an unassigned error code (0xfffffff0), then PING', undefined, (sock) => {
    sock.write(goaway(0xfffffff0));
    sock.write(frame(f.PING, 0, 0, Buffer.from('h2spec00', 'ascii')));
  }],
  ['L3 GOAWAY with PROTOCOL_ERROR, then PING', undefined, (sock) => {
    sock.write(goaway(1));
    sock.write(frame(f.PING, 0, 0, Buffer.from('h2spec00', 'ascii')));
  }],
  ['L4 GOAWAY with NO_ERROR, then PING 100 ms later', undefined, (sock) => {
    sock.write(goaway(0));
    setTimeout(() => sock.write(frame(f.PING, 0, 0, Buffer.from('h2spec00', 'ascii'))), 100);
  }],
  ['L5 GOAWAY with an unassigned code, then PING 100 ms later', undefined, (sock) => {
    sock.write(goaway(17));
    setTimeout(() => sock.write(frame(f.PING, 0, 0, Buffer.from('h2spec00', 'ascii'))), 100);
  }],
  // The exact two frames h2spec writes for http2/7: GOAWAY carrying code 255, which no version of
  // the spec defines, and one PING behind it. The check passes on "the connection closed" OR on a
  // PING ACK, and only an RST -- a close that arrives before the peer's write was read -- fails.
  ['L6 GOAWAY code=255 then PING "h2spec" (the shape h2spec sends)', undefined, (sock) => {
    sock.write(goaway(255));
    sock.write(frame(f.PING, 0, 0, Buffer.from('6832737065630000', 'hex')));
  }],
  ['M1 HEADERS without END_STREAM, DATA, then a second HEADERS block', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, REQ));
    sock.write(frame(f.DATA, 0, 1, Buffer.from('test', 'ascii')));
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.from('4085f2b24a84ff026f6b', 'hex')));
  }],
  ['M1b a second HEADERS block right after the first, neither with END_STREAM', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, REQ));
    sock.write(frame(f.HEADERS, EOH, 1, REQ));
  }],
  ['N1 content-length 1 answered by one DATA of 4 with END_STREAM', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.concat([REQ, Buffer.from('5c0131', 'hex')])));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
  }],
  ['N2 content-length 1 answered by DATA 4 + DATA 4 (the sum is wrong)', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.concat([REQ, Buffer.from('5c0131', 'hex')])));
    sock.write(frame(f.DATA, 0, 1, Buffer.from('test', 'ascii')));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
  }],
  ['N3 content-length 4 answered by DATA 4 (the sum is right, control)', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.concat([REQ, Buffer.from('5c0134', 'hex')])));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
  }],
  ['O1 HPACK dynamic table size update at the END of a header block', undefined, (sock) => {
    sock.write(req(Buffer.concat([REQ, Buffer.from([0x21])])));
  }],
  ['O2 HPACK dynamic table size update at the START of a block (control)', undefined, (sock) => {
    sock.write(req(Buffer.concat([Buffer.from([0x21]), REQ])));
  }],
  ['O3 HPACK size update to 512 at the end of a block (a change, not a no-op)', undefined, (sock) => {
    sock.write(req(Buffer.concat([REQ, intN(0x20, 5, 512)])));
  }],
  ['O4 HPACK size update to the SAME size at the end of a block (no-op change)', undefined, (sock) => {
    sock.write(req(Buffer.concat([REQ, intN(0x20, 5, 4096)])));
  }],
  ['O5 two size updates in a row at the START of a block, then a legal one', undefined, (sock) => {
    sock.write(req(Buffer.concat([intN(0x20, 5, 4096), Buffer.from([0x21]), REQ])));
    sock.write(frame(f.HEADERS, EOH | EOS, 3, Buffer.concat([REQ, litField('x', 'y')])));
  }],

  // ---- the edges the 15 fixes reach but the cases above do not settle --------
  ['P1 PING with the ACK flag and a 4-octet payload', undefined, (sock) => {
    sock.write(frame(f.PING, f.F_ACK, 0, Buffer.alloc(4)));
    sock.write(req(REQ));
  }],
  ['P2 PING with the ACK flag on a stream id other than 0', undefined, (sock) => {
    sock.write(frame(f.PING, f.F_ACK, 3, Buffer.alloc(8)));
    sock.write(req(REQ));
  }],
  ['P3 HEADERS with the priority flag and only 4 octets of payload', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS | f.F_PRIORITY, 1, Buffer.alloc(4)));
    sock.write(frame(f.HEADERS, EOH | EOS, 3, REQ));
  }],
  ['P4 WINDOW_UPDATE for an idle stream above the highest one opened', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
    sock.write(wu(3, 1024));
  }],
  ['P5 RST_STREAM for an idle stream above the highest one opened', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
    sock.write(rst(3, 0));
  }],
  ['P6 content-length 4, body of 2 octets (the sum is short)', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.concat([REQ, Buffer.from('5c0134', 'hex')])));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('ab', 'ascii')));
  }],
  ['P7 content-length 4 on a HEADERS that ends the stream, no DATA', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 1, Buffer.concat([REQ, Buffer.from('5c0134', 'hex')])));
  }],
  ['P8 content-length 0 with a body of 4 octets', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.concat([REQ, Buffer.from('5c0130', 'hex')])));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
  }],
  // NOT the legal shape it looks like: END_STREAM is already on the DATA, so the trailers arrive
  // for a stream that has finished. node answers GOAWAY(STREAM_CLOSED), h2lite resets the stream.
  // Pinned as a divergence to be decided, not as a case that passes.
  ['P9 content-length 4 with DATA 4 and END_STREAM, then trailers AFTER the stream closed', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.concat([REQ, Buffer.from('5c0134', 'hex')])));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
    sock.write(frame(f.HEADERS, EOH | EOS, 1, Buffer.from(litField('x', 'y'))));
  }],
  ['P10 trailers on a request whose body never ended (the 8.1 shape, legal block)',
    undefined, (sock) => {
      sock.write(frame(f.HEADERS, EOH, 1, REQ));
      sock.write(frame(f.DATA, 0, 1, Buffer.from('test', 'ascii')));
      sock.write(frame(f.HEADERS, EOH | EOS, 1, Buffer.from(litField('x', 'y'))));
    }],

  // ---- which half of the two rules is doing the work -------------------------
  // Q3 is 8.1 without the confound M1 carries: h2spec's second block contains a pseudo-header,
  // so M1 cannot tell "a trailer must end the stream" from "a trailer may not hold one".
  ['Q3 a trailer block with a legal field and no END_STREAM', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, REQ));
    sock.write(frame(f.DATA, 0, 1, Buffer.from('test', 'ascii')));
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.from(litField('x', 'y'))));
  }],
  ['Q4 the same trailer block with END_STREAM (control)', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, REQ));
    sock.write(frame(f.DATA, 0, 1, Buffer.from('test', 'ascii')));
    sock.write(frame(f.HEADERS, EOH | EOS, 1, Buffer.from(litField('x', 'y'))));
  }],
  ['Q5 content-length 4 as DATA 3 + DATA 1, sum exact', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.concat([REQ, Buffer.from('5c0134', 'hex')])));
    sock.write(frame(f.DATA, 0, 1, Buffer.from('abc', 'ascii')));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('d', 'ascii')));
  }],
  ['Q6 content-length 4 as DATA 3 + DATA 2, the second frame is over', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.concat([REQ, Buffer.from('5c0134', 'hex')])));
    sock.write(frame(f.DATA, 0, 1, Buffer.from('abc', 'ascii')));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('de', 'ascii')));
  }],
  ['Q7 content-length of ten nines, body of 4', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1,
      Buffer.concat([REQ, Buffer.from('5c0a39393939393939393939', 'hex')])));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
  }],

  // ---- is the over-length rule per frame or cumulative? ----------------------
  // R1/R2/R7/R8 all keep every single DATA frame at or under the declared length while letting
  // the SUM go over it, which is the only shape that tells "this frame is longer than the
  // promise" apart from "the promise has run out". node's answer decides which of the two this
  // library implements, so it is measured rather than reasoned about.
  ['R1 CL 4, DATA 5 with END_STREAM (a single frame over the promise)', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.concat([REQ, Buffer.from('5c0134', 'hex')])));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('abcde', 'ascii')));
  }],
  ['R2 CL 4, DATA 5 without END_STREAM', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.concat([REQ, Buffer.from('5c0134', 'hex')])));
    sock.write(frame(f.DATA, 0, 1, Buffer.from('abcde', 'ascii')));
    sock.write(frame(f.DATA, EOS, 1, Buffer.alloc(0)));
  }],
  ['R7 CL 100, DATA 60 + DATA 60 (each under, the sum over)', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1,
      Buffer.concat([REQ, Buffer.from('5c03313030', 'hex')])));
    sock.write(frame(f.DATA, 0, 1, Buffer.alloc(60, 0x61)));
    sock.write(frame(f.DATA, EOS, 1, Buffer.alloc(60, 0x61)));
  }],
  ['R8 CL 4, DATA 2 + DATA 2 + DATA 1 (the sum over at END_STREAM)', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.concat([REQ, Buffer.from('5c0134', 'hex')])));
    sock.write(frame(f.DATA, 0, 1, Buffer.from('ab', 'ascii')));
    sock.write(frame(f.DATA, 0, 1, Buffer.from('cd', 'ascii')));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('e', 'ascii')));
  }],

  // ---- the three checks that still separated the two stacks after the guards landed ---------
  // S1/S2 are h2spec's http2/6.1 sequence: a request whose body arrives on a stream that has
  // already closed remotely. The verdict that differs is the WINDOW_UPDATE(0) -- one stack gives
  // the connection window credit for bytes it refuses, the other keeps the window shut. S3/S4 do
  // the same for a stream we have already closed with our own END_STREAM answer.
  ['S1 DATA with END_STREAM on a half-closed (remote) stream', undefined, (sock) => {
    sock.write(req(REQ)); // END_STREAM on HEADERS
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
  }],
  ['S2 the same with no responder (the stream stays unanswered)', undefined, (sock) => {
    sock.write(req(REQ));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
  }, false],
  ['S3 DATA after the server answered with END_STREAM', undefined, (sock) => {
    sock.write(req(REQ));
    sock.write(frame(f.DATA, 0, 1, Buffer.from('test', 'ascii')));
  }],
  ['S4 DATA after our answer, no responder', undefined, (sock) => {
    sock.write(req(REQ));
    sock.write(frame(f.DATA, 0, 1, Buffer.from('test', 'ascii')));
  }, false],
  // S5/S6 are h2spec's http2/7: a GOAWAY whose error code is not one of the nine defined ones.
  // The check passes for an endpoint that closes without answering the GOAWAY at all; both stacks
  // do answer here, so what the tool reads is HOW the connection ends -- FIN or a reset -- and
  // whether the frames queued in the same read batch as the GOAWAY reach the wire first.
  ['S5 GOAWAY with an unknown error code, PING behind it', undefined, (sock) => {
    sock.write(goaway(1234));
    sock.write(frame(f.PING, 0, 0, pingData()));
  }],
  ['S6 the same with the PING in front', undefined, (sock) => {
    sock.write(frame(f.PING, 0, 0, pingData()));
    sock.write(goaway(1234));
  }],
  // S7/S8 are h2spec's http2/6.1 connection 2 read off the wire recorder: a request that ends on
  // its HEADERS frame, answered with a body, and a DATA frame that arrives for the stream after it
  // has closed. The tool reads the first frame that follows and stops, so whether the connection
  // credit for the discarded octets goes out BEFORE any refusal frame decides the check.
  ['S7 HEADERS|END_STREAM, answered with a body, then DATA (h2spec 6.1 shape)', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 1,
      Buffer.from('838684418a089d5c0b8170dc79a75d', 'hex')));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
  }, 'body'],
  // ---- does node enforce content-length when the application reads the body? -----------------
  // The shapes above answer without reading, and node refuses none of them. nghttp2 counts the
  // body as it consumes it, so a handler that takes the bytes is the second half of the question:
  // if the verdict changes there, the miss above is a property of the reader and not of the rule.
  // T1 is the short body, T2 the over-long one, T3 the trailer block without END_STREAM.
  ['T1 READ the body: content-length 4, body of 2', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.concat([REQ, Buffer.from('5c0134', 'hex')])));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('ab', 'ascii')));
  }, 'read'],
  ['T2 READ the body: content-length 1, one DATA of 4 with END_STREAM', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.concat([REQ, Buffer.from('5c0131', 'hex')])));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
  }, 'read'],
  ['T3 READ the body: HEADERS, DATA, then a trailer block without END_STREAM', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, REQ));
    sock.write(frame(f.DATA, 0, 1, Buffer.from('test', 'ascii')));
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.from('4085f2b24a84ff026f6b', 'hex')));
  }, 'read'],
  ['T4 READ the body: content-length 4 on a HEADERS that ends the stream', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 1, Buffer.concat([REQ, Buffer.from('5c0134', 'hex')])));
  }, 'read'],

  // ---- U: what a frame AFTER the message has ended is answered with ----------
  // h2spec's http2/6.1 second connection and the 5.1 'closed' / 'half-closed (remote)' arms are
  // this shape: the peer ends the stream with the HEADERS flag and then sends a frame for it
  // anyway. Read off the wire with out/proxy-rec.js, the tool writes HEADERS flags=0x05
  // (END_STREAM and END_HEADERS) on stream 1 with no content-length in the block, then
  // DATA flags=0x01 'test' on the same id.
  ['U1 HEADERS|END_STREAM, then DATA (h2spec 6.1 connection 2, exactly)', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
  }],
  ['U2 the same with content-length 4 in the block', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 1, Buffer.concat([REQ, Buffer.from('5c0134', 'hex')])));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
  }],
  ['U3 HEADERS|END_STREAM, then a second HEADERS', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
    sock.write(frame(f.HEADERS, EOH | EOS, 1, Buffer.from(litField('x', 'y'))));
  }],
  ['U4 HEADERS|END_STREAM, then WINDOW_UPDATE for the same id', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
    sock.write(wu(1, 1024));
  }],
  ['U5 DATA for an id that was never opened at all', undefined, (sock) => {
    sock.write(frame(f.DATA, EOS, 5, Buffer.from('test', 'ascii')));
    sock.write(req(REQ));
  }],
  ['U6 HEADERS, then RST_STREAM from the peer, then DATA for that id', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, REQ));
    sock.write(rst(1, 0));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
  }],
  ['U7 DATA for an id the peer ended with RST_STREAM without ever opening it', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
    sock.write(rst(5, 0));
    sock.write(frame(f.DATA, EOS, 5, Buffer.from('test', 'ascii')));
  }],
  ['U8 DATA after the ANSWERED request ended both halves (peer EOS, we answered)', undefined,
    (sock) => {
      sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
      sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
      sock.write(frame(f.HEADERS, EOH | EOS, 3, REQ));
    }],
  ['U9 a second HEADERS for an id the peer already ended, no DATA in between', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
    sock.write(frame(f.HEADERS, EOH | EOS, 1, Buffer.from(litField('x', 'y'))));
    sock.write(frame(f.HEADERS, EOH | EOS, 3, REQ));
  }],
  // the same rule told by the frame that carried the END_STREAM instead of the HEADERS: the stream
  // is in the table (nothing was answered), so h2lite reaches the state check rather than the
  // history check, and node still has its record. U10-U13 below are the shapes that decide which
  // of the two "the stream is gone" memories may ever answer with a verdict.
  ['U10 HEADERS, DATA|END_STREAM, then a second DATA -- the handler never answers', undefined,
    (sock) => {
      sock.write(frame(f.HEADERS, EOH, 1, REQ));
      sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
      sock.write(frame(f.DATA, EOS, 1, Buffer.from('more', 'ascii')));
    }, false],
  ['U11 HEADERS|END_STREAM then DATA -- the handler never answers', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
  }, false],
  ['U12 HEADERS|END_STREAM, the peer resets it, then DATA for that id', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
    sock.write(rst(1, 0));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
  }],
  ['U13 DATA on stream 0', undefined, (sock) => {
    sock.write(frame(f.DATA, EOS, 0, Buffer.from('test', 'ascii')));
    sock.write(req(REQ));
  }, false],
  ['U14 DATA for an even id above the line', undefined, (sock) => {
    sock.write(frame(f.DATA, EOS, 2, Buffer.from('test', 'ascii')));
    sock.write(req(REQ));
  }, false],
  // the two memories that must NOT answer with a verdict: the id was refused before it was
  // processed, or a reset ended it. U15/U16 say what a repeat HEADERS for such an id gets.
  ['U15 a second HEADERS for a REFUSED id', { maxHeaderListSize: 48 }, (sock) => {
    sock.write(req(Buffer.concat([REQ, lit(1, 46)])));
    sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
  }],
  ['U16 a second HEADERS for an id the peer reset', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, REQ));
    sock.write(rst(1, 0));
    sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
  }],
  // a stream refused for a malformed request, WITHOUT the peer having ended it: the DATA the peer
  // had already put on the wire belongs to the same discard case as U6, not to STREAM_CLOSED.
  ['U17 a stream refused for a bad request, then its DATA (no END_STREAM anywhere)', undefined,
    (sock) => {
      sock.write(frame(f.HEADERS, EOH, 1, Buffer.from([0x82])));
      sock.write(frame(f.DATA, 0, 1, Buffer.from('test', 'ascii')));
      sock.write(frame(f.HEADERS, EOH | EOS, 3, REQ));
    }],
  // An id BELOW the line that this side never saw at all: a peer may skip ids, so "at or below the
  // highest one opened" does not mean "a stream that existed". U18 and U20 ask what the verdict is
  // for such an id once a newer stream has finished, U19 the same for an id that a reset ended.
  ['U18 a HEADERS for an id never opened, below a finished one', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 3, REQ));
    sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
  }],
  ['U19 a HEADERS for an id the peer reset, below a finished one', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, REQ));
    sock.write(rst(1, 0));
    sock.write(frame(f.HEADERS, EOH | EOS, 3, REQ));
    sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
  }],
  ['U20 DATA for an id never opened, below a finished one', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 3, REQ));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
  }],
  // the same question while the newer stream is still live (the peer never ended it): is the lower
  // id still "an unexpected stream identifier" of RFC 9113 8.1 then?
  ['U21 a HEADERS for an id never opened, below a stream that is still live', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 3, REQ));
    sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
  }],
  ['U22 a HEADERS repeating the id of a stream that is still live', undefined, (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, REQ));
    sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
  }]
];

function drive(port, send, respond, gap) {
  return new Promise((resolve) => {
    const seen = [];
    let acc = Buffer.alloc(0);
    let started = false;
    const sock = net.connect(port, '127.0.0.1');
    sock.on('error', (e) => seen.push('sock:' + e.code));
    // The manner of the close is a verdict of its own: a GOAWAY that ends in FIN is "the endpoint
    // closed the connection", while the same GOAWAY followed by ECONNRESET is a reset a peer in
    // the middle of writing gets. Only the second one is visible as sock:ECONNRESET below, so
    // without this listener the two look identical in the transcript.
    sock.on('end', () => seen.push('FIN'));
    sock.on('close', () => seen.push('closed'));
    sock.on('data', (d) => {
      acc = Buffer.concat([acc, d]);
      for (;;) {
        if (acc.length < f.HEADER_LEN) {
          return;
        }
        const len = (acc[0] << 16) | (acc[1] << 8) | acc[2];
        if (acc.length < f.HEADER_LEN + len) {
          return;
        }
        const type = acc[3], flags = acc[4];
        const payload = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
        acc = acc.subarray(f.HEADER_LEN + len);
        if (type === f.SETTINGS && !(flags & f.F_ACK)) {
          // the ACK goes out before anything else, and `send` starts once it is settled
          sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
          if (!started) {
            started = true;
            if (gap) {
              // One write per frame, spaced: node's nghttp2 reports a body that breaks a
              // content-length promise only when the frames arrive as separate reads, and a
              // coalesced batch is what makes the same case answer 'nothing' here. Frames are
              // separate reads on a network, so this is the shape a test has to pin.
              const q = [];
              const sock2 = { write: (b) => q.push(b) };
              send(sock2);
              (function next(i) {
                if (i >= q.length) {
                  return;
                }
                sock.write(q[i]);
                setTimeout(() => next(i + 1), gap);
              })(0);
            } else {
              send(sock);
            }
          }
        } else if (type === f.HEADERS) {
          seen.push('ANSWERED');
        } else if (type === f.SETTINGS) {
          seen.push('ACK');
        } else if (type === f.GOAWAY) {
          seen.push('GOAWAY(' + code(payload.readUInt32BE(4)) + ') last=' + payload.readUInt32BE(0));
        } else if (type === f.RST_STREAM) {
          const c = payload.readUInt32BE(0);
          // RST(NO_ERROR) after an answer is teardown timing, not a verdict
          if (c !== 0 || seen.length === 0) {
            seen.push('RST(' + code(c) + ')');
          }
        } else {
          seen.push('type' + type);
        }
      }
    });
    sock.on('connect', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
    });
    // a spaced batch needs a longer listen: 700 ms is enough for one write, not for six at 60 ms
    setTimeout(() => {
      sock.destroy(); resolve(seen.join(' ') || 'nothing');
    }, gap ? 1800 : 700);
  });
}

(async () => {
  const only = process.argv[2];
  // CASE=A3 -> run one case only, for when a single verdict needs another look
  const pick = process.env.CASE;
  for (const [label, settings, send, respond] of cases) {
    if (pick !== undefined && !label.startsWith(pick)) {
      continue;
    }
    const got = {};
    for (const [name, impl] of [['node', nodeHttp2], ['h2lite', h2lite]]) {
      if (only !== undefined && only !== name) {
        got[name] = '-'; continue;
      }
      const srv = impl.createServer(settings ? { settings } : undefined);
      srv.on('stream', (stream) => {
        stream.on('error', () => {});
        if (respond === false) {
          return;
        } // a case that must stay unanswered
        if (respond === 'read') {
          // consume the body, then answer: nghttp2 counts a promise against the octets it took
          stream.on('data', () => {});
          stream.on('end', () => {
            stream.respond({ ':status': 200 }, { endStream: true });
          });
          return;
        }
        if (respond === 'body') {
          // the check-h2spec target's answer: HEADERS plus a 14-octet body
          stream.respond({ ':status': 200, 'content-type': 'text/plain' });
          stream.end('h2spec target\n');
          return;
        }
        stream.respond({ ':status': 200 }, { endStream: true });
      });
      srv.on('error', () => {});
      const port = await listen(srv);
      got[name] = await drive(port, send, respond, process.env.GAP ? +process.env.GAP : 0);
      await new Promise((r) => srv.close(r));
      if (pick !== undefined) {
        console.log(`    ${name} advertises maxFrameSize=`
          + `${impl.getDefaultSettings().maxFrameSize} listSize=`
          + `${(srv.settings || {}).maxHeaderListSize}`);
      }
    }
    console.log(`\n${label}\n    node   -> ${got.node}\n    h2lite -> ${got.h2lite}`
      + (got.node === got.h2lite ? '' : '   <<< DIFFERS'));
  }
})();
