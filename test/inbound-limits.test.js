// Inbound frame and header-list size limits: what a server answers when a frame is too big,
// when a header list is too big, when HPACK announces a literal it does not send, and when it
// does not know the frame type. Every expectation here was measured against a live node:http2
// server (node v26.12.1) with tools/probe-inbound-limits.js, and the same expectation is then
// asserted against both stacks, so a change in either one fails here instead of turning into an
// interop difference somebody finds in production.
//
// The measured rules, and where h2lean used to disagree:
//
//   * a frame longer than the advertised SETTINGS_MAX_FRAME_SIZE is a connection error, and the
//     boundary is INCLUSIVE: 16 384 octets of DATA are served, 16 385 bring GOAWAY
//     (FRAME_SIZE_ERROR). SETTINGS and PING are never bounded by it: RFC 9113 4.2 sizes a frame
//     by the setting AND by "any limit defined for the frame type", and those two have their own
//     (6-octet entries in 6.5.1, a fixed 8-octet payload in 6.7).
//   * maxHeaderListSize is compared against the HPACK ENTRY size of the list, not against the
//     octets of the block and not against the bare sum of the field octets: RFC 7541 4.1 puts an
//     entry at name + value + 32, and that is what node counts. Measured, a 4-field block whose
//     names and values total 46 octets is REFUSED under a limit of 48 (46 + 4*32 = 174), and a
//     5-field block is served at 87 decoded octets and refused at 93 under a limit of 250, which
//     puts the line exactly at 250 - 5*32 = 90. h2lean compared the plain sum and served three
//     of those blocks, and it also compared the COMPRESSED length of the block while assembling
//     it -- which turned one oversized request into GOAWAY(PROTOCOL_ERROR) for the whole
//     connection.
//   * the answer to an oversized header LIST is RST_STREAM(ENHANCE_YOUR_CALM) -- RFC 9113 6.5.2
//     makes the advertised limit advisory and 7 gives the code for a peer generating excessive
//     load -- and it is per stream: after refusing stream 1, node answers a legal stream 3 on the
//     same connection. h2lean refused with PROTOCOL_ERROR.
//   * a literal whose length prefix announces more than the frame contains is a HPACK failure:
//     GOAWAY(COMPRESSION_ERROR), whether the claim is 2 000 000 or 2 000 000 000 octets. Both
//     stacks answer the same code and the same last-stream id: nghttp2 opens the stream when the
//     HEADERS frame is accepted, before the block is decoded, so the GOAWAY names the stream of
//     the refused frame -- pinned as its own test below.
//   * an unknown frame type is DISCARDED (RFC 9113 5.5): type 10 is ALTSVC (RFC 7807), and the
//     request that follows it is served. Type 99 and 0xff behave the same.
//   * one frame can break two rules at once, and the two stacks then disagree about which rule
//     they report: a header frame that is over SETTINGS_MAX_FRAME_SIZE and does not decode as
//     HPACK gets COMPRESSION_ERROR from node and FRAME_SIZE_ERROR from h2lean. Pinned below.
//
// The second half of the file, added later and measured the same way, covers the frame-state and
// message-framing rules of RFC 9113 5.1, 6.5, 6.7, 8.1, 8.2 and RFC 7541 6.3 -- each of them a
// check h2spec failed for h2lean alone:
//
//   * a connection control frame names no stream: SETTINGS (6.5) and PING (6.7) on stream 1 or 3
//     are GOAWAY(PROTOCOL_ERROR), and so is RST_STREAM with stream id 0 -- a reset always names a
//     stream. A SETTINGS ACK carries no entries: one octet of payload behind the ACK flag is
//     GOAWAY(FRAME_SIZE_ERROR), and a PING ACK whose payload is not eight octets is the same.
//   * a frame that must be five octets long is five octets long: PRIORITY (6.2, and RFC 9218
//     keeps the frame's size) with a 4-octet payload, and HEADERS with the PRIORITY flag and a
//     truncated priority block, are both GOAWAY(FRAME_SIZE_ERROR).
//   * SETTINGS_ENABLE_PUSH is a boolean (6.5.2): a value of 2 is GOAWAY(PROTOCOL_ERROR), the same
//     code the other defined settings use for an out-of-range value.
//   * a stream in the 'idle' state -- an id above the highest one this side has ever been told
//     about -- takes neither WINDOW_UPDATE (6.9 allows it in 'open' and 'half-closed (local)') nor
//     RST_STREAM (6.4): GOAWAY(PROTOCOL_ERROR) both times. Below that line the frame is for a
//     stream that existed and finished, and 5.1 says discard it -- which is why the reset of id 1
//     after a request was served costs nothing.
//   * a DATA or HEADERS naming a stream whose message the peer already ended is a CONNECTION error
//     of type STREAM_CLOSED, whether the stream is still in the table or has just left it (6.1 reads
//     the half-closed(remote) DATA as a stream error, but node ends the connection on both shapes and
//     h2spec accepts the GOAWAY); a frame naming an id that a reset ended, that this side refused, or
//     that the peer skipped, is discarded, and DATA on stream 0, on an even id or on an id above the
//     highest one opened is GOAWAY(PROTOCOL_ERROR).
//   * PRIORITY: naming itself as the dependency is dropped by both (RFC 9113 5.3 deprecated the
//     7540 self-dependency rule with the rest of the signal), but a PRIORITY on stream 0 is where
//     h2lean follows the RFC over node -- 6.3 mandates GOAWAY(PROTOCOL_ERROR) and node drops it
//     silently, the deviation h2spec's 6.3 check flags.
//   * a trailer block has to end the message: a second HEADERS without END_STREAM is
//     RST_STREAM(PROTOCOL_ERROR) and the connection lives, and the refusal happens only once the
//     application has read -- with a handler that never reads the body, node reports nothing.
//   * content-length is a promise with two halves: too FEW octets by the time the message ends is
//     RST_STREAM(PROTOCOL_ERROR) for that stream, too MANY is GOAWAY(INTERNAL_ERROR) for the whole
//     connection, and a promise of a body on a HEADERS frame that already ends the stream is
//     refused before the application ever sees it.
//   * a dynamic-table size update belongs at the START of a header block (RFC 7541 6.3): one that
//     follows a field line is GOAWAY(COMPRESSION_ERROR), whether or not the size it announces is
//     the size the table already has.
//   * a peer GOAWAY carrying a code no version of the spec defines is still a goodbye: the session
//     closes and answers with GOAWAY(NO_ERROR) of its own, not with the code it was given
//     (RFC 9113 6.8, h2spec http2/7) -- and how fast the socket then dies is §10's problem.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const f = require('../lib/frames');
const h2lean = require('../index');
const nodeHttp2 = require('node:http2');
const { listen, closeServer } = require('./diff-harness');

const CODE_NAMES = { 0: 'NO_ERROR', 1: 'PROTOCOL_ERROR', 2: 'INTERNAL_ERROR', 5: 'STREAM_CLOSED',
  6: 'FRAME_SIZE_ERROR', 7: 'REFUSED_STREAM', 9: 'COMPRESSION_ERROR',
  11: 'ENHANCE_YOUR_CALM' };
const name = (n) => CODE_NAMES[n] !== undefined ? CODE_NAMES[n] : String(n);

// :method GET, :path /, :scheme http, :authority localhost -- 14 octets on the wire, 10+6+11+19
// = 46 octets of entry content before the per-entry overhead.
const REQ = Buffer.from([0x82, 0x84, 0x86, 0x01, 9, ...Buffer.from('localhost', 'latin1')]);
// one wire octet, 15 + 13 = 28 octets of content: the static line accept-encoding
const STATIC_LINE = Buffer.from([0x80 | 16]);
const EOH = f.F_END_HEADERS, EOS = f.F_END_STREAM;

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

const req = (block) => frame(f.HEADERS, EOH, 1, block);
const big = (n) => Buffer.alloc(n, 0x61);

// RFC 7541 5.1 -- the integer representation, here with the 7-bit prefix an indexed field uses
// (6.1), so a length over 126 costs two octets instead of wrapping into one.
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

// a literal field line, never indexed, no Huffman: nameLen + valueLen octets of content
function lit(nameLen, valueLen) {
  return Buffer.concat([
    Buffer.from([0x00]), int7(0x00, nameLen), Buffer.from('x'.repeat(nameLen), 'latin1'),
    int7(0x00, valueLen), Buffer.from('y'.repeat(valueLen), 'latin1')
  ]);
}

// a field line whose value length prefix announces `claimed` octets while `sent` follow
function lyingLength(claimed, sent) {
  return Buffer.concat([
    Buffer.from([0x00]), int7(0x00, 1), Buffer.from('n', 'latin1'),
    int7(0x00, claimed), Buffer.from('z'.repeat(sent), 'latin1')
  ]);
}

// RFC 7541 5.1 with an explicit prefix width. int7 hard-codes the 7-bit case an indexed field
// uses; a 5-bit integer (an HPACK table size update, 6.3) carries only 31 in the first octet, so
// reusing int7 there writes a value 96 lower than intended.
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

// a field line with a real name, never indexed: HPACK 6.2.2, a name index of 0 means the name is
// the string literal that follows
function litField(name, value) {
  const n = Buffer.from(name, 'latin1'), v = Buffer.from(value, 'latin1');
  return Buffer.concat([
    Buffer.from([0x00]), int7(0x00, n.length), n, int7(0x00, v.length), v
  ]);
}

// 'content-length: <v>' as one incremental-indexing line: 0x5c is 0101 1100, the 6-bit prefix with
// the static index 28 the field lives at, then the value's length prefix and its digits.
function CL(v) {
  const s = Buffer.from(String(v), 'latin1');
  return Buffer.concat([Buffer.from([0x5c]), int7(0x00, s.length), s]);
}

// the frame payloads the state rules are written with: RFC 9113 6 lays all of these out
function wu(sid, inc) {
  const p = Buffer.alloc(4); p.writeUInt32BE(inc, 0);
  return frame(f.WINDOW_UPDATE, 0, sid, p);
}
function rst(sid, c) {
  const p = Buffer.alloc(4); p.writeUInt32BE(c, 0);
  return frame(f.RST_STREAM, 0, sid, p);
}
// the 5-octet priority block of RFC 9113 6.2 / RFC 9218: dependency in the high 31 bits, weight - 1
function dep32(dep, weight) {
  const p = Buffer.alloc(5); p.writeUInt32BE(dep, 0); p[4] = weight;
  return p;
}
const pri = (sid, dep, weight) => frame(f.PRIORITY, 0, sid, dep32(dep, weight));
function setting(id, value) {
  const p = Buffer.alloc(6); p.writeUInt16BE(id, 0); p.writeUInt32BE(value, 2);
  return p;
}

// Sends the frames after the handshake and returns the VERDICTS only: an answer, a stream
// reset, a connection error. What is deliberately left out of the transcript: the SETTINGS ACK
// (always there), WINDOW_UPDATE (credit bookkeeping is the subject of
// test/backpressure.test.js, and the two stacks coalesce it differently), RST_STREAM(NO_ERROR)
// after an answer (teardown of a stream the peer already finished), and which side's FIN
// arrives first after a GOAWAY (pure timing).
function drive(port, send, deadline = 350) {
  return new Promise((resolve) => {
    const seen = [];
    const lasts = [];
    let acc = Buffer.alloc(0);
    let started = false;
    let settled = false;
    let timer = null;
    // Whether the peer killed the connection instead of answering. A close with unread
    // inbound data is an RST, and an RST discards whatever the peer has already sent us --
    // so a verdict that was on the wire can be lost before this side reads it. The signal
    // is not the verdict: it is the socket error that comes with it.
    let reset = false;
    // A GOAWAY ends the connection, so the transcript cannot grow after it: resolving on the
    // frame rather than on the deadline is what makes the fatal cases independent of how long the
    // server takes to read a large payload. Waiting for the deadline was not: reading a 1.5 MB
    // literal can take the better part of a second, and a run then reported `nothing` for a
    // GOAWAY the server had already sent.
    const finish = () => {
      if (settled) {
        return;
      }
      settled = true;
      if (timer !== null) {
        clearTimeout(timer);
      }
      sock.destroy();
      resolve({ verdict: seen.join(' ') || 'nothing', last: lasts.length ? lasts[0] : null, reset });
    };
    const sock = net.connect(port, '127.0.0.1');
    sock.on('error', (e) => {
      // A reset is the peer's answer, not silence: the case has to be run again, because the
      // frame it is about may have been on the wire when the RST came. Any other error is
      // the deadline's business.
      if (e.code !== 'ECONNRESET' && e.code !== 'EPIPE') {
        return;
      }
      reset = true;
      finish();
    });
    sock.on('data', (d) => {
      acc = Buffer.concat([acc, d]);
      for (;;) {
        if (settled || acc.length < f.HEADER_LEN) {
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
          // the ACK goes out before anything else: a frame written between the handshake and
          // the test frames would change which frame the server sees first
          sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
          if (!started) {
            started = true; send(sock);
          }
        } else if (type === f.HEADERS) {
          seen.push('ANSWERED');
        } else if (type === f.GOAWAY) {
          seen.push('GOAWAY(' + name(payload.readUInt32BE(4)) + ')');
          lasts.push(payload.readUInt32BE(0));
          finish();
        } else if (type === f.RST_STREAM) {
          const c = payload.readUInt32BE(0);
          if (c !== 0) {
            seen.push('RST(' + name(c) + ')');
          }
        }
      }
    });
    sock.on('connect', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
    });
    // the deadline is the fallback for the cases where the server is expected to say NOTHING: a
    // verdict that never arrives has to report as `nothing` rather than hang the file. Cases that
    // hand the server a payload of their own size keep a raised one, so that a slow read is not
    // mistaken for silence -- and every other case takes 350 ms. A raised deadline costs nothing
    // on the path that works: the GOAWAY cases resolve on the frame, so the two seconds only bound
    // the case where the verdict never arrives (a 1.5 MB literal can take the better part of a
    // second to read, and calling that silence was the flake).
    timer = setTimeout(finish, deadline);
  });
}

// One server, one raw client, the transcript. `respond: false` keeps the handler from answering
// at all, which is how a case that ends the connection is kept free of an answer race: with a
// responding handler, h2lean gets its HEADERS out before the fatal frame is read and node does
// not, and the difference is socket scheduling, not protocol. `respond: 'read'` is the third
// shape, and the one the message-framing rules need: nghttp2 measures a body against the octets it
// has CONSUMED, so a handler that answers without reading the request never notices a promise the
// peer broke (measured: content-length 4 with a body of 2 is refused by node only once the handler
// has a 'data' listener). The application here reads and answers in 'end', which is what a real
// upload handler does.
async function once(impl, settings, respond, send, deadline = 350) {
  // A verdict that never arrives because the peer RESET the connection is not silence: the
  // frame was on the wire, and the RST of a close with unread inbound data discarded it
  // before this side read it. Node refuses a 1.5 MB literal as soon as it has read the
  // length prefix -- measured, the server had read between 228 690 and 1 500 030 octets of
  // the frame when it refused, so up to 1 172 776 of them were unread at the close and the
  // close was an RST; the GOAWAY was lost in 1 of 20 rounds (tools/probe-big-lit.js). h2lean
  // reads the whole frame before it decodes it, so it never closes with unread data and its
  // verdict always arrives.
  // The round is run again when the socket was reset, and the assertion stays strict: the
  // verdict has to be the frame.
  let got = null;
  for (let attempt = 0; attempt < 4; attempt++) {
    const srv = impl.createServer(settings ? { settings } : undefined);
    srv.on('stream', (stream) => {
      stream.on('error', () => {});
      if (respond === 'read') {
        stream.on('data', () => {});
        stream.on('end', () => stream.respond({ ':status': 200 }, { endStream: true }));
        return;
      }
      if (respond) {
        stream.respond({ ':status': 200 }, { endStream: true });
      }
    });
    srv.on('error', () => {});
    const port = await listen(srv);
    got = await drive(port, send, deadline);
    await closeServer(srv);
    if (got.verdict !== 'nothing' || !got.reset) {
      return got;
    }
  }
  return got;
}

// node's bundled nghttp2 changed verdicts between the v20/22 and the v24+ bundles -- measured
// on v20.19.5, v22.21.1 and v26.12.1 (the full matrix, out/probe-verdicts-node20.js, kept as a
// scratch because the numbers are the point): the old bundle answers INTERNAL_ERROR where the
// new one maps the protocol failure to PROTOCOL_ERROR, still enforces RFC 7540 5.3.1's
// self-dependency rule for PRIORITY (the new one follows RFC 9218 and drops the frame), and
// reads a too-long content-length as a stream error rather than a connection error. h2lean
// pins the v24+ verdict everywhere; `nodeWant`, when given, is the measured v20/22 verdict
// the node arm may answer instead.
async function both(label, settings, respond, send, want, deadline = 350, nodeWant = want) {
  const node = await once(nodeHttp2, settings, respond, send, deadline);
  const ours = await once(h2lean, settings, respond, send, deadline);
  assert.ok(node.verdict === want || node.verdict === nodeWant,
    `node: ${label} -> ${node.verdict}`);
  assert.strictEqual(ours.verdict, want,
    `h2lean: ${label} -> ${ours.verdict}, node -> ${node.verdict}`);
}

// The third kind of expectation: a frame the peer is allowed to still have on the wire, for which
// the only verdict is silence. Both stacks must say nothing about it -- no GOAWAY, no reset --
// while how many answers escape in front of the silence is write timing, which is why the answer
// count is deliberately not compared here.
async function noVerdictTest(label, respond, send, settings, deadline) {
  const node = await once(nodeHttp2, settings, respond, send, deadline);
  const ours = await once(h2lean, settings, respond, send, deadline);
  for (const [who, got] of [['node', node], ['h2lean', ours]]) {
    assert.doesNotMatch(got.verdict, /GOAWAY|RST\(/,
      `${who}: ${label} -> ${got.verdict} (RFC 9113 5.1 says discard the frame)`);
  }
}

// ---- SETTINGS_MAX_FRAME_SIZE ---------------------------------------------------------------

test('DATA of exactly the advertised max frame size is served', async () => {
  await both('16384-octet DATA', undefined, true, (sock) => {
    sock.write(req(REQ));
    sock.write(frame(f.DATA, 0, 1, big(16384)));
    sock.write(frame(f.DATA, EOS, 1, Buffer.alloc(0)));
  }, 'ANSWERED');
});

test('DATA one octet over the max frame size ends the connection', async () => {
  await both('16385-octet DATA', undefined, false, (sock) => {
    sock.write(req(REQ));
    sock.write(frame(f.DATA, EOS, 1, big(16385)));
  }, 'GOAWAY(FRAME_SIZE_ERROR)');
});

test('an over-size frame on stream 0 is the same connection error', async () => {
  await both('16385-octet DATA on stream 0', undefined, false, (sock) => {
    sock.write(frame(f.DATA, EOS, 0, big(16385)));
  }, 'GOAWAY(FRAME_SIZE_ERROR)');
});

test('WINDOW_UPDATE over the max frame size ends the connection', async () => {
  await both('16385-octet WINDOW_UPDATE', undefined, false, (sock) => {
    const p = Buffer.alloc(16385); p.writeUInt32BE(1, 0);
    sock.write(frame(f.WINDOW_UPDATE, 0, 0, p));
    sock.write(req(REQ));
  }, 'GOAWAY(FRAME_SIZE_ERROR)');
});

test('HEADERS over the max frame size ends the connection, HPACK and all', async () => {
  // 4 000 literal lines of one-octet name and one-octet value: five octets each, 20 014 in all,
  // and a block that IS valid HPACK -- so the only rule that can refuse it is the frame size.
  // maxHeaderListSize is raised out of the way (4 001 fields cost 4 001*34 = 136 034 entry
  // octets, well under the limit), which is what makes this a test of A5 rather than of B.
  const line = lit(1, 1);
  assert.strictEqual(line.length, 5, 'a literal line is 5 octets by construction');
  const parts = [REQ];
  for (let i = 0; i < 4000; i++) {
    parts.push(line);
  }
  const block = Buffer.concat(parts);
  assert.strictEqual(block.length, 20014, 'the block is 20 014 octets, over the 16 384 limit');
  await both('oversized HEADERS', { maxHeaderListSize: 1000000 }, false,
    (sock) => {
      sock.write(frame(f.HEADERS, EOH | EOS, 1, block));
    },
    'GOAWAY(FRAME_SIZE_ERROR)', 2000);
});

// ---- padded DATA frames (RFC 9113 6.1) --------------------------------------------------------

test('a padded DATA frame whose padding does not fit ends the connection', async () => {
  // RFC 9113 6.1: "If the length of the padding is the length of the frame payload
  // or greater, the recipient MUST treat this as a connection error of type
  // PROTOCOL_ERROR." The payload here is four octets -- the pad length and three
  // more -- and the padding claims two hundred of them.
  await both('a pad length of 200 over a 4-octet payload', undefined, false, (sock) => {
    sock.write(req(REQ));
    sock.write(frame(f.DATA, f.F_PADDED | EOS, 1,
      Buffer.concat([Buffer.from([200]), Buffer.alloc(3)])));
  }, 'GOAWAY(PROTOCOL_ERROR)');
});

test('a padded DATA frame with an empty payload ends the connection', async () => {
  // A padded frame with no payload has no pad length to read, so it is malformed
  // before any of the padding rules apply to it. Node answers the same code
  // (measured, tools/probe-pad-bad.js); h2lean used to accept the frame silently.
  await both('PADDED with an empty payload', undefined, false, (sock) => {
    sock.write(req(REQ));
    sock.write(frame(f.DATA, f.F_PADDED | EOS, 1, Buffer.alloc(0)));
  }, 'GOAWAY(PROTOCOL_ERROR)');
});

test('a padded DATA frame whose padding fits is served', async () => {
  // The padding is exactly the rest of the payload, so the frame carries no data
  // at all -- and it is legal: 6.1 refuses a padding that is the length of the
  // payload or greater, and this one is one octet shorter.
  await both('a pad length equal to the rest of the payload', undefined, 'read', (sock) => {
    sock.write(req(REQ));
    sock.write(frame(f.DATA, f.F_PADDED | EOS, 1,
      Buffer.concat([Buffer.from([3]), Buffer.alloc(3)])));
  }, 'ANSWERED');
});

// ---- padded HEADERS frames (RFC 9113 6.2) --------------------------------------------------

test('a padded HEADERS frame whose padding does not fit ends the connection', async () => {
  // RFC 9113 6.2: "If the length of the padding is the length of the frame payload
  // or greater, the recipient MUST treat this as a connection error of type
  // PROTOCOL_ERROR." The payload here is six octets -- the pad length and the
  // five of the block -- and the padding claims two hundred of them. Node answers
  // the same code (measured, tools/probe-hdr-pad.js: "HEADERS: invalid padding").
  await both('a pad length of 200 over the block', undefined, false, (sock) => {
    sock.write(frame(f.HEADERS, f.F_PADDED | EOH, 1,
      Buffer.concat([Buffer.from([200]), REQ])));
  }, 'GOAWAY(PROTOCOL_ERROR)');
});

test('a padded HEADERS frame with an empty payload ends the connection', async () => {
  // A padded frame with no payload has no pad length to read -- the octet it would
  // be read from is not this frame's -- so it is malformed before any of the padding
  // rules apply to it. Node answers the same code (measured, tools/probe-hdr-pad.js:
  // "HEADERS: insufficient padding space"); h2lean used to read the octet after the
  // frame's payload and answered GOAWAY(INTERNAL_ERROR) with a Buffer RangeError as
  // its debug data.
  await both('a padded HEADERS with an empty payload', undefined, false, (sock) => {
    sock.write(frame(f.HEADERS, f.F_PADDED | EOH, 1, Buffer.alloc(0)));
  }, 'GOAWAY(PROTOCOL_ERROR)');
});

test('a padded HEADERS frame whose padding fits is served', async () => {
  // The shape RFC 9113 6.2 describes and h2spec's generic/3.2 sends: the pad length
  // first, the block in the middle, the padding at the end. The padding is not part
  // of the block -- "the remainder of the frame payload is the header block
  // fragment" -- and feeding it to the HPACK decoder is what made h2lean answer
  // GOAWAY(COMPRESSION_ERROR) to a valid frame while node served the request
  // (measured, tools/probe-hdr-pad.js: "the RFC shape: pad length, block, padding";
  // the same frame is what h2spec's generic/3.2 sends, and that check was the one
  // h2lean failed).
  await both('a padded HEADERS with the block in the middle', undefined, 'read', (sock) => {
    sock.write(frame(f.HEADERS, f.F_PADDED | EOH | EOS, 1,
      Buffer.concat([Buffer.from([8]), REQ, Buffer.alloc(8)])));
  }, 'ANSWERED');
});

// ---- SETTINGS_MAX_HEADER_LIST_SIZE ---------------------------------------------------------

test('a header list is measured as HPACK entries, not as the block or the bare field octets',
  async () => {
    // limit 48 against REQ alone: 14 octets on the wire, 46 octets of names and values, and
    // 46 + 4*32 = 174 entry octets. Only the third number is over the limit, and it is the one
    // node applies. Refusing here is also what keeps a lowered maxHeaderListSize honest: the
    // count is per field, so a client cannot hide a large list behind a small block.
    await both('4 fields, 46 octets, limit 48', { maxHeaderListSize: 48 }, true,
      (sock) => {
        sock.write(req(REQ));
      }, 'RST(ENHANCE_YOUR_CALM)');

    // the two sides of the line for a 5-field block: 87 content octets (87 + 5*32 = 247) served
    // and 93 (253) refused under a limit of 250 -- the boundary is exactly 250 - 160.
    await both('5 fields, 87 octets, limit 250', { maxHeaderListSize: 250 }, true,
      (sock) => {
        sock.write(req(Buffer.concat([REQ, lit(1, 40)])));
      }, 'ANSWERED');
    await both('5 fields, 93 octets, limit 250', { maxHeaderListSize: 250 }, true,
      (sock) => {
        sock.write(req(Buffer.concat([REQ, lit(1, 46)])));
      },
      'RST(ENHANCE_YOUR_CALM)');

    // eight static-table lines cost eight wire octets and 224 entry octets: a client that
    // repeats indexed fields gets no free ride past the limit.
    await both('8 fields, 158 octets, limit 250', { maxHeaderListSize: 250 }, true,
      (sock) => {
        sock.write(req(Buffer.concat([REQ, STATIC_LINE, STATIC_LINE, STATIC_LINE, STATIC_LINE,
          STATIC_LINE, STATIC_LINE, STATIC_LINE])));
      }, 'RST(ENHANCE_YOUR_CALM)');
  });

test('an oversized header list refuses the stream and keeps the connection', async () => {
  // RFC 9113 6.5.2 and 7 make this the application's problem to solve, not the connection's: the
  // refusal is RST_STREAM, and the next request on the same session must be served. Before
  // this was measured, h2lean's assembly-time check on the compressed block length answered it
  // with GOAWAY(PROTOCOL_ERROR) -- one bad request taking every other client with it.
  await both('oversized stream 1 then a legal stream 3', { maxHeaderListSize: 250 }, true,
    (sock) => {
      sock.write(req(Buffer.concat([REQ, lit(1, 46)])));
      sock.write(frame(f.HEADERS, EOH | EOS, 3, REQ));
    }, 'RST(ENHANCE_YOUR_CALM) ANSWERED');
});

test('DATA already on its way to a refused stream is ignored, not an error', async () => {
  // A refusal cannot un-send what the peer already put on the wire, so the refused id has to be
  // remembered: the DATA for stream 1 arrives after the RST and must be dropped rather than
  // turn into a connection error, and stream 3 must still be served. Measured on node, and the
  // refused-history bound two tests below is what keeps that memory from growing without end.
  await both('oversized stream 1, its DATA, then a legal stream 3',
    { maxHeaderListSize: 250 }, true, (sock) => {
      sock.write(req(Buffer.concat([REQ, lit(1, 46)])));
      sock.write(frame(f.DATA, EOS, 1, Buffer.from('body', 'ascii')));
      sock.write(frame(f.HEADERS, EOH | EOS, 3, REQ));
    }, 'RST(ENHANCE_YOUR_CALM) ANSWERED');
});

test('the limit is re-checked per request, not once per shape', async () => {
  // The same oversized block twice in a row must be refused twice: the shape cache stores the
  // entry count with the shape, so a repeat that skips the decode cannot skip the refusal.
  await both('the same oversized block twice', { maxHeaderListSize: 48 }, true, (sock) => {
    sock.write(req(REQ));
    sock.write(frame(f.HEADERS, EOH | EOS, 3, REQ));
  }, 'RST(ENHANCE_YOUR_CALM) RST(ENHANCE_YOUR_CALM)');
});

test('a legal header list under the default limit is served by both', async () => {
  await both('88 wire / 114 content octets, default limit', undefined, true,
    (sock) => {
      sock.write(req(Buffer.concat([REQ, lit(45, 23)])));
    }, 'ANSWERED');
});

// ---- HPACK literals ------------------------------------------------------------------------

test('a 6 000-octet literal value is served, a 1 500 000-octet one is not', async () => {
  // The per-frame and per-list limits are not what bounds ONE literal, and nghttp2's old
  // NGHTTP2_HD_MAX_LITERAL of 4096 octets is gone: measured, node serves a 6 000-octet value.
  // Both stacks end the connection on a value of 1 500 000 octets -- COMPRESSION_ERROR, the
  // same code as the lying length prefix below. maxFrameSize is raised to 2 MiB so the frame
  // itself is legal, and maxHeaderListSize to 10 000 000 so the refusal cannot be credited to
  // the list size: 1 500 000 + 32 + 174 entry octets is well under it.
  const settings = { maxFrameSize: 2097152, maxHeaderListSize: 10000000 };
  await both('6 000-octet literal', settings, true,
    (sock) => {
      sock.write(req(Buffer.concat([REQ, lit(1, 6000)])));
    }, 'ANSWERED');
  await both('1 500 000-octet literal', settings, false,
    (sock) => {
      sock.write(req(Buffer.concat([REQ, lit(1, 1500000)])));
    },
    'GOAWAY(COMPRESSION_ERROR)', 2000);
});

test('a literal that announces 2 000 000 octets and sends 4 is a connection error', async () => {
  // The value length prefix is the peer's promise about the octets that follow it. Announcing
  // 2 000 000 (or 2 000 000 000) and sending four is a HPACK failure, and both stacks end the
  // connection with COMPRESSION_ERROR rather than trusting the number -- trusting it is how one
  // length prefix becomes an allocation of two megabytes per connection.
  for (const claimed of [2000000, 2000000000]) {
    await both(`lying literal length ${claimed}`, undefined, false, (sock) => {
      sock.write(req(Buffer.concat([REQ, lyingLength(claimed, 4)])));
    }, 'GOAWAY(COMPRESSION_ERROR)');
  }
});

// ---- connection control frames name no stream (RFC 9113 6.5, 6.7) ---------------------------

test('SETTINGS and PING on a stream id, and RST_STREAM on stream 0, end the connection',
  async () => {
    // Every one of these was answered as though the frame had been addressed to that stream: a
    // SETTINGS on stream 1 got an ACK and lived on, a PING on stream 3 got an ACK with sid 3 (an
    // ill-formed answer the peer may not even parse), a reset naming stream 0 was looked up in the
    // stream table, missed, and dropped. Measured on node, and pinned here as h2spec's http2/6.5,
    // 6.7 and 6.4: all three are connection errors with PROTOCOL_ERROR -- the frame is not about a
    // stream, so there is no stream to reset and the session is what the lie is about.
    await both('SETTINGS on stream 1', undefined, false, (sock) => {
      sock.write(frame(f.SETTINGS, 0, 1, setting(0x3, 65535)));
      sock.write(req(REQ));
    }, 'GOAWAY(PROTOCOL_ERROR)', 350, 'GOAWAY(INTERNAL_ERROR)');
    await both('PING on stream 3', undefined, false, (sock) => {
      sock.write(frame(f.PING, 0, 3, Buffer.alloc(8)));
      sock.write(req(REQ));
    }, 'GOAWAY(PROTOCOL_ERROR)', 350, 'GOAWAY(INTERNAL_ERROR)');
    await both('PING with the ACK flag on stream 3', undefined, false, (sock) => {
      sock.write(frame(f.PING, f.F_ACK, 3, Buffer.alloc(8)));
      sock.write(req(REQ));
    }, 'GOAWAY(PROTOCOL_ERROR)', 350, 'GOAWAY(INTERNAL_ERROR)');
    await both('RST_STREAM with stream id 0', undefined, false, (sock) => {
      sock.write(rst(0, 0));
      sock.write(req(REQ));
    }, 'GOAWAY(PROTOCOL_ERROR)', 350, 'GOAWAY(INTERNAL_ERROR)');
  });

test('an ACK carries no payload: SETTINGS|ACK and PING|ACK are sized', async () => {
  // 6.5.1 and 6.7 both define the ACK as the same frame with an empty (SETTINGS) or exactly
  // eight-octet (PING) payload. Measured on node: one octet behind SETTINGS|ACK and a four-octet
  // PING|ACK are FRAME_SIZE_ERROR, not something to be lenient about -- an ACK that carries entries
  // would have to be either applied or ignored, and a peer that sends one is not speaking the
  // version of the protocol this side answers.
  await both('SETTINGS|ACK with one octet of payload', undefined, false, (sock) => {
    sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(1)));
    sock.write(req(REQ));
  }, 'GOAWAY(FRAME_SIZE_ERROR)');
  await both('PING|ACK with a 4-octet payload', undefined, false, (sock) => {
    sock.write(frame(f.PING, f.F_ACK, 0, Buffer.alloc(4)));
    sock.write(req(REQ));
  }, 'GOAWAY(FRAME_SIZE_ERROR)');
});

test('SETTINGS_ENABLE_PUSH is a boolean', async () => {
  // 6.5.2 defines it as 0 or 1 and 6.5.1 makes any other value of a defined setting a
  // PROTOCOL_ERROR. Measured on node for h2spec's http2/6.5.2: a value of 2 ends the connection,
  // the way the two settings with an explicit range already did (MAX_FRAME_SIZE and
  // INITIAL_WINDOW_SIZE), while 0 and 1 are accepted.
  await both('SETTINGS_ENABLE_PUSH=2', undefined, false, (sock) => {
    sock.write(frame(f.SETTINGS, 0, 0, setting(0x2, 2)));
    sock.write(req(REQ));
  }, 'GOAWAY(PROTOCOL_ERROR)', 350, 'GOAWAY(INTERNAL_ERROR)');
  await both('SETTINGS_ENABLE_PUSH=1 is accepted', undefined, true, (sock) => {
    sock.write(frame(f.SETTINGS, 0, 0, setting(0x2, 1)));
    sock.write(req(REQ));
  }, 'ANSWERED');
});

// ---- the frame-state rules of RFC 9113 5.1 ---------------------------------------------------

test('WINDOW_UPDATE and RST_STREAM for a stream in the idle state end the connection',
  async () => {
    // 5.1 defines 'idle' as an id above the highest one this side has been told about, and the two
    // frames may not be sent there: 6.9 allows WINDOW_UPDATE only in 'open' and 'half-closed
    // (local)', 6.4 gives RST_STREAM as the way to abort a stream that exists. Measured on node
    // (h2spec http2/5.1 and 6.9), each is GOAWAY(PROTOCOL_ERROR). h2lean had been written to the
    // "unknown id" rule of 5.1 -- ignore the frame -- and answered nothing at all, which is the
    // right verdict only for an id that was once live.
    await both('WINDOW_UPDATE for stream 3, nothing opened past 1', undefined, false, (sock) => {
      sock.write(wu(3, 1024));
      sock.write(req(REQ));
    }, 'GOAWAY(PROTOCOL_ERROR)', 350, 'GOAWAY(INTERNAL_ERROR)');
    await both('RST_STREAM for stream 3, nothing opened past 1', undefined, false, (sock) => {
      sock.write(rst(3, 0));
      sock.write(req(REQ));
    }, 'GOAWAY(PROTOCOL_ERROR)', 350, 'GOAWAY(INTERNAL_ERROR)');
    // The line is the highest id the peer has opened, not whether a stream object still exists:
    // after stream 1 was served, its id is at or below that line and a late WINDOW_UPDATE or reset
    // for it is 5.1's "discard" case -- no verdict of any kind, the connection lives. Whether the
    // answer to stream 1 itself still goes out is NOT part of that rule and is not asserted here:
    // measured over six runs of each shape written as ONE coalesced write (out/probe-discard.js,
    // kept as a scratch because the number is the point), node answers the WINDOW_UPDATE shape 6/6
    // and discards the answer to the RST_STREAM shape 6/6 while h2lean answers both 6/6 -- the same
    // queued-answer race as the KNOWN DIVERGENCE at the end of this file -- and under full-suite
    // load node's side of it flips. What neither stack may do is treat the frame as an error.
    await noVerdictTest('WINDOW_UPDATE for a served stream 1', true, (sock) => {
      sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
      sock.write(wu(1, 1024));
    });
    await noVerdictTest('RST_STREAM for a served stream 1', true, (sock) => {
      sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
      sock.write(rst(1, 0));
    });
    // And one step above the line is the error again. The GOAWAY is the verdict and is pinned for
    // both stacks; whether the answer to stream 1 is still in front of it is the same race as above,
    // so it is pinned for h2lean only -- the verdict frame is what the rule decides.
    const idleAfterServed = async (label, respond, send) => {
      const node = await once(nodeHttp2, undefined, respond, send);
      const ours = await once(h2lean, undefined, respond, send);
      // the v20/22 bundle answers INTERNAL_ERROR for the same shape (measured)
      const nodeCode = node.verdict.replace(/^ANSWERED /, '');
      assert.ok(nodeCode === 'GOAWAY(PROTOCOL_ERROR)' || nodeCode === 'GOAWAY(INTERNAL_ERROR)',
        `node: ${label} -> ${node.verdict}`);
      assert.strictEqual(ours.verdict, 'ANSWERED GOAWAY(PROTOCOL_ERROR)',
        `h2lean: ${label} -> ${ours.verdict}, node ${node.verdict}`);
    };
    await idleAfterServed('WINDOW_UPDATE for idle stream 3 after stream 1 was served', true,
      (sock) => {
        sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
        sock.write(wu(3, 1024));
      });
    await idleAfterServed('RST_STREAM for idle stream 3 after stream 1 was served', true,
      (sock) => {
        sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
        sock.write(rst(3, 0));
      });
  });

test('a PRIORITY frame is five octets, names a stream, and its payload is never read', async () => {
  // RFC 9218 deprecated the signal without changing the frame: the four octets of stream
  // dependency and the one of weight are still the whole payload, and 6.2 keeps the length rule.
  // Measured on node (h2spec http2/6.3): a 4-octet PRIORITY is FRAME_SIZE_ERROR. What is still not
  // an error is the content -- a PRIORITY naming itself is dropped without a verdict (RFC 9113 5.3
  // drops the 7540 self-dependency rule with the rest of the signal), which is why h2spec's 5.3.1
  // check fails for node too. A PRIORITY naming stream 0, however, is NOT content: it names no
  // stream at all, and 6.3 makes that a connection error PROTOCOL_ERROR -- the one inbound rule
  // where the RFC and node disagree, so the two stacks are asserted apart rather than in parity.
  await both('PRIORITY with a 4-octet payload', undefined, false, (sock) => {
    sock.write(frame(f.PRIORITY, 0, 1, Buffer.alloc(4)));
    sock.write(req(REQ));
  }, 'GOAWAY(FRAME_SIZE_ERROR)');
  await both('PRIORITY depending on itself is dropped', undefined, true, (sock) => {
    sock.write(req(REQ));
    sock.write(pri(1, 1, 16));
  }, 'ANSWERED', 350, 'GOAWAY(PROTOCOL_ERROR)');
  // node drops a PRIORITY on stream 0 silently -- the deviation h2spec http2/6.3 flags.
  // The v20/22 bundle instead ends the connection with INTERNAL_ERROR (measured); the
  // known deviation changes shape, it does not go away.
  const nodePri0 = await once(nodeHttp2, undefined, false, (sock) => {
    sock.write(pri(0, 1, 16));
  });
  assert.ok(nodePri0.verdict === 'nothing' || nodePri0.verdict === 'GOAWAY(INTERNAL_ERROR)',
    `node: PRIORITY on stream 0 -> ${nodePri0.verdict} (the known node deviation)`);
  // h2lean follows RFC 9113 6.3 and ends the connection instead.
  const h2Pri0 = await once(h2lean, undefined, false, (sock) => {
    sock.write(pri(0, 1, 16));
  });
  assert.strictEqual(h2Pri0.verdict, 'GOAWAY(PROTOCOL_ERROR)',
    `h2lean: PRIORITY on stream 0 -> ${h2Pri0.verdict}, RFC 9113 6.3`);
});

test('a HEADERS priority block is five octets, and a stream may not depend on itself',
  async () => {
    // Both were skipped over without being read: the five octets were moved past whether or not
    // the payload held them, and the dependency field never looked at. Measured on node (h2spec
    // http2/6.2 and 5.3.1): a truncated block is FRAME_SIZE_ERROR, and a stream that depends on
    // itself is a PROTOCOL_ERROR that ends the CONNECTION -- not the stream, which is the
    // surprising half, and the reason a HEADERS frame carries it rather than the PRIORITY one,
    // where node drops the same self-dependency (the test above).
    await both('HEADERS|PRIORITY with only 4 octets of payload', undefined, false, (sock) => {
      sock.write(frame(f.HEADERS, EOH | EOS | f.F_PRIORITY, 1, Buffer.alloc(4)));
      sock.write(frame(f.HEADERS, EOH | EOS, 3, REQ));
    }, 'GOAWAY(FRAME_SIZE_ERROR)');
    await both('HEADERS for stream 1 depending on stream 1', undefined, false, (sock) => {
      sock.write(frame(f.HEADERS, EOH | EOS | f.F_PRIORITY, 1,
        Buffer.concat([dep32(1, 255), REQ])));
    }, 'GOAWAY(PROTOCOL_ERROR)', 350, 'GOAWAY(INTERNAL_ERROR)');
  });

// ---- a frame naming a stream whose message the peer already ended (5.1, 6.1, 8.2) ------------

test('DATA and HEADERS after the peer\'s END_STREAM end the connection with STREAM_CLOSED',
  async () => {
    // RFC 9113 puts the state of the stream a frame names ahead of anything the frame says. A DATA or
    // HEADERS naming a stream whose message the peer already ended -- whether this side has answered
    // (stream still in the table: "half-closed (remote)") or has just closed it (left the table) --
    // is answered with a connection error of type STREAM_CLOSED. 6.1 reads the half-closed(remote)
    // DATA as a stream error (a reset), and h2spec's 5.1 / 6.1 checks accept the reset, the GOAWAY,
    // or a closed connection -- but node -- the oracle -- makes no present/closed distinction and
    // ends the connection on BOTH stacks (measured: its nghttp2 says "DATA: stream in
    // half-closed(remote)"), and the stream-error reading regressed h2spec's DATA check (the tool
    // echoed the DATA back instead of a verdict). So h2lean keeps node's GOAWAY here: 5.1 grants the
    // closed-shape read as a discretionary "MAY ... connection error", which is exactly what we do.
    // h2spec's http2/6.1 writes exactly the first shape: HEADERS flags=0x05 on stream 1, then DATA
    // flags=0x01 on the same id.
    await both('HEADERS|END_STREAM, then DATA on the same id', undefined, false, (sock) => {
      sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
      sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
    }, 'GOAWAY(STREAM_CLOSED)');
    await both('HEADERS|END_STREAM, then a second HEADERS on the same id', undefined, false,
      (sock) => {
        sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
        sock.write(frame(f.HEADERS, EOH | EOS, 1, Buffer.from(litField('x', 'y'))));
      }, 'GOAWAY(STREAM_CLOSED)', 350, 'GOAWAY(INTERNAL_ERROR)');
    // The shapes that keep the stream in the table, because nothing answers it and an open local
    // half holds the record: the state check decides them, not any history, so the verdict does not
    // depend on how the frames were batched (probe cases U10, U11).
    await both('HEADERS, DATA|END_STREAM, then a second DATA, unanswered', undefined, false,
      (sock) => {
        sock.write(frame(f.HEADERS, EOH, 1, REQ));
        sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
        sock.write(frame(f.DATA, EOS, 1, Buffer.from('more', 'ascii')));
      }, 'GOAWAY(STREAM_CLOSED)');
    await both('HEADERS|END_STREAM, then DATA, unanswered', undefined, false, (sock) => {
      sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
      sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
    }, 'GOAWAY(STREAM_CLOSED)');
    // The closed shape with a responding handler: node keeps its stream record until the response
    // reaches the socket, so a frame inside the ONE coalesced write below still sees it (measured
    // 8/8 GOAWAY; the same two frames one per write flip node to silence because the answer has by
    // then destroyed the record -- which is why the write is coalesced, the only fixed-answer form).
    // Whether h2lean's own response escapes in front of the GOAWAY is write timing (the queued-answer
    // divergence at the end of this file), so the verdict is pinned per stack rather than compared.
    const closedError = async (label, frames) => {
      const send = (sock) => sock.write(Buffer.concat(frames));
      const node = await once(nodeHttp2, undefined, true, send);
      const ours = await once(h2lean, undefined, true, send);
      // the v20/22 bundle answers INTERNAL_ERROR for the same shape (measured)
      const nodeCode = node.verdict.replace(/^ANSWERED /, '');
      assert.ok(nodeCode === 'GOAWAY(STREAM_CLOSED)' || nodeCode === 'GOAWAY(INTERNAL_ERROR)',
        `node: ${label} -> ${node.verdict}`);
      assert.strictEqual(ours.verdict, 'ANSWERED GOAWAY(STREAM_CLOSED)',
        `h2lean: ${label} -> ${ours.verdict}, node ${node.verdict}`);
    };
    await closedError('HEADERS|END_STREAM answered, then DATA', [
      frame(f.HEADERS, EOH | EOS, 1, REQ),
      frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii'))
    ]);
    await closedError('HEADERS|END_STREAM answered, then a second HEADERS', [
      frame(f.HEADERS, EOH | EOS, 1, REQ),
      frame(f.HEADERS, EOH | EOS, 1, Buffer.from(litField('x', 'y')))
    ]);
  });

test('a frame naming an id a reset ended, one this side refused, one the peer skipped: discard',
  async () => {
    // The three memories that must NOT answer with a verdict, and the distinction the whole rule
    // turns on: an id is gone either because a reset ended it -- ours or the peer's -- or because
    // the peer ended its message. Only the second one is an error, so the session has to remember
    // which of the two happened (see _dropStream's `killed`).
    //
    // The shapes below are written as ONE coalesced write and asserted as "no verdict": whether the
    // answer to a stream the peer reset still goes out is nghttp2's write timing, and h2lean flushes
    // what its handler queued before the reset arrived (the KNOWN DIVERGENCE at the end of this
    // file). What neither stack may do is turn the peer's own late frame into a GOAWAY.
    await noVerdictTest('HEADERS|END_STREAM, the peer resets it, then DATA for that id', true,
      (sock) => {
        sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
        sock.write(rst(1, 0));
        sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
      });
    await noVerdictTest('a second HEADERS for an id the peer reset', true, (sock) => {
      sock.write(frame(f.HEADERS, EOH, 1, REQ));
      sock.write(rst(1, 0));
      sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
    });
    // The reset may also arrive once the id has left every table, below the highest one opened: it
    // still lands, and what follows it for that id is still the discard (probe U19).
    await noVerdictTest('a HEADERS for an id the peer reset after it finished', true, (sock) => {
      sock.write(frame(f.HEADERS, EOH, 1, REQ));
      sock.write(rst(1, 0));
      sock.write(frame(f.HEADERS, EOH | EOS, 3, REQ));
      sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
    });
    // A refusal that did NOT end the message: the peer had already put the body on the wire, and
    // those octets belong to the stream this side refused, not to a stream that was over. Measured
    // on node, probe U17: the DATA is ignored and the next request is served. This is the shape the
    // "stream id went backwards" reading used to answer with GOAWAY(PROTOCOL_ERROR) for.
    await both('a refused request without END_STREAM, its DATA, then a legal stream 3',
      undefined, true, (sock) => {
        sock.write(frame(f.HEADERS, EOH, 1, Buffer.from([0x82])));
        sock.write(frame(f.DATA, 0, 1, Buffer.from('test', 'ascii')));
        sock.write(frame(f.HEADERS, EOH | EOS, 3, REQ));
      }, 'RST(PROTOCOL_ERROR) ANSWERED');
    await both('a second HEADERS for a refused id', { maxHeaderListSize: 48 }, true, (sock) => {
      sock.write(req(Buffer.concat([REQ, lit(1, 46)])));
      sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
    }, 'RST(ENHANCE_YOUR_CALM)');
    // A peer may skip ids upward, but an id at or below the highest one opened that this side never
    // saw -- not refused, not reset, simply not used -- cannot open a new stream: 5.1.1 names it an
    // unexpected identifier and mandates GOAWAY(PROTOCOL_ERROR). node stays silent (probe U18, U20,
    // U21), so h2lean follows the RFC here -- the third divergence after PRIORITY on stream 0.
    const lowLive = (sock) => {
      sock.write(frame(f.HEADERS, EOH | EOS, 3, REQ));
      sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
    };
    const nodeLowLive = await once(nodeHttp2, undefined, true, lowLive);
    assert.strictEqual(nodeLowLive.verdict, 'ANSWERED',
      `node: HEADERS for an id never opened, below a finished one -> ${nodeLowLive.verdict} ` +
      '(the known node deviation)');
    const h2LowLive = await once(h2lean, undefined, true, lowLive);
    assert.strictEqual(h2LowLive.verdict, 'ANSWERED GOAWAY(PROTOCOL_ERROR)',
      `h2lean: HEADERS for an id never opened, below a finished one -> ${h2LowLive.verdict}, ` +
      'RFC 9113 5.1.1');
    await noVerdictTest('DATA for an id never opened, below a finished one', true, (sock) => {
      sock.write(frame(f.HEADERS, EOH | EOS, 3, REQ));
      sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
    });
    // The same 5.1.1 violation with the higher id still live rather than already answered: the
    // guard reads the highest id this side has OPENED, not the highest one still serving, so the
    // never-used lower id is refused identically. node still says nothing; h2lean ends the
    // connection (RFC 9113 5.1.1).
    const lowIdle = (sock) => {
      sock.write(frame(f.HEADERS, EOH, 3, REQ));
      sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
    };
    const nodeLowIdle = await once(nodeHttp2, undefined, true, lowIdle);
    assert.doesNotMatch(nodeLowIdle.verdict, /GOAWAY|RST\(/,
      `node: HEADERS for an id never opened, below a live stream -> ${nodeLowIdle.verdict} ` +
      '(the known node deviation)');
    const h2LowIdle = await once(h2lean, undefined, true, lowIdle);
    assert.match(h2LowIdle.verdict, /GOAWAY\(PROTOCOL_ERROR\)/,
      `h2lean: HEADERS for an id never opened, below a live stream -> ${h2LowIdle.verdict}, ` +
      'RFC 9113 5.1.1');
    // The reach of the "ended in this batch" memory, and the reason it is cleared: a stream that has
    // finished AND been answered is only remembered until the read that produced the answer is over,
    // because the answer is what makes nghttp2 destroy the record -- and after that the same late
    // DATA is 5.1's discard. Written as two spaced writes, which is the shape node answers with
    // silence for (probe U1, U3, U8, U9 under GAP= in tools/probe-inbound-limits.js).
    await noVerdictTest('DATA for a stream answered in an EARLIER read', true, (sock) => {
      sock.write(frame(f.HEADERS, EOH | EOS, 1, REQ));
      setTimeout(() => sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii'))), 150);
    }, undefined, 600);
  });

test('DATA for a stream in the idle state ends the connection with PROTOCOL_ERROR', async () => {
  // DATA is allowed in 'open' and 'half-closed (local)' only (5.1), and an id above the highest one
  // this side has been told about -- or one that no client may ever own -- is 'idle'. Measured on
  // node (probe U5, U13, U14): the connection ends, including for stream 0 and for an even id, and
  // h2lean had been answering those two with the wrong code, if with a GOAWAY at all.
  await both('DATA for an id that was never opened at all', undefined, false, (sock) => {
    sock.write(frame(f.DATA, EOS, 5, Buffer.from('test', 'ascii')));
    sock.write(req(REQ));
  }, 'GOAWAY(PROTOCOL_ERROR)');
  await both('DATA on stream 0', undefined, false, (sock) => {
    sock.write(frame(f.DATA, EOS, 0, Buffer.from('test', 'ascii')));
    sock.write(req(REQ));
  }, 'GOAWAY(PROTOCOL_ERROR)');
  await both('DATA for an even id', undefined, false, (sock) => {
    sock.write(frame(f.DATA, EOS, 2, Buffer.from('test', 'ascii')));
    sock.write(req(REQ));
  }, 'GOAWAY(PROTOCOL_ERROR)');
});

// ---- the message-framing rules: 8.1 and 8.2 --------------------------------------------------

test('a trailer block has to arrive with END_STREAM', async () => {
  // 8.1: the second HEADERS of a request is the trailer section, and a trailer ends the message --
  // without END_STREAM the body would still be open with its tail already sent. Measured on node
  // with a handler that reads the body (probe case T3, and h2spec http2/8.1): the stream gets
  // RST_STREAM(PROTOCOL_ERROR) and the connection is untouched. h2lean had accepted the block,
  // emitted it as trailers, and answered the request as if nothing had happened.
  await both('HEADERS, DATA, then a trailer block without END_STREAM', undefined, 'read', (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, REQ));
    sock.write(frame(f.DATA, 0, 1, Buffer.from('test', 'ascii')));
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.from(litField('x', 'y'))));
  }, 'RST(PROTOCOL_ERROR)');
  // The control: the same block with END_STREAM is the legal trailer shape and must be served.
  await both('the same trailer block with END_STREAM', undefined, 'read', (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, REQ));
    sock.write(frame(f.DATA, 0, 1, Buffer.from('test', 'ascii')));
    sock.write(frame(f.HEADERS, EOH | EOS, 1, Buffer.from(litField('x', 'y'))));
  }, 'ANSWERED');
});

test('content-length that is too short is the stream\'s problem', async () => {
  // 8.2 makes the field a promise about the body, and the two directions of breaking it are
  // charged differently. Too few octets can only be seen when the message ends, and it ends one
  // request: RST_STREAM(PROTOCOL_ERROR), the connection kept for the next one. Measured on node
  // with a handler that reads the body (probe case T1, h2spec http2/8.1.2.6).
  await both('content-length 4, a body of 2 octets with END_STREAM', undefined, 'read', (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.concat([REQ, CL(4)])));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('ab', 'ascii')));
  }, 'RST(PROTOCOL_ERROR)');
  // A promise of a body on a frame that ends the stream is the same lie told earlier: no DATA can
  // follow, so it is refused at the accept decision and the application never sees the stream
  // (probe case T4). An answer here would be a response to a request this side has refused.
  await both('content-length 4 on a HEADERS that ends the stream', undefined, 'read', (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 1, Buffer.concat([REQ, CL(4)])));
  }, 'RST(PROTOCOL_ERROR)');
  await both('content-length 0 with no body is legal', undefined, 'read', (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 1, Buffer.concat([REQ, CL(0)])));
  }, 'ANSWERED');
  // The accept-decision refusal, told apart from the stream-level one by a handler that answers
  // every stream it is handed. With the refusal BEFORE the stream object exists, the immediate
  // handler never runs and only RST goes out -- which is node's verdict (probe T4: the application
  // never sees the request). Move the check after the stream is created and the handler gets a
  // destroyed-but-answered stream: the wire then carries a HEADERS for a request this side refused.
  // The `respond: 'read'` shapes above cannot see the difference because nghttp2 resets the stream
  // for the broken promise either way; only an answer that does not wait for the body can.
  await both('content-length 4 on a HEADERS that ends the stream, immediate responder',
    undefined, true, (sock) => {
      sock.write(frame(f.HEADERS, EOH | EOS, 1, Buffer.concat([REQ, CL(4)])));
    }, 'RST(PROTOCOL_ERROR)');
});

test('content-length that is too long is the connection\'s problem', async () => {
  // The other half of the promise, and the reason this library counts body octets per stream at
  // all: the frame that runs past the promise ends the CONNECTION with INTERNAL_ERROR -- node's
  // verdict, measured with a reading handler (probe case T2) and the one nghttp2 reaches because an
  // over-long body is a failure of the messaging layer rather than of one request. It is cumulative
  // and not per frame: measured (probe case R7), content-length 100 with two frames of 60 is
  // refused on the second, although neither frame is over the promise on its own.
  await both('content-length 1, one DATA of 4 octets with END_STREAM', undefined, 'read', (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.concat([REQ, CL(1)])));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
  }, 'GOAWAY(INTERNAL_ERROR)', 350, 'RST(PROTOCOL_ERROR)');
  await both('content-length 100, DATA 60 + DATA 60', undefined, 'read', (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.concat([REQ, CL(100)])));
    sock.write(frame(f.DATA, 0, 1, Buffer.alloc(60, 0x61)));
    sock.write(frame(f.DATA, EOS, 1, Buffer.alloc(60, 0x61)));
  }, 'GOAWAY(INTERNAL_ERROR)', 350, 'RST(PROTOCOL_ERROR)');
  await both('content-length 0 with a body of 4 octets', undefined, 'read', (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.concat([REQ, CL(0)])));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
  }, 'GOAWAY(INTERNAL_ERROR)', 350, 'RST(PROTOCOL_ERROR)');
  // The control, and the reason the counter is compared with `>` and not with `!==`: a body of
  // exactly what was promised is the ordinary POST.
  await both('content-length 4, DATA 4 with END_STREAM', undefined, 'read', (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.concat([REQ, CL(4)])));
    sock.write(frame(f.DATA, EOS, 1, Buffer.from('test', 'ascii')));
  }, 'ANSWERED');
});

test('the connection window is credited in half-window steps, not one frame per read', async () => {
  // _creditConnection() releases the connection octets the application has taken only once they
  // reach half the advertised window, which is the cadence nghttp2 keeps -- measured on node 26
  // with tools/probe-conn-credit.js over a 640 KiB upload, where every WINDOW_UPDATE(sid 0) grant
  // is 32768 octets, half the 65535 it advertises. Refilling per read batch instead lets the
  // connection window run down and pays it back a dozen times as often. The claim is a parity one,
  // so it is asserted on both stacks: no grant below the half-window, and the whole upload arrives
  // (a handler that answers only at 'end' proves the peer was never starved of credit).
  const CHUNK = 16384;
  const TOTAL = 40 * CHUNK;
  const flood = (impl) => new Promise((resolve) => {
    const srv = impl.createServer();
    srv.on('stream', (stream) => {
      stream.on('error', () => {});
      let got = 0;
      stream.on('data', (d) => {
        got += d.length;
      });
      stream.on('end', () => {
        got === TOTAL ? stream.respond({ ':status': 200 }, { endStream: true })
          : finish(false);
      });
    });
    srv.on('error', () => {});
    const finish = (done) => {
      sock.destroy();
      closeServer(srv).then(() => resolve({ grants, advertised, done }));
    };
    let acc = Buffer.alloc(0), sent = 0, credit = 65535, streamCredit = 65535;
    let advertised = 65535, headed = false, settled = false;
    const grants = [];
    const sock = new net.Socket();
    listen(srv).then((port) => {
      sock.connect(port, '127.0.0.1');
    });
    function push() {
      if (!headed) {
        sock.write(frame(f.HEADERS, EOH, 1, REQ)); headed = true;
      }
      while (sent < TOTAL && credit >= CHUNK && streamCredit >= CHUNK) {
        const n = Math.min(CHUNK, TOTAL - sent);
        sock.write(frame(f.DATA, n === TOTAL - sent ? EOS : 0, 1, Buffer.alloc(n, 0x61)));
        credit -= n; streamCredit -= n; sent += n;
      }
    }
    sock.on('error', () => {});
    sock.setTimeout(3000, () => {
      if (!settled) {
        settled = true; finish(false);
      }
    });
    sock.on('data', (d) => {
      sock.setTimeout(3000);
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
        const sid = acc.readUInt32BE(5) & 0x7fffffff;
        const payload = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
        acc = acc.subarray(f.HEADER_LEN + len);
        if (type === f.SETTINGS && !(flags & f.F_ACK)) {
          for (let i = 0; i + 6 <= payload.length; i += 6) {
            if (payload.readUInt16BE(i) === 4) {
              advertised = payload.readUInt32BE(i + 2);
            }
          }
          sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
          push();
        } else if (type === f.WINDOW_UPDATE && sid === 0) {
          grants.push(payload.readUInt32BE(0));
          credit += payload.readUInt32BE(0);
          push();
        } else if (type === f.WINDOW_UPDATE) {
          streamCredit += payload.readUInt32BE(0);
          push();
        } else if (type === f.HEADERS) {
          if (!settled) {
            settled = true; finish(true);
          }
        }
      }
    });
    sock.on('connect', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
    });
  });
  for (const [who, impl] of [['node', nodeHttp2], ['h2lean', h2lean]]) {
    const { grants, advertised, done } = await flood(impl);
    assert.ok(done, `${who}: the 640 KiB upload never reached the handler (grants ${grants})`);
    assert.ok(grants.length > 0, `${who}: the connection window was never credited back`);
    const half = advertised >> 1;
    for (const g of grants) {
      assert.ok(g >= half, `${who}: a connection WINDOW_UPDATE of ${g} octets, below the `
        + `half-window ${half} both stacks keep the cadence at`);
    }
  }
});

// ---- where a header block may announce a table size change (RFC 7541 6.3) -------------------

test('a dynamic-table size update is only legal at the start of a header block', async () => {
  // 6.3: the update describes the state the decoder is to be in BEFORE the first field line, so
  // one that arrives after a line has been decoded makes the indices in the lines already read
  // name a table the update says was different. Measured on node (h2spec hpack/4.2): all three
  // shapes below -- an update to a smaller size, to a larger one, and to the size the table
  // already has -- are GOAWAY(COMPRESSION_ERROR), and the code is the point rather than the number:
  // a second decoder reading the block in order would rebuild a different context from ours.
  for (const [label, tail] of [
    ['to 33', Buffer.from([0x21])],
    ['to 512', intN(0x20, 5, 512)],
    ['to 4096, what the table already allows', intN(0x20, 5, 4096)]
  ]) {
    await both(`size update ${label} after a field line`, undefined, false, (sock) => {
      sock.write(req(Buffer.concat([REQ, tail])));
    }, 'GOAWAY(COMPRESSION_ERROR)');
  }
  // At the start of a block the update is legal, twice in a row if the peer likes, and the request
  // that follows it on the same connection is served: the failure above is the position, not the
  // act of resizing.
  await both('size update before the first field line', undefined, true, (sock) => {
    sock.write(req(Buffer.concat([Buffer.from([0x21]), REQ])));
  }, 'ANSWERED');
  await both('two size updates in a row, then a second request', undefined, true, (sock) => {
    sock.write(req(Buffer.concat([intN(0x20, 5, 4096), Buffer.from([0x21]), REQ])));
    sock.write(frame(f.HEADERS, EOH | EOS, 3, Buffer.concat([REQ, litField('x', 'y')])));
  }, 'ANSWERED ANSWERED');
});

// ---- a peer GOAWAY this side does not understand --------------------------------------------

test('a GOAWAY with an unassigned error code is answered with ours and closes the session',
  async () => {
    // RFC 9113 6.8: a code that is not defined is not a reason to keep the connection -- the peer
    // has said it is stopping -- and it is not a reason to send a GOAWAY carrying a code the peer
    // may not know either. Measured on node for h2spec's http2/7 (probe cases L1, L2, L5, L6, the
    // last one byte-for-byte what the tool writes): the session is destroyed, the application is
    // told the code that arrived, and the frame this side puts on the wire is GOAWAY(NO_ERROR).
    // The comparison is on the code alone: which side's FIN arrives first, and whether a reset
    // beats it there, is settled by scheduling -- see the test below for the one verdict that
    // differs when the peer keeps talking after the GOAWAY.
    const goawayOf = (r) => (r.verdict.match(/GOAWAY\((\w+)\)/) || [null, 'none'])[1];
    for (const c of [17, 0xfffffff0, 255]) {
      const body = Buffer.alloc(8);
      body.writeUInt32BE(0, 0); body.writeUInt32BE(c, 4);
      const send = (sock) => {
        sock.write(frame(f.GOAWAY, 0, 0, body));
        sock.write(frame(f.PING, 0, 0, Buffer.from('6832737065630000', 'hex')));
      };
      const nodeGot = goawayOf(await once(nodeHttp2, undefined, false, send));
      const oursGot = goawayOf(await once(h2lean, undefined, false, send));
      assert.strictEqual(nodeGot, 'NO_ERROR', `code ${c}: node answers its own goodbye`
        + ' with NO_ERROR');
      assert.strictEqual(oursGot, 'NO_ERROR', `code ${c}: h2lean answered ${oursGot}, node ` +
        `answered ${nodeGot}`);
    }
  });

test('a peer GOAWAY with NO_ERROR starts our own graceful close', async () => {
  // 6.8 again, and the other half of node's rule on receipt: NO_ERROR is a goodbye and not a
  // failure, so the session begins closing -- no new streams, existing ones finish -- and answers
  // with its own GOAWAY(NO_ERROR). Measured on node for h2spec's http2/5.1-adjacent L4/L1 shapes:
  // both stacks put exactly one GOAWAY(NO_ERROR) on the wire for a peer goodbye carrying an
  // unassigned code, and one for NO_ERROR as well; what they do not agree on is whether a frame
  // the peer sends AFTER that goodbye gets a second GOAWAY out of node, which is the graceful-close
  // open problem AGENTS.md §10 names and does not pretend to have settled here.
  const body = Buffer.alloc(8);
  body.writeUInt32BE(0, 4);
  const nodeGot = (await once(nodeHttp2, undefined, false, (sock) => {
    sock.write(frame(f.GOAWAY, 0, 0, body));
  })).verdict;
  const oursGot = (await once(h2lean, undefined, false, (sock) => {
    sock.write(frame(f.GOAWAY, 0, 0, body));
  })).verdict;
  assert.strictEqual(nodeGot, 'GOAWAY(NO_ERROR)', `node: ${nodeGot}`);
  assert.strictEqual(oursGot, 'GOAWAY(NO_ERROR)',
    `h2lean: ${oursGot}, node: ${nodeGot}`);
});

// ---- what the two stacks still disagree on ---------------------------------------------------

test('the GOAWAY for a refused header block names the block\'s stream, as node\'s does', async () => {
  // nghttp2 opens the stream in prep_frame, after the frame's length, padding and priority
  // checks but BEFORE the block is decoded or any verdict on it is given -- so when a frame is
  // rejected after that point (an undecodable block, a header list over maxHeaderListSize),
  // node's GOAWAY names the stream id it read out of that frame's header. h2lean used to name
  // only the streams a stream object was made for -- 0 here -- which tells a client the request
  // is retryable when it was already acted on: the last-stream id IS the retry boundary, and
  // naming the refused stream is the safe direction. Measured on v26.12.1 for the whole family
  // (out/probe-last-id.js), and the boundary is exact on both stacks: WINDOW_UPDATE, RST_STREAM
  // and DATA refused ABOVE the line answer last=1 -- the served stream -- on both, because
  // nghttp2 does not open a stream for a control frame; frames refused BEFORE the acceptance
  // point (over max frame size, malformed padding, an even id) do not move it either. The CODE
  // is compared in the tests above.
  const oversized = (sock) => {
    sock.write(frame(f.HEADERS, EOH | EOS, 1, Buffer.concat([REQ, big(16370)])));
  };
  assert.strictEqual((await once(nodeHttp2, undefined, false, oversized)).last, 1,
    'node names the stream of the frame it refused');
  assert.strictEqual((await once(h2lean, undefined, false, oversized)).last, 1,
    'h2lean names it too');
  // The same shape for the HPACK 6.3 refusal measured by h2spec's hpack/4.2: the frame decodes
  // far enough to reach a stream, the size update after the field line is what kills the
  // connection, and node names the stream id off the refused frame's header. Checked on the id
  // alone -- the CODE is pinned by the test above, and both stacks answer
  // GOAWAY(COMPRESSION_ERROR) there.
  const lateResize = (sock) => {
    sock.write(req(Buffer.concat([REQ, Buffer.from([0x21])])));
  };
  assert.strictEqual((await once(nodeHttp2, undefined, false, lateResize)).last, 1,
    'node names the stream of the header block it failed to decode');
  assert.strictEqual((await once(h2lean, undefined, false, lateResize)).last, 1,
    'h2lean names it too');
});

test('KNOWN DIVERGENCE: a header frame that is both over size and undecodable', async () => {
  // The frame-size rule and the HPACK rule are both violated by one frame, and the order the
  // two stacks check them in decides the octet that reaches the peer: node answers
  // GOAWAY(COMPRESSION_ERROR) because nghttp2 feeds a header block to the decoder as its
  // octets arrive, so the decoding failure is found before any length verdict could be given,
  // while h2lean checks the length in _allowFrame() without reading the payload at all and
  // answers GOAWAY(FRAME_SIZE_ERROR). Which block is decodable is what separates this case
  // from 'HEADERS over the max frame size ends the connection, HPACK and all' above: there the
  // block IS valid HPACK and both stacks agree on FRAME_SIZE_ERROR.
  //
  // Not emulated, on purpose. Both codes are connection errors and the connection ends either
  // way, and the peer cannot act on the difference; matching node would mean moving the length
  // check after the decode, which is exactly where it can no longer save the reading of an
  // over-size payload. The last-stream id of the two GOAWAYs agrees -- nghttp2 opens the stream
  // when the frame is accepted, before either check runs (the test above) -- so the CODE is the
  // only verdict that differs. Pinned so that a change in either verdict is a decision and not a
  // surprise.
  // Compared: the GOAWAY code alone, not the whole transcript. node also resets the stream whose
  // block failed to decode, and whether that RST_STREAM reaches the peer in front of the GOAWAY is
  // settled by scheduling and nothing else -- asserting the full string made this test fail once in
  // three runs with both stacks answering exactly the codes under test.
  const undecodable = (sock) => {
    sock.write(frame(f.HEADERS, 0, 1, REQ));
    sock.write(frame(f.CONTINUATION, EOH, 1, big(16385)));
  };
  const goaway = (r) => (r.verdict.match(/GOAWAY\((\w+)\)/) || [null, 'none'])[1];
  assert.strictEqual(goaway(await once(nodeHttp2, undefined, false, undecodable)),
    'COMPRESSION_ERROR', 'node reports the decoding failure');
  assert.strictEqual(goaway(await once(h2lean, undefined, false, undecodable)),
    'FRAME_SIZE_ERROR', 'h2lean reports the frame size');
});

test('KNOWN DIVERGENCE: an answer queued in the same batch as a refusal goes out here', async () => {
  // Measured, and left alone rather than emulated. A handler that answers as soon as the stream
  // opens puts a response on the wire, and if the frames in that same read batch break a
  // message-framing rule, nghttp2 often throws the queued response away: it refuses the stream (or
  // the connection) and the peer never sees a HEADERS for it. h2lean always flushes what the
  // handler wrote before the refusal is processed, so its transcript always starts with ANSWERED.
  // Measured with tools/probe-inbound-limits.js, cases M1, M1b, N1, N2, P6 and P8 -- three runs of
  // each: h2lean ANSWERED plus the refusal every time, node the same refusal and NOTHING answered
  // in most runs. Not in all of them: the full-suite run caught node answering this very shape once
  // too, which is what the relaxed assertion below records -- WHETHER node's response escapes is
  // nghttp2's own write-batch timing, and only the discard behaviour is a rule. Asserting the exact
  // string made this test fail in one run out of four for a reason that is node's, not ours.
  //
  // The codes are identical, which is why this is pinned instead of fixed: the peer is refused by
  // the same rule either way, and the only difference is whether it also gets a response it has to
  // discard. Matching node would mean dropping queued output at refuse time -- the stream-object
  // lifetime and graceful-close conflation AGENTS.md §10 carries as the open problem, not a
  // verdict this file can settle. A handler that answers only after reading the body ends the race
  // on its own: the refusal lands before any response exists, and both stacks then say the same
  // thing (see 'content-length that is too short is the stream's problem').
  const trailer = (sock) => {
    sock.write(frame(f.HEADERS, EOH, 1, REQ));
    sock.write(frame(f.DATA, 0, 1, Buffer.from('test', 'ascii')));
    sock.write(frame(f.HEADERS, EOH, 1, Buffer.from('4085f2b24a84ff026f6b', 'hex')));
  };
  const nodeGot = (await once(nodeHttp2, undefined, true, trailer, 700)).verdict;
  const oursGot = (await once(h2lean, undefined, true, trailer, 700)).verdict;
  // node's arm is printed and not pinned: whether its response escapes, and with which reset code
  // the refusal then reaches the peer, is nghttp2's write-batch timing.
  assert.match(oursGot, /RST\(PROTOCOL_ERROR\)/,
    `h2lean must still refuse the trailer shape, however node answered it (${nodeGot}): ${oursGot}`);
  assert.ok(oursGot.startsWith('ANSWERED'),
    `the queued answer is flushed here -- that is the divergence: ${oursGot}, node ${nodeGot}`);
});

// ---- unknown frame types -------------------------------------------------------------------

test('an unknown frame type is ignored, not an error', async () => {
  // RFC 9113 5.5. Type 10 is ALTSVC (RFC 7807) and 0xff is unassigned; the request that follows
  // must be served as if the frame had not arrived. This is also what lib/session.js's
  // `default:` branch in _handle() promises -- the branch has no payload parsing at all, so the
  // only thing a test can check is that nothing bad happens.
  for (const type of [10, 99, 0xff]) {
    await both(`type ${type}`, undefined, true, (sock) => {
      sock.write(frame(type, 0, 0, type === 0xff ? big(4096) : Buffer.from('x', 'ascii')));
      sock.write(req(REQ));
    }, 'ANSWERED');
  }
});

// ---- the internal histories that keep those verdicts cheap --------------------------------

// Opens one raw connection and sends `count` HEADERS frames with the given sid sequence, and the
// `reset` frames if the caller asks for them, then hands back the server's session. Nothing waits
// for the answers: these tests are about how much state the session keeps while it refuses, not
// about what the peer receives.
async function manyRequests(impl, settings, count, first, respond, reset) {
  const srv = impl.createServer(settings ? { settings } : undefined);
  let session = null;
  srv.on('session', (s) => {
    session = s;
  });
  srv.on('stream', (stream) => {
    stream.on('error', () => {});
    if (respond) {
      stream.respond({ ':status': 200 }, { endStream: true });
    }
  });
  srv.on('error', () => {});
  const port = await listen(srv);
  const sock = await new Promise((resolve) => {
    const s = net.connect(port, '127.0.0.1');
    s.on('connect', () => {
      resolve(s);
    });
    s.on('error', () => {});
    s.resume();
  });
  sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
  await new Promise((resolve) => setTimeout(resolve, 60));
  const frames = [];
  for (let i = 0; i < count; i++) {
    frames.push(frame(f.HEADERS, EOH | EOS, first + 2 * i, REQ));
    if (reset) {
      frames.push(rst(first + 2 * i, 0));
    }
  }
  sock.write(Buffer.concat(frames));
  await new Promise((resolve) => setTimeout(resolve, 300));
  sock.destroy();
  await closeServer(srv);
  return session;
}

test('the refused-stream history stays bounded', async () => {
  // Both the concurrency refusal and the oversized-list refusal remember the id, so the peer's
  // already-in-flight DATA for a stream it never got is ignored instead of becoming an error.
  // The memory is the point of the bound: a client that opens and resets in a loop (the
  // CVE-2023-44487 shape) would otherwise grow an array for the life of the connection.
  // 40 refusals on a session allowed one live stream: maxConcurrentStreams is 1, stream 1 is
  // never answered and stays live, and the 39 that follow are refused.
  const session = await manyRequests(h2lean, { maxConcurrentStreams: 1 }, 40, 3, false);
  assert.strictEqual(session._refused.length, 32,
    `the refused history is ${session._refused.length} entries, its bound is 32`);
});

test('the dropped-stream history stays bounded', async () => {
  // The same argument for the ids a reset ended: every RST_STREAM that arrives for an id this side
  // no longer has -- the peer closing a stream whose answer it had already seen, or one it refused
  // -- has to be remembered, or the DATA still in flight for it would read as a stream in the idle
  // state and end the connection. 300 answered requests each reset by the client leave 256 ids
  // behind, not 300.
  const session = await manyRequests(h2lean, undefined, 300, 1, true, true);
  assert.strictEqual(session._dropped.length, 256,
    `the dropped history is ${session._dropped.length} entries, its bound is 256`);
});

test('the ended-stream history stays bounded', async () => {
  // The third memory, and the one with the shortest reach: the ids whose message ended inside the
  // read batch being processed, which is what turns a DATA for one of them into the connection
  // error of 5.1. It is written on every clean completion, so a client that pipelines 300 requests
  // into one write would grow it without a bound if the cap were missing -- and the entries above
  // the cap are the oldest ones, whose answers have long gone out and whose frames are the
  // discard by the time anything could arrive for them.
  const session = await manyRequests(h2lean, undefined, 300, 1, true);
  assert.strictEqual(session._ended.length, 256,
    `the ended history is ${session._ended.length} entries, its bound is 256`);
  assert.strictEqual(session._dropped, null,
    'a stream that completed on its own terms must not join the reset history');
});
