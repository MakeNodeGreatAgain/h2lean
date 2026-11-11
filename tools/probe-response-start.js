// Does a response the application never started get started for it? `write()`/`end()` on a
// server Http2Stream before `respond()` are legal in node, and node produces the headers
// itself -- the question is WHERE it does that and what an empty end() does.
//
// Measured shape of the answer (node v26.12.1, raw peer, HEADERS without END_STREAM so the
// request half stays open the way a real upload would):
//
//   end('body')     node: HEADERS/EH{:status 200} DATA(body)/ES RST_STREAM(NO_ERROR)
//   write+end       node: HEADERS/EH{:status 200} DATA(x) DATA(body)/ES RST(NO_ERROR)
//   write only      node: HEADERS/EH{:status 200} DATA(x)
//   end() (no body) node: nothing at all, and stream.headersSent stays false
//
// So the auto-respond belongs to the WRITE path, not to end(): the first byte of body forces
// the headers out ahead of it. And an end() with no body starts nothing -- the RST_STREAM
// above is node closing a stream whose request nobody read (see probe-stream-teardown.js),
// not part of the response.
//
// h2lean answers the headers on write() too. Where it differs is the empty end(): it replies
// 200 with an empty DATA/ES rather than saying nothing, because a stream the peer never gets
// an answer on is only let go by the idle timeout. That split is pinned in
// test/response-start.test.js and recorded in AGENTS.md section 10.
//
//   node tools/probe-response-start.js          -> both stacks
//   node tools/probe-response-start.js node     -> node only
'use strict';
const net = require('node:net');
const f = require('../lib/frames');
const { Decoder } = require('../lib/hpack');
const h2lean = require('../index');
const nodeHttp2 = require('node:http2');

const REQ = Buffer.from([0x82, 0x84, 0x86, 0x01, 9, ...Buffer.from('localhost', 'latin1')]);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const CODES = { 0: 'NO_ERROR', 1: 'PROTOCOL_ERROR', 3: 'INTERNAL_ERROR',
  11: 'ENHANCE_YOUR_CALM' };
const code = (n) => CODES[n] || String(n);

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

const CASES = {
  // a body with no respond(): the headers have to be produced for the application
  'end(body)': (stream) => {
    stream.end('body');
  },
  // and with nothing at all: node puts no frame on the wire for this one
  'end()': (stream) => {
    stream.end();
  },
  // the first write starts the response, and end() closes it
  'write+end': (stream) => {
    stream.write('x'); stream.end('body');
  },
  // a response that was started but never finished
  'write only': (stream) => {
    stream.write('x');
  },
  // the control: an explicit respond() with the same body
  'respond+end': (stream) => {
    stream.respond({ ':status': 201 }); stream.end('body');
  }
};

async function run(impl, name, label, app) {
  const srv = impl.createServer();
  const events = [];
  let streamRef = null;
  srv.on('stream', (stream) => {
    streamRef = stream;
    stream.on('error', (e) => events.push('streamError:' + e.code));
    stream.on('finish', () => events.push('finish'));
    stream.on('close', () => events.push('close'));
    app(stream);
  });
  srv.on('error', () => {});
  await new Promise((r) => srv.listen(0, '127.0.0.1', r));

  const seen = [];
  const decoder = new Decoder(4096); // one per connection: the table is per peer
  const sock = net.connect(srv.address().port, '127.0.0.1');
  let acc = Buffer.alloc(0);
  sock.on('error', () => {});
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
      const body = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
      acc = acc.subarray(f.HEADER_LEN + len);
      if (type === f.SETTINGS && (flags & f.F_ACK)) {
        continue;
      }
      if (type === f.HEADERS) {
        const out = [];
        decoder.decode(body, 0, body.length, out);
        const fields = [];
        for (let i = 0; i < out.length; i += 2) {
          // the date is generated from each server's clock: never part of the comparison
          if (out[i] !== 'date') {
            fields.push(`${out[i]}=${out[i + 1]}`);
          }
        }
        seen.push(`HEADERS${flags & f.F_END_HEADERS ? '/EH' : ''}`
          + `${flags & f.F_END_STREAM ? '/ES' : ''}{${fields.join(',')}}`);
      } else if (type === f.DATA) {
        seen.push(`DATA(${body.toString('latin1')})${flags & f.F_END_STREAM ? '/ES' : ''}`);
      } else if (type === f.RST_STREAM) {
        seen.push(`RST(${code(body.readUInt32BE(0))})`);
      } else if (type === f.GOAWAY) {
        seen.push(`GOAWAY(${code(body.readUInt32BE(4))})`);
      } else {
        seen.push('type' + type);
      }
    }
  });
  await new Promise((r) => sock.on('connect', r));
  sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
  await sleep(120);
  sock.write(frame(f.HEADERS, f.F_END_HEADERS, 1, REQ)); // no END_STREAM: request half open
  await sleep(300);
  const state = streamRef === null ? '(no stream)' : JSON.stringify({
    headersSent: streamRef.headersSent, closed: streamRef.closed,
    destroyed: streamRef.destroyed, rst: streamRef.rstCode
  });
  sock.destroy();
  srv.close();
  await sleep(50);
  console.log(`${name} ${label.padEnd(14)} wire: ${seen.join(' ') || '(nothing)'}`);
  console.log(`${' '.repeat(name.length + 1)}${' '.repeat(14)}state: ${state}  `
    + `events: ${events.join(' ') || '(none)'}`);
}

const only = process.argv[2];
const stacks = only === 'node' ? [[nodeHttp2, 'node  ']]
  : only === 'h2lean' ? [[h2lean, 'h2lean']]
    : [[nodeHttp2, 'node  '], [h2lean, 'h2lean']];

(async () => {
  for (const label of Object.keys(CASES)) {
    for (const [impl, name] of stacks) {
      await run(impl, name, label, CASES[label]);
    }
  }
})();
