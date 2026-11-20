// What a server stream puts on the wire when the response is STARTED and the stream is then
// destroyed in the same turn of the loop, against a raw peer.
//
// Measured shape of the answer (node v26.12.1, HEADERS without END_STREAM so the request
// half stays open the way an upload the handler never reads does):
//
//   write(''); destroy()          node: RST(NO_ERROR) only -- no HEADERS at all
//   respond(); destroy()          node: RST(NO_ERROR) only
//   respond(); close()            node: RST(NO_ERROR) only
//   respond(); end(); destroy()   node: RST(NO_ERROR) only -- no empty DATA either
//   write(''); write('x'); destroy()  node: RST(NO_ERROR) only (the second write is still
//                                 queued in the Duplex, so nothing was submitted)
//   write('x'); destroy()         node: HEADERS/EH{:status 200} DATA(x) RST(NO_ERROR)
//   respond(); write('x'); destroy()  node: the same -- a non-empty write flushes at once
//   respond(); end('body'); destroy() node: HEADERS DATA(body) RST -- the DATA loses its
//                                 END_STREAM, because endCheckNT had not run yet
//   respond(); setImmediate(destroy)  node: HEADERS RST(NO_ERROR) -- a turn later the
//                                 headers are already out, and h2lean agrees
//
// So the trigger is the TURN, not the empty write: node submits the HEADERS to nghttp2 and
// nothing sends them before the destroy, and the session teardown discards what was queued.
// h2lean frames them into the outbound batch as respond() runs and the batch leaves at the
// end of the read, so they reach the peer. headersSent and rstCode agree on both arms.
// This is the destroy side of the refusal-batch divergence (AGENTS.md section 10,
// CONTRIBUTING.md section 5); the shapes are pinned in test/response-start.test.js.
//
// The empty end() with no respond() at all is a DIFFERENT rule -- node puts nothing on the
// wire and leaves headersSent false -- and belongs to tools/probe-response-start.js.
//
//   node tools/probe-zero-write.js          -> both stacks
//   node tools/probe-zero-write.js node     -> node only
'use strict';
const net = require('node:net');
const f = require('../lib/frames');
const { Decoder } = require('../lib/hpack');
const h2lean = require('../index');
const nodeHttp2 = require('node:http2');

const REQ = Buffer.from([0x82, 0x84, 0x86, 0x01, 9, ...Buffer.from('localhost', 'latin1')]);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const CODES = { 0: 'NO_ERROR', 1: 'PROTOCOL_ERROR', 2: 'INTERNAL_ERROR', 8: 'CANCEL' };
const code = (n) => CODES[n] || String(n);

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

const CASES = {
  'write("")+destroy': (s) => {
    s.write(''); s.destroy();
  },
  'write(0buf)+destroy': (s) => {
    s.write(Buffer.alloc(0)); s.destroy();
  },
  'write("")+destroy(err)': (s) => {
    s.write(''); s.destroy(new Error('boom'));
  },
  'write("")+close': (s) => {
    s.write(''); s.close();
  },
  'respond+destroy': (s) => {
    s.respond({ ':status': 200 }); s.destroy();
  },
  'respond+close': (s) => {
    s.respond({ ':status': 200 }); s.close();
  },
  'respond+end+destroy': (s) => {
    s.respond({ ':status': 200 }); s.end(); s.destroy();
  },
  'respond+end(body)+destroy': (s) => {
    s.respond({ ':status': 200 }); s.end('body'); s.destroy();
  },
  'write("")+write(x)+destroy': (s) => {
    s.write(''); s.write('x'); s.destroy();
  },
  // the controls: bytes submitted, and a turn allowed to pass
  'write(x)+destroy': (s) => {
    s.write('x'); s.destroy();
  },
  'respond+write(x)+destroy': (s) => {
    s.respond({ ':status': 200 }); s.write('x'); s.destroy();
  },
  'respond+imm+destroy': (s) => {
    s.respond({ ':status': 200 }); setImmediate(() => s.destroy());
  },
  'write("")+tick+destroy': (s) => {
    s.write(''); setTimeout(() => s.destroy(), 30);
  },
  // the empty end() with no respond: the other rule, for comparison
  'end()': (s) => {
    s.end();
  },
  'write("")+end': (s) => {
    s.write(''); s.end();
  }
};

async function run(impl, name, label, app) {
  const srv = impl.createServer();
  const events = [];
  let ref = null;
  let threw = null;
  srv.on('stream', (stream) => {
    ref = stream;
    stream.on('error', (e) => events.push('error:' + e.code));
    stream.on('finish', () => events.push('finish'));
    stream.on('close', () => events.push('close'));
    stream.on('aborted', () => events.push('aborted'));
    try {
      app(stream);
    } catch (e) {
      threw = e.code || e.message;
    }
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
      if (type === f.SETTINGS) {
        continue;
      }
      if (type === f.HEADERS) {
        const out = [];
        decoder.decode(body, 0, body.length, out);
        const fields = [];
        for (let i = 0; i < out.length; i += 2) {
          // the date comes from each server's clock: never part of the comparison
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
  const state = ref === null ? '(no stream)' : JSON.stringify({
    headersSent: ref.headersSent, closed: ref.closed,
    destroyed: ref.destroyed, rst: ref.rstCode
  });
  sock.destroy();
  srv.close();
  await sleep(50);
  console.log(`${name} ${label.padEnd(26)} wire: ${seen.join(' ') || '(nothing)'}`);
  console.log(`${' '.repeat(name.length + 1)}${' '.repeat(26)}state: ${state}  `
    + `events: ${events.join(' ') || '(none)'}${threw ? '  threw: ' + threw : ''}`);
}

const only = process.argv[2];
const stacks = only === 'node' ? [[nodeHttp2, 'node  ']]
  : only === 'h2lean' ? [[h2lean, 'h2lean']]
    : [[nodeHttp2, 'node  '], [h2lean, 'h2lean']];

(async () => {
  const names = process.argv[3] ? process.argv[3].split(',') : Object.keys(CASES);
  for (const label of names) {
    for (const [impl, name] of stacks) {
      await run(impl, name, label, CASES[label]);
    }
  }
  process.exit(0);
})();
