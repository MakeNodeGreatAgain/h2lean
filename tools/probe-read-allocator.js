// Does node hand out a fresh, stable buffer per socket read, or a view into
// memory it reuses for later reads? The server keeps every chunk it receives,
// stamps the sequence number it carried on arrival, and after thousands more
// reads checks whether the retained bytes still say the same thing.
//   node tools/probe-read-allocator.js [tls|plain] [chunkSize] [count] [keepFirst]
//
// keepFirst matters: holding on to every chunk pins its memory and would hide
// exactly the reuse this probe is looking for. With keepFirst=N only the first N
// chunks are kept, the rest are dropped so node is free to recycle that buffer.
'use strict';
const net = require('node:net');
const tls = require('node:tls');
const certificates = require('../test/fixtures-tls');

const MODE = process.argv[2] || 'plain';
const SIZE = Number(process.argv[3] || 16384);
const COUNT = Number(process.argv[4] || 300);
const KEEP = Number(process.argv[5] || 0); // 0 = keep everything

const retained = [];
let seen = 0;
// stamp each chunk with its own tag at arrival time, so the check below can tell
// "still the same bytes" from "overwritten by a later read"
function retain(chunk) {
  seen++;
  if (KEEP !== 0 && seen > KEEP) {
    return;
  } // drop it: memory may be reused
  if (chunk.length >= 4) {
    chunk._tag = chunk.readUInt32BE(0);
  }
  retained.push(chunk);
}

const srv = (MODE === 'tls'
  ? tls.createServer(certificates(), (sock) => {
    sock.on('data', retain); sock.on('error', () => {});
  })
  : net.createServer((sock) => {
    sock.on('data', retain); sock.on('error', () => {});
  }));

srv.listen(0, '127.0.0.1', () => {
  const port = srv.address().port;
  const client = MODE === 'tls'
    ? tls.connect({ host: '127.0.0.1', port, rejectUnauthorized: false })
    : net.connect(port, '127.0.0.1');
  client.on('error', (e) => console.log('client error', e.code || e.message));

  function send(i) {
    if (i === COUNT) {
      setTimeout(check, 300); return;
    }
    const b = Buffer.alloc(SIZE);
    b.writeUInt32BE(i, 0); // each chunk carries its own number
    if (!client.write(b)) {
      client.once('drain', () => send(i + 1));
    } else {
      send(i + 1);
    }
  }

  function check() {
    let mismatch = 0, example = null;
    const byBuffer = new Map();
    for (const c of retained) {
      const now = c.readUInt32BE(0);
      if (c._tag !== undefined && c._tag !== now) {
        mismatch++;
        if (!example) {
          example = { was: c._tag, now: now };
        }
      }
      if (!byBuffer.has(c.buffer)) {
        byBuffer.set(c.buffer, 0);
      }
      byBuffer.set(c.buffer, byBuffer.get(c.buffer) + 1);
    }
    let shared = 0;
    for (const count of byBuffer.values()) {
      if (count > 1) {
        shared++;
      }
    }
    console.log(JSON.stringify({
      mode: MODE, chunkSize: SIZE, readsSeen: seen, kept: retained.length,
      distinctArrayBuffers: byBuffer.size,
      arrayBuffersReusedBySeveralChunks: shared,
      tagMismatchAfterLaterReads: mismatch,
      example: example
    }, null, 1));
    process.exit(0);
  }

  client.on(MODE === 'tls' ? 'secureConnect' : 'connect', () => send(0));
});
