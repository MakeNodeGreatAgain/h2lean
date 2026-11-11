// How does a large response actually reach the socket?
//
// bench/h2load.js --resp=262144 put h2lean 1.4x behind node:http2 on cpuPerReq
// while h2lean won by 1.9x at --resp=32768, and a --cpu-prof run of the 256 KB
// case spent 75% of busy time inside node's writeBuffer, 14% in the memcpy into the
// staging buffer and 8.6% in compacting what was left behind. This probe answers what
// those numbers are made of, by counting on the real code path (no patched copy of it):
//
//   * how many socket.write() calls one response needs, and their sizes -- the
//     send window is 64 KB by default, so a 256 KB body cannot go out at once;
//   * how many chunks each of those writes carries, since frames are queued as the
//     buffers that already hold them and handed over corked as one writev;
//   * how many bytes travel by reference and how many still have to be copied, which
//     is the difference between a body frame and a header block;
//   * how full the queue ever gets behind an outstanding write, which is what the
//     MAX_OUT_BATCH ceiling has to bound.
//
//   node tools/probe-outbound.js [respBytes] [requests] [clientWindowBytes]
'use strict';
const http2 = require('node:http2');
const h2lean = require('../index');
const { Http2Session: Session, MAX_OUT_BATCH, BODY_MIN } = require('../lib/session');

const SIZE = Number(process.argv[2] || 262144);
const REQS = Number(process.argv[3] || 20);
// What the client advertises to us. This, not the server's own setting, bounds how
// much DATA we may put on the wire per write, so it is the number that decides how
// many writes one large response costs. Default is node's 65 535.
const WIN = Number(process.argv[4] || 0);
const BODY = Buffer.alloc(SIZE, 0x61);
const HEADERS = Object.freeze({ ':status': 200, 'content-type': 'text/plain' });

const stat = { writes: 0, writeBytes: 0, biggest: 0, frames: 0, chunks: 0, chunkPeak: 0,
  refBytes: 0, copyBytes: 0, queuePeak: 0, backlogPeak: 0, inFlightPeak: 0 };

// Counting wrappers installed on the prototype, so the real methods still run.
const writePending = Session.prototype._writePending;
Session.prototype._writePending = function () {
  // read the batch before the method takes it: _outQ is emptied by the call, and the
  // staging buffer joins it as one more chunk on the way out
  const starts = !this._writing && this._open && this._outLen > 0;
  const chunks = starts ? this._outQ.length + (this._stageLen > 0 ? 1 : 0) : 0;
  const had = this._writing;
  if (this._outLen > stat.backlogPeak) {
    stat.backlogPeak = this._outLen;
  }
  writePending.call(this);
  if (!had && this._writing) {
    stat.writes++;
    stat.writeBytes += this._frozenLen;
    stat.chunks += chunks;
    if (chunks > stat.chunkPeak) {
      stat.chunkPeak = chunks;
    }
    if (this._frozenLen > stat.biggest) {
      stat.biggest = this._frozenLen;
    }
  }
};

const onWritten = Session.prototype._onWritten;
Session.prototype._onWritten = function () {
  if (this._frozenLen > stat.inFlightPeak) {
    stat.inFlightPeak = this._frozenLen;
  }
  onWritten.call(this);
};

const frame = Session.prototype._frame;
Session.prototype._frame = function (type, flags, sid, payload, owned) {
  const len = payload === null || payload === undefined ? 0 : payload.length;
  stat.frames++;
  // the same rule _frame() applies
  if (owned && len >= BODY_MIN) {
    stat.refBytes += len;
  } else {
    stat.copyBytes += len;
  }
  if (this._outLen > stat.queuePeak) {
    stat.queuePeak = this._outLen;
  }
  frame.call(this, type, flags, sid, payload, owned);
};

const srv = h2lean.createServer();
srv.on('stream', (stream) => {
  stream.respond(HEADERS); stream.end(BODY);
});

srv.listen(0, '127.0.0.1', () => {
  const port = srv.address().port;
  const client = WIN ? http2.connect(`http://127.0.0.1:${port}`, {
    settings: { initialWindowSize: WIN }
  }) : http2.connect(`http://127.0.0.1:${port}`);
  client.on('error', () => {});
  let done = 0;
  let received = 0;
  let stopped = false;

  function one() {
    if (stopped) {
      return;
    }
    const req = client.request({ ':method': 'GET', ':path': '/' });
    req.on('data', (d) => {
      received += d.length;
    });
    req.on('error', () => {});
    req.on('end', () => {
      if (++done >= REQS) {
        finish();
      } else {
        one();
      }
    });
    req.end();
  }

  function finish() {
    if (stopped) {
      return;
    }
    stopped = true;
    console.log(`\n${done} responses of ${SIZE} bytes, client window ${WIN || 65535}, `
              + `${received / done} bytes each received`);
    console.log(`  socket.write() calls     ${(stat.writes / done).toFixed(1)} per response`);
    console.log(`  largest single write     ${stat.biggest} bytes`);
    console.log(`  written to the socket    ${(stat.writeBytes / done).toFixed(0)} B/response`);
    console.log(`  frames per response      ${(stat.frames / done).toFixed(1)}`
              + ` (${(stat.chunks / done).toFixed(1)} chunks handed to the socket)`);
    console.log(`  largest batch            ${stat.chunkPeak} chunks`
              + ` (${(stat.chunks / stat.writes).toFixed(1)} per write on average)`);
    console.log(`  payload by reference     ${(stat.refBytes / done).toFixed(0)} B/response`);
    console.log(`  payload copied           ${(stat.copyBytes / done).toFixed(0)} B/response`
              + ' (header blocks and control frames)');
    console.log(`  peak queued, unwritten   ${stat.queuePeak} B`
              + ` (against the ${MAX_OUT_BATCH} B ceiling)`);
    console.log(`  most bytes in flight     ${stat.inFlightPeak} B`
              + ' (handed over, callback not yet fired)');
    console.log(`  backlog, worst moment    ${stat.backlogPeak} B added while a write was`
              + ' already in flight\n');
    client.destroy();
    srv.close();
  }

  for (let i = 0; i < 4; i++) {
    one();
  }
});
