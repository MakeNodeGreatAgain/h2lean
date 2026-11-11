// A measurement arm, not a feature: the outbound multi-chunk batch is handed to the socket
// as `socket.write(Buffer.concat(batch))` instead of the library's corked per-chunk
// `_writev`, so the two write strategies can be paired in one session on one machine.
//
//   PRELOAD=bench/concat-write.js node bench/h2load.js --server=h2lean --api=stream
//   CONCAT=1 PRELOAD=bench/concat-write.js node bench/h2load.js ...   (the concat arm)
//
// CONCAT is unset/0 for the baseline arm: it keeps the library's corked `_writePending`
// unchanged but still installs the same RSS sampler and the same batch-shape counters, so
// the two arms differ only in the write strategy and both the resident-memory and the
// cpuPerReq numbers are directly comparable.
//
// The sampler is the reason this file is required even for the baseline: peak RSS and the
// batch shape are not fields the server script reports, so they are written to stderr on
// exit (stderr is inherited and matrix.sh redirects it into the case file, while the
// server's own stdout is swallowed by bench/h2load.js). The RSS poll is a timer, not a
// per-write `process.memoryUsage()`, so it does not perturb cpuPerReq in one arm and not
// the other -- both arms pay the identical timer cost.
'use strict';
const path = require('node:path');
const { Http2Session } = require(path.join(__dirname, '..', 'lib', 'session'));

if (process.env.CONCAT === '1') {
  const orig = Http2Session.prototype._writePending;
  Http2Session.prototype._writePending = function () {
    if (this._outLen === 0 || !this._open || this._writing) {
      return orig.call(this);
    }
    this._writing = true;
    this._commit();
    const batch = this._outQ;
    this._outQ = [];
    this._frozenLen = this._outLen;
    this._outLen = 0;
    const socket = this.socket;
    if (batch.length === 1) {
      socket.write(batch[0], this._onWrittenCb);
    } else {
      // the whole point of the arm: one copy of every chunk into a fresh contiguous buffer,
      // then one write, instead of cork + N writes through _writev
      socket.write(Buffer.concat(batch), this._onWrittenCb);
    }
  };
  process.stderr.write('write: Buffer.concat arm\n');
} else {
  process.stderr.write('write: corked _writev baseline\n');
}

// Instrumentation is installed LAST, so it wraps whichever `_writePending` is live (the
// library's, or the concat arm's) and the batch-shape numbers mean the same thing in both
// arms: how many chunks a batch carries, and whether the multi-chunk path even fires.
let peakRss = process.memoryUsage().rss;
const sampler = setInterval(() => {
  const r = process.memoryUsage().rss;
  if (r > peakRss) {
    peakRss = r;
  }
}, 200);
sampler.unref();

let nBatches = 0;
let nMulti = 0;
let chunkSum = 0;
let batchBytesSum = 0;
let chunkMax = 0;
const hist = Object.create(null);
const inner = Http2Session.prototype._writePending;
Http2Session.prototype._writePending = function () {
  const starts = !this._writing && this._open && this._outLen > 0;
  const chunks = starts ? this._outQ.length + (this._stageLen > 0 ? 1 : 0) : 0;
  inner.call(this);
  if (starts) {
    nBatches++;
    chunkSum += chunks;
    batchBytesSum += this._frozenLen;
    if (chunks > chunkMax) {
      chunkMax = chunks;
    }
    if (chunks > 1) {
      nMulti++;
      hist[chunks] = (hist[chunks] || 0) + 1;
    }
  }
};

process.on('exit', () => {
  const r = process.memoryUsage().rss;
  if (r > peakRss) {
    peakRss = r;
  }
  process.stderr.write(`PEAK_RSS=${(peakRss / 1024 / 1024).toFixed(1)}MB\n`);
  process.stderr.write(`BATCH batches=${nBatches} multi=${nMulti} `
    + `avgChunks=${(chunkSum / (nBatches || 1)).toFixed(2)} maxChunks=${chunkMax} `
    + `avgBytes=${Math.round(batchBytesSum / (nBatches || 1))}\n`);
  const keys = Object.keys(hist).map(Number).sort((a, b) => a - b).slice(0, 12);
  process.stderr.write(`BATCH hist ${keys.map((k) => `${k}:${hist[k]}`).join(' ')}\n`);
});
