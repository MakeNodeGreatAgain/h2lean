// Write-path counters for the benchmark server: are our socket writes big and few,
// or small and many, and what does it cost to assemble them?
//
// tools/probe-outbound.js answers this on four concurrent streams, which is not the
// shape the matrix runs (64 clients x 32 streams). Under real fan-out the connection
// window, the outstanding batch and the queues all behave differently, and the claim
// that "writeBuffer dominates because we write small packets often" can only be judged
// against the actual size histogram. This module measures it on the live server.
//
// Everything is wrapped onto the prototypes, so the real methods still run and the
// numbers come from the code path the benchmark is actually exercising.
//
//   OUTSTAT=1 node bench/h2lean-h2.js
'use strict';
const { Http2Session, MAX_OUT_BATCH, BODY_MIN } = require('../lib/session');

const BUCKETS = [128, 1024, 8192, 32768, 65536];
const s = {
  writes: 0, bytes: 0, biggest: 0, hist: new Array(BUCKETS.length + 1).fill(0),
  frames: 0, queuedBytes: 0, copiedBytes: 0, chunkRefs: 0, batchPeak: 0,
  queuePeak: 0, bytesHeld: 0,
  splitFrames: 0, splitBytes: 0,
  hdrAppends: 0, hdrBytes: 0, hdrGrows: 0, hdrPeak: 0
};

function bucket(n) {
  for (let i = 0; i < BUCKETS.length; i++) {
    if (n < BUCKETS[i]) {
      return i;
    }
  }
  return BUCKETS.length;
}

function install() {
  // One write to the socket per _writePending() that actually starts one: that is the
  // only place the session hands bytes over, so the histogram of _frozenLen is the
  // histogram of our writes, without patching the transport socket at all. The chunk
  // count has to be read before the call: the batch is taken out of _outQ by the
  // method itself, and the staging buffer joins it as one more chunk on the way out.
  const writePending = Http2Session.prototype._writePending;
  Http2Session.prototype._writePending = function () {
    const starts = !this._writing && this._open && this._outLen > 0;
    const chunks = starts
      ? this._outQ.length + (this._stageLen > 0 ? 1 : 0)
      : 0;
    const backlog = this._outLen;
    const had = this._writing;
    writePending.call(this);
    if (!had && this._writing) {
      s.writes++;
      s.bytes += this._frozenLen;
      s.chunkRefs += chunks;
      if (chunks > s.batchPeak) {
        s.batchPeak = chunks;
      }
      if (backlog > s.queuePeak) {
        s.queuePeak = backlog;
      }
      if (this._frozenLen > s.biggest) {
        s.biggest = this._frozenLen;
      }
      s.hist[bucket(this._frozenLen)]++;
    }
  };

  const frame = Http2Session.prototype._frame;
  Http2Session.prototype._frame = function (type, flags, sid, payload, owned) {
    const len = payload === null || payload === undefined ? 0 : payload.length;
    s.frames++;
    // the same rule _frame() applies: a referenced body is never copied, everything
    // else goes into the staging buffer
    if (owned && len >= BODY_MIN) {
      s.queuedBytes += len;
    } else {
      s.copiedBytes += len;
    }
    return frame.call(this, type, flags, sid, payload, owned);
  };

  // Bytes handed to the socket whose write callback has not fired yet: they are still
  // alive as far as the peer is concerned, and this is what a peer can make us hold.
  const onWritten = Http2Session.prototype._onWritten;
  Http2Session.prototype._onWritten = function () {
    if (this._frozenLen > s.bytesHeld) {
      s.bytesHeld = this._frozenLen;
    }
    return onWritten.call(this);
  };

  // The inbound path: a frame that straddles two reads is kept as the views it
  // arrived in, and only a frame that is decoded into objects is put in a buffer
  // of its own. These counters say how often that happens and how many octets it
  // covers.
  const dispatchPieces = Http2Session.prototype._dispatchPieces;
  Http2Session.prototype._dispatchPieces = function (type, flags, sid, pieces, len) {
    s.splitFrames++;
    s.splitBytes += len;
    return dispatchPieces.call(this, type, flags, sid, pieces, len);
  };

  const appendHdr = Http2Session.prototype._appendHdr;
  Http2Session.prototype._appendHdr = function (buf, start, len) {
    const before = this._hdr;
    s.hdrAppends++;
    s.hdrBytes += len;
    appendHdr.call(this, buf, start, len);
    if (this._hdr !== before) {
      s.hdrGrows++;
    }
    if (this._hdr && this._hdr.length > s.hdrPeak) {
      s.hdrPeak = this._hdr.length;
    }
  };

  return s;
}

function report(reqs) {
  const per = (v) => (v / reqs).toFixed(2);
  const labels = BUCKETS.map((b) => `<${b}`).concat([`>=${BUCKETS[BUCKETS.length - 1]}`]);
  console.log(`[outstat] per request: writes ${per(s.writes)} bytes ${per(s.bytes)} `
    + `frames ${per(s.frames)} chunkrefs ${per(s.chunkRefs)} `
    + `referenced ${per(s.queuedBytes)}B copied ${per(s.copiedBytes)}B `
    + `split frames ${per(s.splitFrames)} ${per(s.splitBytes)}B `
    + `hdr.append ${per(s.hdrAppends)} ${per(s.hdrBytes)}B`);
  const hist = s.hist.map((n, i) => `${labels[i]}:${n}`).join(' ');
  console.log(`[outstat] write sizes  total=${s.writes} largest=${s.biggest}B ${hist}`);
  console.log(`[outstat] queue  peak ${s.queuePeak}B outstanding against the `
    + `${MAX_OUT_BATCH}B ceiling, largest batch ${s.batchPeak} chunks, `
    + `most bytes in flight ${s.bytesHeld}B`);
  console.log(`[outstat] split frames ${s.splitFrames}, ${s.splitBytes}B kept as views`);
  console.log(`[outstat] _hdr grew ${s.hdrGrows}x, peak buf ${s.hdrPeak}B`);
  // The buffer is per session and never shrinks, so the process total is what a peer
  // can drive it to: this is the number that separates "a big batch" from "a defect".
  const mem = process.memoryUsage();
  console.log(`[outstat] memory  external ${(mem.external / 1048576).toFixed(0)}MB `
            + `rss ${(mem.rss / 1048576).toFixed(0)}MB`);
}

module.exports = { install, report };
