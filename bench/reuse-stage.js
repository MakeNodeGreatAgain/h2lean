// A measurement arm, not a feature: the outbound staging buffer is reused from one
// batch to the next instead of being allocated per batch. The census in
// out/alloc-count.js says that allocation is 1.01 per request and 1024 octets --
// the largest single buffer h2lean makes per request, and the only one that a
// shared pool could remove without touching a byte the application can see.
//
//   PRELOAD=bench/reuse-stage.js node bench/h2load.js --server=h2lean --api=stream
//
// The rule the reuse obeys is the one the library already states for a buffer that
// goes into _outQ: it is the socket's until the write completes, and writes
// complete in order, so the buffer is free once the batch's callback has run.
// That is what "!this._writing" means here -- the previous batch's write has
// landed. Nothing is queued either (!this._outQ.length), because a queued buffer
// has not been handed over yet.
//
// This is the arm that answers "is the allocation worth a pool?". A real pool
// would also hand the buffers back to a free list shared between sessions and
// would size them by class; this one keeps one buffer per session, so it also
// costs one staging buffer of resident memory per connection -- which a shared
// pool would not.
'use strict';
const path = require('node:path');
const { Http2Session } = require(path.join(__dirname, '..', 'lib', 'session'));

const origNeed = Http2Session.prototype._need;

Http2Session.prototype._commit = function () {
  if (this._stageLen === 0) {
    return;
  }
  this._outQ.push(this._stage.subarray(0, this._stageLen));
  this._stageKeep = this._stage; // the buffer the socket is about to own
  this._stage = null;
  this._stageLen = 0;
};

Http2Session.prototype._need = function (n) {
  if (this._stage !== null && this._stageLen + n <= this._stage.length) {
    return;
  }
  if (this._stage === null && this._outQ.length === 0 && !this._writing
    && this._stageKeep !== undefined && this._stageKeep !== null
    && this._stageKeep.length >= n) {
    this._stage = this._stageKeep; // the last batch's buffer, write completed
    this._stageLen = 0;
    return;
  }
  origNeed.call(this, n);
};
