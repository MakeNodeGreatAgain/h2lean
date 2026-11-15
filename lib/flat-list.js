'use strict';

// A preallocated field list filled through push. `new Array(capacity)` plus
// `items[count++] = value` never allocates per block and never right-trims the
// store the way `length = 0` does -- the trim makes the next fill re-grow it from
// scratch on V8 (OPTIMIZATION.md item 38) -- and a cursor write beats both
// Array#push shapes at the sizes a header block reaches (bench/micro/prealloc.js,
// item 39). This class keeps the cursor write, hidden inside push(); the cost is
// the array staying HOLEY_ELEMENTS forever, so every read carries a hole check,
// which the whole fill-then-read cycle still pays for.
//
// push() doubles as the overflow detector: a write past the preallocation grows
// the store under the caller, and the oversized store is kept for the rest of the
// event-loop turn (the other blocks of the same read batch reuse it) while one
// setImmediate later -- the check phase of the same iteration -- it is rebuilt at
// the preallocation again, so one oversized block cannot pin its size for the
// process lifetime. The re-grow a workload whose blocks all exceed the capacity
// pays is one small array per turn; the common workload never allocates at all.
//
// Only items[0..count) is valid after a fill. The class fixes neither a size nor
// an instance: the capacity is the caller's decision, made where the fill pattern
// is known.
class FlatList {
  constructor(capacity) {
    this.capacity = capacity;
    this.items = new Array(capacity);
    this.count = 0;
    this._shrinkQueued = false;
  }

  // The decoder's write: appends one item, keeping count in lockstep with the
  // slot writes, and answers the growth question at the exact write that grows
  // the store -- no per-block tail check is needed.
  push(value) {
    if (this.count === this.items.length) {
      this.grown();
    }
    this.items[this.count++] = value;
  }

  // A fill starts at slot 0: move the cursor back. The store is left alone --
  // right-trimming it would make the next fill re-grow it from scratch (item 38).
  reset() {
    this.count = 0;
  }

  grown() {
    if (this._shrinkQueued) {
      return;
    }
    this._shrinkQueued = true;
    setImmediate(() => {
      this._shrinkQueued = false;
      if (this.items.length > this.capacity) {
        this.items = new Array(this.capacity);
      }
    });
  }
}

module.exports = FlatList;
