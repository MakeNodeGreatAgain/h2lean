// Measurement arm: the same server with the generated `date` switched off, so the cost of
// the header can be paired against it in one session on one machine.
//
//   PRELOAD=bench/no-date.js node bench/h2load.js --server=h2lite --api=stream ...
//
// It patches the module-level `block` entry point that lib/stream.js calls, the way
// bench/no-table.js and bench/no-validate.js patch their own targets. The caller's option
// object is a fresh literal inside respond(), so setting the flag on it is enough and adds
// no allocation to the arm being measured.
'use strict';
const hdr = require('../lib/headers');
const real = hdr.block;
hdr.block = function (headers, sensitive, opts, session) {
  if (opts !== null && opts !== undefined) {
    opts.sendDate = false;
  }
  return real.call(this, headers, sensitive, opts, session);
};
