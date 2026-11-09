// What does one header field line cost to validate, and can a cache beat the test?
//
// Four questions, all asked per field line, which is the granularity that matters:
// a compat response passes through parse() with five to ten names, and the stream API
// does the same whenever its header object is not frozen.
//
//   1. isToken.test(name) versus a cache of names already known to be legal.
//   2. CONNECTION_SPECIFIC as a Set versus as an Array with includes() -- five short
//      names, and the earlier container measurement (bench/queue-shape.js) said Array
//      wins at that size, which this repeats or refutes for lookups instead of appends.
//   3. The current httpDate(), which calls Date.now() on every response, against the
//      shape node's own HTTP/1.1 code uses: a cached string plus an unref'd one-shot
//      timer that only invalidates it, so a normal response reads one variable.
//   4. The same invalidation by an interval instead, so the two timer shapes are
//      measured against each other rather than asserted.
//
// Everything is timed per call, ns, with the result fed into a sink so V8 cannot
// hoist the work out of the loop.
//
//   node bench/header-validate.js
'use strict';

const SINK = { n: 0 };

// The names a real response carries. Pseudo-headers are excluded because parse()
// handles them before the token check -- only field names reach the regex. Order and
// length matter: a regex over a 14-char name is not the same cost as over a 4-char one.
const NAMES = ['content-type', 'content-length', 'cache-control',
  'date', 'server', 'etag', 'last-modified', 'x-request-id',
  'vary', 'set-cookie', 'location', 'accept-ranges', 'x-frame-options', 'link'];
const CONNECTION_SPECIFIC_SET = new Set(['connection', 'keep-alive', 'proxy-connection',
  'transfer-encoding', 'upgrade']);
const CONNECTION_SPECIFIC_ARRAY = ['connection', 'keep-alive', 'proxy-connection',
  'transfer-encoding', 'upgrade'];

function time(label, iters, fn) {
  for (let i = 0; i < iters / 10; i++) {
    fn(i);
  } // warm up, let JIT settle
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < iters; i++) {
    fn(i);
  }
  const t1 = process.hrtime.bigint();
  console.log(`  ${label.padEnd(34)} ${(Number(t1 - t0) / iters).toFixed(2)} ns`);
}

// ---- 1. token validation ------------------------------------------------------
const isToken = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;

function benchToken(iters) {
  const set = new Set();
  const arr = [];
  const obj = Object.create(null);
  console.log(`token test, ${NAMES.length} distinct names, ${iters} calls per arm`
    + ' (all of them legal, which is the normal case)');
  time('regex only (current)', iters, (i) => {
    SINK.n += isToken.test(NAMES[i % NAMES.length]) ? 1 : 0;
  });
  time('Set.has, miss path adds', iters, (i) => {
    const n = NAMES[i % NAMES.length];
    if (set.has(n)) {
      SINK.n++; return;
    }
    if (!isToken.test(n)) {
      throw new Error('unreachable');
    }
    set.add(n); SINK.n++;
  });
  time('Array.includes, miss path pushes', iters, (i) => {
    const n = NAMES[i % NAMES.length];
    if (arr.includes(n)) {
      SINK.n++; return;
    }
    if (!isToken.test(n)) {
      throw new Error('unreachable');
    }
    arr.push(n); SINK.n++;
  });
  time('null-proto object, own check', iters, (i) => {
    const n = NAMES[i % NAMES.length];
    if (obj[n] === 1) {
      SINK.n++; return;
    }
    if (!isToken.test(n)) {
      throw new Error('unreachable');
    }
    obj[n] = 1; SINK.n++;
  });
  // Names only ever reach this check after parse() lowercased them, but an
  // application that reflects untrusted input into a header name could grow the cache,
  // so measure the bounded version too: stop inserting past a limit, fall back to the
  // regex for anything new.
  const bounded = new Set();
  const LIMIT = 512;
  time('Set.has bounded at 512 (15 known)', iters, (i) => {
    const n = NAMES[i % NAMES.length];
    if (bounded.has(n)) {
      SINK.n++; return;
    }
    if (!isToken.test(n)) {
      throw new Error('unreachable');
    }
    if (bounded.size < LIMIT) {
      bounded.add(n);
    }
    SINK.n++;
  });
  // The adversarial shape: a new name every call, so nothing ever hits and the cache
  // only adds cost. Bounded and unbounded arms are compared here.
  const adversarial = (i) => `x-${i % 100000}`;
  const adv = new Set();
  time('Set.has, every name new (bounded)', iters / 10, (i) => {
    const n = adversarial(i);
    if (adv.has(n)) {
      SINK.n++; return;
    }
    if (!isToken.test(n)) {
      throw new Error('unreachable');
    }
    if (adv.size < LIMIT) {
      adv.add(n);
    }
    SINK.n++;
  });
  time('regex only, every name new', iters / 10, (i) => {
    SINK.n += isToken.test(adversarial(i)) ? 1 : 0;
  });
}

// ---- 2. connection-specific lookup -------------------------------------------
function benchConnection(iters) {
  // All five forbidden names plus the legal ones they are compared against, because
  // the cost of a miss is what a normal response pays.
  const probe = ['connection', 'content-type', 'upgrade', 'content-length', 'keep-alive',
    'server', 'transfer-encoding', 'etag', 'proxy-connection', 'vary'];
  console.log(`\nconnection-specific lookup, ${probe.length} names`);
  time('Set.has (current)', iters, (i) => {
    SINK.n += CONNECTION_SPECIFIC_SET.has(probe[i % probe.length]) ? 1 : 0;
  });
  time('Array.includes', iters, (i) => {
    SINK.n += CONNECTION_SPECIFIC_ARRAY.includes(probe[i % probe.length]) ? 1 : 0;
  });
  time('null-proto object member', iters, (() => {
    const o = Object.create(null);
    for (const n of CONNECTION_SPECIFIC_ARRAY) {
      o[n] = true;
    }
    return (i) => {
      SINK.n += o[probe[i % probe.length]] === true ? 1 : 0;
    };
  })());
}

// ---- 3/4. the date header -----------------------------------------------------
let dateStamp = -1;
let dateString = '';
function httpDateNow() { // current implementation
  const now = Date.now();
  if (now !== dateStamp) {
    dateStamp = now;
    dateString = new Date(now).toUTCString();
  }
  return dateString;
}

let utcCache; // node internal/http shape
let timer = null;
function arm() {
  const d = new Date();
  utcCache = d.toUTCString();
  timer = setTimeout(() => {
    utcCache = undefined; timer = null;
  },
  1000 - d.getMilliseconds());
  timer.unref();
}
function httpDateLazy() {
  if (utcCache === undefined) {
    arm();
  }
  return utcCache;
}

let intervalString = ''; // the proposal as stated
let intervalTimer = null;
function httpDateInterval() {
  if (intervalTimer === null) {
    intervalString = new Date().toUTCString();
    intervalTimer = setInterval(() => {
      intervalString = new Date().toUTCString();
    }, 1000);
    intervalTimer.unref();
  }
  return intervalString;
}

function benchDate(iters) {
  console.log(`\ndate header, ${iters} calls per arm`);
  time('Date.now() compare (current)', iters, () => {
    SINK.n += httpDateNow().length;
  });
  time('invalidate-by-timeout (node way)', iters, () => {
    SINK.n += httpDateLazy().length;
  });
  time('setInterval refresh', iters, () => {
    SINK.n += httpDateInterval().length;
  });
  // What each arm actually costs across a second boundary matters: the invalidating
  // version rebuilds once per second, so measure the rebuild too.
  time('toUTCString() itself', iters / 100, () => {
    SINK.n += new Date(Date.now()).toUTCString().length;
  });
  if (timer !== null) {
    clearTimeout(timer);
  }
  if (intervalTimer !== null) {
    clearInterval(intervalTimer);
  }
}

const ITERS = 2_000_000;
benchToken(ITERS);
benchConnection(ITERS);
benchDate(ITERS);
console.log(`\n(sink ${SINK.n})`);
