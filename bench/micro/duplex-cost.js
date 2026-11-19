// Calibrated component probe: what the stream.Duplex machinery costs per "request"
// versus the bare EventEmitter that h2lean's server stream is today (OPTIMIZATION.md
// item 41). Isolates the machinery -- object construction, push bookkeeping, the nextTick
// flow loop, state transitions -- with no socket, no HPACK and no parse, so the number
// reads at the granularity where the effect acts (AGENTS.md 7).
//
//   CHUNKS=8 CSZ=2048 N=20000 node bench/micro/duplex-cost.js
//
// Three arms, deliberately distinct in what they measure:
//   lean    - ONE EventEmitter, synchronous emit('data'/'end') to the consumer. Today's design.
//   native  - ONE Duplex fed by stream.push(chunk) directly, no source EventEmitter. This is
//             the apples-to-apples arm: the same single-object shape as `lean`, and the exact
//             way node's `Http2Stream extends Duplex` is driven (core.js onData calls
//             stream.push). native-lean is the intrinsic Duplex machinery cost.
//   wrapper - an EventEmitter pushed INTO a Duplex the consumer reads. The adapter a
//             throwaway shim would bolt on; native is cheaper than this because it drops the
//             source object and the bridging listeners. Read wrapper-lean as an upper bound.
//
// A Duplex delivers 'data' asynchronously (the read runs on nextTick); an EventEmitter
// delivers synchronously -- so the CPU window is drained to setImmediate on every arm to
// capture the full, settled cost rather than only the synchronous part. Identical chunk
// counts and sizes across arms; a fresh object per request (allocation included). The metric
// is process.cpuUsage() us/request, ABBA interleaved over ROUNDS rounds, median reported.
'use strict';

const { EventEmitter } = require('node:events');
const { Duplex } = require('node:stream');

const CHUNKS = Number(process.env.CHUNKS || 8); // chunks per request
const CSZ = Number(process.env.CSZ || 2048); // bytes per chunk (8*2048 = 16 KB)
const N = Number(process.env.N || 20000); // requests per sample
const ROUNDS = Number(process.env.ROUNDS || 5); // ABBA rounds

const body = Buffer.alloc(CSZ, 0x61);
let sink = 0; // module-level so the async callbacks land and are not dead-code-eliminated

// a single EventEmitter handing 'data'/'end' straight to the consumer.
function armLean(n) {
  for (let i = 0; i < n; i++) {
    const src = new EventEmitter();
    src.on('data', (c) => {
      sink += c.length;
    });
    src.on('end', () => {
      sink += 1;
    });
    for (let k = 0; k < CHUNKS; k++) {
      src.emit('data', body);
    }
    src.emit('end');
  }
}

// native parity model: a single Duplex pushed directly, exactly how node feeds its
// Http2Stream. Same one-object-per-request shape as armLean -- the fair comparison.
function armNative(n) {
  for (let i = 0; i < n; i++) {
    const d = new Duplex({
      read() { /* the session pushes; there is nothing to pull */ },
      write(_c, _e, cb) {
        cb();
      },
      final(cb) {
        cb();
      }
    });
    d.on('data', (c) => {
      sink += c.length;
    });
    d.on('end', () => {
      sink += 1;
    });
    for (let k = 0; k < CHUNKS; k++) {
      d.push(body);
    }
    d.push(null);
  }
}

// the wrapper proxy: an EventEmitter pushed INTO a Duplex -- one object and a bridging
// hop more than the native case, so it overstates. Keep it to show the bridge's share.
function armWrapper(n) {
  for (let i = 0; i < n; i++) {
    const src = new EventEmitter();
    const d = new Duplex({
      read() { /* the source pushes; nothing to pull */ },
      write(_c, _e, cb) {
        cb();
      },
      final(cb) {
        cb();
      }
    });
    src.on('data', (c) => {
      d.push(c);
    });
    src.on('end', () => {
      d.push(null);
    });
    d.on('data', (c) => {
      sink += c.length;
    });
    d.on('end', () => {
      sink += 1;
    });
    for (let k = 0; k < CHUNKS; k++) {
      src.emit('data', body);
    }
    src.emit('end');
  }
}

function drain() {
  return new Promise((res) => setImmediate(res));
}

async function measure(fn) {
  for (let i = 0; i < 3; i++) {
    fn(3000); // warm
    await drain();
  }
  const c0 = process.cpuUsage();
  fn(N);
  await drain(); // let the Duplex's nextTick delivery finish inside the window
  const cpu = process.cpuUsage(c0);
  return (cpu.user + cpu.system) / N;
}

function median(a) {
  const s = [...a].sort((x, y) => x - y);
  const m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

(async () => {
  const lean = [];
  const native = [];
  const wrapper = [];
  const ARMS = [['lean', armLean], ['native', armNative], ['wrapper', armWrapper]];
  for (let r = 0; r < ROUNDS; r++) {
    // ABBA on even rounds, the reverse order on odd ones -- same total work, order alternates.
    const order = r % 2 === 0 ? ARMS : [...ARMS].reverse();
    for (const [name, fn] of order) {
      const v = await measure(fn);
      if (name === 'lean') {
        lean.push(v);
      } else if (name === 'native') {
        native.push(v);
      } else {
        wrapper.push(v);
      }
    }
  }
  const mL = median(lean);
  const mN = median(native);
  const mW = median(wrapper);
  console.log(`chunks=${CHUNKS}x${CSZ}B (body ${CHUNKS * CSZ / 1024}KB) N=${N} rounds=${ROUNDS} sink=${sink}`);
  console.log(`  lean    (EventEmitter, today): ${mL.toFixed(3)} us/req  [${lean.map((v) => v.toFixed(3)).join(' ')}]`);
  console.log(`  native  (Duplex, pushed direct): ${mN.toFixed(3)} us/req  [${native.map((v) => v.toFixed(3)).join(' ')}]  = intrinsic machinery: +${(mN - mL).toFixed(3)} us (x${(mN / mL).toFixed(2)})`);
  console.log(`  wrapper (EE -> Duplex bridge):   ${mW.toFixed(3)} us/req  [${wrapper.map((v) => v.toFixed(3)).join(' ')}]  = upper-bound proxy:   +${(mW - mL).toFixed(3)} us (x${(mW / mL).toFixed(2)})`);
  console.log(`  bridging hop alone (wrapper-native) = ${(mW - mN).toFixed(3)} us/req`);
})();
