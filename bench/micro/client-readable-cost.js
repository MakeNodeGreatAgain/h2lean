// Calibrated component probe: what the CLIENT response-read delivery mechanism costs, and
// what a hand-rolled readable would have to reproduce. Decision input for "replace the client
// Duplex with a bare EventEmitter that emits 'data' synchronously, leaning on HTTP/2 flow
// control for backpressure" (the server already does exactly that -- lib/stream.js _recvData).
//
//   CHUNKS=8 CSZ=2048 N=20000 node bench/micro/client-readable-cost.js
//
// Isolates the read-delivery machinery only -- no socket, no HPACK, no parse -- so the number
// reads at the granularity where the effect acts (AGENTS.md 7). The `duplex` arm uses
// require('node:stream'), the very classes node's ClientHttp2Stream extends (core.js:2128), so
// `hand - duplex` is the intrinsic saving against NODE's mechanism, not against a strawman.
// h2lean's client stream is driven the same way: _onData -> this.push(buf), _read(){} empty
// (lib/client-stream.js:181/:223). The end-to-end client share is a separate measurement
// (OPTIMIZATION.md item 30 / bench/client); this probe is the mechanism, not the whole stack.
//
// Arms, identical chunk counts/sizes and a fresh object per request (allocation included):
//   hand        - ONE EventEmitter, synchronous emit('data'/'end'). The proposed bare client.
//   duplex-data - ONE Duplex pushed directly, consumed via 'data' (flowing). node/h2lean today.
//   duplex-read - the same Duplex consumed via read() in paused mode ('readable'). A surface
//                 `hand` does not have; priced to show what emulating it would cost.
//   duplex-await- the same Duplex consumed via `for await`. Async-iterator surface.
//   duplex-pipe - the same Duplex consumed via .pipe(sink). The req.pipe(server).pipe(res) idiom.
//
// A Duplex delivers 'data' asynchronously (the read runs on nextTick); an EventEmitter delivers
// synchronously -- so the CPU window is drained to setImmediate on every arm to capture the
// full settled cost. Metric: process.cpuUsage() us/request, ABBA interleaved over ROUNDS rounds,
// median + spread reported. `hand` only supports the 'data' consumption; the read/await/pipe
// arms exist to price the surfaces a hand-rolled client would have to rebuild by hand.
'use strict';

const { EventEmitter } = require('node:events');
const { Duplex, Writable } = require('node:stream');

const CHUNKS = Number(process.env.CHUNKS || 8);
const CSZ = Number(process.env.CSZ || 2048);
const N = Number(process.env.N || 20000);
const ROUNDS = Number(process.env.ROUNDS || 5);

const body = Buffer.alloc(CSZ, 0x61);
let sink = 0; // module-level so async callbacks land and are not dead-code-eliminated

// The client Duplex configuration h2lean actually uses (lib/client-stream.js:47).
function clientDuplex() {
  return new Duplex({
    allowHalfOpen: true,
    decodeStrings: false,
    read() { /* the session pushes; nothing to pull */ },
    write(_c, _e, cb) {
      cb();
    },
    final(cb) {
      cb();
    }
  });
}

function feed(d) {
  for (let k = 0; k < CHUNKS; k++) {
    d.push(body);
  }
  d.push(null);
}

function armHand(n) {
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

function armDuplexData(n) {
  for (let i = 0; i < n; i++) {
    const d = clientDuplex();
    d.on('data', (c) => {
      sink += c.length;
    });
    d.on('end', () => {
      sink += 1;
    });
    feed(d);
  }
}

function armDuplexRead(n) {
  for (let i = 0; i < n; i++) {
    const d = clientDuplex();
    d.on('readable', () => {
      let c;
      while ((c = d.read()) !== null) {
        sink += c.length;
      }
    });
    d.on('end', () => {
      sink += 1;
    });
    feed(d);
  }
}

function armDuplexAwait(n) {
  for (let i = 0; i < n; i++) {
    const d = clientDuplex();
    feed(d);
    // the iterator is consumed inside the same drained CPU window
    (async () => {
      for await (const c of d) {
        sink += c.length;
      }
      sink += 1;
    })();
  }
}

function armDuplexPipe(n) {
  for (let i = 0; i < n; i++) {
    const d = clientDuplex();
    const out = new Writable({ write(c, _e, cb) {
      sink += c.length; cb();
    } });
    out.on('finish', () => {
      sink += 1;
    });
    d.pipe(out);
    feed(d);
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
  await drain(); // let nextTick delivery / async iteration / pipe finish inside the window
  await drain();
  const cpu = process.cpuUsage(c0);
  return (cpu.user + cpu.system) / N;
}

function median(a) {
  const s = [...a].sort((x, y) => x - y);
  const m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

function spread(a) {
  return Math.max(...a) - Math.min(...a);
}

(async () => {
  const ARMS = [
    ['hand', armHand],
    ['duplex-data', armDuplexData],
    ['duplex-read', armDuplexRead],
    ['duplex-await', armDuplexAwait],
    ['duplex-pipe', armDuplexPipe]
  ];
  const samples = Object.fromEntries(ARMS.map(([name]) => [name, []]));
  for (let r = 0; r < ROUNDS; r++) {
    const order = r % 2 === 0 ? ARMS : [...ARMS].reverse();
    for (const [name, fn] of order) {
      samples[name].push(await measure(fn));
    }
  }
  const med = Object.fromEntries(ARMS.map(([name]) => [name, median(samples[name])]));
  console.log(`chunks=${CHUNKS}x${CSZ}B (body ${CHUNKS * CSZ / 1024}KB) N=${N} rounds=${ROUNDS} sink=${sink}`);
  for (const [name] of ARMS) {
    const s = samples[name];
    console.log(`  ${name.padEnd(13)} ${med[name].toFixed(3)} us/req  spread=${spread(s).toFixed(3)}  [${s.map((v) => v.toFixed(3)).join(' ')}]`);
  }
  const delta = med['duplex-data'] - med.hand;
  console.log(`  intrinsic read-delivery machinery (duplex-data - hand) = +${delta.toFixed(3)} us/req (x${(med['duplex-data'] / med.hand).toFixed(2)})`);
  console.log(`  surfaces a hand-rolled client must rebuild: read +${(med['duplex-read'] - med['duplex-data']).toFixed(3)}, await +${(med['duplex-await'] - med['duplex-data']).toFixed(3)}, pipe +${(med['duplex-pipe'] - med['duplex-data']).toFixed(3)} us/req vs duplex-data`);
})();
