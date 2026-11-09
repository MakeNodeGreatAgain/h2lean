// Which collection shape is cheapest for "keep the pieces, read them later"?
//
// Asked as: should a frame that arrived in pieces be kept in an Array or in a Set?
// The question the file was written for -- "could lib/slab.js stop copying small
// buffers into one big this.buf and hold them in an Array or a Set instead?" --
// has been answered by the code itself: the slab is gone and the pieces are kept
// in an Array of views (lib/pieces.js), so the Array arm below is the shape the
// server ships. The Set is the alternative, and Array and Set are not
// interchangeable: a Set has no index, so "read by position" costs a spread or an
// iterator each time.
//
// Measured per chunk, ns, with the read included, because assembling without
// consuming is not the comparison. Buffer.concat is in as the price of giving a
// contiguous consumer (the HPACK decoder) what it expects.
//
//   node bench/queue-shape.js
//
// One caveat that decides how much this is worth: the path this measures is a
// cold one. A frame that straddles two reads is rare on a loopback, and OUTSTAT=1
// on the live server counts them (bench/outstat.js).
'use strict';

const ROUNDS = 5;
const OPS = Number(process.env.OPS || 200); // iterations per measurement round

function makeChunks(count, size) {
  const out = [];
  for (let i = 0; i < count; i++) {
    const b = Buffer.allocUnsafe(size);
    b[0] = i & 0xff;
    out.push(b);
  }
  return out;
}

// Times one synchronous burst and returns ns per chunk. Everything yields between
// bursts so nothing here depends on the timing of a microtask or an immediate.
async function bench(name, chunks, perIter, work) {
  let sink = 0;
  const run = (iters) => {
    for (let i = 0; i < iters; i++) {
      sink += work(chunks, sink);
    }
  };
  run(20); // warm up
  const samples = [];
  for (let r = 0; r < ROUNDS; r++) {
    const t0 = process.hrtime.bigint();
    for (let done = 0; done < OPS; done += perIter) {
      run(Math.min(perIter, OPS - done));
    }
    samples.push(Number(process.hrtime.bigint() - t0) / (OPS * chunks.length));
    await new Promise((res) => setImmediate(res));
  }
  samples.sort((a, b) => a - b);
  console.log(`    ${name.padEnd(26)} ${samples[0].toFixed(1).padStart(7)} ns/chunk  `
    + `(median ${samples[(ROUNDS / 2) | 0].toFixed(1)})`);
  if (sink === -1) {
    console.log('unreachable');
  }
}

(async () => {
  for (const [count, size] of [[64, 64], [1024, 64], [16, 16384], [1, 16384]]) {
    const chunks = makeChunks(count, size);
    console.log(`\n== ${count} chunks x ${size}B`);

    await bench('array push + indexed', chunks, 8, (cs) => {
      const a = [];
      let t = 0;
      for (const c of cs) {
        a.push(c);
      }
      for (let i = 0; i < a.length; i++) {
        t += a[i][0];
      }
      return t;
    });

    await bench('set add + forEach', chunks, 8, (cs) => {
      const s = new Set();
      let t = 0;
      for (const c of cs) {
        s.add(c);
      }
      for (const c of s) {
        t += c[0];
      }
      return t;
    });

    await bench('set add + spread to index', chunks, 8, (cs) => {
      const s = new Set();
      let t = 0;
      for (const c of cs) {
        s.add(c);
      }
      const a = [...s];
      for (let i = 0; i < a.length; i++) {
        t += a[i][0];
      }
      return t;
    });

    await bench('array push + Buffer.concat', chunks, 8, (cs) => {
      const a = [];
      for (const c of cs) {
        a.push(c);
      }
      const one = Buffer.concat(a);
      let t = 0;
      for (let i = 0; i < cs.length; i++) {
        t += one[i];
      }
      return t;
    });
  }
  const mem = process.memoryUsage();
  console.log(`\n  rss ${(mem.rss / 1048576).toFixed(0)}MB external ${(mem.external / 1048576).toFixed(0)}MB\n`);
})();
