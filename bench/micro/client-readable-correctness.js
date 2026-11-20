// Correctness differential for the hand-rolled-client-readable idea: run the SAME four
// consumption scripts against node's Duplex (what ClientHttp2Stream is) and against a bare
// EventEmitter that emits 'data' synchronously (what the server stream is, lib/stream.js).
// Prints what each delivers and where the hand-rolled one breaks. This is the surface a
// hand-rolled client would have to rebuild -- the cost side of client-readable-cost.js.
//
//   node bench/micro/client-readable-correctness.js
//
// Not a shipped-behaviour test: it probes a hypothetical hand-rolled client readable, so it
// lives in bench/micro (out-of-band), not in test/. The `hand+Stream` row is option A applied
// to the client (extends Stream): it buys legacy pipe() but still no read()/async-iterator.
'use strict';

const { EventEmitter } = require('node:events');
const { Duplex, Stream, Writable } = require('node:stream');

const CHUNKS = 8;
const CSZ = 2048;
const body = Buffer.alloc(CSZ, 0x61);
const EXPECT = CHUNKS * CSZ;

function clientDuplex() {
  return new Duplex({
    allowHalfOpen: true, decodeStrings: false,
    read() {}, write(_c, _e, cb) {
      cb();
    }, final(cb) {
      cb();
    }
  });
}

// The server's design: a bare EventEmitter, synchronous 'data'/'end', no Readable machinery.
class HandEE extends EventEmitter {}
// Option A applied to the client: legacy Stream base adds pipe(), nothing else.
class HandStream extends Stream {}

function feed(src) {
  for (let k = 0; k < CHUNKS; k++) {
    if (typeof src.push === 'function') {
      src.push(body);
    } else {
      src.emit('data', body);
    }
  }
  if (typeof src.push === 'function') {
    src.push(null);
  } else {
    src.emit('end');
  }
}

function settle(p, ms) {
  return Promise.race([p, new Promise((r) => setTimeout(() => r({ timeout: true }), ms))]);
}

// Each consumer returns {bytes, ok, error}. `make` builds the source; feed() drives it.
const MODES = {
  data(make) {
    return settle(new Promise((resolve) => {
      const src = make();
      let bytes = 0;
      src.on('data', (c) => {
        bytes += c.length;
      });
      src.on('end', () => resolve({ bytes, ok: bytes === EXPECT }));
      src.on('error', (e) => resolve({ bytes, ok: false, error: e.code || e.message }));
      feed(src);
    }), 300);
  },
  read(make) {
    return settle(new Promise((resolve) => {
      const src = make();
      let bytes = 0;
      try {
        src.on('readable', () => {
          let c;
          while ((c = src.read()) !== null) {
            bytes += c.length;
          }
        });
        src.on('end', () => resolve({ bytes, ok: bytes === EXPECT }));
        feed(src);
      } catch (e) {
        resolve({ bytes, ok: false, error: e.code || e.message });
      }
    }), 300);
  },
  forawait(make) {
    return settle((async () => {
      const src = make();
      let bytes = 0;
      try {
        feed(src);
        for await (const c of src) {
          bytes += c.length;
        }
        return { bytes, ok: bytes === EXPECT };
      } catch (e) {
        return { bytes, ok: false, error: e.code || e.message };
      }
    })(), 300);
  },
  pipe(make) {
    return settle(new Promise((resolve) => {
      const src = make();
      let bytes = 0;
      try {
        const out = new Writable({ write(c, _e, cb) {
          bytes += c.length; cb();
        } });
        out.on('finish', () => resolve({ bytes, ok: bytes === EXPECT }));
        out.on('error', (e) => resolve({ bytes, ok: false, error: e.code || e.message }));
        src.pipe(out);
        feed(src);
      } catch (e) {
        resolve({ bytes, ok: false, error: e.code || e.message });
      }
    }), 300);
  }
};

const SOURCES = {
  'node-Duplex': clientDuplex,
  'hand-EE': () => new HandEE(),
  'hand+Stream': () => new HandStream()
};

(async () => {
  console.log(`expect ${EXPECT} bytes (${CHUNKS}x${CSZ}) per consumer`);
  for (const [mode, run] of Object.entries(MODES)) {
    for (const [name, make] of Object.entries(SOURCES)) {
      const r = await run(make);
      const verdict = r.timeout ? 'TIMEOUT' : (r.ok ? 'ok' : `FAIL ${r.bytes}B ${r.error || ''}`);
      console.log(`  ${mode.padEnd(9)} ${name.padEnd(12)} -> ${verdict}`);
    }
  }
  process.exit(0);
})();
