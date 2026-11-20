// Port of node's `test/parallel/test-http2-origin-set-max-size.mjs` to the differential
// shape used here (CONTRIBUTING.md §1 — an upstream file is a scenario source, node's
// measured value is the expectation). One server floods ORIGIN frames; each arm connects a
// client from one library with a given `maxOriginSetSize` and records what the application
// sees — the synchronous throw out of `connect()`, the number of 'origin' events, the
// destroy code, and whether a GOAWAY arrived. The two transcripts are then compared, so a
// difference is a bug in one stack rather than an artefact of the test.
//
// The server side is node's own `http2`, exactly as upstream writes it, so the frames the
// client under test parses are the reference implementation's bytes.
//
// Upstream's four groups, all reproduced below: the non-number values that must throw
// `ERR_INVALID_ARG_TYPE` out of `connect()` before a socket exists, `NaN`/`-1` that must
// throw `ERR_OUT_OF_RANGE`, the default (128) that survives twelve frames and fails on the
// thirteenth, the sub-one values (`-0`, `9`, `1.5`) that refuse the very first frame, and
// the above-default values (`512`, `Infinity`) that accept everything the flood can send.
const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const http2 = require('node:http2');
const h2lean = require('../../index');
const { ORIGIN_SET_CAP } = require('../node-version');

// The cap is a node 24+ feature; before that connect() ignores maxOriginSetSize and
// onOrigin adds unconditionally, so the whole scenario has no reference verdict to pin.
const SKIP = ORIGIN_SET_CAP ? false : 'maxOriginSetSize absent before node 24 (test/node-version.js)';

const TLS_KEY = fs.readFileSync(path.join(__dirname, '..', '..', 'out', 'test-key.pem'));
const TLS_CERT = fs.readFileSync(path.join(__dirname, '..', '..', 'out', 'test-cert.pem'));

// 10 fresh origins per frame, every 10 ms, until the session dies — upstream's shape. The
// interval is cleared on session close so no timer outlives the case.
function startFloodServer() {
  return new Promise((resolve) => {
    const server = http2.createSecureServer({ key: TLS_KEY, cert: TLS_CERT });
    server.on('session', (session) => {
      let i = 0;
      const timer = setInterval(() => {
        try {
          session.origin(...Array.from({ length: 10 }, () => `https://o${i++}.example.com`));
        } catch {
          clearInterval(timer);
        }
      }, 10);
      session.on('close', () => clearInterval(timer));
    });
    server.listen(0, 'localhost', () => resolve({ server, port: server.address().port }));
  });
}

// One client arm: what does `connect()` throw (if anything, name + code + full message),
// and what does a live session record while the flood runs? The message is part of the
// transcript on purpose: node builds it from determineSpecificType, and a paraphrase of it
// ('Received type object' for 'Received an instance of Array') is exactly the kind of
// difference an application that matches on error strings would see.
function floodArm(lib, port, maxOriginSetSize, stopAt) {
  return new Promise((resolve) => {
    const options = { ca: TLS_CERT };
    if (maxOriginSetSize !== undefined) {
      options.maxOriginSetSize = maxOriginSetSize;
    }
    let session;
    try {
      session = lib.connect(`https://localhost:${port}`, options);
    } catch (e) {
      resolve({ throws: { name: e.constructor.name, code: e.code, message: e.message } });
      return;
    }
    const out = { throws: null, originEvents: 0, error: undefined, goaway: false };
    session.on('origin', () => {
      out.originEvents++;
      // The above-default group in upstream stops itself once the set passes 128: the
      // flood has no end otherwise. Same stop condition, same reason.
      if (stopAt !== undefined && session.originSet !== undefined
        && session.originSet.length > stopAt) {
        session.destroy();
      }
    });
    session.on('error', (e) => {
      if (out.error === undefined) {
        out.error = e.code;
      }
    });
    session.on('goaway', () => {
      out.goaway = true;
    });
    session.on('close', () => {
      session.destroy();
      resolve(out);
    });
    setTimeout(() => {
      session.destroy(); resolve(out);
    }, 4000);
  });
}

const IMPOSSIBLE = [Symbol('x'), '0', 1n, {}, [], true, false, /s/, () => {}];
const OUT_OF_RANGE = [NaN, -1];
const TOO_SMALL = [-0, 9, 1.5];
const ABOVE_DEFAULT = [512, Infinity];

test('upstream origin-set-max-size: a non-number option throws ERR_INVALID_ARG_TYPE',
  { skip: SKIP }, async () => {
    const { server, port } = await startFloodServer();
    try {
      for (const value of IMPOSSIBLE) {
        const label = String(value);
        const a = await floodArm(http2, port, value);
        const b = await floodArm(h2lean, port, value);
        assert.deepStrictEqual(b.throws, a.throws, `${label}: h2lean must match node`);
        assert.strictEqual(a.throws.code, 'ERR_INVALID_ARG_TYPE',
          `precondition: node throws ERR_INVALID_ARG_TYPE for ${label}`);
      }
    } finally {
      server.close();
    }
  });

test('upstream origin-set-max-size: NaN and -1 throw ERR_OUT_OF_RANGE', { skip: SKIP }, async () => {
  const { server, port } = await startFloodServer();
  try {
    for (const value of OUT_OF_RANGE) {
      const a = await floodArm(http2, port, value);
      const b = await floodArm(h2lean, port, value);
      assert.deepStrictEqual(b.throws, a.throws, `${value}: h2lean must match node`);
      assert.strictEqual(a.throws.code, 'ERR_OUT_OF_RANGE',
        `precondition: node throws ERR_OUT_OF_RANGE for ${value}`);
    }
  } finally {
    server.close();
  }
});

// A throw means no socket was ever made, which is what upstream asserts next with
// `server.getConnections() === 0`. Measured on the live node server here, for both stacks.
test('upstream origin-set-max-size: a rejected option leaves no connection behind',
  { skip: SKIP }, async () => {
    const { server, port } = await startFloodServer();
    try {
      for (const lib of [http2, h2lean]) {
        for (const value of IMPOSSIBLE.concat(OUT_OF_RANGE)) {
          try {
            lib.connect(`https://localhost:${port}`, { ca: TLS_CERT, maxOriginSetSize: value });
          } catch { /* the throw is the case */ }
        }
        const count = await new Promise((resolve, reject) => {
          server.getConnections((e, c) => e ? reject(e) : resolve(c));
        });
        assert.strictEqual(count, 0, 'no connection may survive a rejected option');
      }
    } finally {
      server.close();
    }
  });

test('upstream origin-set-max-size: the default 128 passes 12 frames and fails on the 13th',
  { skip: SKIP }, async () => {
    const { server, port } = await startFloodServer();
    try {
      const a = await floodArm(http2, port, undefined);
      const b = await floodArm(h2lean, port, undefined);
      assert.deepStrictEqual(b, a);
      // Upstream's anchor: twelve 'origin' events (120 origins + the TLS seed = 121, still
      // under 128) and the thirteenth frame's first add is the one that overflows, so the
      // thirteenth event never fires. The destroy is local — no GOAWAY on a client.
      assert.strictEqual(a.originEvents, 12, 'precondition: node counts twelve accepted frames');
      assert.strictEqual(a.error, 'ERR_HTTP2_TOO_MANY_ORIGINS');
      assert.strictEqual(a.goaway, false);
    } finally {
      server.close();
    }
  });

test('upstream origin-set-max-size: -0, 9 and 1.5 refuse the first frame', { skip: SKIP }, async () => {
  const { server, port } = await startFloodServer();
  try {
    for (const value of TOO_SMALL) {
      const a = await floodArm(http2, port, value);
      const b = await floodArm(h2lean, port, value);
      assert.deepStrictEqual(b, a, `${value}: h2lean must match node`);
      assert.strictEqual(a.originEvents, 0,
        'precondition: node never emits \'origin\' when the frame overflows (-0 === 0, ' +
        '1.5 < the seed plus the first origin)');
      assert.strictEqual(a.error, 'ERR_HTTP2_TOO_MANY_ORIGINS');
    }
  } finally {
    server.close();
  }
});

test('upstream origin-set-max-size: 512 and Infinity accept well past the default',
  { skip: SKIP }, async () => {
    const { server, port } = await startFloodServer();
    try {
      for (const value of ABOVE_DEFAULT) {
        const a = await floodArm(http2, port, value, 128);
        const b = await floodArm(h2lean, port, value, 128);
        assert.deepStrictEqual(b, a, `${value}: h2lean must match node`);
        // Upstream stops at thirteen events; the arm destroys itself once originSet
        // passes 128, so the count is the stop condition, not a cap.
        assert.strictEqual(a.originEvents, 13, `precondition: node accepted thirteen frames at ${value}`);
        assert.strictEqual(a.error, undefined);
      }
    } finally {
      server.close();
    }
  });
