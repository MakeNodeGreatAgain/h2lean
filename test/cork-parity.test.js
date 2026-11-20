// What `stream.cork()` / `stream.uncork()` and the writableCorked / writableHighWaterMark
// getters do on the server `stream`-API Http2Stream, measured against a live node:http2
// server in this same run. The same two shapes are then driven through the compat (req,res)
// response, whose cork()/uncork() node delegates onto `this[kStream]`
// (internal/http2/compat.js: `cork(){ this[kStream].cork(); }`), so the response inherits the
// stream's behavior exactly and the delegate is pinned to the same parity/divergence.
//
// node's Http2Stream extends Duplex (internal/http2/core.js:2128), so cork()/uncork() are
// Writable's: cork() buffers write()/end() and defers the DATA to the wire until the cork
// count reaches 0. h2lean's server stream extends `stream.Stream` (lib/stream.js, the base
// chosen on OPTIMIZATION.md item 41), which has no Writable half -- so cork()/uncork() are a
// counting stub: they track writableCorked exactly, but write() sends straight through. The
// per-read socket batch (AGENTS.md section 5) already coalesces a synchronous burst of writes,
// so the common `cork(); write(a); write(b); uncork()` in one turn is effectively parity; what
// differs is the CROSS-TICK deferral, and that is the divergence pinned below.
//
//   * COUNTING is parity: writableCorked starts 0 at the application-visible point (node corks
//     once in its constructor for handle-assignment buffering and is already uncorked by the
//     time the 'stream' handler runs -- measured), rises 1,2 on two corks, falls 1,0 on two
//     uncorks, and floors at 0 on a further uncork. writableHighWaterMark is node's default
//     Writable one (core.js sets no highWaterMark on the stream), read off node itself at
//     module load, so it agrees on every major (16384 on 20, 65536 from 22 on).
//   * DEFERRAL is the KNOWN DIVERGENCE: while corked, node's peer receives NOTHING until the
//     count hits 0 (write('AAA') leaves writableLength 3, the client sees it only after the
//     second uncork); h2lean's peer receives 'AAA' immediately, while still corked. Both
//     deliver the same final body 'AAABBB'.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const nodeHttp2 = require('node:http2');
const h2lean = require('../index');
const { closeServer } = require('./diff-harness');

// The fixed client for every shape is node's own, so the only variable is the server arm.
// The server corks twice, writes 'AAA', then uncorks in two steps spread across ticks,
// snapshotting the cork count and what the client has received at each step. clientGot is
// shared (client and server run in this one process), so the snapshots read the peer's view.
function driveCork(impl) {
  return new Promise((resolve) => {
    let clientGot = '';
    let done = false;
    let client;
    const srv = impl.createServer();
    srv.on('stream', (stream) => {
      stream.on('error', () => {});
      stream.respond({ ':status': 200 });
      const initialCorked = stream.writableCorked;
      const hwm = stream.writableHighWaterMark;
      stream.cork();
      stream.cork();
      const corkedAfter2 = stream.writableCorked;
      stream.write('AAA');
      const timeline = [];
      const snap = (step) => timeline.push({ step, corked: stream.writableCorked, got: clientGot });
      setTimeout(() => {
        snap('corked2'); // both corks held, 'AAA' written
        stream.uncork(); // 2 -> 1
        setTimeout(() => {
          snap('corked1');
          stream.uncork(); // 1 -> 0, node flushes here
          setTimeout(() => {
            snap('corked0');
            stream.uncork(); // floors at 0
            const floored = stream.writableCorked;
            stream.end('BBB');
            setTimeout(() => {
              if (done) {
                return;
              }
              done = true;
              resolve({ initialCorked, hwm, corkedAfter2, floored, timeline, finalGot: clientGot });
              client.destroy(); closeServer(srv).then(() => {});
            }, 80);
          }, 80);
        }, 80);
      }, 80);
    });
    srv.listen(0, '127.0.0.1', () => {
      client = nodeHttp2.connect(`http://127.0.0.1:${srv.address().port}`);
      client.on('error', () => {});
      const r = client.request({ ':path': '/' });
      r.setEncoding('utf8');
      r.on('data', (d) => {
        clientGot += d;
      });
      r.on('error', () => {});
      r.end();
    });
    setTimeout(() => {
      if (!done) {
        done = true; resolve({ note: 'deadline' });
      }
    }, 1500);
  });
}

// The compat (req,res) response is a different object from the low-level stream: node's
// Http2ServerResponse delegates corking onto this[kStream] (internal/http2/compat.js). Same
// drive as above, but through the (req,res) API, so the compat delegate is covered too.
function driveCorkCompat(impl) {
  return new Promise((resolve) => {
    let clientGot = '';
    let done = false;
    let client;
    const srv = impl.createServer((req, res) => {
      req.resume();
      const initialCorked = res.writableCorked;
      const hwm = res.writableHighWaterMark;
      res.cork();
      res.cork();
      const corkedAfter2 = res.writableCorked;
      res.write('AAA');
      const timeline = [];
      const snap = (step) => timeline.push({ step, corked: res.writableCorked, got: clientGot });
      setTimeout(() => {
        snap('corked2');
        res.uncork();
        setTimeout(() => {
          snap('corked1');
          res.uncork();
          setTimeout(() => {
            snap('corked0');
            res.uncork();
            const floored = res.writableCorked;
            res.end('BBB');
            setTimeout(() => {
              if (done) {
                return;
              }
              done = true;
              resolve({ initialCorked, hwm, corkedAfter2, floored, timeline, finalGot: clientGot });
              client.destroy(); closeServer(srv).then(() => {});
            }, 80);
          }, 80);
        }, 80);
      }, 80);
    });
    srv.listen(0, '127.0.0.1', () => {
      client = nodeHttp2.connect(`http://127.0.0.1:${srv.address().port}`);
      client.on('error', () => {});
      const r = client.request({ ':path': '/' });
      r.setEncoding('utf8');
      r.on('data', (d) => {
        clientGot += d;
      });
      r.on('error', () => {});
      r.end();
    });
    setTimeout(() => {
      if (!done) {
        done = true; resolve({ note: 'deadline' });
      }
    }, 1500);
  });
}

test('cork/uncork counting and writableHighWaterMark are parity on both stacks', async () => {
  const node = await driveCork(nodeHttp2);
  assert.strictEqual(node.initialCorked, 0, `node initial: ${JSON.stringify(node)}`);
  assert.strictEqual(node.corkedAfter2, 2, `node after two corks: ${JSON.stringify(node)}`);
  assert.strictEqual(node.floored, 0, `node floors at 0: ${JSON.stringify(node)}`);
  const ours = await driveCork(h2lean);
  assert.strictEqual(ours.initialCorked, node.initialCorked, `h2lean initial: ${JSON.stringify(ours)}`);
  assert.strictEqual(ours.corkedAfter2, node.corkedAfter2, `h2lean after two corks: ${JSON.stringify(ours)}`);
  assert.strictEqual(ours.floored, node.floored, `h2lean floors at 0: ${JSON.stringify(ours)}`);
  // The count at each step agrees; only the bytes received (test below) differ.
  assert.deepStrictEqual(ours.timeline.map((t) => t.corked), node.timeline.map((t) => t.corked),
    `h2lean cork timeline: ${JSON.stringify(ours)}`);
  // node sets no highWaterMark on Http2Stream, so this is the default Writable one -- equal
  // on every major because lib/stream.js reads it off node itself rather than hardcoding it.
  assert.strictEqual(ours.hwm, node.hwm, `h2lean hwm: ${JSON.stringify(ours)}`);
});

test('KNOWN DIVERGENCE: node defers DATA until uncork reaches 0, h2lean writes through', async () => {
  const node = await driveCork(nodeHttp2);
  // node buffers 'AAA' in the Writable state while corked; the peer sees nothing until 0.
  assert.strictEqual(node.timeline[0].got, '', `node corked=2: ${JSON.stringify(node)}`);
  assert.strictEqual(node.timeline[1].got, '', `node corked=1: ${JSON.stringify(node)}`);
  assert.strictEqual(node.timeline[2].got, 'AAA', `node corked=0: ${JSON.stringify(node)}`);
  const ours = await driveCork(h2lean);
  // h2lean's cork() is a counting stub: write() already put 'AAA' on the wire while corked.
  // The cross-tick deferral node's Writable performs is the gap (lib/stream.js header).
  assert.strictEqual(ours.timeline[0].got, 'AAA', `h2lean corked=2: ${JSON.stringify(ours)}`);
  assert.strictEqual(ours.timeline[1].got, 'AAA', `h2lean corked=1: ${JSON.stringify(ours)}`);
  // The final body is the same on both stacks -- only the timing of the first chunk differs.
  assert.strictEqual(ours.finalGot, node.finalGot, `h2lean final: ${JSON.stringify(ours)}`);
  assert.strictEqual(node.finalGot, 'AAABBB', `node final: ${JSON.stringify(node)}`);
});

test('compat res: cork/uncork counting and writableHighWaterMark delegate to parity', async () => {
  const node = await driveCorkCompat(nodeHttp2);
  assert.strictEqual(node.initialCorked, 0, `node initial: ${JSON.stringify(node)}`);
  assert.strictEqual(node.corkedAfter2, 2, `node after two corks: ${JSON.stringify(node)}`);
  assert.strictEqual(node.floored, 0, `node floors at 0: ${JSON.stringify(node)}`);
  const ours = await driveCorkCompat(h2lean);
  // res.cork()/uncork()/writableCorked read/write the wrapped stream's Writable side (node's
  // compat delegate), so the count matches node exactly.
  assert.strictEqual(ours.initialCorked, node.initialCorked, `h2lean initial: ${JSON.stringify(ours)}`);
  assert.strictEqual(ours.corkedAfter2, node.corkedAfter2, `h2lean after two corks: ${JSON.stringify(ours)}`);
  assert.strictEqual(ours.floored, node.floored, `h2lean floors at 0: ${JSON.stringify(ours)}`);
  assert.deepStrictEqual(ours.timeline.map((t) => t.corked), node.timeline.map((t) => t.corked),
    `h2lean cork timeline: ${JSON.stringify(ours)}`);
  assert.strictEqual(ours.hwm, node.hwm, `h2lean hwm: ${JSON.stringify(ours)}`);
});

test('KNOWN DIVERGENCE: compat res defers DATA until uncork reaches 0 on node, writes through here', async () => {
  const node = await driveCorkCompat(nodeHttp2);
  assert.strictEqual(node.timeline[0].got, '', `node corked=2: ${JSON.stringify(node)}`);
  assert.strictEqual(node.timeline[1].got, '', `node corked=1: ${JSON.stringify(node)}`);
  assert.strictEqual(node.timeline[2].got, 'AAA', `node corked=0: ${JSON.stringify(node)}`);
  const ours = await driveCorkCompat(h2lean);
  // The compat delegate inherits the stream's counting-stub behavior: 'AAA' is already out
  // while the response is still corked (lib/compat.js header).
  assert.strictEqual(ours.timeline[0].got, 'AAA', `h2lean corked=2: ${JSON.stringify(ours)}`);
  assert.strictEqual(ours.finalGot, node.finalGot, `h2lean final: ${JSON.stringify(ours)}`);
  assert.strictEqual(node.finalGot, 'AAABBB', `node final: ${JSON.stringify(node)}`);
});
