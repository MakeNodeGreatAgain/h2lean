// Replays the traffic h2spec's http2/5.1.2 check generates -- a flood of HEADERS frames with
// increasing odd stream ids on ONE connection, from a peer that never sends a WINDOW_UPDATE --
// against node:http2 and against h2lean, and reports how much each stack retains per stream.
//
//   node tools/probe-stream-concurrency.js                 -> both stacks, 150k streams each
//   N=400000 MACHINE=h2lean node tools/probe-stream-concurrency.js  -> one stack, more streams
//   node --expose-gc tools/probe-stream-concurrency.js     -> collect before the final number
//
// Why a replay and not the tool: h2spec decides how many streams to send from the
// SETTINGS_MAX_CONCURRENT_STREAMS the server advertises, and sends that many plus one. h2lean
// advertises 4294967295, so the tool loops for hours; node advertises nothing, so the tool skips
// the check and says nothing about what happens under the traffic. The frames below are the ones a
// recorder counted off a real run, which makes the comparison measurable on both stacks.
//
// The peer reads and discards the response bytes, so the only thing that can hold a response back
// is HTTP/2 flow control: the connection send window starts at 65535 and no WINDOW_UPDATE ever
// arrives. Each stream therefore costs the server a live stream plus whatever its unsendable body
// is parked in -- that is the quantity reported, in bytes per stream, on both stacks.
'use strict';
const net = require('node:net');
const h2lean = require('../index');
const nodeHttp2 = require('node:http2');
const f = require('../lib/frames');
const { listen, closeServer } = require('../test/diff-harness');

const N = parseInt(process.env.N, 10) || 150000;
const IMPLS = process.env.MACHINE
  ? [[process.env.MACHINE, process.env.MACHINE === 'node' ? nodeHttp2 : h2lean]]
  : [['node', nodeHttp2], ['h2lean', h2lean]];

// :method GET, :path /, :scheme http from the static table, then :authority as a literal with an
// indexed name -- the same 16-octet block the requests h2spec sends carry.
const BLOCK = Buffer.from([0x82, 0x84, 0x86, 0x01, 9, ...Buffer.from('localhost', 'latin1')]);

function frame(type, flags, sid, payload) {
  const b = Buffer.alloc(f.HEADER_LEN + payload.length);
  f.writeHeader(b, 0, payload.length, type, flags, sid);
  payload.copy(b, f.HEADER_LEN);
  return b;
}

const EMPTY_SETTINGS = frame(f.SETTINGS, 0, 0, Buffer.alloc(0));

// One frame per stream id, allocated fresh: a socket reads a written buffer at flush time, so
// reusing one buffer across queued writes would give every stream the last id written into it.
function nextFrame(sid) {
  const b = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(b, 0, BLOCK.length, f.HEADERS, f.F_END_STREAM | f.F_END_HEADERS, sid);
  return Buffer.concat([b, BLOCK]);
}

// Never faster than the socket will take: an unbounded write loop would measure this process's own
// transmit buffers instead of what the server holds. `until` is exclusive.
function feed(sock, until) {
  let sid = 1;
  const pending = [];
  let room = true;
  return new Promise((resolve) => {
    let done = false;
    const finish = () => {
      if (!done) {
        done = true; resolve();
      }
    };
    sock.once('close', finish); // a refusal ends the flood early, and that is the answer
    const tick = () => {
      if (done || sock.destroyed) {
        return finish();
      }
      while (sid < until && pending.length < 2048) {
        pending.push(nextFrame(sid)); sid += 2;
      }
      while (pending.length && room) {
        room = sock.write(pending.shift());
      }
      if (sid >= until && !pending.length) {
        return finish();
      }
      if (room) {
        setImmediate(tick);
      } else {
        sock.once('drain', () => {
          room = true; tick();
        });
      }
    };
    tick();
  });
}

function serve(impl, counters) {
  const opts = process.env.MAXMEM ? { maxSessionMemory: parseInt(process.env.MAXMEM, 10) } : {};
  const srv = impl.createServer(opts);
  srv.on('stream', (stream) => {
    stream.on('error', () => {});
    counters.opened++;
    counters.live++;
    if (counters.live > counters.peak) {
      counters.peak = counters.live;
    }
    stream.on('close', () => {
      counters.live--;
    });
    stream.respond({ ':status': 200, 'content-type': 'text/plain' });
    stream.end('h2spec target\n');
  });
  srv.on('error', () => {});
  return listen(srv).then((port) => ({ srv, port }));
}

const mb = (b) => (b / 1048576).toFixed(0);

const FRAME_NAMES = { [f.DATA]: 'DATA', [f.HEADERS]: 'HEADERS', [f.PRIORITY]: 'PRIORITY',
  [f.RST_STREAM]: 'RST', [f.SETTINGS]: 'SETTINGS', [f.PING]: 'PING', [f.GOAWAY]: 'GOAWAY',
  [f.WINDOW_UPDATE]: 'WINUP', [f.CONTINUATION]: 'CONT' };

// What the server actually answers the flood with, read off the wire: a GOAWAY or a burst of
// RST_STREAMs is the refusal, and which one it is decides what parity would require here.
function peerLog() {
  const log = { frames: {}, goaway: [], rst: {}, bytes: 0, rest: Buffer.alloc(0) };
  log.feed = (chunk) => {
    log.bytes += chunk.length;
    const buf = log.rest.length ? Buffer.concat([log.rest, chunk]) : chunk;
    let off = 0;
    while (off + f.HEADER_LEN <= buf.length) {
      f.readHeader(buf, off);
      const len = f.FH_LEN, type = f.FH_TYPE;
      if (off + f.HEADER_LEN + len > buf.length) {
        break;
      }
      const name = FRAME_NAMES[type] || `T${type}`;
      log.frames[name] = (log.frames[name] || 0) + 1;
      if (type === f.GOAWAY) {
        log.goaway.push({ last: buf.readUInt32BE(off + f.HEADER_LEN) & 0x7fffffff,
          code: buf.readUInt32BE(off + f.HEADER_LEN + 4) });
      } else if (type === f.RST_STREAM) {
        const c = buf.readUInt32BE(off + f.HEADER_LEN);
        log.rst[c] = (log.rst[c] || 0) + 1;
      }
      off += f.HEADER_LEN + len;
    }
    log.rest = buf.subarray(off);
  };
  return log;
}

const CODE_NAMES = { 0: 'NO_ERROR', 1: 'PROTOCOL_ERROR', 2: 'INTERNAL_ERROR',
  3: 'FLOW_CONTROL_ERROR', 5: 'STREAM_CLOSED', 6: 'FRAME_SIZE_ERROR', 7: 'REFUSED_STREAM',
  8: 'CANCEL', 11: 'ENHANCE_YOUR_CALM' };

async function arm(name, impl) {
  const counters = { opened: 0, live: 0, peak: 0 };
  const { srv, port } = await serve(impl, counters);
  const sock = net.connect(port, '127.0.0.1');
  await new Promise((res) => sock.once('connect', res));
  sock.setNoDelay(true);
  sock.on('error', (e) => {
    counters.sockError = e.code;
  }); // a refusal arrives as a reset
  const peer = peerLog();
  sock.on('data', peer.feed);
  sock.write(f.PREFACE);
  sock.write(EMPTY_SETTINGS);
  await new Promise((res) => setTimeout(res, 100)); // let the settings exchange finish
  const start = process.memoryUsage().heapUsed;
  const sampler = setInterval(() => {
    const mem = process.memoryUsage();
    console.log(`  ${name}: opened ${counters.opened} live ${counters.live} peak ${counters.peak}`
      + ` | heap ${mb(mem.heapUsed)} MB (+${mb(mem.heapUsed - start)}) rss ${mb(mem.rss)} MB`
      + ` | ${((mem.heapUsed - start) / Math.max(1, counters.opened)).toFixed(0)} B/stream`
      + ` | peer read ${mb(peer.bytes)} MB${counters.sockError ? ` [${counters.sockError}]` : ''}`);
  }, 500);
  await feed(sock, 1 + 2 * N);
  // Idle for a second with the connection still open: what is still held after the flood stopped is
  // the retention, while the flood itself is bytes in transit.
  await new Promise((res) => setTimeout(res, 1000));
  clearInterval(sampler);
  if (global.gc) {
    global.gc();
  }
  const m = process.memoryUsage();
  console.log(`${name}: ${counters.opened} streams opened, ${counters.live} still live,`
    + ` peak ${counters.peak} | heap +${mb(m.heapUsed - start)} MB =`
    + ` ${((m.heapUsed - start) / Math.max(1, counters.opened)).toFixed(0)} B/stream`
    + ` | peer read ${mb(peer.bytes)} MB ${JSON.stringify(peer.frames)}`);
  console.log(`${name}: answers -> GOAWAY ${peer.goaway.map((g) => `${CODE_NAMES[g.code] || g.code}`
    + ` last=${g.last}`).join(', ') || 'none'}`
    + ` | RST ${Object.entries(peer.rst).map(([c, n]) => `${CODE_NAMES[c] || c}x${n}`).join(', ') || 'none'}`
    + ` | socket ${counters.sockError || 'open'}`);
  sock.destroy();
  await closeServer(srv);
}

(async () => {
  for (const [name, impl] of IMPLS) {
    await arm(name, impl);
  }
})().catch((err) => {
  console.error(err); process.exit(1);
});
