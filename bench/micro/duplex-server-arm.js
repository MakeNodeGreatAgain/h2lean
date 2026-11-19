// ONE server arm of the stream.Duplex A/B (OPTIMIZATION.md item 41), run as its own
// process so its process.cpuUsage() is pure server cost -- the load generator lives in
// the parent (bench/micro/duplex-server-ab.js).
//
//   PORT=.. ARM=lean|duplex DURATION=.. node bench/micro/duplex-server-arm.js
//
// Both arms do identical work: read the whole upload body, then answer 200. They differ
// ONLY in how the request body is delivered to the handler:
//   lean   -> the h2lean stream's native EventEmitter readable (today's design).
//   duplex -> the same stream wrapped in a stream.Duplex the handler reads off, i.e. the
//             machinery a native `Http2Stream extends Duplex` server stream would carry.
// On exit prints:  CPU=<us-per-req> REQ=<count> BUSY=<pct>
'use strict';

const h2lean = require('../../index');
const { Duplex } = require('node:stream');

const PORT = Number(process.env.PORT);
const ARM = process.env.ARM || 'lean';
const DURATION = Number(process.env.DURATION || 3000);

const HEADERS = Object.freeze({ ':status': 200 });
let req = 0;
let sink = 0; // body octets read; referenced in the exit line so the read is never dead-code-eliminated
const cpu0 = process.cpuUsage();
const t0 = Number(process.hrtime.bigint());

const server = h2lean.createServer();
server.on('stream', (stream) => {
  stream.on('error', () => {});
  if (ARM === 'duplex') {
    const d = new Duplex({
      read() {
        stream.resume();
      },
      write(_c, _e, cb) {
        cb();
      },
      final(cb) {
        cb();
      }
    });
    stream.on('data', (c) => {
      if (!d.push(c)) {
        stream.pause();
      }
    });
    stream.on('end', () => {
      d.push(null);
    });
    stream.on('close', () => d.destroy());
    d.on('drain', () => stream.resume());
    d.on('data', (c) => {
      sink += c.length;
    });
    d.on('end', () => {
      req++; stream.respond(HEADERS, { endStream: true });
    });
  } else {
    stream.on('data', (c) => {
      sink += c.length;
    });
    stream.on('end', () => {
      req++; stream.respond(HEADERS, { endStream: true });
    });
    stream.resume(); // h2lean's EventEmitter stream does not auto-flow on a 'data' listener
  }
});

server.listen(PORT, '127.0.0.1', () => {
  console.log('listening ' + PORT);
  setTimeout(() => {
    const cpu = process.cpuUsage(cpu0);
    const cpuMs = (cpu.user + cpu.system) / 1000; // process.cpuUsage is us -> ms
    const wallMs = (Number(process.hrtime.bigint()) - t0) / 1e6; // ns -> ms
    console.log(`CPU=${(cpuMs * 1000 / (req || 1)).toFixed(3)} REQ=${req} BUSY=${(cpuMs * 100 / wallMs).toFixed(1)} SINK=${sink}`);
    server.close();
    process.exit(0);
  }, DURATION);
});
