// ONE server arm of the NATIVE-Duplex decision matrix (OPTIMIZATION.md item 41),
// run as its own process so process.cpuUsage() is pure server cost -- the load
// generator lives in the parent (bench/micro/duplex-native-ab.js).
//
//   PORT=.. ARM=lean|duplex|node UP=.. DOWN=.. DURATION=.. VARIANT=.. node duplex-native-arm.js
//
// lean   = the shipped EventEmitter stream (this tree)
// duplex = the patched variant the parent built (Http2Stream extends Duplex in
//          node's native shape: push() delivery, _write/_final response)
// node   = node:http2 itself, the paired reference
// All three run the IDENTICAL handler: sink the whole upload body, answer on 'end'.
// On exit prints:  CPU=<us-per-req> REQ=<count> BUSY=<pct> SINK=<octets>
'use strict';

const PORT = Number(process.env.PORT);
const ARM = process.env.ARM || 'lean';
const DOWN = Number(process.env.DOWN || 0);
const DURATION = Number(process.env.DURATION || 5000);

const impl = ARM === 'node' ? require('node:http2')
  : ARM === 'duplex' ? require(process.env.VARIANT)
    : require('../../index');

const HEADERS = Object.freeze({ ':status': 200 });
const body = DOWN > 0 ? Buffer.alloc(DOWN, 0x61) : null;
let req = 0;
let sink = 0;
let cpu0 = process.cpuUsage();
let t0 = Number(process.hrtime.bigint());

const server = impl.createServer();
server.on('stream', (stream) => {
  stream.on('error', () => {});
  stream.on('data', (c) => {
    sink += c.length;
  });
  stream.on('end', () => {
    req++;
    if (body === null) {
      stream.respond(HEADERS, { endStream: true });
    } else {
      stream.respond(HEADERS);
      stream.end(body);
    }
  });
  stream.resume();
});

server.listen(PORT, '127.0.0.1', () => {
  console.log('listening ' + PORT);
  // warmup: JIT + HPACK dynamic table + connection setup leave the window here
  setTimeout(() => {
    req = 0;
    sink = 0;
    cpu0 = process.cpuUsage();
    t0 = Number(process.hrtime.bigint());
  }, 1200);
  setTimeout(() => {
    const cpu = process.cpuUsage(cpu0);
    const cpuMs = (cpu.user + cpu.system) / 1000;
    const wallMs = (Number(process.hrtime.bigint()) - t0) / 1e6;
    console.log(`CPU=${(cpuMs * 1000 / (req || 1)).toFixed(3)} REQ=${req} BUSY=${(cpuMs * 100 / wallMs).toFixed(1)} SINK=${sink}`);
    server.close();
    process.exit(0);
  }, DURATION);
});
