// Showcase arm: h2lite, the stream (Http2Stream) API -- h2c. The library's own
// server, shaped exactly like bench/showcase/srv-node-h2-strm.js so the two
// processes differ only in what parses HTTP/2. Default options: this is the
// drop-in number, not a tuned one.
//
//   PORT=8905 BODY=262144 DURATION=8000 node bench/showcase/srv-h2lite-strm.js
const h2lite = require('../../index');

const PORT = Number(process.env.PORT || 8905);
const BODY = Number(process.env.BODY || 0);
const DURATION = Number(process.env.DURATION || 8000);

const PAYLOAD = BODY > 0 ? Buffer.alloc(BODY, 0x61) : null;
const HEADERS = {
  ':status': 200,
  'content-type': 'text/plain',
  server: 'h2lite-stream',
  'x-trace': '0123456789abcdef0123456789abcdef'
};
if (PAYLOAD) {
  HEADERS['content-length'] = String(BODY);
}
const FROZEN = Object.freeze(HEADERS);

const cpu0 = process.cpuUsage();
let req = 0;
const t0 = Number(process.hrtime.bigint());
const server = h2lite.createServer();
server.on('stream', (stream) => {
  stream.on('error', () => {});
  stream.resume();
  req++;
  if (PAYLOAD) {
    stream.respond(FROZEN);
    stream.end(PAYLOAD);
  } else {
    stream.respond(FROZEN, { endStream: true });
  }
});
server.listen(PORT, '127.0.0.1', () => {
  console.log('listening ' + PORT);
  setTimeout(() => {
    const cpu = process.cpuUsage(cpu0);
    const wall = (Number(process.hrtime.bigint()) - t0) / 1e6;
    const cpuMs = (cpu.user + cpu.system) / 1000;
    console.log(`[h2lite stream] cpuPerReq=${(cpuMs * 1000 / (req || 1)).toFixed(2)}us ` +
                `busy=${(cpuMs * 100 / wall).toFixed(1)}% req=${req}`);
    process.exit(0);
  }, DURATION);
});
