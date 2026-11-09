// Showcase arm: h2lite, the (req, res) compatibility API -- h2c. The drop-in
// surface: `require('node:http2')` becomes `require('h2lite')` and this same
// handler keeps working. Shaped exactly like srv-node-h2-compat.js.
//
//   PORT=8906 BODY=262144 DURATION=8000 node bench/showcase/srv-h2lite-compat.js
const h2lite = require('../../index');

const PORT = Number(process.env.PORT || 8906);
const BODY = Number(process.env.BODY || 0);
const DURATION = Number(process.env.DURATION || 8000);

const PAYLOAD = BODY > 0 ? Buffer.alloc(BODY, 0x61) : null;
const HEADERS = {
  'content-type': 'text/plain',
  server: 'h2lite-compat',
  'x-trace': '0123456789abcdef0123456789abcdef'
};
if (PAYLOAD) {
  HEADERS['content-length'] = String(BODY);
}
const FROZEN = Object.freeze(HEADERS);

const cpu0 = process.cpuUsage();
let req = 0;
const t0 = Number(process.hrtime.bigint());
const server = h2lite.createServer((incoming, res) => {
  incoming.resume();
  req++;
  res.writeHead(200, FROZEN);
  res.end(PAYLOAD);
});
server.listen(PORT, '127.0.0.1', () => {
  console.log('listening ' + PORT);
  setTimeout(() => {
    const cpu = process.cpuUsage(cpu0);
    const wall = (Number(process.hrtime.bigint()) - t0) / 1e6;
    const cpuMs = (cpu.user + cpu.system) / 1000;
    console.log(`[h2lite compat] cpuPerReq=${(cpuMs * 1000 / (req || 1)).toFixed(2)}us ` +
                `busy=${(cpuMs * 100 / wall).toFixed(1)}% req=${req}`);
    process.exit(0);
  }, DURATION);
});
