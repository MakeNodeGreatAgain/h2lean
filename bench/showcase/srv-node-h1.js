// Showcase arm: node:http (HTTP/1.1). Self-contained responder for the
// advertising benchmark in this folder; the workload shape is identical across
// all six arms (same headers, same body size), so the only thing that varies is
// the stack that terminates the protocol.
//
//   PORT=8901 BODY=16384 DURATION=8000 node bench/showcase/srv-node-h1.js
//
// It prints `listening <port>` for the runner to sync on, then after DURATION ms
// prints one machine-readable line with the server-side cost per request and exits.
const http = require('node:http');

const PORT = Number(process.env.PORT || 8901);
const BODY = Number(process.env.BODY || 0);
const DURATION = Number(process.env.DURATION || 8000);

const PAYLOAD = BODY > 0 ? Buffer.alloc(BODY, 0x61) : null;
const HEADERS = {
  'content-type': 'text/plain',
  server: 'node-http1',
  'x-trace': '0123456789abcdef0123456789abcdef'
};
if (PAYLOAD) {
  HEADERS['content-length'] = String(BODY);
}
const FROZEN = Object.freeze(HEADERS);

const cpu0 = process.cpuUsage();
let req = 0;
const t0 = Number(process.hrtime.bigint());
const server = http.createServer((incoming, res) => {
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
    console.log(`[node:http] cpuPerReq=${(cpuMs * 1000 / (req || 1)).toFixed(2)}us ` +
                `busy=${(cpuMs * 100 / wall).toFixed(1)}% req=${req}`);
    process.exit(0);
  }, DURATION);
});
