// Showcase arm: uWebSockets.js (HTTP/1.1). Included as the fastest widely-cited
// Node-adjacent HTTP server so the comparison is not drawn only against node.
// Self-contained responder; workload shape matches the other five arms.
//
//   PORT=8902 BODY=16384 DURATION=8000 node bench/showcase/srv-uws-h1.js
//
// Note for HTTP/1.1 under h2load: uWS adds its own `Content-Length` from the body
// passed to end(), so the handler must NOT write one -- a manual header would
// duplicate it and h2load's strict parser rejects a doubled Content-Length
// (HPE_UNEXPECTED_CONTENT_LENGTH), which reads as 0 succeeded.
const uWS = require('uWebSockets.js');

const PORT = Number(process.env.PORT || 8902);
const BODY = Number(process.env.BODY || 0);
const DURATION = Number(process.env.DURATION || 8000);

const PAYLOAD = BODY > 0 ? Buffer.alloc(BODY, 0x61) : null;

const cpu0 = process.cpuUsage();
let req = 0;
const t0 = Number(process.hrtime.bigint());
uWS.App().get('/*', (res) => {
  req++;
  res.cork(() => {
    res.writeStatus('200 OK')
      .writeHeader('content-type', 'text/plain')
      .writeHeader('server', 'uwebsockets')
      .writeHeader('x-trace', '0123456789abcdef0123456789abcdef');
    // uWS computes Content-Length itself from the end() body, so none is written
    // by hand. The empty case MUST call end() with no argument -- end(null) is
    // rejected ("Text and data can only be passed by String, ArrayBuffer ...").
    if (PAYLOAD) {
      res.end(PAYLOAD);
    } else {
      res.end();
    }
  });
}).listen(PORT, () => {
  console.log('listening ' + PORT);
  setTimeout(() => {
    const cpu = process.cpuUsage(cpu0);
    const wall = (Number(process.hrtime.bigint()) - t0) / 1e6;
    const cpuMs = (cpu.user + cpu.system) / 1000;
    console.log(`[uWebSockets] cpuPerReq=${(cpuMs * 1000 / (req || 1)).toFixed(2)}us ` +
                `busy=${(cpuMs * 100 / wall).toFixed(1)}% req=${req}`);
    process.exit(0);
  }, DURATION);
});
