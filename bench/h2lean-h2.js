// Server role for the benchmark: the real h2lean server, handler-shaped exactly
// like bench/prof-h2.js so the two processes differ only in what parses HTTP/2.
//   ROLE=server node bench/h2lean-h2.js
const h2lean = require('../index');

const PORT = Number(process.env.PORT || 8443);
const DURATION_MS = Number(process.env.DURATION || 10000);
const BODY = Number(process.env.BODY || 0);
// A/B knob for the request shape cache, used when measuring whether it pays.
const SHAPE = process.env.SHAPE !== '0';
// A/B knob for shareHeaders: the frozen, per-shape shared request headers object.
const SHARE = process.env.SHARE === '1';
// READBODY=1 makes the stream-API handler consume the request body. Without it an
// upload workload is not measuring upload handling at all: node answers and resets
// the unread half of the stream, so the two servers are compared on different work.
const READBODY = process.env.READBODY === '1';
// TLS=1 serves the same workload over h2s instead of h2c.
const TLS = process.env.TLS === '1';
// UNIQ=1 puts a unique value in every RESPONSE, which is what a server that
// echoes a request id or a trace id does: no two responses carry the same header
// block, so nothing about the response headers can be reused. The knob exists
// because the header path is the one the compat API pays per request, and the
// interesting case is the one where the headers never repeat.
const UNIQ = process.env.UNIQ === '1';
let seq = 0;
const certificates = require('../test/fixtures-tls');

const RESP_HEADERS = {
  ':status': '200',
  'content-type': 'text/plain',
  server: 'prof',
  'x-trace': '0123456789abcdef0123456789abcdef'
};
if (BODY > 0) {
  RESP_HEADERS['content-length'] = String(BODY);
}
const FROZEN = Object.freeze(RESP_HEADERS);
const RESP_BODY = BODY > 0 ? Buffer.alloc(BODY, 0x61) : null;
// The same answer through the compat API, which is what most applications use.
const API = process.env.API || 'stream';
const COMPAT_HEADERS = Object.freeze({
  'content-type': 'text/plain',
  server: 'prof',
  'x-trace': '0123456789abcdef0123456789abcdef',
  ...(BODY > 0 ? { 'content-length': String(BODY) } : null)
});

const cpu0 = process.cpuUsage();
const t0 = Number(process.hrtime.bigint());
let handled = 0;

// GC=1 reports the collector's share of the run. An allocation-rate regression
// shows up there long before it is visible in cpuPerReq, and the two numbers
// together are what separates "the code got cheaper" from "the GC got quieter".
let gcCount = 0;
let gcMs = 0;
if (process.env.GC === '1') {
  const { PerformanceObserver } = require('node:perf_hooks');
  new PerformanceObserver((list) => {
    for (const e of list.getEntries()) {
      gcCount++; gcMs += e.duration;
    }
  }).observe({ entryTypes: ['gc'] });
}

const OPTIONS = { shapeCache: SHAPE, shareHeaders: SHARE };
if (TLS) {
  Object.assign(OPTIONS, certificates());
}
const compatHandler = (req, res) => {
  req.on('error', () => {});
  req.resume();
  handled++;
  res.writeHead(200, UNIQ ? { ...COMPAT_HEADERS, 'x-req': String(++seq) } : COMPAT_HEADERS);
  res.end(RESP_BODY);
};

const make = TLS ? h2lean.createSecureServer : h2lean.createServer;
const server = API === 'compat' ? make(OPTIONS, compatHandler) : make(OPTIONS);
// OUTSTAT=1 counts writes, their sizes, and everything the outbound path copies,
// on this real fan-out rather than on the four streams tools/probe-outbound.js uses.
const outstat = process.env.OUTSTAT === '1' ? require('./outstat') : null;
if (outstat) {
  outstat.install();
}
if (API !== 'compat') {
  server.on('stream', (stream) => {
    stream.on('error', () => {});
    if (READBODY) {
      stream.resume();
    }
    handled++;
    const headers = UNIQ ? { ...RESP_HEADERS, 'x-req': String(++seq) } : FROZEN;
    if (RESP_BODY) {
      stream.respond(headers);
      stream.end(RESP_BODY);
    } else {
      stream.respond(headers, { endStream: true });
    }
  });
}
server.listen(PORT, '127.0.0.1', () => {
  console.log('server listening', PORT);
  setTimeout(() => {
    const cpu = process.cpuUsage(cpu0);
    const wall = (Number(process.hrtime.bigint()) - t0) / 1e6;
    const cpuMs = (cpu.user + cpu.system) / 1000;
    console.log(`[h2lean ${API}${SHARE ? ' share' : ''}] req=${handled} wall=${wall.toFixed(0)}ms ` +
                `cpu=${cpuMs.toFixed(0)}ms ` +
                `rps=${Math.round(handled / (wall / 1000))} ` +
                `cpuPerReq=${(cpuMs * 1000 / handled).toFixed(2)}us ` +
                `busy=${(cpuMs * 100 / wall).toFixed(1)}% ` +
                `gc=${gcMs.toFixed(0)}ms gcPct=${(gcMs * 100 / wall).toFixed(1)}% gcCount=${gcCount}`);
    if (outstat) {
      outstat.report(handled);
    }
    process.exit(0);
  }, DURATION_MS);
});
