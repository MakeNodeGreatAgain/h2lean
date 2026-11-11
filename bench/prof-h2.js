// Splits node:http2 cost: run server and client in separate processes so the
// CPU profile of the server contains only server-side work.
//   ROLE=server node --cpu-prof --cpu-prof-dir=out bench/prof-h2.js
//   ROLE=client node bench/prof-h2.js
const http2 = require('node:http2');

const PORT = Number(process.env.PORT || 8443);
const CONC = Number(process.env.CONC || 64);
const DURATION_MS = Number(process.env.DURATION || 10000);
const BODY = Number(process.env.BODY || 0);
const ROLE = process.env.ROLE || 'server';

const RESP_BODY = BODY > 0 ? Buffer.alloc(BODY, 0x61) : null;
const RESP_HEADERS = {
  ':status': '200',
  'content-type': 'text/plain',
  server: 'prof',
  'x-trace': '0123456789abcdef0123456789abcdef'
};
if (RESP_BODY) {
  RESP_HEADERS['content-length'] = String(BODY);
}
const FROZEN = Object.freeze(RESP_HEADERS);
// The same answer through the compat API, which is what most applications use.
const API = process.env.API || 'stream';
const COMPAT_HEADERS = Object.freeze({
  'content-type': 'text/plain',
  server: 'prof',
  'x-trace': '0123456789abcdef0123456789abcdef',
  ...(BODY > 0 ? { 'content-length': String(BODY) } : null)
});

const REQ_HEADERS = {
  ':method': 'GET',
  ':path': '/abc/def/ghij?x=1',
  accept: '*/*',
  'user-agent': 'probe/1.0'
};
// VARY=1 puts a unique value in every request, i.e. simulates the common real
// case (x-request-id / trace id / cookie) where no two header blocks are equal.
const VARY = process.env.VARY === '1';
// READBODY=1 makes the stream-API handler consume the request body, so that an
// upload workload really measures upload handling on both servers. See the same
// knob in bench/h2lean-h2.js.
const READBODY = process.env.READBODY === '1';
// TLS=1 serves the same workload over h2s instead of h2c.
const TLS = process.env.TLS === '1';
// UNIQ=1 puts a unique value in every RESPONSE: no two responses carry the same
// header block. The same knob in bench/h2lean-h2.js, so the two arms are measured
// on the same work.
const UNIQ = process.env.UNIQ === '1';
const certificates = require('../test/fixtures-tls');
let seq = 0;

if (ROLE === 'server') {
  let handled = 0;
  const cpu0 = process.cpuUsage();
  const t0 = Number(process.hrtime.bigint());
  // GC=1 reports the collector's share of the run, so that our numbers and
  // node's can be compared on allocation pressure and not just on total CPU.
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
  // TLS=1 puts the same handler behind an h2s listener; h2load then drives it
  // over https and the reported CPU/req includes the record layer.
  const compatHandler = (req, res) => {
    req.on('error', () => {});
    req.resume();
    handled++;
    res.writeHead(200, UNIQ ? { ...COMPAT_HEADERS, 'x-req': String(++seq) } : COMPAT_HEADERS);
    res.end(RESP_BODY);
  };
  const server = TLS
    ? (API === 'compat'
      ? http2.createSecureServer(certificates(), compatHandler)
      : http2.createSecureServer(certificates()))
    : (API === 'compat' ? http2.createServer(compatHandler) : http2.createServer());
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
      console.log(`[node:http2 ${API}] req=${handled} wall=${wall.toFixed(0)}ms cpu=${cpuMs.toFixed(0)}ms ` +
                  `rps=${Math.round(handled / (wall / 1000))} ` +
                  `cpuPerReq=${(cpuMs * 1000 / handled).toFixed(2)}us ` +
                  `busy=${(cpuMs * 100 / wall).toFixed(1)}% ` +
                  `gc=${gcMs.toFixed(0)}ms gcPct=${(gcMs * 100 / wall).toFixed(1)}% gcCount=${gcCount}`);
      process.exit(0);
    }, DURATION_MS);
  });
} else {
  const client = http2.connect(`http://127.0.0.1:${PORT}`);
  client.on('error', (e) => {
    console.error('client err', e); process.exit(1);
  });
  let done = 0;
  let errs = 0;
  let stopped = false;

  function one() {
    if (stopped) {
      return;
    }
    const headers = VARY
      ? { ...REQ_HEADERS, 'x-req': String(++seq) }
      : REQ_HEADERS;
    const req = client.request(headers);
    req.resume();
    req.on('end', () => {
      done++; one();
    });
    req.on('error', () => {
      errs++;
    });
  }

  // warm up for 2s, then start counting
  for (let i = 0; i < CONC; i++) {
    one();
  }
  setTimeout(() => {
    done = 0; errs = 0;
  }, 2000);
  setTimeout(() => {
    stopped = true;
    console.log(`rps=${Math.round(done / (DURATION_MS / 1000) * (DURATION_MS / (DURATION_MS - 2000)))} streams=${CONC} body=${BODY}B ok=${done} err=${errs}`);
    client.close();
    setTimeout(() => process.exit(0), 200);
  }, DURATION_MS);
}
