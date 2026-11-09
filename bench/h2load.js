// Saturate the server with h2load (nghttp2's C client) instead of JS clients.
// The JS harness measures CPU per request, which is the fair metric, but it
// cannot push more than its own client code can generate -- with 8 node clients
// the server still had 45% of a core idle. h2load removes that ceiling, so the
// req/s number here is close to what the server can actually do, and the
// CPU/req number is measured under real saturation.
//
//   node bench/h2load.js --server=h2lite --api=stream --clients=64 --threads=8 --duration=10
//   node bench/h2load.js --server=native --api=compat --clients=64 --threads=8 --duration=10
//   node bench/h2load.js --server=h2lite --body=4096 --duration=10   # POST uploads
//   node bench/h2load.js --server=h2lite --tls --duration=10         # h2s + records
//   node bench/h2load.js --server=h2lite --resp=262144 --winbits=18  # 256 KB window
//
// PRELOAD=<file> starts the server process with `node -r <file>`, which is how a
// measurement arm is switched without touching the server script or the library:
//   PRELOAD=bench/no-table.js node bench/h2load.js --server=h2lite --api=stream
//
// --winbits=N sets the client's advertised flow-control window to (2**N)-1 at both
// the stream and the connection level (h2load -w/-W, default 16 = 65 535). It
// bounds how much DATA the server may put on the wire before it has to wait for a
// WINDOW_UPDATE, so it is the knob that decides how many socket.write() calls one
// large response costs. Server-side settings.initialWindowSize does NOT do this:
// that credits the direction the peer sends in.
//
// With --tls the same server is started behind createSecureServer and h2load
// connects over https. h2load does not verify the peer certificate at all (it has
// no option for it), so the self-signed test pair works as is; the CPU/req figure
// includes encryption of the records but the handshakes are paid once per client,
// because the connections stay open for the whole run.
//
// Caveat: h2load's threads compete with the server for cores. Keep --threads
// well below the core count, or run the client on another machine.
const { spawn, execFileSync } = require('node:child_process');
const fs = require('node:fs');

const opt = Object.fromEntries(process.argv.slice(2).map((a) => {
  const m = /^--([^=]+)=(.*)$/.exec(a);
  return m ? [m[1], m[2]] : [a.slice(2), true];
}));

const kind = opt.server || 'h2lite';
const api = opt.api || 'stream';
const clients = Number(opt.clients || 64);
const threads = Number(opt.threads || 8);
const streams = Number(opt.conc || 32);
const duration = Number(opt.duration || 10);
const body = Number(opt.body || 0); // request body: h2load uploads it with -d
const resp = Number(opt.resp || 0); // response body: the download workload
const winbits = Number(opt.winbits || 0); // client advertised window, 0 = h2load default
const port = Number(opt.port || 8470);
const tlsMode = opt.tls === true || opt.tls === '1';

const SRV_FILES = { native: 'bench/prof-h2.js', h2lite: 'bench/h2lite-h2.js' };

const env = {
  ...process.env,
  ROLE: 'server',
  PORT: String(port),
  DURATION: String(duration * 1000 + 2500),
  BODY: String(resp), // response size, 0 = h2load measures requests only
  API: api,
  SHARE: opt.share ? '1' : '0',
  TLS: tlsMode ? '1' : '0',
  // An upload workload is only comparable when both servers actually read the
  // body: left unread, node answers and resets the stream while we keep buffering
  // it, so the two are measured on different work. GET cases are untouched.
  READBODY: body > 0 ? '1' : '0'
};
if (opt.shape === '0') {
  env.SHAPE = '0';
}
// UNIQ=1 makes every response carry a unique header value, so that no response
// header block can be reused -- the case of a server that echoes a request id.
if (opt.uniq) {
  env.UNIQ = '1';
}

let postFile = null;
if (body > 0) {
  fs.mkdirSync('out', { recursive: true });
  postFile = `out/h2load-body-${body}.bin`;
  fs.writeFileSync(postFile, Buffer.alloc(body, 0x61));
}

const srvArgs = (process.env.PRELOAD
  ? ['-r', require('node:path').resolve(process.env.PRELOAD)] : [])
  .concat([SRV_FILES[kind]]);
const srv = spawn(process.execPath, srvArgs, {
  env, stdio: ['ignore', 'pipe', 'inherit']
});
let srvOut = '';
srv.stdout.on('data', (d) => {
  srvOut += d;
});
srv.on('exit', () => {
  srvOut += '';
});

const wait = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  // A server that dies on startup (a bad PRELOAD path, for instance) used to leave this
  // polling forever, because stdout simply never arrives.
  for (let i = 0; i < 100 && !/listening/.test(srvOut); i++) {
    await wait(100);
  }
  if (!/listening/.test(srvOut)) {
    console.error('server never came up. Its stdout:', JSON.stringify(srvOut));
    srv.kill();
    process.exit(1);
  }
  await wait(300);

  const args = ['-c', clients, '-t', threads, '-m', streams,
    '--duration', duration, '-n', 10000000,
    `${tlsMode ? 'https' : 'http'}://127.0.0.1:${port}/`];
  if (postFile) {
    args.splice(args.length - 1, 0, '-d', postFile);
  }
  if (winbits) {
    args.push('-w', winbits, '-W', winbits);
  }

  let out = '';
  try {
    out = execFileSync('h2load', args.map(String), { encoding: 'utf8', maxBuffer: 8 << 20 });
  } catch (err) {
    // h2load exits non-zero when any request failed, but it still printed the
    // summary: keep parsing it, and let the error lines show what went wrong
    out = String(err.stdout || '');
    if (!/finished in/.test(out)) {
      console.error('h2load failed:', out + String(err.stderr || ''));
      srv.kill();
      process.exit(1);
    }
  }
  console.log(out.replace(/(start time|end time|time for request|time for processing|time to first byte).*\n/g, ''));

  const grab = (re, src = out) => {
    const m = re.exec(src); return m ? m[1] : '?';
  };
  const rps = grab(/finished in [\d.]+s, ([\d.]+) req\/s/);
  const succeeded = grab(/(\d+) succeeded/);
  const failed = grab(/\d+ succeeded, (\d+) failed/);

  // let the server reach the end of its own measurement window
  while (!/cpuPerReq=/.test(srvOut)) {
    await wait(200);
  }
  const grabSrv = (re) => {
    const m = re.exec(srvOut); return m ? m[1] : '?';
  };
  console.log(`\n### h2load ${kind}/${api}${tlsMode ? '/h2s' : ''} | clients=${clients} threads=${threads} ` +
              `streams=${streams} body=${body}B resp=${resp}B winbits=${winbits || 16}`);
  console.log(`    h2load:  ${rps} req/s, succeeded=${succeeded}, failed=${failed}`);
  console.log(`    server:  cpuPerReq=${grabSrv(/cpuPerReq=([\d.]+)us/)}us ` +
              `busy=${grabSrv(/busy=([\d.]+)%/)}% served=${grabSrv(/req=(\d+)/)}` +
              // present only when GC=1 makes the server count the collector
              (process.env.GC === '1'
                ? ` gc=${grabSrv(/gc=([\d.]+)ms/)}ms gcPct=${grabSrv(/gcPct=([\d.]+)%/)}%`
                : ''));
  srv.kill();
  process.exit(0);
})();
