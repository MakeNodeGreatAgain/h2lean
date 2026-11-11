// One showcase arm, one payload: start the server, saturate it with h2load, and
// print a single JSON result line for matrix.js to collect. h2load is nghttp2's
// C client, so it can push past what a JS client could generate and the req/s
// here is close to the server's real ceiling; cpuPerReq is read from inside the
// server process, which is the protocol-fair metric (raw req/s depends on
// whatever else the machine is doing).
//
//   node bench/showcase/run.js --arm="h2lean compat" --body=262144 --duration=6 --port=8970
//
// The protocol decides h2load's flags: HTTP/1.1 arms run `-p http/1.1` (h2load
// speaks HTTP/1.1 with keep-alive); HTTP/2 arms run its cleartext h2c default.
// For h2c a generous flow-control window (-w/-W, 4 MB) is advertised so a 256 KB
// response is not throttled by the default 64 KB window -- otherwise the download
// arm would measure window updates, not the server's send path. HTTP/1.1 has no
// such window (TCP governs), so the flag is h2-only.
const { spawn, execFileSync } = require('node:child_process');
const path = require('node:path');
const ARMS = require('./arms');

const opt = Object.fromEntries(process.argv.slice(2).map((a) => {
  const m = /^--([^=]+)=(.*)$/.exec(a);
  return m ? [m[1], m[2]] : [a.slice(2), true];
}));

const armName = opt.arm;
const arm = ARMS[armName];
if (!arm) {
  console.error('unknown arm: ' + armName + ' (have: ' + Object.keys(ARMS).join(', ') + ')');
  process.exit(2);
}
const body = Number(opt.body || 0);
const duration = Number(opt.duration || 6);
const threads = Number(opt.threads || 4);
const port = Number(opt.port || 8970);
const quiet = opt.verbose !== true && opt.verbose !== '1';

const srvFile = path.join(__dirname, arm.file);
const srv = spawn(process.execPath, [srvFile], {
  env: { ...process.env, PORT: String(port), BODY: String(body),
    DURATION: String(duration * 1000 + 2500) },
  stdio: ['ignore', 'pipe', 'inherit']
});
let srvOut = '';
let srvDead = false;
srv.stdout.on('data', (d) => {
  srvOut += d;
});
srv.on('exit', () => {
  srvDead = true;
});

const wait = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  for (let i = 0; i < 100 && !/listening/.test(srvOut) && !srvDead; i++) {
    await wait(100);
  }
  if (!/listening/.test(srvOut)) {
    console.error(`server for "${armName}" never came up. stdout: ${JSON.stringify(srvOut)}`);
    srv.kill();
    process.exit(1);
  }
  await wait(300);

  const url = `http://127.0.0.1:${port}/`;
  const args = ['-c', arm.conn, '-t', threads, '--duration', duration, '-n', 10000000];
  if (arm.proto === 'h1') {
    args.push('-p', 'http/1.1');
  } else {
    args.push('-m', arm.streams, '-w', 22, '-W', 22);
  }
  args.push(url);

  let load = '';
  try {
    load = execFileSync('h2load', args.map(String), { encoding: 'utf8', maxBuffer: 8 << 20 });
  } catch (err) {
    load = String(err.stdout || '');
    if (!/finished in/.test(load)) {
      console.error(`h2load failed on "${armName}" body=${body}: ${load + String(err.stderr || '')}`);
      srv.kill();
      process.exit(1);
    }
  }
  const grab = (re, src = load) => {
    const m = re.exec(src); return m ? m[1] : '?';
  };
  const rps = grab(/finished in [\d.]+s, ([\d.]+) req\/s/);
  const succeeded = grab(/(\d+) succeeded/);
  const failed = grab(/\d+ succeeded, (\d+) failed/);

  // let the server reach the end of its own measurement window, then read cpuPerReq
  for (let i = 0; i < 60 && !/cpuPerReq=/.test(srvOut) && !srvDead; i++) {
    await wait(200);
  }
  const grabSrv = (re) => {
    const m = re.exec(srvOut); return m ? m[1] : '?';
  };

  const result = {
    arm: armName, label: arm.label, proto: arm.proto, body,
    inflight: arm.inflight, duration, threads,
    rps: Number(rps), cpuPerReq: Number(grabSrv(/cpuPerReq=([\d.]+)us/)),
    busy: Number(grabSrv(/busy=([\d.]+)%/)), served: Number(grabSrv(/req=(\d+)/)),
    succeeded: Number(succeeded), failed: Number(failed)
  };
  if (!quiet) {
    console.log(`\n### ${arm.label} | proto=${arm.proto} body=${body}B inflight=${arm.inflight} ` +
                `clients=${arm.conn} threads=${threads}`);
    console.log(`    h2load: ${result.rps} req/s, succeeded=${result.succeeded}, failed=${result.failed}`);
    console.log(`    server: cpuPerReq=${result.cpuPerReq}us busy=${result.busy}% served=${result.served}`);
  }
  console.log('RESULT ' + JSON.stringify(result));
  srv.kill();
  process.exit(0);
})();
