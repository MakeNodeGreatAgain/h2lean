// Runner: saturate the server with N client processes, report server CPU/req.
//   node bench/run.js --server=h2lean --clients=4 --conc=32 --duration=12000 --body=0
const { spawn } = require('node:child_process');

const argv = process.argv.slice(2);
const opt = Object.fromEntries(argv.map((a) => {
  const m = /^--([^=]+)=(.*)$/.exec(a);
  return m ? [m[1], m[2]] : [a.slice(2), true];
}));

const kind = opt.server || 'native';
const clients = Number(opt.clients || 4);
const conc = Number(opt.conc || 32);
const duration = Number(opt.duration || 12000);
const body = Number(opt.body || 0);
const port = Number(opt.port || 8460);

const SRV_FILES = {
  native: 'bench/prof-h2.js', // node:http2
  h2lean: 'bench/h2lean-h2.js', // this project, API-compatible server
  lite: 'bench/lite-h2.js' // the throwaway PoC: ceiling, no API
};
const srvFile = SRV_FILES[kind] || SRV_FILES.native;
const env = {
  ...process.env, PORT: String(port), DURATION: String(duration),
  BODY: String(body), API: opt.api || 'stream'
};

const srv = spawn(process.execPath, [srvFile], {
  env: { ...env, ROLE: 'server', DURATION: String(duration + 2500) },
  stdio: ['ignore', 'pipe', 'inherit']
});
let srvOut = '';
srv.stdout.on('data', (d) => {
  srvOut += d.toString(); process.stdout.write(d);
});

const wait = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  while (!/listening/.test(srvOut)) {
    await wait(100);
  }
  await wait(300);
  const cs = [];
  for (let i = 0; i < clients; i++) {
    const c = spawn(process.execPath, ['bench/prof-h2.js'], {
      env: { ...env, ROLE: 'client', CONC: String(conc) },
      stdio: ['ignore', 'pipe', 'inherit']
    });
    let out = '';
    c.stdout.on('data', (d) => {
      out += d;
    });
    cs.push({ c, get out() {
      return out;
    } });
  }
  await wait(duration + 4200);
  let totalRps = 0;
  for (const { out } of cs) {
    const m = /rps=(\d+).*?ok=(\d+)/.exec(out);
    if (m) {
      totalRps += Number(m[1]);
    }
  }
  const grab = (re) => {
    const m = re.exec(srvOut); return m ? m[1] : '?';
  };
  console.log(`\n### ${kind} | clients=${clients} conc=${conc} body=${body}B`);
  console.log(`    server: cpuPerReq=${grab(/cpuPerReq=([\d.]+)us/)}us ` +
              `busy=${grab(/busy=([\d.]+)%/)}% req=${grab(/req=(\d+)/)}`);
  console.log(`    clients total rps=${totalRps}`);
  srv.kill();
  for (const { c } of cs) {
    c.kill();
  }
  await wait(300);
  process.exit(0);
})();
