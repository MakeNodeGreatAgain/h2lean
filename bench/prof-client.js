// One client arm for the profile runs: the same loop as the client role of
// bench/prof-h2.js (no promises in the request path, so the profile shows the
// library's own allocations and not the harness's), with the client
// implementation as the only difference.
//
//   IMPL=h2lean PORT=8443 node --cpu-prof --cpu-prof-dir=out/x bench/prof-client.js
//   IMPL=node   PORT=8443 node --cpu-prof --cpu-prof-dir=out/y bench/prof-client.js
//
// The peer is the same server process for both arms, so both clients decode the
// same frames.
const IMPL = process.env.IMPL === 'node' ? 'node:http2' : '../index.js';
const impl = IMPL === 'node:http2' ? require('node:http2') : require(IMPL);

const PORT = Number(process.env.PORT || 8443);
const CONC = Number(process.env.CONC || 64);
const DURATION_MS = Number(process.env.DURATION || 10000);
const VARIANT = process.env.VARIANT === '1';

const REQ_HEADERS = {
  ':method': 'GET',
  ':path': '/abc/def/ghij?x=1',
  accept: '*/*',
  'user-agent': 'probe/1.0'
};
let seq = 0;

const client = impl.connect(`http://127.0.0.1:${PORT}`);
client.on('error', (e) => {
  console.error('client err', e); process.exit(1);
});
let done = 0;
let errs = 0;
let stopped = false;
const cpu0 = process.cpuUsage();
const t0 = Number(process.hrtime.bigint());

function one() {
  if (stopped) {
    return;
  }
  const headers = VARIANT ? { ...REQ_HEADERS, 'x-req': String(++seq) } : REQ_HEADERS;
  const req = client.request(headers);
  req.resume();
  req.on('end', () => {
    done++; one();
  });
  req.on('error', () => {
    errs++;
  });
}

for (let i = 0; i < CONC; i++) {
  one();
}
setTimeout(() => {
  done = 0; errs = 0;
}, 2000);
setTimeout(() => {
  stopped = true;
  const cpu = process.cpuUsage(cpu0);
  const wall = (Number(process.hrtime.bigint()) - t0) / 1e6;
  const cpuMs = (cpu.user + cpu.system) / 1000;
  console.log(`[${IMPL}] done=${done} err=${errs} wall=${wall.toFixed(0)}ms cpu=${cpuMs.toFixed(0)}ms `
    + `cpuPerReq=${(cpuMs * 1000 / Math.max(done, 1)).toFixed(2)}us busy=${(cpuMs * 100 / wall).toFixed(1)}%`);
  client.close();
  setTimeout(() => process.exit(0), 200);
}, DURATION_MS);
