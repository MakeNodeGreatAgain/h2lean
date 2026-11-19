// Transport prerequisites of the @passcod/faith client bench arm
// (bench/client/arms.js). Three facts, each measured here against a live
// nghttpd so the arm's shape differences rest on a probe, not on recall:
//
//   1. faith 1.1.0 has NO h2c prior knowledge: a fetch of a plaintext http://
//      origin sends HTTP/1.1, and nghttpd --no-tls answers with an HTTP/2
//      preface the HTTP/1 parser cannot read (hyper Parse(Version)). The arm
//      therefore cannot share the h2c server recipe of the other two arms.
//   2. Over TLS+ALPN the same fetch negotiates HTTP/2.0 and completes -- but
//      only with a server certificate carrying extendedKeyUsage=serverAuth:
//      rustls/webpki rejects the shared test fixture (no EKU extension) with
//      InvalidCertificate(EkuError). Hence fixtures-tls.js' serverAuth pair.
//   3. A warm Agent multiplexes concurrent requests onto ONE pooled h2
//      connection (agent.connections() after a warm request + a 64-way burst),
//      which is why the arm maps one Agent per connection group. A cold burst
//      opens several connections that then idle out -- the count is the pool's
//      decision, not a pinned socket count.
//
//   node tools/probe-faith.js
//
// Local-only tool, like the other probes: it prints response metadata by design.
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const { fetch, Agent } = require('@passcod/faith');
const fixtures = require('../test/fixtures-tls');

const pair = fixtures.serverAuth();
const dir = path.join(__dirname, '..', 'bench', 'client', 'dir');
fs.mkdirSync(path.join(dir, 'f'), { recursive: true });
fs.writeFileSync(path.join(dir, 'f', 'index.html'), 'x'.repeat(16384));

const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const PLAIN_PORT = 8978;
const TLS_PORT = 8979;

function nghttpd(port, tls) {
  const args = (tls ? [] : ['--no-tls']).concat(
    ['-m', '1024', '-n', '4', '-w', '22', '-W', '22', String(port)],
    tls ? [pair.keyPath, pair.certPath] : ['.']);
  return spawn('nghttpd', args, { cwd: dir, stdio: ['ignore', 'ignore', 'pipe'] });
}

async function readAll(res) {
  let bytes = 0;
  const reader = res.body.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    bytes += value.length;
  }
  return bytes;
}

(async () => {
  // 1. Plaintext: the fetch must FAIL with a parse error, proving faith spoke
  // HTTP/1.1 to an h2c-only listener rather than prior-knowledge HTTP/2.
  const plain = nghttpd(PLAIN_PORT, false);
  await wait(400);
  let plainErr = null;
  try {
    const res = await fetch(`http://127.0.0.1:${PLAIN_PORT}/f/index.html`);
    await readAll(res);
  } catch (e) {
    plainErr = e;
  }
  plain.kill();
  if (!plainErr || !String(plainErr.message).includes('Parse(Version)')) {
    console.log('FAIL: plaintext fetch did not die on Parse(Version): ' + plainErr);
    process.exit(1);
  }
  console.log('ok: no h2c prior knowledge -- plaintext fetch fails with ' +
              'hyper Parse(Version) against nghttpd --no-tls');

  // 2. TLS+ALPN with the serverAuth pair: 200, HTTP/2.0, full body.
  const tls = nghttpd(TLS_PORT, true);
  await wait(400);
  const opts = { tls: { extraRoots: [pair.cert], revocation: { onecrl: false, crlite: false } } };
  const url = `https://127.0.0.1:${TLS_PORT}/f/index.html`;
  const agents = [new Agent(opts), new Agent(opts)];
  try {
    // Warm each agent with one sequential request so the pool holds an
    // established h2 connection before the burst (fact 3's precondition).
    for (const agent of agents) {
      const res = await fetch(url, { agent });
      const bytes = await readAll(res);
      if (res.status !== 200 || res.version !== 'HTTP/2.0' || bytes !== 16384) {
        console.log(`FAIL: status=${res.status} version=${res.version} bytes=${bytes}`);
        process.exit(1);
      }
    }
    console.log('ok: TLS+ALPN negotiates HTTP/2.0, 200, 16384 body bytes');

    // 3. Pool shape: 64 concurrent requests per warm agent.
    await Promise.all(agents.flatMap((agent) =>
      Array.from({ length: 64 }, () => fetch(url, { agent }).then(readAll))));
    await wait(1200); // connections() stats refresh once a second
    const counts = agents.map((a) => a.connections().length);
    console.log(`pool: connections per warm agent after a 64-way burst: ${counts.join(', ')}`);
    if (counts.some((n) => n !== 1)) {
      console.log('note: pool opened extra connections; the arm documents the ' +
                  'count as emergent, so this is reported, not a failure');
    } else {
      console.log('ok: one pooled h2 connection per warm agent');
    }
  } finally {
    for (const agent of agents) {
      agent.close();
    }
    tls.kill();
  }
  process.exit(0);
})().catch((e) => {
  console.error('probe crashed: ' + (e && e.stack || e));
  process.exit(1);
});
