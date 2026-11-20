// The self-signed loopback certificate pair used by the TLS tests and by the TLS
// benchmarks. An EC pair takes ~8 ms to make (measured), so every process makes its
// own instead of sharing one cached pair -- see why that is not just a preference.
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');

const OUT = path.join(__dirname, '..', 'out');

// WHY THE FILE NAMES CARRY THE PID. `node --test` runs one process per test file, so
// on a fresh checkout a dozen processes reach this at the same moment -- and a key
// plus a certificate is not one atomic object, it is two writes. Process A can
// therefore leave its key sitting next to process B's certificate, which node reports
// when the server is built: ERR_OSSL_X509_KEY_VALUES_MISMATCH from createSecureContext.
// Measured on this tree: four TLS test files started in parallel against an empty out/,
// 4 of 5 rounds produced the mismatch. It is the failure the CI run showed on the
// node 20/22 arms of allow-http1.test.js, where the processes are scheduled
// differently than on 24 -- which is why it never reproduced as a stable test bug.
//
// A process that only ever reads what it wrote cannot observe a mixture, so there is
// no lock, no shared cache file and no stale-lock recovery here. Generating per
// process is ~8 ms: cheaper than any coordination about generating once.
function generate(base, extraExtensions) {
  const keyPath = path.join(OUT, `${base}-key-${process.pid}.pem`);
  const certPath = path.join(OUT, `${base}-cert-${process.pid}.pem`);
  fs.mkdirSync(OUT, { recursive: true });
  try {
    execFileSync('openssl', ['req', '-x509', '-newkey', 'ec',
      '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-pkeyopt', 'ec_param_enc:named_curve',
      '-keyout', keyPath, '-out', certPath, '-days', '400', '-nodes',
      '-subj', '/CN=localhost',
      '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1', ...extraExtensions],
    { stdio: ['ignore', 'ignore', 'pipe'] });
  } catch (err) {
    throw new Error('TLS needs an openssl binary to create a certificate: ' + err.message,
      { cause: err });
  }
  return { keyPath, certPath };
}

let plain = null;

module.exports = function certificates() {
  if (plain === null) {
    const { keyPath, certPath } = generate('test', []);
    plain = { key: fs.readFileSync(keyPath), cert: fs.readFileSync(certPath) };
    // The bytes live in memory from here on, and out/ is scratch: unlink the pair
    // instead of leaving one file per test process behind. The files cannot be
    // wanted by anybody else -- they are named after this process.
    fs.unlinkSync(keyPath);
    fs.unlinkSync(certPath);
  }
  return plain;
};

// A second pair for the @passcod/faith bench arm (bench/client/) and the h2load
// probes. rustls, the TLS stack behind faith, rejects the pair above: webpki
// requires an extendedKeyUsage of serverAuth on the server certificate and the
// fixture has no EKU extension at all (measured: InvalidCertificate(EkuError) on
// connect). Same recipe plus the extension. The paths are handed back because the
// caller passes them to a child process, so they stay on disk for its lifetime.
let withEku = null;

module.exports.serverAuth = function serverAuthCertificates() {
  if (withEku === null) {
    const { keyPath, certPath } = generate('serverauth',
      ['-addext', 'extendedKeyUsage=serverAuth']);
    withEku = {
      key: fs.readFileSync(keyPath), cert: fs.readFileSync(certPath),
      keyPath, certPath
    };
  }
  return withEku;
};
