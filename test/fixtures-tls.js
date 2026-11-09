// The self-signed loopback certificate pair used by the TLS tests and by the
// TLS benchmarks. Generated once into out/ and kept, because an EC pair takes
// well under a second to make and the expiry is far out.
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');

const OUT = path.join(__dirname, '..', 'out');
const PEM = path.join(OUT, 'test-key.pem');
const CRT = path.join(OUT, 'test-cert.pem');

module.exports = function certificates() {
  if (!fs.existsSync(PEM) || !fs.existsSync(CRT)) {
    fs.mkdirSync(OUT, { recursive: true });
    try {
      execFileSync('openssl', ['req', '-x509', '-newkey', 'ec',
        '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-pkeyopt', 'ec_param_enc:named_curve',
        '-keyout', PEM, '-out', CRT, '-days', '400', '-nodes',
        '-subj', '/CN=localhost',
        '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1'],
      { stdio: ['ignore', 'ignore', 'pipe'] });
    } catch (err) {
      throw new Error('TLS needs an openssl binary to create a certificate: ' + err.message,
        { cause: err });
    }
  }
  return { key: fs.readFileSync(PEM), cert: fs.readFileSync(CRT) };
};
