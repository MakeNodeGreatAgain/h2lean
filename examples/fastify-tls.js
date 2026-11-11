// h2lean + Fastify over TLS (h2s): HTTP/2 negotiated through ALPN on an
// https:// socket. This is the secure twin of examples/fastify.js -- the only
// real difference is that Fastify is created with an `https` block and the
// serverFactory builds an h2lean.createSecureServer instead of createServer.
//
// How the pieces meet (Fastify v5, verified against its own docs):
//   * `http2: true` + a non-null `https` object tells Fastify this is a secure
//     HTTP/2 server. Instead of calling node:http2 itself, Fastify hands its
//     `handler` and the whole options object to `serverFactory(handler, opts)`.
//   * The TLS material lives in `opts.https` (`{ key, cert }`), so the factory
//     forwards those options to h2lean.createSecureServer and wires `handler`
//     as the request listener. h2lean forces ALPN to ['h2'] the way node does,
//     so a peer that does not offer 'h2' never becomes a session.
//
// A self-signed localhost certificate is generated on first run into out/
// (openssl, same fixtures the TLS tests use); it is a throwaway loopback cert,
// not a credential. Because it is untrusted, clients must skip verification.
//
//   npm install          # fastify is a devDependency of this repo
//   node examples/fastify-tls.js
// then, from another shell:
//   curl -k --http2 https://localhost:8443/
'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const Fastify = require('fastify');
const h2lean = require('../index.js'); // the same surface as require('h2lean')

// Generates (and caches) a self-signed EC key/cert pair valid for localhost.
function localhostCertificates() {
  const out = path.join(__dirname, '..', 'out');
  const key = path.join(out, 'example-key.pem');
  const cert = path.join(out, 'example-cert.pem');
  if (!fs.existsSync(key) || !fs.existsSync(cert)) {
    fs.mkdirSync(out, { recursive: true });
    execFileSync('openssl', ['req', '-x509', '-newkey', 'ec',
      '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-pkeyopt', 'ec_param_enc:named_curve',
      '-keyout', key, '-out', cert, '-days', '400', '-nodes',
      '-subj', '/CN=localhost',
      '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1'],
    { stdio: ['ignore', 'ignore', 'pipe'] });
  }
  return { key: fs.readFileSync(key), cert: fs.readFileSync(cert) };
}

const fastify = Fastify({
  http2: true,
  https: localhostCertificates(),
  // `opts` is the object above, so the TLS material arrives via opts.https.
  serverFactory: (handler, opts) =>
    h2lean.createSecureServer(opts.https, (req, res) => handler(req, res)),
});

fastify.get('/', async () => 'hello world over h2s');

fastify.listen({ port: 8443, host: '127.0.0.1' }, (err) => {
  if (err) {
    fastify.log.error(err);
    process.exit(1);
  }
  console.log('h2lean + Fastify serving h2s on https://localhost:8443');
  console.log('try: curl -k --http2 https://localhost:8443/');
});
