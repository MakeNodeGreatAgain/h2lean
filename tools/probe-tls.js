// What node's createSecureServer actually does, so h2lean can match it: the ALPN
// decision, session.encrypted, and the class shape. Run against node only.
'use strict';
const fs = require('node:fs');
const tls = require('node:tls');
const http2 = require('node:http2');

const key = fs.readFileSync(`${__dirname}/../out/test-key.pem`);
const cert = fs.readFileSync(`${__dirname}/../out/test-cert.pem`);

const srv = http2.createSecureServer({ key, cert });
console.log('instanceof tls.Server:', srv instanceof tls.Server);
srv.on('session', (s) => console.log('  session: encrypted =', s.encrypted,
  ', alpn =', s.socket.alpnProtocol, ', encrypted field type =', typeof s.encrypted));
srv.on('stream', (stream) => {
  stream.respond({ ':status': 200, 'x-alpn': String(stream.session.socket.alpnProtocol) });
  stream.end('ok');
});
srv.on('tlsClientError', (e) => console.log('  tlsClientError:', e.code || e.message));
srv.on('secureConnection', (s) => console.log('  secureConnection: alpn =', String(s.alpnProtocol)));
srv.on('error', (e) => console.log('  server error:', e.code || e.message));

srv.listen(0, '127.0.0.1', () => {
  const port = srv.address().port;
  const base = { host: '127.0.0.1', port, rejectUnauthorized: false };

  function observe(label, sock) {
    return new Promise((resolve) => {
      const seen = [];
      sock.on('data', (d) => seen.push(d.length));
      sock.on('error', (e) => seen.push(`error:${e.code || e.message}`));
      sock.on('close', () => seen.push('close'));
      sock.on('end', () => seen.push('end'));
      setTimeout(() => {
        console.log(`${label}:`, JSON.stringify({
          alpn: String(sock.alpnProtocol), events: seen.slice(0, 4)
        }));
        sock.destroy();
        resolve();
      }, 700);
    });
  }

  (async () => {
    // 1. a real h2 client
    const c1 = http2.connect(`https://127.0.0.1:${port}`, { rejectUnauthorized: false });
    const headers = await new Promise((resolve, reject) => {
      const req = c1.request({ ':path': '/' });
      let body = '';
      req.on('response', resolve);
      req.on('data', (d) => {
        body += d;
      });
      req.on('end', () => resolve(body));
      req.on('error', reject);
      req.end();
    }).catch((e) => `err ${e.code}`);
    console.log('1. http2.connect over https -> response:', JSON.stringify(headers));
    console.log('   client session encrypted:', c1.encrypted, 'alpn:', c1.alpnProtocol);
    c1.destroy();

    // 2. ALPN offers only http/1.1
    await observe('2. ALPN http/1.1 only',
      tls.connect({ ...base, ALPNProtocols: ['http/1.1'] }));

    // 3. no ALPN at all
    await observe('3. no ALPN', tls.connect(base));

    // 4. plain TCP with no TLS handshake at all
    const net = require('node:net');
    await observe('4. raw tcp, no tls', net.connect(port));

    srv.close();
  })();
});
