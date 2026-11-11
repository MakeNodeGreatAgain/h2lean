// The h2s (TLS) front door. Each case is run twice in the same process -- once
// against node:http2's createSecureServer and once against h2lean's -- and the two
// recordings are compared, so what these tests assert is "the peer and the
// application observe the same thing", not an expectation picked from the docs.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const tls = require('node:tls');
const http2 = require('node:http2');
const h2lean = require('../index');
const certificates = require('./fixtures-tls');
const { listen } = require('./diff-harness');

function tlsRequest(client, path_) {
  return new Promise((resolve, reject) => {
    const req = client.request({ ':method': 'GET', ':path': path_, 'x-mark': 'tls' });
    const chunks = [];
    let headers = null;
    req.on('response', (h) => {
      headers = h;
    });
    req.setEncoding('latin1');
    req.on('data', (d) => chunks.push(d));
    req.on('end', () => resolve({ headers, body: chunks.join('') }));
    req.on('error', reject);
  });
}

function connectTls(port) {
  const client = http2.connect(`https://127.0.0.1:${port}`, { rejectUnauthorized: false });
  client.on('error', () => {});
  return client;
}

// Deliberately not shared between the two runs: each gets its own server, client
// and observation record, and the test compares the records.
async function streamRun(impl) {
  const { key, cert } = certificates();
  const srv = impl.createSecureServer({ key, cert });
  const sessions = [];
  srv.on('session', (s) => sessions.push({
    alpnProtocol: s.alpnProtocol,
    encrypted: s.encrypted,
    socketEncrypted: s.socket.encrypted === true,
    serverBackReference: s.server === srv
  }));
  srv.on('stream', (stream, headers) => {
    stream.respond({
      ':status': 200,
      'x-echo': headers[':path'],
      'x-mark': 'tls',
      // the port differs between the two runs, so only the host is compared
      'x-authority': String(headers[':authority']).replace(/:\d+$/, '')
    });
    stream.end(`hello ${headers[':path']}`);
  });
  const port = await listen(srv);
  const client = connectTls(port);
  const answer = await tlsRequest(client, '/over-tls');
  const peer = client.socket.getPeerCertificate();
  const out = {
    sessions,
    status: answer.headers[':status'],
    echo: answer.headers['x-echo'],
    mark: answer.headers['x-mark'],
    authority: answer.headers['x-authority'],
    body: answer.body,
    clientAlpn: client.alpnProtocol,
    clientEncrypted: client.encrypted,
    peerCommonName: peer && peer.subject ? peer.subject.CN : null,
    instanceOfTlsServer: srv instanceof tls.Server
  };
  client.destroy();
  srv.close();
  await new Promise((r) => setTimeout(r, 30));
  return out;
}

test('the stream API over TLS behaves like node', async () => {
  const node = await streamRun(http2);
  const ours = await streamRun(h2lean);
  assert.deepStrictEqual(ours, node);
  // and the reference itself says what h2s is supposed to say
  assert.strictEqual(node.clientAlpn, 'h2');
  assert.strictEqual(node.sessions[0].encrypted, true);
  assert.strictEqual(node.peerCommonName, 'localhost');
  assert.strictEqual(node.instanceOfTlsServer, true);
});

async function compatRun(impl) {
  const { key, cert } = certificates();
  const srv = impl.createSecureServer({ key, cert }, (req, res) => {
    res.writeHead(200, {
      'x-url': req.url,
      'x-encrypted': String(req.socket.encrypted === true),
      'x-alpn': String(req.stream.session.alpnProtocol),
      'x-authority': String(req.headers[':authority']).replace(/:\d+$/, '')
    });
    res.end(`compat ${req.method}`);
  });
  const port = await listen(srv);
  const client = connectTls(port);
  const answer = await tlsRequest(client, '/compat-tls');
  const out = {
    status: answer.headers[':status'],
    url: answer.headers['x-url'],
    encrypted: answer.headers['x-encrypted'],
    alpn: answer.headers['x-alpn'],
    authority: answer.headers['x-authority'],
    body: answer.body
  };
  client.destroy();
  srv.close();
  await new Promise((r) => setTimeout(r, 30));
  return out;
}

test('the compat (req, res) API over TLS behaves like node', async () => {
  const node = await compatRun(http2);
  const ours = await compatRun(h2lean);
  assert.deepStrictEqual(ours, node);
  assert.strictEqual(node.encrypted, 'true');
  assert.strictEqual(node.alpn, 'h2');
});

// A peer that offers only http/1.1 must be refused while the handshake is still
// running. The application cannot prevent that either: node fixes the ALPN list to
// ['h2'], so even the list passed to the constructor below is ignored and openssl
// answers with the no_application_protocol alert instead of creating a session.
async function refusedRun(impl) {
  const { key, cert } = certificates();
  const srv = impl.createSecureServer({ key, cert, ALPNProtocols: ['h2', 'http/1.1'] });
  const events = [];
  srv.on('session', () => events.push('session'));
  srv.on('tlsClientError', (err) => events.push(`tlsClientError:${err.code}`));
  const port = await listen(srv);
  const outcome = await new Promise((resolve) => {
    const sock = tls.connect({
      host: '127.0.0.1', port, rejectUnauthorized: false, ALPNProtocols: ['http/1.1']
    });
    sock.on('error', (err) => resolve(`error:${err.code}`));
    sock.on('secure', () => resolve(`secure:alpn=${sock.alpnProtocol}`));
    sock.on('close', () => resolve('close'));
  });
  await new Promise((r) => setTimeout(r, 120));
  const out = { outcome, events: [...events] };
  srv.close();
  await new Promise((r) => setTimeout(r, 30));
  return out;
}

test('an application-supplied ALPN list cannot make the h2 server accept http/1.1',
  async () => {
    const node = await refusedRun(http2);
    const ours = await refusedRun(h2lean);
    assert.deepStrictEqual(ours, node);
    assert.deepStrictEqual(node.events, ['tlsClientError:ERR_SSL_NO_APPLICATION_PROTOCOL']);
  });

// The application cannot ask an h2 server to also speak http/1.1: the ALPN list is
// fixed. A context installed later with setSecureContext() can negotiate a
// different protocol, and then it is the JS-side guard, not the handshake, that has
// to keep such a connection out of the session list.
async function lateContextRun(impl) {
  const { key, cert } = certificates();
  const srv = impl.createSecureServer({ key, cert });
  srv.setSecureContext({ key, cert, ALPNProtocols: ['h2', 'http/1.1'] });
  const events = [];
  srv.on('session', () => events.push('session'));
  srv.on('secureConnection', (s) => events.push(`secureConnection:${s.alpnProtocol}`));
  const port = await listen(srv);
  const outcome = await new Promise((resolve) => {
    const sock = tls.connect({
      host: '127.0.0.1', port, rejectUnauthorized: false, ALPNProtocols: ['http/1.1']
    });
    sock.on('error', (err) => resolve(`error:${err.code}`));
    sock.on('close', () => resolve('close'));
    sock.on('secure', () => {
      setTimeout(() => resolve(`alpn=${sock.alpnProtocol},destroyed=${sock.destroyed}`), 200);
    });
  });
  await new Promise((r) => setTimeout(r, 100));
  const out = { outcome, events: [...events].sort() };
  srv.close();
  await new Promise((r) => setTimeout(r, 30));
  return out;
}

test('a context installed later still keeps a non-h2 protocol out of the sessions',
  async () => {
    const node = await lateContextRun(http2);
    const ours = await lateContextRun(h2lean);
    assert.deepStrictEqual(ours, node);
    assert.ok(!ours.events.includes('session'),
      'a connection that did not negotiate h2 must never become a session');
  });

// The TLS port is not an h2c port: a peer that connects in cleartext and starts
// talking HTTP/2 immediately must not be handed a session.
async function cleartextRun(impl) {
  const { key, cert } = certificates();
  const srv = impl.createSecureServer({ key, cert });
  const events = [];
  srv.on('session', () => events.push('session'));
  srv.on('tlsClientError', (err) => events.push(`tlsClientError:${err.code || 'none'}`));
  const port = await listen(srv);
  const received = await new Promise((resolve) => {
    const sock = net.connect(port, '127.0.0.1');
    const chunks = [];
    sock.on('data', (d) => chunks.push(d.length));
    sock.on('error', () => {});
    sock.on('close', () => resolve(chunks));
    setTimeout(() => {
      sock.destroy(); resolve(chunks);
    }, 400);
    sock.write('PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n');
  });
  const out = { events: [...events], receivedBytes: received };
  srv.close();
  await new Promise((r) => setTimeout(r, 30));
  return out;
}

test('cleartext HTTP/2 on the TLS port is not accepted as a session', async () => {
  const node = await cleartextRun(http2);
  const ours = await cleartextRun(h2lean);
  assert.deepStrictEqual(ours, node);
  assert.ok(!node.events.includes('session'));
});

// A body larger than one TLS record has to be reassembled by the frame parser out
// of whatever pieces the record layer hands over, and the flow-control credit must
// come back per consumption -- the same code path as h2c, but it is worth proving
// that the transport underneath does not change it.
test('a 70 KB upload over TLS arrives intact on both stacks', async () => {
  const BODY = 'x'.repeat(70000);
  async function uploadRun(impl) {
    const { key, cert } = certificates();
    const srv = impl.createSecureServer({ key, cert }, (req, res) => {
      const chunks = [];
      req.on('data', (d) => chunks.push(d));
      req.on('end', () => {
        res.writeHead(200, { 'x-received': String(Buffer.concat(chunks).length) });
        res.end(String(Buffer.concat(chunks).length));
      });
    });
    const port = await listen(srv);
    const client = connectTls(port);
    const answer = await new Promise((resolve, reject) => {
      const req = client.request({ ':method': 'POST', ':path': '/upload' });
      let body = '';
      let headers = null;
      req.on('response', (h) => {
        headers = h;
      });
      req.setEncoding('latin1');
      req.on('data', (d) => {
        body += d;
      });
      req.on('end', () => resolve({ received: headers['x-received'], body }));
      req.on('error', reject);
      req.end(BODY);
    });
    client.destroy();
    srv.close();
    await new Promise((r) => setTimeout(r, 30));
    return answer;
  }
  const node = await uploadRun(http2);
  const ours = await uploadRun(h2lean);
  assert.deepStrictEqual(ours, node);
  assert.strictEqual(ours.received, String(70000));
});

// Both directions have to survive the socket owning our output buffer after the
// write call returns: a body larger than the outbound staging buffer is flushed in
// pieces, and each piece must reach the peer as the bytes that were built for it.
test('a 200 KB response over TLS arrives intact on both stacks', async () => {
  async function downloadRun(impl) {
    const { key, cert } = certificates();
    const srv = impl.createSecureServer({ key, cert }, (req, res) => {
      res.writeHead(200, { 'content-type': 'application/octet-stream' });
      // several writes so the staging buffer is handed over, refilled and handed again
      for (let i = 0; i < 8; i++) {
        res.write(Buffer.alloc(25000, 0x61 + i));
      }
      res.end();
    });
    const port = await listen(srv);
    const client = connectTls(port);
    const answer = await new Promise((resolve, reject) => {
      const req = client.request({ ':method': 'GET', ':path': '/download' });
      const chunks = [];
      let headers = null;
      req.on('response', (h) => {
        headers = h;
      });
      req.on('data', (d) => chunks.push(d));
      req.on('end', () => resolve({ headers, body: Buffer.concat(chunks) }));
      req.on('error', reject);
    });
    const body = answer.body;
    let patternOk = body.length === 200000;
    for (let i = 0; patternOk && i < 8; i++) {
      for (let j = 0; j < 25000; j += 997) {
        if (body[i * 25000 + j] !== 0x61 + i) {
          patternOk = false;
        }
      }
    }
    const out = { status: answer.headers[':status'], length: body.length, patternOk };
    client.destroy();
    srv.close();
    await new Promise((r) => setTimeout(r, 30));
    return out;
  }
  const node = await downloadRun(http2);
  const ours = await downloadRun(h2lean);
  assert.deepStrictEqual(ours, node);
  assert.strictEqual(ours.length, 200000);
  assert.strictEqual(ours.patternOk, true, 'every byte reached the peer in order');
});

test('the h2c server keeps reporting no encryption and no ALPN', async () => {
  const srv = h2lean.createServer();
  const seen = [];
  srv.on('stream', (stream) => {
    seen.push({ encrypted: stream.session.encrypted, alpn: stream.session.alpnProtocol });
    stream.respond({ ':status': 200 });
    stream.end('clear');
  });
  const port = await listen(srv);
  const client = http2.connect(`http://127.0.0.1:${port}`);
  client.on('error', () => {});
  await tlsRequest(client, '/clear');
  assert.deepStrictEqual(seen, [{ encrypted: false, alpn: null }]);
  client.destroy();
  srv.close();
});

// Over TLS a maximum-size frame is 16393 bytes and a record carries at most
// 16384 bytes of plaintext, so most frames arrive in pieces. A frame that
// arrives in pieces reaches the application as the views it arrived in -- there
// is nothing to copy -- and a wrongly elided copy would show up as a chunk that
// does not hold the bytes that were sent for it. Retaining every chunk until the
// body is complete is what makes that visible.
test('retained upload chunks stay intact over TLS', async () => {
  const SIZE = 200000;
  // not periodic at the frame size, or two frames would carry identical bytes
  const byteAt = (i) => (((i * 2654435761) >>> 24) & 0xff);
  const payload = Buffer.allocUnsafe(SIZE);
  for (let i = 0; i < SIZE; i++) {
    payload[i] = byteAt(i);
  }

  const { key, cert } = certificates();
  let result = null;
  const srv = h2lean.createSecureServer({ key, cert }, (req, res) => {
    const kept = [];
    req.on('data', (d) => kept.push(d));
    req.on('end', () => {
      let at = 0;
      let mismatch = -1;
      for (const chunk of kept) {
        for (let i = 0; i < chunk.length; i++) {
          if (chunk[i] !== byteAt(at + i)) {
            mismatch = at + i; break;
          }
        }
        if (mismatch !== -1) {
          break;
        }
        at += chunk.length;
      }
      result = { total: at, mismatch, chunks: kept.length };
      res.writeHead(200, { 'content-type': 'text/plain' });
      res.end('ok');
    });
  });
  const port = await listen(srv);
  const client = connectTls(port);
  const status = await new Promise((resolve, reject) => {
    const req = client.request({ ':method': 'POST', ':path': '/upload' });
    req.on('response', (h) => resolve(h[':status']));
    req.on('error', reject);
    req.end(payload);
  });
  client.destroy();
  srv.close();
  await new Promise((r) => setTimeout(r, 30));

  assert.strictEqual(status, 200);
  assert.strictEqual(result.total, SIZE);
  assert.ok(result.chunks > 4, `expected the body split over frames, got ${result.chunks}`);
  assert.strictEqual(result.mismatch, -1, 'a retained chunk was overwritten by a later frame');
});
