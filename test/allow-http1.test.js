// The allowHTTP1 / http1Options fallback on the h2s server. Each case runs the same
// script against node:http2's createSecureServer and against h2lean's and compares the
// recordings, so the assertions are node's measured behaviour, not a reading of the
// docs. The reference facts were probed off node 26: with allowHTTP1 the ALPN list
// gains 'http/1.1', one 'request' handler serves both protocols, and a client that
// offers no ALPN at all goes to the fallback too (it is not h2 prior knowledge).
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const tls = require('node:tls');
const http = require('node:http');
const http2 = require('node:http2');
const h2lean = require('../index');
const certificates = require('./fixtures-tls');
const { listen, closeServer } = require('./diff-harness');

// One HTTP/1.1 request over a raw TLS socket. The response is summarised so that the
// per-run noise (the date header, the port) does not reach the comparison. The request
// carries Connection: close unless asked otherwise, so the server closes the socket
// and the read ends on its own.
function h1Request(port, tlsOptions, requestText) {
  return new Promise((resolve, reject) => {
    const sock = tls.connect({
      host: '127.0.0.1', port, rejectUnauthorized: false, ...tlsOptions
    });
    const chunks = [];
    sock.on('data', (d) => chunks.push(d));
    sock.on('error', reject);
    sock.on('close', () => resolve({ alpn: String(sock.alpnProtocol), raw: Buffer.concat(chunks).toString('latin1') }));
    sock.on('secure', () => {
      sock.write(requestText ||
        'GET /both HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n');
    });
  });
}

function summarize(raw) {
  const [head, ...rest] = raw.split('\r\n\r\n');
  const lines = head.split('\r\n');
  const headers = {};
  for (const line of lines.slice(1)) {
    const i = line.indexOf(':');
    headers[line.slice(0, i).toLowerCase()] = line.slice(i + 1).trim();
  }
  return {
    statusLine: lines[0],
    body: rest.join('\r\n\r\n'),
    hasDate: headers.date !== undefined,
    connection: headers.connection
  };
}

// Reads for a bounded time and reports what arrived; for the cases where the peer is
// allowed to answer with nothing (unknownProtocol with a listener). The client socket
// is always destroyed at the end so the server side does not keep the file alive.
function readBounded(port, tlsOptions, requestText, ms) {
  return new Promise((resolve) => {
    const sock = tls.connect({
      host: '127.0.0.1', port, rejectUnauthorized: false, ...tlsOptions
    });
    const chunks = [];
    let done = false;
    const finish = () => {
      if (done) {
        return;
      }
      done = true;
      sock.destroy();
      resolve(Buffer.concat(chunks).toString('latin1'));
    };
    const timer = setTimeout(finish, ms);
    sock.on('data', (d) => chunks.push(d));
    sock.on('error', () => {});
    sock.on('close', () => {
      clearTimeout(timer);
      finish();
    });
    sock.on('secure', () => {
      if (requestText) {
        sock.write(requestText);
      }
    });
  });
}

function h2Get(port, path_) {
  return new Promise((resolve, reject) => {
    const client = http2.connect(`https://127.0.0.1:${port}`, { rejectUnauthorized: false });
    client.on('error', () => {});
    const req = client.request({ ':method': 'GET', ':path': path_ });
    const chunks = [];
    let headers = null;
    req.on('response', (h) => {
      headers = h;
    });
    req.setEncoding('latin1');
    req.on('data', (d) => chunks.push(d));
    req.on('end', () => {
      client.destroy();
      resolve({ status: headers[':status'], body: chunks.join('') });
    });
    req.on('error', reject);
    req.end();
  });
}

// One handler serves an HTTP/1.1 request (ALPN-negotiated), a no-ALPN request and an
// HTTP/2 request, in that order, and records what it saw of each.
async function fallbackRun(impl) {
  const { key, cert } = certificates();
  const seen = [];
  const srv = impl.createSecureServer({ key, cert, allowHTTP1: true }, (req, res) => {
    seen.push({ h1: req.httpVersion === '1.1', httpVersion: req.httpVersion, url: req.url });
    res.end(`protocol-${req.httpVersion}`);
  });
  const port = await listen(srv);
  const viaAlpn = summarize((await h1Request(port, { ALPNProtocols: ['http/1.1'] })).raw);
  const noAlpn = summarize((await h1Request(port, {})).raw);
  const overH2 = await h2Get(port, '/over-h2');
  const out = { seen, viaAlpn, noAlpn, overH2 };
  await closeServer(srv);
  return out;
}

test('allowHTTP1: one request handler serves both protocols like node', async () => {
  const node = await fallbackRun(http2);
  const ours = await fallbackRun(h2lean);
  assert.deepStrictEqual(ours, node);
  assert.deepStrictEqual(node.seen, [
    { h1: true, httpVersion: '1.1', url: '/both' },
    { h1: true, httpVersion: '1.1', url: '/both' },
    { h1: false, httpVersion: '2.0', url: '/over-h2' }
  ]);
  assert.strictEqual(node.viaAlpn.statusLine, 'HTTP/1.1 200 OK');
  assert.strictEqual(node.viaAlpn.body, 'protocol-1.1');
  assert.strictEqual(node.noAlpn.body, 'protocol-1.1', 'a client with no ALPN is served too');
  assert.strictEqual(node.overH2.status, 200);
  assert.strictEqual(node.overH2.body, 'protocol-2.0');
});

// Without allowHTTP1 the no-ALPN connection must never become a session on either
// stack: node emits 'unknownProtocol' and answers the 403 text when nobody listens.
async function unknownRun(impl, withListener) {
  const { key, cert } = certificates();
  // a small timeout so a socket that lingers on the unknown-protocol path cannot hold
  // either the test or the event loop; node accepts the option the same way
  const srv = impl.createSecureServer({ key, cert, unknownProtocolTimeout: 100 });
  const events = [];
  if (withListener) {
    // the handler owns the socket on both stacks (node installs no timeout once a
    // listener exists, and a socket nobody touches stays paused, holding close()) --
    // so the handler destroys it, the way an application that does not want the
    // connection would
    srv.on('unknownProtocol', (socket) => {
      events.push('unknownProtocol');
      socket.destroy();
    });
  }
  srv.on('session', () => events.push('session'));
  const port = await listen(srv);
  const raw = await readBounded(port, {}, 'GET /no-alpn HTTP/1.1\r\nHost: x\r\n\r\n', 300);
  await new Promise((r) => setTimeout(r, 60));
  const out = { events: [...events], raw };
  await closeServer(srv);
  return out;
}

test('no ALPN without allowHTTP1: unknownProtocol event, else the 403 text', async () => {
  const withNode = await unknownRun(http2, true);
  const withOurs = await unknownRun(h2lean, true);
  assert.deepStrictEqual(withOurs, withNode);
  assert.deepStrictEqual(withNode.events, ['unknownProtocol']);
  assert.strictEqual(withNode.raw, '', 'a handled unknown-protocol socket gets no bytes');

  const noneNode = await unknownRun(http2, false);
  const noneOurs = await unknownRun(h2lean, false);
  assert.deepStrictEqual(noneOurs, noneNode);
  assert.ok(noneNode.raw.startsWith('HTTP/1.0 403 Forbidden'), `got: ${noneNode.raw.slice(0, 40)}`);
  assert.ok(noneNode.raw.includes('allowHTTP1'), 'the text is node\'s own, not a bare 403');
});

// http1Options reaches the fallback server; the deprecated top-level class names
// (DEP0202) migrate into it the same way they do on node.
class MyIncomingMessage extends http.IncomingMessage {}

// Whether the host's own node:http2 honours the http1Options bag. The bag does not
// exist before node 23: on 20/22 _http_server's storeHTTPOptions() reads only the
// top-level options.IncomingMessage/ServerResponse (read out of those binaries in
// this session), and internal/http2/core has no http1Options string at all. The
// differential comparison below pins node's measured answer, so the host must be
// one that has the option; probed, not version-tabled, per CONTRIBUTING §1.
async function hostHonoursHttp1Options() {
  const { key, cert } = certificates();
  class Marker extends http.IncomingMessage {}
  const srv = http2.createSecureServer({
    key, cert, allowHTTP1: true, http1Options: { IncomingMessage: Marker }
  }, (req, res) => res.end(String(req instanceof Marker)));
  const port = await listen(srv);
  const raw = (await h1Request(port, { ALPNProtocols: ['http/1.1'] })).raw;
  await closeServer(srv);
  return summarize(raw).body === 'true';
}

async function imRun(impl, via) {
  const { key, cert } = certificates();
  const opts = { key, cert, allowHTTP1: true };
  if (via === 'http1Options') {
    opts.http1Options = { IncomingMessage: MyIncomingMessage };
  } else {
    opts.Http1IncomingMessage = MyIncomingMessage;
  }
  const srv = impl.createSecureServer(opts, (req, res) => {
    res.end(String(req instanceof MyIncomingMessage));
  });
  const port = await listen(srv);
  const raw = (await h1Request(port, { ALPNProtocols: ['http/1.1'] })).raw;
  await closeServer(srv);
  return { isMyIM: summarize(raw).body };
}

test('http1Options.IncomingMessage (and the DEP0202 name) reaches the fallback', async () => {
  const hostSupportsBag = await hostHonoursHttp1Options();
  for (const via of ['http1Options', 'dep0202']) {
    if (via === 'http1Options' && !hostSupportsBag) {
      // No node answer to compare against on this host (20/22 ignore the bag);
      // h2lean's promise is still assertable, against the node 26 probe that
      // produced the 'true' this file pins elsewhere.
      const ours = await imRun(h2lean, via);
      assert.strictEqual(ours.isMyIM, 'true', via);
      continue;
    }
    const node = await imRun(http2, via);
    const ours = await imRun(h2lean, via);
    assert.deepStrictEqual(ours, node, via);
    assert.strictEqual(node.isMyIM, 'true', via);
  }
});

// close() with the fallback live: an idle keep-alive connection is shut down instead
// of holding the close, and a response already in flight is allowed to finish.
async function closeIdleRun(impl) {
  const { key, cert } = certificates();
  const srv = impl.createSecureServer({ key, cert, allowHTTP1: true }, (req, res) => {
    res.end('one');
  });
  const port = await listen(srv);
  const first = (await h1Request(port, { ALPNProtocols: ['http/1.1'] },
    'GET /warm HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n')).raw;
  // the socket above used Connection: close; open a keep-alive one and let it sit
  const idleClosed = new Promise((resolve) => {
    const sock = tls.connect({
      host: '127.0.0.1', port, rejectUnauthorized: false, ALPNProtocols: ['http/1.1']
    });
    sock.on('error', () => {});
    sock.on('secure', () => sock.write('GET /parked HTTP/1.1\r\nHost: x\r\n\r\n'));
    sock.on('data', () => {});
    sock.on('close', resolve);
  });
  await new Promise((r) => setTimeout(r, 80));
  const closeReturned = new Promise((resolve) => srv.close(() => resolve(true)));
  const closed = await Promise.race([
    idleClosed.then(() => true),
    new Promise((r) => setTimeout(() => r(false), 400))
  ]);
  const out = {
    warm: summarize(first).body,
    idleClosedWithin400ms: closed,
    closeCallback: await closeReturned
  };
  await closeServer(srv);
  return out;
}

test('close() shuts an idle keep-alive HTTP/1 connection down', async () => {
  const node = await closeIdleRun(http2);
  const ours = await closeIdleRun(h2lean);
  assert.deepStrictEqual(ours, node);
  assert.strictEqual(node.warm, 'one');
  assert.strictEqual(node.idleClosedWithin400ms, true, 'the parked socket was released');
  assert.strictEqual(node.closeCallback, true);
});

async function closeActiveRun(impl) {
  const { key, cert } = certificates();
  const srv = impl.createSecureServer({ key, cert, allowHTTP1: true }, (req, res) => {
    setTimeout(() => res.end('slow-ok'), 150);
  });
  const port = await listen(srv);
  const answer = new Promise((resolve) => {
    const sock = tls.connect({
      host: '127.0.0.1', port, rejectUnauthorized: false, ALPNProtocols: ['http/1.1']
    });
    const chunks = [];
    sock.on('data', (d) => {
      chunks.push(d);
      // the response is complete: leave the keep-alive socket before closing the
      // server, so neither stack has a lingering connection to drain on close()
      if (Buffer.concat(chunks).toString('latin1').includes('slow-ok')) {
        resolve(Buffer.concat(chunks).toString('latin1'));
        sock.destroy();
      }
    });
    sock.on('error', () => {});
    sock.on('close', () => resolve(Buffer.concat(chunks).toString('latin1')));
    sock.on('secure', () => sock.write('GET /slow HTTP/1.1\r\nHost: x\r\n\r\n'));
    setTimeout(() => sock.destroy(), 1000);
  });
  await new Promise((r) => setTimeout(r, 60));
  srv.close();
  const out = { body: summarize(await answer).body };
  await new Promise((r) => setTimeout(r, 100));
  await closeServer(srv);
  return out;
}

test('a response in flight during close() still reaches the peer', async () => {
  const node = await closeActiveRun(http2);
  const ours = await closeActiveRun(h2lean);
  assert.deepStrictEqual(ours, node);
  assert.strictEqual(node.body, 'slow-ok');
});

// The ALPN verdict itself: with allowHTTP1 the handshake accepts http/1.1, so the
// no_application_protocol refusal that the default server produces disappears.
async function acceptedRun(impl) {
  const { key, cert } = certificates();
  const srv = impl.createSecureServer({ key, cert, allowHTTP1: true }, (req, res) => {
    res.end('ok');
  });
  const port = await listen(srv);
  const answer = await h1Request(port, { ALPNProtocols: ['http/1.1'] });
  await closeServer(srv);
  return { alpn: answer.alpn, statusLine: summarize(answer.raw).statusLine };
}

test('allowHTTP1 lets the handshake negotiate http/1.1', async () => {
  const node = await acceptedRun(http2);
  const ours = await acceptedRun(h2lean);
  assert.deepStrictEqual(ours, node);
  assert.strictEqual(node.alpn, 'http/1.1');
  assert.strictEqual(node.statusLine, 'HTTP/1.1 200 OK');
});
