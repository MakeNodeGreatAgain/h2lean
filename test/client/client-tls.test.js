// The client's TLS transport (h2s). Each test runs the same script against
// node:http2's connect() and against h2lite's and compares the recordings, so
// what they assert is "the peer and the application observe the same thing",
// not an expectation picked from the docs. The shapes measured off node
// v26.12.1:
//  * a connect() to an https URL defers the session's handshake work to the
//    socket's 'secureConnect': encrypted/alpnProtocol read undefined before it
//    and are taken off the socket after it, and the session's 'connect' fires
//    after 'secureConnect' (core.js defers setupHandle while
//    "socket.connecting || socket.secureConnecting");
//  * the application's ALPNProtocols list is overwritten with ['h2'] unless an
//    ALPNCallback is given (initializeTLSOptions) -- measured: an app asking
//    for http/1.1 still negotiates h2;
//  * a failed handshake cancels a queued (never-sent) request with
//    ERR_HTTP2_STREAM_CANCEL -- "The pending stream has been canceled (caused
//    by: <cause>)", .cause set (core.js:1254-1260, internal/errors.js:1335) --
//    and then reports the TLS error on the session; 'connect' never fires.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const tls = require('node:tls');
const http2 = require('node:http2');
const h2lite = require('../../index');
const certificates = require('../fixtures-tls');
const { listen } = require('../diff-harness');

async function roundtripRun(impl) {
  const { key, cert } = certificates();
  const srv = impl.createSecureServer({ key, cert }, (req, res) => {
    // the server-side session over this TLS socket negotiated h2 too
    res.writeHead(200, { 'x-alpn': String(req.stream.session.alpnProtocol) });
    res.end('tls body');
  });
  const port = await listen(srv);
  const out = await new Promise((resolve, reject) => {
    const sess = impl.connect(`https://127.0.0.1:${port}`, { rejectUnauthorized: false });
    const said = { events: [] };
    // connect() has returned but the handshake is still running: node's getters
    // read undefined here (setupHandle has not run)
    said.sync = { encrypted: typeof sess.encrypted, alpn: typeof sess.alpnProtocol };
    sess.on('connect', () => {
      said.events.push('connect');
      said.atConnect = {
        encrypted: sess.encrypted,
        alpnProtocol: sess.alpnProtocol,
        socketEncrypted: sess.socket.encrypted === true,
        socketIsTLSSocket: sess.socket instanceof tls.TLSSocket
      };
    });
    sess.on('error', (e) => said.events.push(`session error ${e.code || e.message}`));
    const req = sess.request({ ':path': '/tls-roundtrip' });
    const chunks = [];
    let headers = null;
    req.on('response', (h) => {
      headers = h;
      said.events.push('response');
    });
    req.setEncoding('latin1');
    req.on('data', (d) => chunks.push(d));
    req.on('end', () => {
      said.events.push('end');
      said.answer = {
        status: headers[':status'], alpn: headers['x-alpn'], body: chunks.join('')
      };
      sess.destroy();
      resolve(said);
    });
    req.on('error', reject);
  });
  srv.close();
  await new Promise((r) => setTimeout(r, 30));
  return out;
}

test('an https connect behaves like node', async () => {
  const node = await roundtripRun(http2);
  const ours = await roundtripRun(h2lite);
  assert.deepStrictEqual(ours, node);
  // and the reference says what h2s is supposed to say
  assert.deepStrictEqual(node.sync, { encrypted: 'undefined', alpn: 'undefined' });
  assert.strictEqual(node.atConnect.encrypted, true);
  assert.strictEqual(node.atConnect.alpnProtocol, 'h2');
  assert.strictEqual(node.atConnect.socketEncrypted, true);
  assert.strictEqual(node.atConnect.socketIsTLSSocket, true);
  assert.strictEqual(node.answer.status, 200);
  assert.strictEqual(node.answer.alpn, 'h2');
  assert.strictEqual(node.answer.body, 'tls body');
});

// The application's ALPN list cannot make an h2 client offer http/1.1: the list
// is overwritten with ['h2'] the way initializeTLSOptions overwrites it, so the
// handshake succeeds against an h2-only server regardless.
async function alpnOverrideRun(impl) {
  const { key, cert } = certificates();
  const srv = impl.createSecureServer({ key, cert }, (req, res) => {
    res.writeHead(200); res.end('ok');
  });
  const port = await listen(srv);
  const out = await new Promise((resolve, reject) => {
    const sess = impl.connect(`https://127.0.0.1:${port}`, {
      rejectUnauthorized: false, ALPNProtocols: ['http/1.1']
    });
    const said = { events: [] };
    sess.on('connect', () => said.events.push(`connect alpn=${sess.alpnProtocol}`));
    sess.on('error', (e) => said.events.push(`session error ${e.code || e.message}`));
    const req = sess.request({ ':path': '/' });
    req.on('response', () => said.events.push('response'));
    req.on('data', () => {});
    req.on('end', () => {
      said.events.push('end');
      sess.destroy();
      resolve(said);
    });
    req.on('error', reject);
  });
  srv.close();
  await new Promise((r) => setTimeout(r, 30));
  return out;
}

test('an application-supplied ALPN list cannot make the client offer http/1.1',
  async () => {
    const node = await alpnOverrideRun(http2);
    const ours = await alpnOverrideRun(h2lite);
    assert.deepStrictEqual(ours, node);
    assert.deepStrictEqual(node.events, ['connect alpn=h2', 'response', 'end']);
  });

// A handshake the peer rejects leaves the queued request cancelled, not silent:
// ERR_HTTP2_STREAM_CANCEL with the TLS error as its cause rides the stream, the
// TLS error itself rides the session, 'connect' never fires.
async function failureRun(impl) {
  const { key, cert } = certificates();
  const srv = impl.createSecureServer({ key, cert }, (req, res) => {
    res.writeHead(200); res.end('ok');
  });
  const port = await listen(srv);
  const out = await new Promise((resolve) => {
    // rejectUnauthorized defaults to true: the self-signed fixture is rejected
    const sess = impl.connect(`https://127.0.0.1:${port}`);
    const said = { events: [] };
    sess.on('connect', () => said.events.push('connect'));
    sess.on('error', (e) => said.events.push(`session error ${e.code || e.message}`));
    sess.on('close', () => said.events.push('close'));
    const req = sess.request({ ':path': '/' });
    req.on('response', () => said.events.push('response'));
    req.on('error', (e) => {
      said.events.push(`stream error ${e.code}`);
      said.streamMessage = e.message;
      said.streamCause = e.cause ? e.cause.code : null;
    });
    setTimeout(() => {
      sess.destroy();
      srv.close();
      resolve(said);
    }, 300);
  });
  await new Promise((r) => setTimeout(r, 30));
  return out;
}

test('a failed handshake cancels the pending request the way node does', async () => {
  const node = await failureRun(http2);
  const ours = await failureRun(h2lite);
  assert.deepStrictEqual(ours, node);
  assert.ok(!node.events.includes('connect'), 'connect fired over a broken handshake');
  assert.ok(node.events.includes('stream error ERR_HTTP2_STREAM_CANCEL'),
    `no pending-stream cancellation (${node.events.join(' | ')})`);
  assert.ok(node.streamCause === 'DEPTH_ZERO_SELF_SIGNED_CERT',
    `the TLS rejection is not the cancellation's cause (${node.streamCause})`);
  assert.match(node.streamMessage, /^The pending stream has been canceled/);
});
