// performServerHandshake: a server session over a socket the caller already holds,
// with no server object. The scenarios mirror node's own
// test/parallel/test-http2-perform-server-handshake.js (a stream.duplexPair round
// trip and the double-bind refusal) and extend them with the socket-lifetime
// behaviour measured off node v26.12.1 in tools/probe-handshake.js: 'connect'
// fires at setup with (session, socket), a paused socket's buffered octets are
// drained, a still-connecting socket is waited for, and the getters read what
// node's read. Every case runs against node:http2 and h2lean and compares.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const tls = require('node:tls');
const { Duplex, duplexPair } = require('node:stream');
const nodeHttp2 = require('node:http2');
const h2lean = require('../index');
const certificates = require('./fixtures-tls');

// Runs one scenario against both stacks and compares what each recorded. The
// scenario owns its teardown; a case that never settles is a hung file, so each
// resolves on a terminal event and destroys what it opened.
async function both(scenario) {
  const native = await scenario(nodeHttp2);
  const ours = await scenario(h2lean);
  assert.deepStrictEqual(ours, native);
  return ours;
}

function drain(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let headers = null;
    req.on('response', (h) => {
      headers = h;
    });
    req.on('data', (d) => chunks.push(d));
    req.on('end', () => resolve({ headers, body: Buffer.concat(chunks).toString('latin1') }));
    req.on('error', reject);
  });
}

test('a duplexPair round trip, the getters and the connect arguments match node', async () => {
  await both(async (impl) => {
    const [clientSide, serverSide] = duplexPair();
    const client = impl.connect('http://example.com', { createConnection: () => clientSide });
    client.on('error', () => {});
    const session = impl.performServerHandshake(serverSide);
    const rec = {
      server: session.server,
      connecting: session.connecting,
      encrypted: session.encrypted,
      alpnProtocol: session.alpnProtocol,
      connectArgs: null,
      order: []
    };
    session.on('error', () => {});
    session.on('connect', function (a) {
      // node wraps a non-StreamBase socket (this generic Duplex) in a
      // JSStreamSocket, so the second argument is the wrapper, not serverSide;
      // h2lean hands over the socket itself. Only the first argument's identity
      // and the argument count are comparable across the two stacks here.
      rec.connectArgs = [a === session, arguments.length === 2];
      rec.order.push('connect');
    });
    session.on('stream', (stream, headers) => {
      rec.order.push('stream');
      rec.path = headers[':path'];
      stream.respond({ ':status': 200 });
      stream.end('hi!');
    });
    const req = client.request({ ':path': '/test' });
    const answer = await drain(req);
    rec.status = answer.headers[':status'];
    rec.body = answer.body;
    client.destroy();
    session.destroy();
    return rec;
  });
});

test('binding a socket twice throws ERR_HTTP2_SOCKET_BOUND, like node', async () => {
  await both(async (impl) => {
    const socket = new Duplex({ read() {}, write() {} });
    const session = impl.performServerHandshake(socket);
    session.on('error', () => {});
    let code = null;
    try {
      impl.performServerHandshake(socket);
    } catch (e) {
      code = e.code;
    }
    session.destroy();
    socket.destroy();
    return { code };
  });
});

test('a socket already bound to a client session is refused too', async () => {
  await both(async (impl) => {
    const socket = new Duplex({ read() {}, write() {} });
    const client = impl.connect('http://example.com', { createConnection: () => socket });
    client.on('error', () => {});
    let code = null;
    try {
      impl.performServerHandshake(socket);
    } catch (e) {
      code = e.code;
    }
    client.destroy();
    socket.destroy();
    return { code };
  });
});

test('a paused socket with the request already buffered is drained, connect first', async () => {
  await both(async (impl) => {
    const [clientSide, serverSide] = duplexPair();
    serverSide.pause();
    const client = impl.connect('http://example.com', { createConnection: () => clientSide });
    client.on('error', () => {});
    const req = client.request({ ':path': '/buffered' });
    req.end();
    // let the preface, SETTINGS and HEADERS land in the paused readable buffer
    await new Promise((r) => setTimeout(r, 50));
    const buffered = serverSide.readableLength > 0;
    const session = impl.performServerHandshake(serverSide);
    const rec = { buffered, order: [] };
    session.on('error', () => {});
    session.on('connect', () => rec.order.push('connect'));
    session.on('stream', (stream, headers) => {
      rec.order.push('stream');
      rec.path = headers[':path'];
      stream.respond({ ':status': 200 });
      stream.end('ok');
    });
    const answer = await drain(req);
    rec.status = answer.headers[':status'];
    rec.body = answer.body;
    client.destroy();
    session.destroy();
    return rec;
  });
});

test('a still-connecting socket is waited for, then connects', async () => {
  await both(async (impl) => {
    const rec = { atCall: null, connectArgs: null, after: null };
    await new Promise((resolve) => {
      const srv = net.createServer((sock) => {
        // the accepted peer is a raw socket; this case is about the handshake
        // side's lifetime, not a round trip
        sock.on('error', () => {});
        sock.resume();
      });
      srv.listen(0, '127.0.0.1', () => {
        const socket = net.connect(srv.address().port, '127.0.0.1');
        socket.on('error', () => {});
        const session = impl.performServerHandshake(socket);
        session.on('error', () => {});
        rec.atCall = session.connecting;
        session.on('connect', (a, b) => {
          rec.connectArgs = [a === session, b === socket];
          rec.after = session.connecting;
          session.destroy();
          socket.destroy();
          srv.close(resolve);
        });
      });
    });
    return rec;
  });
});

test('the options validation refuses what node refuses', async () => {
  await both(async (impl) => {
    const socket = new Duplex({ read() {}, write() {} });
    const codes = {};
    const cases = [
      ['optionsNull', null],
      ['optionsArray', []],
      ['optionsNumber', 5],
      ['settingsNull', { settings: null }],
      ['settingsArray', { settings: [] }],
      ['settingsNumber', { settings: 5 }],
      ['strictString', { strictSingleValueFields: 'yes' }]
    ];
    for (const [label, opts] of cases) {
      // a fresh socket per case: a refused handshake never binds, but a successful
      // one would, and the next case must not trip ERR_HTTP2_SOCKET_BOUND
      const sock = label === 'optionsNull' ? socket : new Duplex({ read() {}, write() {} });
      try {
        const s = impl.performServerHandshake(sock, opts);
        s.on('error', () => {});
        codes[label] = 'accepted';
        s.destroy();
      } catch (e) {
        codes[label] = e.code;
      }
      sock.destroy();
    }
    socket.destroy();
    return codes;
  });
});

test('only the asked-for settings are advertised to the peer', async () => {
  await both(async (impl) => {
    const [clientSide, serverSide] = duplexPair();
    const client = impl.connect('http://example.com', { createConnection: () => clientSide });
    client.on('error', () => {});
    const session = impl.performServerHandshake(serverSide, {
      settings: { maxConcurrentStreams: 100, maxFrameSize: 32768 }
    });
    session.on('error', () => {});
    session.on('stream', (stream) => {
      stream.respond({ ':status': 200 }); stream.end();
    });
    const rec = {};
    await new Promise((resolve) => {
      client.on('remoteSettings', (settings) => {
        rec.maxConcurrentStreams = settings.maxConcurrentStreams;
        rec.maxFrameSize = settings.maxFrameSize;
        // the defaults the application did NOT ask for must not be advertised:
        // node leaves them at the peer's own default, not at a value we sent
        rec.headerTableSize = settings.headerTableSize;
        resolve();
      });
      const req = client.request({ ':path': '/' });
      req.on('error', () => {});
      req.resume();
      req.end();
    });
    client.destroy();
    session.destroy();
    return rec;
  });
});

test('a TLS socket negotiated h2 becomes an encrypted session', async () => {
  const { key, cert } = certificates();
  await both(async (impl) => {
    const rec = {};
    await new Promise((resolve, reject) => {
      const srv = tls.createServer({ key, cert, ALPNProtocols: ['h2'] }, (socket) => {
        const session = impl.performServerHandshake(socket);
        session.on('error', () => {});
        rec.encrypted = session.encrypted;
        rec.alpnProtocol = session.alpnProtocol;
        rec.server = session.server;
        session.on('stream', (stream, headers) => {
          rec.path = headers[':path'];
          stream.respond({ ':status': 200 });
          stream.end('tls-ok');
        });
      });
      srv.on('error', reject);
      srv.listen(0, '127.0.0.1', () => {
        const client = impl.connect(`https://127.0.0.1:${srv.address().port}`,
          { rejectUnauthorized: false });
        client.on('error', () => {});
        const req = client.request({ ':path': '/over-tls' });
        drain(req).then((answer) => {
          rec.status = answer.headers[':status'];
          rec.body = answer.body;
          client.destroy();
          srv.close(resolve);
        }).catch(reject);
        req.end();
      });
    });
    return rec;
  });
});
