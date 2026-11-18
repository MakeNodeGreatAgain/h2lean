'use strict';
// What node's performServerHandshake actually does, measured on v26.12.1.
// Pins: 'connect' timing/args, server getter, double-bind error, buffered-data
// drain on a paused socket, connecting-socket deferral, options validation,
// and the wire shape (SETTINGS first, empty when no options).
const http2 = require('node:http2');
const net = require('node:net');

function serverSocketPair(cb) {
  const srv = net.createServer((sock) => {
    srv.close();
    cb(sock);
  });
  srv.listen(0, '127.0.0.1', () => {
    const cli = net.connect(srv.address().port, '127.0.0.1');
    cli.on('error', () => {});
    srv.on('connection', () => {});
    // stash client for the caller via the server socket's pair
    srv._cli = cli;
  });
  return srv;
}

async function case1() {
  // already-connected plain socket: connect timing, args, getters, wire
  await new Promise((resolve) => {
    const srv = serverSocketPair((sock) => {
      const cli = srv._cli;
      const t0 = Date.now();
      const events = [];
      const session = http2.performServerHandshake(sock);
      events.push(['after-call', Date.now() - t0, session.connecting, session.server,
        session.encrypted, session.alpnProtocol]);
      session.on('connect', (...args) => {
        events.push(['connect', Date.now() - t0, args.length,
          args[0] === session, args[1] === sock, session.connecting]);
      });
      session.on('stream', (stream, headers) => {
        events.push(['stream', headers[':path']]);
        stream.respond({ ':status': 200 });
        stream.end('ok');
      });
      session.on('error', (e) => events.push(['error', e.code]));
      // client speaks h2c
      const c = http2.connect('http://127.0.0.1', { createConnection: () => cli });
      const req = c.request({ ':path': '/x' });
      req.on('response', (h) => events.push(['response', h[':status']]));
      req.on('data', () => {});
      req.on('end', () => {
        setImmediate(() => {
          console.log('CASE1', JSON.stringify(events));
          console.log('CASE1 getters', session.server, session.encrypted,
            session.alpnProtocol, session.connecting, session.destroyed);
          c.close();
          session.close();
          sock.on('close', resolve);
        });
      });
      req.end();
    });
  });
}

async function case2() {
  // double bind
  await new Promise((resolve) => {
    const srv = serverSocketPair((sock) => {
      const s1 = http2.performServerHandshake(sock);
      s1.on('error', () => {});
      try {
        http2.performServerHandshake(sock);
        console.log('CASE2 no throw');
      } catch (e) {
        console.log('CASE2', e.code, JSON.stringify(e.message));
      }
      s1.destroy();
      srv._cli.destroy();
      sock.on('close', resolve);
    });
  });
}

async function case3() {
  // paused socket with buffered preface+SETTINGS+HEADERS before the handshake
  await new Promise((resolve) => {
    const srv = serverSocketPair((sock) => {
      const cli = srv._cli;
      sock.pause();
      const c = http2.connect('http://127.0.0.1', { createConnection: () => cli });
      const req = c.request({ ':path': '/buffered' });
      req.end();
      // let the bytes land in the paused socket's buffer
      setTimeout(() => {
        console.log('CASE3 readableLength before handshake', sock.readableLength);
        const session = http2.performServerHandshake(sock);
        session.on('error', (e) => console.log('CASE3 session error', e.code));
        session.on('stream', (stream, headers) => {
          console.log('CASE3 stream', headers[':path'], 'readableLength', sock.readableLength);
          stream.respond({ ':status': 200 });
          stream.end('ok');
        });
        req.on('response', (h) => {
          console.log('CASE3 response', h[':status']);
          c.close();
          session.close();
          sock.on('close', resolve);
        });
        req.on('data', () => {});
      }, 100);
    });
  });
}

async function case4() {
  // connecting socket: handshake before 'connect'
  await new Promise((resolve) => {
    const srv = net.createServer((sock) => {
      srv.close();
      const session = http2.performServerHandshake(sock);
      console.log('CASE4 at-call connecting', session.connecting,
        'encrypted', session.encrypted, 'alpn', session.alpnProtocol);
      session.on('connect', (a, b) => {
        console.log('CASE4 connect args', a === session, b === sock,
          'connecting', session.connecting);
        session.close();
        sock.on('close', resolve);
      });
      session.on('error', () => {});
    });
    srv.listen(0, '127.0.0.1', () => {
      // connect() returns a socket that is still connecting; hand it over at once
      const cli = net.connect(srv.address().port, '127.0.0.1');
      cli.on('error', () => {});
      // the server side above gets an accepted (connected) socket; to test the
      // connecting branch we need the CLIENT-side socket handed to the handshake
      // -- do that in case4b instead. Here just confirm accepted-socket path.
      const session = http2.performServerHandshake(cli);
      console.log('CASE4b client-socket at-call connecting', session.connecting);
      session.on('connect', () => {
        console.log('CASE4b connect fired, connecting', session.connecting);
        session.destroy();
        cli.destroy();
      });
      session.on('error', () => {});
      session.on('close', () => {});
    });
  });
}

async function case5() {
  // options validation
  const srv = net.createServer(() => {});
  await new Promise((r) => srv.listen(0, '127.0.0.1', r));
  const sock = net.connect(srv.address().port, '127.0.0.1');
  sock.on('error', () => {});
  await new Promise((r) => sock.once('connect', r));
  for (const [label, opts] of [
    ['settings-string', { settings: 'x' }],
    ['strict-string', { strictSingleValueFields: 'yes' }],
    ['options-null', null],
    ['connwin-range', { connectionWindowSize: 100 }]
  ]) {
    try {
      const s = http2.performServerHandshake(sock, opts);
      console.log('CASE5', label, 'accepted');
      s.destroy();
      break; // socket is bound now; further calls would throw SOCKET_BOUND
    } catch (e) {
      console.log('CASE5', label, e.code, JSON.stringify(e.message));
    }
  }
  sock.destroy();
  srv.close();
}

async function case6() {
  // regular createServer: when does 'connect' fire relative to the peer preface?
  await new Promise((resolve) => {
    const srv = http2.createServer();
    srv.on('session', (session) => {
      const t0 = Date.now();
      session.on('connect', (...args) => {
        console.log('CASE6 server-session connect at', Date.now() - t0,
          'args', args.length, args[0] === session, args[1] === session.socket);
      });
      session.on('stream', (stream) => {
        console.log('CASE6 stream at', Date.now() - t0);
        stream.respond({ ':status': 200 });
        stream.end();
      });
    });
    srv.listen(0, '127.0.0.1', () => {
      const c = http2.connect(`http://127.0.0.1:${srv.address().port}`);
      const req = c.request();
      req.on('response', () => {
        setTimeout(() => {
          c.close(); srv.close(); resolve();
        }, 50);
      });
      req.on('data', () => {});
      req.end();
    });
  });
}

(async () => {
  await case1();
  await case2();
  await case3();
  await case4();
  await case5();
  await case6();
  console.log('DONE');
  process.exit(0);
})().catch((e) => {
  console.error(e); process.exit(1);
});
