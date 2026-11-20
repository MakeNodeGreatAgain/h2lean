// Differential test for TODO 4.1: session.socket as node's safe Proxy (core.js handler).
//
// h2lean's public `socket` accessor must match node's *measured* behaviour, so every case runs
// the identical probe against `node:http2` and against h2lean and asserts the two transcripts
// are equal -- nothing here pins h2lean's own behaviour (AGENTS.md §6).
//
// Scope note: this is the session-level proxy (refuses nine socket methods). node's compat
// req.socket is a DIFFERENT proxy (over the stream, refuses four) and is not covered here.
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const nodeHttp2 = require('node:http2');
const h2lean = require('../index');
const { SOCKET_DESTROY_ROUTED } = require('./node-version');

// h2lean routes socket.destroy beside setTimeout/ref/unref, the node 24+ shape; 20/22
// list destroy among the throwing properties, so the probe's read would throw there.
const SKIP = SOCKET_DESTROY_ROUTED ? false : 'socket.destroy routed only from node 24 (test/node-version.js)';

const THROWING = ['write', 'end', 'pause', 'resume', 'read', 'emit',
  'setEncoding', 'setKeepAlive', 'setNoDelay'];
const ROUTED = ['destroy', 'setTimeout', 'ref', 'unref'];

// Accessing a refused prop throws inside the Proxy's get trap -- it is the read, not the call,
// that node rejects -- so probe the property, never invoke it.
function probeProp(obj, name) {
  try {
    void obj[name];
    return 'no-throw';
  } catch (e) {
    return e.code;
  }
}

function surface(socket) {
  return {
    present: socket !== null && socket !== undefined,
    remoteAddressType: typeof socket.remoteAddress,
    readableType: typeof socket.readable,
    bufferSizeType: typeof socket.bufferSize,
    instanceofSocket: socket instanceof net.Socket,
    throwProps: THROWING.map((p) => probeProp(socket, p)),
    routedFn: ROUTED.map((p) => typeof socket[p])
  };
}

function listen(server) {
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve(server.address().port));
  });
}

// Capture the server Http2Session via the 'session' event and read its .socket while live.
function sessionSurface(lib) {
  return new Promise((resolve, reject) => {
    const server = lib.createServer((req, res) => {
      res.end('x');
    });
    let captured;
    server.on('session', (session) => {
      captured = session;
    });
    listen(server).then((port) => {
      const client = nodeHttp2.connect(`http://127.0.0.1:${port}`);
      client.on('error', reject);
      const req = client.request({ ':method': 'GET', ':path': '/' });
      req.on('end', () => {
        resolve(surface(captured.socket));
        client.close(); server.close();
      });
      req.resume();
      req.end();
    });
  });
}

test('server session.socket proxy surface matches node', { skip: SKIP }, async () => {
  const fromNode = await sessionSurface(nodeHttp2);
  const fromH2lean = await sessionSurface(h2lean);
  assert.deepStrictEqual(fromH2lean, fromNode);
  // Guard against a vacuous match: node's reference must refuse the nine and route the four.
  assert.strictEqual(fromNode.present, true);
  assert.strictEqual(fromNode.instanceofSocket, true);
  assert.strictEqual(fromNode.remoteAddressType, 'string');
  assert.deepStrictEqual(fromNode.throwProps,
    THROWING.map(() => 'ERR_HTTP2_NO_SOCKET_MANIPULATION'));
  assert.deepStrictEqual(fromNode.routedFn, ROUTED.map(() => 'function'));
});

// The client session runs the same handler; drive a paired client and read client.socket.
function clientSurface(lib) {
  return new Promise((resolve, reject) => {
    const server = lib.createServer((req, res) => {
      res.end('x');
    });
    listen(server).then((port) => {
      const client = lib.connect(`http://127.0.0.1:${port}`);
      client.on('error', reject);
      const req = client.request({ ':method': 'GET', ':path': '/' });
      req.on('response', () => {
        resolve(surface(client.socket));
        client.close(); server.close();
      });
      req.on('error', reject);
      req.resume();
      req.end();
    });
  });
}

test('client session.socket proxy surface matches node', { skip: SKIP }, async () => {
  const fromNode = await clientSurface(nodeHttp2);
  const fromH2lean = await clientSurface(h2lean);
  assert.deepStrictEqual(fromH2lean, fromNode);
  assert.deepStrictEqual(fromNode.throwProps,
    THROWING.map(() => 'ERR_HTTP2_NO_SOCKET_MANIPULATION'));
});

// session.socket reads undefined once the session has closed -- node's cleanupSession nulls the
// proxy, so the surface must not keep a live-looking socket around a dead session.
function closedSocketState(lib) {
  return new Promise((resolve, reject) => {
    const server = lib.createServer((req, res) => {
      res.end('x');
    });
    let captured;
    server.on('session', (session) => {
      captured = session;
    });
    listen(server).then((port) => {
      const client = nodeHttp2.connect(`http://127.0.0.1:${port}`);
      client.on('error', reject);
      const req = client.request({ ':method': 'GET', ':path': '/' });
      req.on('end', () => {
        captured.close();
        captured.on('close', () => {
          resolve(captured.socket === undefined ? 'undefined' : 'defined');
          client.close(); server.close();
        });
      });
      req.resume();
      req.end();
    });
  });
}

test('session.socket is gone after close, as on node', async () => {
  const fromNode = await closedSocketState(nodeHttp2);
  const fromH2lean = await closedSocketState(h2lean);
  assert.strictEqual(fromNode, 'undefined');
  assert.strictEqual(fromH2lean, fromNode);
});
