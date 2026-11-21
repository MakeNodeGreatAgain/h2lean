// Port of node's `test/parallel/test-http2-sensitive-headers.js` (CONTRIBUTING.md §1: the
// upstream file is a scenario source, node's measured value is the expectation).
//
// Upstream's two blocks, in order:
//
//  1. The server marks response fields on the headers object itself --
//     `[http2.sensitiveHeaders]: ['Sensitive']` on the object passed to respond(). The
//     peer must read the marked names back under its own sensitiveHeaders symbol. The
//     list is matched case-insensitively ('Sensitive' marks the 'sensitive' line), and
//     it arrives as `['cookie', 'sensitive']`, not `['sensitive']`: the 'cookie' entry
//     is unmarked, but nghttp2's deflater sends authorization always and a cookie whose
//     value is under 20 octets as never-indexed literals on its own ("the same criteria
//     used in Firefox codebase", lib/nghttp2_hd.c deflate_nv), so the receiving side
//     reports it. h2lean's encoder reproduces that rule; the client below is node's
//     http2 client on both arms because the wire encoding of the server is what is
//     under test (h2lean's own inbound reporting is pinned by test/sensitive-headers.test.js).
//
//  2. The client marks request fields by hanging the symbol on the headers param -- the
//     flat-array form with a symbol property. The server must receive the marked name
//     in its sensitiveHeaders list, and `req.sentHeaders` must be the null-prototype
//     map with the four pseudo-headers request() resolved plus the symbol carried
//     through from the caller's array.
//
// Adaptations: the duplexPair + `server.emit('connection', ...)` transport becomes a real
// TCP connection (the transport is not what is being compared); `common.mustCall` becomes
// promise resolution; upstream's sentHeaders assertion is synchronous right after
// request() and stays there. :authority is recorded as the live host:port -- each arm
// binds its own port, upstream's 'localhost:80' is the connect authority of its own
// duplex pair.
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const h2lean = require('../../index');
const { listen, closeServer } = require('../diff-harness');

const KS = http2.sensitiveHeaders;

// Upstream block 1. One server arm at a time; the client is node's on both, so the
// assertion reads what node's nghttp2 decoder received off each stack's wire.
async function markedResponseArm(serverLib) {
  const srv = serverLib.createServer();
  const port = await listen(srv);
  try {
    await new Promise((resolve, reject) => {
      srv.on('stream', (stream) => {
        stream.respond({
          'content-type': 'text/html',
          ':status': 200,
          'cookie': 'donotindex',
          'not-sensitive': 'foo',
          'sensitive': 'bar',
          // sensitiveHeaders entries are case-insensitive (upstream's own comment)
          [KS]: ['Sensitive']
        });
        stream.end('<h1>Hello World</h1>');
      });
      const client = http2.connect(`http://127.0.0.1:${port}`);
      client.on('error', reject);
      const req = client.request({ ':path': '/' });
      req.on('response', (headers) => {
        try {
          assert.strictEqual(headers[':status'], 200);
          assert.strictEqual(headers.cookie, 'donotindex');
          assert.deepStrictEqual(headers[KS], ['cookie', 'sensitive']);
          resolve();
        } catch (err) {
          reject(err);
        }
      });
      req.on('error', reject);
      req.resume();
      req.on('end', () => client.close());
    });
  } finally {
    await closeServer(srv);
  }
}

// Upstream block 2, over every server/client pair: the mark has to survive the
// client's encoder on both stacks, the server's report on both stacks, and
// sentHeaders is a client-side getter with a stack-independent shape.
async function markedRequestArm(serverLib, clientLib) {
  const srv = serverLib.createServer();
  const port = await listen(srv);
  try {
    await new Promise((resolve, reject) => {
      srv.on('stream', (stream, headers) => {
        try {
          assert.deepStrictEqual(headers[KS], ['secret']);
          stream.respond({ ':status': 200 });
          stream.end();
        } catch (err) {
          reject(err);
        }
      });
      const client = clientLib.connect(`http://127.0.0.1:${port}`);
      client.on('error', reject);
      const rawHeaders = [
        ':path', '/',
        'secret', 'secret-value'
      ];
      rawHeaders[KS] = ['secret'];

      const req = client.request(rawHeaders);

      try {
        assert.deepStrictEqual(req.sentHeaders, {
          __proto__: null,
          ':method': 'GET',
          ':authority': `127.0.0.1:${port}`,
          ':scheme': 'http',
          ':path': '/',
          'secret': 'secret-value',
          [KS]: ['secret']
        });
      } catch (err) {
        reject(err);
        return;
      }

      req.on('error', reject);
      req.on('close', () => {
        client.close();
        resolve();
      });
      req.resume();
      req.end();
    });
  } finally {
    await closeServer(srv);
  }
}

test('upstream: a marked response list arrives never-indexed on a node client', async () => {
  await markedResponseArm(http2);
  await markedResponseArm(h2lean);
});

test('upstream: a marked request list is honored on the wire and in sentHeaders', async () => {
  for (const serverLib of [http2, h2lean]) {
    for (const clientLib of [http2, h2lean]) {
      await markedRequestArm(serverLib, clientLib);
    }
  }
});
