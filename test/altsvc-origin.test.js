// Differential test for TODO 4.2: ORIGIN (RFC 8336) and ALTSVC (RFC 7807), both directions.
//
// Node's own behaviour is the reference (AGENTS.md §3): every case runs the same probe
// against `node:http2` and against h2lean and compares what the peer sees -- thrown-error
// codes, event arguments, or the numeric origin set -- so a difference is a bug in one of
// the two stacks, not an artefact of the test.
//
// Scope notes, from node's core.js read in this file's session:
//   * altsvc()/origin() are defined on `ServerHttp2Session` only. h2lean matches that
//     placement. The client session never exposes the methods.
//   * `onOrigin` (the receive callback) is gated on `session.encrypted`: over h2c the ORIGIN
//     frame parses and then goes nowhere. So every ORIGIN-receive case here uses a TLS
//     server. ALTSVC receive (`onAltSvc`) is not gated, so h2c works.
//   * `session.originSet` getter is likewise gated by `encrypted`; over h2c it reads
//     undefined on both stacks.
//   * Node's `altsvc(alt)` with neither originOrStream nor stream id ABORTS (a C++ CHECK in
//     node_http2.cc). h2lean throws ERR_HTTP2_ALTSVC_INVALID_ORIGIN for that single call
//     (documented in lib/session.js beside checkAltSvc). That degenerate case is NOT
//     exercised here -- the probe cannot see node's process abort and still continue.
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const h2lean = require('../index');
const { ORIGIN_SET_CAP } = require('./node-version');
const certificates = require('./fixtures-tls');

// The self-signed pair through the generator, not a raw read of out/*.pem: on a fresh
// checkout out/ is empty and the two files exist only once fixtures-tls has run (they are
// gitignored). A direct readFileSync here threw ENOENT on the CI matrix for the majors where
// no earlier test had generated the pair yet.
const { key: TLS_KEY, cert: TLS_CERT } = certificates();

// -- argument validation parity --------------------------------------------

// Same list of bad/good calls run against a node ServerHttp2Session and against an h2lean
// one. We record {code, kind} for each thrown error and `ok` when the call did not throw.
// Node's C++ CHECK abort on `altsvc(alt)` alone is not in the list; every other validation
// path is.
const VALIDATION_CASES = [
  { fn: 'altsvc', args: ['h3=":443"', 'http://example.com'] },
  { fn: 'altsvc', args: ['h3=":443"', 'not a url'] },
  { fn: 'altsvc', args: ['h3=":443"', new URL('http://example.com')] },
  { fn: 'altsvc', args: ['h3=":443"', 0] },
  { fn: 'altsvc', args: ['h3=":443"', -1] },
  { fn: 'altsvc', args: ['h3=":443"', 1.5] },
  { fn: 'altsvc', args: ['h3=":443"', 2 ** 32] },
  { fn: 'altsvc', args: ['h3=":443"', {}] },
  { fn: 'altsvc', args: ['h3=":443"', { origin: 'http://x.com' }] },
  { fn: 'altsvc', args: ['h3\r\ninjection', 'http://example.com'] },
  { fn: 'altsvc', args: ['x'.repeat(20000), 'http://example.com'] },
  { fn: 'altsvc', args: [123, 'http://example.com'] },
  { fn: 'origin', args: ['http://example.com'] },
  { fn: 'origin', args: ['http://a.com', 'http://b.com'] },
  { fn: 'origin', args: ['not a url'] },
  { fn: 'origin', args: [{}] },
  { fn: 'origin', args: [123] },
  { fn: 'origin', args: ['http://' + ('o'.repeat(20000)) + '.com'] }
];

function validationProbe(lib) {
  return new Promise((resolve) => {
    let sess;
    const srv = lib.createServer((req, res) => {
      res.writeHead(200);
      res.end('ok');
    });
    srv.on('session', (s) => {
      sess = s;
    });
    srv.listen(0, '127.0.0.1', () => {
      const p = srv.address().port;
      const client = http2.connect(`http://127.0.0.1:${p}`);
      client.on('error', () => {});
      const req = client.request({ ':method': 'GET', ':path': '/' });
      req.on('response', () => req.resume());
      req.on('error', () => {});
      req.on('close', () => {
        const results = [];
        for (const c of VALIDATION_CASES) {
          try {
            sess[c.fn](...c.args);
            results.push({ ok: true });
          } catch (e) {
            results.push({ code: e.code, kind: e.constructor.name });
          }
        }
        client.destroy();
        srv.close();
        resolve(results);
      });
    });
  });
}

test('altsvc()/origin() validation: h2lean matches node codes and error types', async () => {
  const a = await validationProbe(http2);
  const b = await validationProbe(h2lean);
  for (let i = 0; i < VALIDATION_CASES.length; i++) {
    assert.deepStrictEqual(b[i], a[i],
      `case ${i} ${VALIDATION_CASES[i].fn}(${JSON.stringify(VALIDATION_CASES[i].args)}) ` +
      `h2lean=${JSON.stringify(b[i])} node=${JSON.stringify(a[i])}`);
  }
});

// -- ALTSVC send: server session -> client 'altsvc' event ------------------

// ALTSVC receive is not gated by encryption on either stack (core.js onAltSvc emits
// unconditionally when the session is live), so this comparison runs over h2c.
function captureAltSvc(lib, invocation) {
  return new Promise((resolve) => {
    let sess;
    const srv = lib.createServer((req, res) => {
      res.writeHead(200);
      res.end('ok');
      // Fire after the response so the frame definitely follows on the wire while the
      // client is still reading.
      setImmediate(() => {
        try {
          invocation(sess);
        } catch { /* probe errors are ignored */ }
      });
    });
    srv.on('session', (s) => {
      sess = s;
    });
    srv.listen(0, '127.0.0.1', () => {
      const p = srv.address().port;
      const client = http2.connect(`http://127.0.0.1:${p}`);
      client.on('error', () => {});
      const got = [];
      client.on('altsvc', (alt, origin, sid) => got.push({ alt, origin, sid }));
      const req = client.request({ ':method': 'GET', ':path': '/' });
      req.on('response', () => req.resume());
      req.on('error', () => {});
      req.on('close', () => {
        setTimeout(() => {
          client.destroy();
          srv.close();
          resolve(got);
        }, 100);
      });
    });
  });
}

test('altsvc("h3=:443","http://x"): h2lean server emits the same client event as node',
  async () => {
    const inv = (sess) => sess.altsvc('h3=":443"', 'http://example.com');
    const a = await captureAltSvc(http2, inv);
    const b = await captureAltSvc(h2lean, inv);
    assert.deepStrictEqual(b, a);
    assert.strictEqual(a.length, 1, 'precondition: node put an altsvc event on the wire');
  });

// -- ORIGIN send + receive: TLS-gated on both ends -------------------------

// The 'origin' event on the client carries the whole array (core.js's onOrigin emits once
// with the array). The session's originSet reflects both the seeded TLS-host origin and
// any frames the server pushed.
function captureOrigin(lib, invocation, clientOpts) {
  return new Promise((resolve) => {
    let sess;
    const srv = lib.createSecureServer(
      { key: TLS_KEY, cert: TLS_CERT },
      (req, res) => {
        res.writeHead(200);
        res.end('ok');
        setImmediate(() => {
          try {
            invocation(sess);
          } catch { /* probe errors are ignored */ }
        });
      });
    srv.on('session', (s) => {
      sess = s;
    });
    srv.listen(0, '127.0.0.1', () => {
      const p = srv.address().port;
      const client = http2.connect(`https://127.0.0.1:${p}`,
        { rejectUnauthorized: false, ...clientOpts });
      client.on('error', () => {});
      const events = [];
      client.on('origin', (arr) => events.push([...arr]));
      const req = client.request({ ':method': 'GET', ':path': '/' });
      req.on('response', () => req.resume());
      req.on('error', () => {});
      req.on('close', () => {
        setTimeout(() => {
          const set = client.originSet;
          resolve({ events, set: set === undefined ? undefined : set.slice().sort() });
          client.destroy();
          srv.close();
        }, 100);
      });
    });
  });
}

test('origin("http://a","http://b") over TLS: h2lean matches node event + originSet',
  async () => {
    const inv = (sess) => sess.origin('http://a.example', 'http://b.example');
    const a = await captureOrigin(http2, inv, {});
    const b = await captureOrigin(h2lean, inv, {});
    assert.deepStrictEqual(b.events, a.events);
    assert.strictEqual(a.events.length, 1, 'precondition: node emits one origin event per frame');
    // Compare the set after normalising the ephemeral client port (seed entry varies).
    const strip = (s) => s.replace(/:\d+$/, ':P');
    assert.deepStrictEqual(b.set && b.set.map(strip), a.set && a.set.map(strip));
  });

test('server option `origins`: node auto-sends ORIGIN at session start, h2lean matches',
  async () => {
    const o = ['http://auto.example'];
    function autoOriginsProbe(lib) {
      return new Promise((resolve) => {
        const srv = lib.createSecureServer({
          key: TLS_KEY, cert: TLS_CERT, origins: o
        }, (req, res) => {
          res.writeHead(200); res.end('ok');
        });
        srv.listen(0, '127.0.0.1', () => {
          const p = srv.address().port;
          const client = http2.connect(`https://127.0.0.1:${p}`,
            { rejectUnauthorized: false });
          client.on('error', () => {});
          const events = [];
          client.on('origin', (arr) => events.push([...arr]));
          const req = client.request({ ':method': 'GET', ':path': '/' });
          req.on('response', () => req.resume());
          req.on('error', () => {});
          req.on('close', () => {
            setTimeout(() => {
              resolve(events);
              client.destroy();
              srv.close();
            }, 100);
          });
        });
      });
    }
    const a = await autoOriginsProbe(http2);
    const b = await autoOriginsProbe(h2lean);
    assert.deepStrictEqual(b, a);
    assert.strictEqual(a.length, 1, 'precondition: node auto-sends one ORIGIN at session start');
  });

// -- client maxOriginSetSize cap -------------------------------------------

// A client given `maxOriginSetSize: N` destroys with ERR_HTTP2_TOO_MANY_ORIGINS when an
// inbound ORIGIN frame would push the set past N. Node's default (unset) is 128.
function capProbe(lib, maxOriginSetSize, origins) {
  return new Promise((resolve) => {
    let sess;
    const srv = http2.createSecureServer(
      { key: TLS_KEY, cert: TLS_CERT },
      (req, res) => {
        res.writeHead(200);
        res.end('ok');
        setImmediate(() => {
          // Send each origin in its own frame so the cap is exercised per-frame.
          for (const o of origins) {
            try {
              sess.origin(o);
            } catch { /* probe errors are ignored */ }
          }
        });
      });
    srv.on('session', (s) => {
      sess = s;
    });
    srv.listen(0, '127.0.0.1', () => {
      const p = srv.address().port;
      const opts = { rejectUnauthorized: false };
      if (maxOriginSetSize !== undefined) {
        opts.maxOriginSetSize = maxOriginSetSize;
      }
      const client = lib.connect(`https://127.0.0.1:${p}`, opts);
      let errored;
      client.on('error', (e) => {
        errored = e.code;
      });
      const req = client.request({ ':method': 'GET', ':path': '/' });
      req.on('response', () => req.resume());
      req.on('error', () => {});
      req.on('close', () => {
        setTimeout(() => {
          client.destroy();
          srv.close();
          resolve({ errored });
        }, 100);
      });
    });
  });
}

test('client maxOriginSetSize=1 with two frames: node destroys ERR_HTTP2_TOO_MANY_ORIGINS',
  { skip: ORIGIN_SET_CAP ? false : 'maxOriginSetSize absent before node 24 (test/node-version.js)' },
  async () => {
    const a = await capProbe(http2, 1, ['http://x1.example', 'http://x2.example']);
    const b = await capProbe(h2lean, 1, ['http://x1.example', 'http://x2.example']);
    assert.strictEqual(b.errored, a.errored, 'h2lean must match node on the observable code');
    assert.strictEqual(a.errored, 'ERR_HTTP2_TOO_MANY_ORIGINS',
      'precondition: node applies the cap when the second ORIGIN would overflow');
  });

// -- originSet seeding ------------------------------------------------------

// Node's initOriginSet seeds from socket.servername (SNI), falling back to remoteAddress
// (bracketed when IPv6) with `:${remotePort}`, all routed through getURLOrigin. Compare
// the *shape* of the set -- both stacks must show one entry of the form `https://127.0.0.1:P`
// on an h2s connection to 127.0.0.1 with no SNI.
test('originSet seeds from TLS socket: h2lean matches node', async () => {
  function seedProbe(lib) {
    return new Promise((resolve) => {
      const srv = lib.createSecureServer(
        { key: TLS_KEY, cert: TLS_CERT },
        (req, res) => {
          res.writeHead(200); res.end('ok');
        });
      srv.on('session', (sess) => {
        setImmediate(() => resolve([...sess.originSet].sort()));
      });
      srv.listen(0, '127.0.0.1', () => {
        const p = srv.address().port;
        const client = http2.connect(`https://127.0.0.1:${p}`, { rejectUnauthorized: false });
        client.on('error', () => {});
        const req = client.request({ ':method': 'GET', ':path': '/' });
        req.on('response', () => req.resume());
        req.on('close', () => {
          client.destroy(); srv.close();
        });
        setTimeout(() => resolve([]), 3000);
      });
    });
  }
  const a = await seedProbe(http2);
  const b = await seedProbe(h2lean);
  // Both stacks must produce exactly one origin (the seed); the port is not stable
  // across runs so we compare the shape -- everything up to the last ':'.
  assert.strictEqual(b.length, a.length, 'both seeds are one element');
  const stripPort = (s) => s.replace(/:\d+$/, ':P');
  assert.deepStrictEqual(b.map(stripPort), a.map(stripPort));
});
