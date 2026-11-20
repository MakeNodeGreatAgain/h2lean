// Port of node's `test/parallel/test-http2-origin.js` (RFC 8336 ORIGIN frame) to the
// differential harness used here: every block of the upstream file is run once against
// `node:http2` and once against h2lean, and the two normalised transcripts are compared
// (AGENTS.md §3, CONTRIBUTING.md §1 — the upstream file is a scenario source, and node's
// measured value is the expectation, never h2lean's own behaviour).
//
// The four blocks of the upstream file map to the four cases below:
//   1. `session.origin()` argument validation, plus two ORIGIN frames accumulating into
//      `originSet` in arrival order.
//   2. `createSecureServer({ origins })` auto-sends one ORIGIN at connection start.
//   3. a 421 (Misdirected Request) answer removes the requesting origin from `originSet`.
//   4. over plain-text HTTP/2 the client parses ORIGIN and then ignores it, and
//      `originSet` reads `undefined`.
//
// Upstream uses node's `agent8` fixtures and `common/countdown`; the port uses the
// repository's own self-signed localhost pair and plain `node:test` async cases instead,
// and reads `localhost` as the authority so the TLS seed origin comes from SNI
// (`initOriginSet` prefers `socket.servername`) and is therefore stable across runs. The
// ephemeral port inside that seed origin is normalised away before comparing, since it
// differs per run and per stack.
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const h2lean = require('../../index');
const certificates = require('../fixtures-tls');

// The self-signed pair through the generator, not a raw read of out/*.pem: on a fresh
// checkout out/ is empty and the two files exist only once fixtures-tls has run (they are
// gitignored). A direct readFileSync here threw ENOENT on the CI matrix for the majors where
// no earlier test had generated the pair yet.
const { key: TLS_KEY, cert: TLS_CERT } = certificates();

// The seed origin of a client that connected to https://localhost:P. The port is the only
// part that is not stable, so it is folded to ':P' in every recorded string.
function norm(value) {
  if (value === undefined) {
    return 'undefined';
  }
  if (typeof value === 'string') {
    return value.replace(/:0$/, ':P').replace(/:(\d+)/, ':P');
  }
  if (Array.isArray(value)) {
    return value.map(norm);
  }
  return value;
}

// Records what one client session sees: the 'origin' events, the originSet after each of
// them, and the outcome of every validation call made on the server session.
//
// `serverSessionWork(session)` runs at 'session', `serverStreamWork(stream)` at 'stream';
// `clientAfterOrigin` optionally issues a second request once the given number of 'origin'
// events has arrived (block 3 needs that ordering: the ORIGIN must be in the set before a
// 421 can take it out).
function originProbe(lib, options, work) {
  return new Promise((resolve) => {
    const transcript = { events: [], sets: [], throws: [], status: undefined,
      setAfterResponse: undefined, setAtEnd: undefined };
    const srv = options.origins === undefined
      ? lib.createSecureServer({ key: TLS_KEY, cert: TLS_CERT })
      : lib.createSecureServer({ key: TLS_KEY, cert: TLS_CERT, origins: options.origins });
    srv.on('session', (session) => {
      transcript.throws = (work.sessionWork || []).map((call) => {
        try {
          session.origin(...call.args);
          return 'ok';
        } catch (e) {
          return `${e.constructor.name}/${e.code}`;
        }
      });
      if (work.sessionOrigin) {
        session.origin(...work.sessionOrigin);
      }
    });
    srv.on('stream', (stream) => {
      if (work.streamOrigin) {
        stream.session.origin(...work.streamOrigin);
      }
      stream.respond(work.respond);
      stream.end('ok');
    });
    srv.on('error', () => {});
    srv.listen(0, 'localhost', () => {
      const port = srv.address().port;
      const seed = `https://localhost:${port}`;
      const client = lib.connect(seed, { ca: TLS_CERT });
      client.on('error', () => {});
      client.on('origin', (origins) => {
        transcript.events.push(origins);
        transcript.sets.push(client.originSet);
      });
      let done = 0;
      const finish = () => {
        if (++done < (work.requests || 1)) {
          return;
        }
        transcript.setAtEnd = client.originSet;
        client.destroy();
        srv.close();
        resolve(transcript);
      };
      const issue = () => {
        const req = client.request(work.requestHeaders);
        req.on('response', (headers) => {
          transcript.status = headers[':status'];
          transcript.setAfterResponse = client.originSet;
        });
        req.on('error', () => {});
        req.resume();
        req.on('close', finish);
      };
      if (work.waitOrigins) {
        let seen = 0;
        client.on('origin', () => {
          if (++seen === work.waitOrigins) {
            setImmediate(issue);
          }
        });
      } else {
        issue();
      }
      setTimeout(finish, 5000);
    });
  });
}

// -- block 1: validation + two accumulating ORIGIN frames --------------------------------

// The validation list is upstream's, in upstream's order: the no-argument call that is
// accepted but sends nothing, the four non-origin types, the two non-http(s) origins, the
// unparseable string, and the origin that overflows the 16383-byte frame budget.
const SESSION_CALLS = [
  { args: [] },
  { args: [0] },
  { args: [true] },
  { args: [{}] },
  { args: [[]] },
  { args: [new URL('foo://bar')] },
  { args: ['foo://bar'] },
  { args: ['not a valid url'] },
  { args: [`http://foo.bar${'a'.repeat(16383)}`] }
];

test('upstream origin block 1: validation and an accumulating originSet', async () => {
  const work = {
    sessionWork: SESSION_CALLS,
    sessionOrigin: ['https://foo.org/a/b/c', new URL('https://bar.org')],
    streamOrigin: ['https://example.org/a/b/c', new URL('https://example.com')],
    requests: 1
  };
  const a = await originProbe(http2, {}, work);
  const b = await originProbe(h2lean, {}, work);
  assert.deepStrictEqual(norm(b.events), norm(a.events));
  assert.deepStrictEqual(norm(b.sets), norm(a.sets));
  assert.deepStrictEqual(b.throws, a.throws);
  assert.deepStrictEqual(norm(b.setAtEnd), norm(a.setAtEnd));
  // Anchors read off the live node run: two events of two origins each, in wire order, and
  // the four rejection classes of block 1 land on their own codes.
  assert.deepStrictEqual(norm(a.events),
    [['https://foo.org', 'https://bar.org'], ['https://example.org', 'https://example.com']]);
  assert.deepStrictEqual(a.throws, [
    'ok',
    'TypeError/ERR_INVALID_ARG_TYPE',
    'TypeError/ERR_INVALID_ARG_TYPE',
    'TypeError/ERR_INVALID_ARG_TYPE',
    'TypeError/ERR_INVALID_ARG_TYPE',
    'TypeError/ERR_HTTP2_INVALID_ORIGIN',
    'TypeError/ERR_HTTP2_INVALID_ORIGIN',
    'TypeError/ERR_INVALID_URL',
    'TypeError/ERR_HTTP2_ORIGIN_LENGTH'
  ]);
  assert.deepStrictEqual(norm(a.setAtEnd),
    ['https://localhost:P', 'https://foo.org', 'https://bar.org',
      'https://example.org', 'https://example.com']);
});

// -- block 2: the `origins` server option auto-sends at connection start ----------------

test('upstream origin block 2: createSecureServer({origins}) sends one ORIGIN at start',
  async () => {
    const origins = ['https://foo.org/a/b/c', 'https://bar.org'];
    const work = { respond: undefined, requests: 1 };
    const a = await originProbe(http2, { origins }, work);
    const b = await originProbe(h2lean, { origins }, work);
    assert.deepStrictEqual(norm(b.events), norm(a.events));
    assert.deepStrictEqual(norm(b.setAtEnd), norm(a.setAtEnd));
    assert.deepStrictEqual(norm(a.events), [['https://foo.org', 'https://bar.org']]);
    assert.deepStrictEqual(norm(a.setAtEnd), ['https://localhost:P', 'https://foo.org', 'https://bar.org']);
  });

// -- block 3: a 421 answer removes the requesting origin --------------------------------

test('upstream origin block 3: a 421 removes the request origin from originSet', async () => {
  const work = {
    sessionOrigin: ['https://foo.org'],
    respond: { ':status': 421 },
    requestHeaders: { ':authority': 'foo.org' },
    waitOrigins: 1,
    requests: 1
  };
  const a = await originProbe(http2, {}, work);
  const b = await originProbe(h2lean, {}, work);
  assert.deepStrictEqual(norm(b.events), norm(a.events));
  assert.deepStrictEqual(norm(b.setAfterResponse), norm(a.setAfterResponse));
  assert.deepStrictEqual(norm(b.setAtEnd), norm(a.setAtEnd));
  assert.strictEqual(a.status, 421);
  // RFC 8336 3.2 / core.js emitStreamEvent: the asserted origin is in the set while the
  // 421 is being answered, and gone by the time 'response' runs — the seed stays.
  assert.deepStrictEqual(norm(a.events), [['https://foo.org']]);
  assert.deepStrictEqual(norm(a.setAfterResponse), ['https://localhost:P']);
  assert.deepStrictEqual(norm(a.setAtEnd), ['https://localhost:P']);
});

// -- block 4: plain-text HTTP/2 ignores ORIGIN ------------------------------------------

function plainProbe(lib) {
  return new Promise((resolve) => {
    const out = { events: 0, set: 'unset' };
    const srv = lib.createServer();
    srv.on('stream', (stream) => {
      stream.session.origin('https://example.org/a/b/c', new URL('https://example.com'));
      stream.respond();
      stream.end('ok');
    });
    srv.on('error', () => {});
    srv.listen(0, '127.0.0.1', () => {
      const port = srv.address().port;
      const client = lib.connect(`http://127.0.0.1:${port}`);
      client.on('error', () => {});
      client.on('origin', () => {
        out.events++;
      });
      out.set = client.originSet;
      const req = client.request();
      req.on('error', () => {});
      req.resume();
      req.on('close', () => {
        out.setAfterClose = client.originSet === undefined ? 'undefined' : 'defined';
        client.destroy();
        srv.close();
        resolve(out);
      });
      setTimeout(() => {
        client.destroy(); srv.close(); resolve(out);
      }, 5000);
    });
  });
}

test('upstream origin block 4: over h2c the client ignores ORIGIN and originSet is undefined',
  async () => {
    const a = await plainProbe(http2);
    const b = await plainProbe(h2lean);
    assert.deepStrictEqual(b, a);
    assert.strictEqual(a.events, 0, 'precondition: node sends the frame and ignores it');
    assert.strictEqual(a.set, undefined);
    assert.strictEqual(a.setAfterClose, 'undefined');
  });
