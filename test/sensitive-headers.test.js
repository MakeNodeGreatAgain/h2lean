// node hangs the names of the field lines that arrived as never-indexed HPACK
// literals (RFC 7541 6.2.3, prefix 0001) on every inbound header object, under
// its own `sensitiveHeaders` symbol -- toHeaderObject in internal/util.js does
// the assignment, the nghttp2 layer passes the list beside the headers. What the
// list holds was measured on node v26.12.1 with raw-socket probes: one entry per
// DELIVERED field line, duplicates kept in arrival order, [] when no line was
// never-indexed (the property is always there, never undefined), a field dropped
// for an illegal name never entering it, and the object hanging on every inbound
// kind -- server stream headers, compat req.headers, the 'trailers' objects of
// both layers, client responses and push promises. h2lean used to leave the
// symbol off; everything below pins the list itself, differentially wherever the
// same raw exchange or the same application script runs on both stacks.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const http2 = require('node:http2');
const h2lean = require('../index');
const f = require('../lib/frames');
const KS = http2.sensitiveHeaders;
const { closeServer } = require('./diff-harness');

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// A literal with the never-indexed bit on, new-name form: 0001 prefix, index 0,
// then name and value as length-prefixed strings.
function lit(name, value) {
  return Buffer.concat([
    Buffer.from([0x10, name.length]), Buffer.from(name, 'latin1'),
    Buffer.from([value.length]), Buffer.from(value, 'latin1')
  ]);
}

// :method GET :path / :scheme http :authority a, plus whatever else the case adds.
const BASE = Buffer.from([0x82, 0x84, 0x86, 0x01, 1, 0x61]);

// Sends one request through a raw socket and hands the server-side header object
// (or the trailers one, when `trailer` adds a second block) to the handler. Both
// stacks run the same exchange: the only thing compared is what the application
// reads off the object, so any difference is an implementation difference.
function rawHeaders(impl, blocks, { trailer = null } = {}) {
  return new Promise((resolve, reject) => {
    const srv = impl.createServer();
    srv.on('stream', (stream, headers) => {
      if (trailer === null) {
        resolve(headers);
        stream.respond({ ':status': 200 });
        stream.end('ok');
        return;
      }
      // The response starts only after the trailer block was handed over: a
      // response that finishes first arms the unread-request auto-close (the
      // rule test/response-start pins) and that can end the stream before the
      // trailer block is parsed, so the event may never fire.
      stream.on('trailers', (t) => {
        resolve([headers, t]);
        stream.respond({ ':status': 200 });
        stream.end('ok');
      });
    });
    srv.on('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const sock = net.connect(srv.address().port, '127.0.0.1');
      sock.on('error', () => {});
      sock.on('connect', () => {
        sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
        setTimeout(() => {
          if (trailer !== null) {
            sock.write(frame(f.HEADERS, f.F_END_HEADERS, 1, blocks));
            sock.write(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, trailer));
          } else {
            sock.write(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, blocks));
          }
        }, 50);
      });
      setTimeout(() => {
        sock.destroy();
        reject(new Error('no headers seen'));
      }, 2000).unref();
      setTimeout(() => {
        sock.destroy();
        closeServer(srv);
      }, 3000).unref();
    });
  });
}

// The compat layer, same exchange: the handler answers on the (req, res) pair and
// reports what req.headers (and req.trailers, with `trailer`) carry. The request
// body is consumed -- the auto-close of an unread request (the rule
// test/response-start pins) would end the stream before the trailer block is
// parsed and the 'trailers' event would never fire.
function rawCompat(impl, blocks, { trailer = null } = {}) {
  return new Promise((resolve, reject) => {
    const srv = impl.createServer((req, res) => {
      req.resume();
      if (trailer === null) {
        resolve(req.headers);
        res.end('ok');
      } else {
        // node's compat layer does not emit a 'trailers' event on the request:
        // onStreamTrailers merges the block into req.trailers with Object.assign
        // (which copies the symbol-keyed property along with the strings) and
        // the list is readable at 'end', on both stacks.
        req.on('end', () => {
          resolve([req.headers, req.trailers]);
          res.end('ok');
        });
      }
    });
    srv.on('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const sock = net.connect(srv.address().port, '127.0.0.1');
      sock.on('error', () => {});
      sock.on('connect', () => {
        sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
        setTimeout(() => {
          if (trailer !== null) {
            sock.write(frame(f.HEADERS, f.F_END_HEADERS, 1, blocks));
            sock.write(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, trailer));
          } else {
            sock.write(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, blocks));
          }
        }, 50);
      });
      setTimeout(() => {
        sock.destroy();
        reject(new Error('no request seen'));
      }, 2000).unref();
      setTimeout(() => {
        sock.destroy();
        closeServer(srv);
      }, 3000).unref();
    });
  });
}

async function bothRaw(blocks, opts) {
  return [await rawHeaders(http2, blocks, opts), await rawHeaders(h2lean, blocks, opts)];
}

test('the exported symbol is node own object', () => {
  assert.strictEqual(h2lean.sensitiveHeaders, http2.sensitiveHeaders);
});

test('a request with no never-indexed field reports an empty list', async () => {
  const [nodeGot, ourGot] = await bothRaw(BASE);
  assert.deepStrictEqual(nodeGot[KS], []);
  assert.deepStrictEqual(ourGot[KS], []);
});

test('a never-indexed field line is listed by name', async () => {
  const blocks = Buffer.concat([BASE, lit('authorization', 'x')]);
  const [nodeGot, ourGot] = await bothRaw(blocks);
  assert.deepStrictEqual(nodeGot[KS], ['authorization']);
  assert.deepStrictEqual(ourGot[KS], ['authorization']);
});

test('duplicates keep their arrival order, one entry per line', async () => {
  const blocks = Buffer.concat([BASE, lit('x-a', '1'), lit('x-b', '2'), lit('x-a', '3')]);
  const [nodeGot, ourGot] = await bothRaw(blocks);
  assert.deepStrictEqual(nodeGot[KS], ['x-a', 'x-b', 'x-a']);
  assert.deepStrictEqual(ourGot[KS], ['x-a', 'x-b', 'x-a']);
});

test('a field dropped for an illegal name never reaches the list', async () => {
  // The illegal name is still never-indexed on the wire; node filters it out
  // because the list counts the lines the application was handed.
  const blocks = Buffer.concat([BASE, lit('x(a)', '1'), lit('x-a', '2')]);
  const [nodeGot, ourGot] = await bothRaw(blocks);
  assert.deepStrictEqual(nodeGot[KS], ['x-a']);
  assert.deepStrictEqual(ourGot[KS], ['x-a']);
});

test('the shape cache reproduces the list on a hit', async () => {
  // The identical block twice on one connection: the second request comes off
  // the shape cache, where no decode runs -- the list must be part of what the
  // entry carries, not something re-derived from the decoder's state.
  const blocks = Buffer.concat([BASE, lit('x-secret', 'v')]);
  const srv = h2lean.createServer();
  const seen = [];
  srv.on('stream', (stream, headers) => {
    seen.push(headers[KS]);
    stream.respond({ ':status': 200 });
    stream.end('ok');
  });
  await new Promise((resolve, reject) => {
    srv.on('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const sock = net.connect(srv.address().port, '127.0.0.1');
      sock.on('error', () => {});
      sock.on('connect', () => {
        sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
        setTimeout(() => {
          sock.write(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, blocks));
          sock.write(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 3, blocks));
        }, 50);
      });
      setTimeout(() => {
        sock.destroy();
        closeServer(srv).then(resolve);
      }, 300).unref();
    });
  });
  assert.deepStrictEqual(seen, [['x-secret'], ['x-secret']]);
});

test('compat req.headers carries the same list', async () => {
  const blocks = Buffer.concat([BASE, lit('authorization', 'x')]);
  const nodeGot = await rawCompat(http2, blocks);
  const ourGot = await rawCompat(h2lean, blocks);
  assert.deepStrictEqual(nodeGot[KS], ['authorization']);
  assert.deepStrictEqual(ourGot[KS], ['authorization']);
});

test('compat req.trailers carries the same list', async () => {
  const trailer = Buffer.concat([lit('x-tr-secret', 't')]);
  const nodeGot = await rawCompat(http2, BASE, { trailer });
  const ourGot = await rawCompat(h2lean, BASE, { trailer });
  assert.deepStrictEqual(nodeGot[1][KS], ['x-tr-secret']);
  assert.deepStrictEqual(ourGot[1][KS], ['x-tr-secret']);
});

test('a trailer block lists its never-indexed fields', async () => {
  const trailer = Buffer.concat([lit('x-tr-secret', 't')]);
  const [nodeGot, ourGot] = await bothRaw(BASE, { trailer });
  assert.deepStrictEqual(nodeGot[0][KS], []); // the request itself: none
  assert.deepStrictEqual(nodeGot[1][KS], ['x-tr-secret']);
  assert.deepStrictEqual(ourGot[0][KS], []);
  assert.deepStrictEqual(ourGot[1][KS], ['x-tr-secret']);
});

// The outbound side: node marks an outbound field never-indexed by hanging the
// symbol on the headers object itself -- `respond({..., [KS]: ['x-secret']})`
// (validatePreparedResponseHeaders reads headers[kSensitiveHeaders]; the
// respond *options* carry no such field, measured on v26.12.1). h2lean's block
// builder reads the same property, so the same application script runs on both
// stacks, and each server implementation is served to each client.
async function responseThrough(serverImpl, clientImpl) {
  return new Promise((resolve, reject) => {
    const srv = serverImpl.createServer();
    srv.on('stream', (stream) => {
      stream.respond({ ':status': 200, 'x-secret': 'v', [KS]: ['x-secret'] });
      stream.pushStream({ ':path': '/p', 'x-psecret': 'q', [KS]: ['x-psecret'] }, () => {});
      stream.end('body');
    });
    srv.on('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const client = clientImpl.connect(`http://127.0.0.1:${srv.address().port}`);
      client.on('error', () => {});
      const got = {};
      const finish = () => {
        if (got.resp !== undefined && got.push !== undefined) {
          client.destroy();
          closeServer(srv).then(() => resolve([got.resp, got.push]));
        }
      };
      client.on('stream', (pushed, pushHeaders) => {
        pushed.on('error', () => {});
        got.push = pushHeaders[KS];
        finish();
      });
      const req = client.request({ ':path': '/' });
      req.on('error', reject);
      req.on('response', (h) => {
        got.resp = h[KS];
        finish();
      });
      req.end();
      setTimeout(() => {
        client.destroy();
        closeServer(srv);
        reject(new Error('response or push never seen'));
      }, 2000).unref();
    });
  });
}

test('a response marked sensitive reaches the client object, all four pairs',
  async () => {
    const got = [];
    got.push(await responseThrough(http2, http2));
    got.push(await responseThrough(h2lean, http2));
    got.push(await responseThrough(http2, h2lean));
    got.push(await responseThrough(h2lean, h2lean));
    // Each pair reports the response headers and the push promise's request
    // headers; the pushed response itself arrives on the 'push' event and goes
    // through the same _processBlock path.
    assert.deepStrictEqual(got[0], [['x-secret'], ['x-psecret']]);
    assert.deepStrictEqual(got[1], got[0]);
    assert.deepStrictEqual(got[2], got[0]);
    assert.deepStrictEqual(got[3], got[0]);
  });

test('shareHeaders freezes the list with the object it hangs on', async () => {
  const blocks = Buffer.concat([BASE, lit('x-secret', 'v')]);
  const srv = h2lean.createServer({ shareHeaders: true });
  const seen = [];
  srv.on('stream', (stream, headers) => {
    const list = headers[KS];
    let threw = '';
    try {
      list.push('nope');
    } catch {
      threw = 'frozen';
    }
    seen.push([list, threw, Object.isFrozen(headers)]);
    stream.respond({ ':status': 200 });
    stream.end('ok');
  });
  await new Promise((resolve, reject) => {
    srv.on('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const sock = net.connect(srv.address().port, '127.0.0.1');
      sock.on('error', () => {});
      sock.on('connect', () => {
        sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
        setTimeout(() => {
          sock.write(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, blocks));
        }, 50);
      });
      setTimeout(() => {
        sock.destroy();
        closeServer(srv).then(resolve);
      }, 300).unref();
    });
  });
  assert.deepStrictEqual(seen[0], [['x-secret'], 'frozen', true]);
});
