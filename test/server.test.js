// End-to-end tests of the h2lite server driven by node:http2's client, i.e. by
// a real nghttp2 peer. Everything here is a wire test: if the framing, HPACK,
// flow control or GOAWAY handling is wrong, the client notices first.
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const http2 = require('node:http2');
const h2lite = require('../index');

function listen(server) {
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve(server.address().port));
  });
}

function connect(port) {
  const client = http2.connect(`http://127.0.0.1:${port}`);
  client.on('error', () => {});
  return client;
}

function request(client, headers, body) {
  return new Promise((resolve, reject) => {
    const req = client.request(headers, body ? {} : { endStream: true });
    const chunks = [];
    let responseHeaders = null;
    req.on('response', (h) => {
      responseHeaders = h;
    });
    req.setEncoding('latin1');
    req.on('data', (d) => chunks.push(d));
    req.on('end', () => resolve({ headers: responseHeaders, body: chunks.join('') }));
    req.on('error', reject);
    if (body != null) {
      req.end(body);
    }
  });
}

const GET = { ':method': 'GET', ':path': '/index.html', accept: '*/*' };

test('GET with a small body', async () => {
  const srv = h2lite.createServer();
  srv.on('stream', (stream, headers) => {
    stream.respond({
      ':status': 200,
      'content-type': 'text/plain',
      'x-path': headers[':path']
    });
    stream.end('hello world');
  });
  const port = await listen(srv);
  const client = connect(port);
  const res = await request(client, GET);
  client.destroy();
  srv.close();

  assert.strictEqual(res.headers[':status'], 200);
  assert.strictEqual(res.headers['content-type'], 'text/plain');
  assert.strictEqual(res.headers['x-path'], '/index.html');
  assert.strictEqual(res.body, 'hello world');
});

test('frozen response headers take the cached-block path', async () => {
  const HEADERS = Object.freeze({ ':status': 200, 'x-cached': 'yes' });
  const srv = h2lite.createServer();
  srv.on('stream', (stream) => stream.respond(HEADERS, { endStream: true }));
  const port = await listen(srv);
  const client = connect(port);
  const a = await request(client, GET);
  const b = await request(client, GET);
  client.destroy();
  srv.close();
  assert.strictEqual(a.headers['x-cached'], 'yes');
  assert.strictEqual(b.headers['x-cached'], 'yes');
  assert.strictEqual(a.headers[':status'], 200);
});

test('a 300 KB response crosses the flow-control window and frame size', async () => {
  const size = 300 * 1024;
  const body = Buffer.alloc(size, 0x61);
  const srv = h2lite.createServer();
  srv.on('stream', (stream) => {
    stream.respond({ ':status': 200, 'content-length': String(size) });
    // write in pieces so the window has to be replenished mid-stream
    for (let off = 0; off < size; off += 40 * 1024) {
      const end = Math.min(off + 40 * 1024, size);
      stream.write(body.subarray(off, end));
      if (end === size) {
        stream.end();
      }
    }
  });
  const port = await listen(srv);
  const client = connect(port);
  const res = await request(client, GET);
  client.destroy();
  srv.close();
  assert.strictEqual(res.body.length, size);
  assert.ok(/^a+$/.test(res.body));
});

test('request body is delivered with correct bytes and ends the stream', async () => {
  const payload = 'x'.repeat(70 * 1024); // larger than one read buffer
  const srv = h2lite.createServer();
  const seen = [];
  srv.on('stream', (stream, headers) => {
    assert.strictEqual(headers[':method'], 'POST');
    let received = '';
    stream.on('data', (d) => {
      received += d.toString('latin1');
    });
    stream.on('end', () => {
      seen.push(received);
      stream.respond({ ':status': 200, 'x-len': String(received.length) });
      stream.end();
    });
    stream.on('error', (e) => seen.push(`error:${e.message}`));
  });
  const port = await listen(srv);
  const client = connect(port);
  const res = await request(client, { ':method': 'POST', ':path': '/upload' }, payload);
  client.destroy();
  srv.close();

  assert.strictEqual(res.headers['x-len'], String(payload.length));
  assert.strictEqual(seen.length, 1);
  assert.strictEqual(seen[0].length, payload.length);
});

test('delivered chunks are Buffer views and the body arrives identical', async () => {
  const payload = 'y'.repeat(20000);
  const srv = h2lite.createServer();
  srv.on('stream', (stream) => {
    let total = 0;
    stream.on('data', (d) => {
      assert.ok(Buffer.isBuffer(d), 'chunk must be a Buffer view');
      total += d.length;
    });
    stream.on('end', () => {
      stream.respond({ ':status': 200, 'x-total': String(total) });
      stream.end(payload);
    });
  });
  const port = await listen(srv);
  const client = connect(port);
  const res = await request(client, { ':method': 'POST', ':path': '/' }, payload);
  client.destroy();
  srv.close();
  assert.strictEqual(res.headers['x-total'], String(payload.length));
  assert.strictEqual(res.body, payload);
});

test('50 concurrent streams on one connection', async () => {
  const srv = h2lite.createServer();
  srv.on('stream', (stream, headers) => {
    stream.respond({ ':status': 200, 'x-id': headers[':path'] });
    stream.end(headers[':path']);
  });
  const port = await listen(srv);
  const client = connect(port);
  const results = await Promise.all(
    Array.from({ length: 50 }, (_, i) => request(client, { ':method': 'GET', ':path': `/${i}` })));
  client.destroy();
  srv.close();
  results.forEach((r, i) => {
    assert.strictEqual(r.headers['x-id'], `/${i}`);
    assert.strictEqual(r.body, `/${i}`);
  });
});

test('h2lite decodes the same request headers node:http2 does', async () => {
  const SET = {
    ':method': 'GET',
    ':path': '/abc/def?q=1&r=2',
    'user-agent': 'probe/1.0',
    accept: '*/*',
    'x-trace': '0123456789abcdef0123456789abcdef',
    cookie: 'a=1; b=2'
  };
  const run = (client) => new Promise((resolve, reject) => {
    client.on('error', reject);
    let done = 0;
    const fire = () => {
      const req = client.request(SET);
      req.resume();
      req.on('end', () => {
        if (++done < 4) {
          fire();
        } else {
          client.destroy(); resolve();
        }
      });
      req.on('error', reject);
    };
    fire();
  });

  // node's own answer. node's Http2Server keeps its session list private, so
  // teardown goes through the client socket rather than srv.sessions.
  const nativeOut = await new Promise((resolve, reject) => {
    const srv = http2.createServer();
    const got = [];
    let client = null;
    srv.on('stream', (stream, headers) => {
      got.push({ ...headers });
      stream.respond({ ':status': 200 }, { endStream: true });
      if (got.length === 4) {
        srv.close();
        if (client !== null) {
          client.destroy();
        }
        resolve(got);
      }
    });
    srv.on('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      client = connect(srv.address().port);
      run(client).catch(reject);
    });
  });

  // our answer
  const ours = [];
  const srv2 = h2lite.createServer();
  srv2.on('stream', (stream, headers) => {
    ours.push({ ...headers });
    stream.respond({ ':status': 200 }, { endStream: true });
  });
  const port2 = await listen(srv2);
  await run(connect(port2));
  srv2.close();

  assert.ok(ours.length >= 4, `h2lite saw ${ours.length} streams`);
  assert.ok(nativeOut.length >= 4);
  for (let i = 0; i < 4; i++) {
    const a = { ...ours[i] };
    const b = { ...nativeOut[i] };
    delete a[':authority'];
    delete b[':authority'];
    assert.deepStrictEqual(Object.keys(a).sort(), Object.keys(b).sort(), `stream ${i} keys`);
    for (const k of Object.keys(b)) {
      assert.strictEqual(String(a[k]), String(b[k]), `stream ${i} ${k}`);
    }
  }
});

test('session setTimeout measures silence, not wall clock', async () => {
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const srv = h2lite.createServer();
  srv.on('stream', (stream) => stream.respond({ ':status': 200 }, { endStream: true }));
  const port = await listen(srv);
  const client = connect(port);
  await request(client, GET); // establishes the session
  const session = [...srv.sessions][0];
  let fired = 0;
  session.setTimeout(200, () => {
    fired++;
  });
  session.setTimeout(200, () => {
    fired++;
  }); // must replace, not stack
  try {
    for (let i = 0; i < 3; i++) {
      await sleep(80); // traffic well inside 200 ms
      await request(client, GET);
    }
    assert.strictEqual(fired, 0, 'incoming frames must refresh the idle timer');
    await sleep(400); // then silence
    assert.strictEqual(fired, 1, 'exactly one timeout after the idle period');
  } finally {
    // torn down whatever the assertions did: with the refresh guard removed the first assert
    // throws, the server and its client socket stayed alive, and the file's process never
    // exited -- the runner then waited for a verdict that had already been decided
    // (`node tools/mutate-guards.js 4` reporting a hang instead of the failure it causes).
    client.destroy();
    srv.close();
  }
});

test('a finished stream releases its slot (maxConcurrentStreams=1, 5 sequential)', async () => {
  const srv = h2lite.createServer({ settings: { maxConcurrentStreams: 1 } });
  srv.on('stream', (stream, headers) => {
    stream.respond({ ':status': 200, 'x-path': headers[':path'] });
    stream.end();
  });
  const port = await listen(srv);
  const client = connect(port);
  const paths = [];
  for (let i = 0; i < 5; i++) {
    const res = await request(client, { ':method': 'GET', ':path': `/s${i}` });
    paths.push(res.headers['x-path']);
  }
  // the leak this guards against: a stream that answered before its END_STREAM
  // was reported stayed in the session forever, so slot 1 was never free again
  const session = [...srv.sessions][0];
  assert.strictEqual(session._streams.size, 0, 'no stream may outlive its request');
  assert.strictEqual(session._liveStreams, 0);
  client.destroy();
  srv.close();
  assert.deepStrictEqual(paths, ['/s0', '/s1', '/s2', '/s3', '/s4']);
});

test('a bad connection preface is refused with GOAWAY PROTOCOL_ERROR', async () => {
  const srv = h2lite.createServer();
  let sawStream = false;
  srv.on('stream', () => {
    sawStream = true;
  });
  const port = await listen(srv);
  const raw = await new Promise((resolve, reject) => {
    const sock = net.connect(port, '127.0.0.1', () => {
      sock.write('GET / HTTP/1.1\r\nHost: x\r\n\r\n'); // not an h2c preface
    });
    sock.setTimeout(1500);
    const chunks = [];
    sock.on('data', (d) => chunks.push(d));
    sock.on('timeout', () => {
      sock.destroy(); reject(new Error('no reply'));
    });
    sock.on('close', () => resolve(Buffer.concat(chunks)));
    sock.on('error', () => {});
  });
  srv.close();

  assert.strictEqual(sawStream, false);
  const frames = [];
  for (let i = 0; i + 9 <= raw.length;) {
    const len = (raw[i] << 16) | (raw[i + 1] << 8) | raw[i + 2];
    frames.push({ type: raw[i + 3], sid: raw.readUInt32BE(i + 5), payload: raw.subarray(i + 9, i + 9 + len) });
    i += 9 + len;
  }
  const goaway = frames.find((x) => x.type === 7);
  assert.ok(goaway, `expected a GOAWAY frame, got ${JSON.stringify(frames.map((x) => x.type))}`);
  assert.strictEqual(goaway.sid, 0);
  assert.strictEqual(goaway.payload.readUInt32BE(4), 1, 'PROTOCOL_ERROR');
});

test('maxConcurrentStreams refuses what it will not serve', async () => {
  const srv = h2lite.createServer({ settings: { maxConcurrentStreams: 1 } });
  const held = [];
  srv.on('stream', (stream) => {
    held.push(stream);
  }); // never answer
  const port = await listen(srv);
  const client = connect(port);
  client.on('error', () => {});
  const first = client.request(GET);
  first.resume();
  const second = await new Promise((resolve) => {
    const r = client.request({ ':method': 'GET', ':path': '/second' });
    r.resume();
    r.on('close', () => resolve({ code: r.rstCode }));
    r.on('error', () => {});
    setTimeout(() => resolve({ code: 'timeout' }), 1500);
  });
  client.destroy();
  srv.close();
  for (const s of held) {
    s.destroy(8);
  }
  assert.strictEqual(second.code, 7, 'REFUSED_STREAM expected');
});
