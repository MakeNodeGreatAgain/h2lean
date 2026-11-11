// M6: the compat (req, res) layer. The interesting assertions are differential:
// the same handler and the same client script run against node:http2 and against
// h2lean, and the two observations must agree.
const test = require('node:test');
const assert = require('node:assert');
const { probe, request, nodeHttp2 } = require('./diff-harness');
const h2lean = require('../index');

const GET = {
  ':method': 'GET',
  ':path': '/abc/def?q=1&r=2',
  ':scheme': 'http',
  ':authority': 'localhost',
  accept: '*/*',
  'user-agent': 'probe/1.0'
};

// Strips what legitimately differs between two runs on the same machine.
function settle(res) {
  const headers = { ...res.headers };
  const date = headers.date;
  delete headers.date;
  return { informational: res.informational, continued: res.continued, headers, trailers: res.trailers, body: res.body, date };
}

// Compares two settled results, ignoring `date`. The value is generated from each server's
// own clock and the two probes run milliseconds apart, so they land on different seconds
// whenever they straddle a boundary. The date's *shape* is asserted where it belongs, in
// the first test of this file; equality of two wall clocks was never the parity property.
function sameResult(a, b) {
  const withoutStamp = (r) => ({ ...r, date: null });
  assert.deepStrictEqual(withoutStamp(a), withoutStamp(b));
}

test('the request view and the response bytes match node exactly', async () => {
  const handler = (req, res, log) => {
    log.push({
      method: req.method,
      url: req.url,
      authority: req.authority,
      scheme: req.scheme,
      httpVersion: req.httpVersion,
      httpVersionMajor: req.httpVersionMajor,
      httpVersionMinor: req.httpVersionMinor,
      aborted: req.aborted,
      completeAfterEnd: undefined,
      remoteAddress: req.socket.remoteAddress,
      headerKeys: Object.keys(req.headers).sort(),
      rawHeaders: req.rawHeaders.slice(0),
      headerValues: Object.keys(req.headers).sort().map((k) => String(req.headers[k])),
      hasTrailers: Object.keys(req.trailers).length,
      statusCodeDefault: res.statusCode,
      headersSentBefore: res.headersSent
    });
    res.setHeader('X-Multi', ['a', 'b']);
    res.setHeader('set-cookie', ['c=1', 'd=2']);
    res.writeHead(201, { 'content-type': 'text/plain', 'x-trace': 'ff00' });
    log[log.length - 1].headersSentAfter = res.headersSent;
    log[log.length - 1].writableEndedBefore = res.writableEnded;
    res.end('payload');
    log[log.length - 1].writableEndedAfter = res.writableEnded;
    log[log.length - 1].getHeaderNames = res.getHeaderNames().slice(0).sort();
  };
  const script = async (client) => settle(await request(client, GET));

  const native = await probe(nodeHttp2, handler, script);
  const ours = await probe(h2lean, handler, script);

  // the date is generated per second by each server, so the two probes can land on
  // either side of a second boundary -- sameResult() leaves it out and the format is
  // asserted below, on both sides
  sameResult(ours.result, native.result);
  assert.deepStrictEqual(ours.log, native.log, 'application-visible request');
  // both stacks must send a date, formatted the http way
  for (const run of [native, ours]) {
    assert.match(run.result.date, /^[A-Z][a-z]{2}, \d{2} [A-Z][a-z]{2} \d{4} \d{2}:\d{2}:\d{2} GMT$/);
  }
});

test('a request body is readable as a stream and matches node', async () => {
  const payload = 'q'.repeat(30 * 1024);
  const handler = async (req, res, log) => {
    const chunks = [];
    for await (const chunk of req) {
      chunks.push(chunk);
    }
    const text = Buffer.concat(chunks).toString('latin1');
    log.push({ length: text.length, head: text.slice(0, 8), complete: req.complete });
    // content-length must describe what is actually sent: nghttp2 rejects a lying
    // one on both stacks, so this is part of the comparison, not a detail
    res.writeHead(200, { 'content-length': '8' });
    res.end(text.slice(0, 8));
  };
  const script = async (client) => settle(await request(
    client, { ...GET, ':method': 'POST' }, payload));

  const native = await probe(nodeHttp2, handler, script);
  const ours = await probe(h2lean, handler, script);
  sameResult(ours.result, native.result);
  assert.deepStrictEqual(ours.log, native.log);
  assert.strictEqual(ours.log[0].length, payload.length);
});

test('HEAD gets headers only, exactly like node', async () => {
  const handler = (req, res, log) => {
    log.push(req.method);
    res.setHeader('x-a', '1');
    res.end('this body must not be sent');
  };
  const script = async (client) => settle(await request(
    client, { ...GET, ':method': 'HEAD' }));

  const native = await probe(nodeHttp2, handler, script);
  const ours = await probe(h2lean, handler, script);
  assert.strictEqual(ours.log[0], 'HEAD');
  assert.strictEqual(ours.result.body, '');
  assert.deepStrictEqual(ours.result.headers, native.result.headers);
});

test('trailers arrive as a separate block, like node sends them', async () => {
  const handler = (req, res) => {
    res.setHeader('content-type', 'text/plain');
    res.writeHead(200);
    res.addTrailers({ 'x-tail': 'end-of-body', 'x-second': '2' });
    res.end('body');
  };
  const script = async (client) => settle(await request(client, GET));

  const native = await probe(nodeHttp2, handler, script);
  const ours = await probe(h2lean, handler, script);
  assert.ok(ours.result.trailers, 'h2lean must send a trailers block');
  assert.strictEqual(ours.result.trailers['x-tail'], 'end-of-body');
  assert.deepStrictEqual(ours.result.trailers, native.result.trailers);
  assert.strictEqual(ours.result.body, 'body');
});

test('Expect: 100-continue produces an informational response', async () => {
  const handler = (req, res, log) => {
    log.push(req.headers.expect);
    res.writeHead(200, { 'x-after-continue': 'yes' });
    res.end('done');
  };
  const script = async (client) => settle(await request(
    client, { ...GET, expect: '100-continue' }));

  const native = await probe(nodeHttp2, handler, script);
  const ours = await probe(h2lean, handler, script);
  sameResult(ours.result, native.result);
  assert.deepStrictEqual(ours.log, native.log);
  // node's client reports a 100 as 'continue'; the point is that both stacks put
  // an informational HEADERS block on the wire before the real response
  assert.deepStrictEqual(ours.result.continued, native.result.continued);
  assert.ok(ours.result.continued.length + ours.result.informational.length > 0,
    'no informational response was sent');
});

test('statusCode and header validation match node', async () => {
  const attempts = [
    ['info status on the setter', (res) => {
      res.statusCode = 150;
    }],
    ['out of range on the setter', (res) => {
      res.statusCode = 600;
    }],
    ['writeHead after send', (res) => {
      res.writeHead(200); res.writeHead(204);
    }],
    ['setHeader after send', (res) => {
      res.writeHead(200); res.setHeader('a', 'b');
    }],
    ['connection header', (res) => {
      res.writeHead(200, { connection: 'keep-alive' });
    }],
    ['bad header name', (res) => {
      res.writeHead(200, { 'bad name': 'x' });
    }],
    ['newline in a value', (res) => {
      res.writeHead(200, { ok: 'a\nb' });
    }]
  ];
  const outcomes = async (impl) => {
    const seen = [];
    const handler = (req, res, log, i) => {
      try {
        attempts[i][1](res);
        seen.push('ok');
      } catch (e) {
        seen.push(e.code || e.constructor.name);
      }
      res.end('x');
    };
    const script = async (client) => {
      for (let i = 0; i < attempts.length; i++) {
        const settled = await request(client, GET).catch((e) => e.code);
        if (!settled) {
          break;
        }
      }
      return seen.length;
    };
    await probe(impl, handler, script);
    return seen;
  };

  const native = await outcomes(nodeHttp2);
  const ours = await outcomes(h2lean);
  assert.strictEqual(ours.length, native.length);
  for (let i = 0; i < native.length; i++) {
    assert.strictEqual(ours[i], native[i], `${attempts[i][0]}: node=${native[i]} h2lean=${ours[i]}`);
  }
});

test('write(chunk) applies backpressure and resumes with drain', async () => {
  const srv = h2lean.createServer((req, res) => {
    // 2 MB against a 64 KB window: write() must start returning false
    const big = Buffer.alloc(64 * 1024, 0x62);
    res.writeHead(200, { 'content-length': String(2 * 1024 * 1024) });
    let stalled = false;
    let n = 0;
    const pump = () => {
      while (n < 32) {
        n++;
        if (!res.write(big)) {
          stalled = true; return;
        }
      }
      res.end();
    };
    res.on('drain', () => {
      if (stalled) {
        pump();
      }
    });
    pump();
    srv._drainSeen = stalled;
  });
  const port = await new Promise((resolve) => srv.listen(0, '127.0.0.1', () => resolve(srv.address().port)));
  const client = require('node:http2').connect(`http://127.0.0.1:${port}`);
  client.on('error', () => {});
  const res = await request(client, GET);
  client.destroy();
  srv.close();
  assert.strictEqual(res.body.length, 2 * 1024 * 1024);
  assert.strictEqual(res.headers['content-length'], String(2 * 1024 * 1024));
  assert.strictEqual(srv._drainSeen, true, 'write must have returned false at least once');
});

test('compat chunks stay valid long after the tick that delivered them', async () => {
  // A Readable queues chunks for an unknown amount of time, so what the session
  // hands over must not be recycled underneath it; waiting 20 ms here guarantees
  // the session has handled many more frames since.
  const payload = 'z'.repeat(40 * 1024);
  const srv = h2lean.createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) {
      assert.ok(Buffer.isBuffer(chunk));
      chunks.push(chunk);
    }
    const text = Buffer.concat(chunks).toString('latin1');
    await new Promise((resolve) => setTimeout(resolve, 20));
    res.writeHead(200, { 'x-sum': String(text.length), 'x-uniq': String(new Set(text).size) });
    res.end(text.slice(-8));
  });
  const port = await new Promise((resolve) => srv.listen(0, '127.0.0.1', () => resolve(srv.address().port)));
  const client = require('node:http2').connect(`http://127.0.0.1:${port}`);
  client.on('error', () => {});
  const res = await request(client, { ...GET, ':method': 'POST' }, payload);
  client.destroy();
  srv.close();
  assert.strictEqual(res.headers['x-sum'], String(payload.length));
  assert.strictEqual(res.headers['x-uniq'], '1', 'no other bytes may have landed in the chunks');
  assert.strictEqual(res.body, 'zzzzzzzz');
});

test('a peer reset reports "aborted" exactly while the response is still open', async () => {
  // node's rule is about the RESPONSE, not about the request: "if the writable
  // side of the Http2Stream is still open, emit the 'aborted' event"
  // (internal/http2/core.js, closeStream). The request is complete in both
  // shapes below -- the peer sent the whole body it promised and never sent
  // END_STREAM -- so a rule about the request would answer 'no' twice.
  // Measured against a live node server (out/probe-aborted-rule.js): a peer
  // that resets a stream whose response never started does report 'aborted',
  // and one that resets a stream whose response was ended does not.
  const POST = { ...GET, ':method': 'POST', 'content-length': '1' };
  const shapes = {
    unanswered: (req, res, log) => {
      req.on('aborted', () => log.push('req.aborted'));
      req.on('close', () => log.push('req.close'));
      req.on('end', () => log.push('req.end'));
      req.resume();
    },
    answered: (req, res, log) => {
      req.on('aborted', () => log.push('req.aborted'));
      req.on('close', () => log.push('req.close'));
      req.on('end', () => log.push('req.end'));
      req.resume();
      res.writeHead(200);
      res.end('ok');
    }
  };
  const script = async (client) => {
    const req = client.request(POST);
    req.on('error', () => {});
    req.write('x'); // the body stays open
    await new Promise((r) => setTimeout(r, 80));
    req.close(8); // the peer resets the stream
    await new Promise((r) => setTimeout(r, 250));
    return null;
  };

  for (const shape of ['unanswered', 'answered']) {
    const native = await probe(nodeHttp2, shapes[shape], script);
    const ours = await probe(h2lean, shapes[shape], script);
    // The trace is compared as a set, not in order: the peer's reset is sent
    // as an empty END_STREAM DATA frame followed by the RST_STREAM
    // (out/probe-client-endstream.js), and whether the two arrive in one read
    // or in two decides whether the request's 'end' lands before or after the
    // 'aborted'. The two stacks order that pair differently, so only the
    // events themselves and the rule below are compared.
    assert.deepStrictEqual([...ours.log].sort(), [...native.log].sort(),
      `events differ for '${shape}'`);
    // 'aborted' is reported before the request's 'close' on both stacks: node
    // emits it from the stream's close callback, h2lean from its own.
    if (ours.log.includes('req.aborted')) {
      assert.ok(ours.log.indexOf('req.aborted') < ours.log.indexOf('req.close'),
        '"aborted" must be reported before the close');
    }
    // and the rule itself: 'aborted' while the response was open, not after it
    if (shape === 'unanswered') {
      assert.deepStrictEqual([...native.log].sort(), ['req.aborted', 'req.close', 'req.end'],
        'node\'s measured events for a reset before the response');
      assert.ok(ours.log.includes('req.aborted'), 'the reset must be reported');
    } else {
      assert.deepStrictEqual([...native.log].sort(), ['req.close', 'req.end'],
        'node\'s measured events for a reset after the response');
      assert.ok(!ours.log.includes('req.aborted'), 'a finished response must not report "aborted"');
    }
  }
});

test('createPushResponse reports that push is not implemented', async () => {
  const srv = h2lean.createServer((req, res) => {
    res.createPushResponse({ 'content-type': 'text/plain' }, (err) => {
      assert.strictEqual(err.code, 'ERR_H2LEAN_NOT_IMPLEMENTED');
      res.writeHead(200, { 'x-push': err.code });
      res.end();
    });
  });
  const port = await new Promise((resolve) => srv.listen(0, '127.0.0.1', () => resolve(srv.address().port)));
  const client = require('node:http2').connect(`http://127.0.0.1:${port}`);
  client.on('error', () => {});
  const res = await request(client, GET);
  client.destroy();
  srv.close();
  assert.strictEqual(res.headers['x-push'], 'ERR_H2LEAN_NOT_IMPLEMENTED');
});
