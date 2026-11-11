// Inbound flow control. Credit is released when the application takes the bytes,
// so an upload nobody reads costs the sender instead of our heap -- and a late
// reader must still get every byte, in order.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const { listen, connect, request } = require('./diff-harness');
const h2lean = require('../index');

const POST = { ':method': 'POST', ':path': '/up', ':scheme': 'http', ':authority': 'localhost' };

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function bufferedBytes(srv) {
  let n = 0;
  for (const session of srv._sessions) {
    for (const stream of session._streams.values()) {
      const pending = stream._pending;
      if (pending !== null) {
        for (const chunk of pending) {
          n += chunk.length;
        }
      }
    }
  }
  return n;
}

test('a reader that attaches late still gets every byte, in order', async (t) => {
  const srv = h2lean.createServer();
  const received = [];
  let ended = false;
  srv.on('stream', (stream) => {
    stream.respond({ ':status': 200 });
    // deliberately after the read batch that carried the body has been handled
    setTimeout(() => {
      stream.on('data', (d) => received.push(d));
      stream.on('end', () => {
        ended = true; stream.end();
      });
    }, 60);
  });
  t.after(() => srv.close());
  const port = await listen(srv);
  const client = connect(port);
  t.after(() => client.destroy());

  const payload = Buffer.alloc(40000, 0x6b);
  const res = await request(client, POST, payload);

  assert.strictEqual(ended, true);
  assert.strictEqual(Buffer.concat(received).length, payload.length);
  assert.ok(Buffer.concat(received).equals(payload), 'the replayed bytes must be intact');
  assert.strictEqual(res.headers[':status'], 200);
});

test('a body nobody reads stalls the sender, not the server', async (t) => {
  const srv = h2lean.createServer();
  const window = srv.settings.initialWindowSize;
  const streams = new Map();
  srv.on('stream', (stream) => {
    streams.set(stream.id, stream);
    stream.respond({ ':status': 200 }); // answer without reading the body
  });
  t.after(() => srv.close());
  const port = await listen(srv);
  const client = connect(port);
  t.after(() => client.destroy());

  const req = client.request(POST);
  req.on('error', () => {});
  req.resume();
  const chunk = Buffer.alloc(256 * 1024, 0x7a);
  let sent = 0;
  for (let i = 0; i < 9; i++) {
    req.write(chunk); sent += chunk.length;
  }
  req.end();
  await sleep(250);

  const stream = [...streams.values()];
  assert.strictEqual(stream.length, 1);
  const stalled = stream[0];
  assert.strictEqual(stalled.state.remoteClose, 0, 'END_STREAM must not have arrived yet');
  assert.ok(bufferedBytes(srv) <= window,
    `buffered ${bufferedBytes(srv)} bytes, advertised window ${window}`);

  // reading it must let the rest through -- no deadlock, no loss
  let received = 0;
  const done = new Promise((r) => stalled.on('end', r));
  stalled.on('data', (d) => {
    received += d.length;
  });
  await done;

  assert.strictEqual(received, sent, 'the whole upload must arrive once read');
  assert.ok(stalled.state.localWindowSize <= window, 'and the window stays inside the limit');
});

test('closing a stream with an unread body returns its credit to the connection', async (t) => {
  const srv = h2lean.createServer();
  let served = 0;
  srv.on('stream', (stream, headers) => {
    if (headers['x-final'] !== undefined) {
      // this one we read properly: it only completes if the connection window
      // is still intact after the six refusals
      let n = 0;
      stream.on('data', (d) => {
        n += d.length;
      });
      stream.on('end', () => {
        served++;
        stream.respond({ ':status': 200, 'x-len': String(n) });
        stream.end();
      });
      return;
    }
    served++;
    stream.respond({ ':status': 501 });
    stream.close(8); // cancel without reading the body
  });
  t.after(() => srv.close());
  const port = await listen(srv);
  const client = connect(port);
  t.after(() => client.destroy());

  for (let i = 0; i < 6; i++) {
    const req = client.request(POST);
    req.on('error', () => {});
    req.resume();
    req.end(Buffer.alloc(20480, 0x63)); // more than a whole window in total
    await sleep(40);
  }
  assert.strictEqual(served, 6);

  const res = await request(client, { ...POST, 'x-final': '1' }, Buffer.alloc(20480, 0x64));
  assert.strictEqual(res.headers['x-len'], '20480',
    'the session still works: no connection-window leak after the refusals');
});

test('pause() mid-body holds the sender; resume() delivers the rest intact', async (t) => {
  const payload = Buffer.alloc(300000, 0x7c);
  const srv = h2lean.createServer();
  const window = srv.settings.initialWindowSize;
  const chunks = [];
  const seen = {};
  srv.on('stream', (stream) => {
    stream.respond({ ':status': 200 });
    stream.on('data', (d) => {
      chunks.push(Buffer.from(d));
      if (!seen.paused) {
        stream.pause(); seen.paused = stream.isPaused();
      }
    });
    stream.on('end', () => {
      seen.ended = true; stream.end();
    });
    // by now the sender must be out of credit: everything it managed to push is
    // sitting in our buffer, undelivered
    setTimeout(() => {
      seen.delivered = chunks.length;
      const pending = stream._pending;
      seen.buffered = pending === null ? 0 : pending.reduce((n, c) => n + c.length, 0);
      stream.resume();
    }, 150);
  });
  t.after(() => srv.close());
  const port = await listen(srv);
  const client = connect(port);
  t.after(() => client.destroy());

  const res = await request(client, POST, payload);

  assert.strictEqual(seen.paused, true, 'isPaused() reports the pause');
  assert.strictEqual(seen.delivered, 1, 'exactly one chunk was delivered before pausing');
  assert.ok(seen.buffered <= window + 16384,
    `buffered ${seen.buffered} bytes, advertised window ${window}`);
  assert.strictEqual(Buffer.concat(chunks).length, payload.length, 'nothing was lost');
  assert.ok(Buffer.concat(chunks).equals(payload), 'and nothing was corrupted or reordered');
  assert.strictEqual(res.headers[':status'], 200);
});

test('pause() before the body replays intact bytes', async (t) => {
  const payload = Buffer.alloc(50000, 0x6e);
  const srv = h2lean.createServer();
  let replayed = null;
  srv.on('stream', (stream) => {
    stream.pause(); // nothing may be delivered yet
    stream.respond({ ':status': 200 });
    setTimeout(() => {
      const chunks = [];
      stream.on('data', (d) => chunks.push(Buffer.from(d)));
      stream.on('end', () => {
        replayed = Buffer.concat(chunks); stream.end();
      });
    }, 100);
  });
  t.after(() => srv.close());
  const port = await listen(srv);
  const client = connect(port);
  t.after(() => client.destroy());

  await request(client, POST, payload);

  // the buffered chunks outlive the tick they arrived in and must still be intact
  assert.ok(replayed !== null && replayed.equals(payload),
    'replayed data must not be a recycled view');
});

test('pause()/resume() cycles do not leak the connection window', async (t) => {
  const srv = h2lean.createServer();
  let completed = 0;
  srv.on('stream', (stream) => {
    stream.respond({ ':status': 200 });
    let total = 0;
    stream.on('data', (d) => {
      total += d.length;
      stream.pause(); // stop after every chunk
      setTimeout(() => stream.resume(), 1);
    });
    stream.on('end', () => {
      assert.strictEqual(total, 60000);
      completed++;
      stream.end();
    });
  });
  t.after(() => srv.close());
  const port = await listen(srv);
  const client = connect(port);
  t.after(() => client.destroy());

  // 12 uploads of 60000 bytes each on one connection: bigger than the window per
  // request, so a stalled credit would show up as a hang
  for (let i = 0; i < 12; i++) {
    await request(client, POST, Buffer.alloc(60000, 0x6d));
  }

  assert.strictEqual(completed, 12);
});

test('compat: a handler that reads late still gets the whole body', async (t) => {
  const srv = h2lean.createServer((req, res) => {
    // the body and its END_STREAM land long before the handler asks for data
    setTimeout(() => {
      const chunks = [];
      req.on('data', (d) => chunks.push(d));
      req.on('end', () => {
        res.writeHead(200, { 'x-len': String(Buffer.concat(chunks).length) });
        res.end();
      });
    }, 60);
  });
  t.after(() => srv.close());
  const port = await listen(srv);
  const client = connect(port);
  t.after(() => client.destroy());

  const res = await request(client, POST, Buffer.alloc(40000, 0x66));
  assert.strictEqual(res.headers['x-len'], '40000');
});

test('compat: an unread request gets close and end, like node', async (t) => {
  const events = [];
  const srv = h2lean.createServer((req, res) => {
    // nothing reads req here: attaching 'data' would put the Readable in flowing
    // mode and the body would be consumed, which is not the case under test
    req.on('close', () => events.push('close'));
    req.on('end', () => events.push('end'));
    res.writeHead(200, { 'x-ok': '1' });
    res.end();
  });
  t.after(() => srv.close());
  const port = await listen(srv);
  const client = connect(port);
  t.after(() => client.destroy());

  const body = Buffer.alloc(3000, 0x65);
  const res = await request(client, POST, body);

  assert.strictEqual(res.headers['x-ok'], '1');
  // node reports the 'end' as well, and after the 'close': its compat layer
  // dumps a body nobody read when the stream closes -- "If the user didn't
  // interact with incoming data and didn't pipe it, dump it for compatibility
  // with http1" (internal/http2/compat, onStreamCloseRequest) -- and a Readable
  // that nobody reads never ends until it is resumed. Measured against a live
  // node server (out/probe-unread-end.js): node reports ['close', 'end'] here.
  assert.deepStrictEqual(events, ['close', 'end'],
    'an unread request still ends, after the close, on node too');
});
