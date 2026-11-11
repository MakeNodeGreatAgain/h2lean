// The request shape cache stores the decoded field list under a hash of the
// HPACK block. That is only sound while the dynamic table stays unchanged,
// because an *indexed* name refers to whatever the newest entry happens to be.
// These blocks are built by hand for exactly that reason: no encoder in the
// project is allowed to produce this sequence, so nothing can mask the bug.
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const f = require('../lib/frames');
const h2lean = require('../index');

// :method GET and :path "/" taken from the static table (indices 2 and 4), :scheme http
// (6), and :authority as a literal with an indexed name (static 1). The last two are not
// incidental: RFC 9113 8.1 and 8.3.1 require :method, :path, :scheme and one of
// :authority/host, and a request missing any of them is refused before the shape cache
// is consulted -- so a block that omits them tests the refusal, not the cache.
// Nothing here inserts: a literal without indexing leaves the dynamic table alone, which
// is what makes a cache hit observable.
const PREFIX = Buffer.from([0x82, 0x84, 0x86, 0x01, 9,
  ...Buffer.from('localhost', 'latin1')]);

// 01xxxxxx: literal with incremental indexing, name index 0 = a new name
function insertLiteral(name, value) {
  const n = Buffer.from(name, 'latin1');
  const v = Buffer.from(value, 'latin1');
  return Buffer.from([0x40, n.length, ...n, v.length, ...v]);
}

// 0000nnnn: literal WITHOUT indexing, the name is index 62 = dynamic index 1 =
// the newest entry of the table. Because nothing is inserted, this block can be
// repeated without disturbing the cache -- which is what makes a hit observable.
// 4 bits of index are not enough for 62, so 15 signals a continuation: 15+47.
function refNewestNoInsert(value) {
  const v = Buffer.from(value, 'latin1');
  return Buffer.from([0x0f, 47, v.length, ...v]);
}

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// Sends the blocks over one raw connection and returns the headers object the
// server handed to each stream, by stream id.
async function sendBlocks(srv, blocks, port) {
  const outbound = [f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))];
  blocks.forEach((block, i) => {
    outbound.push(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1 + i * 2, block));
  });
  await new Promise((resolve, reject) => {
    const sock = net.connect(port, '127.0.0.1', () => {
      sock.write(Buffer.concat(outbound));
      setTimeout(() => {
        sock.destroy(); resolve();
      }, 250);
    });
    sock.setTimeout(3000);
    sock.on('timeout', () => {
      sock.destroy(); reject(new Error('server went silent'));
    });
    sock.on('error', () => {});
  });
}

test('a repeated header block is only cached while the HPACK table holds still', async (t) => {
  const srv = h2lean.createServer();
  const seen = new Map();
  srv.on('stream', (stream, headers) => {
    seen.set(stream.id, { ...headers });
    stream.respond({ ':status': 200 }, { endStream: true });
  });
  // cleanup first: a failed assertion below must not leave the port listening
  t.after(() => srv.close());
  const port = await new Promise((resolve) => {
    srv.listen(0, '127.0.0.1', () => resolve(srv.address().port));
  });

  const REPEATED = Buffer.concat([PREFIX, refNewestNoInsert('same')]);
  const blocks = [
    Buffer.concat([PREFIX, insertLiteral('x-a', '1')]), // sid 1: inserts x-a
    REPEATED, // sid 3: the name is x-a
    REPEATED, // sid 5: cache hit, still x-a
    Buffer.concat([PREFIX, insertLiteral('x-b', '2')]), // sid 7: inserts x-b
    REPEATED // sid 9: same bytes, other meaning
  ];
  const outbound = [f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))];
  blocks.forEach((block, i) => {
    outbound.push(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1 + i * 2, block));
  });

  await new Promise((resolve, reject) => {
    const sock = net.connect(port, '127.0.0.1', () => {
      sock.write(Buffer.concat(outbound));
      setTimeout(() => {
        sock.destroy(); resolve();
      }, 250);
    });
    sock.setTimeout(3000);
    sock.on('timeout', () => {
      sock.destroy(); reject(new Error('server went silent'));
    });
    sock.on('error', () => {});
  });

  assert.strictEqual(seen.get(1)['x-a'], '1');
  assert.strictEqual(seen.get(3)['x-a'], 'same');
  assert.strictEqual(seen.get(5)['x-a'], 'same', 'the repeated block must still decode the same');
  assert.strictEqual(seen.get(7)['x-b'], '2');
  assert.strictEqual(seen.get(9)['x-b'], 'same',
    'the byte-identical block must be re-decoded after the table changed');
  assert.strictEqual(seen.get(9)['x-a'], undefined);
});

// Same hazard, but with shareHeaders on, where the cached value is the object
// the application holds: a missing invalidation would hand request 9 the very
// same frozen object request 3 saw, i.e. the wrong headers by identity too.
test('shareHeaders on: a changed HPACK table never reuses the shared object', async (t) => {
  const seen = new Map();
  const srv = h2lean.createServer({ shareHeaders: true });
  srv.on('stream', (stream, headers) => {
    seen.set(stream.id, headers);
    stream.respond({ ':status': 200 }, { endStream: true });
  });
  t.after(() => srv.close());
  const port = await new Promise((resolve) => {
    srv.listen(0, '127.0.0.1', () => resolve(srv.address().port));
  });

  const REPEATED = Buffer.concat([PREFIX, refNewestNoInsert('same')]);
  await sendBlocks(srv, [
    Buffer.concat([PREFIX, insertLiteral('x-a', '1')]), // sid 1: inserts x-a
    REPEATED, // sid 3: the name is x-a
    REPEATED, // sid 5: the shared object
    Buffer.concat([PREFIX, insertLiteral('x-b', '2')]), // sid 7: inserts x-b
    REPEATED // sid 9: same bytes, other meaning
  ], port);

  assert.strictEqual(seen.get(3)['x-a'], 'same');
  assert.strictEqual(seen.get(5), seen.get(3), 'the repeating block is one shared object');
  assert.strictEqual(Object.isFrozen(seen.get(3)), true);
  assert.notStrictEqual(seen.get(9), seen.get(3), 'a stale shared object must not be reused');
  assert.strictEqual(seen.get(9)['x-b'], 'same');
  assert.strictEqual(seen.get(9)['x-a'], undefined);
});
