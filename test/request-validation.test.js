// Inbound request validation (lib/session.js _requestProblem).
//
// RFC 9113 8.1/8.2.2/8.3/8.5 says what a request must contain; it does not say what a
// server does about it. The matrix below was measured against a live node:http2 server
// (which runs nghttp2's lib/nghttp2_http.c) rather than written from the RFC, and it is
// asserted here twice: once against h2lean and once against node, so that a change in
// either shows up as a failing test instead of as a silent difference.
//
// Why this is a security file and not a correctness one: whatever sits in front of us
// translates these fields into HTTP/1.1 lines, and a field we read one way and a
// downstream reader reads another way is request smuggling. CVE-2020-8287 is exactly
// that class in Node.js (two copies of a header field, the first kept and the second
// ignored). The rules that carry it here: a repeated pseudo-header, an empty
// pseudo-header value, a second content-length, `te` other than "trailers",
// connection-specific fields, and a relative :path.
//
// The action matters as much as the rule: RST_STREAM(PROTOCOL_ERROR) and the application
// handler is never reached. A test that only checked "the request failed somehow" would
// still pass if the stream were served and the handler declined it -- which is the
// difference that lets a second reader exist at all.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const f = require('../lib/frames');
const h2lean = require('../index');
const { listen, closeServer } = require('./diff-harness');

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}
// readHeader fills module-level accessors instead of returning a record, so the client
// in this file reads the nine octets itself -- the same three lines as tools/probe-wire
function headOf(buf, off) {
  return { len: (buf[off] << 16) | (buf[off + 1] << 8) | buf[off + 2],
    type: buf[off + 3], flags: buf[off + 4], sid: buf.readUInt32BE(off + 5) & 0x7fffffff };
}
// literal field line without indexing, name not in the static table
function lit(name, value) {
  const n = Buffer.from(name, 'latin1');
  const v = Buffer.from(value, 'latin1');
  return Buffer.concat([Buffer.from([0x00, n.length]), n, Buffer.from([v.length]), v]);
}
const B = (...bytes) => Buffer.from(bytes, 'latin1');
const AUTH = lit(':authority', 'localhost');
// static table indices used below: 0x82 :method GET, 0x84 :path /, 0x86 :scheme http,
// 0x88 :status 200
const GOOD = Buffer.concat([B(0x82, 0x84, 0x86), AUTH]);

// expect: 'serve' -- the handler runs and a response comes back;
//         'rst'    -- RST_STREAM(PROTOCOL_ERROR) and no handler.
const CASES = [
  ['a complete request', GOOD, 'serve'],
  // RFC 9113 8.1: these three are what a request is
  ['missing :method', Buffer.concat([B(0x84, 0x86), AUTH]), 'rst'],
  ['missing :path', Buffer.concat([B(0x82, 0x86), AUTH]), 'rst'],
  ['missing :scheme', Buffer.concat([B(0x82, 0x84), AUTH]), 'rst'],
  // RFC 9113 8.3.1: :authority or host, one of them, is the target
  ['no :authority and no host', B(0x82, 0x84, 0x86), 'rst'],
  ['host in place of :authority',
    Buffer.concat([B(0x82, 0x84, 0x86), lit('host', 'localhost')]), 'serve'],
  // RFC 9113 8.3: at most one of each, and an empty value is not the same as none
  ['duplicate :method', Buffer.concat([B(0x82, 0x82, 0x84, 0x86), AUTH]), 'rst'],
  ['empty :method', Buffer.concat([lit(':method', ''), B(0x84, 0x86), AUTH]), 'rst'],
  ['empty :path', Buffer.concat([B(0x82, 0x86), AUTH, lit(':path', '')]), 'rst'],
  // The empty-value rule has to be tested where nothing else covers it: with a scheme that
  // is not http/https the leading-slash rule does not apply, so an empty :path would ride
  // through if `value.length === 0` were dropped.
  ['empty :path with a non-http scheme',
    Buffer.concat([B(0x82), lit(':scheme', 'aaa'), lit(':path', ''), AUTH]), 'rst'],
  // RFC 9113 8.1: pseudo-headers come before every regular field
  ['regular field before pseudo',
    Buffer.concat([lit('accept', '*/*'), B(0x82, 0x84, 0x86), AUTH]), 'rst'],
  // a server never receives :status, and never receives a pseudo it does not know
  [':status in a request', Buffer.concat([B(0x82, 0x84, 0x86, 0x88), AUTH]), 'rst'],
  ['unknown pseudo-header',
    Buffer.concat([B(0x82, 0x84, 0x86), AUTH, lit(':proto', '1')]), 'rst'],
  // RFC 9113 8.3.1: :path is an absolute path; 8.5: OPTIONS * is the one exception
  [':path without a leading slash',
    Buffer.concat([B(0x82, 0x86), AUTH, lit(':path', 'abc')]), 'rst'],
  ['OPTIONS * as the path',
    Buffer.concat([lit(':method', 'OPTIONS'), B(0x86), AUTH, lit(':path', '*')]), 'serve'],
  // RFC 9113 8.5: CONNECT carries an authority and neither :scheme nor :path
  ['CONNECT with :authority only',
    Buffer.concat([lit(':method', 'CONNECT'), AUTH]), 'serve'],
  ['CONNECT with :scheme',
    Buffer.concat([lit(':method', 'CONNECT'), B(0x86), AUTH]), 'rst'],
  ['CONNECT with :path',
    Buffer.concat([lit(':method', 'CONNECT'), B(0x84, 0x86), AUTH]), 'rst'],
  // RFC 8441 extended CONNECT needs the setting we advertise off. The request is complete
  // apart from that: with :scheme in it, the gate is the only rule refusing it, which is
  // what makes the gate testable rather than hidden behind a missing-field error.
  [':protocol without enableConnectProtocol',
    Buffer.concat([lit(':method', 'CONNECT'), lit(':protocol', 'websocket'), B(0x84, 0x86),
      AUTH]),
    'rst'],
  // RFC 9113 8.2.2: connection-specific fields must not be sent in HTTP/2 at all
  ['connection: close', Buffer.concat([GOOD, lit('connection', 'close')]), 'rst'],
  ['transfer-encoding: chunked',
    Buffer.concat([GOOD, lit('transfer-encoding', 'chunked')]), 'rst'],
  // RFC 9113 8.2.2: the TE exception allows no value but "trailers"
  ['te: chunked', Buffer.concat([GOOD, lit('te', 'chunked')]), 'rst'],
  ['te: trailers', Buffer.concat([GOOD, lit('te', 'trailers')]), 'serve'],
  // CVE-2020-8287: two copies of a field, read differently by two receivers
  ['duplicate content-length',
    Buffer.concat([GOOD, lit('content-length', '5'), lit('content-length', '5')]), 'rst'],
  ['content-length that is not a number',
    Buffer.concat([GOOD, lit('content-length', 'abc')]), 'rst'],
  // RFC 9113 8.2.1: names are lower case -- and an upper-case name is a hard error,
  // unlike a name with an illegal character in it, which only costs the field
  ['upper-case field name', Buffer.concat([GOOD, lit('Accept', '*/*')]), 'rst'],
  ['field name with a space', Buffer.concat([GOOD, lit('bad name', 'x')]), 'serve'],
  // The same rule for the VALUE: a character that may not appear in a field value
  // costs the field, and the stream lives. Measured on node, character by character
  // (out/probe-value-matrix.js): every control character but HTAB, and DEL.
  ['field value with a CR', Buffer.concat([GOOD, lit('x-a', '1\rb')]), 'serve'],
  ['field value with a NUL', Buffer.concat([GOOD, lit('x-a', '1\u0000b')]), 'serve'],
  ['field value with a DEL', Buffer.concat([GOOD, lit('x-a', '1\u007fb')]), 'serve'],
  ['field value with a TAB', Buffer.concat([GOOD, lit('x-a', '1\tb')]), 'serve'],
  // the value shapes: RFC 9110 9 method token, RFC 3986 3.1 scheme, RFC 3986 3.2
  // authority. Measured on node, which refuses all three rather than normalising them.
  [':method containing a space',
    Buffer.concat([lit(':method', 'GE T'), B(0x84, 0x86), AUTH]), 'rst'],
  [':scheme starting with a digit',
    Buffer.concat([B(0x82, 0x84), AUTH, lit(':scheme', '1http')]), 'rst'],
  [':authority containing a slash',
    Buffer.concat([B(0x82, 0x84, 0x86), lit(':authority', 'a/b')]), 'rst']
];

// One connection, one request, and the verdict as a string: what the server answered and
// whether the application was reached.
async function drive(impl, block) {
  const srv = impl.createServer();
  const reached = [];
  // node and h2lean give the handler the same arguments here, so one listener serves
  // both -- which is the point of the comparison, not an accident of it
  srv.on('stream', (stream, headers) => {
    reached.push(Object.keys(headers).join(','));
    // a body as well, so that a served request is two frames and a refused one is one
    // RST; a headers-only response would be ambiguous against a reset after HEADERS
    stream.respond({ ':status': 200 });
    stream.end('ok');
  });
  srv.on('error', () => {}); // a reset stream surfaces here too
  const port = await listen(srv);

  const verdict = await new Promise((resolve) => {
    const out = [];
    const sock = net.connect(port, '127.0.0.1');
    let acc = Buffer.alloc(0);
    sock.on('error', () => {});
    sock.on('data', (d) => {
      acc = Buffer.concat([acc, d]);
      for (;;) {
        if (acc.length < f.HEADER_LEN) {
          return;
        }
        const h = headOf(acc, 0);
        if (acc.length < f.HEADER_LEN + h.len) {
          return;
        }
        const payload = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + h.len);
        acc = acc.subarray(f.HEADER_LEN + h.len);
        if (h.type === f.SETTINGS && !(h.flags & f.F_ACK)) {
          sock.write(frame(f.SETTINGS, f.F_ACK, 0, B()));
          continue;
        }
        if (h.type === f.RST_STREAM) {
          out.push(`rst:${payload.readUInt32BE(0)}`);
        } else if (h.type === f.GOAWAY) {
          out.push(`goaway:${payload.readUInt32BE(4)}`);
        } else if (h.type === f.HEADERS) {
          out.push('headers');
        } else if (h.type === f.DATA) {
          out.push('data');
        }
      }
    });
    sock.on('connect', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, B()),
        frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, block)]));
    });
    // long enough that a server which intends to answer has answered
    setTimeout(() => {
      sock.destroy(); resolve(out.length ? out.join('+') : 'nothing');
    }, 250);
  });
  await closeServer(srv);
  return { verdict, reached };
}

const expect = (v, reached) => (v === 'headers+data' && reached.length === 1 ? 'serve'
  : v === 'rst:1' && reached.length === 0 ? 'rst' : `unexpected:${v}:${reached.length}`);

for (const impl of [{ name: 'h2lean', mod: h2lean }, { name: 'node', mod: require('node:http2') }]) {
  test(`${impl.name}: ${CASES.length} inbound request cases answer as measured`, async () => {
    const fails = [];
    for (const [name, block, want] of CASES) {
      const { verdict, reached } = await drive(impl.mod, block);
      const got = expect(verdict, reached);
      if (got !== want) {
        fails.push(`${name}: wanted ${want}, got ${got} (${verdict})`);
      }
    }
    assert.deepStrictEqual(fails, []);
  });
}

test('the two implementations hand the application the same fields', async () => {
  // 'bad name' is dropped rather than refused: node gives the handler no such key, and
  // neither may we. The same goes for a field whose VALUE carries a character that may
  // not appear in one: the field goes, the request is served. Everything else about the
  // request still has to arrive.
  const block = Buffer.concat([GOOD, lit('bad name', 'x'), lit('x-ok', '1'),
    lit('x-cr', '1\rb')]);
  const lite = await drive(h2lean, block);
  const node = await drive(require('node:http2'), block);
  assert.strictEqual(lite.reached[0], node.reached[0]);
  assert.ok(lite.reached[0].includes('x-ok'), 'the good field survives');
  assert.ok(!lite.reached[0].includes('bad name'), 'the illegal name is dropped');
  assert.ok(!lite.reached[0].includes('x-cr'), 'the illegal value costs the field');
});

test('a dropped field stays dropped when the shape is served from the cache', async () => {
  // The second request is the one that matters. The first decodes, drops the field and
  // stores what is left as the shape; the second is answered from that stored list
  // without decoding at all. If the entry kept the pre-drop list, the illegal name
  // would come back on every request after the first -- one request of testing that
  // would never show it.
  const srv = h2lean.createServer();
  const reached = [];
  srv.on('stream', (stream, headers) => {
    reached.push(Object.keys(headers));
    stream.respond({ ':status': 200 }, { endStream: true });
  });
  srv.on('error', () => {});
  const port = await listen(srv);

  const block = Buffer.concat([GOOD, lit('bad name', 'x'), lit('x-ok', '1')]);
  await new Promise((resolve) => {
    const sock = net.connect(port, '127.0.0.1');
    sock.on('data', () => {});
    sock.on('connect', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, B()),
        frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, block),
        frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 3, block),
        frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 5, block)]));
    });
    setTimeout(() => {
      sock.destroy(); resolve();
    }, 350);
  });
  await closeServer(srv);

  assert.strictEqual(reached.length, 3, 'all three requests were served');
  for (const keys of reached) {
    assert.ok(!keys.includes('bad name'), `the dropped field came back: ${keys.join(',')}`);
    assert.ok(keys.includes('x-ok'));
  }
});

test('a refused shape is refused again on the same connection', async () => {
  // The verdict is cached with the request shape (that is what keeps the check off the
  // per-request cost path). The cache must not turn a second copy of a bad request into
  // an accepted one, and a good shape must still serve after a bad one was cached.
  const srv = h2lean.createServer();
  const reached = [];
  srv.on('stream', (stream, headers) => {
    reached.push(headers[':path']);
    stream.respond({ ':status': 200 }, { endStream: true });
  });
  srv.on('error', () => {});
  const port = await listen(srv);

  const bad = Buffer.concat([B(0x84, 0x86), AUTH]); // no :method
  const res = await new Promise((resolve) => {
    const out = [];
    const sock = net.connect(port, '127.0.0.1');
    let acc = Buffer.alloc(0);
    sock.on('data', (d) => {
      acc = Buffer.concat([acc, d]);
      for (;;) {
        if (acc.length < f.HEADER_LEN) {
          return;
        }
        const h = headOf(acc, 0);
        if (acc.length < f.HEADER_LEN + h.len) {
          return;
        }
        const payload = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + h.len);
        acc = acc.subarray(f.HEADER_LEN + h.len);
        if (h.type === f.SETTINGS && !(h.flags & f.F_ACK)) {
          sock.write(frame(f.SETTINGS, f.F_ACK, 0, B())); continue;
        }
        if (h.type === f.RST_STREAM) {
          out.push(`rst:${payload.readUInt32BE(0)}`);
        } else if (h.type === f.HEADERS) {
          out.push('headers');
        }
      }
    });
    // three streams: bad, good, bad again. Even ids are refused by the session before
    // validation, so the second bad request uses the next odd id.
    sock.on('connect', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, B()),
        frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, bad),
        frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 3, GOOD),
        frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 5, bad)]));
    });
    setTimeout(() => {
      sock.destroy(); resolve(out);
    }, 400);
  });
  await closeServer(srv);
  assert.deepStrictEqual(res, ['rst:1', 'headers', 'rst:1']);
  assert.deepStrictEqual(reached, ['/']);
});

test('a trailer block may not contain pseudo-headers', async () => {
  // RFC 9113 8.1: the fields after the header section are regular fields only. Checked
  // separately because the trailer block goes through the same decode and a different
  // branch of the validator (nothing is cached for it).
  const srv = h2lean.createServer();
  const reached = [];
  srv.on('stream', (stream, _headers) => {
    reached.push('stream');
    stream.on('trailers', () => reached.push('trailers'));
    stream.respond({ ':status': 200 });
    stream.end('body');
  });
  srv.on('error', () => {});
  const port = await listen(srv);

  const res = await new Promise((resolve) => {
    const out = [];
    const sock = net.connect(port, '127.0.0.1');
    let acc = Buffer.alloc(0);
    sock.on('data', (d) => {
      acc = Buffer.concat([acc, d]);
      for (;;) {
        if (acc.length < f.HEADER_LEN) {
          return;
        }
        const h = headOf(acc, 0);
        if (acc.length < f.HEADER_LEN + h.len) {
          return;
        }
        const payload = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + h.len);
        acc = acc.subarray(f.HEADER_LEN + h.len);
        if (h.type === f.SETTINGS && !(h.flags & f.F_ACK)) {
          sock.write(frame(f.SETTINGS, f.F_ACK, 0, B())); continue;
        }
        if (h.type === f.RST_STREAM) {
          out.push(`rst:${payload.readUInt32BE(0)}`);
        } else if (h.type === f.HEADERS && (h.flags & f.F_END_HEADERS)) {
          out.push('headers');
        }
      }
    });
    sock.on('connect', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, B()),
        // request without END_STREAM, then a trailer block carrying :method
        frame(f.HEADERS, f.F_END_HEADERS, 1, GOOD),
        frame(f.DATA, 0, 1, Buffer.from('5')),
        frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, 1, B(0x82))
      ]));
    });
    setTimeout(() => {
      sock.destroy(); resolve(out);
    }, 400);
  });
  await closeServer(srv);
  assert.deepStrictEqual(res, ['headers', 'rst:1'], 'the trailers were refused, not emitted');
  assert.deepStrictEqual(reached, ['stream'], "no 'trailers' event for a bad block");
});
