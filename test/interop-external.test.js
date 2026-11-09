// Interop with clients that belong neither to this repository nor to node: nghttp and curl, each
// built on its own nghttp2 library, driven against an h2lite server the way any real peer would be.
//
// Why the differential suite does not cover this: every other file here compares h2lite with
// node:http2, so a behaviour the two share is invisible no matter how many other implementations
// reject it. These tests ask the opposite question -- does an INDEPENDENT stack accept what h2lite
// puts on the wire -- and the answer comes from a binary this repository does not control, so the
// file skips when the binary is absent instead of failing a machine that never had it.
//
// The cost of that skip is real and is written down: without nghttp2 and curl this file proves
// nothing. It is a gate for a machine that has them, and `npm test` reports it as skipped rather
// than passing silently.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const { spawn, spawnSync } = require('node:child_process');
const h2lite = require('../index');
const certificates = require('./fixtures-tls');
const { listen, closeServer } = require('./diff-harness');

const OUT = path.join(__dirname, '..', 'out');
const BODY = 'h2lite interop\n';

// A version line and a zero status is enough to know the binary is there and runnable; a machine
// without it gets the skip rather than a failure that looks like a protocol bug.
function available(bin) {
  const r = spawnSync(bin, ['--version'], { encoding: 'utf8', timeout: 5000 });
  return !r.error && r.status === 0;
}
const NGHTTP = available('nghttp');
const CURL = available('curl');

// One handler for every case, and it answers with the body it received -- a POST is echoed, a GET
// gets the fixed line -- so the same run exercises the outbound HEADERS/DATA path and the inbound
// DATA path against a peer that was not written for this library. `stats.peak` is how a test tells
// two concurrent streams on one connection from two sequential connections, which the client's own
// exit code cannot say.
function serve(tls) {
  const stats = { peak: 0, live: 0, streams: 0 };
  const srv = tls
    ? h2lite.createSecureServer({ key: certificates().key, cert: certificates().cert })
    : h2lite.createServer();
  srv.on('stream', (stream) => {
    stream.on('close', () => {
      stats.live--;
    });
    stats.live++; stats.streams++;
    if (stats.live > stats.peak) {
      stats.peak = stats.live;
    }
    const chunks = [];
    stream.on('data', (c) => chunks.push(c));
    stream.on('error', () => {});
    stream.on('end', () => {
      const body = chunks.length ? Buffer.concat(chunks) : Buffer.from(BODY);
      stream.respond({ ':status': 200, 'content-type': 'text/plain',
        'content-length': body.length });
      stream.end(body);
    });
  });
  srv.on('error', () => {});
  return listen(srv).then((port) => ({ srv, port, stats }));
}

// Asynchronous on purpose: the server answering these requests runs in THIS process, so a blocking
// spawnSync would freeze the event loop it needs and every case would end in the client's own
// timeout -- which reads as an interop failure and is nothing of the sort. The timer kills a client
// that hangs on a malformed answer instead of letting it take the suite timeout down with it.
// `raw` is the untouched stdout: a body that must arrive octet for octet cannot be compared after
// a utf8 decoding, which is where a non-text payload would lose exactly the bytes under test.
function run(bin, args, ms) {
  return new Promise((resolve, reject) => {
    const child = spawn(bin, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    const parts = [];
    let err = '';
    let timedOut = false;
    child.stdout.on('data', (d) => parts.push(d));
    child.stderr.on('data', (d) => {
      err += d;
    });
    child.on('error', reject);
    const timer = setTimeout(() => {
      timedOut = true; child.kill('SIGTERM');
    }, ms || 8000);
    child.on('close', (code) => {
      clearTimeout(timer);
      const raw = Buffer.concat(parts);
      resolve({ status: timedOut ? `timeout after ${ms || 8000} ms` : code, raw,
        out: raw.toString() + err });
    });
  });
}

test('nghttp fetches a GET over h2c and gets the body back',
  { skip: NGHTTP ? false : 'nghttp not installed' }, async () => {
    const { srv, port } = await serve(false);
    try {
      const r = await run('nghttp', [`http://127.0.0.1:${port}/`]);
      assert.strictEqual(r.status, 0, `nghttp: ${r.out}`);
      assert.strictEqual(r.out, BODY);
    } finally {
      await closeServer(srv);
    }
  });

test('nghttp puts two streams for one URI on a single connection',
  { skip: NGHTTP ? false : 'nghttp not installed' }, async () => {
    const { srv, port, stats } = await serve(false);
    try {
      // -m 2 requests the URI twice. What is asserted is not that nghttp succeeded twice -- that
      // would also be true over two connections -- but that the server saw two streams alive at
      // the same moment, which only a multiplexed connection on a peer that does not know this
      // library can show.
      const r = await run('nghttp', ['-m', '2', `http://127.0.0.1:${port}/`]);
      assert.strictEqual(r.status, 0, `nghttp: ${r.out}`);
      assert.strictEqual(stats.streams, 2, `streams opened: ${stats.streams}, output: ${r.out}`);
      assert.strictEqual(stats.peak, 2, `never concurrent: ${JSON.stringify(stats)}`);
      assert.strictEqual(r.out.split(BODY.trim()).length - 1, 2, r.out);
    } finally {
      await closeServer(srv);
    }
  });

test('nghttp POSTs a body and receives it back intact',
  { skip: NGHTTP ? false : 'nghttp not installed' }, async () => {
    const { srv, port } = await serve(false);
    const file = path.join(OUT, 'interop-post.bin');
    // 208 octets: a body the client may hand over in one DATA frame or in several, and not a
    // multiple of anything that would let an off-by-one in the reassembly look like a pass. The
    // first four carry the length as a binary integer, so the payload is not valid text and an
    // echo that mangled any octet shows up in the byte comparison instead of hiding in a decode.
    const payload = Buffer.alloc(208, 0x61);
    payload.writeUInt32BE(payload.length, 0);
    fs.mkdirSync(OUT, { recursive: true });
    fs.writeFileSync(file, payload);
    try {
      const r = await run('nghttp', ['-d', file, `http://127.0.0.1:${port}/`]);
      assert.strictEqual(r.status, 0, `nghttp: ${r.out}`);
      assert.strictEqual(r.raw.length, payload.length,
        `echoed ${r.raw.length} of ${payload.length} octets`);
      assert.strictEqual(r.raw.compare(payload), 0, 'the echoed bytes differ from the posted ones');
    } finally {
      await closeServer(srv);
    }
  });

test('nghttp negotiates h2 over TLS and fetches a GET',
  { skip: NGHTTP ? false : 'nghttp not installed' }, async () => {
    const { srv, port } = await serve(true);
    try {
      // -y because the fixture certificate is self-signed: the point is the ALPN selection and the
      // frame exchange under TLS, not a trust chain that no loopback certificate can have.
      const r = await run('nghttp', ['-y', `https://127.0.0.1:${port}/`]);
      assert.strictEqual(r.status, 0, `nghttp: ${r.out}`);
      assert.strictEqual(r.out, BODY);
    } finally {
      await closeServer(srv);
    }
  });

test('curl accepts the answer with --http2-prior-knowledge',
  { skip: CURL ? false : 'curl not installed' }, async () => {
    const { srv, port } = await serve(false);
    try {
      // libcurl links its own nghttp2 build, which need not be the one node ships, and
      // %{http_version} is curl's own verdict on which protocol the server actually spoke.
      const r = await run('curl', ['--http2-prior-knowledge', '-sS',
        '-w', '%{http_version} %{http_code}', `http://127.0.0.1:${port}/`]);
      assert.strictEqual(r.status, 0, `curl: ${r.out}`);
      assert.ok(r.out.endsWith('2 200'), `curl saw: ${r.out}`);
      assert.ok(r.out.includes(BODY.trim()), r.out);
    } finally {
      await closeServer(srv);
    }
  });
