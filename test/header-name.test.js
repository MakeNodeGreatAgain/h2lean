// The header names the server accepts.
//
// Three properties, and the first two are differential: the same handler runs
// against node:http2 and against h2lite, and the two must agree.
//
//   1. a name that is not a string is refused. node validates the name before
//      it normalises it -- validateString(name, 'name') in every name-taking
//      accessor (internal/http2/compat.js: setTrailer 581, getHeader 596,
//      hasHeader 618, removeHeader 623, setHeader 639, appendHeader 663) --
//      and validateString throws ERR_INVALID_ARG_TYPE for anything that is not
//      a string (internal/validators.js:165). Coercing the name instead, which
//      is what h2lite used to do, was a silent divergence: the name the
//      application meant and the name that reached the wire were not the same
//      one, and the application was never told.
//   2. the name that reaches the wire is the normalised one: trimmed and lower
//      case for the compat layer, lower case for the encoder.
//   3. the name cache is bounded. The names come from application code, and an
//      application that reflects untrusted input into header names must not be
//      able to grow a process-wide cache for the life of the process.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const { probe, request, nodeHttp2 } = require('./diff-harness');
const { nameCacheSize, trimName, lowerName } = require('../lib/headers');
const h2lite = require('../index');

const GET = {
  ':method': 'GET',
  ':path': '/',
  ':scheme': 'http',
  ':authority': 'localhost'
};

// The six accessors that take a name. node's own words for a name that is not
// a string, read off a live server (out/probe-nonname.js):
//   The "name" argument must be of type string. Received type number (123)
const CALLS = ['setHeader', 'getHeader', 'hasHeader', 'removeHeader', 'appendHeader', 'setTrailer'];

test('a header name that is not a string is refused, on both stacks alike', async () => {
  const handler = (req, res, log) => {
    for (const call of CALLS) {
      let out;
      try {
        res[call](123, 'v');
        out = 'no throw';
      } catch (e) {
        out = `${e.code || e.name}: ${e.message}`;
      }
      log.push(`${call}: ${out}`);
    }
    // the same name written as a string is accepted, and the header map holds it
    res.setHeader('x-ok', 'v');
    log.push(`names: ${JSON.stringify(res.getHeaderNames())}`);
    log.push(`value: ${res.getHeader('x-ok')}`);
    res.end('ok');
  };
  const script = async (client) => {
    const res = await request(client, GET);
    return { body: res.body, headers: res.headers };
  };

  const native = await probe(nodeHttp2, handler, script);
  const ours = await probe(h2lite, handler, script);

  assert.deepStrictEqual(ours.log, native.log, 'what the six accessors do with a number');
  // the refusal is node's own: a TypeError with the code, and the name in the
  // message. Pinned so a change to the wording is a visible one.
  assert.strictEqual(ours.log[0], 'setHeader: ERR_INVALID_ARG_TYPE: '
    + 'The "name" argument must be of type string. Received type number (123)');
  assert.deepStrictEqual(ours.result.headers, native.result.headers);
});

test('a header name is trimmed and lower-cased on the way out, like node does it', async () => {
  const handler = (req, res, log) => {
    res.setHeader('  X-Mixed-Case  ', 'v');
    res.appendHeader('X-Other', 'a');
    res.appendHeader('x-other', 'b');
    log.push(`names: ${JSON.stringify(res.getHeaderNames())}`);
    log.push(`other: ${JSON.stringify(res.getHeader('x-other'))}`);
    log.push(`has: ${res.hasHeader('X-OTHER')}`);
    res.end('ok');
  };
  const script = async (client) => {
    const res = await request(client, GET);
    return { body: res.body, headers: res.headers };
  };

  const native = await probe(nodeHttp2, handler, script);
  const ours = await probe(h2lite, handler, script);

  assert.deepStrictEqual(ours.log, native.log);
  // the map holds the normalised name, and the two spellings of it are one header
  assert.strictEqual(ours.log[0], 'names: ["x-mixed-case","x-other"]');
  assert.strictEqual(ours.log[1], 'other: ["a","b"]');
  assert.deepStrictEqual(ours.result.headers, native.result.headers);
});

test('a response with many distinct names carries every one of them', async () => {
  // 100 names: the peer's own limit is around 130 fields (measured against a
  // live node client, out/probe-many-headers.js -- a response with 150 header
  // fields is refused with NGHTTP2_ENHANCE_YOUR_CALM on both stacks), so this
  // count is under it and the comparison is about the names and not about the
  // peer.
  const COUNT = 100;
  const handler = (req, res) => {
    for (let i = 0; i < COUNT; i++) {
      res.setHeader('x-name-' + i, String(i));
    }
    res.end('ok');
  };
  const script = async (client) => {
    const res = await request(client, GET);
    return {
      count: Object.keys(res.headers).length,
      first: res.headers['x-name-0'],
      last: res.headers['x-name-' + (COUNT - 1)],
      body: res.body
    };
  };

  const native = await probe(nodeHttp2, handler, script);
  const ours = await probe(h2lite, handler, script);

  assert.deepStrictEqual(ours.result, native.result);
  assert.strictEqual(ours.result.count, COUNT + 2, 'the names, plus :status and date');
});

test('the name cache is bounded, and a name past the bound still normalises', () => {
  const before = nameCacheSize();
  // 3000 distinct names: far more than the cache takes
  for (let i = 0; i < 3000; i++) {
    trimName('x-flood-' + i);
  }
  for (let i = 0; i < 3000; i++) {
    lowerName('X-Flood-' + i);
  }
  const after = nameCacheSize();

  assert.ok(after.names - before.names <= 1024,
    `the name cache grew by ${after.names - before.names} entries`);
  assert.ok(after.lowers - before.lowers <= 1024,
    `the lower-case cache grew by ${after.lowers - before.lowers} entries`);
  // the names past the bound are still normalised the same way
  assert.strictEqual(trimName('x-flood-2999'), 'x-flood-2999');
  assert.strictEqual(trimName('  X-Flood-2999  '), 'x-flood-2999');
  assert.strictEqual(lowerName('X-Flood-2999'), 'x-flood-2999');
});
