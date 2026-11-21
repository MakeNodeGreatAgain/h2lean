// Port of node's `test/parallel/test-http2-respond-file-fd-errors.js` (CONTRIBUTING.md §1:
// the upstream file is a scenario source, node's measured value is the expectation).
//
// Upstream's single block runs the whole argument matrix against ONE stream, in order:
// every wrong fd type, every wrong options.{offset,length,statCheck} type, the three
// payload-forbidden statuses, then respond() followed by respondWithFD (headers already
// sent) and destroy() followed by respondWithFD (stream destroyed). Each call must throw
// synchronously with node's exact message.
//
// Adaptations: assert.throws becomes a probe that collects {name, code, message} per call,
// so the same script runs on both stacks and the collected lists are compared -- first
// against node's measured shape (built here with the same inspect() rendering node's
// invalidArgValue uses), then against each other. The fixture is generated into a per-run
// temp directory; the client only drains and closes.
//
// NOTE on `test/parallel/test-http2-respond-with-fd-errors.js`: it drives the error path
// *inside* processRespondWithFD by mocking the C binding (internalBinding('http2'),
// Http2Stream.prototype.respond overridden to return each NGHTTP2_ERR constant) -- there is
// no portable equivalent for that, the binding is not reachable from a test. Its assertable
// argument matrix is the one this file carries; the observable pump-failure path (a bad fd
// on the wire: HEADERS go out, then RST_STREAM(NGHTTP2_INTERNAL_ERROR)) is pinned by
// respond-file-fd-invalid.test.js.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const http2 = require('node:http2');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { inspect } = require('node:util');
const h2lean = require('../../index');
const { listen, closeServer } = require('../diff-harness');

// upstream's type table, in its key order
const TYPES = {
  boolean: true,
  function: () => {},
  number: 1,
  object: {},
  array: [],
  null: null,
  symbol: Symbol('test')
};
const OPTIONS_WITH_TYPE_ERROR = {
  offset: 'number',
  length: 'number',
  statCheck: 'function'
};

// node's measured messages, rebuilt with the renderings its own guards use
function fdMessage(value) {
  let received;
  if (typeof value === 'function') {
    received = `function ${value.name}`;
  } else if (typeof value === 'object' && value !== null) {
    received = `an instance of ${value.constructor.name}`;
  } else if (value === null) {
    received = 'null';
  } else {
    received = `type ${typeof value} (${String(value)})`;
  }
  return 'The "fd" argument must be of type number or an instance of FileHandle. ' +
    `Received ${received}`;
}

function optionMessage(option, value) {
  return `The property 'options.${option}' is invalid. Received ${inspect(value)}`;
}

function matrix() {
  const calls = [];
  // upstream: every fd type but number
  for (const type of Object.keys(TYPES)) {
    if (type === 'number') {
      continue;
    }
    calls.push(['fd', TYPES[type]]);
  }
  // upstream: every options type but the expected one, per guarded option
  for (const [option, expected] of Object.entries(OPTIONS_WITH_TYPE_ERROR)) {
    for (const type of Object.keys(TYPES)) {
      if (type === expected) {
        continue;
      }
      calls.push([`options.${option}`, TYPES[type]]);
    }
  }
  return calls;
}

// The whole matrix on one stream, in upstream's order. Returns what each call threw.
async function arm(serverLib) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'h2-up-ferr-'));
  const file = path.join(dir, 'file');
  fs.writeFileSync(file, 'ok');
  const fd = fs.openSync(file, 'r');
  const srv = serverLib.createServer();
  const port = await listen(srv);
  try {
    return await new Promise((resolve, reject) => {
      const collected = [];
      srv.on('stream', (stream) => {
        const record = (fn) => {
          try {
            fn();
            collected.push({ name: 'no-throw' });
          } catch (err) {
            collected.push({ name: err.name, code: err.code, message: err.message });
          }
        };

        for (const [kind, value] of matrix()) {
          if (kind === 'fd') {
            record(() => stream.respondWithFD(value, { 'content-type': 'text/plain' }));
          } else {
            const option = kind.slice('options.'.length);
            record(() => stream.respondWithFD(fd, { 'content-type': 'text/plain' },
              { [option]: value }));
          }
        }

        for (const status of [204, 205, 304]) {
          record(() => stream.respondWithFD(fd, {
            'content-type': 'text/plain',
            ':status': status
          }));
        }

        stream.respond();
        record(() => stream.respondWithFD(fd, { 'content-type': 'text/plain' }));

        stream.destroy();
        record(() => stream.respondWithFD(fd, { 'content-type': 'text/plain' }));
      });

      const client = http2.connect(`http://127.0.0.1:${port}`);
      client.on('error', reject);
      const req = client.request({ ':path': '/' });
      req.on('error', () => {});
      req.on('close', () => {
        client.close();
        resolve(collected);
      });
      req.end();
    });
  } finally {
    await closeServer(srv);
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

test('upstream: the respondWithFD argument matrix throws what node throws', async () => {
  const expected = [];
  for (const [kind, value] of matrix()) {
    if (kind === 'fd') {
      expected.push({ name: 'TypeError', code: 'ERR_INVALID_ARG_TYPE', message: fdMessage(value) });
    } else {
      const option = kind.slice('options.'.length);
      expected.push({ name: 'TypeError', code: 'ERR_INVALID_ARG_VALUE',
        message: optionMessage(option, value) });
    }
  }
  for (const status of [204, 205, 304]) {
    expected.push({ name: 'Error', code: 'ERR_HTTP2_PAYLOAD_FORBIDDEN',
      message: `Responses with ${status} status must not have a payload` });
  }
  expected.push({ name: 'Error', code: 'ERR_HTTP2_HEADERS_SENT',
    message: 'Response has already been initiated.' });
  expected.push({ name: 'Error', code: 'ERR_HTTP2_INVALID_STREAM',
    message: 'The stream has been destroyed' });

  const nodeGot = await arm(http2);
  const leanGot = await arm(h2lean);
  assert.deepStrictEqual(nodeGot, expected, 'the node arm must produce node\'s messages');
  assert.deepStrictEqual(leanGot, expected, 'the h2lean arm must produce node\'s messages');
});
