// `date` on a response of the stream API (lib/stream.js respond(), lib/headers.js parse).
//
// node adds the header in the same function that fills in a missing :status --
// prepareResponseHeadersObject(), internal/http2/core.js:2700-2704:
//     statusCode = headers[':status'] = statusCode | 0 || HTTP_STATUS_OK;
//     if (!hasDate && (options.sendDate == null || options.sendDate)) headers.date = utcDate();
// so the default is ON, an explicit false turns it off, a value the application set is
// kept, and there is no status condition (respond() cannot carry a 1xx at all).
//
// RFC 9110 6.4.1 is what the field is for -- an origin server stamps the time it placed
// the message into its mailbox -- and RFC 9113 8.2 treats it as an ordinary field, with
// no HTTP/2-specific rule of its own. The reason this file exists is that h2lite's stream
// API used to send no date at all while its compat API did: two APIs of one library, and a
// client that compares responses from `http2` and from `h2lite` saw a different field set.
//
// The cost of the change is not the string, it is the cache: a block of frozen response
// headers was cached without looking at the date, and a date-bearing block cached that way
// would either repeat a stale second for ever or switch the cache off for everyone who
// freezes their response headers. block() therefore stamps each cached entry with the
// second whose date it carries, and the last test below is the one that fails if the
// stamp is dropped -- it is also the only test here that can fail by being *fast*.
'use strict';
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const f = require('../lib/frames');
const h2lite = require('../index');
const { listen, closeServer } = require('./diff-harness');

// RFC 9110 7.1.1.1 IMF-fixdate, which is what toUTCString() produces and what node sends.
const FIXDATE = /^[A-Z][a-z]{2}, \d{2} [A-Z][a-z]{2} \d{4} \d{2}:\d{2}:\d{2} GMT$/;

const REQ = Buffer.from([0x82, 0x84, 0x86, 0x01, 9,
  ...Buffer.from('localhost', 'latin1')]);

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// One connection, driven in batches. Each plan entry is {requests, blocks?, waitMs?}:
// `requests` GETs go out, `blocks` HEADERS blocks are waited for (one per request unless
// a stream answers with a trailers block as well), and the next batch can be delayed --
// which is how a second boundary is crossed on the SAME connection. That matters: the
// block cache is per-session, so a fresh socket starts with an empty one and can never
// show a stale cached entry.
//
// The field lists come from decoding with our HPACK decoder rather than reading a
// client-side header map: a map merges a duplicated `date` into one entry, so sending the
// field twice would be invisible to it.
async function collect(impl, handler, plan) {
  const srv = impl.createServer();
  srv.on('stream', (stream, headers) => {
    stream.on('error', () => {});
    handler(stream, headers);
  });
  srv.on('error', () => {});
  const port = await listen(srv);

  const decoder = new h2lite.HPack.Decoder(4096);
  const total = plan.reduce((n, b) => n + (b.blocks === undefined ? b.requests : b.blocks), 0);
  let sock = null;
  try {
    return await new Promise((resolve, reject) => {
      const out = [];
      let acc = Buffer.alloc(0);
      let nextSid = 1;
      let batch = 0;
      let seenForBatch = 0;
      let sentAt = Date.now(); // when the batch these blocks answer went out
      let target = plan[0].blocks === undefined ? plan[0].requests : plan[0].blocks;
      sock = net.connect(port, '127.0.0.1');
      sock.on('error', reject);

      function sendRequests(count) {
        sentAt = Date.now();
        const parts = [];
        for (let i = 0; i < count; i++) {
          parts.push(frame(f.HEADERS, f.F_END_HEADERS | f.F_END_STREAM, nextSid, REQ));
          nextSid += 2;
        }
        sock.write(Buffer.concat(parts));
      }
      function advance() {
        batch++;
        if (batch >= plan.length) {
          return false;
        }
        seenForBatch = 0;
        const next = plan[batch];
        target = next.blocks === undefined ? next.requests : next.blocks;
        if (next.waitMs) {
          setTimeout(() => sendRequests(next.requests), next.waitMs);
        } else {
          sendRequests(next.requests);
        }
        return true;
      }

      sock.on('data', (d) => {
        acc = Buffer.concat([acc, d]);
        for (;;) {
          if (acc.length < f.HEADER_LEN) {
            return;
          }
          const len = (acc[0] << 16) | (acc[1] << 8) | acc[2];
          if (acc.length < f.HEADER_LEN + len) {
            return;
          }
          const type = acc[3], flags = acc[4];
          const body = acc.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
          acc = acc.subarray(f.HEADER_LEN + len);
          if (type === f.SETTINGS && !(flags & f.F_ACK)) {
            // a connection that starts with HEADERS is refused by node ("SETTINGS
            // expected"), so the handshake has to be finished before the requests go out
            sock.write(frame(f.SETTINGS, f.F_ACK, 0, Buffer.alloc(0)));
            sendRequests(plan[0].requests);
          } else if (type === f.HEADERS && (flags & 0x4)) {
            const flat = [];
            decoder.decode(body, 0, body.length, flat);
            const fields = [];
            for (let i = 0; i < flat.length; i += 2) {
              fields.push([flat[i], flat[i + 1]]);
            }
            out.push({ fields, bytes: body.length, at: sentAt });
            seenForBatch++;
            if (out.length === total) {
              resolve(out); return;
            }
            if (seenForBatch === target && !advance()) {
              return;
            }
          }
        }
      });
      sock.on('connect', () => {
        sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
      });
      const budget = plan.reduce((n, b) => n + (b.waitMs || 0), 1500) + 3000;
      setTimeout(() => reject(new Error(`only ${out.length} of ${total} blocks in ${budget} ms`)),
        budget);
    });
  } finally {
    if (sock !== null) {
      sock.destroy();
    }
    await closeServer(srv);
  }
}

const drive = (impl, count, handler, blocks) =>
  collect(impl, handler, [blocks === undefined ? { requests: count }
    : { requests: count, blocks }]);

const respond200 = (s) => s.respond({ ':status': 200 }, { endStream: true });

function dateOf(response) {
  return response.fields.filter(([n]) => n === 'date');
}

for (const impl of [{ name: 'h2lite', mod: h2lite }, { name: 'node', mod: require('node:http2') }]) {
  // The cases node's own rule produces, measured against a live node server before the
  // behaviour was written here.
  const cases = [
    ['200', respond200, ['date']],
    ['204', (s) => s.respond({ ':status': 204 }, { endStream: true }), ['date']],
    ['304', (s) => s.respond({ ':status': 304 }, { endStream: true }), ['date']],
    ['sendDate false', (s) => s.respond({ ':status': 200 },
      { endStream: true, sendDate: false }), []],
    ['an application date is kept',
      (s) => s.respond({ ':status': 200, date: 'Mon, 01 Jan 2021 00:00:00 GMT' },
        { endStream: true }), ['date']]
  ];

  for (const [label, handler, want] of cases) {
    test(`${impl.name}: ${label}`, async () => {
      // four requests: the first two are the ones the encoder needs to reach its settled
      // form, and the assertions belong to the block that follows them
      const responses = await drive(impl.mod, 4, handler);
      const last = responses[3];
      const dates = dateOf(last).map(([, v]) => v);
      assert.deepStrictEqual(dates.length, want.length,
        `expected ${want.length} date line(s), decoded: ${JSON.stringify(last.fields)}`);
      if (want.length === 0) {
        return;
      }
      if (label.startsWith('an application date')) {
        assert.strictEqual(dates[0], 'Mon, 01 Jan 2021 00:00:00 GMT',
          'the generated date overwrote the one the application set');
      } else {
        assert.match(dates[0], FIXDATE, `"${dates[0]}" is not an IMF-fixdate`);
        // last, because that is where node appends it and the order is on the wire
        assert.strictEqual(last.fields[last.fields.length - 1][0], 'date',
          `date is not the last field: ${JSON.stringify(last.fields)}`);
      }
    });
  }

  test(`${impl.name}: a response with no :status answers 200 and carries a date`, async () => {
    const [r] = await drive(impl.mod, 1,
      (s) => s.respond({ 'x-a': '1' }, { endStream: true }));
    assert.deepStrictEqual(r.fields, [
      [':status', '200'], ['x-a', '1'], ['date', r.fields[2][1]]]);
    assert.match(r.fields[2][1], FIXDATE);
  });

  test(`${impl.name}: a trailers block carries no date`, async () => {
    const responses = await drive(impl.mod, 1, (s) => {
      s.respond({ ':status': 200 }, { waitForTrailers: true });
      s.on('wantTrailers', () => s.sendTrailers({ 'x-tail': 'v' }));
      s.end('body');
    }, 2);
    // two HEADERS blocks on the stream: the response and the trailers
    assert.strictEqual(responses.length, 2, `saw ${responses.length} blocks, expected 2`);
    assert.ok(dateOf(responses[0]).length === 1, 'the response lost its date');
    assert.deepStrictEqual(responses[1].fields, [['x-tail', 'v']],
      'the trailers block is not exactly what the application asked for');
  });
}

test('frozen response headers keep the block cache and still move to the next second', async () => {
  // The trap. `date` is the one response field whose value is not fixed by the header
  // object, and the block cache is keyed on that object -- so either the cache stops
  // applying to date-bearing responses (which is what it did, and what would silently
  // cost every application that freezes its response headers) or the entry has to know
  // which second its bytes belong to.
  const HEADERS = Object.freeze({ ':status': 200, 'x-cached': 'yes' });
  const handler = (s) => s.respond(HEADERS, { endStream: true });

  // One connection, two batches, the second sent a little after the next second boundary.
  // The margin is not for the network: the date string is refreshed by a one-shot timer at
  // the boundary and then recomputed on the next response (see httpDate() in
  // lib/headers.js), so a request fired 5 ms past the boundary could legitimately still
  // carry the previous second on a busy loop. 150 ms gives the timer room and still lands
  // inside the same second it is being compared against.
  const wait = 1000 - (Date.now() % 1000) + 150;
  const run = await collect(h2lite, handler, [{ requests: 10 }, { requests: 6, waitMs: wait }]);
  const first = run.slice(0, 10);
  const later = run.slice(10);
  const stamps = first.map((r) => dateOf(r)[0][1]);

  // Within one second the bytes must be identical: that is the cache answering. Grouped by
  // date value rather than by index, because the first batch may itself straddle a
  // boundary -- that only makes a group shorter, never wrong.
  const byStamp = new Map();
  first.forEach((r, i) => {
    const list = byStamp.get(stamps[i]) || [];
    list.push(r);
    byStamp.set(stamps[i], list);
  });
  for (const [stamp, group] of byStamp) {
    // The first responses of a shape are its warm-up: a line is emitted as a literal,
    // then inserted, then referenced, and only the tail is what the cache returns.
    const settled = group.slice(3);
    if (settled.length < 2) {
      continue;
    } // too short to say anything
    const sizes = new Set(settled.map((r) => r.bytes));
    assert.strictEqual(sizes.size, 1,
      `${settled.length} responses all stamped ${stamp} came out in ${sizes.size} sizes`
      + ` (${[...sizes].join(', ')})`);
    // :status + x-cached + date, each a single indexed octet once the table holds them. A
    // number far above this means the date goes out as a literal every response, which is
    // the other way this cache could lose: correct, but ~30 bytes per request.
    assert.ok(settled[0].bytes <= 8,
      `a settled date-bearing block costs ${settled[0].bytes} bytes: ${settled[0].fields}`);
  }

  // The absolute check, not a comparison of two groups: the block sent in second T must
  // carry the date of second T. Compared against the previous batch's value it would
  // accidentally pass when a boundary falls inside the first batch; compared against the
  // moment the request went out it also fails if the cache keeps serving the old second.
  for (const r of later) {
    assert.strictEqual(dateOf(r)[0][1], new Date(r.at).toUTCString(),
      `a response sent at ${new Date(r.at).toUTCString()} carried "${dateOf(r)[0][1]}"`);
  }
  // and the rebuilt entry is stable for the rest of its own second: the cache came back
  const lateBytes = later.slice(3).map((r) => r.bytes);
  assert.strictEqual(new Set(lateBytes).size, 1,
    `the rebuilt block is not stable within the second: ${lateBytes}`);
});

test('a settled date-bearing shape stops being encoded at all', async () => {
  // The stamp buys the cache hit, and a hit is invisible on the wire: re-encoding a settled
  // shape produces bytes identical to the cached ones, so no response-level assertion can
  // tell "served from the cache" from "built again". The encoder's own call count is the
  // observable. This is the test that fails if date-bearing blocks stop being cached --
  // which is what `cacheable = ... && !sendDate` meant before the stamp existed, and it would
  // have cost every application that freezes its response headers without changing a single
  // byte on the wire.
  //
  // Three lines per block (status, x-cached, date). Warm-up encodes the shape twice and
  // the rest are hits, so the expected total here is around 6-10 for 16 responses. A cache
  // that never hits encodes all of them: 48.
  const { Encoder } = h2lite.HPack;
  const HEADERS = Object.freeze({ ':status': 200, 'x-cached': 'yes' });
  const handler = (s) => s.respond(HEADERS, { endStream: true });
  const real = Encoder.prototype.write;
  let calls = 0;
  Encoder.prototype.write = function (dst, off, name, value, neverIndex) {
    calls++;
    return real.call(this, dst, off, name, value, neverIndex);
  };
  let responses;
  try {
    responses = await collect(h2lite, handler, [{ requests: 16 }]);
  } finally {
    Encoder.prototype.write = real;
  }
  assert.strictEqual(responses.length, 16);
  assert.ok(calls < 16, `${calls} encoder calls for 16 responses of one settled shape:`
    + ' the block is being built every time, not cached');
});
