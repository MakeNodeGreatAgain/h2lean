// Runs h2spec, the conformance tool for RFC 7540 and RFC 7541, against node:http2 and against
// h2lite on the same application script, and reports the checks the two answers disagree on.
//
// The node arm is what makes the output readable. h2lite is built to match node, so a check
// that BOTH stacks fail is a place where node itself deviates from the specification, or where
// the check quotes RFC 7540 wording that RFC 9113 changed -- parity says leave it alone. A check
// only h2lite fails is a bug here. Without the control arm those two classes come out of the
// tool looking identical, and the honest reaction to a red list is then either panic or a shrug.
//
//   node tools/check-h2spec.js                    -> h2c and TLS, both stacks
//   node tools/check-h2spec.js h2lite             -> one stack, its full list printed
//   node tools/check-h2spec.js node tls           -> one stack, one transport
//   SPEC=http2/6.5 node tools/check-h2spec.js     -> one section
//   CAP=120 node tools/check-h2spec.js            -> seconds before a stuck h2spec is killed
//   MEM=2 node tools/check-h2spec.js              -> sample the heap and the session count every 2 s
//
// h2spec is not vendored and not fetched by this repository: it is an independent MIT-licensed
// Go program, so it is installed out of band and H2SPEC points at the copy. A handful of failing
// arms are RFC 9113 divergences of the 7540-era tool, documented in AGENTS.md §10 and carved out
// of the bug count by the DOCUMENTED map below (they are still printed, under their own heading).
// Exit codes: 0 no h2lite-only failure outside that documented set, 1 there is at least one,
// 2 no binary found.
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { spawn, spawnSync } = require('node:child_process');
const { XMLParser } = require('fast-xml-parser'); // the one devDependency; runtime stays dependency-free
const h2lite = require('../index');
const nodeHttp2 = require('node:http2');
const certificates = require('../test/fixtures-tls');
const { listen, closeServer } = require('../test/diff-harness');

const H2SPEC = process.env.H2SPEC || 'h2spec';
const OUT = path.join(__dirname, '..', 'out', 'h2spec');
const PAIR = certificates();
const IMPLS = [['node', nodeHttp2], ['h2lite', h2lite]];

// h2spec states what it needs of a server before it runs anything: GET / and POST / answered
// with 200 and non-empty data. The answer goes out without reading the request -- the tool's own
// POST carries a body, and a target that waited for the application to consume it would make
// every check a test of teardown timing instead of of framing.
function serve(impl, tls) {
  const srv = tls ? impl.createSecureServer({ key: PAIR.key, cert: PAIR.cert })
    : impl.createServer();
  srv.on('stream', (stream) => {
    stream.on('error', () => {});
    stream.respond({ ':status': 200, 'content-type': 'text/plain' });
    stream.end('h2spec target\n');
  });
  srv.on('error', () => {}); // a refused teardown race must not crash the run
  return listen(srv).then((port) => ({ srv, port }));
}

// The JUnit report is the machine-readable half of h2spec's output: one <testcase> per check, an
// <error> or <failure> child when it did not pass. It is read with fast-xml-parser (the tool's one
// devDependency) so the classification below can match a failing arm by name and pull out the
// documented divergences. Two invariants the hand parser earned still hold and are what the parser
// is configured for:
//   * a protocol failure is an <error> CHILD, not a <failure> one -- only generic/ and hpack/ reach
//     the <failure> path, so counting that tag alone made every http2/5.x, 6.x and 8.x regression
//     invisible while still reporting "0 of 146 checks failed" (measured on h2spec 2.6.0 against
//     both stacks: the console said 21 failed for h2lite and 13 for node, the <failure>-only parse
//     said 0 for both, all 21 sitting in <error>). Both tags count here.
//   * a <testcase> identifies itself by package= (the spec section) and classname= (the check);
//     the testsuite element owns the name=. Reading name= off the testcase matches nothing and
//     reports a clean run of zero checks -- so this returns the total it saw and run() cross-checks
//     it against h2spec's own verdict line, and a report shape change surfaces as a DISAGREEMENT
//     rather than as a silent pass.
const XML = new XMLParser({ ignoreAttributes: false }); // attributes keep the '@_' prefix

function failures(file) {
  const parsed = XML.parse(fs.readFileSync(file, 'utf8'));
  const node = parsed?.testsuites?.testsuite;
  const suites = node === undefined ? [] : (Array.isArray(node) ? node : [node]);
  const failed = [];
  let total = 0;
  for (const suite of suites) {
    let cases = suite?.testcase;
    if (cases === undefined) {
      continue;
    }
    if (!Array.isArray(cases)) {
      cases = [cases];
    }
    for (const tc of cases) {
      // A passed check is an empty <testcase ...></testcase>: it still carries package= and
      // classname=, so it counts toward the total; only an <error>/<failure> child marks it failed.
      if (!tc || tc['@_package'] === undefined) {
        continue;
      }
      total++;
      if ('error' in tc || 'failure' in tc) {
        failed.push(`${tc['@_package']}: ${tc['@_classname']}`);
      }
    }
  }
  return { failed, total };
}

// The failing arms that RFC 9113 says are NOT bugs -- the middle/leave bucket written up in
// AGENTS.md §10 with the clause that settles each. h2spec 2.6.0 was written against RFC 7540: three
// of these checks wording RFC 9113 relaxed to a "MAY ... connection error" (discarding the frame is
// conforming, the tool just waits for an error it permits but does not require and times out), and
// the fourth checks a self-dependency rule RFC 9113 dropped with the priority signalling (RFC 9218).
// They are carved out of the h2lite-only bug count so a red run reads as the tool's stale
// expectation, not as work here -- but they are still PRINTED, because "documented" is not
// "invisible". The keys are the exact `package: classname` strings h2spec writes; keep them in step
// with the §10 table, and add a §10 write-up before adding a key here.
const DOCUMENTED = new Map([
  ['http2/5.1: closed: Sends a DATA frame', 'RFC 9113 5.1: a frame on a closed stream MAY be a STREAM_CLOSED connection error -- discarding it is conforming'],
  ['http2/5.1: closed: Sends a HEADERS frame', 'the same 5.1 "MAY ... connection error"'],
  ['http2/5.1: closed: Sends a DATA frame after sending RST_STREAM frame', 'RFC 9113 5.1: after a reset the endpoint MUST minimally process and then DISCARD the frame'],
  ['http2/5.1: closed: Sends a HEADERS frame after sending RST_STREAM frame', 'the same minimal-process-and-discard'],
  ['http2/5.3.1: Sends PRIORITY frame that depend on itself', 'RFC 9113 dropped the self-dependency rule with the priority signalling (RFC 9218) -- dropping the frame is conforming']
]);

// "146 tests, 124 passed, 1 skipped, 21 failed" -- h2spec's own verdict line, printed last. The
// only part of the report that cannot go stale: it is written from the same results the terminal
// ticks come from, while the XML has twice now been read as clean over a red console. Returning
// the parsed number lets run() compare it with what the report said and shout when they disagree.
function summary(out) {
  const m = /(-?[0-9]+) tests, (-?[0-9]+) passed, (-?[0-9]+) skipped, (-?[0-9]+) failed/.exec(out);
  return m ? { total: +m[1], failed: +m[4] } : null;
}

// Asynchronous because the server h2spec is talking to lives in THIS process: a blocking
// spawnSync freezes the event loop it needs to answer, and the run then sits at 0% CPU until the
// cap kills h2spec -- which reads as "the tool hangs" and is really the harness starving it.
// The cap is still kept: h2spec's own -o bounds only the wait for a response, so against a
// server that keeps a connection open instead of closing it, a check can block forever.
//
// The console verdict line is captured as well and compared with the report, because the two have
// different failure shapes and only the line is written straight from the results: a report that
// reads as clean under a red console is the failure mode this harness exists to prevent, and it
// happened for real when <error> children stopped being counted.
function run(port, tls, file) {
  const args = ['-p', String(port), '-o', '5', '-j', file];
  if (tls) {
    args.push('-t', '-k');
  } // -k: the fixture certificate is self-signed
  const section = process.env.SPEC;
  if (section) {
    args.push(section);
  }
  const cap = (parseInt(process.env.CAP, 10) || 600) * 1000;
  console.log(`  running ${section || 'all specs'} over ${tls ? 'tls' : 'h2c'} (cap ${cap / 1000} s)`);
  return new Promise((resolve, reject) => {
    const child = spawn(H2SPEC, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let killed = null;
    let out = '';
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', (d) => {
      out += d;
    });
    child.on('error', (e) => reject(new Error(`${H2SPEC} unavailable: ${e.code}`)));
    const timer = setTimeout(() => {
      killed = `${cap / 1000} s cap`; child.kill('SIGTERM');
    }, cap);
    child.on('close', (code, signal) => {
      clearTimeout(timer);
      if (killed) {
        console.log(`  ${section || 'the run'} was killed after ${killed}: no verdict`);
      } else if (signal) {
        console.log(`  ${section || 'the run'} ended on ${signal}`);
      }
      const got = fs.existsSync(file) ? failures(file) : { failed: [], total: 0 };
      const sum = summary(out);
      if (!killed && sum && (sum.failed !== got.failed.length || sum.total !== got.total)) {
        console.log(`  DISAGREEMENT: the report parsed ${got.failed.length} of ${got.total}, `
          + `h2spec's own line says ${sum.failed} of ${sum.total} -- the report shape changed `
          + 'again, fix failures() before trusting either number');
      }
      resolve({ code, ...got });
    });
  });
}

// A sampler, because a conformance run is a good way to make a server allocate: h2spec opens a
// connection per check, leaves many of them half-finished, and the shape of what it sends is
// deliberately hostile. `heapUsed` and the live-session count next to each other say whether the
// memory belongs to sessions that were never torn down or to one session that read something huge.
function sample(srv) {
  const every = parseInt(process.env.MEM, 10);
  if (!every) {
    return () => {};
  }
  const t = setInterval(() => {
    const m = process.memoryUsage();
    const s = srv.sessions; // a Set on both stacks: .size, not .length
    console.log(`    heap ${(m.heapUsed / 1048576).toFixed(0)} MB rss ${(m.rss / 1048576).toFixed(0)} MB`
      + ` sessions ${s ? (s.size !== undefined ? s.size : s.length) : 'n/a'}`);
  }, every * 1000);
  return () => clearInterval(t);
}

(async () => {
  const want = process.argv[2];
  const wantTransport = process.argv[3];
  if (spawnSync(H2SPEC, ['--version']).error) {
    console.error('h2spec not found: install it from its own releases, or set'
      + ` H2SPEC=/path/to/h2spec (looked for "${H2SPEC}")`);
    process.exit(2);
  }
  fs.mkdirSync(OUT, { recursive: true });

  let oursOnly = 0;
  for (const tls of [false, true]) {
    const transport = tls ? 'tls' : 'h2c';
    if (wantTransport !== undefined && wantTransport !== transport) {
      continue;
    }
    const lists = {};
    for (const [name, impl] of IMPLS) {
      if (want !== undefined && want !== name) {
        continue;
      }
      const { srv, port } = await serve(impl, tls);
      const stop = sample(srv);
      const got = await run(port, tls, path.join(OUT, `${name}-${transport}.xml`));
      stop();
      lists[name] = got.failed;
      await closeServer(srv);
      if (got.total === 0) {
        console.log(`${transport} ${name}: NO CHECKS PARSED -- the report changed shape, read it by hand`);
      } else {
        console.log(`${transport} ${name}: ${got.failed.length} of ${got.total} checks failed`);
      }
      if (want !== undefined) {
        for (const f of got.failed) {
          console.log(DOCUMENTED.has(f) ? `  DOC  ${f}  (documented §10: ${DOCUMENTED.get(f)})`
            : `  FAIL ${f}`);
        }
      }
    }
    if (lists.node && lists.h2lite) {
      // Carve the documented RFC-divergence arms (§10) out first: they are the tool's stale RFC 7540
      // expectation, not bugs, so they must not reach the h2lite-only count that drives the exit
      // code. They are reported under their own heading, and the three buckets below then speak
      // only about checks whose answer is genuinely in this repository's hands.
      const doc = [...new Set([...lists.h2lite, ...lists.node].filter((f) => DOCUMENTED.has(f)))];
      const hl = lists.h2lite.filter((f) => !DOCUMENTED.has(f));
      const nd = lists.node.filter((f) => !DOCUMENTED.has(f));
      const nodeF = new Set(nd);
      const only = hl.filter((f) => !nodeF.has(f));
      const shared = hl.filter((f) => nodeF.has(f));
      const notUs = nd.filter((f) => !hl.includes(f));
      oursOnly += only.length;
      if (doc.length) {
        console.log(`${transport}: documented divergence (${doc.length}) ` +
          '-- RFC 9113 over the 7540 tool, §10, not a bug here');
        for (const f of doc) {
          console.log(`  = ${f} :: ${DOCUMENTED.get(f)}`);
        }
      }
      console.log(`${transport}: h2lite-only (${only.length}) -- bugs to fix here`);
      for (const f of only) {
        console.log(`  * ${f}`);
      }
      console.log(`${transport}: both (${shared.length}) -- node deviates too, parity keeps it`);
      for (const f of shared) {
        console.log(`  - ${f}`);
      }
      console.log(`${transport}: node-only (${notUs.length}) -- h2lite answers differently`);
      for (const f of notUs) {
        console.log(`  + ${f}`);
      }
    }
  }
  process.exit(want !== undefined ? 0 : (oursOnly > 0 ? 1 : 0));
})().catch((err) => {
  console.error(err); process.exit(1);
});
