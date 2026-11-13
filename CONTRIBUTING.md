# CONTRIBUTING.md — contributor guide for h2lean

The rules that decide whether a change is accepted live in `AGENTS.md`; read that file
first. This one carries the working detail behind its §6-§10: how to run and write tests,
how measurement works here, the lint mechanics, the commit/CHANGELOG conventions and the
full write-up of every open problem and known divergence. `OPTIMIZATION.md` carries the
numbered performance knowledge; `SECURITY.md` is the outward-facing threat model.

## 1. Running and extending the test suite

* `npm test` = server suite + client suite
  (`node --test --test-timeout=120000 test/*.test.js && node --test --test-timeout=120000
  test/client/*.test.js`). The client suite is passed as an unquoted glob, not as the
  directory: node 22's runner fails a directory argument with MODULE_NOT_FOUND while
  node 20's does not expand runner-side globs, so the shell has to do it (the server
  suite was already a glob).
  **The timeout is load-bearing**: without it, a test waiting for a response that a newly
  added correct refusal never sends hangs the whole suite silently instead of failing.
  Keep it when invoking `node --test` by hand (the local convention is
  `--test-timeout=20000` for a faster signal). It is 120 s, not 60: on node 20/22 the
  broken http2 `server.close()` (nodejs/node#57611, never backported) costs the deadline
  path ~1 s per GOAWAY shape in the differential files, and `test/inbound-limits.test.js`
  alone runs ~73 s there. The guard property is unchanged -- a hung test still fails, a
  minute later.
* Expect the test count to grow; a change that reduces it needs a reason in the commit
  message.
* The suite is differential by design. Most files run the *same* application script
  against `node:http2` and against h2lean via `test/diff-harness.js` (`probe()`,
  `listen()`, `connect()`, `request()`) and compare what both sides recorded. When adding
  a test, prefer asserting node's measured value over asserting h2lean's own behaviour —
  a pinned string like `'stream,goaway:0/0,goaway:0/0'` with a comment saying it was read
  off node is the standard.
* A late `'error'` event on a stream after the test body finished fails the file silently.
  Stress tests must destroy their clients and clear their timers.

## 2. Known fragility classes — read before believing a regression

* **Deadline vs terminal frame.** A raw-socket transcript reader must resolve **on its
  terminal frame**, not only on a deadline. `test/inbound-limits.test.js` resolved on the
  deadline alone and flaked: reading a 1.5 MB literal can take longer than any comfortable
  deadline, so the `GOAWAY(COMPRESSION_ERROR)` the server had already sent was reported as
  `nothing` — a slow read mistaken for silence. A `GOAWAY` ends the connection, so nothing
  can follow it: resolving on arrival makes the fatal cases independent of payload size.
* **Batching-dependent verdicts.** A raw client that resolves its transcript on a
  deadline must not assert on a verdict that lives or dies with the peer's write
  batching. The message-framing rules of AGENTS.md §8/B2 are only visible to nghttp2 once
  the application has read, and an answer queued in the same read batch as a refusal is
  discarded by node **most** of the time — measured 6/6 in one shape and 0/6 under
  full-suite load in another. Assert the verdict frame (the rule), not the whole
  transcript, and write the measured frequency next to the relaxed assertion.
* **Timing tests use sub-second values** (`< 1000 ms`) unless they deliberately test the
  shared FastTimer path: delays ≥ 1000 ms ride a 499 ms clock with ±500 ms accuracy, so a
  strict assertion against wall clock will flake.
* **Generated wall-clock values.** Never compare the `date` header between two servers in
  one probe — the samples straddle a second boundary. Compare presence and format on one
  side and keep it out of the deep-equal.
* **A file-level failure with no assertion output** is a different bug class from a failed
  assertion: a leaked ref'd interval/handle keeps the child alive, an un-awaited
  `server.close()` or a `listen()` that never binds makes the file stop reporting
  entirely. Fix the harness (`t.after()`, `server.once('error', reject)`, a bounded
  teardown) before believing a regression. Racing `server.close()` is not enough on its
  own: the callback settling does not release the listening handle, and the file still
  cannot exit. `diff-harness.js`'s `closeServer()` therefore escalates on its deadline —
  destroy the live sessions, ask the server to close again, then `unref()` it — so a
  broken liveness guard produces failures instead of silence.
* **node's own C++ can abort a differential file**: `Assertion failed:
  (current_nghttp2_memory_) == (0)` in `node::http2::Http2Session::~Http2Session()`
  (`../src/node_http2.cc`), reached from a scavenge that collects a native session whose
  nghttp2 object still holds memory. It kills the file process, so the symptom is a
  **test count below the known total** with one `✖ test/<file>.test.js` and no failing
  test inside it. It is GC-timing dependent and may not reproduce — re-run before
  investigating — and it is not h2lean's bug: the native arm is the one being collected.
  Do not chase it as a regression; check `ℹ tests` against the expected number.
* **A peer's answer to our GOAWAY is a race on both stacks**, not a property of either:
  node's session is destroyed when its graceful close completes (`internal/http2/core.js`:
  `handle.ongracefulclosecomplete` → `kMaybeDestroy`), h2lean's when the GOAWAY is written
  (`lib/session.js`: `close()` → `_maybeFinishClose()`), and the answer is read only if it
  lands in the window between. Measured with `tools/probe-goaway-echo.js`: node's outcome
  moves with the load — 40 of 40 rounds on an idle machine, 26 of 40 and 29 of 30 in two
  loaded runs — while h2lean reported the echo in every round of every run. So
  `test/event-order.test.js` drops that event outside its `'goaway'` scenario — the one
  where the event is the subject. Do not "restore" the strict comparison: the failure it
  produces is the clock's, not a divergence.
* Two checks depend on binaries this repository does not control, and both **skip**
  rather than fail when the tool is missing — which means a green `npm test` says nothing
  about them:
  * `test/interop-external.test.js` drives `nghttp` and `curl` — clients on their own
    nghttp2 build, written without knowledge of h2lean — against an h2lean server. The
    differential suite cannot answer this question: every test in it compares h2lean with
    node, so anything the two agree on is accepted no matter how many other
    implementations reject it.
  * `npm run spec` (`tools/check-h2spec.js`) runs h2spec, the RFC 7540/7541 conformance
    set, against node and h2lean on the same application script and prints the three-way
    split. It exists because the tool is not node: a check only h2lean fails is a bug
    here, while a check both fail is node's own deviation (or RFC 7540 wording RFC 9113
    changed), and parity says leave that alone. Run it after any change to the inbound or
    outbound frame path, on a machine that has the tool. **The middle bucket is not a
    to-do list**: every arm that sits in it is written up in §5 with the RFC clause that
    settles it, and the standing instruction there is to leave the code alone — h2spec
    2.6.0 checks RFC 7540's wording on three of them and a rule RFC 9113 dropped on the
    fourth, so bending the code to the tool would break the RFC and diverge from node at
    the same time. The four `http2/5.1 closed:` DATA/HEADERS arms and `http2/5.3.1`
    PRIORITY-self-dependency are §5's documented divergences and are carved out of the bug
    count by the `DOCUMENTED` map in the tool: they print under their own "documented
    divergence" heading (still visible, never hidden) and cannot turn `npm run spec` red
    on their own.
  * The JUnit report h2spec writes (`-j`) marks a failed **protocol** check as an
    `<error>` child, not a `<failure>` one; only the `generic/` and `hpack/` sections
    reach the code path that emits `<failure>`. Counting the latter alone reads a red run
    as clean: measured on 2.6.0, the console said 22 of 146 for h2lean and the
    `<failure>`-only parse said 0 of 146. `failures()` reads the report with
    `fast-xml-parser` (the tool's one devDependency), counts both tags, keys each check by
    its `package=` + `classname=` (the testsuite owns `name=`), and cross-checks the total
    against h2spec's own `N tests, ... , M failed` line, so a report shape change
    surfaces as a `DISAGREEMENT` rather than as a pass.
* A check that drives an **external client against a server running in the same process**
  must spawn the client asynchronously. A blocking `spawnSync` freezes the event loop the
  server needs to answer, the client then times out, and the failure looks exactly like a
  protocol bug — the tool that hangs at 0% CPU is starving its own target, not being
  ignored by it. It is easy to write by accident; both external gates here use an async
  spawn with a kill timer.

## 3. Performance work

The convention in one paragraph: the paired matrix is the unit of proof; every claim in
`OPTIMIZATION.md` is itemised with its benchmark. The mechanics:

* `GC=1 sh bench/matrix.sh <label>` writes `/tmp/mx-<label>/<case>.txt`. Every h2lean case
  is paired with the *same* case on `node:http2`. The native arm is the drift sensor:
  identical untouched native code moves 8–13% between runs, and per case it can move far
  more (the native 256 KB download arm has swung +32% while the small-GET pairs stayed
  flat). Therefore: never compare a new number against a stored baseline — re-measure
  both arms in one session; judge drift **per case**; if intra-arm spread exceeds the
  between-arm difference, report "inside noise", not a win; `cpuPerReq` is the metric
  (measured inside the server process), raw req/s depends on whatever else is running.
* **Verify both arms do the same work.** For uploads the handler must consume the body on
  both sides (`bench/h2load.js` sets `READBODY=1` whenever `--body>0`); otherwise node
  answers and resets the unread half (~38% of its CPU) and the numbers invert.
* **Swap arms by interleaving within one session** — ABBA, ≥3 rounds, using saved copies
  of the changed file. Alternating ABAB is not enough: the first arm of each pair
  systematically wins. Whole-server A/B also runs as `PRELOAD=bench/<arm>.js node
  bench/h2load.js …` with arms like `bench/no-table.js`, `bench/no-validate.js`,
  `bench/no-date.js` patching a prototype. Bench flags are **argv options**
  (`--server=native`), not environment variables — passing them as env silently runs the
  same arm twice.
* **Measure at the granularity where the effect acts.** A 100 ns header effect does not
  survive `cpuPerReq`; use the microbenches (`bench/validate-cost.js`,
  `bench/hpack-churn.js`, `bench/rst-cost.js`, `bench/toobject-cost.js`,
  `bench/header-validate.js`, `bench/timers.js`, and the cited archive in
  `bench/micro/`). Both arms of a microbench must have an identical *call shape* — a
  method reached with `.call(receiver, …)` versus a standalone helper reading a captured
  object gets different inlining, and that alone can produce a negative result.
* **Attribute with profiles, not arithmetic.** `GC=1` perf_hooks counters and
  `--cpu-prof` (`bench/prof-h2.js` runs server and client in separate processes so a
  profile contains only server work; `bench/summarize-profile.js` and
  `tools/cpuprofile-report.js` read them). `busy > 100%` is the signature of parallel V8
  GC threads from allocation churn. Never diff profiles **across** a JS and a C++
  implementation of the same protocol — the C++ core is invisible to the sampler. Profiles
  captured before a structural fix are invalid for anything afterwards.
* **Before benchmarking, check for competing work.** `pgrep -fl "node tools/"` for stray
  probe processes still running; anything else on the host moves absolute req/s by more
  than most effects here, which is why only the paired ratio is a result.
* **Guard removal is tested by mutation, not by review.**
  `node tools/mutate-guards.js` rewrites each of the 86 anchors in place, runs the whole
  suite, and restores the file; `--check` verifies that every anchor still matches its
  source and is unique, **exits 1 when one does not**, and refuses a sweep over a tree
  that does not fit the list. An anchor the suite does not catch means the guard has no
  test — or that the guard is unreachable dead code, in which case delete the guard and
  pin the invariant upstream instead of writing a vacuous one. Eight anchors are reported
  MISSED today. Three have their reason written next to them: all three are live guards
  whose shape the peer or the scheduler refuses to pin (§5's FIN-vs-`destroy()` race, an
  event that always re-runs a check, a rule node itself reports two ways). The other five
  do not — the `_dispatch()` return that stops the read batch at the frame that destroyed
  the session, and the four `Buffer.from()` copies that keep a PING payload, a pong and
  two GOAWAY debug payloads valid after the frame that carried them is gone — and they are
  a test gap, not a scheduling problem: re-measured one by one at `HEAD`, on the source
  the anchors were recorded against, they are MISSED there too. The refused-history push
  this set used to carry as a fourth MISSED is caught now: it stopped being redundant the
  moment 5.1.1 was added, because a `HEADERS` on a never-seen lower id became a connection
  error and only the `_refused` record tells a refused stream's in-flight `DATA` apart
  from it. Useful switches: `FILES=test/x.test.js` narrows the run to one file (5 s
  instead of 25 s, and the way to bisect an anchor that hangs the suite), and an index
  list (`node tools/mutate-guards.js 4 20`) re-runs only the anchors you changed. A run
  whose summary line never arrives is HUNG **unless** it had already printed failures:
  `--test-force-exit` is passed to the child because a liveness mutation can leave most
  test files unable to exit — measured on anchor 20, `server.close()` that never gives its
  port back reached the cap with 24 failures in the output and not one of them reported.
  Terminate a run with **SIGTERM/SIGINT, never `kill -9`**: the restore lives in a signal
  handler and `finally`, and residue left in `lib/` reads as a phantom regression.
  Signals are serviced only once the current `spawnSync` returns, so a run looks
  unresponsive for up to the suite's 240 s; the single-instance lock in
  `tools/.mutate-guards.lock` exists because a second run's "restore" writes the first
  one's mutation back as the original. After ANY forced stop: `git status --short lib`,
  then `node tools/mutate-guards.js --check`.

## 4. Lint, commits and CHANGELOG

* `npm run lint` runs after every significant code change, on the same schedule as a
  commit — it is not a release-time check. ESLint 10 with the flat config at
  `eslint.config.js` (`@eslint/js` recommended plus the error-prevention and formatting
  rules this protocol code needs), over `lib/`, `test/`, `bench/`, `tools/` and `index.js`
  — the whole repository, not just the shipped library, because a probe that lies is as
  expensive as a bug in `lib/`. The tree is clean as of this writing; keep it that way,
  and settle a new error rather than re-explaining it. The binary is dev tooling and not
  a runtime dependency — this repository runs the global `eslint` (or `npx eslint@10`);
  `@eslint/js` in `devDependencies` is what `eslint.config.js` itself requires.
  * `--fix` may not reach what the config ignores: `lib/fast-timer.js` is vendored
    byte-for-byte, so its upstream formatting is part of the vendoring, and `out/` and
    `profs*/` are generated scratch.
  * `--fix` over `lib/` also rewrites the compact one-line guards
    (`if (bad) { this._fatal(C, '…'); return; }`) that `tools/mutate-guards.js` quotes
    verbatim, so the sweep is followed by `node tools/mutate-guards.js --check` (it exits
    1 on a mismatch) and by re-recording every anchor that no longer matches its source —
    the harness refuses to mutate a tree that does not fit the list. Measured on the first
    such sweep here: 29 of the 86 anchors went BAD with the suite still green, which is
    the silent part of the cost of `curly: 'all'`. Re-record by turning the *condition*
    off and leaving the guard's body in place, exactly the shape the list already uses;
    the whole line plus its block is quoted so the anchor stays unique.
  * A rule is disabled only in the narrowest scope that is true, with the reason written
    in the config next to it: `no-shadow` is off for test files (a differential test pairs
    a server handler's `(req, res)` with the client's response of the same name, and both
    names are the API's), `no-new-func` is off for `bench/` (the code-generated builder is
    the arm under measurement, AGENTS.md §7), `no-console` is off where the script's
    output *is* the result. `lib/` keeps all of them on.
  * An override block's `files` list is part of the rule: a `**/*.js` entry turns a "for
    test files" block into a global one, and every rule the block turns off is off
    everywhere.
  * An unused parameter that carries a signature keeps its position under `_`
    (`(code, _last)`); dropping it would delete the shape the reader is checking against.
  * Never answer a finding by weakening what `lib/` guards, by reordering a hot path, or
    by turning a rule off repository-wide.
* Measurement narratives live in `OPTIMIZATION.md` (numbered items), not in `lib/`. An
  in-code comment says what the code does and cites the item (`OPTIMIZATION.md item 13`);
  it does not repeat the numbers, the probe paths or the rejected alternatives — that
  knowledge is written down once, centrally, and a second copy in `lib/` goes stale the
  moment the item is re-measured. What stays in `lib/`: the mechanism a reader needs next
  to the code, RFC/§ anchors, and the file-header difference lists (`AGENTS.md` §2, §4).
* The repository is on GitHub (`origin`, branch `main`). One commit per completed change,
  made as soon as the change is verified (see `AGENTS.md` §9 for what "verified" means);
  do not batch unrelated work. Message: `type: imperative summary` (`fix:`, `perf:`,
  `test:`, `docs:`, `refactor:`, `chore:`), subject ≤ 72 chars. The body explains **why**,
  not what — the diff shows what. Include the measurement that justified a performance
  claim and the node source or probe that justified a parity claim.
* `CHANGELOG.md` is the user-facing history, reverse chronological, Keep-a-Changelog
  style:
  * a new `## [Unreleased]` section is where entries land; promote it to a version heading
    (with the date) when the version in `package.json` is bumped;
  * sections `Added` / `Changed` / `Fixed` / `Measured`;
  * record anything observable from outside: new API surface, changed event order, changed
    limit, changed wire behaviour, a performance result **with its paired numbers and its
    noise**;
  * do not record a pure internal refactor that changes nothing observable — that belongs
    in the commit message only.
* Do not commit generated artifacts: `out/`, `profs*/`, `*.cpuprofile`, `.DS_Store`.
  `lib/hpack-data.js` is generated but is committed on purpose — it is the table the
  encoder/decoder needs at require time and regenerating it needs the tools.
  `package-lock.json` is committed (reproducible dev-dep installs); `node_modules/` is
  not.

## 5. Open problems and known divergences — the full write-up

The one-line index is `AGENTS.md` §10. Do not "fix" any of these blind; each entry names
the decision, the measurements and the pins. None of them is a to-do list.

* **Graceful close: who is allowed to finish it.** h2lean's predicate is
  `_liveStreams === 0 && _outLen === 0 && _frozenLen === 0` (bytes handed to the socket
  with the write callback still owed) and it is re-run at `close()` and on a stream
  teardown. node's is `handle.hasPendingData()` — bytes still inside nghttp2, which its
  own write path drains before returning — re-run from `Http2Stream`'s teardown in a
  `setImmediate` ("Delay with setImmediate so we don't do it on the nghttp2 stack") and
  from nghttp2's `ongracefulclosecomplete`. The consequence is measured, not theoretical:
  close a session whose last stream is then answered normally, with a peer that says
  nothing back, and node destroys it within milliseconds while h2lean sits with live
  streams 0, queued bytes 0 and the callback delivered — forever
  (`test/goaway.test.js` pins both arms; `tools/probe-stream-teardown.js`, case
  `request-ended`, prints it). Two fixes were tried and both were measured, so this is a
  decision and not an oversight: re-checking inside `_onWritten` costs seventeen
  event-order traces that agree with node today, and the node-faithful deferred version
  (`setImmediate`) still costs seven — the session's `'close'` then goes out before the
  peer's. The third option is the structural one: node's `finishSessionClose` **does not
  destroy the socket of a gracefully closing session** — it ends it, `resume()`s it and
  waits for the peer, and `session.destroyed` is a state flag set in `closeSession()`, not
  a statement about the socket. h2lean still conflates the two: the socket teardown now has
  node's shape (end, then destroy after the flush — AGENTS.md §8, anchor 15), but the read
  side does not, so the peer's answer to our GOAWAY cannot arrive. What is missing here is
  the predicate, not the teardown: nothing re-runs `_maybeFinishClose()` once the last
  stream has answered.
* **A server stream nobody read the request of.** node closes it as soon as the response
  is finished: `Http2Stream[kMaybeDestroy]()` checks "headersSent, server session, no
  trailers, `!state.didRead`, `readableFlowing === null`" and calls `stream.close()` from
  the writable side's `'finish'`. On the wire that is `RST_STREAM(NO_ERROR)` after the
  response, and it is what releases the session's own closing predicate. h2lean has no
  such rule: the stream stays live until the peer ends its half, so a peer that sends
  HEADERS without END_STREAM and no DATA holds a stream (and a `_liveStreams` slot) open
  indefinitely — only the idle timeout lets it go. Measured both sides with
  `tools/probe-stream-teardown.js` (`answer-only` versus `resume-then-answer`, which is
  node's `didRead` boundary: reading the request stops the auto-close, and h2lean already
  agrees there). Fixing it means emitting the auto-close and its `RST_STREAM` on a path no
  trace covers today — a task of its own, not a switch. One measured consequence of
  leaving it open: when such a peer resets the socket, h2lean reports
  `sessionError:ECONNRESET` because it still owns a live stream, while node — which closed
  the stream at the end of the response — reports nothing.
* **An `end()` with no body on a stream that never responded.** node puts nothing on the
  wire: no HEADERS, no DATA, no END_STREAM, and `headersSent` stays false — the writable
  side finishes and the stream is let go only when the connection is. h2lean answers `200`
  with an empty `DATA/ES`, because a stream the peer never gets an answer on is released
  only by the idle timeout. Pinned as a KNOWN DIVERGENCE in
  `test/response-start.test.js`, which `tools/probe-response-start.js` measures both arms
  for. Matching node means a local-half close with no frame at all, a shape no teardown
  trace covers today. Two adjacent measured facts go with it: the auto-respond h2lean now
  shares with node belongs to the **write** path, not to `end()` (`write('x')` alone puts
  HEADERS in front of the DATA — h2lean used to queue the body first), and node's
  `Http2Stream` is a `Duplex`, so it emits `'finish'` on the stream object while h2lean's
  stream is an `EventEmitter` and does not.
* **An answer queued in the same read batch as a refusal goes out here and is discarded
  by node.** A handler that responds as soon as the stream opens has its HEADERS flushed
  by h2lean even when a later frame in the batch breaks a message-framing rule; nghttp2
  drops the queued response and the peer sees only the reset. Measured with
  `tools/probe-inbound-limits.js` (cases M1, M1b, N1, N2, P6, P8 — three runs each, no
  variation): the **codes agree**, only whether a HEADERS reaches the peer differs. A
  handler that answers after reading the body has nothing queued at refuse time and the
  two stacks then agree completely, which is why the parity tests for those rules are
  written with `respond: 'read'`. Matching node means discarding outbound state at refuse
  time — the stream-object-lifetime and graceful-close conflation of the first two bullets,
  not a patch. Pinned as a KNOWN DIVERGENCE in `test/inbound-limits.test.js`. It is also
  what leaves the two h2lean-only arms in `npm run spec` (http2/8.1.2.6, the
  `content-length` promise that does not equal the sum of several DATA frames, once over
  h2c and once over TLS): the spec target answers without reading (so the queued 200 is in
  the batch), and the frames the two stacks send for the shape are the SAME code —
  `RST_STREAM(PROTOCOL_ERROR)` short, `GOAWAY(INTERNAL_ERROR)` long — only h2lean's
  HEADERS reaches the tool first and node's does not. Re-run against a reading handler and
  the arm is green; the content-length accounting itself is at parity.
* **The h2spec arms that both stacks fail are not ours to fix — do not bend the code to
  the tool.** `npm run spec` prints three buckets and the middle one ("both (n) — node
  deviates too, parity keeps it") is the one that gets read as a bug list. Four arms sit
  in it over h2c and six over TLS on the run in `out/spec-final2.txt`; the count moves
  because two of them are a race (next bullet). h2spec 2.6.0 was written against RFC 7540:
  three of the four check wording RFC 9113 changed and the fourth checks a rule RFC 9113
  dropped, so the tool's expectation is not the RFC's.

  | arm | what the tool sends | what the tool demands | what the current RFC says | node | h2lean |
  |---|---|---|---|---|---|
  | `http2/5.1: closed: Sends a DATA frame` | `HEADERS`(END_STREAM), **wait for the stream to close**, then `DATA` | "MUST treat this as a connection error of type STREAM_CLOSED" | §5.1 *closed*: "An endpoint **MAY** treat receipt of any other type of frame on a closed stream as a connection error of type STREAM_CLOSED" | discards it | discards it |
  | `http2/5.1: closed: Sends a HEADERS frame` | the same, with `HEADERS` | "connection error of type STREAM_CLOSED" | the same MAY | discards it | answers `GOAWAY(PROTOCOL_ERROR)` and closes |
  | `http2/5.1: closed: Sends a DATA frame after sending RST_STREAM frame` | `HEADERS`, `RST_STREAM`, `DATA` | "MUST treat this as a stream error of type STREAM_CLOSED" (RFC 7540 §5.1) | §5.1 *closed* again, and for a frame that follows a reset: "An endpoint MUST minimally process and then discard any frames it receives in this state" | discards it | discards it |
  | `http2/5.1: closed: Sends a HEADERS frame after sending RST_STREAM frame` | `HEADERS`, `RST_STREAM`, `HEADERS` | the same | the same | discards it | discards it |
  | `http2/5.3.1: Sends PRIORITY frame that depend on itself` | `PRIORITY`(stream 1, depends on 1) | "MUST treat this as a stream error of type PROTOCOL_ERROR" (RFC 7540 §5.3.1) | RFC 9113 §5.3.2 deprecated the priority signalling and §6.3 keeps only `PRIORITY` on stream 0 → `PROTOCOL_ERROR` and a length ≠ 5 → `FRAME_SIZE_ERROR`; RFC 9218 deprecated the frame itself. No self-dependency rule survives | drops it | drops it |

  Three of the four are the RFC's own permission read the other way. RFC 9113 §5.1 relaxed
  RFC 7540's "MUST treat that as a connection error of type STREAM_CLOSED" for a frame on a
  closed stream to a "MAY ... as a connection error of type STREAM_CLOSED", and where a
  reset ended the stream it asks for the opposite of an error at all — minimal processing
  and a discard. Both stacks discard, which is what the current RFC asks for; the tool
  waits for the error and times out. The fourth is RFC 7540's self-dependency rule,
  dropped by RFC 9113 together with the signalling it belonged to, so dropping the frame
  is conforming. The tool's own verifier is why the silence is what fails:
  `spec.VerifyStreamError` accepts a `GOAWAY` with a matching code, an `RST_STREAM` with a
  matching code **or a closed connection**, so a stack that answers with the connection
  error the RFC permits passes the arm — only the silence cannot. Measured on both stacks
  in one session (`tools/probe-inbound-limits.js`, cases U1, U3, U6, K4): the
  `HEADERS`+`RST_STREAM`+`DATA` shape (U6) is answered `ACK` by node and `ACK ANSWERED` by
  h2lean — no error frame on either; the `HEADERS`(END_STREAM)+`DATA` shape (U1) with the
  two frames in separate reads is `ACK ANSWERED` on both and with them in one read
  `GOAWAY(5:STREAM_CLOSED)` on both; the self-dependent `PRIORITY` (K4) is `ACK ANSWERED`
  on both, the frame dropped. So there is nothing here to fix: a stack that answered the
  tool's expectation would end the connection on a frame the RFC lets it discard, and
  would do it where node stays silent. These arms are encoded in the `DOCUMENTED` map in
  `tools/check-h2spec.js`, so `npm run spec` reports them under "documented divergence"
  and keeps them out of the h2lean-only count that drives the exit code; add the §5
  write-up before adding a key there.
* **h2spec's `http2/5.1` 'half closed (remote): Sends a DATA frame' arm is a segmentation
  race on both stacks, not a verdict.** The tool writes `HEADERS`(END_STREAM) and then a
  `DATA` frame for the same id, back to back, and waits for an error. Whether the two
  frames reach the server in one read or in two decides the arm. In one read the request
  is still open when the `DATA` arrives — the half-closed(remote) the arm is written for —
  and both stacks refuse it with `GOAWAY(STREAM_CLOSED)`. In two reads the server has
  answered in between, the answer closed the stream, and the late `DATA` lands on a closed
  stream, where RFC 9113 §5.1 lets the endpoint discard it; the tool's wait for the error
  then times out. The report of a failing run says which of the two happened: its last
  event is the answer's 14-octet `DATA` frame, i.e. the server had answered and the frame
  was discarded. Measured with `out/probe-h2spec-51.js` (local scratch; both frames in one
  write versus two writes, six rounds each):
  the two shapes do not separate on loopback — back-to-back writes land in one segment —
  and the run that separates them is a delay between the writes
  (`tools/probe-inbound-limits.js` case U1 with `GAP=40`), where both stacks answer
  `ACK ANSWERED` and nothing else. In the conformance runs the split is not stable
  either: over five runs of the `http2/5.1` section on h2c, h2lean failed this arm in 2 of
  5 and node in 3 of 5 (in the other two node runs it fails the neighbouring 'half closed
  (remote): HEADERS' one instead), while the three 'closed:' arms fail on both stacks in
  5 of 5. The `http2/6.1` 'DATA frame on the stream that is not in "open" or "half-closed
  (local)" state' arm is the same shape with the same expectation — h2spec's own source
  calls it "duplicated with 5.1" — so it races the same way. The arm belongs with the
  other two races (`http2/7` and `http2/8.1.2.6`), not with the stable node-parity
  deviations: it is the third thing that moves the full run's totals between runs.
* **Which of two broken rules a header frame reports.** A CONTINUATION that is both over
  SETTINGS_MAX_FRAME_SIZE and not decodable as HPACK gets `GOAWAY(COMPRESSION_ERROR)` from
  node and `GOAWAY(FRAME_SIZE_ERROR)` from h2lean: nghttp2 hands a header block to the
  decoder while its octets are still arriving, so the decoding failure is what the peer is
  told first, while `_allowFrame()` refuses on the length without reading the payload at
  all. When the oversized block IS valid HPACK both stacks answer FRAME_SIZE_ERROR, and
  that case is asserted as a plain parity test; the undecodable one is pinned beside it as
  a KNOWN DIVERGENCE. Matching node here would mean moving the length check after the
  decode — giving up the earliest refusal in the inbound path so that an error code a peer
  cannot act on matches octet for octet.
* **Teardown event order.** For a request the application never read, node emits `req`'s
  `'end'`/`'close'` before the response finishes; h2lean emits them as the stream is torn
  down. All events fire; the order within the teardown turn differs. Documented in
  `lib/compat.js` and tolerated in `test/event-order.test.js` by exactly two allowances:
  the compat teardown tail is compared as a sorted **set** (`TEARDOWN_IGNORED` is empty —
  every event is present on both sides), and `normalize()` collapses a **repeated**
  `session.goaway`, because node's already-destroyed session never reports the peer's echo
  of ours while h2lean is still reading it. Nothing else in any trace is forgiven; adding
  a third rule needs a reason written next to it.
* `req.pause()`/`req.read()` do not slow the peer down: stream credit is released as the
  buffered body is consumed and connection credit in half-window steps, not per `read()`.
  Documented in `lib/compat.js`; closing it would need partial credit accounting.
