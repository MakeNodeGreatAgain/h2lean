# AGENTS.md — instructions for coding agents working on h2lean

Read this file before changing anything. Sections 2 and 3 are not style advice: they are
the rules that decide whether a change here is accepted or thrown away. Sections 6-9 state
where the working detail lives; this file is the general map, not the archive.

## 1. What this project is

h2lean is a **pure-JS HTTP/2 library** whose public surface is `node:http2`. The goal is a
drop-in replacement for application code: `require('node:http2')` becomes `require('h2lean')`,
and the same handler code keeps working on a stack with no nghttp2 C inside it.

* Compatibility target is **surface level** — names, argument shapes, event orders,
  wire-visible frame sequences, `rstCode`/`goawayCode` numbers. It is *not* byte-identical
  output and *not* identical internal state objects. A gap that is understood and written
  down is worth more than a gap closed by inventing something node does not do.
* **Scope**: the server (`createServer` h2c, `createSecureServer` h2s, the `stream` API and
  the compat (`req, res`) API) is the original and complete target; the client (`connect`)
  supports both transports too — h2c over `net` and h2s over `tls.connect` with the same
  ALPN rules as node (pinned by `test/client/client-tls.test.js`). Server push and
  `performServerHandshake` are not implemented and not on the table silently. They either
  are absent or throw `ERR_H2LEAN_NOT_IMPLEMENTED` — never a silent wrong answer.
* **Zero runtime dependencies.** `package.json` has no `dependencies`; the library (`lib/`,
  `index.js`) must stay importable with nothing installed. Tests run on the built-in
  `node:test`. `devDependencies` are allowed, but only for out-of-band dev tooling the
  shipped library never requires — today `fast-xml-parser` (`tools/check-h2spec.js` reads
  h2spec's JUnit report) and `uWebSockets.js` (a comparison arm in `bench/showcase/`,
  never used by the library). Do not add a dependency to the runtime path to solve
  something that is 30 lines of code. `lib/fast-timer.js` is a vendored byte-for-byte
  copy of undici's and must stay exactly as it is upstream.
* Measured against **node v26.12.1** (`engines: >=20`). Every "node does X" statement in
  this repository means that version unless the comment says otherwise.

## 2. Working rules

* Code, comments and identifiers stay English, because the whole code base is English.
  Analysis is welcome as a preamble to the change, never as a substitute for it.
* Two claims are never accepted here without proof:
  1. **"it is faster"** — worthless unless measured in one session against the paired
     `node:http2` arm (§7). Report the number, the noise and the case, or report nothing.
  2. **"node behaves this way"** — worthless unless read out of node's own bundled source
     or measured off a live node server in the same session (§3). node's documentation has
     been wrong more than once (see the `PEER_DEFAULTS` comment in `lib/session.js`:
     `doc/api/http2.md` quotes different numbers for `initialWindowSize` and
     `maxHeaderListSize` than the running binary reports).
* Keep the diff surgical: touch only what the request requires. Do not reformat, rename or
  "improve" adjacent code, and do not delete code you merely think is dead — say so in the
  reply instead. Exceptions must be paid for with a measurement.
* `npm test` and `npm run lint` belong to a change the way the commit does: run both after
  every significant edit and before the commit (§9). The tree lints clean, so a new error
  is part of the change that introduced it.
* Comments in this repository are documentation. They carry the *reason* a line exists, an
  RFC clause, a CVE id, or the measurement that killed an alternative — and a measurement
  narrative that has been written down in `OPTIMIZATION.md` cites its item number there
  instead of repeating itself in `lib/`. New non-obvious code gets that treatment;
  matching the surrounding comment density is required, not optional.
* Behavioural differences from node are documented **in the header comment of the affected
  file** (`lib/compat.js` and `lib/server.js` carry the difference lists), not in prose
  files. The prose files have fixed scopes: `CHANGELOG.md` = what changed between versions;
  `AGENTS.md` = how to work; `CONTRIBUTING.md` = the contributor-facing testing, lint,
  commit and divergence detail; `OPTIMIZATION.md` = the measured performance knowledge
  (numbered items §7's rule refers to); `SECURITY.md` = the outward-facing threat model.
  None of them is a place to leave an implementation note.
* If a task cannot be finished honestly (a guard no test can reach, a measurement inside
  noise), say that plainly in the report. An overclaimed result is worse than an
  unfinished one.

## 3. How to find out what node actually does

Do not guess, do not recall, do not read the docs. Use, in this order:

1. **Read node's own JS source.** The internal modules are in the binary:
   `node -e 'process.stdout.write(process.binding("natives")["internal/http2/core"])'`,
   or `--expose-internals` plus `require('node:internal/http2/core')`. Note that
   `require('node:module').builtinModules` does **not** list the internal entries.
2. **Measure a live server.** Start `node:http2` on a port, speak to it, print what
   arrives. That is what `tools/probe-wire.js` (frame by frame over a raw socket),
   `tools/probe-events.js` / `tools/probe-session-events.js` (event order per turn of the
   loop) exist for, and what `test/diff-harness.js` automates inside the suite.
3. **Remember that a JS-layer search proves nothing about the C layer.** The rapid-reset
   budget, the SETTINGS-entry cap, the CONTINUATION cap and pseudo-header validation are
   all inside bundled nghttp2, not in `internal/http2/core.js`. A `grep` over node's JS
   that finds no counter is a hint to read nghttp2 or to measure — not evidence node
   lacks the feature. A conclusion drawn from a JS-only grep is the mistake this rule
   exists to prevent.
4. **Cite from a primary source read in the same session.** An RFC clause or a CVE number
   in a comment must come from the nghttp2 file+function name or an authoritative CVE
   record (OSV/GHSA). If a clause cannot be verified, name the class of attack without a
   number rather than guessing one.

The reference implementation is nghttp2: when a codec or protocol rule is derived from the
RFC plus probing, state explicitly where and why it differs from upstream. Closer parity
is a legitimate reason to deviate, and then the deviation gets a measurement (the HPACK
encoder's insert-on-second-sighting rule in `lib/hpack.js` is the worked example).

## 4. Repository layout

```
index.js            public entry: createServer/createSecureServer/connect, settings
                    pack/unpack, constants re-exported from node:http2, notImplemented()
package.json        scripts: test (test:server, test:client), lint, spec, gen, bench:*
lib/
  server.js         front door for both transports; owns the listening socket, the session
                    set, and the decision whether the compat layer is instantiated.
                    Header comment = the connection-lifetime difference list.
  session.js        the HTTP/2 connection state machine (2700 lines): frame dispatch,
                    inbound validation, flow control, HPACK per connection, the outbound
                    staging buffer + queue, GOAWAY/close/destroy, idle timeout. Also the
                    constants the client session reuses. The core of the project.
  stream.js         Http2Stream equivalent: two halves, credit release on consumption,
                    close/destroy semantics, the _endPending deferred-EOF machinery.
  compat.js         the (req, res) layer over a stream. Header comment = the deliberate
                    difference list.
  headers.js        outbound header validation + the frozen-object block cache.
  hpack.js          HPACK decoder/encoder (RFC 7541), dynamic table, eviction caches, and
                    the encoder's insertion policy (header comment = where it differs from
                    nghttp2 and why).
  hpack-data.js     GENERATED by tools/gen-hpack-data.js — never hand-edit.
  frames.js         frame constants and in-place (offset,len) header read/write.
  pieces.js         helpers over an array of buffer views: the octets of a frame that
                    arrived in pieces, read without copying them together.
  fast-timer.js     vendored byte-for-byte (undici).
  client.js         connect() entry for the client.
  client-session.js ClientHttp2Session equivalent: the client-side state machine; reuses
                    session.js for the settings/credit constants.
  client-stream.js  ClientHttp2Stream equivalent (a Duplex).
test/               node:test files, one area each; diff-harness.js is the shared tool;
                    test/client/ holds the client suite
bench/              benchmarks; matrix.sh is the standard one; *.js arms for A/B;
                    bench/micro/ holds the microbenches cited by OPTIMIZATION.md
tools/              probes, the mutation harness, the table generator, the h2spec runner
out/                local scratch and generated artifacts — gitignored, not a reference
```

Read the header comment of the file you are about to change. It usually already contains
the answer to "why is it like this" and the measurement that would be needed to change it.

## 5. The architecture in the handful of facts you need before editing

* **A frame is never an object.** `frames.js` exposes `readHeader/writeHeader` operating on
  a caller-provided buffer and publishes results into module-level slots (`FH_LEN`,
  `FH_TYPE`, …). One allocation avoided per frame is the point.
* **One output staging buffer per connection, plus a reference queue.** Every frame
  produced while draining one socket read is serialised into the staging buffer and
  leaves corked in one batch; an owned body ≥ `BODY_MIN` travels by reference instead
  (OPTIMIZATION.md item 15). Flow-control credits coalesce over the same batch. The
  batch is bounded by `MAX_OUT_BATCH`.
* **The three caches, and what invalidates each.** (a) the response-block cache in
  `headers.js` is keyed by the *identity* of the caller's headers object and only holds
  bytes for `Object.isFrozen()` objects — sealed is not enough, a sealed object can still
  be reassigned — and it lives on the session because the HPACK dynamic table is
  per-connection; (b) the inbound shape cache in `session.js` (`MAX_SHAPES`) maps a header
  block hash to the key array used to build the object; (c) `VALID_NAMES` in `headers.js`.
  Any change to HPACK insertion, table size or the block cache must re-check all three.
* **Issued buffers stay valid.** A chunk handed to the application is either a view of the
  buffer node read off the socket or a buffer built for that frame alone; neither is ever
  recycled underneath the app. A frame that arrives in pieces is kept as the views it
  arrived in (`lib/pieces.js`) — the session holds no buffer per connection for it, and a
  frame that has to be decoded is put in a buffer of its own, never a reused one.
* **Read-side ownership.** When the local side ends a stream that nobody read, the owed
  EOF is replayed (`_endPending`) *before* `close` is emitted, and `close` requires both
  halves done. Checking only one half is how streams get leaked.
* **The compat layer is lazy:** `req`/`res` objects are instantiated only once something
  listens for `'request'`, because a `'stream'` handler must not pay for two objects per
  request. Its handlers are **called, not subscribed** — one dispatch function
  (`lib/stream.js`, `emit()`), no EventEmitter subscriptions on the stream except
  `'error'`, and the dispatch runs *before* the event reaches the emitter, which is the
  order the subscriptions had.
* **`_dispatch()` returns `this._open`**, which is what stops the read batch at the frame
  that destroyed the session. Several "unreachable guard" questions resolve at that line.

## 6. Testing

`npm test` runs the server suite and `test/client/`; the timeout in the script is
load-bearing, and the suite is differential by design — `test/diff-harness.js` runs the
same application script against `node:http2` and against h2lean and compares the
transcripts. Pin node's *measured* value, not h2lean's behaviour. The full rules — the
timeout, the test-count expectation, the known fragility classes (deadline vs terminal
frame, the `date` header, the nghttp2 destructor abort, the GOAWAY-echo race), and the
external gates (`npm run spec`, `test/interop-external.test.js`) — are in
`CONTRIBUTING.md` §1-2. Run `node tools/mutate-guards.js --check` after any edit that
moves a guarded line (CONTRIBUTING.md §3).

## 7. Performance work

Every optimisation is measured or it is not an optimisation. The unit of proof is the
paired matrix — `GC=1 sh bench/matrix.sh <label>`, every h2lean case against the *same*
case on `node:http2`, both arms re-measured in one session, ABBA interleaving with ≥3
rounds, `cpuPerReq` as the metric, drift judged per case, both arms verified to do the
same work (READBODY on uploads). Micro-effects are measured at the granularity where
they act (`bench/*.js` microbenches), with identical call shapes on both arms; attribution
goes by profiles and calibrated component probes, never by arithmetic or cross-language
profile diffs. Details and worked traps: `CONTRIBUTING.md` §3.

**All measured performance knowledge lives in `OPTIMIZATION.md` as numbered items** —
techniques that earned their place and things that were measured and rejected. Cite the
item number from code comments instead of repeating the narrative. Do not re-litigate a
rejected item without new evidence. The current measured position versus node is recorded
in `CHANGELOG.md` (paired ratios only; re-measure both arms before writing new ones).

## 8. Security

h2lean terminates **untrusted bytes from the internet** and turns them into objects an
application trusts. There are exactly two classes of bug that matter here, and every limit
below is a defence against one of them. `SECURITY.md` is the outward-facing version of this
section — reporting, deployment advice, the gaps that are known and stated; this section is
the working rules that keep those claims true, and a change that moves a limit or a gap has
to be written into both.

**A. Translation desync (request smuggling).** Whatever sits behind this server translates
fields into HTTP/1.1 one line at a time; if a receiver can read a field differently than
we do, that is smuggling. The named family in this stack is CVE-2020-8287 (node took the
first of two copies of a field and ignored the second).

* Field names must be tokens and lower case; values may not contain NUL/LF/CR
  (`lib/headers.js`, and RFC 9113 10.3 names HTTP/2→HTTP/1.1 translation as the reason the
  rules are strict: an intermediary "MUST validate fields according to the rules in
  Section 8.2 before translating"). Refusing at encode time, before a single octet is
  produced, is the whole mitigation.
* The five connection-specific fields (`connection`, `keep-alive`, `proxy-connection`,
  `transfer-encoding`, `upgrade`) are refused in **both** directions — inbound as a stream
  error, outbound as a thrown error — and the one exception is `te`, which may carry nothing
  but `trailers` (RFC 9113 8.2.2).
* The two-step decision on malformed inbound headers is deliberate and must not be
  "simplified" into one rule: a bad **pseudo-header** (missing, repeated, empty, unknown,
  out of order, upper-case name) is `RST_STREAM(PROTOCOL_ERROR)` and the stream never
  reaches the application; a **regular** field with an illegal-but-not-uppercase character
  is dropped and the stream lives (nghttp2's `NGHTTP2_ERR_IGN_HTTP_HEADER` — illegal
  regular headers are common on the public internet and killing streams over them breaks
  sites). Both halves are pinned by `test/request-validation.test.js`.
* The HPACK byte cache is keyed in two levels -- the field name, then the value -- so no
  separator character appears in a cache key and no value can alias another field line.
  The NUL check on values is still required (it is what keeps a value from being smuggled
  into a second line by an HTTP/1.1 translator), but it is no longer load-bearing for the
  cache keys. Changing the key shape requires re-checking the value check anyway.

**B. Unbounded work per frame (DoS).** Every bound below was read off nghttp2 or measured
against a live node server so that a peer's budget against node is its budget against us.
Changing a number means changing it to node's, not to a new one.

| limit | value | shape it bounds | pinned by |
|---|---|---|---|
| `MAX_SETTINGS_ENTRIES` | 32 | CVE-2020-11080 — SETTINGS is exempt from the frame-size limit, so entries are the cost | `test/frame-limits.test.js` |
| `MAX_CONTINUATIONS` | 8 | CVE-2024-28182 — one block spread over unbounded CONTINUATION frames | `test/frame-limits.test.js` |
| `RST_BURST` / `RST_RATE` | 1000 / 33 per elapsed second | CVE-2023-44487 rapid reset; server-side only, GOAWAY(INTERNAL_ERROR) on empty | `test/rst-flood.test.js` |
| `MAX_OUT_BATCH` | 256 KiB | unbounded outbound staging when a peer stops reading | `test/outbound-batch.test.js` |
| `MAX_SESSION_HOLD` / `STREAM_HOLD_CHARGE` | 8 MiB / 1 KiB per parked stream | unbounded live streams when a peer sends HEADERS but never returns flow-control credit (h2spec 5.1.2 shape) | `test/outbound-batch.test.js` |
| `VALID_NAMES_MAX` | 1024 | a process-lifetime name cache grown by reflected untrusted input | `test/header-validation.test.js` |
| `MAX_SHAPES` | 128 | the inbound shape cache | `test/shape-cache.test.js` (invalidation only) |
| `localSettings.maxFrameSize` | 16384, enforced inclusively | DATA / HEADERS / WINDOW_UPDATE larger than advertised | `test/inbound-limits.test.js` (PING and SETTINGS are exempt) |
| `localSettings.maxHeaderListSize` | compared in **HPACK entry octets**: Σ(name+value) + 32 per field (RFC 7541 4.1, restated for this setting in RFC 9113 6.5.2) | a header list a peer wants us to decode and hand to the application | `test/inbound-limits.test.js` — both sides of the line, per stream, `RST_STREAM(ENHANCE_YOUR_CALM)`, connection stays up |
| `MAX_LITERAL` (`lib/hpack.js`) | 1 MiB per name or value | one HPACK literal the decoder must buffer | `test/inbound-limits.test.js` (6 000 served, 1 500 000 → `GOAWAY(COMPRESSION_ERROR)`; node has no 4096 cap any more) |
| `MAX_REFUSED_HISTORY` / `MAX_DROPPED_HISTORY` | 32 / 256 ids | the lists `_isGone()` and the two state checks scan per DATA frame, grown by a peer that keeps opening and finishing streams: `_refused` (32), `_dropped` (256, the ids a reset ended) and `_ended` (256, the ids whose message ended inside the read being processed) | `test/inbound-limits.test.js` (40 refusals, 300 answered requests each reset by the client, and 300 answered in one write — all three lengths are asserted) |

A fast-properties header map hangs on the process-wide transition tree of its root map, and
the field names in it come from the peer, so the tree is peer-driven. It is bounded anyway:
V8 rehomes the root map when its transition array fills (observed: `Transitions #1536`), and
the retained heap is flat across 1.8 million distinct six-field name sets
(bench/micro/nullproto-root.js, bench/micro/nullproto-tree2.js, bench/micro/nullproto-tree3.js).
Any change that puts more of the peer's names into a fast map has to re-check this.

**B2. The frame-state and message-framing rules** (RFC 9113 5.1, 5.3, 6.4, 6.5, 6.7, 6.9, 8.1,
8.2 and RFC 7541 6.3) are the same defence in a different shape: each refuses a peer's frame
before it can move a window, open a stream or finish a message that the application is about
to trust. Each was measured with `tools/probe-inbound-limits.js` after h2spec 2.6.0 failed it
for h2lean alone, and each has both a parity assertion and an anchor in
`tools/mutate-guards.js`. The rules:

* A connection control frame names no stream: `SETTINGS`/`PING` on a non-zero id,
  `RST_STREAM` on id 0 → `GOAWAY(PROTOCOL_ERROR)`. A fixed-size frame is that size even
  when we do not read its payload (`SETTINGS|ACK` with entries, `PING|ACK` off 8 octets,
  `PRIORITY` off 5) → `GOAWAY(FRAME_SIZE_ERROR)`; RFC 9218 made PRIORITY's contents
  ignorable, not its length. `SETTINGS_ENABLE_PUSH` is a boolean.
* State rules apply to 'idle' too: `WINDOW_UPDATE`, `RST_STREAM` and `DATA` for an id
  above `_maxInboundSid` end the connection. *Why* an id at or below it is gone decides
  the verdict: a record (`_refused`, `_dropped`) → discard, both stacks silent (a `DATA`
  there still earns connection credit — do not let a peer invent credit for a stream
  nobody opened); a `HEADERS` with no record is RFC 9113 5.1.1's unexpected identifier →
  `GOAWAY(PROTOCOL_ERROR)` while node stays silent — a written-up divergence, §10.
  `PRIORITY` on stream 0 is `GOAWAY(PROTOCOL_ERROR)` while node drops it; a self-dependency
  is dropped (RFC 9218).
* A frame naming a stream whose message the peer already ended is the connection's:
  `DATA`/`HEADERS` after the peer's `END_STREAM` → `GOAWAY(STREAM_CLOSED)`; `_ended` (a
  completed message) vs `_dropped` (reset) is the distinction the rule turns on, and both
  lists are bounded (table above). `DATA` in 'idle' (sid 0, even id, above the highest
  opened) → `GOAWAY(PROTOCOL_ERROR)`.
* A trailer block has to arrive with END_STREAM (8.1) → `RST_STREAM(PROTOCOL_ERROR)`.
  `content-length` is a promise with two halves: too few octets by message end is the
  stream's problem, too many is the connection's (`GOAWAY(INTERNAL_ERROR)`), and a
  promise on a HEADERS that already ends the message is refused at the accept decision.
* Those message-framing checks are **reader-dependent** (nghttp2 measures against consumed
  octets): pin them with `respond: 'read'` — `respond: true` would assert h2lean's
  laxness and call it parity.
* A HPACK dynamic-table size update belongs at the start of a header block (RFC 7541 6.3):
  one after a field line → `GOAWAY(COMPRESSION_ERROR)`, because a second decoder would
  rebuild a different context. `lib/hpack.js` checks `out.length === 0`.
* Connection credit is released in **half-window steps**, not per read batch (measured on
  node: the window runs to 32768 before `WINDOW_UPDATE(0)`). Crediting every consumed
  octet put a `WINDOW_UPDATE` on the wire in front of the frame h2spec 6.1 waits for. The
  threshold still pays for bytes discarded on a gone stream, so the anti-deadlock rule
  the counter exists for holds.
* The header-block **buffer** is bounded without a wire check on `maxHeaderListSize`: at
  most `MAX_CONTINUATIONS` frames of `maxFrameSize` octets. Refusing on the compressed
  length was measured wrong — node answers per stream with `ENHANCE_YOUR_CALM` after
  decoding; a connection-level error for one oversized request punishes every client.
* Three limits have no failing test; the reason is written next to the anchor in
  `tools/mutate-guards.js`, not papered over (the `socket.end()` inside `_maybeFinishClose`
  indistinguishable from the `destroy()` after it, the closing re-check in `_dropStream`
  re-run by the peer's echo, the ignore-`ECONNRESET`-after-GOAWAY rule node itself
  reports two ways). None is a test that could be written against node today.
* Every cache reachable from peer input must be **bounded**, and overflow behaviour must
  be a clear, not a partial invalidation that leaves stale bytes associated with a new
  table state. Encoder/decoder byte caches are dropped on a table-size change by epoch
  counter — a missed invalidation here produces wrong HPACK on the wire.
* The first frame after the preface must be SETTINGS without ACK (RFC 9113 3.4).
* Unknown frame types are **discarded** (RFC 9113 5.5, measured on live node: types 10,
  99, 0xff with 4 KiB payloads leave the connection up). What is *not* ignored is a frame
  that interrupts a header block. `CONTINUATION` is type 9 and `10` is ALTSVC; the numbers
  in `lib/frames.js` were each verified against a live node endpoint rather than recalled.
* TLS: `createSecureServer` overwrites any application `ALPNProtocols` with `['h2']`, the
  way node does, and accepts `'h2'` or no ALPN (prior knowledge over TLS); everything else
  is destroyed without becoming a session. Certificate validation stays delegated to
  `node:tls` — do not add validation logic here. `out/test-cert.pem` / `out/test-key.pem`
  are self-signed localhost fixtures regenerated by `test/fixtures-tls.js`; they are not
  credentials and must never be repurposed or trusted.
* No fuzzing harness exists yet. If one is added, `tools/probe-wire.js` is the starting
  point: it already drives a raw socket and decodes the reply frame by frame.
* **Never log a full header block in library code.** Requests carry `cookie` and
  `authorization`. Probe scripts under `tools/` print headers by design and must stay
  local-only tools; they are not to be wired into the server.

## 9. Commits

* Commit **after each logically significant change**, as soon as it is verified and the user has confirmed — do not batch unrelated work into one commit. Verified means
  `npm test` green **and `npm run lint` clean**, plus the mutation harness re-run for any
  touched guard anchor. A red or half-finished state is not a trigger.
* Message: `type: imperative summary` (`fix:`, `perf:`, `test:`, `docs:`, `refactor:`,
  `chore:`), subject ≤ 72 chars; the body explains **why**, not what, and includes the
  measurement or node source/probe that justified a claim.
* `CHANGELOG.md` keeps the user-facing history under `## [Unreleased]` (Keep-a-Changelog:
  `Added` / `Changed` / `Fixed` / `Measured`): anything observable from outside, with
  paired numbers and noise for a performance result. A pure internal refactor goes in the
  commit message only. Never commit generated artifacts (`out/`, `profs*/`,
  `*.cpuprofile`); `lib/hpack-data.js` is generated but committed on purpose.
* The lint mechanics (flat config scope, `--fix` vs guard anchors, vendored-file ignores,
  override rules) and the full commit/CHANGELOG conventions: `CONTRIBUTING.md` §4.

## 10. Open problems and known divergences (do not "fix" blind; they need a decision)

One line each; the measurements, the tried fixes and the standing instruction live in
`CONTRIBUTING.md` §5. None of these is a to-do list.

* **Graceful close: who is allowed to finish it.** The predicates differ (h2lean counts
  owed write callbacks; node drains nghttp2 first) and three fixes were tried and measured
  red. A decision, not an oversight — pinned by `test/goaway.test.js`.
* **A server stream nobody read the request of.** node auto-closes it after the response
  (`RST_STREAM(NO_ERROR)` on the wire); h2lean holds the stream until the peer ends its
  half. Measured boundary with `tools/probe-stream-teardown.js`.
* **An `end()` with no body on a stream that never responded.** node puts nothing on the
  wire; h2lean answers `200` with an empty `DATA/ES`. KNOWN DIVERGENCE in
  `test/response-start.test.js`.
* **GOAWAY last-stream id for a frame refused before its stream was processed.** node
  names the stream, h2lean names only reached ids. KNOWN DIVERGENCE in
  `test/inbound-limits.test.js`; no client can act on the difference.
* **An answer queued in the same read batch as a refusal** goes out here and is discarded
  by node; the codes agree, only whether a HEADERS reaches the peer differs. KNOWN
  DIVERGENCE in `test/inbound-limits.test.js`; it explains the two h2lean-only
  `npm run spec` arms (http2/8.1.2.6).
* **The h2spec arms both stacks fail are not ours to fix — do not bend the code to the
  tool.** They check RFC 7540 wording RFC 9113 changed or dropped. Encoded in the
  `DOCUMENTED` map in `tools/check-h2spec.js`; add a §5 write-up before adding a key.
* **h2spec's `http2/5.1` half-closed(remote) arms are a segmentation race** on both
  stacks, not a verdict; one read vs two decides it.
* **Which of two broken rules a header frame reports** (over-sized undecodable
  CONTINUATION: `COMPRESSION_ERROR` on node vs `FRAME_SIZE_ERROR` here). KNOWN
  DIVERGENCE beside the parity case in `test/inbound-limits.test.js`.
* **Teardown event order** for a request nobody read: all events fire, the order within
  the teardown turn differs; tolerated by exactly two allowances in
  `test/event-order.test.js`.
* `req.pause()`/`req.read()` do not slow the peer down: stream credit is released as the
  buffered body is consumed and connection credit in half-window steps, not per `read()`.
  Documented in `lib/compat.js`; closing it would need partial credit accounting.
