# Security policy — h2lean

## Reporting a problem

Please report a suspected vulnerability through GitHub's **private security advisory**
for this repository ("Report a vulnerability" on the project's Security page) rather than
in a public issue. A fix, together with a CHANGELOG entry naming the affected versions,
is published with the next release once the report is confirmed.

Do not open a public issue for an unfixed vulnerability, and do not run a vulnerability
scanner against a third-party deployment of this library as "research".

## What h2lean is, and what that means for the threat model

h2lean is a pure-JavaScript **HTTP/2 server library** whose public surface is
`node:http2`. It terminates **untrusted bytes from the network** and turns them into
objects an application trusts: request headers, a readable body stream, flow-control
state, and HPACK table entries. Everything behind it — routing, authorisation, the
HTTP/1.1 translation done by a downstream proxy — inherits whatever h2lean hands over.

There are exactly two classes of bug that matter here:

**A. Translation desync (request smuggling).** Whatever sits behind this server
translates fields into HTTP/1.1 one line at a time. If a recipient can read a field
differently than we do, that is smuggling: two "same" requests that a proxy and an
origin parse apart. The named family in this stack is CVE-2020-8287, where node took the
first of two copies of a field and ignored the second.

**B. Unbounded work per frame (denial of service).** One frame must not cost an
unpredictable amount of memory, CPU or event-loop time. Every limit below exists because
a peer can otherwise choose the cost.

Because the library is JavaScript, the memory-safety class (overruns, use-after-free)
does not apply; the equivalents are unbounded growth, caches that survive a change in
peer state, and work done per frame that a peer controls.

## The rule behind every number

No limit here was invented. Each one was **read out of the nghttp2 that node ships or
measured against a live `node:http2` server**, so that a peer's budget against node is
its budget against h2lean. Changing a value means changing it to node's, and a value that
was measured is pinned by a test that fails when the guard is removed.

| limit | value | what it bounds |
|---|---|---|
| `SETTINGS_MAX_FRAME_SIZE` | 16384 by default, enforced inclusively | a DATA, HEADERS or CONTINUATION payload larger than we advertised → `GOAWAY(FRAME_SIZE_ERROR)`. PING and SETTINGS carry their own fixed sizes and are never bounded by it |
| `SETTINGS_MAX_HEADER_LIST_SIZE` | compared in **HPACK entry octets**: Σ(name + value) + 32 per field | a header list we would otherwise decode and hand to the application. Answered per stream with `RST_STREAM(ENHANCE_YOUR_CALM)`; the connection stays up and the next request is served |
| `MAX_CONTINUATIONS` | 8 | one field block spread over unbounded CONTINUATION frames (CVE-2024-28182) → `GOAWAY(INTERNAL_ERROR)` |
| `MAX_SETTINGS_ENTRIES` | 32 | a SETTINGS frame with an unbounded number of entries, the one frame the size limit does not cover (CVE-2020-11080) → `GOAWAY(ENHANCE_YOUR_CALM)` |
| `RST_BURST` / `RST_RATE` | 1000 burst, 33 per elapsed second | rapid-reset (CVE-2023-44487): a peer opening and resetting streams to make us allocate per stream → `GOAWAY(INTERNAL_ERROR)` |
| `MAX_LITERAL` | 1 MiB per name or value | one HPACK literal the decoder has to buffer → `GOAWAY(COMPRESSION_ERROR)` |
| `MAX_OUT_BATCH` | 256 KiB per session | outbound staging that grows without bound while a peer stops reading |
| `MAX_SESSION_HOLD` / `STREAM_HOLD_CHARGE` | 8 MiB / 1 KiB per parked stream | unbounded live streams when a peer floods HEADERS and never returns flow-control credit; answered per stream with `RST_STREAM(REFUSED_STREAM)`, the connection stays up |
| `MAX_REFUSED_HISTORY` / `MAX_DROPPED_HISTORY` | 32 / 256 ids | the id lists scanned per DATA frame, grown by a peer that keeps opening, finishing and refusing streams: the refused ids (32), the ids a reset ended (256) and the ids whose message ended inside the read being processed (256) |
| `VALID_NAMES_MAX` / `MAX_SHAPES` | 1024 / 128 | the header-name cache and the inbound shape cache, both reachable from peer input |
| connection-window credit step | half of the advertised `initialWindowSize` | how often a `WINDOW_UPDATE` on stream 0 goes out. Node's nghttp2 lets the window run down to 32 768 octets before releasing anything (measured on a 640 KiB upload with `tools/probe-conn-credit.js`); crediting every consumed octet instead floods the wire with frames a peer never asked for |

Inbound flow control is deliberately strict about who pays for an unread body: credit is
returned when the **application** consumes the bytes, not when frames arrive. A body
nobody reads therefore applies backpressure to the sender instead of buffering here.

## Input validation

* Field names must be tokens and lower case (RFC 9113 8.2.1, and RFC 9113 10.3 names
  HTTP/2→HTTP/1.1 translation as the reason the rules are strict). Refusal happens at
  encode time, before a single octet is produced.
* A field value may not carry a control character (every CTL but HTAB) or DEL. On the way
  out the value is refused at encode time, like the name above. h2lean is stricter here
  than node, which does not look at an outbound value at all and puts a CR on the wire
  (measured against a live node server, `out/probe-out-value-matrix.js`): a CR in a value
  is what turns one field into two lines for a translator that splits on it. On the way
  in a regular field whose value carries such a character is **dropped** and the stream
  lives, the way nghttp2 drops it (`NGHTTP2_ERR_IGN_HTTP_HEADER`, measured character by
  character on a live node server, `out/probe-value-matrix.js`).
* The connection-specific fields (`connection`, `keep-alive`, `proxy-connection`,
  `transfer-encoding`, `upgrade`) are refused in both directions; the only exception is
  `te`, which may carry nothing but `trailers` (RFC 9113 8.2.2).
* Pseudo-headers are checked for presence, order, repetition, case and syntax, and a bad
  one is `RST_STREAM(PROTOCOL_ERROR)` — the request never reaches the application
  (RFC 9113 8.3.1, 8.3.2).
* `content-length` must be a digit string and may appear only once, and the arithmetic is
  checked against the DATA that follows (RFC 9113 8.1.1, RFC 9110 8.6): a body that runs past
  the promise ends the connection, one that falls short of it ends the stream, and a promise
  carried by a HEADERS frame that already ends the message is refused before the application
  sees the stream.
* A trailer block must arrive with `END_STREAM` (RFC 9113 8.1) — otherwise the stream is reset
  and the connection lives.
* Frame-state rules are enforced, not assumed (RFC 9113 5.1, 6.4, 6.5, 6.7, 6.9): a connection
  control frame may not name a stream, `RST_STREAM` may not name stream 0, `WINDOW_UPDATE` and
  `RST_STREAM` for a stream in the 'idle' state end the connection, a fixed-size frame is
  required to be that size even when this side ignores its payload (`PRIORITY`, RFC 9218), and
  `SETTINGS_ENABLE_PUSH` is a boolean.
* A `DATA` or `HEADERS` that names a stream whose message the **peer** already ended with
  `END_STREAM` is a connection error of type `STREAM_CLOSED` (RFC 9113 5.1, 8.2), and `DATA` for a
  stream in the 'idle' state — stream 0, an even id, an id above the highest one opened — is
  `GOAWAY(PROTOCOL_ERROR)`. What is *not* an error is a frame naming an id this side refused, an id
  a reset ended, or an id the peer skipped: those are discarded, which is why the session remembers
  the two ways a stream leaves the table apart rather than as one "unknown id". A peer can therefore
  neither invent a body for a finished request nor turn its own late frame into an error for the
  other clients on the connection.
* A HPACK dynamic-table size update is only accepted at the start of a header block
  (RFC 7541 6.3); one after a field line is `GOAWAY(COMPRESSION_ERROR)`, because a second
  decoder reading the block in order would rebuild a different context from ours.
* Malformed **regular** fields with an illegal but not upper-case character are dropped
  rather than killed (nghttp2's ignore rule): illegal fields are common on the public
  internet, and dropping the stream over one breaks sites. Both halves are tested. The
  same rule covers the value: a regular field whose value carries a character that may
  not appear in one disappears as well, and the request is still served.
* Unknown frame types are discarded (RFC 9113 5.5); what is *not* discarded is a frame
  that interrupts a field block, which is a connection error.
* The first frame after the connection preface must be SETTINGS without ACK
  (RFC 9113 3.4), so nothing is answered before the parameters are known.

## TLS

`createSecureServer()` delegates all certificate and protocol decisions to `node:tls` —
there is no certificate validation logic in this repository, by design, so TLS issues are
node's to fix. ALPN is pinned to `h2`; a connection negotiating anything else is destroyed
without becoming a session, and no ALPN at all is accepted (prior knowledge over TLS),
which is what node does.

`out/test-cert.pem` and `out/test-key.pem` are **self-signed localhost fixtures**
generated for the test suite. They are not credentials, are not shipped, and are
regenerated by `test/fixtures-tls.js`; never reuse or trust them.

## Operational notes for a deployment

* **The defaults advertise no SETTINGS values** (matching node: an empty first SETTINGS frame),
  which means no concurrency cap is placed on the peer by default. A server on the public
  internet should set `maxConcurrentStreams` explicitly, along with an `initialWindowSize` that
  matches the memory it is willing to commit per connection. The hold ceiling
  (`MAX_SESSION_HOLD`) bounds the worst case regardless.
* **Never log a full header block.** Requests carry `cookie` and `authorization`. The
  library does not log headers; keep it that way in your handlers too.
* **Network-layer denial of service is out of scope.** Rate limiting, connection
  acceptance policy and per-IP budgets belong in front of the server. These limits bound
  what one accepted connection can ask for; they do not bound how many connections arrive.
* **A shared HPACK table makes header caching per-connection state.** Anything that
  changes insertion, table size or the block caches has to keep the encoder's and the
  decoder's views in step — a desynchronisation there produces wrong HPACK on the wire.

## Known gaps in the hardening

Stated plainly so that nobody assumes coverage that does not exist:

* **No fuzzing harness.** The raw-socket drivers under `tools/` (notably
  `probe-wire.js`, `probe-inbound-limits.js` and `probe-stream-teardown.js`) are the seed
  a fuzzer would start from; the frame-level tests are hand-written cases, not exhaustive
  input generation.
* **No HTTP/2 client and no server push in v1**, so those code paths do not exist to be
  vulnerable. `connect()` and push say so loudly rather than half-working.
* **The client does not look at the value of a request header.** Node's client does not
  either, and both put a value carrying a CR on the wire (measured against a raw server,
  `out/probe-client-cr.js`), so such a value can split a field for a server behind the
  client. The server side of this library refuses to *send* such a value and drops one
  that arrives; the client keeps node's behaviour, which is a stated gap rather than a
  divergence.
* **A stream whose request body nobody read is held until the peer or the idle timeout
  lets it go.** node closes it as soon as the response is finished; h2lean does not yet, so
  a peer that sends HEADERS without END_STREAM occupies a `maxConcurrentStreams` slot and a
  session for as long as the keep-alive timeout allows. This is the open item recorded in
  `AGENTS.md` section 10, and the timeout is the current mitigation.
* **Guard coverage is measured, not assumed.** `tools/mutate-guards.js` rewrites each
  guard in place and runs the suite; three of its 60 anchors are reported as having no
  observable test, each with the reason written next to it — all three are live guards whose shape
  the peer or the scheduler will not pin (§10's FIN-vs-`destroy()` race, an event that always
  re-runs a check, a rule node itself reports two ways). A
  guard that no test can reach is a candidate for deletion, not for confidence.

## Supported versions

Security fixes are made against the current release line (0.1.0 at the time of writing),
verified against the same reference node version the parity claims use (node v26.12.1),
and recorded in `CHANGELOG.md` with the affected behaviour named.
