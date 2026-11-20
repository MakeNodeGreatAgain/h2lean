# Changelog — h2lean

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com); entries are reverse chronological.

## [0.5.4] - 2026-11-20

### Fixed

- **`client.destroy()` says goodbye on the wire.** The staged outbound batch was dropped
  outright, so a peer saw a bare socket close where node's `closeSession` terminates the
  nghttp2 session with the derived code (`core.js:1265`) and leaves
  `GOAWAY(last=0, code)` behind. Measured on a live node client against a raw
  frame-decoding peer and now matched:
  - `destroy()`, `destroy(err)`, `destroy(8)` and `destroy(err, 1)` frame
    `GOAWAY(0, 0 / 2 / 8 / 1)` respectively, with no streams open as well;
  - the goodbye is submitted even after a graceful `close()` already sent one -- node
    emits two `GOAWAY` frames in that order, the second carrying the destroy's code;
  - every open stream **but the last** is reset with the session's code first: N open
    streams produce N-1 `RST_STREAM` frames in ascending id order, then the `GOAWAY`.
    Verified for one to four open streams and codes 0, 2 and 8, and a stream the
    application destroyed beforehand is not counted. Why the last stream is exempt lives
    in the C++ layer, which was not read; the rule implemented here is the measurement;
  - nothing goes out before the handshake completes, and the batch reaches the socket
    ahead of the FIN the way `_fatal`'s goodbye does.
  Pinned by `test/client/client-destroy-goaway.test.js`. This closes the first of the two
  gaps recorded under 0.5.3; the non-zero inbound `GOAWAY` gap remains open.

## [0.5.3] - 2026-11-20

### Fixed

- **A client stream destroyed by the application reports node's reset code.**
  `ClientHttp2Stream._destroy` called `session.rstStream(id)` with no code, so every
  application destroy framed `RST_STREAM(CANCEL)` and left `rstCode` at 0. node derives
  the code (`internal/http2/core.js:2503-2522`): a stream whose wire life already ended
  keeps the code it closed with, otherwise the session's `goawayCode || destroyCode` is
  the starting point and an error overrides it -- the session's code first, then `CANCEL`
  for node's own `AbortError`, then `INTERNAL_ERROR`. Measured on a live node client
  against a raw frame-decoding peer, and now matched:
  - `req.destroy()` frames `RST_STREAM(0)` and reports `rstCode` 0 (was 8 on the wire);
  - `req.destroy(new Error(...))` frames `RST_STREAM(2)`, reports `rstCode` 2 and re-emits
    the caller's error unchanged (was 8 and 0);
  - an aborted `options.signal` frames `RST_STREAM(8)` and reports `rstCode` 8. The
    `AbortError` is now a class rather than a plain `Error` carrying the name, because
    node tests `err instanceof AbortError`: an application error merely *named*
    `AbortError` takes the `INTERNAL_ERROR` branch on both stacks;
  - a request body still open is `'aborted'` and then ended before the reset goes out,
    as node's `closeStream` does. `writableEnded` afterwards is true for `destroy()` and
    false for `destroy(err)`, matching node's own Duplex behaviour;
  - a destroy after the stream closed still puts nothing on the wire.
- **`client.destroy(error, code)` normalizes its arguments as node does**
  (`core.js:1622-1637`): a number *is* the code and builds `ERR_HTTP2_SESSION_ERROR` for a
  non-zero one, an error with no code implies `INTERNAL_ERROR`, and an explicit second
  argument wins. The code is recorded as the session's `destroyCode`, so an open stream
  destroyed with the session reports `rstCode` 0 / 2 / 8 / 1 for `destroy()`,
  `destroy(err)`, `destroy(8)` and `destroy(err, 1)` -- all four previously reported 0,
  and `destroy(8)` emitted the number itself as the error. The inbound-reset and
  outbound-refusal paths now reach the same derivation node uses (`closeStream` records
  the code, `destroy()` carries no error, `_destroy` synthesizes
  `ERR_HTTP2_STREAM_ERROR` from it), so a peer's reset code is no longer at risk of being
  overridden by the error branch.
- Known gaps left open and written into `test/client/client-destroy-code.test.js`: node's
  `session.destroy()` also submits `GOAWAY(last=0, code)` and h2lean's does not; and a
  non-zero inbound `GOAWAY` destroys node's session at once (`core.js:781`) while h2lean
  leaves it open, so a stream destroyed after one reports the caller's error and frames a
  reset where node reports `ERR_HTTP2_SESSION_ERROR` and frames nothing. The `rstCode`
  agrees in both.

## [0.5.2] - 2026-11-20

### Fixed

- **An inbound request refusal no longer emits a session `'frameError'`.**
  `lib/session.js` `_streamError` emitted one with the stream id and the error code
  swapped and `DATA` hardcoded as the frame type. node's `onFrameError` is the *send*-error
  path -- its own comment reads "Called by the native layer when an error has occurred
  sending a frame. This should be exceedingly rare." (`internal/http2/core.js:675`) -- and
  emits `(type, code, id)`. Measured on a live node server for a trailer block carrying a
  pseudo-header, a trailer block without `END_STREAM`, a `WINDOW_UPDATE` overflowing the
  send window, a connection-specific request field and an unknown request pseudo-header:
  node emitted no `'frameError'` on the stream or the session in any of the five. The
  outbound refusals (`_refuseSend`, the PUSH_PROMISE bound) keep the event and already had
  node's argument order. `test/request-validation.test.js` now pins the four
  stream-refusal shapes differentially, on both emitters.

## [0.5.1] - 2026-11-20

### Fixed

- **`client.settings()` takes a settings object, as `node:http2` does.** It previously
  called `Buffer.from(payload)`, i.e. it expected a raw SETTINGS payload, so drop-in code
  calling `session.settings({headerTableSize: 4096})` failed with `ERR_INVALID_ARG_TYPE`.
  The method now mirrors node's `settings()` -> `submitSettings()` -> `settingsCallback()`
  (`internal/http2/core.js`), measured against a live node client through a raw
  frame-decoding peer:
  - only the entries the caller named go on the wire, in ascending setting-id order
    rather than the object's key order;
  - the argument checks are node's: `undefined` is legal and submits an empty SETTINGS
    frame, a `Buffer` passes and submits nothing, `null`/a string/an array are
    `ERR_INVALID_ARG_TYPE`, and an out-of-range or wrongly-typed value is
    `ERR_HTTP2_INVALID_SETTING_VALUE` (a `TypeError` carrying `.actual`, plus `.min`/`.max`
    for a range check) -- not `ERR_OUT_OF_RANGE`. A non-function truthy callback is
    `ERR_INVALID_ARG_TYPE`;
  - `maxHeaderSize` is honoured as the deprecated alias of `maxHeaderListSize`, with
    node's warning when the two differ;
  - the callback is deferred to the matching `SETTINGS|ACK` and receives
    `(null, localSettings, duration)`, and `'localSettings'` is emitted on every ACK --
    including the one for the connection's own first SETTINGS frame, which has no
    callback. A submitted value reaches `localSettings` when its own ACK arrives, not at
    submit time (measured for all seven settings: `headerTableSize: 8192` produced events
    `[4096, 8192]`);
  - `duration` is node's monotonic-clock reading in milliseconds, not an elapsed time
    (measured: a submit 120 ms earlier answered with `524545981.423` where
    `Number(process.hrtime.bigint())/1e6` read `524545981.445`);
  - a call made while connecting is deferred to `'connect'` instead of being refused;
  - ten unacked SETTINGS frames are the limit, counting the connection's own first one:
    the next submit sends nothing and destroys the session with
    `ERR_HTTP2_MAX_PENDING_SETTINGS_ACK`, as node does.
- **`client.localSettings` reports the eight keys node reports**, in `getSettings()`'s
  order, with `maxHeaderListSize` and its alias `maxHeaderSize` at `4294967295` -- what
  nghttp2 has in effect on a live client, not the `65535` `getDefaultSettings()` prints.
  The client enforces no inbound header-list bound, so this changes the reported object
  (the one the `settings()` callback hands out), not a guard or the wire.

  Known gap, stated in the `lib/client-session.js` header: node also accepts a
  `customSettings` object of raw id/value pairs and submits those entries; h2lean submits
  the seven standard settings only. `test/client/client-settings-call.test.js` pins every
  rule above differentially against `node:http2`.

## [0.5.0] - 2026-11-20

### Added

- **`options.signal` and `options.waitForTrailers` on `client.request()`** (TODO 4.3).
  A `ClientHttp2Session.request()` call now accepts `options.signal`: the signal's
  `abort` listener destroys the stream with node's `AbortError` shape (`name`
  `'AbortError'`, `code` `'ABORT_ERR'`, message `'The operation was aborted'`,
  `cause` = `signal.reason`), a non-`AbortSignal` throws `ERR_INVALID_ARG_TYPE`
  synchronously, and an already-aborted signal destroys the stream before any
  frame goes out. The listener is disposed on the stream's `'close'`. Signal
  handling lives on the client request path only, exactly as in node's
  `internal/http2/core.js` -- `stream.respond()` and the compat server API do
  not read a signal on either stack. `waitForTrailers` mirrors node's
  `STREAM_FLAGS_HAS_TRAILERS`: the last DATA leaves without `END_STREAM`,
  `'wantTrailers'` fires before the Duplex `'finish'`, and `stream.sendTrailers()`
  then puts the trailer block on the wire with `END_STREAM` (a block with no fields in
  it sends no header section at all -- see Changed). Calling
  `sendTrailers()` on a stream that was not opened with `waitForTrailers`
  throws `ERR_HTTP2_TRAILERS_NOT_READY`. `test/abort-waitfortrailers.test.js`
  pins every case differentially against `node:http2`.
- **ORIGIN (RFC 8336) and ALTSVC (RFC 7807), both directions** (TODO 4.2). A server
  session now exposes `session.altsvc(alt, originOrStream)` and `session.origin(...origins)`,
  mirroring node's `ServerHttp2Session` methods and their argument validation exactly (same
  `ERR_HTTP2_ALTSVC_INVALID_ORIGIN`, `ERR_HTTP2_ALTSVC_LENGTH`, `ERR_HTTP2_ORIGIN_LENGTH`,
  `ERR_INVALID_URL`, `ERR_OUT_OF_RANGE` codes; same `kQuotedString` character check on `alt`;
  same 16382-byte budget). A session built with `createServer({origins: [...]})` auto-sends
  one ORIGIN frame at connect, the way core.js `setupHandle` does. Both stacks now also
  parse inbound ALTSVC (frame type 10) and ORIGIN (frame type 12) and emit the client
  `'altsvc'` (`alt, origin, streamId`) and `'origin'` (array) events; a malformed payload
  on either is discarded rather than fatal, as measured on live node. `session.originSet`
  is now a real getter (core.js's `ArrayFrom(initOriginSet(this))`): the TLS seed origin
  (from `socket.servername` or `socket.remoteAddress`, bracketed on IPv6, plus
  `:${socket.remotePort}`, run through `getURLOrigin`), plus every origin a peer asserted
  via ORIGIN. Over h2c it reads `undefined`, matching node's TLS gate. A client session
  accepts `options.maxOriginSetSize` (node default 128, min 0); an inbound ORIGIN that
  would overflow it destroys the session with `ERR_HTTP2_TOO_MANY_ORIGINS`. Node's C++
  `CHECK`-abort on `altsvc(alt)` with neither an origin nor a stream id cannot be reproduced
  in pure JS; h2lean throws `ERR_HTTP2_ALTSVC_INVALID_ORIGIN` for that one degenerate call
  instead (documented in `lib/session.js` beside `checkAltSvc`). `test/altsvc-origin.test.js`
  pins every case differentially against `node:http2`.
- **`session.socket` now returns node's safe socket Proxy** (TODO 4.1). Both the
  server `Http2Session` and the client `ClientHttp2Session` expose a public
  `socket` that is a `Proxy` over the session, as `node:http2` does: `setTimeout`,
  `ref`, `unref` and `destroy` are re-bound to the session, the raw-socket mutation
  accessors (`write`, `end`, `pause`, `read`, `resume`, `emit`, `setEncoding`,
  `setKeepAlive`, `setNoDelay`) throw `ERR_HTTP2_NO_SOCKET_MANIPULATION` on access,
  and every other property falls through to the underlying `net`/`tls` socket, so
  `session.socket instanceof net.Socket` holds and reads like `remoteAddress`
  work. The proxy is built lazily and dropped (reads `undefined`) once the session
  has closed, matching node's `cleanupSession`. `test/socket-proxy.test.js` pins
  the surface differentially against `node:http2`. Drop-in code that reached for
  `session.socket` (previously absent) now finds node's object.

### Changed

- **`sentHeaders` / `sentTrailers` report the message that went out, not the object the
  application passed.** `respond()` used to store the caller's own map (or the `{}` it
  normalizes no argument to), and a client stream's `sentHeaders` was `null`. Both are now
  node's snapshots, read out of `internal/http2/core.js` (`prepareResponseHeadersObject`,
  the memoized getter at :2225, `sendTrailers`) and `internal/http2/util.js`
  (`prepareRequestHeadersObject` / `prepareRequestHeadersArray`) and measured off a live
  node server and client in one session: a null-prototype map that keeps the caller's key
  spelling and order, `':status'` as the coerced **number** (`'201'` reads back `201`, no
  status reads back `200`), the generated `date` last (absent with `sendDate: false` or
  when the application supplied its own), a repeated name in the flat-array form folded
  into an array of values, and the pseudo-headers `request()` resolved -- appended in the
  order `:method`, `:authority`, `:scheme`, `:path` for an object, prepended in that order
  for an array. A pushed client stream's `sentHeaders` is `undefined` (there is no request
  map to fold), a pushed server stream's is the resolved request map, and `sentTrailers`
  is a null-prototype copy on both sides, `undefined` until a block is sent. The views are
  built on the first read and kept, so the encode path allocates nothing extra and the
  frozen-identity HPACK block cache is untouched; the caller's object is never mutated.
  The client's informational `'headers'` event also carries the raw
  HEADERS frame flags as its second argument, the way node's
  `emit(stream, event, obj, flags, headers)` does. Pinned by
  `test/upstream/sent-headers.test.js` (a port of `test-http2-sent-headers.js`) and
  `test/client/client-push.test.js`.

- **Trailer semantics follow node, on the stream API and on the compat API.** Four rules
  were read out of `internal/http2/core.js` and `internal/http2/compat.js` in one session
  and measured off a live `node:http2` server frame by frame:
  * An **empty** trailer block puts no HEADERS on the wire. The END_STREAM the body
    withheld rides on a zero-length DATA instead (node's compat plain answer:
    `HEADERS/EH, DATA len4 flags 0x0, DATA len0 flags 0x1`; h2lean sent
    `HEADERS flags=0x5 len=0` there, a trailing header section with no fields in it).
  * The compat answer asks for trailers on every response -- `kBeginSend` passes
    `waitForTrailers: true` -- and answers the `'wantTrailers'` callback with the
    response's trailer map. `res.end()` is now node's `stream.end()` and nothing else;
    the block is sent from the callback, so an application that called `addTrailers()`
    still gets its fields after the body and one that did not gets no header section.
  * `stream.sendTrailers()` is node's three synchronous throws in node's order --
    `ERR_HTTP2_INVALID_STREAM`, then `ERR_HTTP2_TRAILERS_ALREADY_SENT`, then
    `ERR_HTTP2_TRAILERS_NOT_READY` -- where the permission to call it comes from the
    `'wantTrailers'` callback (`state.trailersReady`), not from the `waitForTrailers`
    option. A call that arrived before the callback used to emit a stream `'error'`.
  * A stream that still owes a trailer block is not finished: `localClose` no longer lets
    the peer's END_STREAM close it. Without that, a compat answer with no body against an
    unread request put HEADERS on the wire and nothing after it, and the client waited for
    an END_STREAM that never came.
  KNOWN DIVERGENCE, one error name: a second `sendTrailers()` in the same turn as the
  first reports `ERR_HTTP2_TRAILERS_ALREADY_SENT` on node and `ERR_HTTP2_INVALID_STREAM`
  here, because node defers the trailer submit to a `setImmediate` while h2lean frames it
  in the call. AGENTS.md §10 / CONTRIBUTING.md §5. Pinned by
  `test/upstream/trailers.test.js` (ports of `test-http2-trailers.js` and
  `test-http2-no-wanttrailers-listener.js`), which compares the whole transcript.

### Fixed

- **A client stream reset by the peer now tears down the way node's does.** Three things
  were missing, all in `onStreamClose` / `closeStream` / `_destroy`
  (`internal/http2/core.js`). A `RST_STREAM(NO_ERROR)` is a clean end, not a discard: node
  pushes null and defers the destroy to `'end'` ("Preserve buffered reads after a clean
  reset"), so the buffered body is delivered and `'end'` fires before `'close'` with
  `rstCode` 0 — h2lean destroyed at once and never reported `'end'`. A reset that finds the
  request body still open emits `'aborted'` and ends the writable half, so `'finish'` fires
  and `writableEnded` is true; h2lean reported neither and left `writableEnded` false.
  And `_destroy` pushes null unconditionally, which is why `CANCEL` (8) also reaches
  `'end'` while an error code does not — `destroy(err)` skips `'end'`, `destroy()` does not.
  The client stream now carries node's `aborted` flag, and `_pump` settles the writable
  half of a reset stream without framing: nghttp2 refuses DATA and HEADERS on a closed
  stream, where h2lean put an `END_STREAM` on the wire for a stream the peer had already
  reset. Measured on v26.12.1 against a raw peer over the nine reachable combinations of
  code (0, 1, 8), `END_STREAM` ahead of the reset or not, and the request body ended or
  not: every transcript now matches, event order included. The unread-body case is a wait
  on both stacks — no `'end'` and no `'close'` until the body is consumed — and is pinned
  as one. Pinned by `test/client/client-rst.test.js` and by
  `test/upstream/large-write-destroy.test.js`, a port of node's
  `test-http2-large-write-destroy.js` (a 60000-octet write then `destroy()`, against a
  content-length promise of 120000): the wire was already identical on both arms —
  HEADERS, four DATA frames, `RST_STREAM(0)` — and only the client's reading of it
  differed. A body over the send window is pinned beside it: `write()` reports
  backpressure and 65535 octets reach the client before the reset ends the message.
  One divergence is written down rather than closed: a stream leaves the client session's
  map at `END_STREAM`, where node keeps it until the Duplex is destroyed, so a reset
  arriving after a completed response never reaches a stream whose body was never ended
  or whose response is still buffered unread — node reports `aborted`/`finish`/`close`,
  h2lean waits. Recorded in `lib/client-session.js` and excluded from the new test.

- **An outbound header block over the session's `maxSendHeaderBlockLength` is now refused,
  the way nghttp2 refuses it.** h2lean put any block on the wire however large; node
  compares an *estimate* against a per-session bound at frame-prep time
  (`nghttp2_hd_deflate_bound` = `12 + 12*fields + Σ(name.length + value.length)`, plus
  `NGHTTP2_PRIORITY_SPECLEN` = 5 for HEADERS and 0 for PUSH_PROMISE, against
  `NGHTTP2_MAX_HEADERSLEN` = 65536 by default). The estimate counts JS string lengths, not
  UTF-8 octets — measured on v26.12.1, values of `é` (2 octets/char) and `€` (3) fall on the
  same line as `X`. For `{'test-trailer': 'X'.repeat(n)}` the estimate is `41 + n`: under
  `maxSendHeaderBlockLength: 200`, n=159 is served and n=160 refused; under the default,
  n=65495 is served and n=65496 refused. The option is taken on a `typeof === 'number'` test
  with no range check and coerced to uint32 (`updateOptionsBuffer`,
  `internal/http2/util.js`) — measured, 200.9 bounds like 200 and -1 like 4294967295.
  The refusal is not a throw: `respond()`, `sendTrailers()` and `additionalHeaders()` return
  normally, the stream emits `frameError(1, 6, sid)`, and a turn later it closes with
  `ERR_HTTP2_STREAM_ERROR` / `Stream closed with error code NGHTTP2_FRAME_SIZE_ERROR` and
  `rstCode` 6 while the session closes behind it (node's `onFrameError`). A refused
  *response* block also stops the body: nghttp2's response-DATA predicate requires a stream
  opened by a sent response HEADERS, so `end('hello')` puts no DATA out either. A refused
  *informational* block stops nothing — the final response still goes out and the stream
  closes with `rstCode` 0. The estimate is compared when the block is framed, so a trailer
  block stashed behind a body that did not fit the send window is refused when the queue
  drains — measured, the same transcript with a 200 000-octet body ahead of the 64 KiB
  trailer, and no `'end'` on the client because the `END_STREAM` the trailers carried never
  went out. One divergence is written down rather than closed: with no reader
  on the request half, node's unread-request auto-close reaches the stream before the
  deferred error close and reports 0 where h2lean reports 6, because the two schedule the
  same pair in opposite orders. Pinned by
  `test/upstream/exceeds-server-trailer-size.test.js`, a port of node's
  `test-http2-exceeds-server-trailer-size.js` with both sides of the bound, the multi-byte
  values and the option coercion asserted against node's measured transcript first.

- **The same outbound bound now applies to a client's request and trailer blocks and to a
  server's `PUSH_PROMISE`.** `connect()` accepts `maxSendHeaderBlockLength` (it was read on
  the server only, and silently ignored on the client), with the same `typeof === 'number'`
  gate and uint32 coercion. The estimate is accumulated in the pass that already sizes the
  encoder scratch and carried in the request-block cache, so a cached block is bounded like
  a fresh one. Which stream closes on the refusal is nghttp2's prep-error path, and it
  differs per frame kind — all three measured on v26.12.1:
  - a **request HEADERS** opens the stream, so nghttp2 closes it itself with
    `NGHTTP2_REFUSED_STREAM`: `frameError(1, 6, sid)`, `ERR_HTTP2_STREAM_ERROR` naming
    `NGHTTP2_REFUSED_STREAM`, `rstCode` **7**, session close, and **nothing on the wire** —
    the server sees no stream at all. For a request with a pinned one-character
    `:authority` the estimate is `120 + n`; under a bound of 200, n=80 is served and n=81
    refused.
  - a **client trailer HEADERS** is submitted on a stream that is already open, so nghttp2
    closes nothing and node's deferred `close(6)` lands: `rstCode` **6**, and
    `RST_STREAM(6)` does go out — the peer reports `ERR_HTTP2_STREAM_ERROR` / 6. One field
    `'x-big'` gives `34 + n`; under a bound of 150, n=116 is served and n=117 refused.
  - a **PUSH_PROMISE** rides on the parent's id, so `frameError(5, 6, parentSid)` lands on
    the *parent* while the *promised* stream closes with `ERR_HTTP2_STREAM_ERROR` /
    `NGHTTP2_INTERNAL_ERROR`, `rstCode` **2**. The parent's own response still goes out and
    closes it with `rstCode` 0, and the client never sees a push. `pushStream()`'s callback
    still runs with no error and still receives the stream object: node hands the callback
    to `process.nextTick` and the send loop refuses the frame after that tick, so the
    promised stream's `respond()`/`end()` submit into a closed stream and reach no wire.
    No priority spec, so five fields give `121 + n`; under a bound of 150, n=29 is pushed
    and n=30 refused, and under the default n=65415 / 65416.
  Pinned by `test/outbound-bound.test.js`, which asserts node's measured transcript first
  for every case. The GOAWAY echo is deliberately not recorded there: whether the peer's
  GOAWAY arrives before the local session finishes closing is a scheduling race, and node's
  server sends two where h2lean sends one.
- **A header block that does not fit one frame carries `END_STREAM` on its HEADERS frame,
  not on the CONTINUATION that finishes it.** Both senders (`lib/session.js sendHeaders`,
  `lib/client-session.js _sendHeaders`) put the flag on the last CONTINUATION and left the
  HEADERS at `0x0`. RFC 9113 §6.2 makes the CONTINUATION frames part of the HEADERS frame,
  §5.1 says they cause no state transition, and §8.1 states it outright for trailers: the
  sequence starts "with a HEADERS frame ... where the HEADERS frame bears an END_STREAM
  flag". Measured on node's wire for a 20 000-octet trailer value: `HEADERS sid 1 flags 0x1
  len 16384`, then `CONTINUATION flags 0x4`. No receiver that follows the RFC accepted the
  old placement — measured across all four stack pairings, a node client answered an h2lean
  server with `ERR_HTTP2_STREAM_ERROR` and `rstCode` 1 (`PROTOCOL_ERROR`) after the response
  headers, and an h2lean client against an h2lean server received the trailers and then
  waited forever: no `'end'`, no `'close'`. All four pairings now report
  `response, end, trailers, close:0`. Pinned by `test/header-split.test.js`, which asserts
  the frame/flag sequence on both arms and the four-pairing round trip. A third path frames
  a trailer block — one stashed behind a body that did not fit the send window, framed when
  the queue drains — and it put the whole block in a single HEADERS however large, with no
  CONTINUATION at all. It goes through `sendHeaders` now. Measured with a 200 000-octet body
  ahead of a 20 000-octet trailer value: node serves the exchange, while h2lean's peer
  answered `ERR_HTTP2_ERROR` and never saw `'end'`. Pinned by a second four-pairing case in
  the same file.
- **A stream ended by `RST_STREAM` reports node's error, and a graceful reset or a client
  abort reports none.** node's rule is one line (`internal/http2/core.js:2543`): an error is
  built only when the code is neither `NGHTTP2_NO_ERROR` (0) nor `NGHTTP2_CANCEL` (8), and
  its message is `Stream closed with error code <name>` from the C++ `nameForErrorCode`
  table. Measured off a live node client and a live node server, one reset per code, all 14
  codes plus 14, 100 and 4294967295 (outside the table the message names the number). Two
  gaps closed: a client stream answered *every* code with `ERR_HTTP2_STREAM_ERROR` and the
  message `HTTP/2 stream error`, including 0 and 8, which node does not error on at all; and
  a server stream answered a peer's reset with `'close'` and no `'error'` for any code.
  Both sides now share one factory (`lib/stream.js streamErrorOf`, names in
  `lib/frames.js ERROR_NAMES`). An inbound reset is still not a `'frameError'` on the stream
  or the session. Pinned by `test/rst-error.test.js`, which drives a raw peer and compares
  the full transcript on both arms for every code.
- **A `421 Misdirected Request` answer now removes the origin it was served on from
  `session.originSet`** (RFC 8336 §3.2; node's `emitStreamEvent`). The client session
  records the origin each request is really sent for — an explicit `:scheme` and
  `:authority`/`host` in the request's own headers win over the session's, the order core.js
  `prepareRequestHeadersObject`/`getAuthority` uses — and the first response `HEADERS` with
  `:status` 421 deletes that origin. Before this, the refused origin stayed in the set, so
  an application that reads `originSet` to pick an authority kept choosing the one the server
  had just rejected. Over h2c nothing changes; `originSet` reads `undefined` there on both
  stacks. Pinned by `test/upstream/origin.test.js`.
- **`session.altsvc()`, `session.origin()` and `connect({ maxOriginSetSize })` now produce
  node's error strings.** Three paraphrases were visible to code that matches on
  `error.message`. A non-string argument said `Received object` where node names the value
  (`Received an instance of Array`, `Received type symbol (Symbol(x))`,
  `Received function namedFn`, a string truncated at 28 characters): node's
  `determineSpecificType` and `addNumericalSeparator` are ported in `lib/session.js` and
  shared with the client option check. The length errors said "16382 octets" where node says
  "16382 bytes". And `maxOriginSetSize` reported one error class for every bad value where
  node splits `ERR_INVALID_ARG_TYPE` (a non-number) from `ERR_OUT_OF_RANGE` (`NaN`, negative).
  Verified against `node:http2` in one session — a 26-call `altsvc` matrix, the `origin`
  validation list, and a 17-value `maxOriginSetSize` sweep, 0 message differences — and
  pinned by `test/upstream/altsvc.test.js`, `test/upstream/origin.test.js` and
  `test/upstream/origin-set-max-size.test.js`.
- **A client session's teardown now follows node's lifecycle rules in four places**, all
  read out of `internal/http2/core.js` and measured on v26.12.1:
  - `request()` on a destroyed or closed session **returns a stream that errors and closes**
    instead of throwing. node keeps argument validation synchronous but captures the
    session-state failure and dispatches `process.nextTick(requestOnError)` —
    `this.destroy(error)` (:1836-1844, :1930-1931) — so a retry issued from a stream callback
    does not throw before the session's lifecycle handlers run. A destroyed session gives
    `ERR_HTTP2_INVALID_SESSION`, a closed one `ERR_HTTP2_GOAWAY_SESSION`, both on the
    stream's `'error'` and followed by `'close'`.
  - `ping()`, `settings()`, `goaway()`, `setNextStreamID()` and `setLocalWindowSize()` throw
    `ERR_HTTP2_INVALID_SESSION` once the session is destroyed. Each opens with that guard in
    node; h2lean had none, so the calls silently succeeded (and `settings({})` failed later
    with an unrelated `ERR_INVALID_ARG_TYPE`).
  - A request **abandoned while the socket is still connecting submits nothing**. node's
    `requestOnConnect` returns early when the stream's session reference is gone, and
    `_destroy` clears it (:2546), so `req.destroy()` or an aborted signal before `'connect'`
    leaves no stream on the server. A session closed in the meantime refuses the queued
    request with `ERR_HTTP2_GOAWAY_SESSION` rather than sending it.
  - When the socket closes, **every live and pending stream is closed with
    `NGHTTP2_CANCEL`** before the session closes (`socketOnClose`, :3653), and CANCEL is not
    an error (:2543). Without the sweep a request whose HEADERS went out but whose answer
    never came stayed live forever: the session emitted `'close'` while the stream was still
    open and never destroyed. Measured on a server that destroys its session from its
    `'stream'` handler — node reports the request's `'close'` with `rstCode` 8, h2lean left
    `req.destroyed` false indefinitely.
  Pinned by `test/upstream/client-destroy.test.js`, a port of node's
  `test-http2-client-destroy.js`: all nine blocks, each asserted against node's measured
  transcript first. The raw-socket destroy upstream reaches through the internal `kSocket`
  symbol is re-expressed with the public `options.createConnection`. The session-level
  `AbortSignal` block needed no implementation — node's http2 core wires a signal only on
  the request path, and `connect(url, {signal})` works through `net`/`tls`, which both
  stacks already pass the option to; the h2c and h2s transcripts are identical. The
  request's `rstCode` in the destroy-before-goaway block is deliberately not recorded: it
  depends on whether the GOAWAY echo lands before the socket dies, the race
  `CONTRIBUTING.md` §5 describes.

### Measured

- **The native-Duplex server stream question is settled with the true arm, not a
  proxy** (`bench/micro/duplex-native-ab.js`, OPTIMIZATION.md item 41). A patched
  copy of `lib/` (`duplex-native-variant.patch`, built and applied by the harness)
  makes `Http2Stream extends Duplex` in node's native shape; three arms (lean EE /
  native Duplex / node:http2), four cases, ABCCBA x3, server cpuPerReq, SINK/req
  verified equal. Medians lean -> duplex: GET 0 B 2.192 -> 3.383 us/req (x1.54),
  GET 1 KB down 3.243 -> 3.891 (x1.20), POST 1 KB up 2.758 -> 3.993 (x1.45),
  POST 16 KB up 9.130 -> 9.535 (x1.04); spreads 0.065-1.211 us. The +1.2 us on
  small requests matches the three-arm micro's ~1.4 us intrinsic prediction, so
  the EventEmitter design stands and the parity route stays a lazy shim, not
  `extends Duplex`. No library change.

## [0.4.2] - 2026-11-19

### Added

- **A third client benchmark arm: `@passcod/faith`** (`bench/client/`), the
  Rust/reqwest-backed fetch client, driven by the same nghttpd responder, slot
  loop and cpuPerReq metric as the node:http2 and h2lean arms. Added to check
  faith's README claim that it is faster than node:http2: on this matrix it is
  3-8x slower per request, and two committed probes show why both results are
  honest. `tools/probe-faith-tls.js` rules out the TLS asymmetry (all three
  clients on the same TLS+ALPN nghttpd: TLS costs the JS arms ~11-14 µs/req at
  16 KiB, faith stays ~4-5x above them). `tools/probe-faith-shape.js` replicates
  faith's own bench — an IN-PROCESS node:http2 server judged by wall-clock rps,
  not CPU — where faith does win on 64 KiB bodies (14.2k vs 7.1k req/s at c64)
  by moving TLS/HPACK/framing onto Rust threads off the shared event loop,
  while still paying more total CPU and losing at 1 KiB. The arm is a
  whole-client comparison, not a shape-identical one: faith 1.1.0 has no h2c
  prior knowledge, so its arm alone runs nghttpd with TLS+ALPN, one pooled
  Agent per connection group replaces the pinned sockets, and a fetch +
  body-reader loop replaces `sess.request()`. `tools/probe-faith.js` pins the
  transport facts; the server certificate is a new serverAuth fixture pair,
  because rustls rejects the existing test fixture's missing EKU extension.
  The package is a devDependency only — the library never requires it.

### Fixed

- **`session.request(headers, null)` now throws a `TypeError`** where it used to be
  silently accepted — the closer shape to node, whose `assertIsObject(options)` throws
  `ERR_INVALID_ARG_TYPE` there (measured live; the code and message still differ).
  The unread `{ ...options }` defensive copy is gone: node spreads options because it
  validates and then mutates the copy for priority options, h2lean only reads
  `endStream`, so the copy had no reader — one object allocation per request removed
  (the speed effect measured inside noise, so no performance claim).
  `test/client/client-session.test.js` pins the synchronous throw on both stacks.

## [0.4.1] - 2026-11-19

### Fixed

- **Upstream push-test parity.** The portable push files of node's own suite
  (`test-http2-*` with `pushStream` / `createPushResponse` / `captureRejections`, tag
  v26.12.1) are ported to `test/upstream-push.test.js` — 16 tests, each run against both
  stacks, h2lean and `node:http2` as the opposing peer. The fixes below are what the port
  found.
- **Compat `res.createPushResponse(headers, callback)`** — server push over the
  `(req, res)` API, with node's order: callback validation, closed-response check,
  `pushStream` with empty options (`test/upstream-push.test.js`).
- **`captureRejections` routing for push** — with `EventEmitter.captureRejections` on, a
  rejecting `'stream'` listener now destroys that stream (the pushed one for a push) and
  every other rejecting event destroys the session, the way node's
  `captureRejectionSymbol` handlers do; the server and client sessions both route it.
- **Client `maxReservedRemoteStreams`** (default 200, like node): a PUSH_PROMISE that
  would exceed the limit is answered `RST_STREAM(CANCEL)` on the promised id and the
  stream is never emitted.
- `remoteSettings.enablePush` / `enableConnectProtocol` are booleans, the way node's
  `getUnpackedSettings` reports them (`value !== 0`, internal/http2/core.js:3786) — a
  listener doing `settings.enablePush === false` now sees what node shows.
- A client stream's `rstCode` is initialised to `0` (`NGHTTP2_NO_ERROR`, core.js:2152):
  a stream closed without a reset now reports `0`, not `null`.
- A rejection routed to a pushed stream whose two halves completed in the same turn no
  longer loses its `'error'` event — node's push streams run `autoDestroy: false` so its
  `destroy(err)` always reaches a live Duplex; h2lean's completion-driven auto-destroy
  could swallow it (the upstream capture-rejection block-4 shape, measured against live
  node).

## [0.4.0] - 2026-11-18

### Added

- **Client PUSH_PROMISE reception.** `connect()` now accepts server pushes by default
  (`enablePush: true` in the client's local settings, matching node). The session emits
  `'stream'` with `(pushedStream, requestHeaders, flags)` when a PUSH_PROMISE arrives;
  the pushed stream emits `'push'` (not `'response'`) for its response headers. The
  pushed stream's writable half is pre-ended (`writableFinished === true` at the event).
  CONTINUATION-split promise blocks are reassembled. Destroying the pushed stream sends
  `RST_STREAM(NO_ERROR)`; late frames for it are silently ignored. A PUSH_PROMISE
  received with `enablePush: false` is `GOAWAY(PROTOCOL_ERROR)`, as node does.
  Pinned by `test/client/client-push.test.js` (10 differential tests against node's
  own server and cross-stack round trips).

## [0.3.0] - 2026-11-18

### Added

- **Server push: `stream.pushStream(headers, options, callback)` and `stream.pushAllowed`.**
  The server can now promise resources to the client via PUSH_PROMISE (RFC 9113 6.6).
  `pushAllowed` reflects the client's SETTINGS_ENABLE_PUSH; `pushStream` validates
  request-style pseudo-headers, fills defaults from the parent request (`:method` GET,
  `:authority`, `:scheme`, `:path` '/'), reserves sequential even stream ids, splits
  oversized promise blocks into CONTINUATION, and delivers the pushed stream via
  `callback(null, stream, headers, 0)` on the next tick. Synchronous throws:
  `ERR_HTTP2_PUSH_DISABLED`, `ERR_HTTP2_NESTED_PUSH`. HEAD implies `endStream`.
  The pushed stream's readable side is pre-ended (no request body); the application
  answers it through the normal `respond()`/`write()`/`end()` path. Inbound
  RST_STREAM/WINDOW_UPDATE on locally-pushed even ids are accepted; GOAWAY
  last-stream-id is not moved by push. Pinned by `test/server-push.test.js`
  (14 differential tests against node's own client).

## [0.2.6] - 2026-11-18

### Added

- **`performServerHandshake(socket, options)`** — a server `Http2Session` over a socket the
  caller already holds, with no server object, as node's does (`initializeOptions` +
  `new ServerHttp2Session(options, socket, undefined)`). It speaks the `stream` API only (no
  compat layer, because there is no server to attach one to), accepts a `net.Socket`, a
  `TLSSocket` or any generic `Duplex` (`stream.duplexPair()` works), and matches node on the
  socket-lifetime behaviour measured off v26.12.1 (`tools/probe-handshake.js`): a
  still-connecting socket is waited for, a paused socket's buffered octets are drained, and
  binding a socket twice throws `ERR_HTTP2_SOCKET_BOUND`. `options`/`options.settings` are
  validated with node's `ERR_INVALID_ARG_TYPE` codes, and only the asked-for settings are
  advertised to the peer. Pinned by `test/server-handshake.test.js`, whose scenarios mirror
  node's own `test/parallel/test-http2-perform-server-handshake.js`.

### Fixed

- **A server session's `'connect'` event now fires at setup with `(session, socket)`, as
  node's does.** It previously fired when the peer's preface arrived and carried no
  arguments; node emits it from `setupHandle` on the next tick after the session is bound,
  before any frame is read (`tools/probe-handshake.js` CASE1/CASE6).
- **A cleartext server session reports `alpnProtocol === 'h2c'`, as node's does.** It was
  `null` before; node reads `'h2c'` for a non-encrypted session.

## [0.2.5] - 2026-11-15

### Fixed

- **A server stream nobody read the request of is now auto-closed when the response
  finishes, as node does.** node's `Http2Stream[kMaybeDestroy]` closes such a stream from
  the writable side's `'finish'` — on the wire `RST_STREAM(NO_ERROR)` right after the
  response — while h2lean held it until the peer ended its half, so a peer that sent
  HEADERS without END_STREAM and never any DATA kept a stream (and its `_liveStreams`
  slot) open until the idle timeout. The node predicate is ported (`lib/stream.js`
  `_maybeAutoClose`): it fires only when the response is fully written, no trailer was
  requested or sent, and the request half was never read or flowed — reading the request,
  pausing it or `stream.on('data')` all stop the auto-close, exactly node's `didRead`
  boundary. Measured against node with `tools/probe-stream-teardown.js` across all five
  request shapes; the teardown event order (`'end'` before `'close'` on the unread half)
  matches node's trace as well.

- **A malformed trailer on a live stream is a stream error, as on node.** The refusal of
  the trailer block (a pseudo-header in it, or the missing-END_STREAM shape) now closes
  the stream object — the application sees `ERR_HTTP2_STREAM_ERROR` on `'error'` and then
  `'close'`, and the closed stream suppresses the unread-request auto-close, so the peer
  receives exactly one `RST_STREAM(PROTOCOL_ERROR)` — the shape measured on node
  (`tools/probe-trailer-rst.js`). h2lean previously reset the wire without touching the
  stream object, leaving a zombie stream that then also emitted the auto-close's second
  `RST(NO_ERROR)` behind the refusal.

## [0.2.4] - 2026-11-13

### Fixed

- **The GOAWAY for a refused header block names the block's stream as its last-stream id, as
  node's does.** nghttp2 opens the stream when the HEADERS frame is accepted — after the
  length, padding and priority checks, before the block is decoded — so when the block is
  refused (an undecodable one, a header list over `maxHeaderListSize`), node's GOAWAY names
  that stream id while h2lean named only the streams a stream object was made for — 0 here.
  The last-stream id is the retry boundary a client reads: the lower id told the peer it may
  resend a request that was already acted on. Control frames refused above the line and frames
  refused before the acceptance point (over `maxFrameSize`, malformed padding) do not move the
  id — measured on both stacks, the boundary is exact.

## [0.2.3] — 2026-11-13

### Fixed

- **A second `respond()` no longer emits `'error'` on the stream.** Node's `respond()` only
  throws `ERR_HTTP2_HEADERS_SENT` ("Response has already been initiated.") when the HEADERS
  went out already — internal/http2/core.js has no emission — while h2lean also emitted
  `'error'` on the stream, which on a stream without an `'error'` listener turned the
  application's own (catchable) throw into an uncaught exception node never produces. The
  emit is gone and the message text matches node's.

## [0.2.2] — 2026-11-13

### Added

- **The outbound half of the strict-single-value rule.** Node throws `ERR_HTTP2_HEADER_SINGLE_VALUE`
  when the application hands it a repeated single-value field or a multi-element array on one —
  in the object and the flat-array form, on `request()` (client) and on `respond()`/`trailers`
  (server), for regular and pseudo-headers alike — and h2lean used to encode both copies, or
  spread the array, onto the wire. The check now runs before any octet exists, and the new
  `strictSingleValueFields` option (default `true`, validated as a boolean at
  `createServer`/`connect` time like node's `validateSessionOptions`) turns the rule off. The
  message text matches node's byte for byte; nested-pair input still raises the same `TypeError`
  node raises. Pinned by a differential test on both stacks, strict and opt-out.

### Fixed

- **A repeated single-value field keeps the first copy, as on node.** When a peer put two
  copies of one of node's ~40 single-value fields (`authorization`, `content-type`, `date`,
  `user-agent`, …) on the wire, h2lean joined the values with ", " — so the application read
  a value no downstream HTTP/1.1 translator agrees with, the CVE-2020-8287 shape on the
  reading side. Node's `toHeaderObject` keeps the first copy and ignores the rest
  (`kSingleValueFields`, internal/http2/util.js), and h2lean now does the same, on the
  server (request headers) and the client (response headers) alike. `cookie` still joins
  with `"; "` and `set-cookie` still accumulates into one array — both measured to sit
  outside node's set. Pinned by a differential test that sends the duplicates raw and
  compares what each stack hands the handler.

## [0.2.1] — 2026-11-13

### Fixed

- With `allowHTTP1` live, a **`removeListener` now takes the handler off the HTTP/1 side
  too**. Node serves both protocols from one listener list, so a handler the application
  removes stops seeing requests of either kind; the 0.2.0 mirror was one-way and the
  removed handler kept firing for HTTP/1.1 connections (double execution if a replacement
  handler had been added). A `once` listener now fires at most once across both protocols,
  the way node's single emitter does. `removeAllListeners` remains unmirrored — it fires
  no per-listener hook — recorded in the `lib/server.js` header comment.

## [0.2.0] — 2026-11-13

### Added

- **`createSecureServer({ allowHTTP1: true })`** — the HTTP/1.1 fallback: the ALPN list
  becomes `['h2', 'http/1.1']`, a connection negotiating `http/1.1` (or offering no ALPN at
  all) is served by an embedded `node:http.Server` configured from **`http1Options`**
  (the option bag node passes to its HTTP/1 server, with the deprecated top-level
  `Http1IncomingMessage` / `Http1ServerResponse` names migrating into it the way node's
  DEP0202 does), and one `request` handler — or `upgrade`, `checkContinue`, `connect`,
  `clientError` listener — receives both kinds of request. With the fallback off, a
  no-ALPN connection now gets node's `unknownProtocol` event, or the 403 text node sends
  when nobody listens, instead of a silent destroy; the socket is bounded by
  `unknownProtocolTimeout` (default 10000). Probed off a live node v26.12.1 server and
  `internal/http2/core.js` in one session; node serves the fallback from the same emitter
  via a private mixin h2lean cannot reach, so the one-listener behaviour is carried by a
  `newListener` mirror — a `removeListener` does not unmirror (see the `lib/server.js`
  header comment).

### Changed

- A TLS connection that offers **no ALPN is no longer accepted as an HTTP/2 session**.
  Node's `connectionListener` routes it to the HTTP/1 fallback / `unknownProtocol` path —
  measured: a no-ALPN client speaking HTTP/1.1 receives node's `HTTP/1.0 403 Forbidden`
  text, never a session — and h2lean's old rule (it attached a session, documented as
  "h2 prior knowledge over TLS") was written down against that measurement in error. Pinned
  by `test/allow-http1.test.js` on both stacks.

## [0.1.1] — 2026-11-11

### Fixed

- The 0.1.0 tarball was cut before the `h2lean` name had reached the shipped code: the
  not-implemented error carried `err.code === 'ERR_H2LITE_NOT_IMPLEMENTED'` and the
  published `repository.url` still pointed at the previous repository. Both are
  `h2lean` now, so a handler that switches on the code has to match the new string
  (`lib/client.js`, `lib/server.js`).

## [0.1.0] — 2026-11-09

First public release.

### Added

- **Server**: `createServer` (h2c) and `createSecureServer` (h2s over `node:tls`,
  with node's ALPN rules — the offered list is forced to `['h2']`), both serving the
  stream API (`'stream'`, `Http2Stream`) and the compat (`req, res`) API.
- **Client**: `connect()` over both transports — h2c over `net`, TLS over `tls.connect`
  with the same ALPN handling node's client has, the preface deferred to
  `secureConnect`, certificate validation delegated to `node:tls`.
- **The rest of the `node:http2` surface**: settings pack/unpack (`HEADER_TABLE_SIZE`
  and the other RFC 9113 settings included), all of the module's constants re-exported
  from `node:http2`, `getDefaultSettings`. Server push and `performServerHandshake` are
  the one deliberate gap: they throw `ERR_H2LEAN_NOT_IMPLEMENTED` rather than answering
  wrong.
- **Zero runtime dependencies.** The library is importable with nothing installed;
  `engines` is node >= 20 (measured against node v26.12.1). The differential suite
  runs green on node 20, 22 and 26: it pins h2lean to the node 24+ verdicts and lets
  the `node:http2` arm answer whichever verdict its bundled nghttp2 produces, because
  the pre-24 bundle reports some protocol failures differently (`INTERNAL_ERROR` where
  node 24+ maps them to `PROTOCOL_ERROR`, a `GOAWAY` code 2 instead of 1).

### Performance

All numbers are paired: h2lean and `node:http2` on the same host, in one session,
ABBA-interleaved, `cpuPerReq` as the metric — re-measurable with
`node bench/showcase/matrix.js` and `node bench/client/matrix.js`
([PERFORMANCE.md](./PERFORMANCE.md) has the full tables, the load shape and the noise).

| workload | versus `node:http2` v26.12.1 |
|---|---|
| server, headers only (stream API) | **~8× less CPU**, ~6.8× more req/s |
| server, 16 kB body | ~2.5× less CPU |
| server, 256 kB body | parity |
| client, headers only | ~3.4× less CPU |
| client, 16 kB body | ~1.7× less CPU |
| client, 256 kB body | parity |

h2lean also outperforms a uWebSockets.js (C++) HTTP/1.1 arm on the header-bound
no-payload case (`bench/showcase/`).

### Compatibility

- Compatibility is **surface level and differential**: names, argument shapes, event
  orders and wire-visible frame sequences (`rstCode`/`goawayCode` included). The suite
  runs the same application script against `node:http2` and against h2lean and compares
  the transcripts (`test/diff-harness.js`).
- Conformance: h2spec 2.6.0 — **146/146**, the same verdict as node's own run
  (`npm run spec`).
- The handful of deliberate, written-down divergences are listed in the header comments
  of `lib/server.js` / `lib/compat.js` and in `CONTRIBUTING.md` §5 — none is silent.

### Security

([SECURITY.md](./SECURITY.md) is the full threat model.)

- **Request smuggling** (translation desync, the CVE-2020-8287 family): field names must
  be lower-case tokens, values may not contain NUL/LF/CR, the five connection-specific
  fields are refused in both directions (RFC 9113 8.2.2, 10.3) — refused at encode/decode
  time, before a single translated line is produced.
- **Unbounded work per frame**: every DoS bound is matched to nghttp2 so that a peer's
  budget against node is its budget against h2lean — SETTINGS entry cap 32
  (CVE-2020-11080), CONTINUATION cap 8 (CVE-2024-28182), rapid-reset budget
  (CVE-2023-44487), outbound staging bounded at 256 KiB, header-list size compared in
  HPACK entry octets (RFC 9113 6.5.2), 1 MiB per HPACK literal. Each bound is pinned by
  a test.
