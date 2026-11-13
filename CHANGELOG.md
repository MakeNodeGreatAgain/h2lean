# Changelog — h2lean

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com); entries are reverse chronological.

## [0.2.0] — 2026-11-13

### Added

- **`createSecureServer({ allowHTTP1: true })`** — the HTTP/1.1 fallback: the ALPN list
  becomes `['h2', 'http/1.1']`, a connection negotiating `http/1.1` (or offering no ALPN at
  all) is served by an embedded `node:http.Server` configured from **`http1Options`**
  (the option bag node passes to its HTTP/1 server, with the deprecated top-level
  `Http1IncomingMessage` / `Http1ServerResponse` names migrating into it the way node's
  DEP0201 does), and one `request` handler — or `upgrade`, `checkContinue`, `connect`,
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
