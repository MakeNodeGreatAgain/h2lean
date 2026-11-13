# OPTIMIZATION.md — the measured performance knowledge of h2lean

One numbered item = one claim. Every claim here was measured in this repository; the
benchmark that produced it is named next to it, and the noise that could bury it is
stated with the number. Code comments in `lib/` may cite an item by its number
("OPTIMIZATION.md, п. 13" is written in code as "OPTIMIZATION.md item 13") instead of
repeating the measurement narrative. When a comment and this file disagree, the comment
carries the local reason and this file carries the number — re-measure before editing
either. New knowledge is appended with the next free number, wherever the sequence
currently ends: code comments cite items by number, so an existing number is never
reassigned and never renumbered.

The rules that decide whether a change is accepted at all ("it is faster" is worthless
unless paired and measured in one session, ABBA ≥3 rounds, `cpuPerReq` as the metric)
live in `AGENTS.md` §7; this file is the result side of that convention, not the process
side.

## A. Where the CPU actually goes

1. **Raw req/s is not a result; `cpuPerReq` (µs/req, measured inside the server
   process) is.** Identical untouched native code moves 8–13% between runs, and a single
   case can move far more (the native 256 KB download arm has swung +32% while the
   small-GET pairs stayed flat). Judge drift per case; if the intra-arm spread exceeds
   the between-arm difference, the result is "inside noise". The full convention, the
   ABBA rule and the `GC=1 bench/matrix.sh` harness: `AGENTS.md` §7.
2. **CPU profiles cannot attribute cost across the JS/C++ boundary.** Diffing an h2lean
   profile against a `node:http2` one compares a JS stack with an invisible nghttp2 core —
   the sampler cannot see the C++ side, so the diff is fiction. Within one implementation,
   `busy > 100%` is the signature of parallel V8 GC threads from allocation churn; a
   profile captured before a structural fix is invalid for anything afterwards
   (`bench/prof-h2.js`, `bench/summarize-profile.js`, `tools/cpuprofile-report.js`).
3. **Measure a component's share before removing its abstraction.** Calibrated
   `process.hrtime.bigint()` wrappers on prototype methods, installed in A/B rounds
   inside one process (subtract the measured no-op wrapper cost, exclude nested timers),
   answer "is this class too heavy" better than any profile: a "heavy-looking" abstraction
   can account for ~2.5% of CPU while the real cost sits in parsing and kernel-read
   machinery the sampler never sees. Never infer syscall counts from a method's call
   count — most calls can be early returns.

## B. Techniques that earned their place

4. **Null-prototype header maps are built with `Object.setPrototypeOf({}, null)`, not
   with `{ __proto__: null }` or `Object.create(null)`.** The latter two are
   dictionary-mode: on the real `_toObject`, 216 ns to build against 148 ns, and 350 ns
   to read back against 199 ns (`bench/micro/toobject-real.js`; the semantic equivalence
   — null prototype, enumeration order, freeze behaviour — is checked by
   `bench/micro/nullproto-semantics.js`). The exception: **a map that stays empty keeps
   the literal** — an empty fast map costs 59 ns to make against the literal's 20 ns
   (`bench/micro/nullproto-alloc.js`), which is why `res._trailers` in `lib/compat.js`
   is a literal and `res._headers` is not. A peer-driven fast map hangs on the
   process-wide transition tree; that risk is bounded and re-checked per `AGENTS.md` §8.
5. **The compat layer's handlers are called, not subscribed.** One dispatch function in
   `lib/stream.js` (`emit()`) instead of EventEmitter subscriptions: ten subscriptions
   cost 264 ns per request in `EventEmitter._addListener` alone; the dispatch costs one
   call per event and runs before the emitter, which is the order the subscriptions had
   (`lib/compat.js` header, `lib/stream.js`).
6. **Cache at the right granularity: the whole encoded block, keyed by object identity.**
   Encoding one field line costs a key string plus a Map lookup (~100 ns each); encoding
   the block once and memcpy-ing it costs ~26 ns per response (`lib/headers.js` header).
   Sound only for `Object.isFrozen()` callers' objects — sealed is not enough — and the
   cache lives on the session because the HPACK dynamic table is per-connection.
7. **A bounded set cache of validated field names beats the token regex.** 7.4 ns
   (`Set.has`) against 21.0 ns per field line (`bench/header-validate.js`); the adversarial
   shape — a new name every call — costs 122.9 ns against 40.9 ns for regex-only, accepted
   as the price of the 1024-entry bound (`VALID_NAMES_MAX`, `AGENTS.md` §8). Rejected
   containers: `Array.includes` 15.2 ns, null-proto object 33.1 ns.
8. **Name normalisation: Map cache over `trim().toLowerCase()`.** 8.98 ns against
   19.5 ns per call (`bench/micro/norm-cache.js`). The insert into a growing Map is the
   expensive part — 195 ns for a name built per request against 17 ns for the
   normalisation alone — which is why the cache is bounded and names are interned.
9. **The `date` header is an invalidate-only cache, not a timer-driven one.** Cached
   string plus a one-shot unref'd timer that only invalidates: 4.4 ns per response against
   35.7 ns for the `Date.now()` comparison, and one 409 ns `toUTCString()` per second
   instead of per millisecond (`bench/header-validate.js`). `setInterval` measured
   identically fast (4.2 ns) and was rejected: it fires on an idle server and goes stale
   under a blocked loop. The shape is node's own internal/http `utcCache`/`resetCache`,
   read from the binary.
10. **A `Set` lookup is already the fastest container at five names — the operation
    shape decides, not the container size.** `Set.has` 9.3 ns against `Array.includes`
    9.7 ns against object 11.8 ns (`bench/header-validate.js`). The earlier "Array beats
    Set at small N" result (`bench/queue-shape.js`) is about appends and indexed reads; a
    Set has no index, so it does not transfer to lookups.
11. **HPACK encoder: insert on the second sighting, no name-based never-index list,
    two-level byte caches, reinsert stop.** All four decided on `bench/hpack-churn.js`,
    not on preference, against nghttp2 `lib/nghttp2_hd.c` v1.70.0 (`lib/hpack.js` header).
    Key numbers: a composite `name\0value` cache key costs 25 ns to build plus 70 ns to
    hash per field line against 7 ns for the two-level lookups
    (`bench/micro/hdr-path.js`); a cold unique line costs ~83 ns in the seen-once check
    (`bench/micro/uniq-variants.js`), which is what `ONCE_PER_NAME = 64` bounds; a working
    set that overflows the table spends 1343 ns encoding one block — 30× the no-table
    cost — which is what `REINSERT_MAX` stops. Insert-on-second-sighting costs one extra
    warm-up response and reaches node's own steady-state 5 octets per request
    (`bench/hpack-churn.js`). Copying nghttp2's name-based never-index list measured
    worse on both axes (item 27).
12. **Uncached literal forms are written straight into the destination buffer.** The
    first sighting of a pair and its insertion were built in their own buffer and copied;
    removing that buffer took `hdr.block()` with a fresh value from 1034 ns to 754 ns
    (−27%), end-to-end 3.08 → 2.69 µs/req (stream-uniq) and 4.62 → 4.42 (compat-uniq),
    wire bytes proven identical (`lib/hpack.js`, `bench/hpack-churn.js`). The same
    reasoning killed the "many small buffers" premise: the encoder allocates 0.00 buffers
    per response in both shapes. It extends to the string forms: `Buffer.from(s, 'latin1')`
    measured 106 ns for 38 characters and allocates a buffer per string, so the encoder
    Huffman-codes straight from the string instead of through a scratch buffer
    (`bench/micro/hdr-path2.js`); and the byte copy into the destination is `dst.set()`,
    not `dst.copy()` — 12.2 ns against 18.8 for the twelve octets of a field line
    (`bench/micro/copy-set.js`).
13. **Decoder: small latin1 literals are cased inline.** ≤ 4-octets literals go through a
    switch instead of `Buffer.toString`: worth ~45 ns each inside the decode loop
    (728 ns faster on the path); the eight-case version measured 6% SLOWER on a real
    block, so the switch stops at four (`lib/hpack.js`). Re-measured on node v26.12.1:
    the raw win itself extends well past four -- switch8 is −60..−65% at 5–8 octets
    (`bench/micro/probe-latin1-concat.js`) -- but widening the switch reproduces the
    old cost on the blocks it does not help (short literals +3..5%, 9–12 +5%). The
    shape that keeps both: the switch stays at four and the default chunks anything
    up to 12 octets into 4+4(+4) fromCharCode halves (`latin1()` in `lib/hpack.js`).
    Block-level ABBA, min of 4 rounds (`bench/micro/probe-latin1-block.js`): 5–8-octet
    values −25%, 9–12 −13%, Huffman-decoded 5–8 −15%, decoded 9–12 −6%, realistic long
    values −11% (stable across sessions, mechanism not attributed), all-short block
    level. The 8-case switch and a loop-chunked variant (up to 12) lose to it on every
    block, so the eight-case verdict stands; the concat of a JS and a native half
    (`latin1(4) + toString`) measured +11..25% everywhere and was rejected.
14. **A HPACK size update reserves its length in the block buffer instead of
    `Buffer.concat`.** The old concat was a second allocation plus a second copy of the
    whole block per request; reserving costs one memcpy of the update: 350 ns against
    486 ns for an eight-field block whose table shrank, 320 ns on the steady-state no-update
    branch (`bench/micro/probe-sizeupdate.js`, `lib/client-session.js`).
15. **Outbound: hybrid staging buffer + reference queue for owned payloads ≥ `BODY_MIN`
    (4 KiB), shipped 1 Nov 2026.** The rule that survived: a payload travels by reference
    only when the session owns the memory (explicit 5th `owned` argument of `_frame()`)
    and it is ≥ `BODY_MIN`; headers and control frames are copied into the per-batch
    staging buffer. The threshold itself is a measured break-even on a 256 KB response:
    below 4 KB the extra chunk in the writev batch costs more than the memcpy, and one
    frame at the default maxFrameSize (16 384) is comfortably above it either way.
    Measured (3 interleaved rounds): the pure reference queue saved 13%
    on a 32 KB download but cost +20% on a 512 B GET (1.05 → 1.25 µs/req) and +6% on TLS
    upload — node's write path charges ~150–200 ns per chunk, and the staging buffer turns
    a batch into ONE chunk (65 chunks → 1 under the benchmark's client shape: 2.63 →
    2.86 µs/req the other way). The +20% GET cost belongs to the rejected pure reference
    queue, not to what shipped: the hybrid keeps a GET batch single-chunk (staged) and
    re-measured equal to pre-item-15 on GET (out/writev-small-concat.js, node v26.12.1:
    2-chunk 64B+520B batch — writev 1122 ns, concat 1065, staged 1026; 17 x 64B —
    writev 2303, concat 1603, staged 1553; corked writev is the per-chunk charge, concat
    pays the same memcpys plus an allocation, staging is cheapest — which is the rule).
    Correctness invariants: a committed buffer is never
    written into again; one batch in flight; `MAX_OUT_BATCH` bounds the queue (it came
    from a measured 16 MB/session blow-up, `bench/outstat.js`).
16. **A single-chunk batch is written without cork.** `cork`/`uncork` adds a round trip
    through the stream's own buffer: +5% `cpuPerReq` on GET, measured twice (server path
    `lib/session.js` `_flush`, same rule in `lib/client-session.js`).
17. **A frame is never an object.** `lib/frames.js` reads/writes the 9-octet header
    in place on a caller-provided buffer and publishes results into module-level slots
    (`FH_LEN`, `FH_TYPE`, …): one allocation avoided per frame, and the read path hands
    out views of the socket chunk (`lib/pieces.js` keeps a fragmented frame as the views
    it arrived in) instead of assembling a copy.
18. **Slice copies use `allocUnsafe(len)` + `copy()`, not `Buffer.from(view)`.**
    `Buffer.from(view)` always allocates a standalone backing store; `allocUnsafe` below
    `poolSize >>> 1` takes the shared 65 600-byte pool. Measured on node v26.12.1, 5e6
    iterations, medians of 3 runs (`bench/micro/subarray-copy-bench.js`): 9 B 92.9 →
    53.8 ns, 36 B 84.6 → 58.4, 180 B 87.4 → 64.4. This is what the last commit of
    §history (HEAD) wired into the PING/GOAWAY payload copies.
19. **Held bytes are tracked by an incremental counter, not by walking `stream._queued`
    at accept time.** A profile of the 1 KB download case put 59% of the whole server's
    CPU inside a per-stream scan (`lib/session.js` `_releaseAll`); scanning every stream
    on each connection-level WINDOW_UPDATE is quadratic under a large response body. The
    same shape holds on the client's credit path: visiting every open stream per pass
    measured 7.4 µs/req on a 256 kB download (`bench/client`, `_creditRecv`), which is
    why `lib/client-session.js` keeps a `_debt` list of the streams that took DATA.
20. **The rapid-reset bucket samples the clock only when the token balance is empty.**
    `Date.now()` measured 32.3 of the 37.8 ns the per-frame branch would cost
    (`bench/rst-cost.js`); deferring is exactly equivalent because the balance only
    decreases between samples. Both arms of that microbench must keep an identical call
    shape — a method via `.call(receiver, …)` versus a standalone helper gets different
    inlining, and that alone can read as a negative cost.
21. **Idle timeouts ride the shared FastTimer clock, not one `setTimeout` per
    connection.** `bench/timers.js` measures the per-timer cost (arm/refresh/clear) and
    the number of timer resources held for N connections; the refresh path matters most
    because `Http2Session` refreshes on every socket read. Caveat: delays ≥ 1000 ms ride
    a 499 ms clock with ±500 ms accuracy — timing tests stay sub-second (`AGENTS.md` §6).
22. **`shareHeaders: true` is an opt-in that hands the application a frozen shared
    headers object for byte-identical request blocks.** Removes the ~200 ns object
    construction per repeating request: −28% CPU on repeating blocks (1.70 → 1.22 µs/req),
    **+7% on varying ones** — production traffic (trace-id per request) sits in the second
    column, so the default is off and the flag is a user decision that breaks node's
    mutability contract (`lib/session.js`).
32. **Flow-control credit is released in half-window steps, not per DATA frame.** An
    eager one-frame-per-DATA cadence on a 256 kB download cost the h2lean client ~45 µs/req
    over node's own — 16 WINDOW_UPDATE pairs per response instead of the ~8 half-window
    steps the bytes actually span; the half-window cadence itself was read off node's
    nghttp2 (`tools/probe-conn-credit.js`; `lib/session.js` `_creditConnection`,
    `lib/client-session.js` `_creditRecv`).
33. **A one-character test reads the code point, not the character.** `key.charCodeAt(0)`
    against `key[0] === ':'`: indexing boxes a fresh one-char string on every call, and
    measured over 5M calls on the pseudo-header line the code-point read is the cheaper
    spelling on both the match case and the fall-through one
    (`bench/micro/probe-micro2.js`, `lib/client-session.js`).
34. **The outbound batch ceiling is 256 KiB, and it is what decides writev economics on
    downloads.** The ceiling bounds `_outLen`, so at 128 KiB a 256 KB response leaves in
    two writev calls with a park/resume round-trip between them (instrumented on the
    down256k matrix shape: 1.55 writev calls per request, the largest batch 128 KiB +
    HEADERS); at 256 KiB it leaves in one (0.96 calls/request, one ~254 KB batch,
    `out/native-writecount.js`). Paired ABBA in one session (`out/batch-ab.sh`, 64
    clients x 8 threads, 8 s, constant flipped between runs and restored), arm ranges
    non-overlapping: 40.15–42.25 µs/req at 128 KiB against 36.22–39.06 at 256 KiB
    (median 40.4 → 37.6, −7%), throughput 23.4–25.5k → 25.9–28.4k req/s; the GET control
    is unchanged (1.10–1.18 µs/req on both ceilings — its batch is single-chunk and
    never reaches the ceiling). The price is the worst case against a peer that stops
    reading: bytes held double, 128 → 256 KiB per session (SECURITY.md table). Against
    node:http2 on the same shape in the same session the 256 KiB arm measured
    35.44–38.21 µs/req against node's 39.63–43.47 — the former 1.12× deficit is gone,
    with node's own drift on this case spanning 33.2–43.5 across sessions.
35. **Short raw literal strings are written from `charCodeAt`, not `Buffer#write`**
    (`latin1Into()` in `lib/hpack.js`, the encode-side mirror of item 13). `Buffer#write`
    is one call into C++ with ~16–19 ns of fixed cost; the element writes are ~1.3 ns
    per code unit, so the loop wins up to ~16 code units on a flat one-byte string
    (−22% at four, −13% at eight, level at sixteen) and loses past it — flat one-byte
    ~1.3 ns/unit, two-byte ~2.6, a rope ~2.1, crossover ~16 for all three, so the
    threshold `RAW_WRITE_MAX = 16` has no regression zone. Measured per representation
    (`bench/micro/probe-str-write.js`) and at the literalInto granularity on the raw
    (non-Huffman) branch, where the win is −13% at eight code units and the two arms
    are byte-identical on the wire
    (`bench/micro/probe-hpack-str-enc.js`, `lib/hpack.js`).
36. **The strict-single-value duplicate check rides `_addField`'s one loop**, against a
    per-session scratch array cleared per request — not a second pass over the flat
    list with a `Set`: 451/453 → 399/407 ns per `request()` on an 8-field object with
    two single-value fields, the check's own cost ~128 → ~76 ns, and the strict-off
    arm unchanged (319/326 vs 323/332 ns — noise; `bench/micro/req-singles.js`).
    node's `buildNgHeaderString` runs the same check inline in its one pass with
    `includes` over a lazily-grown array, so the merged shape is node's shape too,
    and the residual cost is the per-field `SINGLE_VALUE_FIELDS.has()` lookup the
    rule itself requires. The `DEFAULT_METHOD` hoist and the one-use closure removal
    made in the same pass measured nothing — V8 interns string literals and a closure
    is one allocation — they are readability only.

## C. Measured and rejected — do not re-litigate without new evidence

23. **SliderBuffer as an allocation arena, in every path considered.** Node 26
    already bump-allocates everything below 32 KiB from a shared 65 600-byte pool
    (`poolSize = 64 KiB`, cutoff `poolSize >>> 1`, read out of the running binary), so the
    arena's edge exists only at ≥ 16 KiB and vanishes inside the server's noise.
    Data-payload path: end-to-end A/B on the upload matrix was inside the noise (TLS 46.3
    vs 45.4 µs/req arena-behind, h2c 17.3 vs 18.9 arena-ahead, ±10% intra-arm).
    HPACK literals: at 48 B — the size a literal actually is — `getBuffer()` 39.8 ns vs
    `allocUnsafe` 37.3, whole literal 782 ns vs 779; a second micro-shape put them at
    28.9 vs 29.9, i.e. the small-size difference is noise. The right optimisation of that
    allocation was to remove it (item 12), not to pool it. The vendored file and
    `bench/alloc.js` were deleted on 8 Nov 2026 once this conclusion was written down;
    re-measure against node's pool before ever reintroducing an arena.
24. **Code-generated header-object builders** (`new Function` per shape): 229 ns against
    205 ns for the plain key-array loop and 235 ns for today's compare loop
    (`bench/toobject-cost.js`). Object creation sits at V8's floor; micro-variants are
    inside noise.
25. **Cloning a cached headers object** with spread (683 ns) or
    `Object.assign(Object.create(null), cached)` (795 ns): 3–4× slower than building it
    (`bench/toobject-cost.js`). The same verdict holds on the response side: a merged
    copy of `res._headers` per `respond()` cost 145 ns and one allocation and bought
    nothing, because the map is never frozen and the block cache cannot use it
    (`bench/micro/memo-cost.js`, removed from `lib/compat.js`).
26. **`NODE_OPTIONS=--max-semi-space-size=64` as a deployment lever**: 5.43 against
    4.66 µs/req — worse.
27. **Copying nghttp2's name-based never-index list** (`:path`, `etag`, `location`,
    `age`, `content-length`, `if-*`): 4 → 33 octets per block and ~2× the CPU — an ETag
    is per-resource, so indexing it is the whole point (`lib/hpack.js` header,
    `bench/hpack-churn.js`). Only the size half of the rule survives (nothing ≥ ¾ of the
    table is inserted).
28. **A pure outbound write queue in place of the hybrid** — see item 15 for the numbers;
    `Buffer.concat` before one write is the same memcpy plus an allocation, and `Set` as
    the queue container is 4–6× slower than `Array` for indexed reads
    (`bench/queue-shape.js`).
29. **A shared pool of outbound staging buffers.** The staging buffer is the only buffer
    a request still allocates (1 031 of the 1 050 octets; reuse takes the census from 2.05
    to 1.04 allocations/request, `tools/probe-alloc-count.js`), but the CPU does not move:
    −0.5% / −1.8% / +7.7% of `cpuPerReq` over three ABBA rounds — a spread wider than the
    effect — and the server's whole GC budget is 1.0–1.4% of its CPU (`bench/reuse-stage.js`).
30. **Bypassing the Node `Duplex` delivery on the client** with a direct `emit('data')`:
    objectively tested and failed — the terminal `'end'` is lost (`readableEnded` stays
    false), the benchmark shows up as n=0/Infinity rather than an assertion failure. The
    measured share did not justify the fight anyway: `push()` is ~4.4 µs/req = 2.5% of
    client CPU at 256 kB against a ~40 µs/req gap versus node. Client-side
    `WINDOW_UPDATE` batching into one `socket.write()` per read cycle was reverted after
    instrumentation recorded 0 executions of the branch under load — unexercised code
    paths rot silently and mislead later attribution.
31. **`Buffer.from(string)` vs `allocUnsafe + write` as an h2lean change.** External
    advice measured `Buffer.from` faster (54.0 vs 73.4 ns at 8 B; 384.2 vs 642.3 at 4 KB,
    `bench/micro/attack-costs.js`) — and it does not apply: the HPACK encoder writes
    strings straight into the caller's destination buffer (`dst.write(s, off, 'latin1')`),
    so adopting either intermediate form would be a measured regression. Check whether the
    code already writes into a destination before importing an allocator tip.
