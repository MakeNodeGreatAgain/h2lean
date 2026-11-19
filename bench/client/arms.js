// The client arms, in one table so run.js and matrix.js never disagree about
// which module to load. Unlike the showcase (where every arm was a different
// SERVER driven by h2load), here the server is ONE nghttpd process shared by all
// arms and only the client implementation swaps -- the drop-in promise inverted:
// the same application loop, written once per arm, against node:http2, against
// h2lean.connect() and against @passcod/faith's fetch().
//
// WHY faith is here: its README ("Is it faster?") claims it beats node:http2.
// This matrix measures the opposite -- faith costs 3-8x more CPU per request --
// and two probes explain why both results are honest:
//   * tools/probe-faith-tls.js -- the gap is NOT the TLS asymmetry below: with
//     all three arms on the SAME TLS+ALPN nghttpd, node/h2lean pay ~11-14 us/req
//     for TLS at 16 KiB while faith stays ~4-5x above them (119.6 vs 30.3/25.5).
//   * tools/probe-faith-shape.js -- faith's own bench (passcod/faith bench/) is
//     a different measurement: the h2 server is node:http2 IN THE SAME PROCESS,
//     and the verdict is wall-clock rps, not CPU. Replicated here, faith DOES
//     win that metric on 64 KiB bodies (14.2k vs 7.1k rps at c64) because its
//     Rust threads move TLS/HPACK/framing off the shared event loop -- while
//     still paying more total CPU, and losing even the wall-clock race at 1 KiB.
//     Against a separate nghttpd the off-loop offload buys nothing.
//
// `conn` x `streams` is the offered load: each arm keeps conn x streams requests
// in flight, one worker loop per stream slot, identical code in every arm. 8x64
// is the same 512-in-flight shape the showcase server matrix points h2load at;
// it fits under any concurrency cap we set server-side, so the peer's
// SETTINGS_MAX_CONCURRENT_STREAMS never parks a request in any arm.
//
// The faith arm is NOT shape-identical to the other two, and the differences are
// the arm's own, not the harness's (tools/probe-faith.js pins them):
//   * kind: 'fetch' -- faith is a fetch API over Rust reqwest/hyper, not a
//     node:http2 surface; run.js drives it with one Agent per connection group
//     and a body-read loop instead of sess.request() + 'data'/'end'.
//   * tls: true -- faith 1.1.0 has no h2c prior knowledge (a plaintext fetch
//     sends HTTP/1.1 and dies on nghttpd's HTTP/2 reply), so its nghttpd runs
//     with TLS+ALPN. Its cpuPerReq therefore includes TLS record protection the
//     h2c arms never pay; process.cpuUsage() is process-wide, so the Rust
//     network threads are counted too.
//   * connections are the pool's decision: a warm Agent multiplexes its slots
//     onto one h2 connection (measured via agent.connections()), so 8 agents
//     approximate the 8-connection shape without pinning sockets.
module.exports = {
  'node client': { impl: 'node:http2', label: 'node:http2 client', conn: 8, streams: 64 },
  'h2lean client': { impl: 'h2lean', label: 'h2lean client', conn: 8, streams: 64 },
  'faith client': {
    impl: '@passcod/faith', label: 'faith (Rust fetch) client',
    conn: 8, streams: 64, kind: 'fetch', tls: true
  }
};
