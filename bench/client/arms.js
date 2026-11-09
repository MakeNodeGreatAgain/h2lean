// The two client arms, in one table so run.js and matrix.js never disagree about
// which module to load. Unlike the showcase (where every arm was a different
// SERVER driven by h2load), here the server is ONE nghttpd process shared by both
// arms and only the client implementation swaps -- the drop-in promise inverted:
// the same application loop, written twice, once against node:http2 and once
// against h2lite.connect().
//
// `conn` x `streams` is the offered load: each arm keeps conn x streams requests
// in flight, one worker loop per stream slot, identical code in both arms. 8x64
// is the same 512-in-flight shape the showcase server matrix points h2load at;
// it fits under any concurrency cap we set server-side, so the peer's
// SETTINGS_MAX_CONCURRENT_STREAMS never parks a request in either arm.
module.exports = {
  'node client': { impl: 'node:http2', label: 'node:http2 client', conn: 8, streams: 64 },
  'h2lite client': { impl: 'h2lite', label: 'h2lite client', conn: 8, streams: 64 }
};
