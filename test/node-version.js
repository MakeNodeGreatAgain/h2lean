'use strict';
// The node major the suite runs under. The differential suite pins node v26.12.1
// (AGENTS.md 1); a handful of verdicts changed between the engines floor (20) and 26,
// each measured off that binary's bundled internal/http2 source or a live probe in the
// same session. These gates let the suite run green across 20..26 without bending h2lean
// to an old verdict: where h2lean deliberately follows 26 and an older node differs, the
// case is skipped or the node-side expectation is gated; h2lean's own expectation never
// moves. The boundary for each was read off node 20.19.5, 22.21.1, 24.22.3 and 26.12.1.
const NODE_MAJOR = Number(process.versions.node.split('.')[0]);

// request() defers the session-state failure to the returned stream from 24 on
// (requestError -> process.nextTick(requestOnError)); 20/22 throw ERR_HTTP2_INVALID_SESSION
// / ERR_HTTP2_GOAWAY_SESSION synchronously. Argument validation stays synchronous on all.
const DEFERRED_REQUEST_ERROR = NODE_MAJOR >= 24;
// maxOriginSetSize / kMaxOriginSetSize / ERR_HTTP2_TOO_MANY_ORIGINS exist from 24 on;
// before that onOrigin adds unconditionally and connect() silently ignores the option.
const ORIGIN_SET_CAP = NODE_MAJOR >= 24;
// addAbortListener registers an enumerable 'abort' listener from 24 on (listenerCount 1);
// 20/22 register none enumerable (count 0). The abort itself fires identically on all.
const ABORT_LISTENER_VISIBLE = NODE_MAJOR >= 24;
// The session socket proxy routes destroy beside setTimeout/ref/unref from 24 on;
// 20/22 list destroy among the throwing properties.
const SOCKET_DESTROY_ROUTED = NODE_MAJOR >= 24;
// The settings({initialWindowSize}) ceiling is kMaxInitialWindowSize (2^31-1) from 24 on;
// 20/22 allow up to kMaxInt (2^32-1) in JS, then nghttp2 rejects 2^31 and
// Http2Settings::Send() CHECKs the submit result, aborting the process.
const INITIAL_WINDOW_CEILING_2_31 = NODE_MAJOR >= 24;
// A non-zero inbound GOAWAY that destroys a multi-stream session resets none of them from
// 24 on; 20/22 submit RST_STREAM for the earlier open streams (measured: two open streams
// -> RST(1,2) before the GOAWAY echo). The single-stream case agrees on every version.
const NO_RST_ON_MULTI_DESTROY = NODE_MAJOR >= 24;
// The server stream writableHighWaterMark is 65536 from 22 on; 16384 on 20.
const STREAM_HWM_64K = NODE_MAJOR >= 22;

module.exports = {
  NODE_MAJOR,
  DEFERRED_REQUEST_ERROR,
  ORIGIN_SET_CAP,
  ABORT_LISTENER_VISIBLE,
  SOCKET_DESTROY_ROUTED,
  INITIAL_WINDOW_CEILING_2_31,
  NO_RST_ON_MULTI_DESTROY,
  STREAM_HWM_64K
};
