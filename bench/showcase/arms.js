// The six showcase arms, in one table so run.js and matrix.js never disagree
// about which file to spawn or which protocol h2load must speak to it.
//
//   proto 'h1'   -> h2load -p http/1.1  (uWebsockets, node:http terminate HTTP/1.1)
//   proto 'h2c'  -> h2load's cleartext HTTP/2 default (the four HTTP/2 arms)
//
// `inflight` is the number of simultaneous requests the client is pointed at:
// for h2c it is connections x streams, for h1 it is connections (one request per
// keep-alive connection at a time). Both are set to 512 so the offered load is
// the same shape on every arm -- see the methodology note in matrix.js.
module.exports = {
  'node:http':         { file: 'srv-node-h1.js', proto: 'h1', label: 'node:http', conn: 512, streams: 1, inflight: 512 },
  'uwebsockets':       { file: 'srv-uws-h1.js', proto: 'h1', label: 'uWebSockets (http1)', conn: 512, streams: 1, inflight: 512 },
  'node:http2 stream': { file: 'srv-node-h2-strm.js', proto: 'h2c', label: 'node:http2 (stream)', conn: 32, streams: 16, inflight: 512 },
  'node:http2 compat': { file: 'srv-node-h2-compat.js', proto: 'h2c', label: 'node:http2 (compat)', conn: 32, streams: 16, inflight: 512 },
  'h2lean stream':     { file: 'srv-h2lean-strm.js', proto: 'h2c', label: 'h2lean (stream)', conn: 32, streams: 16, inflight: 512 },
  'h2lean compat':     { file: 'srv-h2lean-compat.js', proto: 'h2c', label: 'h2lean (compat)', conn: 32, streams: 16, inflight: 512 }
};
