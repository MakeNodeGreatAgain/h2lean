// Measurement-only arm: the same server with the inbound request validator switched
// off, so the cost of RFC 9113 8.1/8.2.2/8.3/8.5 checking can be paired against the
// library's real behaviour. It patches the prototype, nothing in lib/ reads this file,
// and it exists only for `PRELOAD=bench/no-validate.js node bench/h2load.js ...`.
//
// What it disables: _requestProblem returns null, so every request is accepted as
// decoded -- which is exactly what h2lean did before the validator existed, the point at
// which node's nghttp2 had been refusing these requests for years.
'use strict';
const { Http2Session } = require('../lib/session');
Http2Session.prototype._requestProblem = function () {
  return null;
};
console.log('validate: OFF (measurement arm)');
