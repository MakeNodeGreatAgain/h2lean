'use strict';
// Cost of the request() header-marshalling pass: the _addField loop, the
// strict-single-value duplicate check, and the stream object the call must
// always build. The session is not started, so request() parks the request
// and the measured region is exactly the work the call does before any wire
// work -- the part a validation scheme adds to.
//   node bench/micro/req-singles.js
const { ClientHttp2Session } = require('../../lib/client-session');

const REQ_HEADERS = {
  ':method': 'GET',
  ':path': '/abc/def/ghij?x=1',
  ':scheme': 'http',
  ':authority': 'localhost',
  accept: '*/*',
  'user-agent': 'probe/1.0',
  authorization: 'Bearer 0123456789abcdef',
  'content-type': 'application/json'
};

const stubSocket = {
  // connecting: the constructor defers _start() to a 'connect' that never fires
  // here, so request() parks the request -- the measured region is the
  // marshalling only, with no HPACK encode or socket write in it
  connecting: true,
  on() {}, once() {}, write() {}, end() {}, destroy() {},
  setNoDelay() {}, setKeepAlive() {}
};

function bench(label, fn, n = 500_000) {
  for (let i = 0; i < 50_000; i++) {
    fn(i);
  } // warm up / let V8 tier up
  globalThis.sink = null;
  const t0 = Number(process.hrtime.bigint());
  for (let i = 0; i < n; i++) {
    fn(i);
  }
  const ns = (Number(process.hrtime.bigint()) - t0) / n;
  console.log(`${ns.toFixed(0).padStart(7)} ns/op   ${label}`);
  return ns;
}

const session = new ClientHttp2Session(stubSocket, {}, 'http', 'localhost:80');
bench('request(), strict on (default)', () => {
  globalThis.sink = session.request(REQ_HEADERS);
  // one fixed-length clear per call, identical before and after the change:
  // keeps the park queue from growing under the benchmark
  session._queuedReq.length = 0;
});

const off = new ClientHttp2Session(stubSocket, { strictSingleValueFields: false },
  'http', 'localhost:80');
bench('request(), strict off', () => {
  globalThis.sink = off.request(REQ_HEADERS);
  off._queuedReq.length = 0;
});
