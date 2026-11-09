// h2lite -- a pure-JS HTTP/2 library with a node:http2-shaped API.
//
// Servers: h2c and h2s (prior-knowledge, ALPN 'h2'), the `stream` and compat
// (`request`) APIs. Client: `connect()` + `session.request()` over h2c and h2s
// (see lib/client.js); server push on the client stays notImplemented.
// Deliberately reused from node:http2, because re-typing them would only create
// drift: the 240 protocol constants and the `sensitiveHeaders` symbol. A peer
// must see the same rstCode/goawayCode numbers as with node, and user code
// compares against those names.
'use strict';

const nodeHttp2 = require('node:http2');
const { createServer, createSecureServer, Http2Server, Http2SecureServer,
  Http2ServerRequest, Http2ServerResponse, notImplemented } = require('./lib/server');
const { Http2Session, OUR_DEFAULTS, PEER_DEFAULTS, SETTINGS_BY_ID, ID_BY_SETTING } = require('./lib/session');
const { DATA, HEADERS, PRIORITY, RST_STREAM, SETTINGS, PUSH_PROMISE, PING, GOAWAY,
  WINDOW_UPDATE, CONTINUATION } = require('./lib/frames');
const { Decoder, Encoder, HPackError } = require('./lib/hpack');
const { connect } = require('./lib/client');

// The packed SETTINGS blob node's getPackedSettings produces: id/value pairs.
function getPackedSettings(settings) {
  const names = Object.keys(settings).filter((k) => ID_BY_SETTING[k] !== undefined);
  const body = Buffer.alloc(names.length * 6);
  let n = 0;
  for (const name of names) {
    const value = typeof settings[name] === 'boolean' ? (settings[name] ? 1 : 0) : settings[name];
    body.writeUInt16BE(ID_BY_SETTING[name], n);
    body.writeUInt32BE(value >>> 0, n + 2);
    n += 6;
  }
  return body;
}

function getUnpackedSettings(buf) {
  const out = {};
  if (!Buffer.isBuffer(buf)) throw new TypeError('a Buffer is required');
  if (buf.length % 6 !== 0) {
    const err = new Error('buffer length must be a multiple of 6');
    err.code = 'ERR_HTTP2_INVALID_PACKED_SETTINGS_LENGTH';
    throw err;
  }
  for (let i = 0; i + 6 <= buf.length; i += 6) {
    const name = SETTINGS_BY_ID[buf.readUInt16BE(i)];
    if (name === undefined) continue;
    out[name] = buf.readUInt32BE(i + 2);
  }
  return out;
}

module.exports = {
  constants: nodeHttp2.constants,
  sensitiveHeaders: nodeHttp2.sensitiveHeaders,
  createServer,
  createSecureServer,
  connect,
  performServerHandshake: () => { throw notImplemented('performServerHandshake'); },
  getDefaultSettings: () => ({ ...OUR_DEFAULTS }),
  getPackedSettings,
  getUnpackedSettings,
  Http2Server,
  Http2SecureServer,
  Http2Session,
  // node exports exactly these two compat classes at the top level
  Http2ServerRequest,
  Http2ServerResponse,
  // internal pieces, exported because tests and benchmarks reach for them
  HPack: { Decoder, Encoder, HPackError },
  FrameType: { DATA, HEADERS, PRIORITY, RST_STREAM, SETTINGS, PUSH_PROMISE, PING,
    GOAWAY, WINDOW_UPDATE, CONTINUATION },
  PeerDefaults: PEER_DEFAULTS,
};
