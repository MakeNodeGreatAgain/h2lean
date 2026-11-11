// connect(): the client front door. Mirrors node's `connect(authority, options,
// listener)` in internal/http2/core.js -- same argument juggling, same
// authority/protocol/port derivation, same `createConnection` override -- so that
// `require('h2lean').connect(url)` is a drop-in for `require('node:http2').connect(url)`.
//
// Both transports are supported: h2c over `net`, and h2s over `tls.connect` the
// way core.js initializeTLSOptions builds it -- the ALPN list is forced to ['h2']
// (an application ALPNProtocols list is ignored, the way node's is; only an
// options.ALPNCallback keeps it), and servername is defaulted to the host when
// the host is not an IP. The session itself stays transport-agnostic; it holds
// its handshake back until the socket reports connected (client-session.js
// _start, deferred to 'secureConnect' under TLS the way core.js defers
// setupHandle), and certificate validation is delegated to node:tls -- no
// validation logic lives here.
'use strict';

const net = require('node:net');
const tls = require('node:tls');
const { ClientHttp2Session } = require('./client-session');

function notImplemented(what) {
  const err = new Error(`h2lean: not implemented: ${what}`);
  err.code = 'ERR_H2LEAN_NOT_IMPLEMENTED';
  return err;
}

function connect(authority, options, listener) {
  if (typeof options === 'function') {
    listener = options; options = undefined;
  }
  options = { ...options };

  if (typeof authority === 'string') {
    authority = new URL(authority);
  }
  if (authority === null || typeof authority !== 'object') {
    throw new TypeError('The "authority" argument must be of type string, an object, or a URL');
  }

  const protocol = authority.protocol || options.protocol || 'https:';
  const port = '' + (authority.port !== ''
    ? authority.port : (authority.protocol === 'http:' ? 80 : 443));
  let host = 'localhost';
  if (authority.hostname) {
    host = authority.hostname;
    if (host[0] === '[') {
      host = host.slice(1, -1);
    }
  } else if (authority.host) {
    host = authority.host;
  }

  let socket;
  if (typeof options.createConnection === 'function') {
    socket = options.createConnection(authority, options);
  } else if (protocol === 'http:') {
    socket = net.connect({ port: Number(port), host, ...options });
  } else if (protocol === 'https:') {
    // core.js initializeTLSOptions: the ALPN list belongs to the protocol, not
    // to the application -- only an ALPNCallback keeps ours out. servername is
    // defaulted to the host for the SNI extension when the host is a name
    // (net.isIP host means no SNI, the same default tls.connect applies).
    const tlsOptions = { ...options };
    if (!tlsOptions.ALPNCallback) {
      tlsOptions.ALPNProtocols = ['h2'];
    }
    if (!tlsOptions.servername && !net.isIP(host)) {
      tlsOptions.servername = host;
    }
    socket = tls.connect({ port: Number(port), host, ...tlsOptions });
  } else {
    throw notImplemented(`connect over ${protocol}`);
  }

  const scheme = protocol === 'https:' ? 'https' : 'http';
  const authorityHeader = `${host}:${port}`;
  const session = new ClientHttp2Session(socket, options, scheme, authorityHeader);

  if (typeof listener === 'function') {
    session.once('connect', listener);
  }
  return session;
}

module.exports = { connect };
