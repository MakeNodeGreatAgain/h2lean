// Server front door, both transports: h2c on a net.Server and h2s on a tls.Server.
// This file owns the listening socket, one Http2Session per connection, and the
// decision of whether the compat (req, res) layer is instantiated at all.
//
// Connection-lifetime surface, measured against node 26 rather than against its
// documentation:
//   * node's Http2Server has close, setTimeout and updateSettings -- and NOTHING else: no
//     sessionTimeout, no closeIdleSessions, no sessions getter, no settings property. node keeps
//     the settings updateSettings merges in a private options object; h2lite keeps them in the
//     public `settings` field it also builds sessions from, which is a third name node does not
//     have. The extras implemented here are additive, not substitutions:
//       server.sessions          -- the live session set, kept because h2lite tracks sessions
//                                   in it and node keeps the same set under a symbol;
//       server.closeIdleSessions() -- documented by node for years and gone in 26; the
//                                   keep-alive policy it served is session.setTimeout() plus
//                                   the application destroying the session, which both stacks
//                                   do implement. Harmless to keep, surprising to drop.
//   * what this server advertises on the wire matches node: the first SETTINGS frame carries only
//     the values the application asked for -- an EMPTY payload when it asked for nothing -- and a
//     later server.updateSettings() reaches the NEXT connection, not the live sessions. The local
//     defaults still guard the inbound path; they are simply not advertised, because an advertised
//     value is a promise to the peer rather than a local choice. Measured with
//     bench/micro/probe-settings-frame.js: both stacks send an empty frame, then "3=100,5=32768" for
//     settings {maxConcurrentStreams: 100, maxFrameSize: 32768}, then "3=100,4=65536" for the
//     connection that follows server.updateSettings({initialWindowSize: 65536}).
//   * closing a connection is not the same MOMENT on the two stacks, and one event depends on it.
//     node's session survives close() until its graceful close completes (internal/http2/core.js:
//     handle.ongracefulclosecomplete -> kMaybeDestroy), so it can still read -- and report -- the
//     peer's answer to the GOAWAY we just sent. h2lite destroys the session as soon as that GOAWAY
//     is written (lib/session.js: close() -> _maybeFinishClose), but its socket stays readable
//     until the write is flushed, so the answer is read there too. The two therefore disagree
//     about ONE event -- the 'goaway' the peer's answer produces -- and only inside a window:
//     measured with tools/probe-goaway-echo.js, node's outcome moves with the load (40 of 40
//     rounds on an idle machine, 26 of 40 and 29 of 30 in two loaded runs) while h2lite reports
//     the echo in every round of every run. test/event-order.test.js drops the echo outside its
//     'goaway' scenario for that reason.
'use strict';

const net = require('node:net');
const tls = require('node:tls');
const { Http2Session, OUR_DEFAULTS } = require('./session');
const { onServerStream, Http2ServerRequest, Http2ServerResponse } = require('./compat');

const kCompat = Symbol('h2lite.compatListener');
// The transport's own close/on, captured before the shared API overrides them:
// net.Server and tls.Server are unrelated base classes and JS has only single
// inheritance, so the shared behaviour is mixed onto the prototype the same way
// node's own core does it, with the overridden methods kept reachable by symbol.
const kTransportClose = Symbol('h2lite.transportClose');
const kTransportOn = Symbol('h2lite.transportOn');

function notImplemented(what) {
  const err = new Error(`h2lite v1 does not implement ${what}`);
  err.code = 'ERR_H2LITE_NOT_IMPLEMENTED';
  return err;
}

function initHttp2Server(server, options) {
  const opts = options || {};
  server.settings = { ...OUR_DEFAULTS, ...opts.settings };
  server._sessionOptions = {
    settings: server.settings,
    // Which values the application asked for. server.settings is what the session's guards read,
    // this list is what the peer is told, and the two are deliberately not the same object: node
    // advertises only what was set (created with no options, its SETTINGS frame is empty), and an
    // advertised number decides how hard peers drive the connection. See Http2Session._advertised.
    advertised: Object.keys(opts.settings || {}),
    // There is no copy to opt out of: a DATA payload reaches the application
    // either as a view of the buffer node read off the socket or as a buffer
    // built for that frame alone, and both stay readable for as long as the
    // application holds them.
    shapeCache: opts.shapeCache !== false,
    // Opt-in: req.headers is frozen and one object per distinct header block.
    // Off by default, because node lets the application write into headers.
    shareHeaders: opts.shareHeaders === true
  };
  server._sessions = new Set();
  server[kCompat] = null;
}

// Turns an accepted socket -- plain or already decrypted -- into a session.
function attachSession(server, socket) {
  socket.setNoDelay(true);
  const session = new Http2Session(socket, server._sessionOptions);
  session.server = server;
  server._sessions.add(session);
  session.on('stream', (stream, headers, options, rawHeaders) =>
    server.emit('stream', stream, headers, options, rawHeaders));
  session.on('error', (err) => {
    if (server.listenerCount('sessionError') > 0) {
      server.emit('sessionError', err);
    }
  });
  session.once('close', () => {
    server._sessions.delete(session);
  });
  server.emit('session', session);
}

function applyHttp2ServerApi(proto) {
  proto[kTransportClose] = proto.close;
  proto[kTransportOn] = proto.on;

  // node instantiates the compat req/res only when the application asks for
  // them: a plain createServer() with a 'stream' handler never pays for two
  // objects per request. Adding the first 'request' listener arms it.
  proto._setupCompat = function () {
    if (this[kCompat] !== null) {
      return this;
    }
    this[kCompat] = onServerStream.bind(this);
    this._sessionOptions.compat = true;
    for (const session of this._sessions) {
      session._wantRaw = true;
    }
    return proto[kTransportOn].call(this, 'stream', this[kCompat]);
  };

  proto.on = function (event, listener) {
    if (event === 'request') {
      this._setupCompat();
    }
    return proto[kTransportOn].call(this, event, listener);
  };

  proto.addListener = function (event, listener) {
    return this.on(event, listener);
  };

  proto.close = function (cb) {
    // node's Http2Server.close() is `ReflectApply(NETServer.prototype.close, this, arguments)`
    // followed by closeAllSessions(this) (internal/http2/core): the listening socket goes away
    // right away and its callback is net.Server's own. Waiting for the session set to empty first
    // was ours, and it made close(cb) a promise that a session which cannot drain never keeps --
    // 'close' then depended on every connection finishing.
    proto[kTransportClose].call(this, cb);
    for (const session of [...this._sessions]) {
      session.close();
    }
    return this;
  };

  proto.closeIdleSessions = function () {
    for (const session of this._sessions) {
      if (session._liveStreams === 0) {
        session.close();
      }
    }
    return this;
  };

  Object.defineProperty(proto, 'sessions', {
    get() {
      return this._sessions;
    },
    configurable: true,
    enumerable: false
  });

  // node's own Http2Server.setTimeout (internal/http2/core.js:3541) only records the value --
  // `this.timeout = msecs`, the same field net.Server uses -- and attaches the callback. It
  // does NOT walk the sessions. Measured against node with an idle connection and
  // server.setTimeout(300): no 'timeout' on the session, no close, the connection simply
  // stays. Arming a session idle timeout for every live session (what this used to do) both
  // invented an event node does not emit and applied to connections that already existed,
  // silently skipping the ones accepted later. The per-connection idle policy is
  // session.setTimeout(), which is implemented on the session.
  proto.setTimeout = function (msecs, cb) {
    this.timeout = msecs;
    if (cb !== undefined) {
      if (typeof cb !== 'function') {
        throw new TypeError('cb must be a function');
      }
      this.on('timeout', cb);
    }
    return this;
  };

  // node's Http2Server.updateSettings() (internal/http2/core.js, next to its setTimeout) only
  // merges into the server's own settings object -- it does not walk the live sessions. That
  // is exactly what the wire shows: after updateSettings({maxConcurrentStreams: 7}) the next
  // connection is sent SETTINGS(3)=7 while an established session keeps the values it was
  // opened with. `_sessionOptions.settings` is the object sessions are built from, so pointing
  // it at the merged copy gives the same reach: new connections, no effect on old ones.
  proto.updateSettings = function (settings) {
    if (settings === null || typeof settings !== 'object') {
      throw new TypeError('settings must be an object');
    }
    this.settings = { ...this.settings, ...settings };
    this._sessionOptions.settings = this.settings;
    // the values merged here become promises to the next peer, so they join the advertisement;
    // first-seen order is kept, and it is the order the SETTINGS entries go out in
    this._sessionOptions.advertised = [...new Set(
      [...this._sessionOptions.advertised, ...Object.keys(settings)])];
    return this;
  };
}

class Http2Server extends net.Server {
  constructor(options) {
    super();
    initHttp2Server(this, options);
    this.on('connection', (socket) => attachSession(this, socket));
  }
}

// node's h2s server insists on h2 and overwrites whatever ALPNProtocols the
// application passed: a peer offering only http/1.1 is refused by the handshake
// itself, with the TLS alert no_application_protocol, rather than after it. The
// list is therefore fixed here too, and the ALPN check in secureConnection below
// stays as the guard for contexts installed later through setSecureContext().
function alpnOptions(options) {
  const opts = { ...(options || {}) };
  opts.ALPNProtocols = ['h2'];
  return opts;
}

class Http2SecureServer extends tls.Server {
  constructor(options) {
    super(alpnOptions(options));
    initHttp2Server(this, options);
    this.on('secureConnection', (socket) => {
      // node's rule: 'h2' negotiated, or no ALPN at all -- which is h2 with prior
      // knowledge carried over a TLS connection. Anything else is dropped without
      // ever becoming a session.
      const alpn = socket.alpnProtocol;
      if (alpn === 'h2' || alpn === false) {
        attachSession(this, socket);
      } else {
        socket.destroy();
      }
    });
  }
}

applyHttp2ServerApi(Http2Server.prototype);
applyHttp2ServerApi(Http2SecureServer.prototype);

function createServer(options, onRequestHandler) {
  let opts = options;
  let handler = onRequestHandler;
  if (typeof opts === 'function') {
    handler = opts; opts = undefined;
  }
  const server = new Http2Server(opts);
  if (typeof handler === 'function') {
    server._setupCompat();
    server.on('request', handler);
  }
  return server;
}

// node's createSecureServer takes the tls.Server options (key, cert, ca, ...) and
// hands the rest to the same machinery as the cleartext server.
function createSecureServer(options, onRequestHandler) {
  let opts = options;
  let handler = onRequestHandler;
  if (typeof opts === 'function') {
    handler = opts; opts = undefined;
  }
  const server = new Http2SecureServer(opts);
  if (typeof handler === 'function') {
    server._setupCompat();
    server.on('request', handler);
  }
  return server;
}

module.exports = {
  Http2Server, Http2SecureServer, createServer, createSecureServer, notImplemented,
  Http2ServerRequest, Http2ServerResponse
};
