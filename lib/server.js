// Server front door, both transports: h2c on a net.Server and h2s on a tls.Server.
// This file owns the listening socket, one Http2Session per connection, and the
// decision of whether the compat (req, res) layer is instantiated at all.
//
// Connection-lifetime surface, measured against node 26 rather than against its
// documentation:
//   * node's Http2Server has close, setTimeout and updateSettings -- and NOTHING else: no
//     sessionTimeout, no closeIdleSessions, no sessions getter, no settings property. node keeps
//     the settings updateSettings merges in a private options object; h2lean keeps them in the
//     public `settings` field it also builds sessions from, which is a third name node does not
//     have. The extras implemented here are additive, not substitutions:
//       server.sessions          -- the live session set, kept because h2lean tracks sessions
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
//     peer's answer to the GOAWAY we just sent. h2lean destroys the session as soon as that GOAWAY
//     is written (lib/session.js: close() -> _maybeFinishClose), but its socket stays readable
//     until the write is flushed, so the answer is read there too. The two therefore disagree
//     about ONE event -- the 'goaway' the peer's answer produces -- and only inside a window:
//     measured with tools/probe-goaway-echo.js, node's outcome moves with the load (40 of 40
//     rounds on an idle machine, 26 of 40 and 29 of 30 in two loaded runs) while h2lean reports
//     the echo in every round of every run. test/event-order.test.js drops the echo outside its
//     'goaway' scenario for that reason.
//   * allowHTTP1 + http1Options (both createSecureServer options, per node's docs): when the
//     fallback is live, node serves HTTP/1.1 from the SAME object -- _http_server's private
//     storeHTTPOptions() writes the HTTP/1 options into private fields of the Http2SecureServer
//     and http's _connectionListener is bound to it -- so one emitter and one listener list
//     carry both protocols. Those symbols are not reachable from node:http, so the fallback
//     here is an embedded http.Server that never listens, fed the sockets the ALPN verdict
//     hands it, with a newListener mirror (node's own setupCompat hook) carrying the
//     application's request/upgrade/clientError/... listeners onto it. Three written-down
//     consequences of that model: a removeListener does not unmirror (node has no such seam);
//     the embedded server's own idle-connection bookkeeping never starts, because its
//     'listening' never fires, so the handed-off sockets are tracked here and close() shuts
//     the idle keep-alive ones down while letting a response in flight finish (node waits
//     through net.Server's own bookkeeping); and an unknownProtocol socket -- no ALPN, no
//     allowHTTP1 -- is answered with node's 403 text instead of being destroyed silently,
//     which is what node does (core.js connectionListener) and what this file used to claim
//     wrong: no ALPN is not h2 prior knowledge, and it never became a session on node.
'use strict';

const net = require('node:net');
const tls = require('node:tls');
const { Http2Session, OUR_DEFAULTS } = require('./session');
const { onServerStream, Http2ServerRequest, Http2ServerResponse } = require('./compat');

const kCompat = Symbol('h2lean.compatListener');
// The transport's own close/on, captured before the shared API overrides them:
// net.Server and tls.Server are unrelated base classes and JS has only single
// inheritance, so the shared behaviour is mixed onto the prototype the same way
// node's own core does it, with the overridden methods kept reachable by symbol.
const kTransportClose = Symbol('h2lean.transportClose');
const kTransportOn = Symbol('h2lean.transportOn');

function notImplemented(what) {
  const err = new Error(`h2lean v1 does not implement ${what}`);
  err.code = 'ERR_H2LEAN_NOT_IMPLEMENTED';
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
  // node validates this as uint32 and defaults it to 10000 (initializeOptions, core.js);
  // it bounds how long an unknown-protocol socket may sit after the 403 text.
  if (opts.unknownProtocolTimeout !== undefined &&
      (!Number.isInteger(opts.unknownProtocolTimeout) || opts.unknownProtocolTimeout < 0)) {
    const err = new RangeError(`Invalid unknownProtocolTimeout: ${opts.unknownProtocolTimeout}`);
    err.code = 'ERR_OUT_OF_RANGE';
    throw err;
  }
  server._sessionOptions.unknownProtocolTimeout = opts.unknownProtocolTimeout ?? 10000;
  // allowHTTP1 has nothing to negotiate over a plain socket (no ALPN, so the branch
  // that would consume it is unreachable -- the same reason node's cleartext server
  // ignores it), and http1Options is accepted here only so that the option bag a
  // caller builds for node keeps working verbatim. The live HTTP/1 machinery is
  // built, when allowHTTP1 is set, in initHttp1Fallback below.
  server._http1 = null;
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
    // 'close' then depended on every connection finishing. When the HTTP/1 fallback is live,
    // node also runs httpServerPreClose() (closeIdleConnections + the checker interval); the
    // public-API equivalent here shuts the idle keep-alive sockets down and lets the active
    // responses finish, see closeHttp1().
    if (this._http1) {
      closeHttp1(this);
    }
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

// node's h2s server fixes the ALPN list the way initializeTLSOptions (core.js) does:
// ['h2'] -- with 'http/1.1' added when allowHTTP1 asks for the fallback -- and an
// options.ALPNCallback keeps the application's list out of reach. A peer offering only
// http/1.1 without allowHTTP1 is therefore still refused by the handshake itself, with
// the TLS alert no_application_protocol, rather than after it. The ALPN check in
// secureConnection below stays as the guard for contexts installed later through
// setSecureContext().
function alpnOptions(options) {
  const opts = { ...(options || {}) };
  if (!opts.ALPNCallback) {
    opts.ALPNProtocols = ['h2'];
    if (opts.allowHTTP1 === true) {
      opts.ALPNProtocols.push('http/1.1');
    }
  }
  return opts;
}

// The events an HTTP/1 connection produces that an application registers on the h2
// server. node serves both protocols from ONE emitter (http's _connectionListener is
// bound to the Http2SecureServer itself), so a listener sees both kinds of request;
// the mirror below is what makes one 'request' handler keep working here, where the
// fallback lives on an embedded server instead.
const HTTP1_MIRROR = new Set([
  'request', 'checkContinue', 'checkExpectation', 'upgrade', 'connect', 'clientError'
]);

// node's fallback is a mixin: _http_server's private storeHTTPOptions() writes the
// HTTP/1 options into private fields of the Http2SecureServer itself, so there is no
// second server object and no second listener list. Those symbols are not reachable
// from node:http, so the fallback here is a real http.Server that never listens and
// receives the sockets the ALPN verdict hands it. Consequences written down in the
// header comment: the mirror is one-way (removeListener does not unmirror), and the
// embedded server's own connection bookkeeping never starts because 'listening' never
// fires -- the handed-off sockets are tracked here instead.
function initHttp1Fallback(server, opts) {
  const http = require('node:http');
  // DEP0201 (initializeOptions, core.js): the top-level Http1IncomingMessage and
  // Http1ServerResponse names migrate into http1Options.
  const http1Options = { ...(opts.http1Options || {}) };
  if (opts.Http1IncomingMessage !== undefined && http1Options.IncomingMessage === undefined) {
    http1Options.IncomingMessage = opts.Http1IncomingMessage;
  }
  if (opts.Http1ServerResponse !== undefined && http1Options.ServerResponse === undefined) {
    http1Options.ServerResponse = opts.Http1ServerResponse;
  }
  server._http1 = new http.Server(http1Options);
  server._http1Sockets = new Set();
  server._http1Active = 0;
  server._http1Closing = false;
  server.on('newListener', (event, listener) => {
    if (HTTP1_MIRROR.has(event)) {
      server._http1.on(event, listener);
    }
  });
  // Per-socket activity, so close() can tell an idle keep-alive socket (destroy it,
  // the way closeIdleConnections does on node) from one with a response in flight.
  server._http1.on('request', (req, res) => {
    server._http1Active++;
    req.socket._h2leanActive = true;
    res.once('finish', () => {
      server._http1Active--;
      req.socket._h2leanActive = false;
      if (server._http1Closing && server._http1Active === 0) {
        for (const socket of server._http1Sockets) {
          socket.destroy();
        }
      }
    });
  });
}

function handOffHttp1(server, socket) {
  server._http1Sockets.add(socket);
  socket.once('close', () => server._http1Sockets.delete(socket));
  server._http1.emit('connection', socket);
}

// The public-API equivalent of node's httpServerPreClose(): the idle keep-alive
// sockets go away right away, a response in flight is allowed to finish, and when the
// last one finishes the sockets it was talking on go too. node's mixin waits through
// net.Server's own connection bookkeeping instead; that bookkeeping is exactly what
// the embedded-server model cannot reach.
function closeHttp1(server) {
  server._http1Closing = true;
  for (const socket of [...server._http1Sockets]) {
    if (!socket._h2leanActive) {
      socket.destroy();
    }
  }
}

function unknownProtocol(server, socket) {
  if (server.emit('unknownProtocol', socket)) {
    return;
  }
  // core.js connectionListener: with no listener, the socket is told in a format it
  // might understand why it got nothing, and an unref'd timer bounds how long a peer
  // that never closes can hold it.
  socket.end('HTTP/1.0 403 Forbidden\r\n' +
             'Content-Type: text/plain\r\n\r\n' +
             'Missing ALPN Protocol, expected `h2` to be available.\n' +
             'If this is a HTTP request: The server was not ' +
             'configured with the `allowHTTP1` option or a ' +
             'listener for the `unknownProtocol` event.\n');
  const timer = setTimeout(() => {
    if (!socket.destroyed) {
      socket.destroy();
    }
  }, server._sessionOptions.unknownProtocolTimeout);
  timer.unref();
  socket.once('close', () => clearTimeout(timer));
}

class Http2SecureServer extends tls.Server {
  constructor(options) {
    const opts = options || {};
    super(alpnOptions(opts));
    initHttp2Server(this, opts);
    if (opts.allowHTTP1 === true) {
      initHttp1Fallback(this, opts);
    }
    this.on('secureConnection', (socket) => {
      // node's connectionListener (core.js): 'h2' negotiated becomes a session;
      // 'http/1.1' -- or no ALPN at all, which is NOT h2 prior knowledge, whatever an
      // older comment here claimed -- goes to the HTTP/1 fallback when allowHTTP1 is
      // set, and otherwise to 'unknownProtocol', with the 403 text when nobody
      // listens. Measured on node 26 in this session: a no-ALPN client speaking
      // HTTP/1.1 is served by the fallback under allowHTTP1 and receives the 403
      // without it.
      const alpn = socket.alpnProtocol;
      if (alpn === 'h2') {
        attachSession(this, socket);
      } else if (alpn === 'http/1.1' || alpn === false) {
        if (this._http1) {
          handOffHttp1(this, socket);
        } else {
          unknownProtocol(this, socket);
        }
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
