// Mutation check: every guard this library carries must have a test that fails when
// the guard is removed. Each anchor is rewritten in place, the whole suite runs against it,
// and the original is restored -- an anchor the suite does not catch is a guard with no test.
//
//   node tools/mutate-guards.js            -> all anchors
//   node tools/mutate-guards.js 0 2        -> only those indices
//   node tools/mutate-guards.js --check    -> verify every anchor matches its source
//
// Interrupt a run with SIGINT or SIGTERM between anchors, and verify after ANY forced stop:
//
//   git checkout -- lib && node tools/mutate-guards.js --check
//
// spawnSync blocks the event loop, so a signal sent while a suite is running is not serviced
// until that suite finishes -- it looks ignored, and killing the process hard (kill -9) leaves
// the mutated file on disk. Residue like that reads as a phantom regression, and it also
// overwrites any in-progress edit to the same file, which is what the preflight below catches.
'use strict';
const fs = require('node:fs');
const { spawnSync } = require('node:child_process');

const ROOT = process.cwd();

const anchors = [
  {
    file: 'lib/stream.js',
    what: 'the owed EOF is delivered before close when the local side finishes a stream '
        + 'nobody read',
    from: '    if (this._endPending) {',
    to: '    if (false && this._endPending) {'
  },
  {
    file: 'lib/server.js',
    what: 'server.setTimeout() does NOT arm a session idle timeout (node records the value)',
    from: '    this.timeout = msecs;\n    if (cb !== undefined) {',
    to: '    this.timeout = msecs;\n'
      + '    for (const session of this._sessions) session.setTimeout(msecs, cb);\n'
      + '    if (cb !== undefined) {'
  },
  {
    file: 'lib/server.js',
    what: 'server.updateSettings() reaches the sessions created afterwards',
    from: '    this._sessionOptions.settings = this.settings;',
    to: ''
  },
  {
    file: 'lib/session.js',
    what: 'session.settings() answers nothing, the way node does',
    from: '    this._sendSettings();\n    if (cb) {\n      process.nextTick(cb);\n    }\n  }',
    to: '    this._sendSettings();\n    if (cb) {\n      process.nextTick(cb);\n    }\n    return this;\n  }'
  },
  {
    file: 'lib/session.js',
    what: 'a socket read refreshes the idle timeout instead of restarting it',
    from: '    if (this._idleTimer !== null) {\n      this._idleTimer.refresh();\n    }',
    to: ''
  },
  {
    file: 'lib/session.js',
    what: 'destroying the session disarms its idle timeout',
    from: '    this._liveStreams = 0;\n    if (this._idleTimer !== null) {\n      idleClearTimeout(this._idleTimer);',
    to: '    this._liveStreams = 0;\n    if (false) {\n      idleClearTimeout(this._idleTimer);'
  },
  {
    file: 'lib/server.js',
    what: 'closeIdleSessions() only closes sessions with no live stream',
    from: '      if (session._liveStreams === 0) {\n        session.close();\n      }',
    to: '      session.close();'
  },
  {
    file: 'lib/session.js',
    what: 'sending a GOAWAY is not an event; only receiving one is',
    from: '    const tail = reason ? Buffer.byteLength(reason) : 0;\n    const body = Buffer.alloc(8 + tail);',
    to: '    this.emit("goaway", code, lastStreamID, reason);\n'
      + '    const tail = reason ? Buffer.byteLength(reason) : 0;\n    const body = Buffer.alloc(8 + tail);'
  },
  {
    file: 'lib/session.js',
    what: 'the read batch stops at the frame that destroyed the session (this is why _onGoaway '
        + 'needs no destroyed check of its own)',
    from: '    return this._open;\n  }\n\n  _handle(type, flags, sid, buf, start, len) {',
    to: '    return true;\n  }\n\n  _handle(type, flags, sid, buf, start, len) {'
  },
  {
    file: 'lib/session.js',
    what: 'a peer GOAWAY does not tear down the streams it names -- they are the SENDER\'s '
        + 'streams, and this side still owes an answer for the request in front of it',
    from: '    if (this._goawayCode === NO_ERROR) {\n      this.close(); return;\n    }',
    to: '    for (const [sid, s] of [...this._streams]) {\n'
      + '      if (sid > this._goawayLastStreamID) s._destroy(PROTOCOL_ERROR, undefined, false);\n'
      + '    }\n    if (this._goawayCode === NO_ERROR) {\n      this.close(); return;\n    }'
  },
  {
    file: 'lib/session.js',
    what: 'receiving GOAWAY(NO_ERROR) starts a graceful close, not a destroy',
    from: '    if (this._goawayCode === NO_ERROR) {\n      this.close(); return;\n    }',
    to: '    if (false) {\n      this.close(); return;\n    }'
  },
  {
    file: 'lib/session.js',
    what: 'a goodbye that arrives from the peer is still answered with one of our own',
    from: '    if (this._open && !this._goawaySent) {\n      this.sendGoaway(this._maxInboundSid, code);',
    to: '    if (false && this._open) {\n      this.sendGoaway(this._maxInboundSid, code);'
  },
  {
    file: 'lib/session.js',
    what: 'close() is not a way to undo a destroy (node bails on both flags)',
    from: '    if (this.closed || this.destroyed) {\n      return this;\n    }',
    to: '    if (this.closed) {\n      return this;\n    }'
  },
  {
    file: 'lib/session.js',
    what: 'the application is told WHY the session was destroyed',
    from: "    if (appError !== undefined) {\n      process.nextTick(emitNT, this, 'error', appError);\n    }",
    to: ''
  },
  {
    file: 'lib/session.js',
    what: 'every open stream is told about it before the session is',
    from: '    for (const s of [...this._streams.values()]) {\n      s._destroy(code, undefined, false, appError);\n    }',
    to: '    for (const s of [...this._streams.values()]) {\n      s._destroy(code, undefined, false);\n    }'
  },
  {
    file: 'lib/session.js',
    // MISSED, and it is not a missing test: measured with tools/probe-stream-teardown.js, a peer
    // reads a FIN either way -- the destroy() that follows ends the socket too (node's shape: end,
    // then destroy after the flush), which puts the same octets on the wire. What end() is FOR is
    // the half-open read side node keeps (finishSessionClose ends the socket, resumes it, and does
    // not destroy it when the session was closed gracefully) so the peer's answer can still arrive;
    // h2lean destroys instead, and that difference is AGENTS.md section 10's open problem, not a
    // test.
    what: 'a drained closing session lets the connection go (the peer sees a FIN)',
    from: '    this.socket.end();\n    this.destroy(NO_ERROR);',
    to: '    this.destroy(NO_ERROR);'
  },
  {
    file: 'lib/session.js',
    what: 'session.goawayCode reads NO_ERROR before anything has arrived, the way node\'s '
        + 'getter does',
    from: '  get goawayCode() {\n    return this._goawayCode === null ? NO_ERROR : this._goawayCode;\n  }',
    to: '  get goawayCode() {\n    return this._goawayCode;\n  }'
  },
  {
    file: 'lib/session.js',
    what: 'a closing session with bytes still in flight is not finished yet',
    from: '    if (this._liveStreams > 0 || this._outLen > 0 || this._frozenLen > 0) {\n      return;\n    }',
    to: ''
  },
  {
    file: 'lib/session.js',
    // MISSED, because every shape that reaches this line has another event re-running the same
    // predicate a moment later: a peer that answers our GOAWAY reaches it through _onGoaway, and
    // a peer that answers NOTHING leaves the stream undroppable at all (the session is stuck
    // closed-but-not-finished, which test/goaway.test.js now pins as a known divergence).
    // node's own re-check hangs off the stream teardown in a setImmediate, so this line is the
    // faithful shape of it; deleting it would only be visible in a world where the silent-peer
    // case above is fixed first.
    what: 'a closing session whose last stream has gone is finished by that stream teardown',
    from: '    if (this._closing) {\n      this._maybeFinishClose();\n    }',
    to: '    if (false) {\n      this._maybeFinishClose();\n    }'
  },
  {
    file: 'lib/session.js',
    // MISSED. The rule is read straight off node (socketOnError in internal/http2/core: ECONNRESET
    // with a goawayCode already recorded is `return session.destroy()` -- no error to the
    // application, "the other side is fully within its rights"). What cannot be pinned is the
    // shape: a peer that sends a GOAWAY and then RSTs its socket wins a race node itself does not
    // answer the same way twice -- measured both `goaway` reported before the reset and node
    // reporting sessionError:ERR_HTTP2_ERROR with the goodbye never dispatched at all. A test here
    // would be a test of scheduling, so the only thing that currently notices the removal is one
    // load-sensitive event-order trace.
    what: 'a reset that arrives after the peers GOAWAY is not an error of ours',
    from: "    if (err !== undefined && err.code === 'ECONNRESET' && this._goawayCode !== null) {",
    to: '    if (false) {'
  },
  {
    file: 'lib/server.js',
    what: 'server.close() closes the listening socket first, the way node does, instead of '
        + 'waiting for the session set to empty',
    from: '    proto[kTransportClose].call(this, cb);\n'
      + '    for (const session of [...this._sessions]) {\n      session.close();\n    }',
    to: '    for (const session of [...this._sessions]) {\n      session.close();\n    }\n'
      + '    if (this._sessions.size === 0) {\n      proto[kTransportClose].call(this, cb);\n    }'
  },
  {
    file: 'lib/session.js',
    what: 'maxHeaderListSize is measured in HPACK entry octets (+32 per field, RFC 7541 4.1)',
    from: '        // flat is name, value, name, value: one entry overhead per field line\n'
      + '        bytes += (flat.length >> 1) * ENTRY_OVERHEAD;\n',
    to: ''
  },
  {
    file: 'lib/session.js',
    what: 'an oversized header list is refused with ENHANCE_YOUR_CALM, the code node answers with',
    from: '      this.sendRst(sid, ENHANCE_YOUR_CALM);',
    to: '      this.sendRst(sid, PROTOCOL_ERROR);'
  },
  {
    file: 'lib/session.js',
    // Caught now, though for a long stretch it read as redundant: the `_maxInboundSid` advance sits
    // BEFORE every request refusal (anchor 51), so a refused id is always at or below that line and
    // its in-flight DATA could fall to the same discard at the bottom of _onDataFrame whether or not
    // the id is also in _refused. It stopped being redundant the moment 5.1.1 was added: a HEADERS on
    // a never-seen lower id is now a connection error, so the _refused record is what tells a refused
    // stream's in-flight DATA apart from that error. The test that names the difference is
    // 'a refused request without END_STREAM, its DATA, then a legal stream 3'. The record is the one
    // that says WHY the frame is a discard, and if a later edit moved the id-taking back below the
    // refusals it is what would keep an oversized-header upload from being read as DATA on an idle
    // stream (the PROTOCOL_ERROR the refusal used to raise).
    what: 'a stream refused for its header list size is remembered, so its in-flight DATA is ignored',
    from: '      this.sendRst(sid, ENHANCE_YOUR_CALM);\n      this._refused.push(sid);',
    to: '      this.sendRst(sid, ENHANCE_YOUR_CALM);'
  },
  {
    file: 'lib/session.js',
    what: 'the refused-stream history stays bounded',
    from: '    if (this._liveStreams >= this.localSettings.maxConcurrentStreams) {\n'
      + '      this.sendRst(sid, REFUSED_STREAM);\n      this._refused.push(sid);\n'
      + '      if (this._refused.length > MAX_REFUSED_HISTORY) {\n        this._refused.shift();\n      }',
    to: '    if (this._liveStreams >= this.localSettings.maxConcurrentStreams) {\n'
      + '      this.sendRst(sid, REFUSED_STREAM);\n      this._refused.push(sid);'
  },
  {
    file: 'lib/session.js',
    what: 'the dropped-stream history stays bounded',
    from: '    this._dropped.push(sid);\n'
      + '    if (this._dropped.length > MAX_DROPPED_HISTORY) {\n      this._dropped.shift();\n    }',
    to: '    this._dropped.push(sid);'
  },
  {
    file: 'lib/stream.js',
    what: 'the first write() starts an unstarted response, so HEADERS precede DATA',
    from: "    if (!this.headersSent) {\n      this.respond({ ':status': 200 });\n    }",
    to: ''
  },
  {
    file: 'lib/session.js',
    what: 'an unknown frame type is ignored rather than a connection error (RFC 9113 5.5)',
    from: '      default:\n'
      + '        // RFC 9113 5.5: "Implementations MUST discard frames that have unknown or unsupported',
    to: '      default:\n        this._fatal(PROTOCOL_ERROR, \'unknown frame type\');\n'
      + '        // RFC 9113 5.5: "Implementations MUST discard frames that have unknown or unsupported'
  },
  {
    file: 'lib/session.js',
    what: 'SETTINGS advertises only what the application set, not every local default',
    from: '    const names = [...this._advertised].filter((k) => ID_BY_SETTING[k] !== undefined\n'
      + '      && s[k] !== undefined);',
    to: '    const names = Object.keys(ID_BY_SETTING).filter((k) => s[k] !== undefined);'
  },
  {
    file: 'lib/session.js',
    what: 'the hold ceiling refuses a new stream when the parked cost exceeds the bound',
    from: '    if (this._heldCost() >= MAX_SESSION_HOLD) {\n      this.sendRst(sid, REFUSED_STREAM);',
    to: '    if (false) {\n      this.sendRst(sid, REFUSED_STREAM);'
  },

  // ---- the frame-state and message-framing guards of RFC 9113 5/6/8 and RFC 7541 6.3 -------
  // One anchor per rule the h2spec sweep used to fail for h2lean alone. Each is a condition
  // turned off, never a body rewritten: the suite has to fail because the guard stopped
  // refusing, not because the harness could not find the code any more.
  {
    file: 'lib/session.js',
    what: 'SETTINGS is a connection control frame and names no stream',
    from: '        if (sid !== 0) {\n          this._fatal(PROTOCOL_ERROR, \'SETTINGS with a non-zero stream id\');',
    to: '        if (false) {\n          this._fatal(PROTOCOL_ERROR, \'SETTINGS with a non-zero stream id\');'
  },
  {
    file: 'lib/session.js',
    what: 'a SETTINGS ACK carries no entries',
    from: '          if (len !== 0) {\n            this._fatal(FRAME_SIZE_ERROR, \'SETTINGS ACK must have an empty payload\');',
    to: '          if (false) {\n            this._fatal(FRAME_SIZE_ERROR, \'SETTINGS ACK must have an empty payload\');'
  },
  {
    file: 'lib/session.js',
    what: 'PING is a connection control frame and names no stream',
    from: "        if (sid !== 0) {\n          this._fatal(PROTOCOL_ERROR, 'PING with a non-zero stream id'); return;\n        }",
    to: "        if (false) {\n          this._fatal(PROTOCOL_ERROR, 'PING with a non-zero stream id'); return;\n        }"
  },
  {
    file: 'lib/session.js',
    what: 'a PING is eight octets, ACK or not',
    from: "        if (len !== 8) {\n          this._fatal(FRAME_SIZE_ERROR, 'PING payload must be 8 octets'); return;\n        }",
    to: "        if (false) {\n          this._fatal(FRAME_SIZE_ERROR, 'PING payload must be 8 octets'); return;\n        }"
  },
  {
    file: 'lib/session.js',
    what: 'a PRIORITY frame is five octets long, even though its payload is ignored',
    from: "        if (len !== 5) {\n          this._fatal(FRAME_SIZE_ERROR, 'PRIORITY payload must be 5 octets'); return;\n        }",
    to: "        if (false) {\n          this._fatal(FRAME_SIZE_ERROR, 'PRIORITY payload must be 5 octets'); return;\n        }"
  },
  {
    file: 'lib/session.js',
    what: 'a PRIORITY frame naming stream 0 ends the connection, even though the frame is ignored',
    from: "        if (sid === 0) {\n          this._fatal(PROTOCOL_ERROR, 'PRIORITY on stream 0'); return;\n        }",
    to: "        if (false) {\n          this._fatal(PROTOCOL_ERROR, 'PRIORITY on stream 0'); return;\n        }"
  },
  {
    file: 'lib/session.js',
    what: 'SETTINGS_ENABLE_PUSH is a boolean',
    from: '      if (id === 0x2 && value > 1) {',
    to: '      if (false) {'
  },
  {
    file: 'lib/session.js',
    what: 'a HEADERS priority block is five octets, padding included',
    from: '      if (l < 5) {\n        this._fatal(FRAME_SIZE_ERROR, \'HEADERS priority block must be 5 octets\');',
    to: '      if (false) {\n        this._fatal(FRAME_SIZE_ERROR, \'HEADERS priority block must be 5 octets\');'
  },
  {
    file: 'lib/session.js',
    what: 'a stream may not depend on itself, and the failure is the connection\'s',
    from: '      if ((head.readUInt32BE(base + p) & 0x7fffffff) === sid) {',
    to: '      if (false) {'
  },
  {
    file: 'lib/session.js',
    what: 'a trailer block has to arrive with END_STREAM',
    from: '      if (!end) {\n        // RFC 9113 8.1: a HEADERS block after the request headers',
    to: '      if (false) {\n        // RFC 9113 8.1: a HEADERS block after the request headers'
  },
  {
    file: 'lib/session.js',
    what: 'a content-length promise on a HEADERS that ends the stream is refused before the '
        + 'application sees the stream',
    from: "    if (end && headers['content-length'] !== undefined && +headers['content-length'] > 0) {",
    to: '    if (false) {'
  },
  {
    file: 'lib/session.js',
    what: 'the promised content-length reaches the stream as a number, so the body can be '
        + 'measured against it',
    from: '    if (promised !== undefined) {\n      stream._expLen = +promised;\n    }',
    to: ''
  },
  {
    file: 'lib/session.js',
    what: 'a body that runs past its content-length is the connection\'s problem',
    from: '    if (stream._expLen >= 0 && stream._bodyLen + l > stream._expLen) {',
    to: '    if (false) {'
  },
  {
    file: 'lib/session.js',
    what: 'WINDOW_UPDATE for a stream in the idle state ends the connection',
    from: '      if (sid > this._maxInboundSid) {\n        this._fatal(PROTOCOL_ERROR, \'WINDOW_UPDATE on an idle stream\');',
    to: '      if (false) {\n        this._fatal(PROTOCOL_ERROR, \'WINDOW_UPDATE on an idle stream\');'
  },
  {
    file: 'lib/session.js',
    what: 'RST_STREAM never names stream 0',
    from: '    this._rstTokens--;\n\n    if (sid === 0) {',
    to: '    this._rstTokens--;\n\n    if (false) {'
  },
  {
    file: 'lib/session.js',
    what: 'RST_STREAM for a stream in the idle state ends the connection',
    from: '      if (sid > this._maxInboundSid) {\n        this._fatal(PROTOCOL_ERROR, \'RST_STREAM on an idle stream\');',
    to: '      if (false) {\n        this._fatal(PROTOCOL_ERROR, \'RST_STREAM on an idle stream\');'
  },
  {
    file: 'lib/session.js',
    what: 'the connection window is credited in half-window steps, not per read batch',
    from: '    if (this._connRecvUsed < this.localSettings.initialWindowSize >> 1) {\n      return;\n    }',
    to: '    if (this._connRecvUsed === 0) {\n      return;\n    }'
  },
  {
    file: 'lib/stream.js',
    what: 'a body shorter than its content-length is the stream\'s problem',
    from: '    if (this._expLen >= 0 && this._bodyLen < this._expLen) {',
    to: '    if (false) {'
  },
  {
    file: 'lib/hpack.js',
    what: 'a dynamic-table size update belongs at the start of a header block',
    from: '        // the octets arrive would rebuild a different context from ours.\n'
      + '        if (out.length !== 0) {',
    to: '        // the octets arrive would rebuild a different context from ours.\n'
      + '        if (false) {'
  },

  // ---- the stream-state memory of RFC 9113 5.1: what a frame naming a finished stream IS -------
  // DATA and HEADERS that name a stream whose message the peer already ended are answered with a
  // connection error of type STREAM_CLOSED, whether the stream is still in the table (half-closed
  // (remote)) or has just left it (_ended): 5.1 reads the closed shape as "MAY ... as a connection
  // error" and node -- the oracle -- makes no present/closed distinction here, so h2lean keeps its
  // verdict (a stream error on the present shape is the other RFC reading, but it regressed h2spec's
  // DATA check). What is DISCARDED, not errored, is an id a reset ended or this side refused
  // (_isGone) and one the peer never opened (the skip branch): those are the memories below.
  {
    file: 'lib/session.js',
    what: 'DATA after the peer\'s END_STREAM is the connection\'s error, not the stream\'s',
    from: '    if (stream.state.remoteClose === 1) {',
    to: '    if (false) {'
  },
  {
    file: 'lib/session.js',
    what: 'the same, once the stream has left the table: the id ended in this read is still remembered',
    from: '      if (this._ended.includes(sid)) {\n'
      + "        this._fatal(STREAM_CLOSED, 'DATA after the peer ended the stream');",
    to: '      if (false) {\n'
      + "        this._fatal(STREAM_CLOSED, 'DATA after the peer ended the stream');"
  },
  {
    file: 'lib/session.js',
    what: 'HEADERS after the peer\'s END_STREAM, whichever fields the block holds',
    from: '    if (existing !== undefined ? existing.state.remoteClose === 1\n'
      + '      : (!this._isGone(sid) && this._ended.includes(sid))) {',
    to: '    if (false) {'
  },
  {
    file: 'lib/session.js',
    what: 'DATA for a stream in the idle state -- sid 0, an even id, an id never opened -- is '
        + 'the PROTOCOL_ERROR it is named for',
    from: '      if (sid === 0 || (sid & 1) === 0 || sid > this._maxInboundSid) {',
    to: '      if (false) {'
  },
  {
    file: 'lib/session.js',
    what: 'an id below the highest one opened is discarded only if this side has a record of it',
    from: '      if (sid <= this._maxInboundSid) {',
    to: '      if (false) {'
  },
  {
    file: 'lib/session.js',
    what: 'an id below the highest one opened that this side never saw is the connection error 5.1.1 '
        + 'mandates (the deviation node stays silent on)',
    from: "        this._fatal(PROTOCOL_ERROR, 'HEADERS on an id below the highest one opened');",
    to: "        if (false) this._fatal(PROTOCOL_ERROR, 'HEADERS on an id below the highest one opened');"
  },
  {
    file: 'lib/session.js',
    what: 'the peer\'s id is taken BEFORE anything refuses the request, so the frames it already '
        + 'sent for that id are a refused stream\'s, not an idle one\'s',
    from: '      this._maxInboundSid = sid;\n'
      + '      this.state.lastProcStreamID = sid;\n'
      + '    }\n\n'
      + '    // RFC 9113 8.1.1: a request that fails the message parsing rules is refused with\n'
      + '    // RST_STREAM(PROTOCOL_ERROR), before the application ever sees it. Checking here --\n'
      + '    // after decode, before any stream object or event -- is what makes the difference\n'
      + '    // between "the handler declined the request" and "the handler was never reached",\n'
      + '    // and only the second one denies a smuggling chain its second reader.\n'
      + '    if (verdict !== null && verdict.problem !== undefined) {\n'
      + '      this.sendRst(sid, PROTOCOL_ERROR);\n'
      + '      return;\n'
      + '    }\n',
    to: '      this.state.lastProcStreamID = sid;\n'
      + '    }\n\n'
      + '    // RFC 9113 8.1.1: a request that fails the message parsing rules is refused with\n'
      + '    // RST_STREAM(PROTOCOL_ERROR), before the application ever sees it. Checking here --\n'
      + '    // after decode, before any stream object or event -- is what makes the difference\n'
      + '    // between "the handler declined the request" and "the handler was never reached",\n'
      + '    // and only the second one denies a smuggling chain its second reader.\n'
      + '    if (verdict !== null && verdict.problem !== undefined) {\n'
      + '      this.sendRst(sid, PROTOCOL_ERROR);\n'
      + '      return;\n'
      + '    }\n'
      + '    if (existing === undefined) this._maxInboundSid = sid;\n'
  },
  {
    file: 'lib/session.js',
    what: 'a reset that arrives for an id nobody has left in the table still lands: it is what '
        + 'makes that id\'s in-flight frames the discard',
    from: '      this._rememberKilled(sid);',
    to: ''
  },
  {
    file: 'lib/session.js',
    what: 'the two ways a stream leaves the table are remembered apart -- a reset is not the '
        + 'peer ending its message',
    from: '    if (killed) {\n      // remember the id: DATA the peer already put on the wire will turn up next',
    to: '    if (false) {\n      // remember the id: DATA the peer already put on the wire will turn up next'
  },
  {
    file: 'lib/session.js',
    what: 'the ended-in-this-batch memory reaches only as far as the read that made it',
    from: '    this._ended = []; // the last batch\'s finished streams are no longer here',
    to: '    if (false) this._ended = []; // the last batch\'s finished streams are no longer here'
  },
  {
    file: 'lib/session.js',
    what: 'the ended-stream history stays bounded',
    from: '      if (this._ended.length < MAX_DROPPED_HISTORY) {\n        this._ended.push(stream.id);\n      }',
    to: '      this._ended.push(stream.id);'
  },
  {
    file: 'lib/client-session.js',
    what: 'credit not yet due stays in the debt list instead of being forgotten',
    from: '      } else {\n        debt[k++] = stream;\n      }\n    }\n    debt.length = k;',
    to: '      }\n    }\n    debt.length = k;'
  },
  {
    file: 'lib/client-session.js',
    what: 'the mark is cleared only by the pass that actually released the credit',
    from: '    let k = 0;\n    for (let i = 0; i < debt.length; i++) {\n      const stream = debt[i];',
    to: '    let k = 0;\n    for (let i = 0; i < debt.length; i++) {\n      const stream = debt[i];\n      stream._debtMarked = false;'
  },
  {
    file: 'lib/client-session.js',
    what: 'a credit release with nothing queued goes straight to the socket, not into the queue',
    from: '      if (this._writing && this._open && this._started && this._outLen === 0\n        && this.socket.writable) {',
    to: '      if (false) {'
  },
  {
    file: 'lib/client-session.js',
    what: 'a credit release never overtakes frames that are still queued',
    from: '      if (this._writing && this._open && this._started && this._outLen === 0\n        && this.socket.writable) {',
    to: '      if (this._wuLen !== 0) {'
  },
  {
    file: 'lib/session.js',
    what: 'a padded DATA frame whose payload straddles the reads is handed over as views, '
        + 'not assembled into a buffer of its own',
    from: '      if (type === DATA) {\n        // RFC 9113 6.1: a padded payload\'s views are not the payload -- the pad',
    to: '      if (false) {\n        // RFC 9113 6.1: a padded payload\'s views are not the payload -- the pad'
  },
  {
    file: 'lib/session.js',
    what: 'the padding is taken off the views of a straddling padded payload',
    from: '          data = trimPadding(pieces);',
    to: '          data = len;'
  },
  {
    file: 'lib/session.js',
    what: 'a padded DATA frame with an empty payload is a connection error',
    from: "      if (len === 0) {\n        this._fatal(PROTOCOL_ERROR, 'padded DATA frame with an empty payload'); return;\n      }",
    to: "      if (false) {\n        this._fatal(PROTOCOL_ERROR, 'padded DATA frame with an empty payload'); return;\n      }"
  },
  {
    file: 'lib/session.js',
    what: 'a padding larger than the DATA payload is a connection error',
    from: "      if (l < 0) {\n        this._fatal(PROTOCOL_ERROR, 'pad larger than the DATA payload'); return;\n      }",
    to: "      if (false) {\n        this._fatal(PROTOCOL_ERROR, 'pad larger than the DATA payload'); return;\n      }"
  },
  {
    file: 'lib/client-session.js',
    what: 'a padded DATA frame whose payload straddles the reads is handed over as views, '
        + 'not assembled into a buffer of its own',
    from: '    if (type === DATA) {\n      // RFC 9113 6.1: a padded payload\'s views are not the payload -- the pad',
    to: '    if (false) {\n      // RFC 9113 6.1: a padded payload\'s views are not the payload -- the pad'
  },
  {
    file: 'lib/client-session.js',
    what: 'the padding is taken off the views of a straddling padded payload',
    from: '        data = trimPadding(pieces);',
    to: '        data = len;'
  },
  {
    file: 'lib/client-session.js',
    what: 'a padded DATA frame with an empty payload is a connection error',
    from: "      if (len === 0) {\n        this._fatal(PROTOCOL_ERROR, 'padded DATA frame with an empty payload'); return;\n      }",
    to: "      if (false) {\n        this._fatal(PROTOCOL_ERROR, 'padded DATA frame with an empty payload'); return;\n      }"
  },
  {
    file: 'lib/client-session.js',
    what: 'a padding larger than the DATA payload is a connection error',
    from: "      if (l < 0) {\n        this._fatal(PROTOCOL_ERROR, 'pad larger than the DATA payload'); return;\n      }",
    to: "      if (false) {\n        this._fatal(PROTOCOL_ERROR, 'pad larger than the DATA payload'); return;\n      }"
  },
  {
    file: 'lib/client-session.js',
    what: 'a stream runs on the window the peer advertised when it is opened, not on the '
        + 'one the stream object was built with',
    from: '    req.stream._sendWin = this.remoteSettings.initialWindowSize;',
    to: ''
  },
  {
    file: 'lib/client-session.js',
    what: 'the first request leaves in the first flight, without waiting for the peer\'s SETTINGS',
    from: '    if (this._outboundQueue.length === 0\n      && (max === undefined || this._streams.size < max)) {',
    to: '    if (false) {'
  },
  {
    file: 'lib/session.js',
    // MISSED, and the same for the three copies below: the frame payload these lines copy out
    // of lives in a buffer that is never reused -- a control frame that straddled the reads
    // was assembled by pieceConcat into a fresh buffer, and one that did not is a view of the
    // socket chunk, which node never hands out twice -- so the view the mutation leaves behind
    // is as stable as the copy. The guard is defence-in-depth for AGENTS.md 5: it becomes
    // observable the day the assembled control-frame buffer is made reusable, and until then
    // test/buffer-lifetime.test.js pins the invariant the other way -- it would catch a reused
    // parse buffer through the same four payloads.
    what: 'the PING payload the application keeps is copied out of the frame it arrived in',
    from: '        const payload = Buffer.allocUnsafe(8);\n        buf.copy(payload, 0, start, start + 8);',
    to: '        const payload = buf.subarray(start, start + 8);'
  },
  {
    file: 'lib/session.js',
    what: 'the GOAWAY debug data the application keeps is copied out of the frame it arrived in',
    from: '      debug = Buffer.allocUnsafe(len - 8);\n      buf.copy(debug, 0, start + 8, start + len);',
    to: '      debug = buf.subarray(start + 8, start + len);'
  },
  {
    file: 'lib/client-session.js',
    what: 'the pong a ping callback keeps is copied out of the frame it arrived in',
    from: '            const pong = Buffer.allocUnsafe(8);\n            buf.copy(pong, 0, start, start + 8);',
    to: '            const pong = buf.subarray(start, start + 8);'
  },
  {
    file: 'lib/client-session.js',
    what: 'the GOAWAY debug data the application keeps is copied out of the frame it arrived in',
    from: '      debug = Buffer.allocUnsafe(len - 8);\n      buf.copy(debug, 0, start + 8, start + len);',
    to: '      debug = buf.subarray(start + 8, start + len);'
  },
  {
    file: 'lib/session.js',
    what: 'a header block that straddles the reads is decoded out of the views, not out of a copy',
    from: '      if (type === HEADERS) {\n        this._beginHeadersPieces(sid, flags, pieces, len);',
    to: '      if (false) {\n        this._beginHeadersPieces(sid, flags, pieces, len);'
  },
  {
    file: 'lib/client-session.js',
    what: 'a header block that straddles the reads is decoded out of the views, not out of a copy',
    from: '    if (type === HEADERS) {\n      this._beginHeadersPieces(sid, flags, pieces, len);',
    to: '    if (false) {\n      this._beginHeadersPieces(sid, flags, pieces, len);'
  },
  {
    file: 'lib/hpack.js',
    what: 'the piece cursor steps over an empty view instead of stopping on it',
    from: '    while (this.i < this.pieces.length && this.pieces[this.i].length === 0) {\n      this.i++;\n    }\n',
    to: ''
  },
  {
    file: 'lib/hpack.js',
    what: 'a size update after a field line is refused by the piece decoder too',
    from: '        // block -- before its first field line. See decode() for the measurement.\n'
      + '        if (out.length !== 0) {',
    to: '        // block -- before its first field line. See decode() for the measurement.\n'
      + '        if (false) {'
  },
  {
    file: 'lib/session.js',
    what: 'a padded HEADERS frame with an empty payload is a connection error',
    from: "      if (headLen < 1) {\n        this._fatal(PROTOCOL_ERROR, 'padded HEADERS frame with an empty payload');",
    to: "      if (false) {\n        this._fatal(PROTOCOL_ERROR, 'padded HEADERS frame with an empty payload');"
  },
  {
    file: 'lib/session.js',
    what: 'a padding larger than the HEADERS payload is a connection error',
    from: "      if (l < 0) {\n        this._fatal(PROTOCOL_ERROR, 'pad larger than the HEADERS payload'); return -1;\n      }",
    to: "      if (false) {\n        this._fatal(PROTOCOL_ERROR, 'pad larger than the HEADERS payload'); return -1;\n      }"
  },
  {
    file: 'lib/session.js',
    what: 'the padding of a padded HEADERS frame is not part of the header block',
    from: '    this._hdrFrag = l;\n    return p;',
    to: '    this._hdrFrag = len - p;\n    return p;'
  },
  {
    file: 'lib/headers.js',
    what: 'a dynamic table size update is written in front of the block it announces',
    from: '  const at = update === null ? 0 : update.length;',
    to: '  const at = 0;'
  }
];

function runSuite() {
  const t0 = Date.now();
  // The cap has to outlast a mutation that makes tests HANG, not just fail, but it must not be
  // so long that a hung run is indistinguishable from a slow one: a clean suite takes ~25 s, so
  // four minutes is already a hang. A run that reaches the cap is reported as HUNG unless it had
  // failures to show -- reading a killed run as a caught guard is how a bogus pass gets logged.
  //
  // --test-force-exit is what keeps a LIVENESS mutation from silencing the report altogether:
  // node's runner prints a file's results only once its process exits, so anchor 20 -- a
  // `server.close()` that never gives its port back -- left 17 of the 23 files ref'd, the cap was
  // reached and the output contained not one failure, even though the mutation does cause them.
  // Forcing the exit after the run has reported changes nothing about the assertions or the
  // per-test timeouts; it only stops a leaked listening socket from eating the verdict.
  const r = spawnSync('node', ['--test', '--test-timeout=20000', '--test-force-exit',
    // FILES=test/idle-timeout.test.js narrows the run to one file: enough to decide whether a
    // guard has a failing test, and cheap enough to bisect an anchor that hangs the whole suite.
    ...(process.env.FILES ? process.env.FILES.trim().split(/\s+/) : ['test/'])], {
    cwd: ROOT, encoding: 'utf8', timeout: 240000
  });
  const out = (r.stdout || '') + (r.stderr || '');
  const fail = /^# fail (\d+)$/m.exec(out) || /^ℹ fail (\d+)$/m.exec(out);
  const names = [...out.matchAll(/^✖ (.+?) \(/gm)].map((m) => m[1]);
  // no summary line means the run died, timed out or failed to load a file -- keep the tail so
  // that "the suite failed" and "the suite never reported" can be told apart by reading.
  return { code: r.status, failed: fail ? Number(fail[1]) : -1, names, ms: Date.now() - t0,
    killed: r.signal ? 'signal=' + r.signal : '',
    tail: fail ? '' : out.slice(-700) };
}

let missed = 0;
let hung = 0;
// A SIGTERM between the mutation write and the restore leaves the library file MUTATED, and the
// next test run then fails for a reason that has nothing to do with the code: a guard's body sits
// blanked, and the failure reads as a phantom regression in that guard. Restore on the way out.
let dirty = null;
function restore() {
  if (dirty !== null) {
    fs.writeFileSync(`${ROOT}/${dirty.file}`, dirty.original);
    console.log(`interrupted: ${dirty.file} restored`);
    dirty = null;
  }
}
for (const sig of ['SIGINT', 'SIGTERM', 'SIGHUP']) {
  process.on(sig, () => {
    restore(); process.exit(130);
  });
}
const only = process.argv.slice(2).map(Number).filter((n) => Number.isInteger(n));
// One instance at a time. A second run mutates the same files, and each run's "restore" writes
// back the content it captured -- which for the other run is a mutation -- so the tree ends up
// with anchor residue that reads as a phantom regression, and every verdict of both runs is
// about a library neither of them describes. Overlapping a sweep with a `git checkout -- lib`
// on the same files produces exactly that: a tree neither run describes and verdicts that cannot
// be reproduced.
if (!process.argv.includes('--check')) {
  const lock = `${ROOT}/tools/.mutate-guards.lock`;
  let holder = null;
  try {
    holder = fs.readFileSync(lock, 'utf8').trim();
  } catch { /* no lock yet */ }
  if (holder !== null) {
    let alive = true;
    try {
      process.kill(Number(holder), 0);
    } catch (e) {
      alive = e.code === 'EPERM';
    }
    if (alive) {
      console.error(`another mutation run holds ${lock} (pid ${holder}).\n`
        + 'Wait for it, or stop it and verify the tree before starting again:\n'
        + '  kill -9 ' + holder + ' && git status --short lib && node tools/mutate-guards.js --check');
      process.exit(3);
    }
  }
  fs.writeFileSync(lock, `${process.pid}\n`);
  process.on('exit', () => {
    try {
      fs.unlinkSync(lock);
    } catch { /* already gone */ }
  });
}
// Refuse to start unless every anchor still matches its source exactly once. A leftover
// mutation from an interrupted run makes one anchor match nothing, and a sweep over a file that
// is not what this list describes reports nothing about the code. (This also allows legitimate
// uncommitted work in lib/, which a dirty-tree check would have refused.)
if (!process.argv.includes('--check')) {
  const stale = [];
  for (let index = 0; index < anchors.length; index++) {
    const a = anchors[index];
    const n = fs.readFileSync(a.file, 'utf8').split(a.from).length - 1;
    if (n !== 1) {
      stale.push(`  ${index}  ${a.file}  matches=${n}  ${a.what}`);
    }
  }
  if (stale.length > 0) {
    console.error('anchors do not match the tree -- restore it first (`git checkout -- lib`),\n'
      + 'then `node tools/mutate-guards.js --check`:\n' + stale.join('\n'));
    process.exit(2);
  }
}
if (process.argv.includes('--check')) {
  // Print the match count of every anchor without running anything: an anchor that matches zero
  // times or twice is a typo in this file, not a result about the code. The exit code carries the
  // same verdict, because a checker that always exits 0 is only read by a human: `eslint --fix`
  // over lib/ rewrites the guards the anchors quote verbatim, and 29 of them did that silently.
  let bad = 0;
  for (let index = 0; index < anchors.length; index++) {
    const a = anchors[index];
    const n = fs.readFileSync(a.file, 'utf8').split(a.from).length - 1;
    if (n !== 1) {
      bad++;
    }
    console.log(`${n === 1 ? 'ok  ' : 'BAD '} ${index}  ${a.file}  matches=${n}  ${a.what}`);
  }
  console.log(`anchors=${anchors.length} bad=${bad}`);
  process.exit(bad === 0 ? 0 : 1);
}
for (let index = 0; index < anchors.length; index++) {
  if (only.length > 0 && !only.includes(index)) {
    continue;
  }
  const a = anchors[index];
  const path = `${ROOT}/${a.file}`;
  const original = fs.readFileSync(a.file, 'utf8');
  const parts = original.split(a.from);
  if (parts.length !== 2) {
    console.log(`AMBIGUOUS anchor "${a.what}" (${parts.length - 1} matches in ${a.file})`);
    missed++;
    continue;
  }
  try {
    dirty = { file: a.file, original };
    fs.writeFileSync(path, original.replace(a.from, a.to));
    const res = runSuite();
    // A run with no summary line was killed -- by the cap, by a signal, or by a suite that could
    // not finish. That is only a HANG when it also reported no failing test: measured with anchor
    // 20, a `server.close()` that never gives up its port leaves 17 of the 23 test files unable to
    // exit, so the cap was reached while eight tests in that same output had already failed. A
    // guard whose removal makes the suite unable to finish AND makes tests fail is a caught guard;
    // a killed run with nothing to show is what needs looking at.
    const summaryless = res.failed === -1;
    const stalled = summaryless && res.names.length === 0;
    const caught = summaryless ? !stalled : (res.failed > 0 || res.code !== 0);
    if (stalled) {
      hung++;
    } else if (!caught) {
      missed++;
    }
    console.log(`${stalled ? 'HUNG  ' : caught ? 'CAUGHT' : 'MISSED'}  ${a.what}`
      + `  [${(res.ms / 1000).toFixed(0)}s ${res.killed}`
      + (summaryless ? `killed, ${res.names.length} tests had failed` : `fail=${res.failed}`)
      + (caught ? `: ${res.names.slice(0, 3).join(' | ')}` : '') + ']');
    if (stalled) {
      console.log('  ---- output tail ----\n' + res.tail);
    }
  } finally {
    fs.writeFileSync(path, original); // restore even if the run threw
    dirty = null;
  }
}
console.log(`anchors=${anchors.length} missed=${missed} hung=${hung}`);
