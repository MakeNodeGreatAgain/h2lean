// Response/trailer header validation and HPACK block construction.
//
// The optimisation that matters here is granularity: encoding one field line at a
// time costs a key string plus a Map lookup each, while encoding the *whole block*
// once and memcpy-ing it is nearly free (OPTIMIZATION.md item 6). So the cache
// is keyed by the identity of the caller's headers object and holds the finished
// HPACK bytes.
//
// That is only sound when the object cannot change underneath us, so caching is
// restricted to frozen objects (Object.isSealed is not enough: sealed objects still
// allow a value to be reassigned). Non-frozen objects are encoded fresh on every call,
// exactly like node does. And because the encoder keeps an HPACK dynamic table, the
// bytes also depend on the connection they go out on: the cache lives on the session.
'use strict';

const { Encoder } = require('./hpack');
const { sensitiveHeaders: KSensitiveHeaders } = require('node:http2');

// A header map with no prototype.
//
// The prototype is taken away after the object is built rather than never set,
// because V8 keeps such an object in fast-properties mode, where the stores and
// the keyed reads cost less than on a dictionary-mode one (OPTIMIZATION.md item 4;
// the measured numbers live there, not here).
//
// The prototype still has to be null: a field named "__proto__" must become an
// ordinary field and not a write to the map's prototype, and a map that keeps
// Object.prototype would answer "constructor" or "toString" for a field that
// was never set -- two readers of one map disagreeing about what it carries.
// node's own header objects are { __proto__: null } literals
// (internal/http2/util.js, toHeaderObject), and the two shapes are
// indistinguishable from the outside: same prototype, same enumeration order,
// same JSON, same freeze behaviour (bench/micro/nullproto-semantics.js).
//
// A fast map hangs on the process-wide transition tree of its root map, and the
// field names come from the peer -- so the question is whether that tree can
// grow without bound. It cannot: V8 rehomes the root map once its transition
// array fills (observed: "Transitions #1536"), and the retained heap is flat
// across 1.8 million distinct six-field name sets (bench/micro/nullproto-root.js,
// bench/micro/nullproto-tree2.js, bench/micro/nullproto-tree3.js).
function nullProto() {
  return Object.setPrototypeOf({}, null);
}

// Encoded bytes for one response, reused between calls. The encoder that fills it
// belongs to the session (lib/session), because a dynamic table is per-connection
// state, so nothing on this path is shared between sessions.
// A response block lives for microseconds: _frame() copies it into the staging buffer
// of the outbound queue, so the queue never holds a view of this scratch. See block()
// before holding on to the returned view across another call.
let scratch = Buffer.allocUnsafe(4096);

const isToken = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
// Whether a field name is legal depends only on its characters, so the names already
// accepted are remembered and the regex is not run again for them (OPTIMIZATION.md
// item 7), which a compat
// response pays for every header it sends because its header object is not frozen
// and cannot be cached.
// Bounded: an application that reflects untrusted input into header names would
// otherwise grow the cache for the life of the process, and a name that is new to a
// full cache costs the regex and nothing else -- the same as before this existed.
// A Set rather than an object: 'constructor' and '__proto__' must not be special, and
// the object form is the slower one (OPTIMIZATION.md item 7).
const VALID_NAMES = new Set();
const VALID_NAMES_MAX = 1024;

// RFC 9113 8.2.1: a field name is a token, and it is lower case. The token check is not
// cosmetic -- a name carrying CR, LF or NUL is how one header becomes two *lines* again
// once a proxy translates this connection to HTTP/1.1, which is the class of CVE-2020-8287
// (Node.js read one of two copies of a field and ignored the other, and the same split
// happens when a separator is smuggled inside a field). Refusing the name here, before a
// single octet is produced, is the whole mitigation: the application cannot build a header
// that a downstream reader would parse differently. RFC 9113 10.3 calls out exactly this
// HTTP/2-to-HTTP/1.1 translation risk as the reason the rules are strict.
function isFieldName(key) {
  if (VALID_NAMES.has(key)) {
    return true;
  }
  if (!isToken.test(key)) {
    return false;
  }
  if (VALID_NAMES.size < VALID_NAMES_MAX) {
    VALID_NAMES.add(key);
  }
  return true;
}

// A field name in the form the wire wants it.
//
// Two shapes, and they are not the same one: the compat layer's name is
// trimmed and lower case (node's compat layer does `name.trim().toLowerCase()`,
// internal/http2/compat.js), the encoder's is lower case alone (node's
// buildNgHeaderString does `key.toLowerCase()`, internal/http2/util.js).
//
// Both are cached. A name that is already in the wanted shape costs nothing to
// produce -- trim() and toLowerCase() return the very same string object --
// so what is left is the call itself -- a Map lookup beats the two calls it
// replaces (OPTIMIZATION.md item 8).
//
// Bounded, and it never evicts, like VALID_NAMES above: the names come from
// application code, and an application that reflects untrusted input into
// header names would otherwise grow the cache for the life of the process.
// The bound is the answer to that, and a better one than a periodic reset: a
// reset needs a timer (a live handle) and throws away the hot entries, while
// the bound caps the damage and keeps what is hot. A name that is new to a
// full cache costs the lookup and the normalisation, which is what a hit
// costs anyway.
//
// The miss is the expensive direction, and the insert into a growing Map is what
// makes it so (OPTIMIZATION.md item 8). So the cache pays for the names an application
// writes as literals, whose hash V8 computes once and keeps in the string, and it
// loses for the names it builds fresh every time.
const NAME_CACHE = new Map();
const LOWER_CACHE = new Map();
const NAME_CACHE_MAX = 1024;

// The compat layer's shape. A name that is not a string throws here, the way
// node's private kSetHeader does (name.trim() is not a function), and the
// public accessors in lib/compat.js refuse it before they get here.
function trimName(name) {
  let out = NAME_CACHE.get(name);
  if (out === undefined) {
    out = name.trim().toLowerCase();
    if (NAME_CACHE.size < NAME_CACHE_MAX) {
      NAME_CACHE.set(name, out);
    }
  }
  return out;
}

// The encoder's shape: lower case, not trimmed. Trimming here would change
// what the encoder accepts -- a name with a space around it is a token error
// today, and it has to stay one (RFC 9113 8.2.1).
function lowerName(name) {
  let out = LOWER_CACHE.get(name);
  if (out === undefined) {
    out = name.toLowerCase();
    if (LOWER_CACHE.size < NAME_CACHE_MAX) {
      LOWER_CACHE.set(name, out);
    }
  }
  return out;
}

// The size of the two caches, for the test that pins the bound. The caches are
// module state, and a bound that nothing can see is a bound nothing checks.
function nameCacheSize() {
  return { names: NAME_CACHE.size, lowers: LOWER_CACHE.size };
}

// RFC 9113 8.2: a field value may not contain NUL, LF or CR (HTAB is allowed), and no
// other control character -- the same injection class as the name check above, seen from
// the value side. It is also what makes a value safe to use as a map key: the encoder's
// byte cache is keyed by the name and then by the value (lib/hpack.js), so the two are
// never concatenated and no separator can be spoofed -- but a key shape that does
// concatenate them would need this check to stay.
//
// The class is the one nghttp2 applies to an INBOUND value, measured against a live
// node server (bench/micro/probe-value-matrix.js): every control character but HTAB, and DEL.
// A field whose value carries one is ignored (NGHTTP2_ERR_IGN_HTTP_HEADER) and the
// stream lives -- the request is served, without the field.
//
// The OUTBOUND check is h2lite's own, and stricter than node's: node does not look at
// an outbound value at all and puts a CR on the wire (bench/micro/probe-out-value-matrix.js --
// every one of the fifteen characters tried is accepted, and the CR arrives in the
// HEADERS frame). Refusing here is the mitigation the CVE-2020-8287 class needs: the
// value never becomes an octet, so a translator behind this server cannot read it
// differently than we do. It costs an application that sets such a value a refusal
// node does not make.
const badValue = /[\u0000-\u0008\u000a-\u001f\u007f]/;

// Kept as a Set: at five names the shape of the operation decides it, not the
// size (OPTIMIZATION.md item 10).
// RFC 9113 8.2.2 forbids these on an HTTP/2 connection: they are hop-by-hop in HTTP/1.1,
// so sending one would either be ignored by the peer (a lie to the application) or
// forwarded by a downstream translator (CVE-2020-8287's class: two readers, one field).
const CONNECTION_SPECIFIC = new Set(['connection', 'keep-alive', 'proxy-connection',
  'transfer-encoding', 'upgrade']);

// Whether a field value carries a character that may not appear in one. The server's
// inbound path asks the same question (lib/session.js) and answers it the way nghttp2
// does: the field is ignored, the stream lives.
function hasBadValue(value) {
  return badValue.test(value);
}

function httpError(code, msg) {
  const err = new Error(msg);
  err.code = code;
  return err;
}

// node adds a `date` header to every response unless the application set one or
// turned sendDate off. The value only changes once a second, so it is cached -- and
// the cache is invalidated by a one-shot unref'd timer rather than compared against
// the clock on every call, which is what node's own HTTP/1.1 date cache does
// (internal/http; OPTIMIZATION.md item 9 for the measurements). An idle server holds
// no timer at all; a refresh happens on the next response after the boundary, which
// can be a little late if the loop was busy -- exactly as in node.
let dateString;
let dateTimer = null;
let dateSecond = 0; // counts the boundaries crossed, see touchDate()
function httpDate() {
  if (dateString === undefined) {
    refreshDate();
  }
  return dateString;
}

// Refreshes the string if the second has moved and reports which second that is. A block
// cache entry records it, because the generated date is the one field whose value is not
// fixed by the header object the entry is keyed on. Callers read the stamp *before* the
// value: a boundary can then only make the value newer than its stamp, which costs one
// re-encode, never one stale response.
function touchDate() {
  if (dateString === undefined) {
    refreshDate();
  }
  return dateSecond;
}

function refreshDate() {
  const now = new Date();
  dateString = now.toUTCString();
  dateSecond++;
  const ms = now.getMilliseconds();
  dateTimer = setTimeout(() => {
    dateString = undefined;
    dateTimer = null;
  }, ms === 0 ? 1000 : 1000 - ms);
  dateTimer.unref();
}

// -> { status, pseudo: [name, value, ...], fields: [name, value, never, ...] }
// The two lists are flat: a field line is three slots in one array, not an array of
// its own. The compat API parses the headers of every response, and a response with
// five headers used to allocate five arrays plus the two lists that hold them; the
// flat form allocates the two lists and nothing else, and the loops that read them
// need no iterator.
// `info` marks the informational (1xx) block that additionalHeaders() sends: node applies
// a different status rule and different error codes there, and it generates no date for it.
function parse(headers, sensitive, { trailer = false, sendDate = false, info = false } = {}) {
  if (headers === null || typeof headers !== 'object') {
    throw httpError('ERR_HTTP2_INVALID_HEADER_VALUE', 'headers must be an object');
  }
  const neverSet = sensitive || headers[KSensitiveHeaders];
  const neverList = Array.isArray(neverSet) && neverSet.length > 0
    ? new Set(neverSet.map((s) => lowerName(s)))
    : null;

  const pseudo = [];
  const fields = [];
  let status;
  let hasDate = false;
  let sawStatus = false;
  const keys = Object.keys(headers);
  for (let i = 0; i < keys.length; i++) {
    const key = lowerName(keys[i]);
    const raw = headers[keys[i]];
    // Only an absent value is skipped. An empty name is not: node rejects it with
    // ERR_INVALID_HTTP_TOKEN, and the token check below does the same, because the
    // pattern requires at least one character.
    if (raw === undefined) {
      continue;
    }

    if (key[0] === ':') {
      if (trailer) {
        throw httpError('ERR_HTTP2_PSEUDOHEADER_NOT_ALLOWED', 'cannot send trailers with pseudo-headers');
      }
      if (key !== ':status') {
        throw httpError('ERR_HTTP2_PSEUDOHEADER_NOT_ALLOWED', `cannot send ${key} in a response`);
      }
      const single = Array.isArray(raw) ? raw[0] : raw;
      // node coerces with `statusCode | 0` -- not Number(), and not a range check on the
      // string: '200.5' is 200, 1.9 is 1, 'abc' is 0. Both paths then add their own rule,
      // measured against a live node server:
      //   respond():            0 becomes 200, anything outside 200..599 throws
      //                         ERR_HTTP2_STATUS_INVALID (core.js:2700 and 2747), so 103
      //                         and 600 are refused here although this used to accept both;
      //   additionalHeaders():  101 is ERR_HTTP2_STATUS_101 and every other value outside
      //                         100..199 is ERR_HTTP2_INVALID_INFO_STATUS (core.js:3268-3274).
      // The value that goes on the wire is the coerced one, which is why :status is
      // emitted below, after the default and the range have been decided.
      const code = Number(single) | 0;
      if (info) {
        // 101 is Switching Protocols, which HTTP/2 forbids outright (RFC 9113 8.5): an
        // upgrade can only be expressed as a connection-specific field, and RFC 9113 8.2.2
        // removes those from this protocol.
        if (code === 101) {
          throw httpError('ERR_HTTP2_STATUS_101', 'HTTP status code 101 is disallowed');
        }
        if (code < 100 || code >= 200) {
          throw httpError('ERR_HTTP2_INVALID_INFO_STATUS',
            `invalid informational status code: ${single}`);
        }
      }
      status = code;
      sawStatus = true;
      continue;
    }

    if (!isFieldName(key)) {
      throw httpError('ERR_INVALID_HTTP_TOKEN', `${key} is an invalid header token`);
    }
    if (CONNECTION_SPECIFIC.has(key)) {
      throw httpError('ERR_HTTP2_INVALID_CONNECTION_HEADERS',
        `HTTP/1 Connection specific header is forbidden: ${key}`);
    }
    const never = neverList !== null && neverList.has(key);
    if (Array.isArray(raw)) {
      for (let j = 0; j < raw.length; j++) {
        fields.push(key, checked(key, raw[j]), never);
      }
      if (key === 'date') {
        hasDate = true;
      }
    } else {
      fields.push(key, checked(key, raw), never);
      if (key === 'date') {
        hasDate = true;
      }
    }
  }

  // A response without :status is not an error in node: the same function that adds the
  // date does `statusCode | 0 || HTTP_STATUS_OK`, so a missing (or NaN-ish) status
  // answers 200. Measured: respond({'x-a': 1}) arrives as :status 200 with a date, where
  // this used to throw. An informational block has no such default -- node validates a
  // :status only if the application put one there -- and trailers have no status at all.
  if (!trailer) {
    if (info) {
      if (sawStatus) {
        pseudo.push(':status', String(status));
      }
    } else {
      status = status || 200;
      if (status < 200 || status > 599) {
        throw httpError('ERR_HTTP2_STATUS_INVALID', `invalid status code: ${status}`);
      }
      pseudo.push(':status', String(status));
    }
  }
  // node's rule (`internal/http2/core.js:2702`) is: no date when the application already
  // set one, none when options.sendDate says so, and no status condition at all -- because
  // respond() cannot carry a 1xx in the first place (node rejects :status 103 with
  // ERR_HTTP2_STATUS_INVALID), and an informational block goes through additionalHeaders(),
  // which asks for no date. Appended last, because that is where node's respond() puts it
  // and header order is visible on the wire.
  if (!trailer && !info && sendDate && status >= 200 && !hasDate) {
    fields.push('date', httpDate());
  }
  if (status === 204 || status === 304 || (status >= 100 && status < 200)) {
    for (let i = 0; i < fields.length; i += 3) {
      if (fields[i] === 'content-length' && fields[i + 1] !== '0') {
        throw httpError('ERR_HTTP2_PAYLOAD_FORBIDDEN',
          `response with ${status} status must not declare a payload`);
      }
    }
  }
  return { status, pseudo, fields };
}

function checked(key, value) {
  // String() on a string returns the very same string, so the call is only worth
  // its price for the other types: the check below is the expensive half.
  const v = typeof value === 'string' ? value : String(value);
  if (badValue.test(v)) {
    throw httpError('ERR_HTTP2_INVALID_HEADER_VALUE', `invalid value "${v}" for header "${key}"`);
  }
  return v;
}

// The HPACK bytes for one HEADERS payload. The encoder carries a dynamic table, which
// is per-connection state, so the block cache lives on the session and is stamped with
// the table's epoch: cached bytes may name an index that a later insertion moved.
// The returned Buffer is a view into a shared scratch block: valid until the
// next call to block(), which is why every caller copies it immediately.
function block(headers, sensitive, opts, session) {
  const sendDate = !!(opts && opts.sendDate);
  const enc = session._encoder;
  // The second the generated date belongs to, or -1 when the block carries none. A
  // `date` block is cached for its own second and no longer: the value cannot change
  // inside it, and across a boundary the entry has to be rebuilt rather than repeated.
  const stamp = sendDate ? touchDate() : -1;
  // A block carrying a pending size update is never cached: the announcement has to reach
  // the peer with the next block rather than be swallowed by a cache hit (RFC 7541 6.3).
  const cacheable = Object.isFrozen(headers) && enc.pendingSize === null;
  if (cacheable) {
    const hit = session._blocks.get(headers);
    if (hit !== undefined && hit.epoch === enc.epoch && hit.stamp === stamp) {
      return hit.bytes;
    }
  }

  const update = enc.takeSizeUpdate();
  const parsed = parse(headers, sensitive,
    { trailer: !!(opts && opts.trailer), sendDate, info: !!(opts && opts.info) });

  // A shape reaches its final form over two or three responses, the way any HPACK
  // encoder warms up: the first sends a line as a literal and remembers it, the second
  // inserts it, the third references it. Only the last form is stable enough to cache,
  // so a build that moved the table (`epoch`) or left a line pending promotion
  // (`unsettled`) is re-encoded next response instead. Building twice in one call is
  // not an option: the discarded bytes were the ones that performed the insertion, and
  // dropping them would leave our table ahead of the peer's -- a desynchronisation the
  // peer can only report as COMPRESSION_ERROR.
  const epoch = enc.epoch;
  // The announcement goes in front of the block, in the same buffer: the old form was
  // Buffer.concat([update, out]) -- a second allocation, a copy of the whole block and a
  // temporary array, on the one path where the peer changed the table size. The client
  // does the same thing in its own _encodeRequest (lib/client-session.js).
  const at = update === null ? 0 : update.length;
  const end = encodeAt(parsed, enc, at);
  if (at !== 0) {
    update.copy(scratch, 0);
  }
  if (cacheable && enc.epoch === epoch && !enc.unsettled) {
    // a cached block has to outlive the scratch, so only the cached path copies --
    // allocUnsafe, not Buffer.from(subarray): at the sizes a response block has the
    // pool copy is the cheaper allocation (OPTIMIZATION.md item 18)
    const bytes = Buffer.allocUnsafe(end - at);
    scratch.copy(bytes, 0, at, end);
    session._blocks.set(headers, { bytes, epoch: enc.epoch, stamp });
  }
  return scratch.subarray(0, end);
}

// One pass over the parsed fields. `enc.unsettled` is reported back to block(): some
// line was seen for the first time and would be encoded differently next time.
function encode(parsed, enc) {
  return scratch.subarray(0, encodeAt(parsed, enc, 0));
}

// The same pass, written at `at` instead of at the front of the scratch -- the place a
// size update's octets took, so that the announcement and the block are one buffer and
// one write. Returns the offset the block ends at.
function encodeAt(parsed, enc, at) {
  enc.unsettled = false;
  const pseudo = parsed.pseudo;
  const fields = parsed.fields;
  let cap = 16 + at;
  for (let i = 0; i < pseudo.length; i += 2) {
    cap += Encoder.maxBytesOf(pseudo[i], pseudo[i + 1]);
  }
  for (let i = 0; i < fields.length; i += 3) {
    cap += Encoder.maxBytesOf(fields[i], fields[i + 1]);
  }
  if (scratch.length < cap) {
    scratch = Buffer.allocUnsafe(cap);
  }
  let off = at;
  for (let i = 0; i < pseudo.length; i += 2) {
    off = enc.write(scratch, off, pseudo[i], pseudo[i + 1], false);
  }
  for (let i = 0; i < fields.length; i += 3) {
    off = enc.write(scratch, off, fields[i], fields[i + 1], fields[i + 2]);
  }
  return off;
}

module.exports = { block, encode, parse, nullProto, isFieldName, trimName, lowerName,
  nameCacheSize, hasBadValue, CONNECTION_SPECIFIC, KSensitiveHeaders };
