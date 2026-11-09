// Pure-JS h2c responder — upper-bound experiment vs native node:http2.
//
// Scope is deliberately narrow but protocol-honest for the benchmark client:
//   * prior-knowledge h2c (no TLS), same as http2.createServer() plaintext
//   * real frame state machine: SETTINGS(+ACK), PING(ACK), HEADERS, DATA,
//     WINDOW_UPDATE, PRIORITY, RST_STREAM, GOAWAY
//   * response HPACK block built ONCE as bytes (static-indexed :status +
//     literal fields), reused for every stream — zero per-request encoding
//   * header block payload is consumed but values are not materialized into
//     strings (see README notes: that is the part a real server must add)
//   * write coalescing: all frames produced while draining one socket read
//     batch go out in ONE socket.write()
//   * WINDOW_UPDATE credit is coalesced per read batch as well
const net = require('node:net');

const PORT = Number(process.env.PORT || 8443);
const DURATION_MS = Number(process.env.DURATION || 10000);
const BODY = Number(process.env.BODY || 0);

const FLAG_ACK = 0x1, FLAG_END_HEADERS = 0x4, FLAG_END_STREAM = 0x1;

const PREFACE_LEN = 24; // "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

// ---------- HPACK encoder helpers (literal-only, no huffman, no dynamic table)
function litHeader(name, value) {
  const n = Buffer.from(name, 'latin1');
  const v = Buffer.from(String(value), 'latin1');
  // 0x00 = literal without indexing, new name
  return Buffer.concat([
    Buffer.from([0x00, n.length]), n,
    Buffer.from([v.length]), v
  ]);
}

function buildResponseBlock() {
  const parts = [
    Buffer.from([0x88]), // indexed: :status: 200 (static table entry 8)
    litHeader('content-type', 'text/plain'),
    litHeader('server', 'prof'),
    litHeader('x-trace', '0123456789abcdef0123456789abcdef')
  ];
  if (BODY > 0) {
    parts.push(litHeader('content-length', BODY));
  }
  return Buffer.concat(parts);
}

const RESP_BLOCK = buildResponseBlock();
const RESP_BODY = BODY > 0 ? Buffer.alloc(BODY, 0x61) : null;

// our SETTINGS: big windows so flow control never throttles the benchmark
// (MAX_FRAME_SIZE must stay within 16384..16777215 -> 1<<20)
const OUR_SETTINGS = Buffer.alloc(12);
OUR_SETTINGS.writeUInt16BE(0x1, 0); OUR_SETTINGS.writeUInt32BE(1 << 20, 2); // INITIAL_WINDOW_SIZE
OUR_SETTINGS.writeUInt16BE(0x5, 6); OUR_SETTINGS.writeUInt32BE(1 << 20, 8); // MAX_FRAME_SIZE

class Conn {
  constructor(socket) {
    this.s = socket;
    this.preface = 0; // preface bytes seen
    this.out = []; // pending frames for this read batch
    this.outLen = 0;
    this.stash = undefined; // leftover partial frame
    this.wuConn = 0; // credit to return for DATA we received
    this.sendWin = 65536; // our connection-level send window (RFC 9113 6.9.2)
    this.deferred = []; // frames waiting for send-window credit
    this.handled = 0;

    socket.setNoDelay(true);
    // our SETTINGS first (frame header + payload)
    this.push(4, 0, 0, OUR_SETTINGS);
    this.flush();

    socket.on('data', (d) => this.onData(d));
    socket.on('error', () => {});
    socket.on('end', () => this.flush());
  }

  push(type, flags, streamId, payload) {
    const len = payload ? payload.length : 0;
    const h = Buffer.allocUnsafe(9);
    h[0] = len >> 16; h[1] = len >> 8; h[2] = len;
    h[3] = type; h[4] = flags;
    h.writeUInt32BE(streamId >>> 0, 5); // clears high bit
    this.out.push(h);
    if (len) {
      this.out.push(payload);
    }
    this.outLen += 9 + len;
  }

  flush() {
    if (this.outLen === 0) {
      return;
    }
    if (this.out.length === 1) {
      this.s.write(this.out[0]);
    } else {
      this.s.write(Buffer.concat(this.out, this.outLen));
    }
    this.out.length = 0;
    this.outLen = 0;
  }

  onData(d) {
    let i = 0;
    if (this.preface < PREFACE_LEN) {
      const take = Math.min(PREFACE_LEN - this.preface, d.length);
      this.preface += take;
      i = take;
      if (i >= d.length) {
        return;
      }
    }
    // fast path: parse frames straight out of the incoming chunk
    for (;;) {
      if (d.length - i < 9) {
        this.remember(d.subarray(i)); break;
      }
      const len = (d[i] << 16) | (d[i + 1] << 8) | d[i + 2];
      if (d.length - i - 9 < len) {
        this.remember(d.subarray(i)); break;
      }
      const type = d[i + 3];
      const flags = d[i + 4];
      const sid = d.readUInt32BE(i + 5) & 0x7fffffff;
      this.frame(type, flags, sid, i + 9, len, d);
      i += 9 + len;
      if (i >= d.length) {
        break;
      }
    }
    // coalesced window updates for everything we sent in this batch
    if (this.wuConn > 0) {
      const b = Buffer.allocUnsafe(4);
      b.writeUInt32BE(this.wuConn, 0);
      this.push(8, 0, 0, b);
      this.wuConn = 0;
    }
    this.flush();
  }

  remember(rest) {
    // rare in the benchmark (frames arrive whole); keep a compact leftover
    if (this.stash === undefined) {
      this.stash = rest;
    } else {
      this.stash = Buffer.concat([this.stash, rest]);
    }
    if (this.stash.length >= 9) {
      const len = (this.stash[0] << 16) | (this.stash[1] << 8) | this.stash[2];
      if (this.stash.length >= 9 + len) {
        const s = this.stash;
        this.stash = undefined;
        this.onData(s);
      }
    }
  }

  frame(type, flags, sid, pos, len, d) {
    switch (type) {
      case 4: // SETTINGS
        if (flags & FLAG_ACK) {
          return;
        }
        this.push(4, FLAG_ACK, 0, null);
        return;
      case 6: // PING
        if (flags & FLAG_ACK) {
          return;
        }
        this.push(6, FLAG_ACK, 0, d.subarray(pos, pos + len));
        return;
      case 1: // HEADERS
        if (READ) {
          // Shape cache keyed on the raw HPACK block bytes: one small Latin1
          // string + one Map lookup per request, instead of ~2 V8 strings per
          // header field + an array + an object (what node:http2 builds).
          const key = d.latin1Slice(pos, pos + len);
          let e = shapeCache.get(key);
          if (e === undefined) {
            e = { fields: len, path: key }; // miss -> would be a real HPACK decode
            shapeCache.set(key, e);
            shapeMisses++;
          } else {
            shapeHits++;
          }
          this.lastShape = e; // stands in for the header object a handler would read
        }
        this.respond(sid);
        return;
      case 0: // DATA
        this.wuConn += len;
        if (flags & FLAG_END_STREAM) {
          this.respond(sid);
        }
        return;
      case 8: // WINDOW_UPDATE
        if (sid === 0) {
          this.sendWin += d.readUInt32BE(pos) & 0x7fffffff;
          this.drainDeferred();
        }
        break;

      case 2: case 3: case 7: case 0xa: // PRIORITY / RST / GOAWAY / CONTINUATION
      default:
        // unknown frame type: ignore per RFC 9113 §6.1

    }
  }

  drainDeferred() {
    while (this.deferred.length && this.sendWin >= this.deferred[0].body.length) {
      const f = this.deferred.shift();
      this.sendWin -= f.body.length;
      this.push(0, FLAG_END_STREAM, f.sid, f.body);
    }
  }

  respond(sid) {
    this.handled++;
    globalReqCount++;
    if (RESP_BODY) {
      this.push(1, FLAG_END_HEADERS, sid, RESP_BLOCK);
      if (this.sendWin >= RESP_BODY.length) {
        this.sendWin -= RESP_BODY.length;
        this.push(0, FLAG_END_STREAM, sid, RESP_BODY);
      } else {
        this.deferred.push({ sid, body: RESP_BODY });
      }
    } else {
      this.push(1, FLAG_END_HEADERS | FLAG_END_STREAM, sid, RESP_BLOCK);
    }
  }
}

let globalReqCount = 0;
const READ = process.env.READ === '1';
const shapeCache = new Map();
let shapeHits = 0, shapeMisses = 0;

const cpu0 = process.cpuUsage();
const t0 = Number(process.hrtime.bigint());
const server = net.createServer((socket) => {
  const c = new Conn(socket);
  socket.on('close', () => {
    conns.delete(c);
  });
  conns.add(c);
});
const conns = new Set();

server.listen(PORT, '127.0.0.1', () => {
  console.log('lite server listening', PORT);
  setTimeout(() => {
    const cpu = process.cpuUsage(cpu0);
    const wall = (Number(process.hrtime.bigint()) - t0) / 1e6;
    const cpuMs = (cpu.user + cpu.system) / 1000;
    console.log(`[lite-h2]    req=${globalReqCount} wall=${wall.toFixed(0)}ms cpu=${cpuMs.toFixed(0)}ms ` +
                `rps=${Math.round(globalReqCount / (wall / 1000))} ` +
                `cpuPerReq=${(cpuMs * 1000 / (globalReqCount || 1)).toFixed(2)}us ` +
                `busy=${(cpuMs * 100 / wall).toFixed(1)}% ` +
                `read=${READ ? `hits=${shapeHits} misses=${shapeMisses} cachedShapes=${shapeCache.size}` : 'off'}`);
    process.exit(0);
  }, DURATION_MS);
});
