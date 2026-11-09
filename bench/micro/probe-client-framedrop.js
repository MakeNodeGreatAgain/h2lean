// Which DATA frame disappears? Same raw server as probe-client-body-loss.js (8 concurrent
// streams, each response one write of HEADERS + 16 x 16 384 DATA), but the client is
// wrapped: every frame the session dispatches is logged with its sid/len/flags, and every
// chunk the application receives with its sid and running length. At the end the two
// ledgers are compared per stream, so the answer is a frame, not a feeling.
//
//   node bench/micro/probe-client-framedrop.js [streams] [rounds]
const net = require('node:net');
const f = require('../../lib/frames');
const { Encoder } = require('../../lib/hpack');
const { ClientHttp2Session } = require('../../lib/client-session');

const STREAMS = Number(process.argv[2] || 8);
const ROUNDS = Number(process.argv[3] || 5);
const PIECE = 16384, NFRAMES = 16, BODY = PIECE * NFRAMES;
const body = Buffer.alloc(BODY);
for (let i = 0; i < BODY; i++) {
  body[i] = (i >> 12) % 251;
}

function frame(type, flags, sid, payload) {
  const len = payload ? payload.length : 0;
  const buf = Buffer.allocUnsafe(f.HEADER_LEN + len);
  f.writeHeader(buf, 0, len, type, flags, sid);
  if (len) {
    payload.copy(buf, f.HEADER_LEN);
  }
  return buf;
}

const srv = net.createServer((sock) => {
  let acc = Buffer.alloc(0), preface = 24;
  sock.write(frame(f.SETTINGS, 0, 0, null));
  sock.on('data', (d) => {
    acc = Buffer.concat([acc, d]);
    if (preface > 0) {
      if (acc.length < preface) {
        return;
      } acc = acc.subarray(preface); preface = 0;
    }
    for (;;) {
      if (acc.length < f.HEADER_LEN) {
        return;
      }
      f.readHeader(acc, 0);
      const len = f.FH_LEN, type = f.FH_TYPE;
      if (acc.length < f.HEADER_LEN + len) {
        return;
      }
      if (type === f.HEADERS) {
        const sid = f.FH_SID;
        const enc = new Encoder(4096, 4096);
        const blk = Buffer.alloc(64);
        const n = enc.write(blk, 0, ':status', '200', true);
        const out = [frame(f.HEADERS, f.F_END_HEADERS, sid, blk.subarray(0, n))];
        for (let off = 0; off < BODY; off += PIECE) {
          out.push(frame(f.DATA, off + PIECE >= BODY ? f.F_END_STREAM : 0, sid,
            body.subarray(off, off + PIECE)));
        }
        sock.write(Buffer.concat(out));
      }
      acc = acc.subarray(f.HEADER_LEN + len);
    }
  });
});

// ledger: per sid, what the session dispatched and what the application saw
const dispatched = new Map(), received = new Map(), opened = new Map();
let trace = [];

const proto = ClientHttp2Session.prototype;
const realHandle = proto._handle;
proto._handle = function (type, flags, sid, buf, start, len) {
  trace.push({ type, flags, sid, len });
  if (type === f.DATA) {
    const list = dispatched.get(sid) || [];
    list.push({ len, es: (flags & f.F_END_STREAM) !== 0 });
    dispatched.set(sid, list);
  }
  return realHandle.call(this, type, flags, sid, buf, start, len);
};

srv.listen(0, '127.0.0.1', () => {
  const port = srv.address().port;
  let round = 0, bad = 0;

  const once = () => new Promise((res) => {
    dispatched.clear(); received.clear(); opened.clear(); trace = [];
    const sess = new ClientHttp2Session(require('node:net').connect(port, '127.0.0.1'),
      {}, 'http', '127.0.0.1');
    sess.on('error', (e) => console.log(`  round ${round}: session error ${e.code || ''} ${e.message}`));
    const pending = { left: STREAMS };
    const finish = () => {
      try {
        sess.destroy();
      } catch { /* destroy after a lost socket is not a finding */ } res();
    };
    const timer = setTimeout(() => {
      console.log(`  round ${round}: TIMEOUT, ${pending.left} streams unfinished`);
      report(); bad++;
      console.log(`  timeouts so far: ${bad}`); finish();
    }, 5000);
    for (let s = 0; s < STREAMS; s++) {
      const st = sess.request({ ':method': 'GET', ':path': '/' });
      const sid = st.id;
      opened.set(sid, true);
      let bytes = 0, firstErr = null;
      st.on('data', (d) => {
        for (let i = 0; i < d.length; i++) {
          if (d[i] !== ((bytes + i) >> 12) % 251) {
            console.log(`  round ${round} sid ${sid}: WRONG BYTE at ${bytes + i}`);
            firstErr = firstErr || 'wrong'; break;
          }
        }
        bytes += d.length;
        received.set(sid, (received.get(sid) || 0) + d.length);
      });
      st.on('error', (e) => {
        firstErr = e.code || e.message;
      });
      st.on('end', () => {
        if (bytes !== BODY) {
          console.log(`  round ${round} sid ${sid}: SHORT ${bytes}${firstErr ? ' ' + firstErr : ''}`);
        }
        if (--pending.left === 0) {
          clearTimeout(timer); report(); finish();
        }
      });
    }

    function report() {
      for (const sid of opened.keys()) {
        const d = dispatched.get(sid) || [];
        const sum = d.reduce((a, x) => a + x.len, 0);
        const got = received.get(sid) || 0;
        const esFrames = d.filter((x) => x.es).length;
        if (sum !== BODY || got !== sum || d.length !== NFRAMES || esFrames !== 1) {
          badReport(sid, d, sum, got, esFrames);
        }
      }
    }
  });

  function badReport(sid, d, sum, got, esFrames) {
    console.log(`  sid ${sid}: frames=${d.length} (want ${NFRAMES}) dispatched=${sum} ` +
      `delivered=${got} END_STREAM count=${esFrames}`);
    const lens = {};
    for (const x of d) {
      lens[`${x.len}${x.es ? '/ES' : ''}`] = (lens[`${x.len}${x.es ? '/ES' : ''}`] || 0) + 1;
    }
    console.log(`     ${JSON.stringify(lens)}`);
  }

  (async () => {
    for (round = 1; round <= ROUNDS; round++) {
      await once();
    }
    console.log(`${ROUNDS} rounds x ${STREAMS} streams -> done`);
    srv.close();
    process.exit(0);
  })();
});
