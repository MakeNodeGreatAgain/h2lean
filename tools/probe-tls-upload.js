// Focused repro probe: N sequential uploads against h2lean, over h2s or h2c, with
// the server's outbound frames decoded and every reset/goaway logged.
//   PLAIN=1 node tools/probe-tls-upload.js [bodyBytes] [rounds]
'use strict';
const http2 = require('node:http2');
const h2lean = require('../index');
const f = require('../lib/frames');
const certificates = require('../test/fixtures-tls');

const PLAIN = process.env.PLAIN === '1';

const SIZE = Number(process.argv[2] || 32768);
const ROUNDS = Number(process.argv[3] || 1);
const BODY = Buffer.alloc(SIZE, 0x78);
const NAMES = { 0: 'DATA', 1: 'HEADERS', 3: 'RST_STREAM', 4: 'SETTINGS', 6: 'PING',
  7: 'GOAWAY', 8: 'WINDOW_UPDATE', 9: 'CONTINUATION' };

const srv = PLAIN
  ? h2lean.createServer((req, res) => {
    let got = 0;
    req.on('data', (d) => {
      got += d.length;
    });
    req.on('end', () => {
      console.log(`  [srv] complete ${got}`);
      res.writeHead(200, { 'x-got': String(got) });
      res.end('ok');
    });
    req.on('error', (e) => console.log('  [srv] req error', e.code));
  })
  : h2lean.createSecureServer(certificates(), (req, res) => {
    let got = 0;
    req.on('data', (d) => {
      got += d.length;
    });
    req.on('end', () => {
      console.log(`  [srv] complete ${got}`);
      res.writeHead(200, { 'x-got': String(got) });
      res.end('ok');
    });
    req.on('error', (e) => console.log('  [srv] req error', e.code));
  });
srv.on('session', (s) => {
  const ow = s.socket.write.bind(s.socket);
  s.socket.write = (b, cb) => {
    // decode what we are about to put on the wire
    let off = 0;
    while (off + 9 <= b.length) {
      const len = (b[off] << 16) | (b[off + 1] << 8) | b[off + 2];
      const type = b[off + 3], flags = b[off + 4], sid = b.readUInt32BE(off + 5) & 0x7fffffff;
      let note = '';
      if (type === f.WINDOW_UPDATE) {
        note = ' inc=' + b.readUInt32BE(off + 9);
      }
      if (type === f.GOAWAY) {
        note = ' err=0x' + b.readUInt32BE(off + 13).toString(16);
      }
      console.log(`  [srv->] ${NAMES[type] || type} len=${len} flags=${flags} sid=${sid}${note}`);
      off += 9 + len;
    }
    const r = ow(b, cb);
    if (r === false) {
      console.log('  [srv->] write returned FALSE');
    }
    return r;
  };
  s.socket.on('drain', () => console.log('  [srv->] DRAIN'));
  const odp = s._dispatch.bind(s);
  s._dispatch = (buf, start, len, type, flags, sid) => {
    console.log(`  [srv<-] ${NAMES[type] || type} len=${len} flags=${flags} sid=${sid}`);
    return odp(buf, start, len, type, flags, sid);
  };
  s.on('stream', (st) => st.once('close', () => {
    console.log(`  [srv] stream ${st.id} closed, remoteClose=${st.state.remoteClose} localClose=${st.state.localClose}`);
  }));
  s.on('error', (e) => console.log('  [srv] session error', e.code || e.message));
});

srv.listen(0, '127.0.0.1', () => {
  const url = (PLAIN ? 'http' : 'https') + `://127.0.0.1:${srv.address().port}`;
  const client = http2.connect(url, PLAIN ? {} : { rejectUnauthorized: false });
  client.on('error', (e) => console.log('  [cli] session error', e.code, e.message));
  let i = 0;
  function next() {
    if (i++ >= ROUNDS) {
      console.log('ALL DONE'); process.exit(0);
    }
    const req = client.request({ ':method': 'POST', ':path': '/u' + i });
    let done = false;
    const finish = (what) => {
      if (done) {
        return;
      }
      done = true;
      console.log(`  [cli] request ${i}: ${what}`);
      next();
    };
    req.on('response', (h) => {
      req.resume(); finish('response ' + JSON.stringify(h));
    });
    req.on('end', () => finish('end'));
    req.on('error', (e) => finish('error ' + e.code + ' errno=' + e.errno));
    setTimeout(() => finish('TIMEOUT'), 3000);
    req.end(BODY);
  }
  next();
});
