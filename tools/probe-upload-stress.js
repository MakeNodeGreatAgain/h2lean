// Stress probe for concurrent uploads: several connections, several streams per
// connection, each request carrying a body larger than one flow-control window.
// Reports every client-visible failure and the server-side reason for it.
//   PLAIN=1 node tools/probe-upload-stress.js [bodyBytes] [seconds]
'use strict';
const http2 = require('node:http2');
const h2lite = require('../index');
const certificates = require('../test/fixtures-tls');

const PLAIN = process.env.PLAIN === '1';
const SIZE = Number(process.argv[2] || 32768);
const SECONDS = Number(process.argv[3] || 4);
const CONNS = Number(process.argv[4] || 8);
const STREAMS = Number(process.argv[5] || 4);
const BODY = Buffer.alloc(SIZE, 0x78);

const failures = [];
const srvEvents = [];

function handler(req, res) {
  let got = 0;
  req.on('data', (d) => {
    got += d.length;
  });
  req.on('end', () => {
    if (got !== SIZE) {
      srvEvents.push(`short body ${got}/${SIZE}`);
    }
    res.writeHead(200);
    res.end('ok');
  });
  req.on('error', (e) => srvEvents.push(`req error ${e.code}`));
}

const srv = PLAIN ? h2lite.createServer(handler)
  : h2lite.createSecureServer(certificates(), handler);
srv.on('sessionError', (e) => srvEvents.push(`sessionError ${e.code || e.message}`));
srv.on('session', (s) => {
  s.on('stream', (st) => {
    st.on('error', (e) => srvEvents.push(`stream ${st.id} error ${e.code || ''} ${e.message}`));
  });
});

srv.listen(0, '127.0.0.1', () => {
  const url = (PLAIN ? 'http' : 'https') + `://127.0.0.1:${srv.address().port}`;
  const stopAt = Date.now() + SECONDS * 1000;
  let ok = 0;
  let closed = false;
  let pending = CONNS * STREAMS; // loops still running
  const clients = [];

  for (let c = 0; c < CONNS; c++) {
    const client = http2.connect(url, PLAIN ? {} : { rejectUnauthorized: false });
    client.on('error', (e) => failures.push(`session ${c} ${e.code}`));
    clients.push(client);
    for (let m = 0; m < STREAMS; m++) {
      const loop = () => {
        if (closed) {
          return;
        }
        if (Date.now() >= stopAt) {
          if (--pending === 0) {
            report();
          }
          return;
        }
        const req = client.request({ ':method': 'POST', ':path': '/u' });
        req.on('response', () => req.resume());
        req.on('error', (e) => {
          failures.push(`req ${e.code}${e.errno !== undefined ? ' errno=' + e.errno : ''}`);
          loop();
        });
        req.on('end', () => {
          ok++; loop();
        });
        req.end(BODY);
      };
      loop();
    }
  }

  function report() {
    closed = true;
    console.log(`bodies=${SIZE}B conns=${CONNS} streams=${STREAMS} ok=${ok}`);
    console.log(`client failures: ${failures.length ? JSON.stringify(failures.slice(0, 8)) : 'none'}`);
    console.log(`server events:   ${srvEvents.length ? JSON.stringify(srvEvents.slice(0, 8)) : 'none'}`);
    for (const cl of clients) {
      cl.destroy();
    }
    srv.close();
    setTimeout(() => process.exit(0), 200);
  }
});
