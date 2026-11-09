// What goes into the first SETTINGS frame of a session, and what goes into the one that
// follows an updateSettings() -- node:http2 against h2lite, on a raw socket so the frame
// is read as bytes and not as an object.
//
//   node bench/micro/probe-settings-frame.js
'use strict';
const net = require('node:net');
const h2lite = require('../../index');
const nodeHttp2 = require('node:http2');

const f = require('../../lib/frames');

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

function parseSettings(body) {
  const out = [];
  for (let i = 0; i + 6 <= body.length; i += 6) {
    const id = body.readUInt16BE(i);
    const value = body.readUInt32BE(i + 2);
    out.push(`${id}=${value}`);
  }
  return out.join(',');
}

// Read the frames the server sends until the first SETTINGS arrives.
function readSettings(port, after) {
  return new Promise((resolve) => {
    const sock = net.connect(port, '127.0.0.1', () => {
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0))]));
    });
    let buf = Buffer.alloc(0);
    const seen = [];
    const done = () => {
      sock.removeAllListeners('data');
      sock.destroy();
      resolve(seen);
    };
    sock.on('data', (d) => {
      buf = Buffer.concat([buf, d]);
      for (;;) {
        if (buf.length < f.HEADER_LEN) {
          return;
        }
        const len = buf.readUIntBE(0, 3);
        const type = buf[3]; // length(3) type(1) flags(1) streamId(4)
        if (buf.length < f.HEADER_LEN + len) {
          return;
        }
        const body = buf.subarray(f.HEADER_LEN, f.HEADER_LEN + len);
        buf = buf.subarray(f.HEADER_LEN + len);
        if (type === f.SETTINGS) {
          seen.push(parseSettings(body));
          if (after) {
            after(); continue;
          }
          return done();
        }
      }
    });
    sock.on('error', () => done());
    setTimeout(done, 1500);
  });
}

async function run(name, impl, options, update) {
  const srv = impl.createServer(options);
  srv.on('error', () => {});
  const port = await new Promise((r) => srv.listen(0, '127.0.0.1', () => r(srv.address().port)));
  const seen = await readSettings(port, null);
  if (update) {
    // node's server.updateSettings() only merges into the server's own settings -- it does not
    // walk the live sessions, so the effect shows up on the NEXT connection.
    srv.updateSettings(update);
    seen.push(...(await readSettings(port, null)));
  }
  srv.close();
  await new Promise((r) => srv.close(r));
  console.log(`${name}: ${JSON.stringify(seen)}`);
  return seen;
}

(async () => {
  console.log('--- no options ---');
  await run('node  ', nodeHttp2, undefined, null);
  await run('h2lite', h2lite, undefined, null);

  console.log('--- settings: {maxConcurrentStreams: 100, maxFrameSize: 32768} ---');
  await run('node  ', nodeHttp2, { settings: { maxConcurrentStreams: 100, maxFrameSize: 32768 } }, null);
  await run('h2lite', h2lite, { settings: { maxConcurrentStreams: 100, maxFrameSize: 32768 } }, null);

  console.log('--- after server.updateSettings({initialWindowSize: 65536}) ---');
  await run('node  ', nodeHttp2, { settings: { maxConcurrentStreams: 100 } }, { initialWindowSize: 65536 });
  await run('h2lite', h2lite, { settings: { maxConcurrentStreams: 100 } }, { initialWindowSize: 65536 });
})();
