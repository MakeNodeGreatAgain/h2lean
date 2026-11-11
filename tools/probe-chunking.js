// How many 'data' events does a peer see when a DATA frame straddles two socket
// reads? Both directions, both stacks.
//
//   node tools/probe-chunking.js
//
//  * download: a raw server answers with a DATA frame whose payload is cut in two,
//    and the client counts the chunks node:http2 and h2lean deliver;
//  * upload: a raw client sends a request whose DATA payload is cut in two, and
//    the server counts the chunks node:http2 and h2lean hand to the handler.
//
// The verdict decides what a straddling payload may be handed over as. Node
// delivers it in the pieces it arrived in -- one per read -- so a frame-atomic
// delivery (one chunk per frame) would be a divergence from node in the one place
// the application can see it, and a payload that is not contiguous cannot be a
// single view either: the zero-copy form is one view per read.
//
// Measured on node v26.12.1: node's server and node's client both deliver
// [5, 16379] for a payload cut in two -- two chunks, one per read -- and h2lean
// delivers the same after the change (before it, [16384]).
//
// Two details in the raw server are load-bearing rather than decorative:
//
//  * its SETTINGS go out as soon as the connection is up, not after the request.
//    h2lean's client holds its first request back until the peer's SETTINGS have
//    been applied (lib/client-session.js, _enqueue), so a server that answers
//    SETTINGS only once it has seen the request deadlocks against it. Node's
//    client does not wait, but a real server sends SETTINGS first anyway.
//  * it answers once, when the request's HEADERS frame is complete. Answering on
//    the preface alone would race the request against the response.
'use strict';

const net = require('node:net');
const nodeHttp2 = require('node:http2');
const h2lean = require('../index');
const f = require('../lib/frames');

const PAYLOAD = 16384;

function frame(type, flags, sid, payload) {
  const head = Buffer.alloc(f.HEADER_LEN);
  f.writeHeader(head, 0, payload.length, type, flags, sid);
  return Buffer.concat([head, payload]);
}

// A raw server that answers with one DATA frame whose payload arrives in two writes.
function rawServer() {
  return new Promise((resolve) => {
    const srv = net.createServer((sock) => {
      sock.on('error', () => {});
      let buf = Buffer.alloc(0);
      let answered = false;
      sock.write(frame(f.SETTINGS, 0, 0, Buffer.alloc(0)));
      const respond = () => {
        const head = frame(f.HEADERS, f.F_END_HEADERS, 1, Buffer.from([0x88])); // :status 200
        const body = Buffer.alloc(PAYLOAD);
        for (let i = 0; i < PAYLOAD; i++) {
          body[i] = i & 0xff;
        }
        const data = frame(f.DATA, f.F_END_STREAM, 1, body);
        sock.write(head);
        sock.write(data.subarray(0, f.HEADER_LEN + 5)); // the read ends inside the payload
        setTimeout(() => sock.write(data.subarray(f.HEADER_LEN + 5)), 20);
      };
      sock.on('data', (d) => {
        if (answered) {
          return;
        }
        buf = Buffer.concat([buf, d]);
        // The preface is 24 octets; after it come frames.
        let off = f.PREFACE_LEN;
        while (off + f.HEADER_LEN <= buf.length) {
          const len = (buf[off] << 16) | (buf[off + 1] << 8) | buf[off + 2];
          const type = buf[off + 3];
          const flags = buf[off + 4];
          if (off + f.HEADER_LEN + len > buf.length) {
            break;
          }
          off += f.HEADER_LEN + len;
          if (type === f.HEADERS && (flags & f.F_END_HEADERS)) {
            answered = true;
            respond();
            return;
          }
        }
      });
    });
    srv.listen(0, '127.0.0.1', () => resolve(srv));
  });
}

// A raw client that sends one request whose DATA payload arrives in two writes.
function rawClientUpload(port) {
  return new Promise((resolve) => {
    const sock = net.connect(port, '127.0.0.1', () => {
      const block = Buffer.concat([Buffer.from([0x83, 0x84, 0x86]), Buffer.from(
        [0x00, 10, ...Buffer.from(':authority', 'latin1'), 9,
          ...Buffer.from('localhost', 'latin1')])]);
      const body = Buffer.alloc(PAYLOAD);
      for (let i = 0; i < PAYLOAD; i++) {
        body[i] = i & 0xff;
      }
      const data = frame(f.DATA, f.F_END_STREAM, 1, body);
      sock.write(Buffer.concat([f.PREFACE, frame(f.SETTINGS, 0, 0, Buffer.alloc(0)),
        frame(f.HEADERS, f.F_END_HEADERS, 1, block)]));
      sock.write(data.subarray(0, f.HEADER_LEN + 5));
      setTimeout(() => sock.write(data.subarray(f.HEADER_LEN + 5)), 20);
    });
    sock.on('error', () => {});
    setTimeout(() => {
      sock.destroy(); resolve();
    }, 400);
  });
}

function downloadNode(port) {
  return new Promise((resolve, reject) => {
    const sess = nodeHttp2.connect(`http://127.0.0.1:${port}`);
    const timer = setTimeout(() => {
      sess.destroy(); reject(new Error('node download timeout'));
    }, 2000);
    sess.on('error', reject);
    const req = sess.request({ ':path': '/' });
    const chunks = [];
    req.on('data', (c) => chunks.push(c.length));
    req.on('end', () => {
      clearTimeout(timer); sess.destroy(); resolve(chunks);
    });
    req.on('error', reject);
  });
}

function downloadH2lean(port) {
  return new Promise((resolve, reject) => {
    const sess = h2lean.connect(`http://127.0.0.1:${port}`);
    const timer = setTimeout(() => {
      sess.destroy(); reject(new Error('h2lean download timeout'));
    }, 2000);
    sess.on('error', reject);
    const req = sess.request({ ':path': '/' });
    const chunks = [];
    req.on('data', (c) => chunks.push(c.length));
    req.on('end', () => {
      clearTimeout(timer); sess.destroy(); resolve(chunks);
    });
    req.on('error', reject);
  });
}

// The upload direction: the server counts the chunks its handler receives.
function uploadServer(impl) {
  return new Promise((resolve) => {
    const srv = impl.createServer();
    const chunks = [];
    srv.on('stream', (stream) => {
      stream.on('data', (c) => chunks.push(c.length));
      stream.on('end', () => {
        stream.respond({ ':status': 200 }, { endStream: true });
      });
    });
    srv.on('error', () => {});
    srv.listen(0, '127.0.0.1', () => resolve({ srv, chunks }));
  });
}

(async () => {
  const raw = await rawServer();
  const rawPort = raw.address().port;
  for (let i = 0; i < 3; i++) {
    const node = await downloadNode(rawPort);
    const ours = await downloadH2lean(rawPort);
    console.log('download round', i,
      'node:', JSON.stringify(node), 'h2lean:', JSON.stringify(ours));
  }
  raw.close();

  for (const [who, impl] of [['node', nodeHttp2], ['h2lean', h2lean]]) {
    const { srv, chunks } = await uploadServer(impl);
    const port = srv.address().port;
    await rawClientUpload(port);
    await new Promise((r) => setTimeout(r, 100));
    console.log('upload', who, 'chunks:', chunks.length, JSON.stringify(chunks));
    srv.close();
  }
})();
