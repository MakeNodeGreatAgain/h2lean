// Port of node's `test/parallel/test-http2-malformed-altsvc.js` to the differential shape
// used here (CONTRIBUTING.md §1). Upstream builds one deliberately malformed ALTSVC frame —
// `new h2test.AltSvcFrame((1 << 14) + 1)`: a frame whose payload is 16389 octets, so it is
// already over the 16384 an endpoint advertises, and whose two-octet origin-length field
// claims 16385 origins in a block that cannot hold them. Before nghttp2 1.31.1 that shape
// segfaulted the peer, and upstream's success condition is exactly that: no stream reaches
// the application and the process is still running.
//
// The port keeps both conditions and adds the wire transcript. "Still running" is proven by
// an answer, not by a socket close — a destroyed request emits 'close' too — so a second,
// well-formed connection must receive its 200. Upstream ignores any error on the raw client
// ("an error may or may not be emitted ... we don't care"), so the EPIPE-style noise of
// tearing the socket down immediately after the write is deliberately not recorded here.
const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const http2 = require('node:http2');
const h2lean = require('../../index');

const PREFACE = 'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n';
const SETTINGS_TYPE = 4;
const GOAWAY_TYPE = 7;
const RST_STREAM_TYPE = 3;
const ALTSVC_TYPE = 10;

function frame(type, flags, streamId, payload) {
  const len = payload.length;
  const head = Buffer.alloc(9);
  head[0] = (len >> 16) & 0xff;
  head[1] = (len >> 8) & 0xff;
  head[2] = len & 0xff;
  head[3] = type;
  head[4] = flags;
  head.writeUInt32BE(streamId >>> 0, 5);
  return Buffer.concat([head, payload]);
}

const EMPTY_SETTINGS = frame(SETTINGS_TYPE, 0, 0, Buffer.alloc(0));
const SETTINGS_ACK = frame(SETTINGS_TYPE, 1, 0, Buffer.alloc(0));

// The malformed ALTSVC: the origin-length field says 16385, the payload cannot carry it.
const ORIGIN_LEN_FIELD = (1 << 14) + 1;
const MALFORMED_ALTSVC = frame(ALTSVC_TYPE, 0, 1,
  Buffer.concat([
    Buffer.from([0x40, 0x01]), // 16385, big-endian, in the two-octet origin-length field
    Buffer.alloc(ORIGIN_LEN_FIELD, 0x61), // the origins that do not fit
    Buffer.from('h2') // the two octets of alt the upstream builder appends
  ]));

assert.strictEqual(MALFORMED_ALTSVC.length - 9, ORIGIN_LEN_FIELD + 4,
  'the frame payload is 16389 octets, five over the advertised maximum frame size');

// Names the frames the server put on the wire for it, skipping its own SETTINGS. A frame
// header is nine octets, so the payload starts at p + 9: RST_STREAM carries its error code
// first, GOAWAY its last-stream id before the code.
function verdict(buf) {
  const seen = [];
  for (let p = 0; p + 9 <= buf.length;) {
    const len = (buf[p] << 16) | (buf[p + 1] << 8) | buf[p + 2];
    const type = buf[p + 3];
    if (type !== SETTINGS_TYPE) {
      seen.push(type === RST_STREAM_TYPE
        ? `type=${type} code=${buf.readUInt32BE(p + 9)}`
        : type === GOAWAY_TYPE
          ? `type=${type} code=${buf.readUInt32BE(p + 13)}`
          : `type=${type} len=${len}`);
    }
    p += 9 + len;
  }
  return seen.length ? seen.join(',') : 'nothing';
}

function malformedArm(lib) {
  return new Promise((resolve) => {
    const out = { rawStreams: 0, reply: 'nothing', answered: 'no' };
    let sessions = 0;
    const server = lib.createServer();
    server.on('session', () => {
      sessions++;
    });
    server.on('stream', (stream) => {
      if (sessions === 1) {
        // the raw malformed-ALTSVC connection: upstream's mustNotCall()
        out.rawStreams++;
        return;
      }
      stream.respond({ ':status': 200 });
      stream.end('ok');
    });
    server.on('error', () => {});
    server.listen(0, '127.0.0.1', () => {
      const port = server.address().port;
      const chunks = [];
      const socket = net.connect(port, '127.0.0.1', () => {
        socket.write(PREFACE);
        socket.write(EMPTY_SETTINGS);
        socket.write(SETTINGS_ACK);
        socket.write(MALFORMED_ALTSVC, () => socket.destroy());
      });
      socket.on('data', (c) => chunks.push(c));
      socket.on('error', () => {});
      socket.on('close', () => {
        out.reply = verdict(Buffer.concat(chunks));
        const probe = http2.connect(`http://127.0.0.1:${port}`);
        probe.on('error', () => {});
        const req = probe.request();
        req.on('response', (headers) => {
          out.answered = String(headers[':status']);
        });
        req.on('error', () => {});
        req.resume();
        req.on('close', finish);
        function finish() {
          probe.destroy();
          server.close(() => resolve(out));
        }
        setTimeout(finish, 3000);
      });
      setTimeout(() => resolve(out), 5000);
    });
  });
}

test('upstream malformed-altsvc: a 16389-octet ALTSVC kills neither the server nor the stream',
  async () => {
    const a = await malformedArm(http2);
    const b = await malformedArm(h2lean);
    assert.deepStrictEqual(b, a, 'h2lean must answer the malformed frame as node does');
    assert.strictEqual(a.rawStreams, 0, 'precondition: the frame never reaches the application');
    assert.strictEqual(a.answered, '200',
      'precondition: node is still serving a well-formed request after the frame');
  });
