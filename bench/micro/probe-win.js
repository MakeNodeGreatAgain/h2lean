// Where does a non-default advertised SETTINGS_INITIAL_WINDOW_SIZE deadlock the
// client? One connection, one request, and a print of every frame in both directions
// with the window bookkeeping next to it. win=65535 (the default) completes; the same
// file with --win=524288 is what out/probe-cadence.js showed serving nothing at all.
//   node bench/micro/probe-win.js --win=524288 --streams=2
'use strict';

const { spawn } = require('node:child_process');
const fs = require('node:fs');
const net = require('node:net');
const path = require('node:path');
const f = require('../../lib/frames');

const opt = Object.fromEntries(process.argv.slice(2).map((a) => {
  const m = /^--([^=]+)=(.*)$/.exec(a);
  return m ? [m[1], m[2]] : [a.slice(2), true];
}));
const win = opt.win === undefined ? undefined : Number(opt.win);
const nStreams = Number(opt.streams || 2);
const port = Number(opt.port || 8961);
const body = Number(opt.body || 262144);

const lite = require('../../index');
const { ClientHttp2Session } = require('../../lib/client-session');

const NAMES = {};
for (const k of ['DATA', 'HEADERS', 'PRIORITY', 'RST_STREAM', 'SETTINGS', 'PUSH_PROMISE',
  'PING', 'GOAWAY', 'WINDOW_UPDATE', 'CONTINUATION']) {
  NAMES[f[k]] = k;
}

function trace(which, type, flags, sid, len) {
  console.log(`    ${which} ${NAMES[type] || type} sid=${sid} len=${len}`
    + ` flags=0x${flags.toString(16)}`
    + ` connSend=${which === '>' ? '' : ''}`);
}

// inbound frames: wrap _handle. outbound: wrap _frame.
const handle = ClientHttp2Session.prototype._handle;
ClientHttp2Session.prototype._handle = function (type, flags, sid, buf, start, len) {
  trace('<', type, flags, sid, len);
  return handle.call(this, type, flags, sid, buf, start, len);
};
const frame = ClientHttp2Session.prototype._frame;
ClientHttp2Session.prototype._frame = function (type, flags, sid, payload, owned) {
  const len = payload === null || payload === undefined ? 0 : payload.length;
  trace('>', type, flags, sid, len);
  return frame.call(this, type, flags, sid, payload, owned);
};
const pump = ClientHttp2Session.prototype._pump;
ClientHttp2Session.prototype._pump = function (stream) {
  if (global.DEBUG_PUMP) {
    console.log(`      pump sid=${stream.id} conn=${this._connSendWin} stream=${stream._sendWin} pending=${stream._pending === null ? 'no' : stream._pending.length}`);
  }
  return pump.call(this, stream);
};
// The credit side of the same story: what each read batch owes and what it actually
// puts on the wire.
const creditRecv = ClientHttp2Session.prototype._creditRecv;
ClientHttp2Session.prototype._creditRecv = function () {
  const owed = [];
  for (const s of this._streams.values()) {
    if (s._recvUsed) {
      owed.push(`${s.id}:${s._recvUsed}`);
    }
  }
  console.log(`      credit inBatch=${this._inBatch} streamUsed=[${owed.join(' ')}]`
    + ` connUsed=${this._connRecvUsed} threshold=${this.localSettings.initialWindowSize >> 1}`
    + ` halfStream=${Math.min(this.localSettings.initialWindowSize, this.remoteSettings.initialWindowSize) >> 1}`
    + ` connWin=${this._connRecvWin} streams=${this._streams.size}`);
  return creditRecv.call(this);
};
const wu = ClientHttp2Session.prototype._windowUpdate;
ClientHttp2Session.prototype._windowUpdate = function (sid, inc) {
  const before = this._stageLen + (this._outQ.length ? 1 : 0) * 0;
  const r = wu.call(this, sid, inc);
  console.log(`      _windowUpdate sid=${sid} inc=${inc} staged=${this._stageLen - before}`
    + ` outLen=${this._outLen} inBatch=${this._inBatch} writing=${this._writing}`);
  return r;
};
const commit = ClientHttp2Session.prototype._commit;
ClientHttp2Session.prototype._commit = function () {
  if (this._stageLen > 0) {
    console.log(`      commit ${this._stageLen} bytes -> _outQ`);
  }
  return commit.call(this);
};
const flush = ClientHttp2Session.prototype._flush;
ClientHttp2Session.prototype._flush = function () {
  console.log(`      flush outLen=${this._outLen} queued=${this._outQ.length}`);
  return flush.call(this);
};

const dir = path.join(__dirname, 'winroot');
fs.mkdirSync(path.join(dir, 'f'), { recursive: true });
fs.writeFileSync(path.join(dir, 'f', 'index.html'), 'x'.repeat(body));
const srv = spawn('nghttpd',
  ['--no-tls', '-m', '1024', '-n', '4', '-w', '22', '-W', '22', String(port), '.'],
  { cwd: dir, stdio: ['ignore', 'ignore', 'pipe'] });
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

function ready(cb) {
  const deadline = Date.now() + 3000;
  const probe = () => {
    const s = net.connect(port, '127.0.0.1', () => {
      s.destroy(); cb(true);
    });
    s.on('error', () => {
      s.destroy();
      if (Date.now() < deadline) {
        setTimeout(probe, 25);
      } else {
        cb(false);
      }
    });
  };
  probe();
}

(async () => {
  if (!await new Promise((r) => ready(r))) {
    console.error('no server'); srv.kill(); process.exit(1);
  }
  await wait(200);
  const options = win === undefined ? undefined : { settings: { initialWindowSize: win } };
  console.log(`connect win=${win === undefined ? 'default' : win} streams=${nStreams} body=${body}`);
  const sess = lite.connect(`http://127.0.0.1:${port}`, options);
  sess.on('error', (e) => console.log('  session error', e.code || e.message));
  await new Promise((r) => sess.on('connect', r));
  console.log(`  connected; remoteSettings.initialWindowSize=${sess.remoteSettings.initialWindowSize}`
    + ` localSettings.initialWindowSize=${sess.localSettings.initialWindowSize}`);
  await wait(200); // let the peer SETTINGS land before opening streams
  global.DEBUG_PUMP = true;
  const results = [];
  for (let i = 0; i < nStreams; i++) {
    results.push(new Promise((resolve) => {
      const s = sess.request({ ':method': 'GET', ':path': '/f/index.html' });
      let bytes = 0;
      s.on('data', (d) => {
        bytes += d.length;
      });
      s.on('error', (e) => resolve(`sid=${s.id} ERROR ${e.code || e.message} after ${bytes}`));
      s.on('end', () => resolve(`sid=${s.id} got ${bytes} bytes`));
    }));
  }
  const timer = setTimeout(() => console.log('  ...3s passed'), 3000);
  timer.unref();
  console.log((await Promise.race([
    Promise.all(results).then((r) => ({ all: r })),
    wait(6000).then(() => ({ all: ['<no completion within 6s>'] }))
  ])).all.join('\n  '));
  sess.destroy();
  srv.kill();
  process.exit(0);
})().catch((e) => {
  console.error(e); srv.kill(); process.exit(1);
});
