// Measures the API surface a drop-in node:http2 clone must reproduce.
// Grabs live instances (session / server stream / client stream / req / res)
// and walks their full prototype chains, symbols included.
//   node --expose-internals bench/api-surface.js
const http2 = require('node:http2');

function describe(label, proto) {
  const meth = [], get = [], set = [], data = [], syms = [], bases = [];
  let p = proto;
  let depth = 0;
  while (p && p !== Object.prototype && depth < 8) {
    bases.push(p.constructor?.name ?? '(anon)');
    for (const k of Object.getOwnPropertyNames(p)) {
      if (k === 'constructor') {
        continue;
      }
      const d = Object.getOwnPropertyDescriptor(p, k);
      if (d.get) {
        get.push(k);
      } else if (d.set) {
        set.push(k);
      } else if (typeof d.value === 'function') {
        meth.push(k);
      } else {
        data.push(k);
      }
    }
    for (const k of Object.getOwnPropertySymbols(p)) {
      syms.push(k.description || '?');
    }
    p = Object.getPrototypeOf(p);
    depth++;
  }
  const all = [...new Set([...meth, ...get, ...set, ...data])];
  console.log(`\n== ${label} ==`);
  console.log(`   chain: ${bases.join(' -> ')}`);
  console.log(`   members: ${all.length}  (methods ${meth.length}, getters ${get.length}, ` +
              `setters ${set.length}, data ${data.length}, symbols ${syms.length})`);
  console.log(`   methods: ${[...new Set(meth)].sort().join(', ')}`);
  if (get.length) {
    console.log(`   getters: ${[...new Set(get)].sort().join(', ')}`);
  }
  if (syms.length) {
    console.log(`   symbols: ${[...new Set(syms)].sort().join(', ')}`);
  }
  return all.length;
}

const srv = http2.createServer();
let captured = 0;

srv.on('stream', (stream, _headers) => {
  if (captured++ === 0) {
    total += describe('ServerHttp2Stream (live)', Object.getPrototypeOf(stream));
    // the request/response compat objects hang off symbol-keyed own props
    for (const k of Object.getOwnPropertySymbols(stream)) {
      const v = stream[k];
      if (v && typeof v === 'object') {
        const cn = v.constructor?.name ?? '';
        if (/Http2Server(Request|Response)/.test(cn)) {
          total += describe(`${cn} (live)`, Object.getPrototypeOf(v));
        }
      }
    }
    const own = Object.getOwnPropertyNames(stream)
      .concat(Object.getOwnPropertySymbols(stream).map((s) => `Symbol(${s.description})`));
    console.log(`\n== instance own props of ServerHttp2Stream ==\n   ${own.join(', ')}`);
  }
  stream.respond({ ':status': '200' });
  stream.end('ok');
});

let total = 0;

srv.listen(0, '127.0.0.1', () => {
  const client = http2.connect(`http://127.0.0.1:${srv.address().port}`);
  total += describe('Http2Session (client)', Object.getPrototypeOf(client));

  const req = client.request({ ':method': 'GET', ':path': '/' });
  total += describe('ClientHttp2Stream (live)', Object.getPrototypeOf(req));

  req.on('end', () => {
    console.log(`\nTOTAL distinct API members walked: ${total}`);

    const modKeys = Object.getOwnPropertyNames(http2);
    console.log(`\n== module surface ==\n   ${modKeys.join(', ')}`);
    console.log(`   symbols: ${Object.getOwnPropertySymbols(http2).map((s) => s.description).join(', ') || '(none)'}`);

    const consts = Object.keys(http2.constants);
    const groups = {};
    for (const c of consts) {
      const g = /^NGHTTP2_ERR/.test(c) ? 'NGHTTP2_ERR_*'
        : /^NGHTTP2/.test(c) ? 'NGHTTP2_* (frame/settings/stream enums)'
          : /^HTTP2/.test(c) ? 'HTTP2_* (header names etc.)' : 'other';
      groups[g] = (groups[g] || 0) + 1;
    }
    console.log(`\n== constants: ${consts.length} ==\n   ${JSON.stringify(groups)}`);

    let codes = [];
    try {
      codes = Object.keys(require('node:internal/errors').codes)
        .filter((c) => /^ERR_(HTTP2|NGHTTP2)/.test(c));
    } catch { /* no internals */ }
    console.log(`\n== error codes ERR_HTTP2*/ERR_NGHTTP2*: ${codes.length} ==\n   ${codes.join(', ')}`);

    // events that the session/stream emit -- enumerated from listener wiring
    req.resume();
    client.close();
    srv.close();
    setTimeout(() => process.exit(0), 300);
  });
  req.resume();
});
