// What does node do with a header name that is not a string?
//
//   node bench/micro/probe-nonname.js
//
// node's compat layer calls validateString(name, 'name') before it normalises
// the name (internal/http2/compat.js: setTrailer 581, getHeader 596, hasHeader
// 618, removeHeader 623, setHeader 639, appendHeader 663), and validateString
// throws ERR_INVALID_ARG_TYPE for anything that is not a string
// (internal/validators.js:165). This probe measures the live server, so the
// answer does not rest on reading the JS alone.
'use strict';

const http2 = require('node:http2');

const CALLS = ['setHeader', 'getHeader', 'hasHeader', 'removeHeader', 'appendHeader', 'setTrailer'];

const server = http2.createServer();
server.on('request', (req, res) => {
  for (const call of CALLS) {
    let out;
    try {
      res[call](123, 'v');
      out = 'no throw';
    } catch (e) {
      out = `${e.code || e.name}: ${e.message}`;
    }
    console.log(`  res.${call}(123, 'v') -> ${out}`);
  }
  // the same name, as a string
  for (const call of CALLS) {
    let out;
    try {
      res[call]('123', 'v');
      out = 'no throw';
    } catch (e) {
      out = `${e.code || e.name}: ${e.message}`;
    }
    console.log(`  res.${call}('123', 'v') -> ${out}`);
  }
  res.end('ok');
});

server.listen(0, () => {
  const port = server.address().port;
  const client = http2.connect(`http://127.0.0.1:${port}`);
  client.on('error', () => {});
  const req = client.request({ ':path': '/' });
  let body = '';
  req.setEncoding('utf8');
  req.on('data', (c) => {
    body += c;
  });
  req.on('end', () => {
    console.log(`  body: ${JSON.stringify(body)}`);
    client.close();
    server.close();
  });
  req.end();
});
