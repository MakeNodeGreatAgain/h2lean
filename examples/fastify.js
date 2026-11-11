// Run h2lean under Fastify instead of node:http2.
//
// Fastify creates the HTTP/2 server itself by calling
// `http2.createServer(...)` from `node:http2`. To swap the engine we hand
// Fastify a `serverFactory`: a function that receives Fastify's request
// `handler` and must return a listening server that forwards every incoming
// (req, res) pair to it. Building that server with h2lean's `createServer`
// is the whole trick -- the req/res contract is node:http2-shaped, so
// Fastify cannot tell the difference.
//
// This example serves h2c (HTTP/2 over cleartext TCP), the same transport
// `http2.createServer` gives you. For h2s over TLS, use
// `h2lean.createSecureServer({ key, cert }, ...)`.
//
// h2lean itself is this repository; run it from here:
//   npm install fastify            # fastify is not a dependency of h2lean
//   node examples/fastify.js
// then, from another shell:
//   curl --http2-prior-knowledge http://localhost:8080/
'use strict';

const Fastify = require('fastify');
const h2lean = require('../index.js'); // the same surface as require('h2lean')

const fastify = Fastify({
  http2: true,
  // Forward every request Fastify would have handed to node:http2 through
  // an h2lean server instead. `handler` is Fastify's own (req, res) entry.
  serverFactory: (handler) => h2lean.createServer((req, res) => handler(req, res)),
});

fastify.get('/', async () => 'hello world');

fastify.listen({ port: 8080, host: '127.0.0.1' }, (err) => {
  if (err) {
    fastify.log.error(err);
    process.exit(1);
  }
  console.log('h2lean + Fastify serving h2c on http://localhost:8080');
  console.log('try: curl --http2-prior-knowledge http://localhost:8080/');
});
