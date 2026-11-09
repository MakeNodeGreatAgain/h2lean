// Shared harness for the differential tests: the same application code and the
// same client script are run against node:http2 and against h2lite, so anything
// that shows up in the comparison is an implementation difference and not an
// artefact of the test.
const http2 = require('node:http2');

function listen(server) {
  return new Promise((resolve, reject) => {
    server.listen(0, '127.0.0.1', () => resolve(server.address().port));
    // without this a listen that never succeeds (EMFILE, a refused bind) just hangs, and the test
    // file that hit it stops reporting anything at all
    server.once('error', reject);
  });
}

function connect(port) {
  const client = http2.connect(`http://127.0.0.1:${port}`);
  client.on('error', () => {});
  return client;
}

// Closing a server is awaited through its callback, and a broken close -- one that waits for a
// session set that never drains -- then hangs the test FILE instead of failing a test: the runner
// waits for a promise that no timeout can settle and the file stops reporting. Racing the
// callback is not enough by itself, because the listening socket is the other half of the deal:
// measured with `node tools/mutate-guards.js 20` (a mutation that closes the sessions first and
// the transport only if the set happened to be empty), every test in the file ran and reported,
// and the process still never exited -- a ref'd port keeps `node --test` waiting for the file.
// So the deadline also releases what could be holding the close and asks again, which lets a
// broken liveness guard show up as the failures it causes instead of as silence. A healthy close
// lands here in a millisecond and never pays the wait.
function closeServer(srv) {
  return new Promise((resolve) => {
    let done = false;
    const finish = () => {
      if (done) {
        return;
      }
      done = true;
      clearTimeout(timer);
      resolve();
    };
    const timer = setTimeout(() => {
      for (const session of [...(srv.sessions || [])]) {
        session.destroy();
      }
      srv.close(finish); // already closing: net.Server answers with
      setTimeout(finish, 500).unref(); // ERR_SERVER_NOT_RUNNING, which is also a finish
      // and if the port still will not be given up, stop it holding the process open: a test that
      // has finished asserting must be allowed to report.
      if (srv.unref) {
        srv.unref();
      }
    }, 1000);
    srv.close(finish);
  });
}

// One request, everything the client can observe about the answer.
function request(client, headers, body) {
  return new Promise((resolve, reject) => {
    const req = client.request(headers, body === undefined ? { endStream: true } : {});
    const informational = [];
    const continued = [];
    const chunks = [];
    let responseHeaders = null;
    let trailers = null;
    // node surfaces a 100 as 'continue' and other 1xx as 'information'; both are
    // recorded so the comparison does not depend on which name a version uses
    req.on('information', (h) => informational.push(h[':status']));
    req.on('continue', () => continued.push(100));
    req.on('response', (h) => {
      responseHeaders = { ...h };
    });
    req.on('trailers', (t) => {
      trailers = { ...t };
    });
    req.setEncoding('latin1');
    req.on('data', (d) => chunks.push(d));
    req.on('end', () => resolve({
      informational,
      continued,
      headers: responseHeaders,
      trailers,
      body: chunks.join('')
    }));
    req.on('error', reject);
    if (body !== undefined) {
      req.end(body);
    }
  });
}

// Starts impl.createServer(handler), runs script(port) and reports what both
// sides recorded. `log` is appended to by the handler, which is handed
// (req, res, log, index) -- node ignores the extra arguments.
async function probe(impl, handler, script) {
  const log = [];
  const server = impl.createServer((req, res) => handler(req, res, log, log.length));
  const port = await listen(server);
  const client = connect(port);
  try {
    const result = await script(client, port, server);
    return { result, log };
  } finally {
    client.destroy();
    server.close();
  }
}

module.exports = { listen, connect, closeServer, request, probe, nodeHttp2: http2 };
