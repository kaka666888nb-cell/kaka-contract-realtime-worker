import http from 'node:http';
import { gunzipSync } from 'node:zlib';
import { installRenderEgressCostGuard } from '../src/render-egress-cost-guard.mjs';

installRenderEgressCostGuard();

const bufferedJson = JSON.stringify({
  ok: true,
  rows: Array.from({ length: 100 }, (_, index) => ({ index, text: 'abcdefghij' })),
});

const server = http.createServer((req, res) => {
  if (req.url === '/buffered') {
    res.setHeader('content-type', 'application/json; charset=utf-8');
    res.end(bufferedJson);
    return;
  }
  if (req.url === '/stream') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
    res.write('{"ok":true,"rows":[');
    res.write('{"x":1},');
    res.end('{"x":2}]}');
    return;
  }
  res.end('not-found');
});

await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
const port = server.address().port;

function read(path) {
  return new Promise((resolve, reject) => {
    const request = http.request(
      {
        hostname: '127.0.0.1',
        port,
        path,
        headers: { 'accept-encoding': 'gzip' },
      },
      (response) => {
        const chunks = [];
        response.on('data', (chunk) => chunks.push(chunk));
        response.on('end', () => resolve({
          headers: response.headers,
          body: Buffer.concat(chunks),
        }));
      },
    );
    request.on('error', reject);
    request.end();
  });
}

try {
  const buffered = await read('/buffered');
  if (buffered.headers['content-encoding'] !== 'gzip') {
    throw new Error('buffered JSON did not use gzip');
  }
  if (gunzipSync(buffered.body).toString('utf8') !== bufferedJson) {
    throw new Error('buffered gzip body mismatch');
  }

  const streamed = await read('/stream');
  if (streamed.headers['content-encoding']) {
    throw new Error('streaming response incorrectly advertised compression');
  }
  const streamedText = streamed.body.toString('utf8');
  if (streamedText !== '{"ok":true,"rows":[{"x":1},{"x":2}]}') {
    throw new Error(`streaming response body mismatch: ${streamedText}`);
  }

  console.log('PASS Step1060.33.3 egress streaming regression');
} finally {
  server.close();
}
