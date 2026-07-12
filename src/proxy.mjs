import http from 'node:http';
import { spawn } from 'node:child_process';
import { handleContractFlow } from './contract-flow.mjs';

const PORT = Number(process.env.PORT || 10000);
const CHILD_PORT = Number(process.env.KAKA_CHILD_PORT || 10001);
const child = spawn(process.execPath, ['src/server.mjs'], {
  env: { ...process.env, PORT: String(CHILD_PORT) },
  stdio: 'inherit',
});

child.on('exit', (code, signal) => {
  console.error(`[Step614.2.1] legacy worker exited code=${code} signal=${signal || ''}`);
  process.exit(code || 1);
});

function proxyHttp(req, res) {
  const upstream = http.request({
    hostname: '127.0.0.1',
    port: CHILD_PORT,
    method: req.method,
    path: req.url,
    headers: { ...req.headers, host: `127.0.0.1:${CHILD_PORT}` },
  }, (upstreamRes) => {
    res.writeHead(upstreamRes.statusCode || 502, upstreamRes.headers);
    upstreamRes.pipe(res);
  });
  upstream.setTimeout(30000, () => upstream.destroy(new Error('legacy_worker_timeout')));
  upstream.on('error', (error) => {
    if (res.headersSent) return res.end();
    res.writeHead(502, { 'content-type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ ok: false, error: `legacy_worker_unavailable:${error.message}` }));
  });
  req.pipe(upstream);
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  if (url.pathname === '/health') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(JSON.stringify({
      ok: true,
      service: 'kaka-contract-realtime-worker',
      version: '614.2.1',
      legacy_worker: '515.1.2',
      protocol: 'kaka.market.realtime.v1',
      providers: ['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate'],
      spot_providers: ['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate'],
      contract_providers: ['binance', 'okx', 'bybit', 'bitget', 'gate'],
      contract_flow: '/api/contract-flow',
      contract_flow_warm: '/api/contract-flow/warm',
      contract_flow_persistence: Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY),
      time: new Date().toISOString(),
    }));
    return;
  }
  try {
    if (await handleContractFlow(req, res, url)) return;
  } catch (error) {
    if (!res.headersSent) {
      res.writeHead(500, { 'content-type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: false, error: String(error?.message || error) }));
    }
    return;
  }
  proxyHttp(req, res);
});

server.on('upgrade', (req, socket, head) => {
  const upstream = http.request({
    hostname: '127.0.0.1',
    port: CHILD_PORT,
    method: 'GET',
    path: req.url,
    headers: { ...req.headers, host: `127.0.0.1:${CHILD_PORT}` },
  });
  upstream.on('upgrade', (upstreamRes, upstreamSocket, upstreamHead) => {
    let response = `HTTP/${upstreamRes.httpVersion} ${upstreamRes.statusCode} ${upstreamRes.statusMessage}\r\n`;
    for (const [name, value] of Object.entries(upstreamRes.headers)) {
      if (Array.isArray(value)) for (const item of value) response += `${name}: ${item}\r\n`;
      else if (value != null) response += `${name}: ${value}\r\n`;
    }
    response += '\r\n';
    socket.write(response);
    if (head?.length) upstreamSocket.write(head);
    if (upstreamHead?.length) socket.write(upstreamHead);
    socket.pipe(upstreamSocket).pipe(socket);
  });
  upstream.on('response', (upstreamRes) => {
    socket.write(`HTTP/1.1 ${upstreamRes.statusCode || 502} ${upstreamRes.statusMessage || 'Bad Gateway'}\r\n\r\n`);
    socket.destroy();
  });
  upstream.on('error', () => socket.destroy());
  upstream.end();
});

function shutdown(signal) {
  console.log(`[Step614.2.1] shutdown ${signal}`);
  server.close(() => {
    child.kill('SIGTERM');
    process.exit(0);
  });
  setTimeout(() => process.exit(0), 5000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[Step614.2.1] proxy + contract flow listening on 0.0.0.0:${PORT}; legacy=${CHILD_PORT}`);
});
