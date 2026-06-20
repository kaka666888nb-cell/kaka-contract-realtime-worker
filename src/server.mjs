import http from 'node:http';
import { URL } from 'node:url';
import WebSocket, { WebSocketServer } from 'ws';

const VERSION = 'Step413.2';
const PROTOCOL = 'kaka.market.realtime.v1';
const PORT = Number.parseInt(process.env.PORT ?? '8080', 10);
const ALLOWED_INTERVALS = new Set([
  '1m', '3m', '5m', '15m', '30m',
  '1h', '2h', '4h', '6h', '8h', '12h',
  '1d', '3d', '1w', '1M',
]);
const hubs = new Map();

function jsonResponse(res, status, body) {
  const text = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'access-control-allow-origin': '*',
  });
  res.end(text);
}

function normalizeSymbol(value) {
  const symbol = String(value ?? '').trim().toUpperCase();
  return /^[A-Z0-9]{2,30}USDT$/.test(symbol) ? symbol : '';
}

function normalizeInterval(value) {
  const interval = String(value ?? '').trim();
  return ALLOWED_INTERVALS.has(interval) ? interval : '';
}

function streamName(symbol, interval) {
  return `${symbol.toLowerCase()}@kline_${interval}`;
}

function upstreamCandidates(symbol, interval) {
  const stream = streamName(symbol, interval);
  return [
    `wss://fstream.binance.com/market/ws/${stream}`,
    `wss://fstream.binance.com/market/stream?streams=${encodeURIComponent(stream)}`,
    `wss://fstream.binance.com/ws/${stream}`,
  ];
}

function unwrapPayload(raw) {
  try {
    const parsed = JSON.parse(raw.toString());
    return parsed && typeof parsed === 'object' && parsed.data && typeof parsed.data === 'object'
      ? parsed.data
      : parsed;
  } catch {
    return null;
  }
}

function klineFromRaw(raw) {
  const payload = unwrapPayload(raw);
  const kline = payload && typeof payload === 'object' ? payload.k : null;
  if (!kline || typeof kline !== 'object') return null;
  if (kline.t == null || kline.T == null || kline.c == null) return null;
  return {
    symbol: String(kline.s ?? payload.s ?? ''),
    interval: String(kline.i ?? ''),
    openTime: Number(kline.t),
    closeTime: Number(kline.T),
    close: String(kline.c),
    closed: kline.x === true,
  };
}

function waitForFirstKline(url, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const startedAt = Date.now();
    const ws = new WebSocket(url, {
      handshakeTimeout: timeoutMs,
      perMessageDeflate: false,
    });
    const timer = setTimeout(() => finish(new Error('upstream timeout')), timeoutMs);

    function finish(error, result) {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try { ws.close(); } catch {}
      if (error) reject(error); else resolve(result);
    }

    ws.once('open', () => {});
    ws.on('message', (raw) => {
      const kline = klineFromRaw(raw);
      if (!kline) return;
      finish(null, { url, elapsedMs: Date.now() - startedAt, ...kline });
    });
    ws.once('error', (error) => finish(error));
    ws.once('close', (code, reason) => {
      if (!settled) finish(new Error(`upstream closed code=${code} reason=${reason.toString()}`));
    });
  });
}

async function diagnoseContract(symbol, interval) {
  const attempts = [];
  for (const url of upstreamCandidates(symbol, interval)) {
    const startedAt = Date.now();
    try {
      const result = await waitForFirstKline(url);
      return { ok: true, version: VERSION, market: 'contract', mode: 'websocket', candidate: url, ...result };
    } catch (error) {
      attempts.push({ url, elapsedMs: Date.now() - startedAt, error: String(error?.message ?? error) });
    }
  }
  return { ok: false, version: VERSION, market: 'contract', error: 'all_upstream_candidates_failed', attempts };
}

class KlineHub {
  constructor(symbol, interval) {
    this.symbol = symbol;
    this.interval = interval;
    this.key = `${symbol}:${interval}`;
    this.clients = new Set();
    this.upstream = null;
    this.connecting = false;
    this.reconnectTimer = null;
    this.idleTimer = null;
    this.attempt = 0;
    this.lastPayload = null;
    this.candidateIndex = 0;
  }

  add(client) {
    clearTimeout(this.idleTimer);
    this.idleTimer = null;
    this.clients.add(client);
    if (this.lastPayload && client.readyState === WebSocket.OPEN) {
      client.send(this.lastPayload);
    }
    this.ensureUpstream();
  }

  remove(client) {
    this.clients.delete(client);
    if (this.clients.size === 0 && !this.idleTimer) {
      this.idleTimer = setTimeout(() => this.shutdown(), 12000);
    }
  }

  ensureUpstream() {
    if (this.clients.size === 0 || this.connecting || this.upstream?.readyState === WebSocket.OPEN) return;
    this.connecting = true;
    const candidates = upstreamCandidates(this.symbol, this.interval);
    const url = candidates[this.candidateIndex % candidates.length];
    const ws = new WebSocket(url, {
      handshakeTimeout: 10000,
      perMessageDeflate: false,
    });
    this.upstream = ws;

    const openTimer = setTimeout(() => {
      try { ws.terminate(); } catch {}
    }, 11000);

    ws.once('open', () => {
      clearTimeout(openTimer);
      this.connecting = false;
      this.attempt = 0;
      this.candidateIndex = 0;
      console.log(`[${VERSION}] upstream open ${this.key} ${url}`);
    });

    ws.on('message', (raw, isBinary) => {
      const text = isBinary ? raw : raw.toString();
      if (!klineFromRaw(text)) return;
      this.lastPayload = text;
      for (const client of this.clients) {
        if (client.readyState === WebSocket.OPEN) {
          try { client.send(text); } catch {}
        }
      }
    });

    ws.once('error', (error) => {
      console.error(`[${VERSION}] upstream error ${this.key} ${url}: ${error.message}`);
    });

    ws.once('close', (code, reason) => {
      clearTimeout(openTimer);
      this.connecting = false;
      if (this.upstream === ws) this.upstream = null;
      console.warn(`[${VERSION}] upstream close ${this.key} code=${code} reason=${reason.toString()}`);
      if (this.clients.size > 0) {
        this.candidateIndex += 1;
        this.scheduleReconnect();
      }
    });
  }

  scheduleReconnect() {
    if (this.reconnectTimer || this.clients.size === 0) return;
    const delay = Math.min(30000, 1000 * (2 ** Math.min(this.attempt, 5)));
    this.attempt += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.ensureUpstream();
    }, delay);
  }

  shutdown() {
    clearTimeout(this.reconnectTimer);
    clearTimeout(this.idleTimer);
    this.reconnectTimer = null;
    this.idleTimer = null;
    for (const client of this.clients) {
      try { client.close(1001, 'hub shutdown'); } catch {}
    }
    this.clients.clear();
    if (this.upstream) {
      try { this.upstream.close(1000, 'no clients'); } catch {}
      this.upstream = null;
    }
    hubs.delete(this.key);
  }
}

function getHub(symbol, interval) {
  const key = `${symbol}:${interval}`;
  let hub = hubs.get(key);
  if (!hub) {
    hub = new KlineHub(symbol, interval);
    hubs.set(key, hub);
  }
  return hub;
}

function browserTestHtml() {
  return `<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Kaka Web3 Step413.2 Test</title>
<style>body{font-family:system-ui;background:#f4f6f9;margin:0;padding:24px;color:#172033}.card{max-width:860px;margin:auto;background:#fff;border-radius:18px;padding:22px;box-shadow:0 8px 30px #0001}.ok{color:#079447}.bad{color:#d92d20}.wait{color:#a66b00}pre{background:#111827;color:#e5e7eb;padding:14px;border-radius:12px;white-space:pre-wrap;word-break:break-word}</style></head>
<body><div class="card"><h2>Kaka Web3 Step413.2 合约 Worker 测试</h2>
<p id="health" class="wait">1. Worker 健康检查：等待</p>
<p id="upstream" class="wait">2. Binance 合约上游：等待</p>
<p id="socket" class="wait">3. App→Worker 合约 WebSocket：等待</p>
<h2 id="result" class="wait">正在测试，请等待约 20 秒……</h2><pre id="log"></pre></div>
<script>
const logEl=document.getElementById('log'); const log=(x)=>{logEl.textContent+=x+'\\n'};
const set=(id,text,ok)=>{const e=document.getElementById(id);e.textContent=text;e.className=ok?'ok':'bad'};
(async()=>{try{
 const health=await fetch('/health',{cache:'no-store'}); const hj=await health.json(); log('health '+JSON.stringify(hj)); if(!health.ok||!hj.ok)throw new Error('health failed'); set('health','1. Worker 健康检查：通过',true);
 const d=await fetch('/diagnose?market=contract&symbol=BTCUSDT&interval=1m',{cache:'no-store'}); const dj=await d.json(); log('diagnose '+JSON.stringify(dj)); if(!d.ok||!dj.ok)throw new Error('Binance 合约上游失败'); set('upstream','2. Binance 合约上游：通过，价格 '+dj.close,true);
 await new Promise((resolve,reject)=>{const scheme=location.protocol==='https:'?'wss:':'ws:'; const u=scheme+'//'+location.host+'/ws?protocol=${PROTOCOL}&channel=kline&provider=binance&market=contract&symbol=BTCUSDT&interval=1m'; const ws=new WebSocket(u); const t=setTimeout(()=>{try{ws.close()}catch{} reject(new Error('App WebSocket timeout'))},12000); ws.onmessage=(ev)=>{try{const p=JSON.parse(ev.data);const x=p.data||p;if(x.k&&x.k.c){clearTimeout(t);set('socket','3. App→Worker 合约 WebSocket：通过，BTCUSDT 1m 价格 '+x.k.c,true);log('socket price '+x.k.c);ws.close();resolve();}}catch{}}; ws.onerror=()=>{clearTimeout(t);reject(new Error('App WebSocket failed'))};});
 const r=document.getElementById('result');r.textContent='Step413.2 合约实时链路全部测试通过';r.className='ok';
}catch(e){log(String(e.stack||e));const r=document.getElementById('result');r.textContent='测试未通过：'+e.message;r.className='bad';}})();
</script></body></html>`;
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`);
  if (url.pathname === '/' || url.pathname === '/health') {
    return jsonResponse(res, 200, {
      ok: true,
      service: 'kaka-contract-realtime-worker',
      version: VERSION,
      protocol: PROTOCOL,
      market: 'contract',
      websocket: '/ws',
      diagnose: '/diagnose',
      browserTest: '/browser-test',
      serverTime: new Date().toISOString(),
    });
  }
  if (url.pathname === '/browser-test') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
    return res.end(browserTestHtml());
  }
  if (url.pathname === '/diagnose') {
    const symbol = normalizeSymbol(url.searchParams.get('symbol') ?? 'BTCUSDT');
    const interval = normalizeInterval(url.searchParams.get('interval') ?? '1m');
    if (!symbol || !interval) return jsonResponse(res, 400, { ok: false, error: 'invalid_symbol_or_interval' });
    const result = await diagnoseContract(symbol, interval);
    return jsonResponse(res, result.ok ? 200 : 503, result);
  }
  return jsonResponse(res, 404, { ok: false, error: 'not_found' });
});

const wss = new WebSocketServer({ noServer: true, perMessageDeflate: false });

server.on('upgrade', (req, socket, head) => {
  try {
    const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`);
    if (url.pathname !== '/ws') return socket.destroy();
    const protocol = url.searchParams.get('protocol') ?? '';
    const channel = url.searchParams.get('channel') ?? '';
    const provider = (url.searchParams.get('provider') ?? '').toLowerCase();
    const market = (url.searchParams.get('market') ?? '').toLowerCase();
    const symbol = normalizeSymbol(url.searchParams.get('symbol'));
    const interval = normalizeInterval(url.searchParams.get('interval'));
    if (protocol !== PROTOCOL || channel !== 'kline' || provider !== 'binance' || market !== 'contract' || !symbol || !interval) {
      socket.write('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n');
      return socket.destroy();
    }
    wss.handleUpgrade(req, socket, head, (client) => {
      client.isAlive = true;
      client.on('pong', () => { client.isAlive = true; });
      client.kakaHub = getHub(symbol, interval);
      client.kakaHub.add(client);
      client.on('close', () => client.kakaHub?.remove(client));
      client.on('error', () => client.kakaHub?.remove(client));
      wss.emit('connection', client, req);
    });
  } catch {
    socket.destroy();
  }
});

const heartbeat = setInterval(() => {
  for (const client of wss.clients) {
    if (client.isAlive === false) {
      try { client.terminate(); } catch {}
      continue;
    }
    client.isAlive = false;
    try { client.ping(); } catch {}
  }
}, 25000);
heartbeat.unref();

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[${VERSION}] contract realtime worker listening on 0.0.0.0:${PORT}`);
});

function shutdown() {
  clearInterval(heartbeat);
  for (const hub of hubs.values()) hub.shutdown();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
