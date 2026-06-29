import { handleMarketApi } from './market-rest.mjs';
import http from 'node:http';
import { WebSocketServer, WebSocket } from 'ws';

const PORT = Number(process.env.PORT || 10000);
const PROVIDERS = new Set(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const VALID_INTERVALS = new Set(['1m','3m','5m','15m','30m','1h','2h','4h','6h','8h','12h','1d','3d','1w','1M']);

function providerKey(raw) {
  const value = String(raw || '').trim().toLowerCase().replaceAll('gate.io', 'gate');
  if (value === 'okex') return 'okx';
  return PROVIDERS.has(value) ? value : null;
}
function marketKey(raw) {
  const value = String(raw || '').trim().toLowerCase();
  return /contract|future|perpetual|swap|linear/.test(value) ? 'contract' : 'spot';
}
function symbolKey(raw) {
  return String(raw || '').trim().toUpperCase().replace(/-SWAP$/i, '').replace(/_UMCBL$/i, '').replace(/[^A-Z0-9]/g, '');
}
function splitSymbol(symbol) {
  for (const quote of ['USDT','USDC','USD']) if (symbol.endsWith(quote)) return [symbol.slice(0, -quote.length), quote];
  return [symbol, 'USDT'];
}
function okxInstId(symbol, market) { const [base, quote] = splitSymbol(symbol); return `${base}-${quote}${market === 'contract' ? '-SWAP' : ''}`; }
function gateSymbol(symbol) { const [base, quote] = splitSymbol(symbol); return `${base}_${quote}`; }
function intervalMs(interval) {
  const map = { '1m':60000,'3m':180000,'5m':300000,'15m':900000,'30m':1800000,'1h':3600000,'2h':7200000,'4h':14400000,'6h':21600000,'8h':28800000,'12h':43200000,'1d':86400000,'3d':259200000,'1w':604800000,'1M':2592000000 };
  return map[interval] || 900000;
}
function okxChannel(interval) {
  const map = {'1m':'candle1m','3m':'candle3m','5m':'candle5m','15m':'candle15m','30m':'candle30m','1h':'candle1H','2h':'candle2H','4h':'candle4H','6h':'candle6H','12h':'candle12H','1d':'candle1Dutc','3d':'candle3Dutc','1w':'candle1Wutc','1M':'candle1Mutc'};
  return map[interval] || 'candle15m';
}
function gateInterval(interval) {
  const map = {'1m':'1m','5m':'5m','15m':'15m','30m':'30m','1h':'1h','2h':'2h','4h':'4h','6h':'6h','8h':'8h','12h':'12h','1d':'1d','3d':'3d','1w':'7d'};
  return map[interval] || '15m';
}
function bitgetChannel(interval) {
  const map = {'1m':'candle1m','3m':'candle3m','5m':'candle5m','15m':'candle15m','30m':'candle30m','1h':'candle1H','2h':'candle2H','4h':'candle4H','6h':'candle6H','12h':'candle12H','1d':'candle1D','3d':'candle3D','1w':'candle1W','1M':'candle1M'};
  return map[interval] || 'candle15m';
}
function bybitInterval(interval) {
  const map = {'1m':'1','3m':'3','5m':'5','15m':'15','30m':'30','1h':'60','2h':'120','4h':'240','6h':'360','12h':'720','1d':'D','1w':'W','1M':'M'};
  return map[interval] || '15';
}
function num(v, fallback = '0') { const n = Number(v); return Number.isFinite(n) ? String(v) : fallback; }
function normalizedMessage(provider, market, symbol, interval, a, closed = false, trades = 0) {
  const t = Number(a[0]);
  const open = num(a[1]); const high = num(a[2]); const low = num(a[3]); const close = num(a[4]);
  const volume = num(a[5]); const quoteVolume = num(a[6]);
  return JSON.stringify({
    stream: `${provider}:${symbol}:${market}:${interval}`,
    provider,
    market,
    data: {
      e: 'kline', E: Date.now(), s: symbol,
      k: { t, T: t + intervalMs(interval) - 1, s: symbol, i: interval, o: open, h: high, l: low, c: close, v: volume, q: quoteVolume, V: '0', Q: '0', n: Number(trades) || 0, x: !!closed }
    }
  });
}
function upstreamConfig(provider, market, symbol, interval) {
  if (provider === 'binance') return {
    url: market === 'contract'
      ? `wss://fstream.binance.com/ws/${symbol.toLowerCase()}@kline_${interval}`
      : `wss://stream.binance.com:9443/ws/${symbol.toLowerCase()}@kline_${interval}`,
    subscribe: null,
    parse(raw) {
      const message = JSON.parse(raw.toString());
      const k = message?.k || message?.data?.k;
      if (!k) return null;
      return normalizedMessage(provider, market, symbol, interval, [k.t,k.o,k.h,k.l,k.c,k.v,k.q], k.x, k.n);
    },
  };
  if (provider === 'okx') return {
    url: 'wss://ws.okx.com:8443/ws/v5/business',
    subscribe: { op: 'subscribe', args: [{ channel: okxChannel(interval), instId: okxInstId(symbol, market) }] },
    parse(raw) {
      const message = JSON.parse(raw.toString());
      const a = Array.isArray(message?.data) ? message.data[0] : null;
      if (!Array.isArray(a)) return null;
      return normalizedMessage(provider, market, symbol, interval, [a[0],a[1],a[2],a[3],a[4],a[5],a[7]], String(a[8]) === '1');
    },
  };
  if (provider === 'gate') {
    const contract = market === 'contract';
    return {
      url: contract ? 'wss://fx-ws.gateio.ws/v4/ws/usdt' : 'wss://api.gateio.ws/ws/v4/',
      subscribe: {
        time: Math.floor(Date.now()/1000),
        channel: contract ? 'futures.candlesticks' : 'spot.candlesticks',
        event: 'subscribe',
        payload: [gateInterval(interval), gateSymbol(symbol)],
      },
      parse(raw) {
        const message = JSON.parse(raw.toString());
        const expected = contract ? 'futures.candlesticks' : 'spot.candlesticks';
        if (message?.channel !== expected || message?.event !== 'update') return null;
        const r = Array.isArray(message.result) ? message.result[0] : message.result;
        if (!r) return null;
        const t = Number(r.t) * (Number(r.t) < 10_000_000_000 ? 1000 : 1);
        return normalizedMessage(provider, market, symbol, interval, [t,r.o,r.h,r.l,r.c,r.v,r.a ?? r.sum], false, r.n);
      },
    };
  }
  if (provider === 'bitget') return {
    url: 'wss://ws.bitget.com/v2/ws/public',
    subscribe: { op: 'subscribe', args: [{ instType: market === 'contract' ? 'USDT-FUTURES' : 'SPOT', channel: bitgetChannel(interval), instId: symbol }] },
    parse(raw) {
      const message = JSON.parse(raw.toString());
      const a = Array.isArray(message?.data) ? message.data[0] : null;
      if (!Array.isArray(a)) return null;
      return normalizedMessage(provider, market, symbol, interval, [a[0],a[1],a[2],a[3],a[4],a[5],a[6]], false);
    },
  };
  return {
    url: `wss://stream.bybit.com/v5/public/${market === 'contract' ? 'linear' : 'spot'}`,
    subscribe: { op: 'subscribe', args: [`kline.${bybitInterval(interval)}.${symbol}`] },
    heartbeatMessage: { op: 'ping' },
    parse(raw) {
      const message = JSON.parse(raw.toString());
      if (!String(message?.topic || '').startsWith('kline.')) return null;
      const a = Array.isArray(message?.data) ? message.data[0] : null;
      if (!a || typeof a !== 'object') return null;
      return normalizedMessage(provider, market, symbol, interval, [a.start,a.open,a.high,a.low,a.close,a.volume,a.turnover], a.confirm === true);
    },
  };
}

const server = http.createServer(async (req, res) => {
  const parsedHttpUrl = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  if (await handleMarketApi(req, res, parsedHttpUrl)) return;
  if (req.url?.startsWith('/health')) {
    res.writeHead(200, {'content-type':'application/json'});
    res.end(JSON.stringify({ ok:true, protocol:'kaka.market.realtime.v1', providers:[...PROVIDERS], markets:['spot','contract'], time:new Date().toISOString() }));
    return;
  }
  if (req.url?.startsWith('/diagnose') || req.url?.startsWith('/browser-test')) {
    res.writeHead(200, {'content-type':'text/html; charset=utf-8'});
    res.end('<!doctype html><meta charset="utf-8"><title>Kaka market realtime</title><h1>Kaka market realtime worker</h1><p>Providers: Binance / OKX / Bybit / Bitget / Gate</p><p>Markets: spot / contract</p><p>Use /ws?provider=bybit&market=contract&symbol=BTCUSDT&interval=15m</p>');
    return;
  }
  res.writeHead(404, {'content-type':'application/json'});
  res.end(JSON.stringify({ok:false,error:'not found'}));
});

const wss = new WebSocketServer({ noServer: true });
server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  if (url.pathname !== '/ws') { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req, url));
});

wss.on('connection', (client, _req, parsedUrl) => {
  const provider = providerKey(parsedUrl.searchParams.get('provider'));
  const symbol = symbolKey(parsedUrl.searchParams.get('symbol'));
  const interval = parsedUrl.searchParams.get('interval') || '15m';
  const market = marketKey(parsedUrl.searchParams.get('market'));
  if (!provider || !symbol || !VALID_INTERVALS.has(interval) || !['spot','contract'].includes(market)) {
    client.close(1008, 'invalid market channel'); return;
  }
  const cfg = upstreamConfig(provider, market, symbol, interval);
  let upstream;
  let heartbeat;
  try {
    upstream = new WebSocket(cfg.url, { handshakeTimeout: 15000 });
  } catch (error) {
    client.close(1011, String(error)); return;
  }
  const cleanup = () => {
    clearInterval(heartbeat);
    try { if (upstream?.readyState === WebSocket.OPEN || upstream?.readyState === WebSocket.CONNECTING) upstream.close(); } catch {}
  };
  client.on('close', cleanup);
  client.on('error', cleanup);
  upstream.on('open', () => {
    if (cfg.subscribe) upstream.send(JSON.stringify(cfg.subscribe));
    client.send(JSON.stringify({ type:'ready', provider, market, symbol, interval, protocol:'kaka.market.realtime.v1' }));
    heartbeat = setInterval(() => {
      if (upstream.readyState === WebSocket.OPEN) {
        if (cfg.heartbeatMessage) upstream.send(JSON.stringify(cfg.heartbeatMessage));
        else if (provider === 'okx' || provider === 'bitget') upstream.send('ping');
        else upstream.ping();
      }
      if (client.readyState === WebSocket.OPEN) client.ping();
    }, 20_000);
  });
  upstream.on('message', (raw) => {
    const text = raw.toString();
    if (text === 'pong') return;
    try {
      const normalized = cfg.parse(raw);
      if (normalized && client.readyState === WebSocket.OPEN) client.send(normalized);
    } catch {}
  });
  upstream.on('close', (code, reason) => {
    clearInterval(heartbeat);
    if (client.readyState === WebSocket.OPEN) client.close(code === 1000 ? 1000 : 1012, `upstream closed ${reason || ''}`.slice(0,120));
  });
  upstream.on('error', () => {
    if (client.readyState === WebSocket.OPEN) client.close(1011, 'upstream connection failed');
  });
});

server.listen(PORT, () => console.log(`Kaka market realtime worker 513.0 listening on ${PORT}`));
