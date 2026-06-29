import { handleMarketApi, fetchMarketKlines } from './market-rest.mjs';
import http from 'node:http';
import { WebSocketServer, WebSocket } from 'ws';

const PORT = Number(process.env.PORT || 10000);
const PROVIDERS = new Set(['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate']);
const SPOT_PROVIDERS = ['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate'];
const CONTRACT_PROVIDERS = ['binance', 'okx', 'bybit', 'bitget', 'gate'];
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
function providerMarketAllowed(provider, market) {
  return market === 'contract' ? CONTRACT_PROVIDERS.includes(provider) : SPOT_PROVIDERS.includes(provider);
}
function symbolKey(raw) {
  return String(raw || '')
    .trim()
    .toUpperCase()
    .replace(/-SWAP$/i, '')
    .replace(/_UMCBL$/i, '')
    .replace(/[^A-Z0-9]/g, '');
}
function splitSymbol(symbol) {
  for (const quote of ['USDT', 'USDC', 'USD']) {
    if (symbol.endsWith(quote)) return [symbol.slice(0, -quote.length), quote];
  }
  return [symbol, 'USDT'];
}
function coinbaseProductId(symbol) {
  const [base, quote] = splitSymbol(symbol);
  return `${base}-${quote}`;
}
function okxInstId(symbol, market) {
  const [base, quote] = splitSymbol(symbol);
  return `${base}-${quote}${market === 'contract' ? '-SWAP' : ''}`;
}
function gateSymbol(symbol) {
  const [base, quote] = splitSymbol(symbol);
  return `${base}_${quote}`;
}
function intervalMs(interval) {
  const map = {
    '1m':60_000,'3m':180_000,'5m':300_000,'15m':900_000,'30m':1_800_000,
    '1h':3_600_000,'2h':7_200_000,'4h':14_400_000,'6h':21_600_000,
    '8h':28_800_000,'12h':43_200_000,'1d':86_400_000,'3d':259_200_000,
    '1w':604_800_000,'1M':2_592_000_000,
  };
  return map[interval] || 900_000;
}
function okxChannel(interval) {
  const map = {
    '1m':'candle1m','3m':'candle3m','5m':'candle5m','15m':'candle15m','30m':'candle30m',
    '1h':'candle1H','2h':'candle2H','4h':'candle4H','6h':'candle6H','12h':'candle12H',
    '1d':'candle1Dutc','3d':'candle3Dutc','1w':'candle1Wutc','1M':'candle1Mutc',
  };
  return map[interval] || 'candle15m';
}
function gateInterval(interval) {
  const map = {
    '1m':'1m','5m':'5m','15m':'15m','30m':'30m','1h':'1h','2h':'2h','4h':'4h',
    '6h':'6h','8h':'8h','12h':'12h','1d':'1d','3d':'3d','1w':'7d',
  };
  return map[interval] || '15m';
}
function bitgetChannel(interval) {
  const map = {
    '1m':'candle1m','3m':'candle3m','5m':'candle5m','15m':'candle15m','30m':'candle30m',
    '1h':'candle1H','2h':'candle2H','4h':'candle4H','6h':'candle6H','12h':'candle12H',
    '1d':'candle1D','3d':'candle3D','1w':'candle1W','1M':'candle1M',
  };
  return map[interval] || 'candle15m';
}
function bybitInterval(interval) {
  const map = {
    '1m':'1','3m':'3','5m':'5','15m':'15','30m':'30','1h':'60','2h':'120',
    '4h':'240','6h':'360','12h':'720','1d':'D','1w':'W','1M':'M',
  };
  return map[interval] || '15';
}
function numberText(value, fallback = '0') {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? String(value) : fallback;
}
function normalizedMessage(provider, market, symbol, interval, values, closed = false, trades = 0) {
  const timestamp = Number(values[0]);
  const open = numberText(values[1]);
  const high = numberText(values[2]);
  const low = numberText(values[3]);
  const close = numberText(values[4]);
  const volume = numberText(values[5]);
  const quoteVolume = numberText(values[6]);
  return JSON.stringify({
    stream: `${provider}:${symbol}:${market}:${interval}`,
    provider,
    market,
    data: {
      e: 'kline',
      E: Date.now(),
      s: symbol,
      k: {
        t: timestamp,
        T: timestamp + intervalMs(interval) - 1,
        s: symbol,
        i: interval,
        o: open,
        h: high,
        l: low,
        c: close,
        v: volume,
        q: quoteVolume,
        V: '0',
        Q: '0',
        n: Number(trades) || 0,
        x: !!closed,
      },
    },
  });
}

async function coinbaseConfig(symbol, interval) {
  const productId = coinbaseProductId(symbol);
  const bucketMs = intervalMs(interval);
  let candle = null;
  try {
    const rows = await fetchMarketKlines('coinbase', 'spot', symbol, interval, Date.now(), 3);
    const latest = rows.at(-1);
    if (latest) {
      candle = {
        start: Number(latest.open_time_ms),
        open: Number(latest.open),
        high: Number(latest.high),
        low: Number(latest.low),
        close: Number(latest.close),
        volume: Number(latest.volume || 0),
        quoteVolume: Number(latest.quote_volume || 0),
        trades: Number(latest.trade_count || 0),
      };
    }
  } catch (_) {}

  function candleMessage(current, closed = false) {
    return normalizedMessage(
      'coinbase',
      'spot',
      symbol,
      interval,
      [current.start,current.open,current.high,current.low,current.close,current.volume,current.quoteVolume],
      closed,
      current.trades,
    );
  }

  return {
    url: 'wss://advanced-trade-ws.coinbase.com',
    subscribe: [
      { type: 'subscribe', product_ids: [productId], channel: 'market_trades' },
      { type: 'subscribe', channel: 'heartbeats' },
    ],
    parse(raw) {
      const message = JSON.parse(raw.toString());
      if (message?.channel !== 'market_trades') return null;
      const trades = [];
      for (const event of Array.isArray(message.events) ? message.events : []) {
        for (const trade of Array.isArray(event?.trades) ? event.trades : []) trades.push(trade);
      }
      trades.sort((a, b) => Date.parse(a.time || '') - Date.parse(b.time || ''));
      const outputs = [];
      let changed = false;
      for (const trade of trades) {
        if (String(trade.product_id || '').toUpperCase() !== productId) continue;
        const timestamp = Date.parse(String(trade.time || ''));
        const price = Number(trade.price);
        const size = Number(trade.size);
        if (!Number.isFinite(timestamp) || !Number.isFinite(price) || !Number.isFinite(size)) continue;
        const start = Math.floor(timestamp / bucketMs) * bucketMs;
        if (!candle || start > candle.start) {
          if (candle) outputs.push(candleMessage(candle, true));
          candle = {
            start,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: size,
            quoteVolume: size * price,
            trades: 1,
          };
          changed = true;
        } else if (start === candle.start) {
          candle.high = Math.max(candle.high, price);
          candle.low = Math.min(candle.low, price);
          candle.close = price;
          candle.volume += size;
          candle.quoteVolume += size * price;
          candle.trades += 1;
          changed = true;
        }
      }
      if (changed && candle) outputs.push(candleMessage(candle, false));
      return outputs.length ? outputs : null;
    },
  };
}

async function upstreamConfig(provider, market, symbol, interval) {
  if (provider === 'coinbase') return coinbaseConfig(symbol, interval);
  if (provider === 'binance') return {
    url: market === 'contract'
      ? `wss://fstream.binance.com/ws/${symbol.toLowerCase()}@kline_${interval}`
      : `wss://stream.binance.com:9443/ws/${symbol.toLowerCase()}@kline_${interval}`,
    subscribe: null,
    parse(raw) {
      const message = JSON.parse(raw.toString());
      const kline = message?.k || message?.data?.k;
      if (!kline) return null;
      return normalizedMessage(provider, market, symbol, interval,
        [kline.t,kline.o,kline.h,kline.l,kline.c,kline.v,kline.q], kline.x, kline.n);
    },
  };
  if (provider === 'okx') return {
    url: 'wss://ws.okx.com:8443/ws/v5/business',
    subscribe: { op: 'subscribe', args: [{ channel: okxChannel(interval), instId: okxInstId(symbol, market) }] },
    parse(raw) {
      const message = JSON.parse(raw.toString());
      const candle = Array.isArray(message?.data) ? message.data[0] : null;
      if (!Array.isArray(candle)) return null;
      return normalizedMessage(provider, market, symbol, interval,
        [candle[0],candle[1],candle[2],candle[3],candle[4],candle[5],candle[7]], String(candle[8]) === '1');
    },
  };
  if (provider === 'gate') {
    const contract = market === 'contract';
    return {
      url: contract ? 'wss://fx-ws.gateio.ws/v4/ws/usdt' : 'wss://api.gateio.ws/ws/v4/',
      subscribe: {
        time: Math.floor(Date.now() / 1000),
        channel: contract ? 'futures.candlesticks' : 'spot.candlesticks',
        event: 'subscribe',
        payload: [gateInterval(interval), gateSymbol(symbol)],
      },
      parse(raw) {
        const message = JSON.parse(raw.toString());
        const expected = contract ? 'futures.candlesticks' : 'spot.candlesticks';
        if (message?.channel !== expected || message?.event !== 'update') return null;
        const result = Array.isArray(message.result) ? message.result[0] : message.result;
        if (!result) return null;
        const timestamp = Number(result.t) * (Number(result.t) < 10_000_000_000 ? 1000 : 1);
        return normalizedMessage(provider, market, symbol, interval,
          [timestamp,result.o,result.h,result.l,result.c,result.v,result.a ?? result.sum], false, result.n);
      },
    };
  }
  if (provider === 'bitget') return {
    url: 'wss://ws.bitget.com/v2/ws/public',
    subscribe: {
      op: 'subscribe',
      args: [{
        instType: market === 'contract' ? 'USDT-FUTURES' : 'SPOT',
        channel: bitgetChannel(interval),
        instId: symbol,
      }],
    },
    parse(raw) {
      const message = JSON.parse(raw.toString());
      const candle = Array.isArray(message?.data) ? message.data[0] : null;
      if (!Array.isArray(candle)) return null;
      return normalizedMessage(provider, market, symbol, interval,
        [candle[0],candle[1],candle[2],candle[3],candle[4],candle[5],candle[6]], false);
    },
  };
  if (provider === 'bybit') return {
    url: `wss://stream.bybit.com/v5/public/${market === 'contract' ? 'linear' : 'spot'}`,
    subscribe: { op: 'subscribe', args: [`kline.${bybitInterval(interval)}.${symbol}`] },
    heartbeatMessage: { op: 'ping' },
    parse(raw) {
      const message = JSON.parse(raw.toString());
      if (!String(message?.topic || '').startsWith('kline.')) return null;
      const candle = Array.isArray(message?.data) ? message.data[0] : null;
      if (!candle || typeof candle !== 'object') return null;
      return normalizedMessage(provider, market, symbol, interval,
        [candle.start,candle.open,candle.high,candle.low,candle.close,candle.volume,candle.turnover], candle.confirm === true);
    },
  };
  throw new Error('unsupported provider');
}

const server = http.createServer(async (req, res) => {
  const parsedHttpUrl = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  if (await handleMarketApi(req, res, parsedHttpUrl)) return;
  if (req.url?.startsWith('/health')) {
    res.writeHead(200, {'content-type':'application/json'});
    res.end(JSON.stringify({
      ok: true,
      protocol: 'kaka.market.realtime.v1',
      providers: [...PROVIDERS],
      spot_providers: SPOT_PROVIDERS,
      contract_providers: CONTRACT_PROVIDERS,
      markets: ['spot', 'contract'],
      time: new Date().toISOString(),
    }));
    return;
  }
  if (req.url?.startsWith('/diagnose') || req.url?.startsWith('/browser-test')) {
    res.writeHead(200, {'content-type':'text/html; charset=utf-8'});
    res.end(
      '<!doctype html><meta charset="utf-8"><title>Kaka market realtime</title>' +
      '<h1>Kaka market realtime worker</h1>' +
      '<p>Spot: Binance / Coinbase / OKX / Bybit / Bitget / Gate</p>' +
      '<p>Contract: Binance / OKX / Bybit / Bitget / Gate</p>' +
      '<p>Use /ws?provider=coinbase&market=spot&symbol=BTCUSD&interval=15m</p>',
    );
    return;
  }
  res.writeHead(404, {'content-type':'application/json'});
  res.end(JSON.stringify({ok:false,error:'not found'}));
});

const wss = new WebSocketServer({ noServer: true });
server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  if (url.pathname !== '/ws') {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req, url));
});

wss.on('connection', async (client, _req, parsedUrl) => {
  const provider = providerKey(parsedUrl.searchParams.get('provider'));
  const symbol = symbolKey(parsedUrl.searchParams.get('symbol'));
  const interval = parsedUrl.searchParams.get('interval') || '15m';
  const market = marketKey(parsedUrl.searchParams.get('market'));
  if (!provider || !symbol || !VALID_INTERVALS.has(interval) || !providerMarketAllowed(provider, market)) {
    client.close(1008, 'invalid market channel');
    return;
  }

  let cfg;
  try {
    cfg = await upstreamConfig(provider, market, symbol, interval);
  } catch (error) {
    client.close(1011, String(error).slice(0, 120));
    return;
  }

  let upstream;
  let heartbeat;
  try {
    upstream = new WebSocket(cfg.url, { handshakeTimeout: 15_000 });
  } catch (error) {
    client.close(1011, String(error).slice(0, 120));
    return;
  }

  const cleanup = () => {
    clearInterval(heartbeat);
    try {
      if (upstream?.readyState === WebSocket.OPEN || upstream?.readyState === WebSocket.CONNECTING) upstream.close();
    } catch (_) {}
  };
  client.on('close', cleanup);
  client.on('error', cleanup);

  upstream.on('open', () => {
    const subscriptions = Array.isArray(cfg.subscribe) ? cfg.subscribe : (cfg.subscribe ? [cfg.subscribe] : []);
    for (const subscription of subscriptions) upstream.send(JSON.stringify(subscription));
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({ type:'ready', provider, market, symbol, interval, protocol:'kaka.market.realtime.v1' }));
    }
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
      if (!normalized || client.readyState !== WebSocket.OPEN) return;
      const messages = Array.isArray(normalized) ? normalized : [normalized];
      for (const message of messages) {
        if (message && client.readyState === WebSocket.OPEN) client.send(message);
      }
    } catch (_) {}
  });

  upstream.on('close', (code, reason) => {
    clearInterval(heartbeat);
    if (client.readyState === WebSocket.OPEN) {
      client.close(code === 1000 ? 1000 : 1012, `upstream closed ${reason || ''}`.slice(0, 120));
    }
  });
  upstream.on('error', () => {
    if (client.readyState === WebSocket.OPEN) client.close(1011, 'upstream connection failed');
  });
});

server.listen(PORT, () => console.log(`Kaka market realtime worker 514.0 listening on ${PORT}`));
