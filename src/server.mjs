import { handleMarketApi, fetchMarketKlines } from './market-rest.mjs';
import http from 'node:http';
import { WebSocketServer, WebSocket } from 'ws';

const PORT = Number(process.env.PORT || 10000);
const PROVIDERS = new Set(['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate']);
const SPOT_PROVIDERS = ['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate'];
const CONTRACT_PROVIDERS = ['binance', 'okx', 'bybit', 'bitget', 'gate'];
const VALID_INTERVALS = new Set(['timeline','1s','1m','3m','5m','15m','30m','1h','2h','4h','6h','8h','12h','1d','3d','1w','1M']);

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
    'timeline':60_000,'1s':1_000,'1m':60_000,'3m':180_000,'5m':300_000,'15m':900_000,'30m':1_800_000,
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
  return map[interval] || null;
}
function gateInterval(interval, market) {
  const spot = {
    '1m':'1m','5m':'5m','15m':'15m','30m':'30m','1h':'1h',
    '4h':'4h','8h':'8h','1d':'1d','1w':'7d','1M':'30d',
  };
  const contract = {
    '1m':'1m','5m':'5m','15m':'15m','30m':'30m','1h':'1h',
    '4h':'4h','8h':'8h','1d':'1d','1w':'7d',
  };
  return (market === 'contract' ? contract : spot)[interval] || null;
}
function bitgetChannel(interval) {
  const map = {
    '1m':'candle1m','3m':'candle3m','5m':'candle5m','15m':'candle15m','30m':'candle30m',
    '1h':'candle1H','2h':'candle2H','4h':'candle4H','6h':'candle6H','12h':'candle12H',
    '1d':'candle1D','3d':'candle3D','1w':'candle1W','1M':'candle1M',
  };
  return map[interval] || null;
}
function bybitInterval(interval) {
  const map = {
    '1m':'1','3m':'3','5m':'5','15m':'15','30m':'30','1h':'60','2h':'120',
    '4h':'240','6h':'360','12h':'720','1d':'D','1w':'W','1M':'M',
  };
  return map[interval] || null;
}
function usesRestPolling(provider, interval, upstreamInterval) {
  if (provider === 'coinbase' || provider === 'binance' || interval === 'timeline') return false;
  return upstreamInterval !== interval;
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


function isRealtimeSecondInterval(interval) {
  return interval === '1s';
}

function sourceInterval(provider, market, interval) {
  if (interval === 'timeline') return '1m';
  const fallback = {
    okx: { '8h':'4h' },
    bitget: { '2h':'1h', '8h':'4h' },
    bybit: { '8h':'4h', '3d':'1d' },
  };
  if (provider === 'gate') {
    const gateFallback = market === 'contract'
      ? { '3m':'1m', '2h':'1h', '6h':'1h', '12h':'4h', '3d':'1d', '1M':'1d' }
      : { '3m':'1m', '2h':'1h', '6h':'1h', '12h':'4h', '3d':'1d' };
    return gateFallback[interval] || interval;
  }
  return fallback[provider]?.[interval] || interval;
}

function tradeItem(timestamp, price, size) {
  const time = Number(timestamp);
  const px = Number(price);
  const qty = Math.abs(Number(size));
  if (!Number.isFinite(time) || !Number.isFinite(px) || px <= 0 || !Number.isFinite(qty)) return null;
  const normalizedTime = time < 10_000_000_000 ? time * 1000 : time;
  return { time: normalizedTime, price: px, size: qty };
}

function secondTradeConfig(provider, market, symbol) {
  if (provider === 'binance') {
    return {
      tradeMode: true,
      url: market === 'contract'
        ? `wss://fstream.binance.com/market/ws/${symbol.toLowerCase()}@aggTrade`
        : `wss://stream.binance.com:9443/ws/${symbol.toLowerCase()}@aggTrade`,
      subscribe: null,
      parseTrades(raw) {
        const message = JSON.parse(raw.toString());
        const payload = message?.data ?? message;
        if (payload?.e !== 'aggTrade') return [];
        const item = tradeItem(payload.T ?? payload.E, payload.p, payload.q);
        return item ? [item] : [];
      },
    };
  }
  if (provider === 'coinbase') {
    const productId = coinbaseProductId(symbol);
    return {
      tradeMode: true,
      url: 'wss://advanced-trade-ws.coinbase.com',
      subscribe: [
        { type: 'subscribe', product_ids: [productId], channel: 'market_trades' },
        { type: 'subscribe', channel: 'heartbeats' },
      ],
      parseTrades(raw) {
        const message = JSON.parse(raw.toString());
        if (message?.channel !== 'market_trades') return [];
        const items = [];
        for (const event of Array.isArray(message.events) ? message.events : []) {
          for (const trade of Array.isArray(event?.trades) ? event.trades : []) {
            if (String(trade.product_id || '').toUpperCase() !== productId) continue;
            const item = tradeItem(Date.parse(String(trade.time || '')), trade.price, trade.size);
            if (item) items.push(item);
          }
        }
        return items;
      },
    };
  }
  if (provider === 'okx') {
    return {
      tradeMode: true,
      url: 'wss://ws.okx.com:8443/ws/v5/public',
      subscribe: { op: 'subscribe', args: [{ channel: 'trades', instId: okxInstId(symbol, market) }] },
      parseTrades(raw) {
        const message = JSON.parse(raw.toString());
        if (message?.arg?.channel !== 'trades') return [];
        const items = [];
        for (const trade of Array.isArray(message?.data) ? message.data : []) {
          const item = tradeItem(trade.ts, trade.px ?? trade.price, trade.sz ?? trade.size);
          if (item) items.push(item);
        }
        return items;
      },
    };
  }
  if (provider === 'bybit') {
    return {
      tradeMode: true,
      url: `wss://stream.bybit.com/v5/public/${market === 'contract' ? 'linear' : 'spot'}`,
      subscribe: { op: 'subscribe', args: [`publicTrade.${symbol}`] },
      heartbeatMessage: { op: 'ping' },
      parseTrades(raw) {
        const message = JSON.parse(raw.toString());
        if (!String(message?.topic || '').startsWith('publicTrade.')) return [];
        const items = [];
        for (const trade of Array.isArray(message?.data) ? message.data : []) {
          const item = tradeItem(trade.T ?? message.ts, trade.p ?? trade.price, trade.v ?? trade.size);
          if (item) items.push(item);
        }
        return items;
      },
    };
  }
  if (provider === 'bitget') {
    return {
      tradeMode: true,
      url: 'wss://ws.bitget.com/v2/ws/public',
      subscribe: {
        op: 'subscribe',
        args: [{
          instType: market === 'contract' ? 'USDT-FUTURES' : 'SPOT',
          channel: 'trade',
          instId: symbol,
        }],
      },
      parseTrades(raw) {
        const message = JSON.parse(raw.toString());
        if (message?.arg?.channel !== 'trade') return [];
        const items = [];
        for (const trade of Array.isArray(message?.data) ? message.data : []) {
          if (Array.isArray(trade)) {
            const item = tradeItem(trade[0], trade[1], trade[2]);
            if (item) items.push(item);
          } else if (trade && typeof trade === 'object') {
            const item = tradeItem(trade.ts ?? message.ts, trade.price ?? trade.px, trade.size ?? trade.sz);
            if (item) items.push(item);
          }
        }
        return items;
      },
    };
  }
  if (provider === 'gate') {
    const contract = market === 'contract';
    return {
      tradeMode: true,
      url: contract ? 'wss://fx-ws.gateio.ws/v4/ws/usdt' : 'wss://api.gateio.ws/ws/v4/',
      subscribe: {
        time: Math.floor(Date.now() / 1000),
        channel: contract ? 'futures.trades' : 'spot.trades',
        event: 'subscribe',
        payload: [gateSymbol(symbol)],
      },
      parseTrades(raw) {
        const message = JSON.parse(raw.toString());
        const expected = contract ? 'futures.trades' : 'spot.trades';
        if (message?.channel !== expected || message?.event !== 'update') return [];
        const list = Array.isArray(message?.result) ? message.result : (message?.result ? [message.result] : []);
        const items = [];
        for (const trade of list) {
          const time = trade.create_time_ms ?? trade.create_time ?? trade.time_ms ?? trade.time;
          const size = contract ? (trade.size ?? trade.amount) : (trade.amount ?? trade.size);
          const item = tradeItem(time, trade.price, size);
          if (item) items.push(item);
        }
        return items;
      },
    };
  }
  throw new Error('unsupported trade provider');
}

function createSecondTradeAggregator({ provider, market, symbol, interval, client }) {
  let candle = null;
  let lastOfficialPrice = null;
  let lastTradeAt = 0;
  let lastSentSignature = '';

  function sendCandle(current, closed) {
    if (!current || client.readyState !== WebSocket.OPEN) return;
    const signature = `${current.start}:${current.open}:${current.high}:${current.low}:${current.close}:${current.volume}:${current.trades}:${closed}`;
    if (signature === lastSentSignature) return;
    lastSentSignature = signature;
    client.send(normalizedMessage(
      provider,
      market,
      symbol,
      interval,
      [current.start,current.open,current.high,current.low,current.close,current.volume,current.quoteVolume],
      closed,
      current.trades,
    ));
  }

  function newFlatCandle(start, price) {
    return {
      start,
      open: price,
      high: price,
      low: price,
      close: price,
      volume: 0,
      quoteVolume: 0,
      trades: 0,
    };
  }

  function advanceTo(targetStart) {
    if (!candle || lastOfficialPrice == null || targetStart <= candle.start) return;
    sendCandle(candle, true);
    const missing = Math.min(120, Math.max(0, Math.floor((targetStart - candle.start) / 1000) - 1));
    let nextStart = candle.start + 1000;
    for (let i = 0; i < missing; i++, nextStart += 1000) {
      const flat = newFlatCandle(nextStart, lastOfficialPrice);
      sendCandle(flat, true);
    }
    candle = newFlatCandle(targetStart, lastOfficialPrice);
  }

  function ingest(rawTrades) {
    const now = Date.now();
    const trades = (Array.isArray(rawTrades) ? rawTrades : [])
      .filter((item) => item && item.time >= now - 120_000 && item.time <= now + 10_000)
      .sort((a, b) => a.time - b.time);
    let changed = false;
    for (const trade of trades) {
      const start = Math.floor(trade.time / 1000) * 1000;
      if (!candle) {
        candle = newFlatCandle(start, trade.price);
        candle.volume = trade.size;
        candle.quoteVolume = trade.size * trade.price;
        candle.trades = 1;
      } else if (start > candle.start) {
        advanceTo(start);
        candle.open = trade.price;
        candle.high = trade.price;
        candle.low = trade.price;
        candle.close = trade.price;
        candle.volume = trade.size;
        candle.quoteVolume = trade.size * trade.price;
        candle.trades = 1;
      } else if (start === candle.start) {
        candle.high = Math.max(candle.high, trade.price);
        candle.low = Math.min(candle.low, trade.price);
        candle.close = trade.price;
        candle.volume += trade.size;
        candle.quoteVolume += trade.size * trade.price;
        candle.trades += 1;
      } else {
        continue;
      }
      lastOfficialPrice = trade.price;
      lastTradeAt = Math.max(lastTradeAt, trade.time);
      changed = true;
    }
    if (changed) sendCandle(candle, false);
  }

  function tick() {
    if (!candle || lastOfficialPrice == null) return;
    const nowBucket = Math.floor(Date.now() / 1000) * 1000;
    if (nowBucket > candle.start) advanceTo(nowBucket);
    sendCandle(candle, false);
  }

  function seedRows(rows) {
    const sorted = (Array.isArray(rows) ? rows : [])
      .filter((row) => row && Number.isFinite(Number(row.open_time_ms)) && Number(row.close) > 0)
      .sort((a, b) => Number(a.open_time_ms) - Number(b.open_time_ms));
    for (const row of sorted) {
      if (client.readyState !== WebSocket.OPEN) break;
      client.send(normalizedMessage(
        provider,
        market,
        symbol,
        interval,
        [row.open_time_ms,row.open,row.high,row.low,row.close,row.volume,row.quote_volume],
        Number(row.open_time_ms) + 1000 <= Date.now(),
        row.trade_count,
      ));
    }
    const latest = sorted.at(-1);
    if (!latest) return;
    const latestStart = Number(latest.open_time_ms);
    if (!candle || latestStart > candle.start) {
      candle = {
        start: latestStart,
        open: Number(latest.open),
        high: Number(latest.high),
        low: Number(latest.low),
        close: Number(latest.close),
        volume: Number(latest.volume || 0),
        quoteVolume: Number(latest.quote_volume || 0),
        trades: Number(latest.trade_count || 0),
      };
      lastOfficialPrice = candle.close;
      lastTradeAt = latestStart;
    }
  }

  return {
    ingest,
    tick,
    seedRows,
    status() {
      return { hasTrade: lastOfficialPrice != null, lastTradeAt };
    },
  };
}

async function coinbaseConfig(symbol, interval, outputInterval = interval) {
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
      outputInterval,
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
  const upstreamInterval = sourceInterval(provider, market, interval);
  if (isRealtimeSecondInterval(interval) && provider === 'binance' && market === 'spot') return {
    url: `wss://stream.binance.com:9443/ws/${symbol.toLowerCase()}@kline_1s`,
    subscribe: null,
    parse(raw) {
      const message = JSON.parse(raw.toString());
      const kline = message?.k || message?.data?.k;
      if (!kline) return null;
      return normalizedMessage(provider, market, symbol, interval,
        [kline.t,kline.o,kline.h,kline.l,kline.c,kline.v,kline.q], kline.x, kline.n);
    },
  };
  if (isRealtimeSecondInterval(interval) && provider === 'binance' && market === 'contract') return {
    url: `wss://fstream.binance.com/market/ws/${symbol.toLowerCase()}_perpetual@continuousKline_1s`,
    subscribe: null,
    parse(raw) {
      const message = JSON.parse(raw.toString());
      const kline = message?.k || message?.data?.k;
      if (!kline) return null;
      return normalizedMessage(provider, market, symbol, interval,
        [kline.t,kline.o,kline.h,kline.l,kline.c,kline.v,kline.q], kline.x, kline.n);
    },
  };
  if (isRealtimeSecondInterval(interval)) return secondTradeConfig(provider, market, symbol);
  if (provider === 'coinbase') return coinbaseConfig(symbol, upstreamInterval, interval);
  if (usesRestPolling(provider, interval, upstreamInterval)) return { restPoll: true, sourceInterval: upstreamInterval };
  if (provider === 'binance') return {
    url: market === 'contract'
      ? `wss://fstream.binance.com/market/ws/${symbol.toLowerCase()}@kline_${upstreamInterval}`
      : `wss://stream.binance.com:9443/ws/${symbol.toLowerCase()}@kline_${upstreamInterval}`,
    subscribe: null,
    parse(raw) {
      const message = JSON.parse(raw.toString());
      const kline = message?.k || message?.data?.k;
      if (!kline) return null;
      return normalizedMessage(provider, market, symbol, interval,
        [kline.t,kline.o,kline.h,kline.l,kline.c,kline.v,kline.q], kline.x, kline.n);
    },
  };
  if (provider === 'okx') {
    const channel = okxChannel(upstreamInterval);
    if (!channel) throw new Error(`okx interval ${upstreamInterval} is not supported`);
    return {
      url: 'wss://ws.okx.com:8443/ws/v5/business',
      subscribe: { op: 'subscribe', args: [{ channel, instId: okxInstId(symbol, market) }] },
      parse(raw) {
        const message = JSON.parse(raw.toString());
        const candle = Array.isArray(message?.data) ? message.data[0] : null;
        if (!Array.isArray(candle)) return null;
        return normalizedMessage(provider, market, symbol, interval,
          [candle[0],candle[1],candle[2],candle[3],candle[4],candle[5],candle[7]], String(candle[8]) === '1');
      },
    };
  }
  if (provider === 'gate') {
    const contract = market === 'contract';
    const channelInterval = gateInterval(upstreamInterval, market);
    if (!channelInterval) throw new Error(`gate ${market} interval ${upstreamInterval} is not supported`);
    return {
      url: contract ? 'wss://fx-ws.gateio.ws/v4/ws/usdt' : 'wss://api.gateio.ws/ws/v4/',
      subscribe: {
        time: Math.floor(Date.now() / 1000),
        channel: contract ? 'futures.candlesticks' : 'spot.candlesticks',
        event: 'subscribe',
        payload: [channelInterval, gateSymbol(symbol)],
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
  if (provider === 'bitget') {
    const channel = bitgetChannel(upstreamInterval);
    if (!channel) throw new Error(`bitget ${market} interval ${upstreamInterval} is not supported`);
    return {
      url: 'wss://ws.bitget.com/v2/ws/public',
      subscribe: {
        op: 'subscribe',
        args: [{
          instType: market === 'contract' ? 'USDT-FUTURES' : 'SPOT',
          channel,
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
  }
  if (provider === 'bybit') {
    const channelInterval = bybitInterval(upstreamInterval);
    if (!channelInterval) throw new Error(`bybit interval ${upstreamInterval} is not supported`);
    return {
      url: `wss://stream.bybit.com/v5/public/${market === 'contract' ? 'linear' : 'spot'}`,
      subscribe: { op: 'subscribe', args: [`kline.${channelInterval}.${symbol}`] },
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
  }
  throw new Error('unsupported provider');
}

const server = http.createServer(async (req, res) => {
  const parsedHttpUrl = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  if (process.env.KAKA_DISABLE_MARKET_API !== '1' && await handleMarketApi(req, res, parsedHttpUrl)) return;
  if (req.url?.startsWith('/health')) {
    res.writeHead(200, {'content-type':'application/json'});
    res.end(JSON.stringify({
      ok: true,
      version: '515.1.2',
      protocol: 'kaka.market.realtime.v1',
      realtime_intervals: ['timeline', '1s'],
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
  let restPollTimer;
  let secondTickTimer;
  let secondAggregator;
  let restPollBusy = false;
  if (cfg.restPoll === true) {
    const sendLatest = async () => {
      if (restPollBusy || client.readyState !== WebSocket.OPEN) return;
      restPollBusy = true;
      try {
        const rows = await fetchMarketKlines(provider, market, symbol, cfg.sourceInterval || interval, Date.now(), 3);
        const latest = rows.at(-1);
        if (!latest || client.readyState !== WebSocket.OPEN) return;
        client.send(normalizedMessage(
          provider,
          market,
          symbol,
          interval,
          [latest.open_time_ms,latest.open,latest.high,latest.low,latest.close,latest.volume,latest.quote_volume],
          Date.now() > Date.parse(latest.close_time || ''),
          latest.trade_count,
        ));
      } catch (_) {
        // 保持连接，下一轮继续读取同平台官方公开K线；绝不跨平台回落。
      } finally {
        restPollBusy = false;
      }
    };
    client.send(JSON.stringify({ type:'ready', provider, market, symbol, interval, protocol:'kaka.market.realtime.v1', mode:'official_rest_poll' }));
    await sendLatest();
    restPollTimer = setInterval(sendLatest, 2500);
    heartbeat = setInterval(() => {
      if (client.readyState === WebSocket.OPEN) client.ping();
    }, 20_000);
    const cleanupPoll = () => {
      clearInterval(restPollTimer);
      clearInterval(heartbeat);
    };
    client.on('close', cleanupPoll);
    client.on('error', cleanupPoll);
    return;
  }
  try {
    upstream = new WebSocket(cfg.url, { handshakeTimeout: 15_000 });
  } catch (error) {
    client.close(1011, String(error).slice(0, 120));
    return;
  }

  const cleanup = () => {
    clearInterval(heartbeat);
    clearInterval(secondTickTimer);
    try {
      if (upstream?.readyState === WebSocket.OPEN || upstream?.readyState === WebSocket.CONNECTING) upstream.close();
    } catch (_) {}
  };
  client.on('close', cleanup);
  client.on('error', cleanup);

  upstream.on('open', () => {
    const subscriptions = Array.isArray(cfg.subscribe) ? cfg.subscribe : (cfg.subscribe ? [cfg.subscribe] : []);
    for (const subscription of subscriptions) upstream.send(JSON.stringify(subscription));
    if (cfg.tradeMode === true) {
      secondAggregator = createSecondTradeAggregator({ provider, market, symbol, interval, client });
      secondTickTimer = setInterval(() => secondAggregator?.tick(), 250);
      fetchMarketKlines(provider, market, symbol, '1s', Date.now(), 500)
        .then((historyRows) => secondAggregator?.seedRows(historyRows))
        .catch(() => {
          // 某平台最近成交历史暂不可用时继续实时流，不跨平台回落。
        });
    }
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({
        type:'ready', provider, market, symbol, interval,
        protocol:'kaka.market.realtime.v1',
        mode: cfg.tradeMode === true ? 'official_public_trade_1s' : 'official_public_kline',
      }));
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
      if (cfg.tradeMode === true) {
        const trades = cfg.parseTrades(raw);
        secondAggregator?.ingest(trades);
        return;
      }
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

server.listen(PORT, () => console.log(`Kaka market realtime worker 515.1.2 listening on ${PORT}`));
