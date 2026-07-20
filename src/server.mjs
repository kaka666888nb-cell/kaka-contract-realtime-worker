import { handleMarketApi, fetchMarketKlines, resolveNativeMarketIdentity } from './market-rest.mjs';
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

function secondTradeConfig(provider, market, symbol, nativeSymbol = symbol, quoteAsset = splitSymbol(symbol)[1]) {
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
      subscribe: { op: 'subscribe', args: [`publicTrade.${nativeSymbol}`] },
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
          instType: market === 'contract'
            ? (String(quoteAsset).toUpperCase() === 'USDC' ? 'USDC-FUTURES' : 'USDT-FUTURES')
            : 'SPOT',
          channel: 'trade',
          instId: nativeSymbol,
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

function createSecondTradeAggregator({ provider, market, symbol, interval, client = null, emit = null }) {
  let candle = null;
  let lastOfficialPrice = null;
  let lastTradeAt = 0;
  let lastSentSignature = '';

  function sendCandle(current, closed) {
    if (!current) return;
    if (!emit && client?.readyState !== WebSocket.OPEN) return;
    const signature = `${current.start}:${current.open}:${current.high}:${current.low}:${current.close}:${current.volume}:${current.trades}:${closed}`;
    if (signature === lastSentSignature) return;
    lastSentSignature = signature;
    const payload = normalizedMessage(
      provider,
      market,
      symbol,
      interval,
      [current.start,current.open,current.high,current.low,current.close,current.volume,current.quoteVolume],
      closed,
      current.trades,
    );
    if (emit) emit(payload);
    else client.send(payload);
  }

  function newTradeCandle(start, trade) {
    return {
      start,
      open: trade.price,
      high: trade.price,
      low: trade.price,
      close: trade.price,
      volume: trade.size,
      quoteVolume: trade.size * trade.price,
      trades: 1,
    };
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
        candle = newTradeCandle(start, trade);
      } else if (start > candle.start) {
        sendCandle(candle, true);
        candle = newTradeCandle(start, trade);
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
    const nowBucket = Math.floor(Date.now() / 1000) * 1000;

    // Step650.8.15.11: a 1-second chart is a wall-clock series. When the venue has
    // no trade in a second, carry the last official price into a zero-volume,
    // zero-trade candle for that second. This is display continuity only: the price
    // comes from the same venue's latest official trade, no cross-venue value is used,
    // and no volume/trade is fabricated.
    if (!Number.isFinite(lastOfficialPrice) || lastOfficialPrice <= 0) return;

    if (candle && nowBucket > candle.start) {
      // Only resend a closing update when the prior second contained a real trade.
      // Pure carry-forward candles were already emitted once, so avoiding a second
      // close message keeps visible-only WebSocket bandwidth near one message/second.
      if (Number(candle.trades || 0) > 0 || Number(candle.volume || 0) > 0) {
        sendCandle(candle, true);
      }
      candle = null;
    }

    if (!candle || nowBucket > candle.start) {
      candle = {
        start: nowBucket,
        open: lastOfficialPrice,
        high: lastOfficialPrice,
        low: lastOfficialPrice,
        close: lastOfficialPrice,
        volume: 0,
        quoteVolume: 0,
        trades: 0,
      };
      sendCandle(candle, false);
    }
  }

  function seedRows(rows) {
    const sorted = (Array.isArray(rows) ? rows : [])
      .filter((row) =>
        row &&
        Number.isFinite(Number(row.open_time_ms)) &&
        Number(row.close) > 0 &&
        Number(row.trade_count || 0) > 0
      )
      .sort((a, b) => Number(a.open_time_ms) - Number(b.open_time_ms));
    for (const row of sorted) {
      if (!emit && client?.readyState !== WebSocket.OPEN) break;
      const payload = normalizedMessage(
        provider,
        market,
        symbol,
        interval,
        [row.open_time_ms,row.open,row.high,row.low,row.close,row.volume,row.quote_volume],
        Number(row.open_time_ms) + 1000 <= Date.now(),
        row.trade_count,
      );
      if (emit) emit(payload);
      else client.send(payload);
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
  let nativeSymbol = symbol;
  let quoteAsset = splitSymbol(symbol)[1];
  if (market === 'contract' && (provider === 'bybit' || provider === 'bitget')) {
    const identity = await resolveNativeMarketIdentity(provider, market, symbol);
    nativeSymbol = symbolKey(identity.native_symbol || identity.raw_symbol || symbol);
    quoteAsset = String(identity.quote_asset || quoteAsset).toUpperCase();
  }
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
  if (isRealtimeSecondInterval(interval)) return secondTradeConfig(provider, market, symbol, nativeSymbol, quoteAsset);
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
          instType: market === 'contract'
            ? (quoteAsset === 'USDC' ? 'USDC-FUTURES' : 'USDT-FUTURES')
            : 'SPOT',
          channel,
          instId: nativeSymbol,
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
      subscribe: { op: 'subscribe', args: [`kline.${channelInterval}.${nativeSymbol}`] },
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


const BINANCE_SHARED_STREAM_MAX = 64;
const BINANCE_SHARED_IDLE_MS = 30_000;
const BINANCE_SHARED_RECONNECT_MAX_MS = 30_000;
const BINANCE_SHARED_CONNECT_GAP_MS = 1_500;
const BINANCE_SHARED_MAX_CONNECT_ATTEMPTS_5M = 60;
const BINANCE_SHARED_MAX_TOTAL_CLIENTS = 1000;
const BINANCE_SHARED_MAX_CLIENTS_PER_STREAM = 250;
const BINANCE_SHARED_MAX_CLIENT_BUFFERED_BYTES = 1_000_000;
const BINANCE_SHARED_MAX_CLIENTS_PER_IP = 50;
const BINANCE_SHARED_MAX_STREAMS_PER_IP = 16;
const BINANCE_SHARED_MAX_CONNECT_ATTEMPTS_PER_IP_1M = 60;
const binanceSharedStreams = new Map();
const binanceClientsByIp = new Map();
const binanceStreamsByIp = new Map();
const binanceConnectAttemptsByIp = new Map();
const binanceConnectAttempts = [];
let binanceConnectChain = Promise.resolve();
let binanceLastConnectAt = 0;
const binanceSharedStats = {
  created: 0,
  reused: 0,
  rejected_capacity: 0,
  reconnects: 0,
  connect_rate_waits: 0,
  connect_rate_rejections: 0,
  upstream_messages: 0,
  downstream_messages: 0,
  slow_client_disconnects: 0,
  downstream_ip_capacity_rejections: 0,
  downstream_ip_rate_rejections: 0,
  last_error: '',
};

function binanceSharedStreamKey(market, symbol, interval) {
  return `${market}|${symbol}|${interval}`;
}

function sendWsSafe(client, payload) {
  if (client?.readyState !== WebSocket.OPEN) return false;
  if (Number(client.bufferedAmount || 0) > BINANCE_SHARED_MAX_CLIENT_BUFFERED_BYTES) {
    binanceSharedStats.slow_client_disconnects += 1;
    try { client.close(1013, 'slow client'); } catch (_) {}
    return false;
  }
  try {
    client.send(payload);
    return true;
  } catch (_) {
    return false;
  }
}

function broadcastBinanceShared(entry, payload, { remember = true } = {}) {
  if (remember) entry.lastPayload = payload;
  for (const client of [...entry.clients]) {
    const sent = sendWsSafe(client, payload);
    if (!sent) {
      if (client.readyState !== WebSocket.OPEN) entry.clients.delete(client);
      continue;
    }
    binanceSharedStats.downstream_messages += 1;
  }
}

function binanceReadyMessage(entry) {
  return JSON.stringify({
    type: 'ready',
    provider: 'binance',
    market: entry.market,
    symbol: entry.symbol,
    interval: entry.interval,
    protocol: 'kaka.market.realtime.v1',
    mode: entry.cfg.tradeMode === true ? 'official_public_trade_1s_shared' : 'official_public_kline_shared',
    shared_upstream: true,
  });
}

function pruneBinanceConnectAttempts() {
  const cutoff = Date.now() - 5 * 60_000;
  while (binanceConnectAttempts.length && binanceConnectAttempts[0] < cutoff) {
    binanceConnectAttempts.shift();
  }
}

async function acquireBinanceConnectSlot() {
  let release;
  const previous = binanceConnectChain;
  binanceConnectChain = new Promise((resolve) => { release = resolve; });
  await previous;
  try {
    pruneBinanceConnectAttempts();
    if (binanceConnectAttempts.length >= BINANCE_SHARED_MAX_CONNECT_ATTEMPTS_5M) {
      binanceSharedStats.connect_rate_rejections += 1;
      throw new Error('binance_shared_ws_connect_rate_limited');
    }
    const waitMs = Math.max(0, BINANCE_SHARED_CONNECT_GAP_MS - (Date.now() - binanceLastConnectAt));
    if (waitMs > 0) {
      binanceSharedStats.connect_rate_waits += 1;
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
    binanceLastConnectAt = Date.now();
    binanceConnectAttempts.push(binanceLastConnectAt);
  } finally {
    release();
  }
}

function closeBinanceSharedEntry(entry, reason = 'closed') {
  entry.closed = true;
  clearTimeout(entry.reconnectTimer);
  clearTimeout(entry.idleTimer);
  clearInterval(entry.heartbeat);
  clearInterval(entry.secondTickTimer);
  entry.reconnectTimer = null;
  entry.idleTimer = null;
  entry.heartbeat = null;
  entry.secondTickTimer = null;
  entry.secondAggregator = null;
  try {
    if (entry.upstream?.readyState === WebSocket.OPEN || entry.upstream?.readyState === WebSocket.CONNECTING) {
      entry.upstream.close(1000, reason);
    }
  } catch (_) {}
  entry.upstream = null;
  if (binanceSharedStreams.get(entry.key) === entry) binanceSharedStreams.delete(entry.key);
}

function evictIdleBinanceSharedStreams() {
  for (const entry of [...binanceSharedStreams.values()]) {
    if (entry.clients.size === 0) closeBinanceSharedEntry(entry, 'capacity_evict_idle');
    if (binanceSharedStreams.size < BINANCE_SHARED_STREAM_MAX) break;
  }
}

function scheduleBinanceSharedReconnect(entry) {
  if (entry.closed || entry.reconnectTimer || entry.clients.size === 0) return;
  const delay = Math.min(
    BINANCE_SHARED_RECONNECT_MAX_MS,
    1_000 * (2 ** Math.min(entry.reconnectAttempt, 5)),
  );
  entry.reconnectAttempt += 1;
  binanceSharedStats.reconnects += 1;
  entry.reconnectTimer = setTimeout(() => {
    entry.reconnectTimer = null;
    connectBinanceSharedEntry(entry).catch(() => {});
  }, delay);
  entry.reconnectTimer.unref?.();
}

async function connectBinanceSharedEntry(entry) {
  if (entry.closed || entry.clients.size === 0) return;
  if (entry.upstream?.readyState === WebSocket.OPEN || entry.connecting) return entry.connecting;
  entry.connecting = (async () => {
    await acquireBinanceConnectSlot();
    if (entry.closed || entry.clients.size === 0) return;
    const upstream = new WebSocket(entry.cfg.url, { handshakeTimeout: 15_000 });
    entry.upstream = upstream;
    await new Promise((resolve, reject) => {
      let settled = false;
      const timer = setTimeout(() => {
        if (settled) return;
        settled = true;
        try { upstream.terminate(); } catch (_) {}
        reject(new Error('binance_shared_ws_open_timeout'));
      }, 16_000);
      timer.unref?.();
      upstream.once('open', () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        resolve();
      });
      upstream.once('error', (error) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        reject(error);
      });
    });
    entry.reconnectAttempt = 0;
    const subscriptions = Array.isArray(entry.cfg.subscribe)
      ? entry.cfg.subscribe
      : (entry.cfg.subscribe ? [entry.cfg.subscribe] : []);
    for (const subscription of subscriptions) upstream.send(JSON.stringify(subscription));
    if (entry.cfg.tradeMode === true) {
      clearInterval(entry.secondTickTimer);
      entry.secondAggregator = createSecondTradeAggregator({
        provider: 'binance',
        market: entry.market,
        symbol: entry.symbol,
        interval: entry.interval,
        emit: (payload) => broadcastBinanceShared(entry, payload),
      });
      entry.secondTickTimer = setInterval(() => entry.secondAggregator?.tick(), 250);
      entry.secondTickTimer.unref?.();
    }
    broadcastBinanceShared(entry, binanceReadyMessage(entry), { remember: false });
    clearInterval(entry.heartbeat);
    entry.heartbeat = setInterval(() => {
      if (upstream.readyState === WebSocket.OPEN) {
        try { upstream.ping(); } catch (_) {}
      }
      for (const client of entry.clients) {
        if (client.readyState === WebSocket.OPEN) {
          try { client.ping(); } catch (_) {}
        }
      }
    }, 20_000);
    entry.heartbeat.unref?.();

    upstream.on('message', (raw) => {
      try {
        if (entry.cfg.tradeMode === true) {
          const trades = entry.cfg.parseTrades(raw);
          if (Array.isArray(trades) && trades.length) {
            binanceSharedStats.upstream_messages += trades.length;
            entry.secondAggregator?.ingest(trades);
          }
          return;
        }
        const normalized = entry.cfg.parse(raw);
        if (!normalized) return;
        const messages = Array.isArray(normalized) ? normalized : [normalized];
        for (const message of messages) {
          if (!message) continue;
          binanceSharedStats.upstream_messages += 1;
          broadcastBinanceShared(entry, message);
        }
      } catch (_) {}
    });
    upstream.on('close', () => {
      clearInterval(entry.heartbeat);
      clearInterval(entry.secondTickTimer);
      entry.heartbeat = null;
      entry.secondTickTimer = null;
      entry.secondAggregator = null;
      if (entry.upstream === upstream) entry.upstream = null;
      scheduleBinanceSharedReconnect(entry);
    });
    upstream.on('error', (error) => {
      binanceSharedStats.last_error = String(error?.message || error);
    });
  })().catch((error) => {
    binanceSharedStats.last_error = String(error?.message || error);
    if (entry.upstream) {
      try { entry.upstream.terminate(); } catch (_) {}
      entry.upstream = null;
    }
    scheduleBinanceSharedReconnect(entry);
    throw error;
  }).finally(() => {
    entry.connecting = null;
  });
  return entry.connecting;
}

function totalBinanceSharedClients() {
  return [...binanceSharedStreams.values()].reduce((sum, entry) => sum + entry.clients.size, 0);
}

async function attachBinanceSharedClient(client, market, symbol, interval, cfg) {
  const key = binanceSharedStreamKey(market, symbol, interval);
  let entry = binanceSharedStreams.get(key);
  if (totalBinanceSharedClients() >= BINANCE_SHARED_MAX_TOTAL_CLIENTS ||
      (entry && entry.clients.size >= BINANCE_SHARED_MAX_CLIENTS_PER_STREAM)) {
    binanceSharedStats.rejected_capacity += 1;
    client.close(1013, 'binance shared downstream capacity reached');
    return;
  }
  if (!entry) {
    if (binanceSharedStreams.size >= BINANCE_SHARED_STREAM_MAX) evictIdleBinanceSharedStreams();
    if (binanceSharedStreams.size >= BINANCE_SHARED_STREAM_MAX) {
      binanceSharedStats.rejected_capacity += 1;
      client.close(1013, 'binance shared stream capacity reached');
      return;
    }
    entry = {
      key,
      market,
      symbol,
      interval,
      cfg,
      clients: new Set(),
      upstream: null,
      connecting: null,
      reconnectTimer: null,
      reconnectAttempt: 0,
      heartbeat: null,
      idleTimer: null,
      closed: false,
      createdAt: Date.now(),
      secondAggregator: null,
      secondTickTimer: null,
      lastPayload: null,
    };
    binanceSharedStreams.set(key, entry);
    binanceSharedStats.created += 1;
  } else {
    binanceSharedStats.reused += 1;
  }
  clearTimeout(entry.idleTimer);
  entry.idleTimer = null;
  entry.clients.add(client);

  const cleanup = () => {
    entry.clients.delete(client);
    if (entry.clients.size === 0 && !entry.closed) {
      clearTimeout(entry.idleTimer);
      entry.idleTimer = setTimeout(() => closeBinanceSharedEntry(entry, 'idle'), BINANCE_SHARED_IDLE_MS);
      entry.idleTimer.unref?.();
    }
  };
  client.on('close', cleanup);
  client.on('error', cleanup);

  if (entry.upstream?.readyState === WebSocket.OPEN) {
    sendWsSafe(client, binanceReadyMessage(entry));
    if (entry.lastPayload) sendWsSafe(client, entry.lastPayload);
    return;
  }
  try {
    await connectBinanceSharedEntry(entry);
  } catch (_) {
    if (client.readyState === WebSocket.OPEN) {
      sendWsSafe(client, JSON.stringify({
        type: 'status',
        provider: 'binance',
        market,
        symbol,
        interval,
        status: 'reconnecting',
      }));
    }
  }
}

function binanceSharedWsHealth() {
  pruneBinanceConnectAttempts();
  return {
    enabled: true,
    active_streams: binanceSharedStreams.size,
    max_streams: BINANCE_SHARED_STREAM_MAX,
    total_clients: totalBinanceSharedClients(),
    max_total_clients: BINANCE_SHARED_MAX_TOTAL_CLIENTS,
    max_clients_per_stream: BINANCE_SHARED_MAX_CLIENTS_PER_STREAM,
    max_client_buffered_bytes: BINANCE_SHARED_MAX_CLIENT_BUFFERED_BYTES,
    official_production_hosts: ['fstream.binance.com/market', 'fstream.binance.com/public', 'stream.binance.com:9443'],
    futures_ws_route_migration: 'market_public_split',
    futures_ws_legacy_root_disabled: true,
    futures_ws_market_channels: ['kline','continuousKline','aggTrade','ticker','contractInfo','forceOrder'],
    futures_ws_public_channels: ['bookTicker','depth'],
    max_clients_per_ip: BINANCE_SHARED_MAX_CLIENTS_PER_IP,
    max_streams_per_ip: BINANCE_SHARED_MAX_STREAMS_PER_IP,
    max_connect_attempts_per_ip_1m: BINANCE_SHARED_MAX_CONNECT_ATTEMPTS_PER_IP_1M,
    tracked_downstream_ips: binanceClientsByIp.size,
    connect_attempts_5m: binanceConnectAttempts.length,
    max_connect_attempts_5m: BINANCE_SHARED_MAX_CONNECT_ATTEMPTS_5M,
    connect_gap_ms: BINANCE_SHARED_CONNECT_GAP_MS,
    idle_ms: BINANCE_SHARED_IDLE_MS,
    streams: [...binanceSharedStreams.values()].map((entry) => ({
      market: entry.market,
      symbol: entry.symbol,
      interval: entry.interval,
      clients: entry.clients.size,
      connected: entry.upstream?.readyState === WebSocket.OPEN,
      reconnect_attempt: entry.reconnectAttempt,
    })),
    ...binanceSharedStats,
  };
}

const server = http.createServer(async (req, res) => {
  const parsedHttpUrl = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  if (process.env.KAKA_DISABLE_MARKET_API !== '1' && await handleMarketApi(req, res, parsedHttpUrl)) return;
  if (req.url?.startsWith('/ws-health')) {
    res.writeHead(200, {'content-type':'application/json','cache-control':'no-store'});
    res.end(JSON.stringify({ ok: true, version: '650.8.15.11', binance_shared_ws: binanceSharedWsHealth(), time: new Date().toISOString() }));
    return;
  }
  if (req.url?.startsWith('/health')) {
    res.writeHead(200, {'content-type':'application/json'});
    res.end(JSON.stringify({
      ok: true,
      version: '650.8.15.11',
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

function downstreamClientIp(req) {
  // Render documents that the first X-Forwarded-For entry is the real client
  // address. Use that platform-normalized value for downstream connection caps.
  const forwarded = String(req?.headers?.['x-forwarded-for'] || '')
    .split(',')
    .map((part) => part.trim())
    .find(Boolean) || '';
  return forwarded || String(req?.socket?.remoteAddress || 'unknown');
}

function pruneDownstreamIpAttempts(ip) {
  const cutoff = Date.now() - 60_000;
  const attempts = binanceConnectAttemptsByIp.get(ip) || [];
  while (attempts.length && attempts[0] < cutoff) attempts.shift();
  if (attempts.length) binanceConnectAttemptsByIp.set(ip, attempts);
  else binanceConnectAttemptsByIp.delete(ip);
  return attempts;
}

function registerBinanceDownstreamClient(client, req, streamKey) {
  const ip = downstreamClientIp(req);
  const attempts = pruneDownstreamIpAttempts(ip);
  if (attempts.length >= BINANCE_SHARED_MAX_CONNECT_ATTEMPTS_PER_IP_1M) {
    binanceSharedStats.downstream_ip_rate_rejections += 1;
    return { ok: false, reason: 'binance downstream IP rate limit' };
  }
  attempts.push(Date.now());
  binanceConnectAttemptsByIp.set(ip, attempts);
  const clients = Number(binanceClientsByIp.get(ip) || 0);
  const streams = binanceStreamsByIp.get(ip) || new Map();
  if (clients >= BINANCE_SHARED_MAX_CLIENTS_PER_IP || (!streams.has(streamKey) && streams.size >= BINANCE_SHARED_MAX_STREAMS_PER_IP)) {
    binanceSharedStats.downstream_ip_capacity_rejections += 1;
    return { ok: false, reason: 'binance downstream IP capacity' };
  }
  binanceClientsByIp.set(ip, clients + 1);
  streams.set(streamKey, Number(streams.get(streamKey) || 0) + 1);
  binanceStreamsByIp.set(ip, streams);
  let released = false;
  const release = () => {
    if (released) return;
    released = true;
    const nextClients = Math.max(0, Number(binanceClientsByIp.get(ip) || 0) - 1);
    if (nextClients) binanceClientsByIp.set(ip, nextClients); else binanceClientsByIp.delete(ip);
    const currentStreams = binanceStreamsByIp.get(ip);
    if (currentStreams) {
      const count = Math.max(0, Number(currentStreams.get(streamKey) || 0) - 1);
      if (count) currentStreams.set(streamKey, count); else currentStreams.delete(streamKey);
      if (currentStreams.size) binanceStreamsByIp.set(ip, currentStreams); else binanceStreamsByIp.delete(ip);
    }
  };
  client.once('close', release);
  client.once('error', release);
  return { ok: true, ip };
}

wss.on('connection', async (client, req, parsedUrl) => {
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

  if (provider === 'binance') {
    const downstream = registerBinanceDownstreamClient(
      client,
      req,
      binanceSharedStreamKey(market, symbol, interval),
    );
    if (!downstream.ok) {
      client.close(1013, downstream.reason);
      return;
    }
    await attachBinanceSharedClient(client, market, symbol, interval, cfg);
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
      // Step650.8.15.8: the WS-only child must never become a second Binance REST
      // caller. Binance 1s aggregation starts directly from the official aggTrade
      // WebSocket; other providers may still seed from their own public REST.
      if (provider !== 'binance') {
        fetchMarketKlines(provider, market, symbol, '1s', Date.now(), 500)
          .then((historyRows) => secondAggregator?.seedRows(historyRows))
          .catch(() => {
            // 某平台最近成交历史暂不可用时继续实时流，不跨平台回落。
          });
      }
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

server.listen(PORT, () => console.log(`Kaka market realtime worker 650.8.15.11 listening on ${PORT}`));

export const _test = {
  createSecondTradeAggregator,
  binanceSharedWsHealth,
};
