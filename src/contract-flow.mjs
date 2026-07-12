import { WebSocket } from 'ws';

const PROVIDERS = new Set(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const states = new Map();
const MAX_TRADES_PER_STREAM = 120000;
const MAX_AGE_MS = 24 * 60 * 60 * 1000;
const IDLE_CLOSE_MS = 12 * 60 * 1000;
const RECONNECT_MAX_MS = 30000;

function providerKey(raw) {
  const value = String(raw || '').trim().toLowerCase().replaceAll('gate.io', 'gate');
  const normalized = value === 'okex' ? 'okx' : value;
  return PROVIDERS.has(normalized) ? normalized : null;
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

function okxInstId(symbol) {
  const [base, quote] = splitSymbol(symbol);
  return `${base}-${quote}-SWAP`;
}

function gateSymbol(symbol) {
  const [base, quote] = splitSymbol(symbol);
  return `${base}_${quote}`;
}

function asNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeTime(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  if (parsed < 10_000_000_000) return Math.round(parsed * 1000);
  if (parsed > 10_000_000_000_000) return Math.round(parsed / 1000);
  return Math.round(parsed);
}

function tradeItem(time, price, size, side) {
  const ts = normalizeTime(time);
  const px = asNumber(price);
  const qty = Math.abs(asNumber(size) ?? 0);
  const normalizedSide = String(side || '').toLowerCase();
  if (!ts || !px || px <= 0 || !qty || qty <= 0) return null;
  if (normalizedSide !== 'buy' && normalizedSide !== 'sell') return null;
  return { time: ts, price: px, size: qty, quote: px * qty, side: normalizedSide };
}

function configFor(provider, symbol) {
  if (provider === 'binance') {
    return {
      url: `wss://fstream.binance.com/market/ws/${symbol.toLowerCase()}@aggTrade`,
      subscriptions: [],
      parse(raw) {
        const message = JSON.parse(raw.toString());
        const payload = message?.data ?? message;
        if (payload?.e !== 'aggTrade') return [];
        const item = tradeItem(payload.T ?? payload.E, payload.p, payload.q, payload.m === true ? 'sell' : 'buy');
        return item ? [item] : [];
      },
    };
  }
  if (provider === 'okx') {
    return {
      url: 'wss://ws.okx.com:8443/ws/v5/public',
      subscriptions: [{ op: 'subscribe', args: [{ channel: 'trades', instId: okxInstId(symbol) }] }],
      heartbeat: 'ping',
      parse(raw) {
        const text = raw.toString();
        if (text === 'pong') return [];
        const message = JSON.parse(text);
        if (message?.arg?.channel !== 'trades') return [];
        const items = [];
        for (const row of Array.isArray(message.data) ? message.data : []) {
          const item = tradeItem(row.ts, row.px, row.sz, row.side);
          if (item) items.push(item);
        }
        return items;
      },
    };
  }
  if (provider === 'bybit') {
    return {
      url: 'wss://stream.bybit.com/v5/public/linear',
      subscriptions: [{ op: 'subscribe', args: [`publicTrade.${symbol}`] }],
      heartbeat: { op: 'ping' },
      parse(raw) {
        const message = JSON.parse(raw.toString());
        if (!String(message?.topic || '').startsWith('publicTrade.')) return [];
        const items = [];
        for (const row of Array.isArray(message.data) ? message.data : []) {
          const item = tradeItem(row.T ?? message.ts, row.p, row.v, row.S);
          if (item) items.push(item);
        }
        return items;
      },
    };
  }
  if (provider === 'bitget') {
    return {
      url: 'wss://ws.bitget.com/v2/ws/public',
      subscriptions: [{ op: 'subscribe', args: [{ instType: 'USDT-FUTURES', channel: 'trade', instId: symbol }] }],
      heartbeat: 'ping',
      parse(raw) {
        const text = raw.toString();
        if (text === 'pong') return [];
        const message = JSON.parse(text);
        if (message?.arg?.channel !== 'trade') return [];
        const items = [];
        for (const row of Array.isArray(message.data) ? message.data : []) {
          if (Array.isArray(row)) {
            const item = tradeItem(row[0], row[1], row[2], row[3]);
            if (item) items.push(item);
          } else if (row && typeof row === 'object') {
            const item = tradeItem(row.ts ?? message.ts, row.price ?? row.px, row.size ?? row.sz, row.side ?? row.S);
            if (item) items.push(item);
          }
        }
        return items;
      },
    };
  }
  if (provider === 'gate') {
    return {
      url: 'wss://fx-ws.gateio.ws/v4/ws/usdt',
      subscriptions: [{
        time: Math.floor(Date.now() / 1000),
        channel: 'futures.trades',
        event: 'subscribe',
        payload: [gateSymbol(symbol)],
      }],
      parse(raw) {
        const message = JSON.parse(raw.toString());
        if (message?.channel !== 'futures.trades' || message?.event !== 'update') return [];
        const rows = Array.isArray(message.result) ? message.result : (message.result ? [message.result] : []);
        const items = [];
        for (const row of rows) {
          const signedSize = asNumber(row.size ?? row.amount);
          const side = row.side || (signedSize == null ? '' : signedSize >= 0 ? 'buy' : 'sell');
          const item = tradeItem(row.create_time_ms ?? row.create_time ?? row.time_ms ?? row.time, row.price, signedSize, side);
          if (item) items.push(item);
        }
        return items;
      },
    };
  }
  throw new Error('unsupported_provider');
}

function prune(state) {
  const cutoff = Date.now() - MAX_AGE_MS;
  if (state.trades.length > MAX_TRADES_PER_STREAM) {
    state.trades.splice(0, state.trades.length - MAX_TRADES_PER_STREAM);
  }
  let remove = 0;
  while (remove < state.trades.length && state.trades[remove].time < cutoff) remove += 1;
  if (remove > 0) state.trades.splice(0, remove);
}

function ingest(state, items) {
  const now = Date.now();
  let added = 0;
  for (const item of items) {
    if (!item || item.time < now - MAX_AGE_MS || item.time > now + 15000) continue;
    const signature = `${item.time}:${item.price}:${item.size}:${item.side}`;
    if (state.recentIds.has(signature)) continue;
    state.recentIds.add(signature);
    state.trades.push(item);
    added += 1;
  }
  if (state.recentIds.size > 5000) {
    const keep = [...state.recentIds].slice(-2500);
    state.recentIds = new Set(keep);
  }
  if (added > 0) {
    state.trades.sort((a, b) => a.time - b.time);
    state.lastTradeAt = state.trades.at(-1)?.time || state.lastTradeAt;
    prune(state);
    for (const waiter of [...state.waiters]) waiter();
  }
}

function startStream(state) {
  if (state.ws && (state.ws.readyState === WebSocket.OPEN || state.ws.readyState === WebSocket.CONNECTING)) return;
  clearTimeout(state.reconnectTimer);
  const cfg = configFor(state.provider, state.symbol);
  state.status = 'connecting';
  state.error = '';
  const ws = new WebSocket(cfg.url, { handshakeTimeout: 15000 });
  state.ws = ws;
  ws.on('open', () => {
    state.status = 'open';
    state.reconnectAttempt = 0;
    for (const subscription of cfg.subscriptions) ws.send(JSON.stringify(subscription));
    clearInterval(state.heartbeatTimer);
    state.heartbeatTimer = setInterval(() => {
      if (ws.readyState !== WebSocket.OPEN) return;
      try {
        if (typeof cfg.heartbeat === 'string') ws.send(cfg.heartbeat);
        else if (cfg.heartbeat) ws.send(JSON.stringify(cfg.heartbeat));
        else ws.ping();
      } catch (_) {}
    }, 20000);
  });
  ws.on('message', (raw) => {
    try { ingest(state, cfg.parse(raw)); } catch (_) {}
  });
  const close = (reason) => {
    if (state.ws !== ws) return;
    clearInterval(state.heartbeatTimer);
    state.heartbeatTimer = null;
    state.ws = null;
    state.status = 'closed';
    state.error = String(reason || 'upstream_closed').slice(0, 180);
    if (Date.now() - state.lastRequestedAt <= IDLE_CLOSE_MS) {
      state.reconnectAttempt += 1;
      const delay = Math.min(RECONNECT_MAX_MS, 1000 * 2 ** Math.min(5, state.reconnectAttempt));
      state.reconnectTimer = setTimeout(() => startStream(state), delay);
    }
  };
  ws.on('close', (code, reason) => close(`${code}:${reason || ''}`));
  ws.on('error', (error) => close(error?.message || 'upstream_error'));
}

function getState(provider, symbol) {
  const key = `${provider}:${symbol}`;
  let state = states.get(key);
  if (!state) {
    state = {
      key, provider, symbol, trades: [], recentIds: new Set(), waiters: new Set(),
      ws: null, status: 'idle', error: '', lastTradeAt: 0, lastRequestedAt: Date.now(),
      reconnectAttempt: 0, reconnectTimer: null, heartbeatTimer: null,
    };
    states.set(key, state);
  }
  state.lastRequestedAt = Date.now();
  startStream(state);
  return state;
}

function percentile(sorted, percentileValue) {
  if (!sorted.length) return 0;
  const index = Math.max(0, Math.min(sorted.length - 1, Math.ceil(sorted.length * percentileValue) - 1));
  return sorted[index];
}

function chooseBucketMs(coverageMs) {
  if (coverageMs >= 20 * 60 * 60 * 1000) return 2 * 60 * 60 * 1000;
  if (coverageMs >= 6 * 60 * 60 * 1000) return 60 * 60 * 1000;
  if (coverageMs >= 60 * 60 * 1000) return 10 * 60 * 1000;
  if (coverageMs >= 10 * 60 * 1000) return 2 * 60 * 1000;
  if (coverageMs >= 2 * 60 * 1000) return 30 * 1000;
  return Math.max(1000, Math.ceil(Math.max(1000, coverageMs) / 10 / 1000) * 1000);
}


async function fetchJson(url, timeoutMs = 3500) {
  const response = await fetch(url, {
    headers: { 'accept': 'application/json', 'user-agent': 'KakaWeb3/614.1.3.3' },
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`http_${response.status}:${text.slice(0, 120)}`);
  return JSON.parse(text);
}

function isoFrom(value) {
  const time = normalizeTime(value);
  return time ? new Date(time).toISOString() : new Date().toISOString();
}

async function fetchVenueMetrics(state) {
  const now = Date.now();
  if (state.metricCache && now - state.metricCache.at < 30000) return state.metricCache.value;
  const provider = state.provider;
  const symbol = state.symbol;
  const [base] = splitSymbol(symbol);
  const empty = { oi_rows: [], ratio_rows: [] };
  let value = empty;
  try {
    if (provider === 'okx') {
      const instId = okxInstId(symbol);
      const [oiRaw, ratioRaw] = await Promise.allSettled([
        fetchJson(`https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId=${encodeURIComponent(instId)}`),
        fetchJson(`https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy=${encodeURIComponent(base)}&period=5m&limit=48`),
      ]);
      const oiRows = oiRaw.status === 'fulfilled'
        ? (Array.isArray(oiRaw.value?.data) ? oiRaw.value.data : []).map((row) => ({
            source_time: isoFrom(row.ts), open_interest: asNumber(row.oi), open_interest_value: asNumber(row.oiUsd),
          })).filter((row) => row.open_interest != null || row.open_interest_value != null)
        : [];
      const ratioRows = [];
      if (ratioRaw.status === 'fulfilled') {
        for (const row of Array.isArray(ratioRaw.value?.data) ? ratioRaw.value.data : []) {
          const ts = Array.isArray(row) ? row[0] : row.ts;
          const ratio = asNumber(Array.isArray(row) ? row[1] : (row.ratio ?? row.longShortRatio));
          if (ratio != null) ratioRows.push({ source_time: isoFrom(ts), ratio_type: 'global_account', long_short_ratio: ratio });
        }
      }
      value = { oi_rows: oiRows, ratio_rows: ratioRows };
    } else if (provider === 'bybit') {
      const endpoints = ['https://api.bybit.com', 'https://api.bytick.com'];
      let oiRaw = null;
      let ratioRaw = null;
      for (const host of endpoints) {
        try {
          [oiRaw, ratioRaw] = await Promise.all([
            fetchJson(`${host}/v5/market/open-interest?category=linear&symbol=${encodeURIComponent(symbol)}&intervalTime=5min&limit=48`),
            fetchJson(`${host}/v5/market/account-ratio?category=linear&symbol=${encodeURIComponent(symbol)}&period=5min&limit=48`),
          ]);
          break;
        } catch (_) {}
      }
      const oiRows = (Array.isArray(oiRaw?.result?.list) ? oiRaw.result.list : []).map((row) => ({
        source_time: isoFrom(row.timestamp), open_interest: asNumber(row.openInterest),
      })).filter((row) => row.open_interest != null);
      const ratioRows = (Array.isArray(ratioRaw?.result?.list) ? ratioRaw.result.list : []).map((row) => {
        const buy = asNumber(row.buyRatio);
        const sell = asNumber(row.sellRatio);
        return { source_time: isoFrom(row.timestamp), ratio_type: 'global_account', long_short_ratio: buy != null && sell && sell > 0 ? buy / sell : null };
      }).filter((row) => row.long_short_ratio != null);
      value = { oi_rows: oiRows, ratio_rows: ratioRows };
    } else if (provider === 'bitget') {
      const productType = 'USDT-FUTURES';
      const [oiRaw, ratioRaw] = await Promise.allSettled([
        fetchJson(`https://api.bitget.com/api/v2/mix/market/open-interest?symbol=${encodeURIComponent(symbol)}&productType=${productType}`),
        fetchJson(`https://api.bitget.com/api/v2/mix/market/account-long-short?symbol=${encodeURIComponent(symbol)}&productType=${productType}&period=5m`),
      ]);
      const oiData = oiRaw.status === 'fulfilled' ? oiRaw.value?.data : null;
      const oiList = Array.isArray(oiData?.openInterestList) ? oiData.openInterestList : (Array.isArray(oiData) ? oiData : oiData ? [oiData] : []);
      const oiRows = oiList.map((row) => ({
        source_time: isoFrom(row.ts ?? row.timestamp), open_interest: asNumber(row.size ?? row.openInterest ?? row.amount),
      })).filter((row) => row.open_interest != null);
      const ratioData = ratioRaw.status === 'fulfilled' ? ratioRaw.value?.data : null;
      const ratioList = Array.isArray(ratioData) ? ratioData : (Array.isArray(ratioData?.list) ? ratioData.list : ratioData ? [ratioData] : []);
      const ratioRows = ratioList.map((row) => {
        const buy = asNumber(row.buyRatio ?? row.longAccountRatio ?? row.longRatio);
        const sell = asNumber(row.sellRatio ?? row.shortAccountRatio ?? row.shortRatio);
        const direct = asNumber(row.longShortRatio ?? row.ratio);
        return { source_time: isoFrom(row.ts ?? row.timestamp), ratio_type: 'global_account', long_short_ratio: direct ?? (buy != null && sell && sell > 0 ? buy / sell : null) };
      }).filter((row) => row.long_short_ratio != null);
      value = { oi_rows: oiRows, ratio_rows: ratioRows };
    } else if (provider === 'gate') {
      const contract = gateSymbol(symbol);
      const raw = await fetchJson(`https://api.gateio.ws/api/v4/futures/usdt/contract_stats?contract=${encodeURIComponent(contract)}&interval=5m&limit=48`);
      const list = Array.isArray(raw) ? raw : [];
      const oiRows = list.map((row) => ({
        source_time: isoFrom(row.time), open_interest: asNumber(row.open_interest), open_interest_value: asNumber(row.open_interest_usd),
      })).filter((row) => row.open_interest != null || row.open_interest_value != null);
      const ratioRows = list.map((row) => ({
        source_time: isoFrom(row.time), ratio_type: 'global_account', long_short_ratio: asNumber(row.long_short_ratio),
      })).filter((row) => row.long_short_ratio != null);
      value = { oi_rows: oiRows, ratio_rows: ratioRows };
    }
  } catch (error) {
    value = { ...empty, error: String(error?.message || error).slice(0, 180) };
  }
  state.metricCache = { at: now, value };
  return value;
}

function summarize(state, venueMetrics = { oi_rows: [], ratio_rows: [] }) {
  prune(state);
  const trades = state.trades;
  const firstTime = trades[0]?.time || 0;
  const lastTime = trades.at(-1)?.time || 0;
  const coverageMs = firstTime && lastTime ? Math.max(0, lastTime - firstTime) : 0;
  const quoteAmounts = trades.map((trade) => trade.quote).filter(Number.isFinite).sort((a, b) => a - b);
  const p70 = percentile(quoteAmounts, 0.70);
  const p95 = percentile(quoteAmounts, 0.95);

  const distribution = {
    large_buy_quote: 0, large_sell_quote: 0,
    regular_buy_quote: 0, regular_sell_quote: 0,
  };
  const tierMap = new Map([
    ['large', { tier: 'large', buy_quote: 0, sell_quote: 0, buy_count: 0, sell_count: 0 }],
    ['medium', { tier: 'medium', buy_quote: 0, sell_quote: 0, buy_count: 0, sell_count: 0 }],
    ['small', { tier: 'small', buy_quote: 0, sell_quote: 0, buy_count: 0, sell_count: 0 }],
  ]);

  for (const trade of trades) {
    const large = p95 > 0 && trade.quote >= p95;
    const tierName = large ? 'large' : (p70 > 0 && trade.quote >= p70 ? 'medium' : 'small');
    const tier = tierMap.get(tierName);
    if (trade.side === 'buy') {
      tier.buy_quote += trade.quote;
      tier.buy_count += 1;
      distribution[large ? 'large_buy_quote' : 'regular_buy_quote'] += trade.quote;
    } else {
      tier.sell_quote += trade.quote;
      tier.sell_count += 1;
      distribution[large ? 'large_sell_quote' : 'regular_sell_quote'] += trade.quote;
    }
  }

  const bucketMs = chooseBucketMs(coverageMs);
  const grouped = new Map();
  for (const trade of trades) {
    const start = Math.floor(trade.time / bucketMs) * bucketMs;
    let bucket = grouped.get(start);
    if (!bucket) {
      bucket = { start, end: start + bucketMs, buy: 0, sell: 0, samples: 0 };
      grouped.set(start, bucket);
    }
    if (trade.side === 'buy') bucket.buy += trade.quote;
    else bucket.sell += trade.quote;
    bucket.samples += 1;
  }
  let buckets = [...grouped.values()].sort((a, b) => a.start - b.start);
  if (buckets.length > 24) buckets = buckets.slice(-24);
  const flowBuckets = buckets.map((bucket) => ({
    start_time_ms: bucket.start,
    end_time_ms: bucket.end,
    buy_quote_volume: bucket.buy,
    sell_quote_volume: bucket.sell,
    net_quote_volume: bucket.buy - bucket.sell,
    samples: bucket.samples,
  }));
  const takerRows = flowBuckets.map((bucket) => ({
    source_time: new Date(bucket.start_time_ms).toISOString(),
    open_time: new Date(bucket.start_time_ms).toISOString(),
    buy_quote_volume: bucket.buy_quote_volume,
    sell_quote_volume: bucket.sell_quote_volume,
    buy_sell_ratio: bucket.sell_quote_volume > 0 ? bucket.buy_quote_volume / bucket.sell_quote_volume : null,
    sample_count: bucket.samples,
  }));
  let cumulative = 0;
  const cvdRows = flowBuckets.map((bucket) => {
    const delta = bucket.net_quote_volume;
    cumulative += delta;
    return {
      source_time: new Date(bucket.start_time_ms).toISOString(),
      open_time: new Date(bucket.start_time_ms).toISOString(),
      delta_quote: delta,
      delta_volume: delta,
      cvd_quote: cumulative,
      cvd: cumulative,
    };
  });
  const tiers = [...tierMap.values()].map((tier) => ({
    ...tier,
    net_quote: tier.buy_quote - tier.sell_quote,
    trade_count: tier.buy_count + tier.sell_count,
  }));

  const totalBuy = distribution.large_buy_quote + distribution.regular_buy_quote;
  const totalSell = distribution.large_sell_quote + distribution.regular_sell_quote;
  const scope = coverageMs >= 20 * 60 * 60 * 1000 ? 'rolling_24h' : 'rolling_sample';
  return {
    ok: true,
    version: '614.1.3.3',
    provider: state.provider,
    symbol: state.symbol,
    source: 'render_exchange_websocket_rolling',
    scope,
    stream_status: state.status,
    stream_error: state.error,
    trade_count: trades.length,
    sample_started_at_ms: firstTime,
    sample_ended_at_ms: lastTime,
    coverage_ms: coverageMs,
    bucket_ms: bucketMs,
    thresholds: { p70_quote: p70, p95_quote: p95 },
    distribution,
    tiers,
    flow_buckets: flowBuckets,
    totals: {
      buy_quote: totalBuy,
      sell_quote: totalSell,
      net_quote: totalBuy - totalSell,
    },
    metrics: {
      oi_rows: Array.isArray(venueMetrics.oi_rows) ? venueMetrics.oi_rows : [],
      ratio_rows: Array.isArray(venueMetrics.ratio_rows) ? venueMetrics.ratio_rows : [],
      taker_rows: takerRows,
      cvd_rows: cvdRows,
    },
    generated_at: new Date().toISOString(),
  };
}

function sendJson(res, status, body) {
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'access-control-allow-origin': '*',
  });
  res.end(JSON.stringify(body));
}

function waitForTrades(state, minTrades, waitMs) {
  if (state.trades.length >= minTrades || waitMs <= 0) return Promise.resolve();
  return new Promise((resolve) => {
    let timer;
    const done = () => {
      clearTimeout(timer);
      state.waiters.delete(check);
      resolve();
    };
    const check = () => {
      if (state.trades.length >= minTrades) done();
    };
    state.waiters.add(check);
    timer = setTimeout(done, waitMs);
  });
}

export async function handleContractFlow(req, res, url) {
  if (url.pathname === '/api/contract-flow/health') {
    sendJson(res, 200, { ok: true, version: '614.1.3.3', streams: states.size, time: new Date().toISOString() });
    return true;
  }
  if (url.pathname !== '/api/contract-flow') return false;
  if (req.method !== 'GET' && req.method !== 'POST') {
    sendJson(res, 405, { ok: false, error: 'method_not_allowed' });
    return true;
  }
  let provider = providerKey(url.searchParams.get('provider'));
  let symbol = symbolKey(url.searchParams.get('symbol'));
  let waitMs = Math.min(5000, Math.max(0, Number(url.searchParams.get('wait_ms') || 3200)));
  if (req.method === 'POST') {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    try {
      const body = JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}');
      provider = providerKey(body.provider) || provider;
      symbol = symbolKey(body.symbol) || symbol;
      if (Number.isFinite(Number(body.wait_ms))) waitMs = Math.min(5000, Math.max(0, Number(body.wait_ms)));
    } catch (_) {}
  }
  if (!provider || !symbol || !symbol.endsWith('USDT')) {
    sendJson(res, 400, { ok: false, error: 'invalid_provider_or_symbol' });
    return true;
  }
  const state = getState(provider, symbol);
  await waitForTrades(state, 60, waitMs);
  const venueMetrics = await fetchVenueMetrics(state);
  const payload = summarize(state, venueMetrics);
  const status = payload.trade_count > 0 ? 200 : 503;
  sendJson(res, status, payload.trade_count > 0 ? payload : { ...payload, ok: false, error: 'waiting_for_exchange_trades' });
  return true;
}

setInterval(() => {
  const now = Date.now();
  for (const [key, state] of states.entries()) {
    prune(state);
    if (now - state.lastRequestedAt <= IDLE_CLOSE_MS) continue;
    clearInterval(state.heartbeatTimer);
    clearTimeout(state.reconnectTimer);
    try { state.ws?.close(1000, 'idle'); } catch (_) {}
    states.delete(key);
  }
}, 60000).unref();
