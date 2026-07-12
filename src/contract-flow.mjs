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


const FIVE_MIN_MS = 5 * 60 * 1000;
const HISTORY_MS = 24 * 60 * 60 * 1000;
const RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const MAX_ACTIVE_TRADES_PER_BUCKET = 120000;
const CORE_SYMBOLS = String(process.env.KAKA_FLOW_CORE_SYMBOLS || 'BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,AVAXUSDT,LINKUSDT,TRXUSDT,DOTUSDT,LTCUSDT')
  .split(',').map(symbolKey).filter((value) => value && value.endsWith('USDT'));
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');
const PERSISTENCE_ENABLED = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const persistQueue = new Map();
let persistFlushPromise = null;

function percentile(sorted, percentileValue) {
  if (!sorted.length) return 0;
  const index = Math.max(0, Math.min(sorted.length - 1, Math.ceil(sorted.length * percentileValue) - 1));
  return sorted[index];
}

function emptyBucket(start) {
  return { start, end: start + FIVE_MIN_MS, trades: [] };
}

function finalizeBucket(bucket, provider, symbol) {
  const trades = Array.isArray(bucket?.trades) ? bucket.trades : [];
  if (!trades.length) return null;
  const amounts = trades.map((item) => item.quote).filter(Number.isFinite).sort((a, b) => a - b);
  const p70 = percentile(amounts, 0.70);
  const p95 = percentile(amounts, 0.95);
  const row = {
    provider, symbol, bucket_time: new Date(bucket.start).toISOString(), bucket_end_time: new Date(bucket.end).toISOString(),
    buy_quote: 0, sell_quote: 0,
    large_buy_quote: 0, large_sell_quote: 0,
    medium_buy_quote: 0, medium_sell_quote: 0,
    small_buy_quote: 0, small_sell_quote: 0,
    large_buy_count: 0, large_sell_count: 0,
    medium_buy_count: 0, medium_sell_count: 0,
    small_buy_count: 0, small_sell_count: 0,
    trade_count: 0, p70_quote: p70, p95_quote: p95,
    source: 'render_exchange_websocket', updated_at: new Date().toISOString(),
  };
  for (const trade of trades) {
    const tier = p95 > 0 && trade.quote >= p95 ? 'large' : (p70 > 0 && trade.quote >= p70 ? 'medium' : 'small');
    const side = trade.side === 'buy' ? 'buy' : 'sell';
    row[`${side}_quote`] += trade.quote;
    row[`${tier}_${side}_quote`] += trade.quote;
    row[`${tier}_${side}_count`] += 1;
    row.trade_count += 1;
  }
  return row;
}

function normalizePersistedRow(row) {
  const start = normalizeTime(row.bucket_time);
  const end = normalizeTime(row.bucket_end_time) || (start ? start + FIVE_MIN_MS : 0);
  if (!start) return null;
  const numberKeys = [
    'buy_quote','sell_quote','large_buy_quote','large_sell_quote','medium_buy_quote','medium_sell_quote','small_buy_quote','small_sell_quote',
    'large_buy_count','large_sell_count','medium_buy_count','medium_sell_count','small_buy_count','small_sell_count','trade_count','p70_quote','p95_quote'
  ];
  const next = { ...row, start, end };
  for (const key of numberKeys) next[key] = asNumber(row[key]) ?? 0;
  return next;
}

function queuePersist(row) {
  if (!PERSISTENCE_ENABLED || !row) return;
  const key = `${row.provider}:${row.symbol}:${row.bucket_time}`;
  persistQueue.set(key, row);
}

async function flushPersistQueue() {
  if (!PERSISTENCE_ENABLED || persistFlushPromise || persistQueue.size === 0) return;
  const rows = [...persistQueue.values()].slice(0, 500);
  for (const row of rows) persistQueue.delete(`${row.provider}:${row.symbol}:${row.bucket_time}`);
  persistFlushPromise = (async () => {
    try {
      const response = await fetch(`${SUPABASE_URL}/rest/v1/app_contract_flow_5m_cache?on_conflict=provider,symbol,bucket_time`, {
        method: 'POST',
        headers: {
          apikey: SUPABASE_SERVICE_ROLE_KEY,
          authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          'content-type': 'application/json',
          prefer: 'resolution=merge-duplicates,return=minimal',
        },
        body: JSON.stringify(rows),
        signal: AbortSignal.timeout(15000),
      });
      if (!response.ok) throw new Error(`persist_http_${response.status}:${(await response.text()).slice(0, 180)}`);
    } catch (error) {
      for (const row of rows) persistQueue.set(`${row.provider}:${row.symbol}:${row.bucket_time}`, row);
      console.error(`[Step614.2] flow bucket persist failed: ${error?.message || error}`);
    }
  })().finally(() => { persistFlushPromise = null; });
  await persistFlushPromise;
}

async function loadPersistedHistory(state) {
  if (!PERSISTENCE_ENABLED || state.historyLoaded || state.historyLoading) return;
  state.historyLoading = true;
  try {
    const cutoff = new Date(Date.now() - HISTORY_MS - FIVE_MIN_MS).toISOString();
    const query = new URLSearchParams({
      select: '*', provider: `eq.${state.provider}`, symbol: `eq.${state.symbol}`,
      bucket_time: `gte.${cutoff}`, order: 'bucket_time.asc', limit: '400',
    });
    const response = await fetch(`${SUPABASE_URL}/rest/v1/app_contract_flow_5m_cache?${query}`, {
      headers: { apikey: SUPABASE_SERVICE_ROLE_KEY, authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}` },
      signal: AbortSignal.timeout(12000),
    });
    const text = await response.text();
    if (!response.ok) throw new Error(`history_http_${response.status}:${text.slice(0, 180)}`);
    const rows = JSON.parse(text);
    for (const raw of Array.isArray(rows) ? rows : []) {
      const row = normalizePersistedRow(raw);
      if (row) state.completedBuckets.set(row.start, row);
    }
    state.historyLoaded = true;
  } catch (error) {
    state.historyError = String(error?.message || error).slice(0, 180);
  } finally {
    state.historyLoading = false;
  }
}

function pruneState(state) {
  const cutoff = Date.now() - RETENTION_MS;
  for (const [start] of state.completedBuckets) if (start < cutoff) state.completedBuckets.delete(start);
  for (const [start] of state.openBuckets) if (start < Date.now() - FIVE_MIN_MS * 2) state.openBuckets.delete(start);
}

function finalizeReadyBuckets(state, now = Date.now()) {
  const readyBefore = Math.floor((now - 15000) / FIVE_MIN_MS) * FIVE_MIN_MS;
  for (const [start, bucket] of [...state.openBuckets.entries()]) {
    if (start >= readyBefore) continue;
    const row = finalizeBucket(bucket, state.provider, state.symbol);
    state.openBuckets.delete(start);
    if (!row) continue;
    const normalized = normalizePersistedRow(row);
    if (normalized) state.completedBuckets.set(start, normalized);
    queuePersist(row);
  }
  pruneState(state);
}

function ingest(state, items) {
  const now = Date.now();
  let added = 0;
  for (const item of items) {
    if (!item || item.time < now - FIVE_MIN_MS * 2 || item.time > now + 15000) continue;
    const signature = `${item.time}:${item.price}:${item.size}:${item.side}`;
    if (state.recentIds.has(signature)) continue;
    state.recentIds.add(signature);
    const start = Math.floor(item.time / FIVE_MIN_MS) * FIVE_MIN_MS;
    let bucket = state.openBuckets.get(start);
    if (!bucket) { bucket = emptyBucket(start); state.openBuckets.set(start, bucket); }
    if (bucket.trades.length < MAX_ACTIVE_TRADES_PER_BUCKET) bucket.trades.push(item);
    added += 1;
  }
  if (state.recentIds.size > 10000) state.recentIds = new Set([...state.recentIds].slice(-5000));
  if (added > 0) {
    state.lastTradeAt = now;
    finalizeReadyBuckets(state, now);
    for (const waiter of [...state.waiters]) waiter();
  }
}

function startStream(state) {
  if (state.ws && (state.ws.readyState === WebSocket.OPEN || state.ws.readyState === WebSocket.CONNECTING)) return;
  clearTimeout(state.reconnectTimer);
  const cfg = configFor(state.provider, state.symbol);
  state.status = 'connecting'; state.error = '';
  const ws = new WebSocket(cfg.url, { handshakeTimeout: 15000 });
  state.ws = ws;
  ws.on('open', () => {
    state.status = 'open'; state.reconnectAttempt = 0;
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
  ws.on('message', (raw) => { try { ingest(state, cfg.parse(raw)); } catch (_) {} });
  const close = (reason) => {
    if (state.ws !== ws) return;
    clearInterval(state.heartbeatTimer); state.heartbeatTimer = null; state.ws = null;
    state.status = 'closed'; state.error = String(reason || 'upstream_closed').slice(0, 180);
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
      key, provider, symbol, openBuckets: new Map(), completedBuckets: new Map(), recentIds: new Set(), waiters: new Set(),
      ws: null, status: 'idle', error: '', lastTradeAt: 0, lastRequestedAt: Date.now(), reconnectAttempt: 0,
      reconnectTimer: null, heartbeatTimer: null, metricCache: null, historyLoaded: false, historyLoading: false, historyError: '',
    };
    states.set(key, state);
  }
  state.lastRequestedAt = Date.now();
  loadPersistedHistory(state).catch(() => {});
  startStream(state);
  return state;
}

function provisionalRows(state) {
  const rows = [];
  for (const bucket of state.openBuckets.values()) {
    const row = finalizeBucket(bucket, state.provider, state.symbol);
    const normalized = row ? normalizePersistedRow(row) : null;
    if (normalized) rows.push(normalized);
  }
  return rows;
}

function chooseBucketMs(coverageMs) {
  if (coverageMs >= 20 * 60 * 60 * 1000) return 2 * 60 * 60 * 1000;
  if (coverageMs >= 6 * 60 * 60 * 1000) return 60 * 60 * 1000;
  if (coverageMs >= 60 * 60 * 1000) return 10 * 60 * 1000;
  if (coverageMs >= 10 * 60 * 1000) return 2 * 60 * 1000;
  return FIVE_MIN_MS;
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


function mergeRowsFor24h(state) {
  finalizeReadyBuckets(state);
  const cutoff = Date.now() - HISTORY_MS;
  const byStart = new Map();
  for (const row of state.completedBuckets.values()) if (row.end >= cutoff) byStart.set(row.start, row);
  for (const row of provisionalRows(state)) if (row.end >= cutoff) byStart.set(row.start, row);
  return [...byStart.values()].sort((a, b) => a.start - b.start);
}

function summarize(state, venueMetrics = { oi_rows: [], ratio_rows: [] }) {
  const rows = mergeRowsFor24h(state);
  const firstTime = rows[0]?.start || 0;
  const lastTime = rows.at(-1)?.end || 0;
  const coverageMs = firstTime && lastTime ? Math.max(0, Math.min(HISTORY_MS, lastTime - firstTime)) : 0;
  const completeCount = rows.filter((row) => row.end <= Date.now()).length;
  const historyComplete = coverageMs >= HISTORY_MS - 15 * 60 * 1000 && completeCount >= 276;
  const bucketMs = chooseBucketMs(coverageMs);
  const grouped = new Map();
  for (const row of rows) {
    const start = Math.floor(row.start / bucketMs) * bucketMs;
    let bucket = grouped.get(start);
    if (!bucket) bucket = { start, end: start + bucketMs, buy: 0, sell: 0, samples: 0 };
    bucket.buy += row.buy_quote; bucket.sell += row.sell_quote; bucket.samples += row.trade_count;
    grouped.set(start, bucket);
  }
  let flowBuckets = [...grouped.values()].sort((a,b)=>a.start-b.start);
  if (flowBuckets.length > 24) flowBuckets = flowBuckets.slice(-24);
  flowBuckets = flowBuckets.map((bucket) => ({
    start_time_ms: bucket.start, end_time_ms: bucket.end,
    buy_quote_volume: bucket.buy, sell_quote_volume: bucket.sell,
    net_quote_volume: bucket.buy - bucket.sell, samples: bucket.samples,
  }));

  const distribution = { large_buy_quote: 0, large_sell_quote: 0, regular_buy_quote: 0, regular_sell_quote: 0 };
  const tierMap = new Map([
    ['large', { tier:'large', buy_quote:0, sell_quote:0, buy_count:0, sell_count:0 }],
    ['medium', { tier:'medium', buy_quote:0, sell_quote:0, buy_count:0, sell_count:0 }],
    ['small', { tier:'small', buy_quote:0, sell_quote:0, buy_count:0, sell_count:0 }],
  ]);
  let p70Weighted = 0, p95Weighted = 0, thresholdWeight = 0, tradeCount = 0;
  for (const row of rows) {
    distribution.large_buy_quote += row.large_buy_quote; distribution.large_sell_quote += row.large_sell_quote;
    distribution.regular_buy_quote += row.medium_buy_quote + row.small_buy_quote;
    distribution.regular_sell_quote += row.medium_sell_quote + row.small_sell_quote;
    for (const tierName of ['large','medium','small']) {
      const tier = tierMap.get(tierName);
      tier.buy_quote += row[`${tierName}_buy_quote`]; tier.sell_quote += row[`${tierName}_sell_quote`];
      tier.buy_count += row[`${tierName}_buy_count`]; tier.sell_count += row[`${tierName}_sell_count`];
    }
    const weight = Math.max(1, row.trade_count); p70Weighted += row.p70_quote * weight; p95Weighted += row.p95_quote * weight; thresholdWeight += weight;
    tradeCount += row.trade_count;
  }
  const tiers = [...tierMap.values()].map((tier) => ({ ...tier, net_quote:tier.buy_quote-tier.sell_quote, trade_count:tier.buy_count+tier.sell_count }));
  const totalBuy = rows.reduce((sum,row)=>sum+row.buy_quote,0);
  const totalSell = rows.reduce((sum,row)=>sum+row.sell_quote,0);
  const takerRows = flowBuckets.map((bucket) => ({
    source_time:new Date(bucket.start_time_ms).toISOString(), open_time:new Date(bucket.start_time_ms).toISOString(),
    buy_quote_volume:bucket.buy_quote_volume, sell_quote_volume:bucket.sell_quote_volume,
    buy_sell_ratio:bucket.sell_quote_volume>0?bucket.buy_quote_volume/bucket.sell_quote_volume:null, sample_count:bucket.samples,
  }));
  let cumulative=0;
  const cvdRows = flowBuckets.map((bucket)=>{ cumulative += bucket.net_quote_volume; return {
    source_time:new Date(bucket.start_time_ms).toISOString(), open_time:new Date(bucket.start_time_ms).toISOString(),
    delta_quote:bucket.net_quote_volume, delta_volume:bucket.net_quote_volume, cvd_quote:cumulative, cvd:cumulative,
  };});
  return {
    ok:true, version:'614.2', provider:state.provider, symbol:state.symbol,
    source:'render_exchange_websocket_supabase_5m', scope:historyComplete?'rolling_24h':'building_24h', history_complete:historyComplete,
    persistence_enabled:PERSISTENCE_ENABLED, history_loaded:state.historyLoaded, history_error:state.historyError,
    stream_status:state.status, stream_error:state.error, trade_count:tradeCount,
    sample_started_at_ms:firstTime, sample_ended_at_ms:lastTime, coverage_ms:coverageMs, bucket_ms:bucketMs,
    stored_5m_buckets:rows.length, expected_5m_buckets:288, coverage_ratio:Math.min(1, rows.length/288),
    thresholds:{ p70_quote:thresholdWeight?p70Weighted/thresholdWeight:0, p95_quote:thresholdWeight?p95Weighted/thresholdWeight:0 },
    distribution, tiers, flow_buckets:flowBuckets,
    totals:{ buy_quote:totalBuy, sell_quote:totalSell, net_quote:totalBuy-totalSell },
    metrics:{ oi_rows:Array.isArray(venueMetrics.oi_rows)?venueMetrics.oi_rows:[], ratio_rows:Array.isArray(venueMetrics.ratio_rows)?venueMetrics.ratio_rows:[], taker_rows:takerRows, cvd_rows:cvdRows },
    generated_at:new Date().toISOString(),
  };
}

function sendJson(res,status,body){res.writeHead(status,{'content-type':'application/json; charset=utf-8','cache-control':'no-store','access-control-allow-origin':'*'});res.end(JSON.stringify(body));}
function waitForTrades(state,minTrades,waitMs){
  const count=()=>[...state.openBuckets.values()].reduce((sum,b)=>sum+b.trades.length,0);
  if(count()>=minTrades||waitMs<=0)return Promise.resolve();
  return new Promise((resolve)=>{let timer;const done=()=>{clearTimeout(timer);state.waiters.delete(check);resolve();};const check=()=>{if(count()>=minTrades)done();};state.waiters.add(check);timer=setTimeout(done,waitMs);});
}

export async function handleContractFlow(req,res,url){
  if(url.pathname==='/api/contract-flow/health'){
    sendJson(res,200,{ok:true,version:'614.2',streams:states.size,persistence_enabled:PERSISTENCE_ENABLED,persist_queue:persistQueue.size,core_symbols:CORE_SYMBOLS,time:new Date().toISOString()});return true;
  }
  if(url.pathname==='/api/contract-flow/warm'){
    let started=0;
    for(const provider of PROVIDERS)for(const symbol of CORE_SYMBOLS){const state=getState(provider,symbol);state.lastRequestedAt=Date.now();started+=1;}
    sendJson(res,200,{ok:true,version:'614.2',started,persistence_enabled:PERSISTENCE_ENABLED,core_symbols:CORE_SYMBOLS,time:new Date().toISOString()});return true;
  }
  if(url.pathname!=='/api/contract-flow')return false;
  if(req.method!=='GET'&&req.method!=='POST'){sendJson(res,405,{ok:false,error:'method_not_allowed'});return true;}
  let provider=providerKey(url.searchParams.get('provider'));let symbol=symbolKey(url.searchParams.get('symbol'));let waitMs=Math.min(5000,Math.max(0,Number(url.searchParams.get('wait_ms')||3200)));
  if(req.method==='POST'){const chunks=[];for await(const chunk of req)chunks.push(chunk);try{const body=JSON.parse(Buffer.concat(chunks).toString('utf8')||'{}');provider=providerKey(body.provider)||provider;symbol=symbolKey(body.symbol)||symbol;if(Number.isFinite(Number(body.wait_ms)))waitMs=Math.min(5000,Math.max(0,Number(body.wait_ms)));}catch(_){}}
  if(!provider||!symbol||!symbol.endsWith('USDT')){sendJson(res,400,{ok:false,error:'invalid_provider_or_symbol'});return true;}
  const state=getState(provider,symbol);await loadPersistedHistory(state);await waitForTrades(state,20,waitMs);const venueMetrics=await fetchVenueMetrics(state);const payload=summarize(state,venueMetrics);
  const hasData=payload.trade_count>0||payload.stored_5m_buckets>0;sendJson(res,hasData?200:503,hasData?payload:{...payload,ok:false,error:'building_24h_history'});return true;
}

setInterval(()=>{const now=Date.now();for(const [key,state] of states.entries()){finalizeReadyBuckets(state,now);if(now-state.lastRequestedAt<=IDLE_CLOSE_MS)continue;clearInterval(state.heartbeatTimer);clearTimeout(state.reconnectTimer);try{state.ws?.close(1000,'idle');}catch(_){}states.delete(key);}},60000).unref();
setInterval(()=>{flushPersistQueue().catch(()=>{});},20000).unref();
