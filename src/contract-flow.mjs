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
  if (value instanceof Date) {
    const parsedDate = value.getTime();
    return Number.isFinite(parsedDate) ? parsedDate : null;
  }
  if (typeof value === 'string') {
    const text = value.trim();
    if (!text) return null;
    const numeric = Number(text);
    if (Number.isFinite(numeric)) {
      if (numeric < 10_000_000_000) return Math.round(numeric * 1000);
      if (numeric > 10_000_000_000_000) return Math.round(numeric / 1000);
      return Math.round(numeric);
    }
    const parsedIso = Date.parse(text);
    return Number.isFinite(parsedIso) ? parsedIso : null;
  }
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
const RETENTION_MS = 72 * 60 * 60 * 1000;
const FLOW_HISTOGRAM_MIN_LOG10 = -2;
const FLOW_HISTOGRAM_MAX_LOG10 = 10;
const FLOW_HISTOGRAM_STEP = 0.10;
const FLOW_HISTOGRAM_BINS = Math.ceil((FLOW_HISTOGRAM_MAX_LOG10 - FLOW_HISTOGRAM_MIN_LOG10) / FLOW_HISTOGRAM_STEP);
const MAX_RECENT_IDS = 3000;
const MAX_ACTIVE_STATES = 80;
const CORE_SYMBOLS = String(process.env.KAKA_FLOW_CORE_SYMBOLS || 'BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,AVAXUSDT,LINKUSDT,TRXUSDT,DOTUSDT,LTCUSDT')
  .split(',').map(symbolKey).filter((value) => value && value.endsWith('USDT'));
const CORE_SYMBOL_SET = new Set(CORE_SYMBOLS);
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');
const PERSISTENCE_ENABLED = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const persistQueue = new Map();
let persistFlushPromise = null;
const METRIC_HISTORY_MS = 24 * 60 * 60 * 1000;
const METRIC_REFRESH_MS = 5 * 60 * 1000;
const METRIC_TABLE = 'app_contract_position_5m_cache';
const metricPersistQueue = new Map();
let metricPersistFlushPromise = null;
const BINANCE_API_KEY = String(process.env.BINANCE_API_KEY || '').trim();

function percentile(sorted, percentileValue) {
  if (!sorted.length) return 0;
  const index = Math.max(0, Math.min(sorted.length - 1, Math.ceil(sorted.length * percentileValue) - 1));
  return sorted[index];
}

function emptyHistogram() {
  return {
    buyQuote: new Float64Array(FLOW_HISTOGRAM_BINS),
    sellQuote: new Float64Array(FLOW_HISTOGRAM_BINS),
    buyCount: new Uint32Array(FLOW_HISTOGRAM_BINS),
    sellCount: new Uint32Array(FLOW_HISTOGRAM_BINS),
  };
}

function flowHistogramIndex(quote) {
  if (!Number.isFinite(quote) || quote <= 0) return 0;
  const raw = Math.floor((Math.log10(quote) - FLOW_HISTOGRAM_MIN_LOG10) / FLOW_HISTOGRAM_STEP);
  return Math.max(0, Math.min(FLOW_HISTOGRAM_BINS - 1, raw));
}

function flowHistogramQuoteAt(index) {
  const center = FLOW_HISTOGRAM_MIN_LOG10 + (index + 0.5) * FLOW_HISTOGRAM_STEP;
  return 10 ** center;
}

function emptyBucket(start) {
  return {
    start,
    end: start + FIVE_MIN_MS,
    histogram: emptyHistogram(),
    buyQuote: 0,
    sellQuote: 0,
    buyCount: 0,
    sellCount: 0,
    tradeCount: 0,
  };
}

function addTradeToBucket(bucket, trade) {
  const index = flowHistogramIndex(trade.quote);
  const isBuy = trade.side === 'buy';
  if (isBuy) {
    bucket.buyQuote += trade.quote;
    bucket.buyCount += 1;
    bucket.histogram.buyQuote[index] += trade.quote;
    bucket.histogram.buyCount[index] += 1;
  } else {
    bucket.sellQuote += trade.quote;
    bucket.sellCount += 1;
    bucket.histogram.sellQuote[index] += trade.quote;
    bucket.histogram.sellCount[index] += 1;
  }
  bucket.tradeCount += 1;
}

function histogramPercentileIndex(bucket, fraction) {
  const target = Math.max(1, Math.ceil(bucket.tradeCount * fraction));
  let cumulative = 0;
  for (let i = 0; i < FLOW_HISTOGRAM_BINS; i += 1) {
    cumulative += bucket.histogram.buyCount[i] + bucket.histogram.sellCount[i];
    if (cumulative >= target) return i;
  }
  return FLOW_HISTOGRAM_BINS - 1;
}

function finalizeBucket(bucket, provider, symbol) {
  if (!bucket || !bucket.tradeCount) return null;
  const p70Index = histogramPercentileIndex(bucket, 0.70);
  const p95Index = histogramPercentileIndex(bucket, 0.95);
  const row = {
    provider, symbol, bucket_time: new Date(bucket.start).toISOString(), bucket_end_time: new Date(bucket.end).toISOString(),
    buy_quote: bucket.buyQuote, sell_quote: bucket.sellQuote,
    large_buy_quote: 0, large_sell_quote: 0,
    medium_buy_quote: 0, medium_sell_quote: 0,
    small_buy_quote: 0, small_sell_quote: 0,
    large_buy_count: 0, large_sell_count: 0,
    medium_buy_count: 0, medium_sell_count: 0,
    small_buy_count: 0, small_sell_count: 0,
    trade_count: bucket.tradeCount,
    p70_quote: flowHistogramQuoteAt(p70Index),
    p95_quote: flowHistogramQuoteAt(p95Index),
    source: 'render_exchange_websocket_histogram', updated_at: new Date().toISOString(),
  };
  for (let i = 0; i < FLOW_HISTOGRAM_BINS; i += 1) {
    const tier = i >= p95Index ? 'large' : (i >= p70Index ? 'medium' : 'small');
    row[`${tier}_buy_quote`] += bucket.histogram.buyQuote[i];
    row[`${tier}_sell_quote`] += bucket.histogram.sellQuote[i];
    row[`${tier}_buy_count`] += bucket.histogram.buyCount[i];
    row[`${tier}_sell_count`] += bucket.histogram.sellCount[i];
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
      console.error(`[Step615] flow bucket persist failed: ${error?.message || error}`);
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
    addTradeToBucket(bucket, item);
    state.lastPrice = item.price;
    added += 1;
  }
  if (state.recentIds.size > MAX_RECENT_IDS) state.recentIds = new Set([...state.recentIds].slice(-Math.floor(MAX_RECENT_IDS / 2)));
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

function closeAndDeleteState(state, reason = 'evicted') {
  if (!state) return;
  clearInterval(state.heartbeatTimer);
  clearTimeout(state.reconnectTimer);
  state.lastRequestedAt = 0;
  const ws = state.ws;
  state.ws = null;
  try { ws?.close(1000, reason); } catch (_) {}
  states.delete(state.key);
}

function ensureStateCapacity(provider, symbol) {
  const key = `${provider}:${symbol}`;
  if (states.has(key) || states.size < MAX_ACTIVE_STATES) return;
  const candidates = [...states.values()]
    .filter((state) => !CORE_SYMBOL_SET.has(state.symbol))
    .sort((a, b) => a.lastRequestedAt - b.lastRequestedAt);
  if (candidates.length) closeAndDeleteState(candidates[0], 'capacity');
}

function getState(provider, symbol) {
  ensureStateCapacity(provider, symbol);
  const key = `${provider}:${symbol}`;
  let state = states.get(key);
  if (!state) {
    state = {
      key, provider, symbol, openBuckets: new Map(), completedBuckets: new Map(), recentIds: new Set(), waiters: new Set(),
      ws: null, status: 'idle', error: '', lastTradeAt: 0, lastPrice: null, lastRequestedAt: Date.now(), reconnectAttempt: 0,
      reconnectTimer: null, heartbeatTimer: null,
      metricRows: new Map(), metricLoaded: false, metricLoading: false, metricError: '', metricFetchedAt: 0,
      metricFetchPromise: null, metricCooldownUntil: 0,
      historyLoaded: false, historyLoading: false, historyError: '',
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
  // Persisted source rows are fixed 5-minute buckets. Every display bucket
  // must therefore be a whole multiple of five minutes; otherwise the API
  // would label 5-minute rows as 2-minute data and mislead the chart.
  if (coverageMs >= 20 * 60 * 60 * 1000) return 2 * 60 * 60 * 1000;
  if (coverageMs >= 6 * 60 * 60 * 1000) return 60 * 60 * 1000;
  if (coverageMs >= 60 * 60 * 1000) return 10 * 60 * 1000;
  return FIVE_MIN_MS;
}
function isoFrom(value) {
  const ms = normalizeTime(value);
  return ms ? new Date(ms).toISOString() : null;
}

async function fetchJson(url, { headers = {}, timeoutMs = 8000 } = {}) {
  const response = await fetch(url, {
    headers: { accept: 'application/json', 'user-agent': 'KakaWeb3/615', ...headers },
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await response.text();
  if (!response.ok) {
    const error = new Error(`upstream_${response.status}:${text.slice(0, 220)}`);
    error.status = response.status;
    throw error;
  }
  try { return text.trim() ? JSON.parse(text) : null; }
  catch (_) { throw new Error(`upstream_invalid_json:${text.slice(0, 160)}`); }
}

function metricBucketStart(value = Date.now()) {
  const ms = normalizeTime(value) || Date.now();
  return Math.floor(ms / FIVE_MIN_MS) * FIVE_MIN_MS;
}

function emptyMetricRow(provider, symbol, time) {
  const start = metricBucketStart(time);
  return {
    provider, symbol, bucket_time: new Date(start).toISOString(),
    open_interest: null, open_interest_value: null,
    global_long_short_ratio: null, global_long_account: null, global_short_account: null,
    top_account_long_short_ratio: null, top_account_long: null, top_account_short: null,
    top_position_long_short_ratio: null, top_position_long: null, top_position_short: null,
    source: 'render_exchange_public_metrics', updated_at: new Date().toISOString(),
  };
}

function metricKey(row) {
  return `${row.provider}:${row.symbol}:${row.bucket_time}`;
}

function metricRowStart(row) {
  return normalizeTime(row?.bucket_time) || 0;
}

function normalizeMetricRow(raw) {
  const provider = providerKey(raw?.provider);
  const symbol = symbolKey(raw?.symbol);
  const start = normalizeTime(raw?.bucket_time);
  if (!provider || !symbol || !start) return null;
  const row = emptyMetricRow(provider, symbol, start);
  const fields = [
    'open_interest','open_interest_value',
    'global_long_short_ratio','global_long_account','global_short_account',
    'top_account_long_short_ratio','top_account_long','top_account_short',
    'top_position_long_short_ratio','top_position_long','top_position_short',
  ];
  for (const field of fields) row[field] = asNumber(raw[field]);
  row.source = String(raw?.source || row.source);
  row.updated_at = String(raw?.updated_at || row.updated_at);
  return row;
}

function mergeMetricRow(targetMap, incoming) {
  const row = normalizeMetricRow(incoming);
  if (!row) return null;
  const start = metricRowStart(row);
  const current = targetMap.get(start) || emptyMetricRow(row.provider, row.symbol, start);
  const merged = { ...current };
  for (const [key, value] of Object.entries(row)) {
    if (value != null && value !== '') merged[key] = value;
  }
  targetMap.set(start, merged);
  return merged;
}

function queueMetricPersist(row) {
  if (!PERSISTENCE_ENABLED || !row) return;
  metricPersistQueue.set(metricKey(row), row);
}

async function flushMetricPersistQueue() {
  if (!PERSISTENCE_ENABLED || metricPersistFlushPromise || metricPersistQueue.size === 0) return;
  const rows = [...metricPersistQueue.values()].slice(0, 500);
  for (const row of rows) metricPersistQueue.delete(metricKey(row));
  metricPersistFlushPromise = (async () => {
    try {
      const response = await fetch(`${SUPABASE_URL}/rest/v1/${METRIC_TABLE}?on_conflict=provider,symbol,bucket_time`, {
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
      if (!response.ok) throw new Error(`metric_persist_http_${response.status}:${(await response.text()).slice(0, 180)}`);
    } catch (error) {
      for (const row of rows) metricPersistQueue.set(metricKey(row), row);
      console.error(`[Step615] contract metrics persist failed: ${error?.message || error}`);
    }
  })().finally(() => { metricPersistFlushPromise = null; });
  await metricPersistFlushPromise;
}

async function loadPersistedMetrics(state) {
  if (!PERSISTENCE_ENABLED || state.metricLoaded || state.metricLoading) return;
  state.metricLoading = true;
  try {
    const cutoff = new Date(Date.now() - METRIC_HISTORY_MS - FIVE_MIN_MS).toISOString();
    const query = new URLSearchParams({
      select: '*', provider: `eq.${state.provider}`, symbol: `eq.${state.symbol}`,
      bucket_time: `gte.${cutoff}`, order: 'bucket_time.asc', limit: '400',
    });
    const response = await fetch(`${SUPABASE_URL}/rest/v1/${METRIC_TABLE}?${query}`, {
      headers: { apikey: SUPABASE_SERVICE_ROLE_KEY, authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}` },
      signal: AbortSignal.timeout(12000),
    });
    const text = await response.text();
    if (!response.ok) throw new Error(`metric_history_http_${response.status}:${text.slice(0, 180)}`);
    const rows = JSON.parse(text);
    for (const raw of Array.isArray(rows) ? rows : []) mergeMetricRow(state.metricRows, raw);
    state.metricLoaded = true;
    state.metricError = '';
  } catch (error) {
    state.metricError = String(error?.message || error).slice(0, 220);
  } finally {
    state.metricLoading = false;
  }
}

function metricPoint(map, provider, symbol, time) {
  const start = metricBucketStart(time);
  let row = map.get(start);
  if (!row) {
    row = emptyMetricRow(provider, symbol, start);
    map.set(start, row);
  }
  return row;
}

function applyRatio(row, prefix, ratio, longShare, shortShare) {
  const r = asNumber(ratio);
  const l = asNumber(longShare);
  const sh = asNumber(shortShare);
  if (r != null) row[`${prefix}_long_short_ratio`] = r;
  if (l != null) row[`${prefix}_long`] = l;
  if (sh != null) row[`${prefix}_short`] = sh;
  if (r == null && l != null && sh != null && sh > 0) row[`${prefix}_long_short_ratio`] = l / sh;
}

async function firstWorkingJson(urls, options = {}) {
  const errors = [];
  for (const url of urls) {
    try { return await fetchJson(url, options); }
    catch (error) { errors.push(String(error?.message || error)); }
  }
  throw new Error(errors.join(' | ').slice(0, 700) || 'all_upstreams_failed');
}

async function fetchBinanceMetricRows(state) {
  const hosts = ['https://fapi.binance.com','https://fapi1.binance.com'];
  const headers = BINANCE_API_KEY ? { 'X-MBX-APIKEY': BINANCE_API_KEY } : {};
  let settled = null;
  let lastError = null;
  for (const host of hosts) {
    try {
      settled = await Promise.allSettled([
        fetchJson(`${host}/futures/data/openInterestHist?symbol=${encodeURIComponent(state.symbol)}&period=5m&limit=288`, { headers, timeoutMs: 6500 }),
        fetchJson(`${host}/futures/data/globalLongShortAccountRatio?symbol=${encodeURIComponent(state.symbol)}&period=5m&limit=288`, { headers, timeoutMs: 6500 }),
        fetchJson(`${host}/futures/data/topLongShortAccountRatio?symbol=${encodeURIComponent(state.symbol)}&period=5m&limit=288`, { headers, timeoutMs: 6500 }),
        fetchJson(`${host}/futures/data/topLongShortPositionRatio?symbol=${encodeURIComponent(state.symbol)}&period=5m&limit=288`, { headers, timeoutMs: 6500 }),
      ]);
      if (settled.some((item) => item.status === 'fulfilled')) break;
      lastError = new Error(settled.map((item) => item.status === 'rejected' ? item.reason?.message : '').join(' | '));
    } catch (error) { lastError = error; }
  }
  if (!settled || !settled.some((item) => item.status === 'fulfilled')) throw lastError || new Error('binance_metrics_unavailable');
  const map = new Map();
  const oi = settled[0].status === 'fulfilled' && Array.isArray(settled[0].value) ? settled[0].value : [];
  for (const item of oi) {
    const row = metricPoint(map, state.provider, state.symbol, item.timestamp);
    row.open_interest = asNumber(item.sumOpenInterest);
    row.open_interest_value = asNumber(item.sumOpenInterestValue);
  }
  const parseRatioList = (index, prefix) => {
    const list = settled[index].status === 'fulfilled' && Array.isArray(settled[index].value) ? settled[index].value : [];
    for (const item of list) {
      const row = metricPoint(map, state.provider, state.symbol, item.timestamp);
      applyRatio(row, prefix, item.longShortRatio, item.longAccount, item.shortAccount);
    }
  };
  parseRatioList(1, 'global');
  parseRatioList(2, 'top_account');
  parseRatioList(3, 'top_position');
  return [...map.values()];
}

async function fetchOkxMetricRows(state) {
  const [base] = splitSymbol(state.symbol);
  const instId = okxInstId(state.symbol);
  const now = Date.now();
  const begin = now - METRIC_HISTORY_MS;
  const settled = await Promise.allSettled([
    firstWorkingJson([
      `https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId=${encodeURIComponent(instId)}`,
      `https://aws.okx.com/api/v5/public/open-interest?instType=SWAP&instId=${encodeURIComponent(instId)}`,
    ]),
    firstWorkingJson([
      `https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-volume?ccy=${encodeURIComponent(base)}&period=5m&begin=${begin}&end=${now}`,
      `https://aws.okx.com/api/v5/rubik/stat/contracts/open-interest-volume?ccy=${encodeURIComponent(base)}&period=5m&begin=${begin}&end=${now}`,
    ]),
    firstWorkingJson([
      `https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy=${encodeURIComponent(base)}&period=5m&begin=${begin}&end=${now}`,
      `https://aws.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy=${encodeURIComponent(base)}&period=5m&begin=${begin}&end=${now}`,
    ]),
  ]);
  if (!settled.some((item) => item.status === 'fulfilled')) throw new Error('okx_metrics_unavailable');
  const map = new Map();
  if (settled[1].status === 'fulfilled') {
    for (const item of Array.isArray(settled[1].value?.data) ? settled[1].value.data : []) {
      const ts = Array.isArray(item) ? item[0] : item.ts;
      const row = metricPoint(map, state.provider, state.symbol, ts);
      if (Array.isArray(item)) {
        // OKX Rubik returns aggregate OI first and turnover fields after it.
        // Treat the aggregate OI as a quote-value series; do not mislabel turnover as OI.
        const value = asNumber(item[1]);
        if (value != null) row.open_interest_value = value;
      } else {
        const amount = asNumber(item.oi ?? item.openInterest);
        const value = asNumber(item.oiUsd ?? item.openInterestUsd);
        if (amount != null) row.open_interest = amount;
        if (value != null) row.open_interest_value = value;
      }
    }
  }
  if (settled[2].status === 'fulfilled') {
    for (const item of Array.isArray(settled[2].value?.data) ? settled[2].value.data : []) {
      const ts = Array.isArray(item) ? item[0] : item.ts;
      const ratio = asNumber(Array.isArray(item) ? item[1] : (item.ratio ?? item.longShortRatio));
      const row = metricPoint(map, state.provider, state.symbol, ts);
      applyRatio(row, 'global', ratio, null, null);
    }
  }
  if (settled[0].status === 'fulfilled') {
    for (const item of Array.isArray(settled[0].value?.data) ? settled[0].value.data : []) {
      const row = metricPoint(map, state.provider, state.symbol, item.ts ?? now);
      row.open_interest = asNumber(item.oi);
      row.open_interest_value = asNumber(item.oiUsd);
    }
  }
  return [...map.values()];
}

async function fetchBybitMetricRows(state) {
  const hosts = ['https://api.bybit.com','https://api.bytick.com'];
  let pair = null;
  let lastError = null;
  for (const host of hosts) {
    try {
      pair = await Promise.all([
        fetchJson(`${host}/v5/market/open-interest?category=linear&symbol=${encodeURIComponent(state.symbol)}&intervalTime=5min&limit=200`, { timeoutMs: 7000 }),
        fetchJson(`${host}/v5/market/account-ratio?category=linear&symbol=${encodeURIComponent(state.symbol)}&period=5min&limit=500`, { timeoutMs: 7000 }),
      ]);
      break;
    } catch (error) { lastError = error; }
  }
  if (!pair) throw lastError || new Error('bybit_metrics_unavailable');
  const map = new Map();
  for (const item of Array.isArray(pair[0]?.result?.list) ? pair[0].result.list : []) {
    const row = metricPoint(map, state.provider, state.symbol, item.timestamp);
    row.open_interest = asNumber(item.openInterest);
    if (row.open_interest != null && state.lastPrice) row.open_interest_value = row.open_interest * state.lastPrice;
  }
  for (const item of Array.isArray(pair[1]?.result?.list) ? pair[1].result.list : []) {
    const row = metricPoint(map, state.provider, state.symbol, item.timestamp);
    applyRatio(row, 'global', null, item.buyRatio, item.sellRatio);
  }
  return [...map.values()];
}

async function fetchBitgetMetricRows(state) {
  const settled = await Promise.allSettled([
    fetchJson(`https://api.bitget.com/api/v2/mix/market/open-interest?symbol=${encodeURIComponent(state.symbol)}&productType=usdt-futures`, { timeoutMs: 7000 }),
    firstWorkingJson([
      `https://api.bitget.com/api/v3/market/futures-long-short?symbol=${encodeURIComponent(state.symbol)}`,
      `https://api.bitget.com/api/v2/mix/market/account-long-short?symbol=${encodeURIComponent(state.symbol)}&productType=usdt-futures&period=5m`,
    ], { timeoutMs: 7000 }),
  ]);
  if (!settled.some((item) => item.status === 'fulfilled')) throw new Error('bitget_metrics_unavailable');
  const map = new Map();
  if (settled[0].status === 'fulfilled') {
    const data = settled[0].value?.data;
    const list = Array.isArray(data?.openInterestList) ? data.openInterestList : (Array.isArray(data) ? data : data ? [data] : []);
    for (const item of list) {
      const row = metricPoint(map, state.provider, state.symbol, item.ts ?? item.timestamp ?? Date.now());
      row.open_interest = asNumber(item.size ?? item.openInterest ?? item.amount);
      row.open_interest_value = asNumber(item.openInterestUsd ?? item.usdValue);
      if (row.open_interest_value == null && row.open_interest != null && state.lastPrice) row.open_interest_value = row.open_interest * state.lastPrice;
    }
  }
  if (settled[1].status === 'fulfilled') {
    const data = settled[1].value?.data;
    const list = Array.isArray(data) ? data : (Array.isArray(data?.list) ? data.list : data ? [data] : []);
    for (const item of list) {
      const row = metricPoint(map, state.provider, state.symbol, item.ts ?? item.timestamp ?? item.time ?? Date.now());
      const longShare = item.buyRatio ?? item.longAccountRatio ?? item.longRatio ?? item.longAccount;
      const shortShare = item.sellRatio ?? item.shortAccountRatio ?? item.shortRatio ?? item.shortAccount;
      applyRatio(row, 'global', item.longShortRatio ?? item.ratio, longShare, shortShare);
    }
  }
  return [...map.values()];
}

async function fetchGateMetricRows(state) {
  const contract = gateSymbol(state.symbol);
  const raw = await firstWorkingJson([
    `https://api.gateio.ws/api/v4/futures/usdt/contract_stats?contract=${encodeURIComponent(contract)}&interval=5m&limit=288`,
    `https://fx-api.gateio.ws/api/v4/futures/usdt/contract_stats?contract=${encodeURIComponent(contract)}&interval=5m&limit=288`,
  ], { timeoutMs: 8000 });
  const map = new Map();
  for (const item of Array.isArray(raw) ? raw : []) {
    const row = metricPoint(map, state.provider, state.symbol, item.time ?? item.timestamp);
    row.open_interest = asNumber(item.open_interest);
    row.open_interest_value = asNumber(item.open_interest_usd);
    applyRatio(row, 'global', item.long_short_ratio, item.long_ratio, item.short_ratio);
  }
  return [...map.values()];
}

async function fetchProviderMetricRows(state) {
  if (state.provider === 'binance') return fetchBinanceMetricRows(state);
  if (state.provider === 'okx') return fetchOkxMetricRows(state);
  if (state.provider === 'bybit') return fetchBybitMetricRows(state);
  if (state.provider === 'bitget') return fetchBitgetMetricRows(state);
  if (state.provider === 'gate') return fetchGateMetricRows(state);
  return [];
}

function metricPayloadFromState(state) {
  const cutoff = Date.now() - METRIC_HISTORY_MS;
  const rows = [...state.metricRows.values()]
    .filter((row) => metricRowStart(row) >= cutoff)
    .sort((a, b) => metricRowStart(a) - metricRowStart(b))
    .slice(-288);
  const oiRows = [];
  const ratioRows = [];
  for (const row of rows) {
    const sourceTime = row.bucket_time;
    if (row.open_interest != null || row.open_interest_value != null) {
      oiRows.push({ source_time: sourceTime, open_interest: row.open_interest, open_interest_value: row.open_interest_value });
    }
    const ratios = [
      ['global_account', row.global_long_short_ratio, row.global_long_account, row.global_short_account],
      ['top_account', row.top_account_long_short_ratio, row.top_account_long, row.top_account_short],
      ['top_position', row.top_position_long_short_ratio, row.top_position_long, row.top_position_short],
    ];
    for (const [type, ratio, longShare, shortShare] of ratios) {
      if (ratio == null && longShare == null && shortShare == null) continue;
      ratioRows.push({ source_time: sourceTime, ratio_type: type, long_short_ratio: ratio, long_account: longShare, short_account: shortShare });
    }
  }
  return { oi_rows: oiRows, ratio_rows: ratioRows, metric_error: state.metricError, metric_updated_at: rows.at(-1)?.bucket_time || null };
}

async function fetchVenueMetrics(state) {
  await loadPersistedMetrics(state);
  const now = Date.now();
  const latest = [...state.metricRows.keys()].sort((a, b) => b - a)[0] || 0;
  const fresh = latest && now - latest < METRIC_REFRESH_MS + 30000;
  if (fresh || now < state.metricCooldownUntil) return metricPayloadFromState(state);
  if (!state.metricFetchPromise) {
    state.metricFetchPromise = (async () => {
      try {
        const rows = await fetchProviderMetricRows(state);
        if (!rows.length) throw new Error(`${state.provider}_metrics_empty`);
        for (const raw of rows) {
          const merged = mergeMetricRow(state.metricRows, raw);
          if (merged) queueMetricPersist(merged);
        }
        state.metricFetchedAt = Date.now();
        state.metricError = '';
        state.metricCooldownUntil = 0;
        await flushMetricPersistQueue();
      } catch (error) {
        const message = String(error?.message || error).slice(0, 700);
        state.metricError = message;
        const restricted = /upstream_(418|429|451)|banned|restricted|cloudfront/i.test(message);
        state.metricCooldownUntil = Date.now() + (restricted ? 30 * 60 * 1000 : 5 * 60 * 1000);
      }
    })().finally(() => { state.metricFetchPromise = null; });
  }
  await state.metricFetchPromise;
  return metricPayloadFromState(state);
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
    ok:true, version:'615', provider:state.provider, symbol:state.symbol,
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
  const count=()=>[...state.openBuckets.values()].reduce((sum,b)=>sum+(b.tradeCount||0),0);
  if(count()>=minTrades||waitMs<=0)return Promise.resolve();
  return new Promise((resolve)=>{let timer;const done=()=>{clearTimeout(timer);state.waiters.delete(check);resolve();};const check=()=>{if(count()>=minTrades)done();};state.waiters.add(check);timer=setTimeout(done,waitMs);});
}

export async function handleContractFlow(req,res,url){
  if(url.pathname==='/api/contract-flow/health'){
    sendJson(res,200,{ok:true,version:'615',streams:states.size,persistence_enabled:PERSISTENCE_ENABLED,persist_queue:persistQueue.size,metric_persist_queue:metricPersistQueue.size,metric_table:METRIC_TABLE,flow_memory_mode:'fixed_histogram',max_active_streams:MAX_ACTIVE_STATES,core_symbols:CORE_SYMBOLS,time:new Date().toISOString()});return true;
  }
  if(url.pathname==='/api/contract-flow/warm'){
    let started=0;
    for(const provider of PROVIDERS)for(const symbol of CORE_SYMBOLS){const state=getState(provider,symbol);state.lastRequestedAt=Date.now();started+=1;}
    sendJson(res,200,{ok:true,version:'615',started,persistence_enabled:PERSISTENCE_ENABLED,core_symbols:CORE_SYMBOLS,time:new Date().toISOString()});return true;
  }
  if(url.pathname!=='/api/contract-flow')return false;
  if(req.method!=='GET'&&req.method!=='POST'){sendJson(res,405,{ok:false,error:'method_not_allowed'});return true;}
  let provider=providerKey(url.searchParams.get('provider'));let symbol=symbolKey(url.searchParams.get('symbol'));let waitMs=Math.min(5000,Math.max(0,Number(url.searchParams.get('wait_ms')||3200)));
  if(req.method==='POST'){const chunks=[];for await(const chunk of req)chunks.push(chunk);try{const body=JSON.parse(Buffer.concat(chunks).toString('utf8')||'{}');provider=providerKey(body.provider)||provider;symbol=symbolKey(body.symbol)||symbol;if(Number.isFinite(Number(body.wait_ms)))waitMs=Math.min(5000,Math.max(0,Number(body.wait_ms)));}catch(_){}}
  if(!provider||!symbol||!symbol.endsWith('USDT')){sendJson(res,400,{ok:false,error:'invalid_provider_or_symbol'});return true;}
  const state=getState(provider,symbol);await loadPersistedHistory(state);await waitForTrades(state,20,waitMs);const venueMetrics=await fetchVenueMetrics(state);const payload=summarize(state,venueMetrics);
  const hasData=payload.trade_count>0||payload.stored_5m_buckets>0;sendJson(res,hasData?200:503,hasData?payload:{...payload,ok:false,error:'building_24h_history'});return true;
}

setInterval(()=>{const now=Date.now();for(const [key,state] of states.entries()){finalizeReadyBuckets(state,now);if(now-state.lastRequestedAt<=IDLE_CLOSE_MS)continue;closeAndDeleteState(state,'idle');}},60000).unref();
setInterval(()=>{flushPersistQueue().catch(()=>{});flushMetricPersistQueue().catch(()=>{});},20000).unref();
