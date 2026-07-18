const STEP_VERSION = '650.8.15';
const SUPPORTED_PROVIDERS = new Set(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const GLOBAL_FEED_PROVIDERS = new Set(['binance', 'okx', 'bitget']);
const FEEDS = new Map();
const STATS = new Map();
const META_CACHE = new Map();
const SERVICE_STARTED_AT_MS = Date.now();
const READY_TIMEOUT_MS = 7_000;
const DYNAMIC_FEED_IDLE_MS = 24 * 60 * 60_000;
const RECENT_EVENT_RETENTION_MS = 24 * 60 * 60_000;
const MAX_EVENTS_PER_SYMBOL = 60;
const DEDUPE_RETENTION_MS = 2 * 60 * 60_000;
const MINUTE_BUCKET_MS = 60_000;
const QUARTER_BUCKET_MS = 15 * 60_000;
const HOUR_BUCKET_MS = 60 * 60_000;
const MINUTE_RETENTION_MS = 65 * 60_000;
const QUARTER_RETENTION_MS = 25 * 60 * 60_000;
const HOUR_RETENTION_MS = 15 * 24 * 60 * 60_000;
const STATS_RETENTION_MS = 15 * 24 * 60 * 60_000;
const META_FRESH_MS = 6 * 60 * 60_000;
const DYNAMIC_LIMIT_PER_PROVIDER = Math.max(4, Math.min(24, Number(process.env.KAKA_LIQUIDATION_DYNAMIC_LIMIT || 12)));
const CORE_SYMBOLS = String(process.env.KAKA_LIQUIDATION_CORE_SYMBOLS || 'BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,AVAXUSDT,LINKUSDT,SUIUSDT')
  .split(',')
  .map((value) => value.trim().toUpperCase().replace(/[^A-Z0-9]/g, ''))
  .filter(Boolean)
  .slice(0, 20);
const PERIODS = Object.freeze({
  '15m': { durationMs: 15 * 60_000, chartBucketMs: 60_000, source: 'minute' },
  '1h': { durationMs: 60 * 60_000, chartBucketMs: 5 * 60_000, source: 'minute' },
  '4h': { durationMs: 4 * 60 * 60_000, chartBucketMs: 15 * 60_000, source: 'quarter' },
  '12h': { durationMs: 12 * 60 * 60_000, chartBucketMs: 30 * 60_000, source: 'quarter' },
  '24h': { durationMs: 24 * 60 * 60_000, chartBucketMs: 60 * 60_000, source: 'quarter' },
  '3d': { durationMs: 3 * 24 * 60 * 60_000, chartBucketMs: 4 * 60 * 60_000, source: 'hour' },
  '7d': { durationMs: 7 * 24 * 60 * 60_000, chartBucketMs: 12 * 60 * 60_000, source: 'hour' },
  '14d': { durationMs: 14 * 24 * 60 * 60_000, chartBucketMs: 24 * 60 * 60_000, source: 'hour' },
});
const BINANCE_LIQUIDATION_CONNECT_GAP_MS = 5_000;
const BINANCE_LIQUIDATION_CONNECT_WINDOW_MS = 5 * 60_000;
const BINANCE_LIQUIDATION_MAX_CONNECT_ATTEMPTS_5M = 10;
const binanceLiquidationConnectAttempts = [];
let binanceLiquidationConnectChain = Promise.resolve();
let binanceLiquidationLastConnectAt = 0;
const binanceLiquidationWsStats = { attempts: 0, waits: 0, window_blocks: 0 };
let WS_CTOR_PROMISE = null;

async function resolveWebSocketCtor() {
  if (!WS_CTOR_PROMISE) {
    WS_CTOR_PROMISE = (async () => {
      if (typeof globalThis.WebSocket === 'function') return globalThis.WebSocket;
      const imported = await import('ws');
      return imported.WebSocket || imported.default;
    })();
  }
  return WS_CTOR_PROMISE;
}

function wsListen(socket, eventName, handler) {
  if (typeof socket?.addEventListener === 'function') {
    socket.addEventListener(eventName, handler);
    return;
  }
  if (typeof socket?.on === 'function') {
    socket.on(eventName, handler);
    return;
  }
  socket[`on${eventName}`] = handler;
}

function wsReady(socket) {
  return socket && Number(socket.readyState) === 1;
}

function closeWsQuietly(socket) {
  try {
    if (typeof socket?.terminate === 'function') socket.terminate();
    else if (typeof socket?.close === 'function') socket.close();
  } catch (_) {}
}

function sendWs(socket, payload) {
  if (!wsReady(socket)) return false;
  try {
    socket.send(typeof payload === 'string' ? payload : JSON.stringify(payload));
    return true;
  } catch (_) {
    return false;
  }
}

async function wsMessageText(eventOrData) {
  const value = eventOrData && typeof eventOrData === 'object' && 'data' in eventOrData
    ? eventOrData.data
    : eventOrData;
  if (typeof value === 'string') return value;
  if (Buffer.isBuffer(value)) return value.toString('utf8');
  if (value instanceof ArrayBuffer) return Buffer.from(value).toString('utf8');
  if (ArrayBuffer.isView(value)) return Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString('utf8');
  if (value && typeof value.text === 'function') return await value.text();
  return String(value ?? '');
}

function normalizeProvider(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'okex') return 'okx';
  if (raw === 'gate.io' || raw === 'gateio') return 'gate';
  return raw;
}

function compactSymbol(value) {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/PERPETUAL$/i, '')
    .replace(/-SWAP$/i, '')
    .replace(/[^A-Z0-9]/g, '');
}

function quoteFromCompact(symbol) {
  for (const quote of ['USDT', 'USDC', 'USD']) {
    if (symbol.endsWith(quote) && symbol.length > quote.length) return quote;
  }
  return 'USDT';
}

function baseFromCompact(symbol) {
  const quote = quoteFromCompact(symbol);
  return symbol.endsWith(quote) ? symbol.slice(0, -quote.length) : symbol;
}

function providerSymbol(provider, rawSymbol) {
  const compact = compactSymbol(rawSymbol);
  const quote = quoteFromCompact(compact);
  const base = baseFromCompact(compact);
  if (!base || !quote) throw new Error('invalid_symbol');
  if (provider === 'okx') return `${base}-${quote}-SWAP`;
  if (provider === 'gate') return `${base}_${quote}`;
  return `${base}${quote}`;
}

function numberValue(value) {
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  const parsed = Number(String(value ?? '').trim());
  return Number.isFinite(parsed) ? parsed : null;
}

function positiveNumber(value) {
  const parsed = numberValue(value);
  return parsed != null && parsed > 0 ? parsed : null;
}

function integerValue(value) {
  const parsed = numberValue(value);
  return parsed == null ? 0 : Math.trunc(parsed);
}

function sendJson(res, statusCode, payload, extraHeaders = {}) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(statusCode, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET, OPTIONS',
    'content-length': String(body.length),
    ...extraHeaders,
  });
  res.end(body);
}

async function fetchJson(url, timeoutMs = 8_000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        accept: 'application/json',
        'user-agent': 'KakaWeb3-contract-liquidation/640',
      },
      signal: controller.signal,
    });
    const text = await response.text();
    if (!response.ok) {
      const error = new Error(`HTTP_${response.status}`);
      error.statusCode = response.status;
      error.bodyText = text.slice(0, 800);
      throw error;
    }
    return JSON.parse(text);
  } finally {
    clearTimeout(timer);
  }
}

async function fetchFirstJson(urls, timeoutMs = 8_000) {
  let lastError = null;
  for (const url of urls) {
    try {
      return await fetchJson(url, timeoutMs);
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError || new Error('all_upstreams_failed');
}

async function okxContractMultiplier(instId) {
  const key = `okx:${instId}`;
  const cached = META_CACHE.get(key);
  if (cached && Date.now() - cached.storedAt <= META_FRESH_MS) return cached.multiplier;
  const decoded = await fetchJson(`https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId=${encodeURIComponent(instId)}`);
  const row = Array.isArray(decoded?.data) ? decoded.data[0] : null;
  const ctVal = positiveNumber(row?.ctVal) ?? 1;
  const ctMult = positiveNumber(row?.ctMult) ?? 1;
  const multiplier = ctVal * ctMult;
  META_CACHE.set(key, { multiplier, storedAt: Date.now() });
  return multiplier;
}

async function gateContractMultiplier(contract) {
  const key = `gate:${contract}`;
  const cached = META_CACHE.get(key);
  if (cached && Date.now() - cached.storedAt <= META_FRESH_MS) return cached.multiplier;
  const decoded = await fetchFirstJson([
    `https://fx-api.gateio.ws/api/v4/futures/usdt/contracts/${encodeURIComponent(contract)}`,
    `https://api.gateio.ws/api/v4/futures/usdt/contracts/${encodeURIComponent(contract)}`,
  ]);
  const multiplier = positiveNumber(decoded?.quanto_multiplier) ?? 1;
  META_CACHE.set(key, { multiplier, storedAt: Date.now() });
  return multiplier;
}

function feedKey(provider, symbol) {
  if (GLOBAL_FEED_PROVIDERS.has(provider)) return `${provider}|all`;
  return `${provider}|${providerSymbol(provider, symbol)}`;
}

function sourceInfo(provider) {
  switch (provider) {
    case 'binance':
      return {
        source: 'binance_official_public_contract_liquidation_websocket',
        transport: 'websocket_all_market_forceOrder',
        upstream_host: 'fstream.binance.com',
        coverage: 'largest_liquidation_per_symbol_within_1000ms',
      };
    case 'okx':
      return {
        source: 'okx_official_public_contract_liquidation_websocket',
        transport: 'websocket_public_liquidation-orders',
        upstream_host: 'ws.okx.com',
        coverage: 'recent_liquidation_orders_not_total_market_count',
      };
    case 'bybit':
      return {
        source: 'bybit_official_public_contract_liquidation_websocket',
        transport: 'websocket_public_allLiquidation',
        upstream_host: 'stream.bybit.com',
        coverage: 'all_liquidation_stream_500ms',
      };
    case 'bitget':
      return {
        source: 'bitget_official_public_contract_liquidation_websocket',
        transport: 'websocket_public_liquidation',
        upstream_host: 'ws.bitget.com',
        coverage: 'largest_long_and_short_liquidation_per_pair_per_second',
      };
    case 'gate':
      return {
        source: 'gate_official_public_contract_liquidation_websocket',
        transport: 'websocket_public_liquidates',
        upstream_host: 'fx-ws.gateio.ws',
        coverage: 'up_to_one_liquidation_order_per_contract_per_second',
      };
    default:
      return { source: '', transport: '', upstream_host: '', coverage: '' };
  }
}

function createFeed(provider, symbol) {
  const key = feedKey(provider, symbol);
  const info = sourceInfo(provider);
  const feed = {
    key,
    provider,
    requestedNativeSymbol: providerSymbol(provider, symbol),
    socket: null,
    connecting: null,
    reconnectTimer: null,
    reconnectAttempt: 0,
    ready: false,
    manuallyClosing: false,
    openedAt: 0,
    lastMessageAt: 0,
    lastError: '',
    eventsBySymbol: new Map(),
    accessBySymbol: new Map(),
    waiters: new Set(),
    heartbeatTimer: null,
    persistent: GLOBAL_FEED_PROVIDERS.has(provider),
    core: false,
    lastAccessAt: Date.now(),
    ...info,
  };
  FEEDS.set(key, feed);
  return feed;
}

function getFeed(provider, symbol) {
  const key = feedKey(provider, symbol);
  return FEEDS.get(key) || createFeed(provider, symbol);
}

function touchFeed(feed, symbol) {
  const now = Date.now();
  feed.lastAccessAt = now;
  feed.accessBySymbol.set(compactSymbol(symbol), now);
}

function symbolIsActive(feed, symbol) {
  if (feed.persistent) return true;
  const compact = compactSymbol(symbol);
  const touchedAt = feed.accessBySymbol.get(compact);
  return touchedAt != null && Date.now() - touchedAt <= DYNAMIC_FEED_IDLE_MS;
}

function feedHasActiveSymbols(feed) {
  if (feed.persistent) return true;
  const now = Date.now();
  for (const [symbol, time] of [...feed.accessBySymbol.entries()]) {
    if (now - time <= DYNAMIC_FEED_IDLE_MS) return true;
    feed.accessBySymbol.delete(symbol);
  }
  return false;
}

function feedEvents(feed, symbol) {
  const compact = compactSymbol(symbol);
  const rows = feed.eventsBySymbol.get(compact);
  return Array.isArray(rows) ? rows : [];
}


function statsKey(provider, symbol) {
  return `${provider}|${compactSymbol(symbol)}`;
}

function createBucket(startMs, durationMs) {
  return {
    start_ms: startMs,
    end_ms: startMs + durationMs,
    long_notional: 0,
    short_notional: 0,
    total_notional: 0,
    long_count: 0,
    short_count: 0,
    event_count: 0,
    largest_event: null,
  };
}

function cloneLargestEvent(row) {
  if (!row) return null;
  return {
    id: String(row.id || ''),
    provider: String(row.provider || ''),
    symbol: compactSymbol(row.symbol),
    time_ms: integerValue(row.time_ms),
    price: positiveNumber(row.price),
    notional: positiveNumber(row.notional),
    liquidation_side: String(row.liquidation_side || ''),
  };
}

function applyEventToBucket(bucket, row) {
  const value = positiveNumber(row?.notional);
  if (value == null) return;
  const side = String(row?.liquidation_side || '').toLowerCase();
  bucket.total_notional += value;
  bucket.event_count += 1;
  if (side === 'long') {
    bucket.long_notional += value;
    bucket.long_count += 1;
  } else if (side === 'short') {
    bucket.short_notional += value;
    bucket.short_count += 1;
  }
  if (!bucket.largest_event || value > Number(bucket.largest_event.notional || 0)) {
    bucket.largest_event = cloneLargestEvent(row);
  }
}

function mergeBucketInto(target, bucket) {
  if (!bucket) return;
  target.long_notional += Number(bucket.long_notional || 0);
  target.short_notional += Number(bucket.short_notional || 0);
  target.total_notional += Number(bucket.total_notional || 0);
  target.long_count += Number(bucket.long_count || 0);
  target.short_count += Number(bucket.short_count || 0);
  target.event_count += Number(bucket.event_count || 0);
  const candidate = bucket.largest_event;
  if (candidate && (!target.largest_event || Number(candidate.notional || 0) > Number(target.largest_event.notional || 0))) {
    target.largest_event = cloneLargestEvent(candidate);
  }
}

function getStats(provider, symbol, { create = true, observedSinceMs = null } = {}) {
  const normalizedSymbol = compactSymbol(symbol);
  const key = statsKey(provider, normalizedSymbol);
  let state = STATS.get(key);
  if (!state && create) {
    const now = Date.now();
    state = {
      key,
      provider,
      symbol: normalizedSymbol,
      createdAt: Number(observedSinceMs || now),
      observedSinceMs: Number(observedSinceMs || now),
      lastAccessAt: now,
      lastEventAt: 0,
      lastGapAtMs: 0,
      minuteBuckets: new Map(),
      quarterBuckets: new Map(),
      hourBuckets: new Map(),
      recentEvents: [],
      dedupe: new Map(),
    };
    STATS.set(key, state);
  }
  if (state) {
    state.lastAccessAt = Date.now();
    if (observedSinceMs && observedSinceMs > 0) {
      state.observedSinceMs = Math.min(Number(state.observedSinceMs || observedSinceMs), Number(observedSinceMs));
    }
  }
  return state;
}


function markFeedGap(feed) {
  const now = Date.now();
  if (GLOBAL_FEED_PROVIDERS.has(feed.provider)) {
    for (const state of STATS.values()) {
      if (state.provider === feed.provider) state.lastGapAtMs = now;
    }
    return;
  }
  const symbols = new Set([
    compactSymbol(feed.requestedNativeSymbol),
    ...feed.accessBySymbol.keys(),
  ]);
  for (const symbol of symbols) {
    const state = getStats(feed.provider, symbol, { create: false });
    if (state) state.lastGapAtMs = now;
  }
}

function bucketFor(map, timeMs, durationMs) {
  const start = Math.floor(timeMs / durationMs) * durationMs;
  let bucket = map.get(start);
  if (!bucket) {
    bucket = createBucket(start, durationMs);
    map.set(start, bucket);
  }
  return bucket;
}

function updateStats(row, observedSinceMs = null) {
  const provider = normalizeProvider(row?.provider);
  const symbol = compactSymbol(row?.symbol);
  const timeMs = integerValue(row?.time_ms);
  const id = String(row?.id || '');
  if (!provider || !symbol || timeMs <= 0 || !id) return false;
  const state = getStats(provider, symbol, { observedSinceMs });
  if (!state) return false;
  const seenAt = state.dedupe.get(id);
  if (seenAt && Date.now() - seenAt <= DEDUPE_RETENTION_MS) return false;
  state.dedupe.set(id, timeMs);
  state.lastEventAt = Math.max(state.lastEventAt || 0, timeMs);
  applyEventToBucket(bucketFor(state.minuteBuckets, timeMs, MINUTE_BUCKET_MS), row);
  applyEventToBucket(bucketFor(state.quarterBuckets, timeMs, QUARTER_BUCKET_MS), row);
  applyEventToBucket(bucketFor(state.hourBuckets, timeMs, HOUR_BUCKET_MS), row);
  state.recentEvents.unshift({ ...row });
  state.recentEvents.sort((a, b) => integerValue(b.time_ms) - integerValue(a.time_ms));
  if (state.recentEvents.length > MAX_EVENTS_PER_SYMBOL) {
    state.recentEvents.length = MAX_EVENTS_PER_SYMBOL;
  }
  trimStatsState(state);
  return true;
}

function trimBucketMap(map, cutoffMs) {
  for (const key of [...map.keys()]) {
    if (Number(key) < cutoffMs) map.delete(key);
  }
}

function trimStatsState(state) {
  const now = Date.now();
  trimBucketMap(state.minuteBuckets, now - MINUTE_RETENTION_MS);
  trimBucketMap(state.quarterBuckets, now - QUARTER_RETENTION_MS);
  trimBucketMap(state.hourBuckets, now - HOUR_RETENTION_MS);
  state.recentEvents = state.recentEvents.filter((row) => integerValue(row.time_ms) >= now - RECENT_EVENT_RETENTION_MS).slice(0, MAX_EVENTS_PER_SYMBOL);
  for (const [id, timeMs] of [...state.dedupe.entries()]) {
    if (now - Number(timeMs || 0) > DEDUPE_RETENTION_MS) state.dedupe.delete(id);
  }
}

function sourceMapForPeriod(state, period) {
  if (period.source === 'minute') return state.minuteBuckets;
  if (period.source === 'quarter') return state.quarterBuckets;
  return state.hourBuckets;
}

function buildStatistics(state, periodKey, now = Date.now()) {
  const period = PERIODS[periodKey] || PERIODS['24h'];
  const cutoff = now - period.durationMs;
  const sourceMap = sourceMapForPeriod(state, period);
  const sourceBuckets = [...sourceMap.values()]
    .filter((bucket) => Number(bucket.end_ms || 0) > cutoff && Number(bucket.start_ms || 0) <= now)
    .sort((a, b) => Number(a.start_ms || 0) - Number(b.start_ms || 0));
  const summary = createBucket(cutoff, period.durationMs);
  for (const bucket of sourceBuckets) mergeBucketInto(summary, bucket);

  const chart = new Map();
  for (const bucket of sourceBuckets) {
    const chartStart = Math.floor(Number(bucket.start_ms || 0) / period.chartBucketMs) * period.chartBucketMs;
    let target = chart.get(chartStart);
    if (!target) {
      target = createBucket(chartStart, period.chartBucketMs);
      chart.set(chartStart, target);
    }
    mergeBucketInto(target, bucket);
  }
  const chartBuckets = [...chart.values()].sort((a, b) => a.start_ms - b.start_ms);
  const observedSinceMs = Math.max(0, Number(state?.observedSinceMs || state?.createdAt || now));
  const coveredMs = Math.max(0, now - observedSinceMs);
  const lastGapAtMs = Math.max(0, Number(state?.lastGapAtMs || 0));
  const recentGap = lastGapAtMs > 0 && now - lastGapAtMs < period.durationMs;
  return {
    period: periodKey,
    requested_duration_ms: period.durationMs,
    chart_bucket_ms: period.chartBucketMs,
    source_bucket: period.source,
    coverage_start_ms: observedSinceMs,
    coverage_end_ms: now,
    covered_ms: Math.min(period.durationMs, coveredMs),
    coverage_complete: coveredMs >= period.durationMs && !recentGap,
    last_gap_at_ms: lastGapAtMs || null,
    recent_gap: recentGap,
    total_notional: summary.total_notional,
    long_notional: summary.long_notional,
    short_notional: summary.short_notional,
    event_count: summary.event_count,
    long_count: summary.long_count,
    short_count: summary.short_count,
    largest_event: summary.largest_event,
    buckets: chartBuckets,
  };
}

function enforceDynamicFeedLimit(provider) {
  if (GLOBAL_FEED_PROVIDERS.has(provider)) return;
  const dynamic = [...FEEDS.values()]
    .filter((feed) => feed.provider === provider && !feed.persistent && !feed.core)
    .sort((a, b) => Number(b.lastAccessAt || 0) - Number(a.lastAccessAt || 0));
  for (const feed of dynamic.slice(DYNAMIC_LIMIT_PER_PROVIDER)) {
    closeFeed(feed);
    FEEDS.delete(feed.key);
  }
}

function notifyReady(feed) {
  for (const waiter of [...feed.waiters]) {
    feed.waiters.delete(waiter);
    clearTimeout(waiter.timer);
    waiter.resolve();
  }
}

function rejectReady(feed, error) {
  for (const waiter of [...feed.waiters]) {
    feed.waiters.delete(waiter);
    clearTimeout(waiter.timer);
    waiter.reject(error);
  }
}

function trimEvents(rows) {
  const cutoff = Date.now() - RECENT_EVENT_RETENTION_MS;
  const deduped = [];
  const seen = new Set();
  for (const row of rows) {
    if (!row || integerValue(row.time_ms) < cutoff) continue;
    const id = String(row.id || '');
    if (!id || seen.has(id)) continue;
    seen.add(id);
    deduped.push(row);
    if (deduped.length >= MAX_EVENTS_PER_SYMBOL) break;
  }
  return deduped;
}

function addEvent(feed, event) {
  const symbol = compactSymbol(event?.symbol);
  const timeMs = integerValue(event?.time_ms);
  const price = positiveNumber(event?.price);
  const notional = positiveNumber(event?.notional);
  const liquidationSide = String(event?.liquidation_side || '').toLowerCase();
  if (!symbol || timeMs <= 0 || price == null || notional == null || !['long', 'short'].includes(liquidationSide)) return;
  const id = String(event.id || `${feed.provider}:${symbol}:${liquidationSide}:${timeMs}:${price}:${notional}`);
  const row = {
    id,
    provider: feed.provider,
    symbol,
    native_symbol: String(event.native_symbol || providerSymbol(feed.provider, symbol)),
    time_ms: timeMs,
    price,
    quantity: positiveNumber(event.quantity),
    quantity_contracts: positiveNumber(event.quantity_contracts),
    notional,
    liquidation_side: liquidationSide,
    order_side: String(event.order_side || '').toLowerCase(),
    price_type: String(event.price_type || ''),
  };
  for (const key of ['quantity', 'quantity_contracts']) {
    if (row[key] == null) delete row[key];
  }
  const observedSinceMs = feed.openedAt || SERVICE_STARTED_AT_MS;
  const inserted = updateStats(row, observedSinceMs);
  if (!inserted) return;
  const rows = [row, ...feedEvents(feed, symbol)];
  rows.sort((a, b) => integerValue(b.time_ms) - integerValue(a.time_ms));
  feed.eventsBySymbol.set(symbol, trimEvents(rows));
  feed.lastMessageAt = Date.now();
}

function websocketUrl(feed) {
  const native = feed.requestedNativeSymbol;
  if (feed.provider === 'binance') {
    return 'wss://fstream.binance.com/market/ws/!forceOrder@arr';
  }
  if (feed.provider === 'okx') return 'wss://ws.okx.com:8443/ws/v5/public';
  if (feed.provider === 'bybit') return 'wss://stream.bybit.com/v5/public/linear';
  if (feed.provider === 'bitget') return 'wss://ws.bitget.com/v3/ws/public';
  if (feed.provider === 'gate') return 'wss://fx-ws.gateio.ws/v4/ws/usdt';
  throw new Error('unsupported_provider');
}

function subscribeFeed(feed) {
  const socket = feed.socket;
  if (feed.provider === 'binance') return true;
  if (feed.provider === 'okx') {
    return sendWs(socket, { id: 'kaka640', op: 'subscribe', args: [{ channel: 'liquidation-orders', instType: 'SWAP' }] });
  }
  if (feed.provider === 'bybit') {
    return sendWs(socket, { op: 'subscribe', args: [`allLiquidation.${feed.requestedNativeSymbol}`] });
  }
  if (feed.provider === 'bitget') {
    return sendWs(socket, { op: 'subscribe', args: [{ instType: 'usdt-futures', topic: 'liquidation' }] });
  }
  if (feed.provider === 'gate') {
    return sendWs(socket, {
      time: Math.floor(Date.now() / 1000),
      channel: 'futures.public_liquidates',
      event: 'subscribe',
      payload: [feed.requestedNativeSymbol],
    });
  }
  return false;
}

function startHeartbeat(feed) {
  if (feed.heartbeatTimer) clearInterval(feed.heartbeatTimer);
  if (feed.provider === 'binance') return;
  feed.heartbeatTimer = setInterval(() => {
    if (!wsReady(feed.socket)) return;
    if (feed.provider === 'bybit') sendWs(feed.socket, { op: 'ping' });
    else if (feed.provider === 'gate') {
      sendWs(feed.socket, { time: Math.floor(Date.now() / 1000), channel: 'futures.ping' });
    } else {
      sendWs(feed.socket, 'ping');
    }
  }, 20_000);
  feed.heartbeatTimer.unref?.();
}

function stopHeartbeat(feed) {
  if (feed.heartbeatTimer) clearInterval(feed.heartbeatTimer);
  feed.heartbeatTimer = null;
}

function scheduleReconnect(feed) {
  if (feed.manuallyClosing || feed.reconnectTimer || !feedHasActiveSymbols(feed)) return;
  const delay = Math.min(15_000, 800 * (2 ** Math.min(feed.reconnectAttempt, 5)));
  feed.reconnectAttempt += 1;
  feed.reconnectTimer = setTimeout(() => {
    feed.reconnectTimer = null;
    ensureFeed(feed).catch(() => {});
  }, delay);
  feed.reconnectTimer.unref?.();
}

function closeFeed(feed) {
  markFeedGap(feed);
  feed.manuallyClosing = true;
  if (feed.reconnectTimer) clearTimeout(feed.reconnectTimer);
  feed.reconnectTimer = null;
  stopHeartbeat(feed);
  closeWsQuietly(feed.socket);
  feed.socket = null;
  feed.connecting = null;
  feed.ready = false;
  rejectReady(feed, new Error('feed_closed'));
}

async function handleBinance(feed, data) {
  const payload = data?.data ?? data;
  if (Array.isArray(payload)) {
    for (const item of payload) await handleBinance(feed, item);
    return;
  }
  const event = payload;
  if (String(event?.e || '') !== 'forceOrder' || !event?.o) return;
  if (integerValue(event?.st) === 2) return;
  const order = event.o;
  const symbol = compactSymbol(order?.s);
  const side = String(order?.S || '').toUpperCase();
  const price = positiveNumber(order?.ap) ?? positiveNumber(order?.L) ?? positiveNumber(order?.p);
  const quantity = positiveNumber(order?.z) ?? positiveNumber(order?.l) ?? positiveNumber(order?.q);
  const timeMs = integerValue(order?.T) || integerValue(event?.E) || Date.now();
  if (!symbol || !['BUY', 'SELL'].includes(side) || price == null || quantity == null) return;
  addEvent(feed, {
    id: `binance:${symbol}:${String(order?.i || '')}:${timeMs}:${side}`,
    symbol,
    native_symbol: String(order?.s || symbol),
    time_ms: timeMs,
    price,
    quantity,
    notional: price * quantity,
    liquidation_side: side === 'SELL' ? 'long' : 'short',
    order_side: side.toLowerCase(),
    price_type: positiveNumber(order?.ap) != null ? 'average_execution' : 'order_or_last_fill',
  });
}

async function handleOkx(feed, data) {
  if (String(data?.arg?.channel || '') !== 'liquidation-orders' || !Array.isArray(data?.data)) return;
  for (const group of data.data) {
    const native = String(group?.instId || '');
    const symbol = compactSymbol(native);
    if (!symbol || !Array.isArray(group?.details)) continue;
    const multiplier = await okxContractMultiplier(native);
    for (const detail of group.details) {
      const price = positiveNumber(detail?.bkPx);
      const contracts = positiveNumber(detail?.sz);
      const timeMs = integerValue(detail?.ts) || Date.now();
      const posSide = String(detail?.posSide || '').toLowerCase();
      const orderSide = String(detail?.side || '').toLowerCase();
      const liquidationSide = ['long', 'short'].includes(posSide)
        ? posSide
        : orderSide === 'sell'
          ? 'long'
          : orderSide === 'buy'
            ? 'short'
            : '';
      const quantity = contracts == null ? null : contracts * multiplier;
      if (price == null || quantity == null || quantity <= 0 || !liquidationSide) continue;
      addEvent(feed, {
        id: `okx:${native}:${timeMs}:${liquidationSide}:${contracts}`,
        symbol,
        native_symbol: native,
        time_ms: timeMs,
        price,
        quantity,
        quantity_contracts: contracts,
        notional: price * quantity,
        liquidation_side: liquidationSide,
        order_side: orderSide,
        price_type: 'bankruptcy',
      });
    }
  }
}

async function handleBybit(feed, data) {
  if (!String(data?.topic || '').startsWith('allLiquidation.') || !Array.isArray(data?.data)) return;
  for (const row of data.data) {
    const symbol = compactSymbol(row?.s);
    const side = String(row?.S || '').toLowerCase();
    const price = positiveNumber(row?.p);
    const quantity = positiveNumber(row?.v);
    const timeMs = integerValue(row?.T) || integerValue(data?.ts) || Date.now();
    if (!symbol || !['buy', 'sell'].includes(side) || price == null || quantity == null) continue;
    addEvent(feed, {
      id: `bybit:${symbol}:${timeMs}:${side}:${quantity}`,
      symbol,
      native_symbol: String(row?.s || symbol),
      time_ms: timeMs,
      price,
      quantity,
      notional: price * quantity,
      liquidation_side: side === 'buy' ? 'long' : 'short',
      order_side: side,
      price_type: 'bankruptcy',
    });
  }
}

async function handleBitget(feed, data) {
  if (String(data?.arg?.topic || '') !== 'liquidation' || !Array.isArray(data?.data)) return;
  for (const row of data.data) {
    const symbol = compactSymbol(row?.symbol);
    if (!symbol) continue;
    const side = String(row?.side || '').toLowerCase();
    const price = positiveNumber(row?.price);
    const notional = positiveNumber(row?.amount);
    const timeMs = integerValue(row?.ts) || integerValue(data?.ts) || Date.now();
    const quantity = price != null && notional != null ? notional / price : null;
    if (!symbol || !['buy', 'sell'].includes(side) || price == null || notional == null || quantity == null || quantity <= 0) continue;
    addEvent(feed, {
      id: `bitget:${symbol}:${timeMs}:${side}:${notional}`,
      symbol,
      native_symbol: String(row?.symbol || symbol),
      time_ms: timeMs,
      price,
      quantity,
      notional,
      liquidation_side: side === 'buy' ? 'long' : 'short',
      order_side: side,
      price_type: 'liquidation',
    });
  }
}

async function handleGate(feed, data) {
  if (String(data?.channel || '') !== 'futures.public_liquidates' || String(data?.event || '') !== 'update' || !Array.isArray(data?.result)) return;
  for (const row of data.result) {
    const native = String(row?.contract || '');
    const symbol = compactSymbol(native);
    const signedContracts = numberValue(row?.size);
    const contracts = signedContracts == null ? null : Math.abs(signedContracts);
    const price = positiveNumber(row?.price);
    const timeMs = integerValue(row?.time_ms) || integerValue(row?.time) * 1000 || integerValue(data?.time_ms) || Date.now();
    if (!symbol || signedContracts == null || signedContracts === 0 || contracts == null || price == null) continue;
    const multiplier = await gateContractMultiplier(native);
    const quantity = contracts * multiplier;
    if (!Number.isFinite(quantity) || quantity <= 0) continue;
    addEvent(feed, {
      id: `gate:${native}:${timeMs}:${signedContracts}`,
      symbol,
      native_symbol: native,
      time_ms: timeMs,
      price,
      quantity,
      quantity_contracts: contracts,
      notional: price * quantity,
      liquidation_side: signedContracts < 0 ? 'long' : 'short',
      order_side: signedContracts < 0 ? 'sell' : 'buy',
      price_type: 'liquidation_order',
    });
  }
}

async function handlePayload(feed, raw) {
  let data;
  try {
    const text = await wsMessageText(raw);
    if (text === 'pong' || text === 'ping') return;
    data = JSON.parse(text);
  } catch (_) {
    return;
  }
  if (data?.event === 'error' || data?.code && String(data.code) !== '0') {
    feed.lastError = String(data?.msg || data?.ret_msg || data?.code || 'subscription_error');
    feed.ready = false;
    closeWsQuietly(feed.socket);
    return;
  }
  try {
    if (feed.provider === 'binance') await handleBinance(feed, data);
    else if (feed.provider === 'okx') await handleOkx(feed, data);
    else if (feed.provider === 'bybit') await handleBybit(feed, data);
    else if (feed.provider === 'bitget') await handleBitget(feed, data);
    else if (feed.provider === 'gate') await handleGate(feed, data);
  } catch (error) {
    feed.lastError = String(error?.message || error);
  }
  feed.lastMessageAt = Date.now();
}

function pruneBinanceLiquidationConnectAttempts(now = Date.now()) {
  while (binanceLiquidationConnectAttempts.length && now - binanceLiquidationConnectAttempts[0] >= BINANCE_LIQUIDATION_CONNECT_WINDOW_MS) {
    binanceLiquidationConnectAttempts.shift();
  }
}

async function acquireBinanceLiquidationConnectSlot() {
  let release;
  const previous = binanceLiquidationConnectChain;
  binanceLiquidationConnectChain = new Promise((resolve) => { release = resolve; });
  await previous;
  try {
    const now = Date.now();
    pruneBinanceLiquidationConnectAttempts(now);
    const gapWait = Math.max(0, BINANCE_LIQUIDATION_CONNECT_GAP_MS - (now - binanceLiquidationLastConnectAt));
    const windowWait = binanceLiquidationConnectAttempts.length >= BINANCE_LIQUIDATION_MAX_CONNECT_ATTEMPTS_5M
      ? Math.max(0, binanceLiquidationConnectAttempts[0] + BINANCE_LIQUIDATION_CONNECT_WINDOW_MS - now)
      : 0;
    const waitMs = Math.max(gapWait, windowWait);
    if (waitMs > 0) {
      binanceLiquidationWsStats.waits += 1;
      if (windowWait > 0) binanceLiquidationWsStats.window_blocks += 1;
      await new Promise((resolve) => {
        const timer = setTimeout(resolve, waitMs);
        timer.unref?.();
      });
    }
    binanceLiquidationLastConnectAt = Date.now();
    binanceLiquidationConnectAttempts.push(binanceLiquidationLastConnectAt);
    binanceLiquidationWsStats.attempts += 1;
  } finally {
    release();
  }
}

export function getBinanceLiquidationWsHealth() {
  pruneBinanceLiquidationConnectAttempts();
  return {
    connect_gap_ms: BINANCE_LIQUIDATION_CONNECT_GAP_MS,
    max_connect_attempts_5m: BINANCE_LIQUIDATION_MAX_CONNECT_ATTEMPTS_5M,
    connect_attempts_in_window: binanceLiquidationConnectAttempts.length,
    connect_attempts_total: binanceLiquidationWsStats.attempts,
    connect_waits: binanceLiquidationWsStats.waits,
    connect_window_blocks: binanceLiquidationWsStats.window_blocks,
    production_ws_only: true,
  };
}

async function openFeed(feed) {
  if (feed.provider === 'binance') await acquireBinanceLiquidationConnectSlot();
  const WebSocketCtor = await resolveWebSocketCtor();
  return await new Promise((resolve, reject) => {
    const socket = new WebSocketCtor(websocketUrl(feed));
    let settled = false;
    const timeout = setTimeout(() => {
      if (settled) return;
      settled = true;
      closeWsQuietly(socket);
      reject(new Error('liquidation_websocket_open_timeout'));
    }, READY_TIMEOUT_MS);
    timeout.unref?.();
    wsListen(socket, 'message', (payload) => {
      handlePayload(feed, payload).catch(() => {});
    });
    wsListen(socket, 'open', () => {
      if (settled) return;
      feed.socket = socket;
      const subscribed = subscribeFeed(feed);
      if (!subscribed) {
        settled = true;
        clearTimeout(timeout);
        closeWsQuietly(socket);
        reject(new Error('liquidation_websocket_subscribe_failed'));
        return;
      }
      settled = true;
      clearTimeout(timeout);
      feed.ready = true;
      feed.openedAt = Date.now();
      feed.lastError = '';
      feed.reconnectAttempt = 0;
      startHeartbeat(feed);
      notifyReady(feed);
      resolve();
    });
    wsListen(socket, 'close', () => {
      if (feed.socket === socket) feed.socket = null;
      feed.ready = false;
      markFeedGap(feed);
      stopHeartbeat(feed);
      if (!feed.manuallyClosing) scheduleReconnect(feed);
    });
    wsListen(socket, 'error', () => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      closeWsQuietly(socket);
      reject(new Error('liquidation_websocket_open_failed'));
    });
  });
}

async function ensureFeed(feed) {
  if (wsReady(feed.socket) && feed.ready) return;
  if (feed.connecting) return feed.connecting;
  feed.manuallyClosing = false;
  feed.connecting = openFeed(feed)
    .catch((error) => {
      feed.ready = false;
      feed.lastError = String(error?.message || error);
      closeWsQuietly(feed.socket);
      feed.socket = null;
      rejectReady(feed, error);
      scheduleReconnect(feed);
      throw error;
    })
    .finally(() => {
      feed.connecting = null;
    });
  return feed.connecting;
}

async function waitForReady(feed) {
  if (wsReady(feed.socket) && feed.ready) return;
  const promise = new Promise((resolve, reject) => {
    const waiter = {
      resolve,
      reject,
      timer: setTimeout(() => {
        feed.waiters.delete(waiter);
        reject(new Error('liquidation_feed_ready_timeout'));
      }, READY_TIMEOUT_MS),
    };
    waiter.timer.unref?.();
    feed.waiters.add(waiter);
  });
  ensureFeed(feed).catch(() => {});
  return promise;
}

const cleanupTimer = setInterval(() => {
  const now = Date.now();
  for (const [key, feed] of [...FEEDS.entries()]) {
    for (const [symbol, rows] of [...feed.eventsBySymbol.entries()]) {
      const trimmed = trimEvents(rows);
      if (trimmed.length) feed.eventsBySymbol.set(symbol, trimmed);
      else feed.eventsBySymbol.delete(symbol);
    }
    if (!feedHasActiveSymbols(feed)) {
      closeFeed(feed);
      FEEDS.delete(key);
      continue;
    }
    if (!wsReady(feed.socket) && !feed.connecting && !feed.reconnectTimer) {
      scheduleReconnect(feed);
    }
    if (feed.openedAt > 0 && now - feed.openedAt > 23 * 60 * 60_000) {
      closeWsQuietly(feed.socket);
    }
  }
  for (const [key, state] of [...STATS.entries()]) {
    trimStatsState(state);
    const lastRelevant = Math.max(Number(state.lastEventAt || 0), Number(state.lastAccessAt || 0));
    if (lastRelevant > 0 && now - lastRelevant > STATS_RETENTION_MS) STATS.delete(key);
  }
}, 15_000);
cleanupTimer.unref?.();

function clampLimit(value) {
  const parsed = integerValue(value);
  return Math.max(1, Math.min(parsed || 80, 120));
}



function markCoreFeed(feed) {
  feed.core = true;
  feed.persistent = true;
  feed.lastAccessAt = Date.now();
  return feed;
}

async function bootstrapCollection() {
  for (const provider of ['binance', 'okx', 'bitget']) {
    const feed = markCoreFeed(getFeed(provider, 'BTCUSDT'));
    touchFeed(feed, 'BTCUSDT');
    ensureFeed(feed).catch(() => {});
  }
  for (const provider of ['bybit', 'gate']) {
    for (const symbol of CORE_SYMBOLS) {
      const feed = markCoreFeed(getFeed(provider, symbol));
      touchFeed(feed, symbol);
      getStats(provider, symbol, { observedSinceMs: Date.now() });
      ensureFeed(feed).catch(() => {});
    }
  }
}

const bootstrapTimer = setTimeout(() => {
  bootstrapCollection().catch(() => {});
}, 900);
bootstrapTimer.unref?.();

export async function handleContractLiquidation(req, res, url) {
  if (url.pathname !== '/api/contract-liquidation') return false;
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET, OPTIONS',
      'access-control-allow-headers': 'content-type',
      'cache-control': 'no-store',
    });
    res.end();
    return true;
  }
  if (req.method !== 'GET') {
    sendJson(res, 405, { ok: false, version: STEP_VERSION, error: 'method_not_allowed' });
    return true;
  }

  const provider = normalizeProvider(url.searchParams.get('provider'));
  const symbol = compactSymbol(url.searchParams.get('symbol'));
  const limit = clampLimit(url.searchParams.get('limit'));
  const sinceMs = Math.max(0, integerValue(url.searchParams.get('since_ms')));
  const requestedPeriod = String(url.searchParams.get('period') || '24h').trim().toLowerCase();
  const period = Object.hasOwn(PERIODS, requestedPeriod) ? requestedPeriod : '24h';
  if (!SUPPORTED_PROVIDERS.has(provider)) {
    sendJson(res, 400, { ok: false, version: STEP_VERSION, error: 'unsupported_provider', provider });
    return true;
  }
  if (!symbol) {
    sendJson(res, 400, { ok: false, version: STEP_VERSION, error: 'invalid_symbol' });
    return true;
  }

  const feed = getFeed(provider, symbol);
  touchFeed(feed, symbol);
  enforceDynamicFeedLimit(provider);
  const state = getStats(provider, symbol, {
    observedSinceMs: feed.openedAt || (GLOBAL_FEED_PROVIDERS.has(provider) ? SERVICE_STARTED_AT_MS : Date.now()),
  });
  try {
    await waitForReady(feed);
    const currentState = getStats(provider, symbol, {
      observedSinceMs: feed.openedAt || (GLOBAL_FEED_PROVIDERS.has(provider) ? SERVICE_STARTED_AT_MS : Date.now()),
    }) || state;
    const recentRows = currentState?.recentEvents || feedEvents(feed, symbol);
    const items = recentRows
      .filter((row) => sinceMs <= 0 || integerValue(row.time_ms) >= sinceMs)
      .slice(0, limit)
      .map((row) => ({ ...row }));
    const statistics = buildStatistics(currentState, period);
    sendJson(res, 200, {
      ok: true,
      version: STEP_VERSION,
      provider,
      market_type: 'contract',
      symbol,
      native_symbol: providerSymbol(provider, symbol),
      connected: wsReady(feed.socket) && feed.ready,
      source: feed.source,
      transport: feed.transport,
      upstream_host: feed.upstream_host,
      coverage: feed.coverage,
      aggregation_scope: 'single_provider_single_symbol',
      retention: {
        minute_buckets_minutes: 60,
        quarter_hour_buckets_hours: 24,
        hourly_buckets_days: 14,
        raw_events_persisted: false,
        process_memory_only: true,
      },
      available_periods: Object.keys(PERIODS),
      service_started_at_ms: SERVICE_STARTED_AT_MS,
      session_started_at_ms: feed.openedAt || Date.now(),
      timestamp_ms: items[0]?.time_ms || feed.lastMessageAt || Date.now(),
      last_event_at_ms: items[0]?.time_ms || currentState?.lastEventAt || null,
      statistics,
      items,
    });
  } catch (error) {
    sendJson(res, 502, {
      ok: false,
      version: STEP_VERSION,
      provider,
      market_type: 'contract',
      symbol,
      error: String(error?.message || error),
      reason: feed.lastError || 'upstream_unavailable',
    });
  }
  return true;
}
