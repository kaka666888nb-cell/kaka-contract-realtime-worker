import WebSocket from 'ws';

const VERSION = '650.8.15.167';
const WS_URL = 'wss://ws-api.binance.com:443/ws-api/v3?returnRateLimits=false';
const CONNECT_TIMEOUT_MS = 12_000;
const REQUEST_TIMEOUT_MS = 12_000;
const IDLE_RECONNECT_MS = 20 * 60_000;
const MAX_PENDING = 64;
const LOCAL_WEIGHT_LIMIT_PER_MINUTE = 600;
const CACHE_MAX = 512;

let socket = null;
let connectPromise = null;
let requestSeq = 0;
let lastMessageAt = 0;
let lastOpenAt = 0;
let lastError = '';
let reconnects = 0;
const pending = new Map();
const inflight = new Map();
const cache = new Map();
const weightEvents = [];

const stats = {
  connects_started: 0,
  connects_succeeded: 0,
  connects_failed: 0,
  socket_closes: 0,
  requests_started: 0,
  requests_succeeded: 0,
  requests_failed: 0,
  request_timeouts: 0,
  pending_rejections: 0,
  weight_rejections: 0,
  cache_hits: 0,
  inflight_hits: 0,
  kline_requests: 0,
  depth_requests: 0,
  aggregate_trade_requests: 0,
};

function nowIso(value = Date.now()) {
  return new Date(value).toISOString();
}

function pruneCache() {
  const now = Date.now();
  for (const [key, entry] of cache.entries()) {
    if (Number(entry?.expiresAt || 0) <= now) cache.delete(key);
  }
  while (cache.size > CACHE_MAX) {
    const oldest = cache.keys().next().value;
    if (oldest == null) break;
    cache.delete(oldest);
  }
}

function pruneWeights() {
  const cutoff = Date.now() - 60_000;
  while (weightEvents.length && weightEvents[0].time < cutoff) weightEvents.shift();
}

function weightUsed() {
  pruneWeights();
  return weightEvents.reduce((sum, item) => sum + item.weight, 0);
}

function reserveWeight(weight) {
  const safeWeight = Math.max(1, Number(weight) || 1);
  const used = weightUsed();
  if (used + safeWeight > LOCAL_WEIGHT_LIMIT_PER_MINUTE) {
    stats.weight_rejections += 1;
    throw new Error(`binance_spot_ws_api_local_weight_guard:${used}+${safeWeight}>${LOCAL_WEIGHT_LIMIT_PER_MINUTE}`);
  }
  weightEvents.push({ time: Date.now(), weight: safeWeight });
}

function rejectAllPending(error) {
  for (const [id, item] of pending.entries()) {
    clearTimeout(item.timer);
    pending.delete(id);
    item.reject(error);
  }
}

function clearSocket(reason = '') {
  const current = socket;
  socket = null;
  connectPromise = null;
  if (reason) lastError = String(reason).slice(0, 400);
  try { current?.terminate(); } catch (_) {}
}

async function ensureSocket() {
  if (socket?.readyState === WebSocket.OPEN) {
    if (lastMessageAt > 0 && Date.now() - lastMessageAt > IDLE_RECONNECT_MS) {
      clearSocket('binance_spot_ws_api_idle_reconnect');
      reconnects += 1;
    } else {
      return socket;
    }
  }
  if (connectPromise) return await connectPromise;

  stats.connects_started += 1;
  connectPromise = new Promise((resolve, reject) => {
    let settled = false;
    let candidate;
    const finishReject = (error) => {
      if (settled) return;
      settled = true;
      stats.connects_failed += 1;
      lastError = String(error?.message || error || 'binance_spot_ws_api_connect_failed').slice(0, 400);
      try { candidate?.terminate(); } catch (_) {}
      if (socket === candidate) socket = null;
      connectPromise = null;
      reject(error instanceof Error ? error : new Error(lastError));
    };
    const timer = setTimeout(() => finishReject(new Error('binance_spot_ws_api_connect_timeout')), CONNECT_TIMEOUT_MS);
    timer.unref?.();
    try {
      candidate = new WebSocket(WS_URL, {
        handshakeTimeout: CONNECT_TIMEOUT_MS,
        perMessageDeflate: false,
        headers: { 'user-agent': 'KakaWeb3/650.8.15.167-binance-spot-shared-ws-api' },
      });
    } catch (error) {
      clearTimeout(timer);
      finishReject(error);
      return;
    }
    socket = candidate;
    candidate.on('open', () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      stats.connects_succeeded += 1;
      lastOpenAt = Date.now();
      lastMessageAt = Date.now();
      lastError = '';
      connectPromise = null;
      resolve(candidate);
    });
    candidate.on('message', (raw) => {
      lastMessageAt = Date.now();
      let decoded;
      try { decoded = JSON.parse(Buffer.isBuffer(raw) ? raw.toString('utf8') : String(raw)); }
      catch (_) { return; }
      const id = String(decoded?.id ?? '');
      if (!id) return;
      const item = pending.get(id);
      if (!item) return;
      pending.delete(id);
      clearTimeout(item.timer);
      const status = Number(decoded?.status || 0);
      if (status !== 200) {
        const code = decoded?.error?.code ?? decoded?.code ?? '';
        const msg = decoded?.error?.msg ?? decoded?.msg ?? `status_${status || 'unknown'}`;
        item.reject(new Error(`binance_spot_ws_api_${item.method}_${status}:${code}:${msg}`));
        return;
      }
      item.resolve(decoded?.result);
    });
    candidate.on('error', (error) => {
      if (!settled) {
        clearTimeout(timer);
        finishReject(error);
      } else {
        lastError = String(error?.message || error).slice(0, 400);
      }
    });
    candidate.on('close', () => {
      stats.socket_closes += 1;
      const wasCurrent = socket === candidate;
      if (wasCurrent) {
        socket = null;
        connectPromise = null;
        rejectAllPending(new Error('binance_spot_ws_api_socket_closed'));
      }
      if (!settled) {
        clearTimeout(timer);
        finishReject(new Error('binance_spot_ws_api_closed_before_open'));
      }
    });
  });
  return await connectPromise;
}

async function requestRaw(method, params, { weight = 1 } = {}) {
  if (pending.size >= MAX_PENDING) {
    stats.pending_rejections += 1;
    throw new Error('binance_spot_ws_api_pending_full');
  }
  reserveWeight(weight);
  const ws = await ensureSocket();
  if (ws.readyState !== WebSocket.OPEN) throw new Error('binance_spot_ws_api_not_open');
  const id = `kaka-spot-${Date.now()}-${++requestSeq}`;
  stats.requests_started += 1;
  return await new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      pending.delete(id);
      stats.request_timeouts += 1;
      stats.requests_failed += 1;
      reject(new Error(`binance_spot_ws_api_${method}_timeout`));
    }, REQUEST_TIMEOUT_MS);
    timer.unref?.();
    pending.set(id, {
      method,
      timer,
      resolve: (value) => {
        stats.requests_succeeded += 1;
        resolve(value);
      },
      reject: (error) => {
        stats.requests_failed += 1;
        reject(error);
      },
    });
    try {
      ws.send(JSON.stringify({ id, method, params }));
    } catch (error) {
      const item = pending.get(id);
      if (item) {
        pending.delete(id);
        clearTimeout(item.timer);
      }
      stats.requests_failed += 1;
      reject(error);
    }
  });
}

async function cachedRequest(key, ttlMs, loader) {
  pruneCache();
  const now = Date.now();
  const existing = cache.get(key);
  if (existing && existing.expiresAt > now) {
    stats.cache_hits += 1;
    return existing.value;
  }
  const running = inflight.get(key);
  if (running) {
    stats.inflight_hits += 1;
    return await running;
  }
  const task = Promise.resolve().then(loader);
  inflight.set(key, task);
  try {
    const value = await task;
    cache.set(key, { value, expiresAt: Date.now() + Math.max(250, Number(ttlMs) || 0) });
    pruneCache();
    return value;
  } finally {
    if (inflight.get(key) === task) inflight.delete(key);
  }
}

function cleanSymbol(value) {
  return String(value || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}

export async function fetchBinanceSpotWsApiKlines({ symbol, interval, endTime, limit = 500 } = {}) {
  const safeSymbol = cleanSymbol(symbol);
  const safeInterval = String(interval || '15m').trim();
  const safeEnd = Math.max(1, Math.trunc(Number(endTime) || Date.now()));
  const total = Math.max(1, Math.min(5000, Math.trunc(Number(limit) || 500)));
  if (!safeSymbol) throw new Error('binance_spot_ws_api_symbol_required');
  stats.kline_requests += 1;
  const rows = [];
  let cursorEnd = safeEnd;
  for (let page = 0; page < 5 && rows.length < total; page += 1) {
    const pageLimit = Math.min(1000, total - rows.length);
    const bucket = Math.floor(cursorEnd / 1000);
    const key = `klines:${safeSymbol}:${safeInterval}:${pageLimit}:${bucket}`;
    const result = await cachedRequest(key, 45_000, () => requestRaw('klines', {
      symbol: safeSymbol,
      interval: safeInterval,
      endTime: cursorEnd,
      limit: pageLimit,
    }, { weight: 2 }));
    const pageRows = Array.isArray(result) ? result : [];
    if (!pageRows.length) break;
    rows.unshift(...pageRows);
    const oldestOpen = Number(pageRows[0]?.[0]);
    if (!Number.isFinite(oldestOpen) || oldestOpen <= 0 || pageRows.length < pageLimit) break;
    cursorEnd = oldestOpen - 1;
  }
  return rows.slice(-total);
}

export async function fetchBinanceSpotWsApiDepth({ symbol, limit = 20 } = {}) {
  const safeSymbol = cleanSymbol(symbol);
  const safeLimit = Math.max(5, Math.min(100, Math.trunc(Number(limit) || 20)));
  if (!safeSymbol) throw new Error('binance_spot_ws_api_symbol_required');
  stats.depth_requests += 1;
  return await cachedRequest(`depth:${safeSymbol}:${safeLimit}`, 1_200, () => requestRaw('depth', {
    symbol: safeSymbol,
    limit: safeLimit,
    symbolStatus: 'TRADING',
  }, { weight: 5 }));
}

export async function fetchBinanceSpotWsApiAggregateTrades({ symbol, limit = 100 } = {}) {
  const safeSymbol = cleanSymbol(symbol);
  const safeLimit = Math.max(1, Math.min(1000, Math.trunc(Number(limit) || 100)));
  if (!safeSymbol) throw new Error('binance_spot_ws_api_symbol_required');
  stats.aggregate_trade_requests += 1;
  return await cachedRequest(`agg:${safeSymbol}:${safeLimit}`, 1_200, () => requestRaw('trades.aggregate', {
    symbol: safeSymbol,
    limit: safeLimit,
  }, { weight: 4 }));
}

export function getBinanceSpotWsApiHealth() {
  return {
    ok: true,
    version: VERSION,
    mode: 'one_shared_spot_websocket_api_connection_exact_key_cache_inflight',
    websocket_url: 'wss://ws-api.binance.com:443/ws-api/v3',
    connected: socket?.readyState === WebSocket.OPEN,
    ready_state: socket?.readyState ?? null,
    pending: pending.size,
    max_pending: MAX_PENDING,
    inflight: inflight.size,
    cache_entries: cache.size,
    cache_max: CACHE_MAX,
    local_weight_last_60s: weightUsed(),
    local_weight_limit_per_minute: LOCAL_WEIGHT_LIMIT_PER_MINUTE,
    official_weight_limit_per_minute: 6000,
    user_reads_open_new_connections: false,
    same_exact_key_reads_share_cache_and_inflight: true,
    binance_contract_rest_touched: false,
    render_direct_binance_rest_touched: false,
    last_open_at: lastOpenAt ? nowIso(lastOpenAt) : null,
    last_message_at: lastMessageAt ? nowIso(lastMessageAt) : null,
    last_error: lastError,
    reconnects,
    stats: { ...stats },
    time: nowIso(),
  };
}

export const __binanceSpotWsApiStep1032_2Test = Object.freeze({
  cleanSymbol,
});
