import { fetchBinancePublicRestRelayJson } from './binance-contract-kline-relay.mjs';
import { getBinanceContractRealtimeMeta } from './binance-contract-market.mjs';

const ROUTE = '/api/contract-funding';
const VERSION = '650.8.15.14';
const SUPPORTED = new Set(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const CACHE = new Map();
const INFLIGHT = new Map();
const FRESH_MS = 30_000;
const STALE_MS = 10 * 60_000;
const BINANCE_HISTORY_REFRESH_MS = 5 * 60_000;
const BINANCE_HISTORY_BACKGROUND_DELAY_MS = 10_000;
const BINANCE_REALTIME_WAIT_MS = 1_800;
const BINANCE_HISTORY_REFRESH = new Map();

function sendJson(res, status, payload) {
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': String(body.length),
  });
  res.end(body);
}

function providerKey(value) {
  return String(value || '').trim().toLowerCase().replace('gate.io', 'gate');
}

function canonicalSymbol(value) {
  return String(value || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function splitSymbol(symbol) {
  for (const quote of ['USDT', 'USDC', 'USD']) {
    if (symbol.endsWith(quote) && symbol.length > quote.length) {
      return { base: symbol.slice(0, -quote.length), quote };
    }
  }
  return { base: symbol.replace(/USDT$/, ''), quote: 'USDT' };
}

function nativeSymbol(provider, symbol) {
  const { base, quote } = splitSymbol(symbol);
  if (provider === 'okx') return `${base}-${quote}-SWAP`;
  if (provider === 'gate') return `${base}_${quote}`;
  return symbol;
}

function numberOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function msValue(value) {
  if (typeof value === 'string' && value.trim() && !Number.isFinite(Number(value))) {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed) && parsed > 0) return parsed;
  }
  const n = numberOrNull(value);
  if (n == null || n <= 0) return null;
  return n < 1e12 ? Math.round(n * 1000) : Math.round(n);
}

function iso(value) {
  const ms = msValue(value);
  if (ms == null) return null;
  try { return new Date(ms).toISOString(); } catch (_) { return null; }
}

function currentRow({ provider, symbol, rate, nextTime, mark, index, sourceTime, intervalHours }) {
  const decimal = numberOrNull(rate);
  return {
    provider,
    market_type: 'contract',
    symbol,
    last_funding_rate: decimal,
    funding_rate: decimal,
    last_funding_rate_percent: decimal == null ? null : decimal * 100,
    funding_rate_percent: decimal == null ? null : decimal * 100,
    next_funding_time: iso(nextTime),
    mark_price: numberOrNull(mark),
    index_price: numberOrNull(index),
    funding_interval_hours: numberOrNull(intervalHours),
    source_time: iso(sourceTime) || new Date().toISOString(),
    cached_at: new Date().toISOString(),
  };
}

function historyRow({ provider, symbol, rate, time, mark }) {
  const decimal = numberOrNull(rate);
  const fundingTime = iso(time);
  if (decimal == null || fundingTime == null) return null;
  return {
    provider,
    market_type: 'contract',
    symbol,
    funding_time: fundingTime,
    funding_rate: decimal,
    funding_rate_percent: decimal * 100,
    mark_price: numberOrNull(mark),
    cached_at: new Date().toISOString(),
  };
}

async function fetchJson(url, timeoutMs = 8000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        accept: 'application/json',
        'user-agent': 'KakaWeb3/641.1 contract-funding',
      },
    });
    const text = await response.text();
    if (!response.ok) throw new Error(`HTTP_${response.status}:${text.slice(0, 160)}`);
    return JSON.parse(text);
  } finally {
    clearTimeout(timer);
  }
}

async function fetchBinanceJson(url, timeoutMs = 8000, source = 'contract_funding', options = {}) {
  // Step650.8.15.14: preserve Binance funding current/history without using the
  // banned Render egress. The Edge relay has a strict endpoint/parameter allowlist.
  void timeoutMs;
  return await fetchBinancePublicRestRelayJson(url, {
    source,
    lane: options.lane || 'auxiliary',
    priority: Number(options.priority || 0),
  });
}



async function fetchBinancePair(currentUrl, historyUrl) {
  let currentRaw = null;
  let historyRaw = null;
  const warnings = [];
  try { currentRaw = await fetchBinanceJson(currentUrl, 8000, 'funding:current'); }
  catch (error) { warnings.push(`current:${error?.message || error}`); }
  try { historyRaw = await fetchBinanceJson(historyUrl, 8000, 'funding:history'); }
  catch (error) { warnings.push(`history:${error?.message || error}`); }
  if (currentRaw == null && historyRaw == null) throw new Error(warnings.join(';') || 'binance_funding_unavailable');
  return { currentRaw, historyRaw, warnings };
}

async function fetchPair(currentUrl, historyUrl) {
  const [currentResult, historyResult] = await Promise.allSettled([
    fetchJson(currentUrl),
    fetchJson(historyUrl),
  ]);
  if (currentResult.status === 'rejected' && historyResult.status === 'rejected') {
    throw new Error(`current:${currentResult.reason?.message || currentResult.reason};history:${historyResult.reason?.message || historyResult.reason}`);
  }
  return {
    currentRaw: currentResult.status === 'fulfilled' ? currentResult.value : null,
    historyRaw: historyResult.status === 'fulfilled' ? historyResult.value : null,
    warnings: [
      currentResult.status === 'rejected' ? `current:${currentResult.reason?.message || currentResult.reason}` : null,
      historyResult.status === 'rejected' ? `history:${historyResult.reason?.message || historyResult.reason}` : null,
    ].filter(Boolean),
  };
}

function binanceRealtimeCurrent(symbol) {
  const raw = getBinanceContractRealtimeMeta(symbol);
  if (!raw || typeof raw !== 'object') return null;
  const rate = raw.last_funding_rate ?? raw.funding_rate;
  const current = currentRow({
    provider: 'binance', symbol,
    rate,
    nextTime: raw.next_funding_time,
    mark: raw.mark_price,
    index: raw.index_price,
    sourceTime: raw.source_time ?? raw.cached_at,
  });
  current.last_price = numberOrNull(raw.last_price ?? raw.price);
  current.source = 'binance_official_public_mark_price_websocket';
  current.realtime = true;
  return current;
}

async function waitForBinanceRealtimeCurrent(symbol, waitMs = BINANCE_REALTIME_WAIT_MS) {
  const immediate = binanceRealtimeCurrent(symbol);
  if (immediate) return immediate;
  const deadline = Date.now() + Math.max(0, waitMs);
  while (Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 120));
    const row = binanceRealtimeCurrent(symbol);
    if (row) return row;
  }
  return null;
}

async function fetchBinanceFundingHistory(symbol, limit) {
  const historyRaw = await fetchBinanceJson(
    `https://fapi.binance.com/fapi/v1/fundingRate?symbol=${encodeURIComponent(symbol)}&limit=${limit}`,
    8000,
    'funding:history_background',
    { lane: 'auxiliary', priority: -10 },
  );
  return Array.isArray(historyRaw) ? historyRaw.map((item) => historyRow({
    provider: 'binance', symbol,
    rate: item?.fundingRate,
    time: item?.fundingTime,
    mark: item?.markPrice,
  })).filter(Boolean) : [];
}

function scheduleBinanceFundingHistoryRefresh(key, symbol, limit) {
  const existing = BINANCE_HISTORY_REFRESH.get(key);
  if (existing) return existing;
  const currentCached = CACHE.get(key);
  const age = currentCached ? Date.now() - currentCached.storedAt : Number.POSITIVE_INFINITY;
  if (age <= BINANCE_HISTORY_REFRESH_MS && Array.isArray(currentCached?.payload?.history) && currentCached.payload.history.length) {
    return null;
  }
  const promise = (async () => {
    try {
      // Give Kline and first-paint OI requests a clean priority window.
      await new Promise((resolve) => setTimeout(resolve, BINANCE_HISTORY_BACKGROUND_DELAY_MS));
      const history = await fetchBinanceFundingHistory(symbol, limit);
      const cached = CACHE.get(key);
      const current = binanceRealtimeCurrent(symbol) || cached?.payload?.current || null;
      const payload = {
        ok: true,
        version: VERSION,
        provider: 'binance',
        market_type: 'contract',
        symbol,
        native_symbol: symbol,
        source: 'binance_mark_price_websocket_plus_background_funding_history_edge_relay',
        current,
        history: history.slice(0, limit),
        warnings: [],
        partial: current == null,
        timestamp_ms: Date.now(),
      };
      CACHE.set(key, { storedAt: Date.now(), payload });
      return payload;
    } catch (error) {
      const cached = CACHE.get(key);
      if (cached) {
        cached.payload = {
          ...cached.payload,
          warnings: [...new Set([...(cached.payload.warnings || []), `history:${String(error?.message || error)}`])],
        };
      }
      return null;
    }
  })().finally(() => BINANCE_HISTORY_REFRESH.delete(key));
  BINANCE_HISTORY_REFRESH.set(key, promise);
  return promise;
}

async function serveBinanceFunding(res, symbol, limit, key, { scheduleHistory = true } = {}) {
  const cached = CACHE.get(key);
  const current = await waitForBinanceRealtimeCurrent(symbol);
  const history = Array.isArray(cached?.payload?.history) ? cached.payload.history.slice(0, limit) : [];
  const payload = {
    ok: true,
    version: VERSION,
    provider: 'binance',
    market_type: 'contract',
    symbol,
    native_symbol: symbol,
    source: 'binance_official_public_mark_price_websocket',
    current: current || cached?.payload?.current || null,
    history,
    warnings: current ? [] : ['mark_price_websocket_warming'],
    partial: !current || history.length === 0,
    background_history_refresh: scheduleHistory,
    timestamp_ms: Date.now(),
  };
  CACHE.set(key, { storedAt: cached?.storedAt || Date.now(), payload });
  if (scheduleHistory) scheduleBinanceFundingHistoryRefresh(key, symbol, limit);
  sendJson(res, 200, { ...payload, cache_state: current ? (history.length ? 'realtime-plus-cache' : 'realtime') : 'warming' });
}

async function fetchBinance(symbol, limit) {
  // Retained for compatibility with load(); the request handler uses the fast
  // stale-while-revalidate path above so App first paint never waits for history.
  const current = await waitForBinanceRealtimeCurrent(symbol);
  const history = await fetchBinanceFundingHistory(symbol, limit);
  return {
    current,
    history,
    warnings: current ? [] : ['mark_price_websocket_warming'],
    source: 'binance_mark_price_websocket_plus_funding_history_edge_relay',
  };
}

async function fetchOkx(symbol, limit) {
  const native = nativeSymbol('okx', symbol);
  const { currentRaw, historyRaw, warnings } = await fetchPair(
    `https://www.okx.com/api/v5/public/funding-rate?instId=${encodeURIComponent(native)}`,
    `https://www.okx.com/api/v5/public/funding-rate-history?instId=${encodeURIComponent(native)}&limit=${limit}`,
  );
  const item = Array.isArray(currentRaw?.data) ? currentRaw.data[0] : null;
  const current = currentRow({
    provider: 'okx', symbol,
    rate: item?.fundingRate ?? item?.settFundingRate,
    nextTime: item?.nextFundingTime,
    sourceTime: item?.ts,
  });
  const history = Array.isArray(historyRaw?.data) ? historyRaw.data.map((row) => historyRow({
    provider: 'okx', symbol,
    rate: row?.realizedRate || row?.fundingRate,
    time: row?.fundingTime,
  })).filter(Boolean) : [];
  return { current, history, warnings, source: 'okx_official_public_funding_rest', native_symbol: native };
}

async function fetchBybit(symbol, limit) {
  const { currentRaw, historyRaw, warnings } = await fetchPair(
    `https://api.bybit.com/v5/market/tickers?category=linear&symbol=${encodeURIComponent(symbol)}`,
    `https://api.bybit.com/v5/market/funding/history?category=linear&symbol=${encodeURIComponent(symbol)}&limit=${limit}`,
  );
  const item = Array.isArray(currentRaw?.result?.list) ? currentRaw.result.list[0] : null;
  const current = currentRow({
    provider: 'bybit', symbol,
    rate: item?.fundingRate,
    nextTime: item?.nextFundingTime,
    mark: item?.markPrice,
    index: item?.indexPrice,
    sourceTime: currentRaw?.time,
    intervalHours: item?.fundingIntervalHour,
  });
  const history = Array.isArray(historyRaw?.result?.list) ? historyRaw.result.list.map((row) => historyRow({
    provider: 'bybit', symbol,
    rate: row?.fundingRate,
    time: row?.fundingRateTimestamp,
  })).filter(Boolean) : [];
  return { current, history, warnings, source: 'bybit_official_public_funding_rest' };
}

async function fetchBitget(symbol, limit) {
  const q = `symbol=${encodeURIComponent(symbol)}&productType=usdt-futures`;
  const { currentRaw, historyRaw, warnings } = await fetchPair(
    `https://api.bitget.com/api/v2/mix/market/current-fund-rate?${q}`,
    `https://api.bitget.com/api/v2/mix/market/history-fund-rate?${q}&pageSize=${limit}`,
  );
  const item = Array.isArray(currentRaw?.data) ? currentRaw.data[0] : currentRaw?.data;
  const current = currentRow({
    provider: 'bitget', symbol,
    rate: item?.fundingRate,
    nextTime: item?.nextUpdate ?? item?.nextFundingTime,
    sourceTime: currentRaw?.requestTime,
    intervalHours: item?.fundingRateInterval,
  });
  const list = Array.isArray(historyRaw?.data) ? historyRaw.data : (Array.isArray(historyRaw?.data?.list) ? historyRaw.data.list : []);
  const history = list.map((row) => historyRow({
    provider: 'bitget', symbol,
    rate: row?.fundingRate,
    time: row?.fundingTime ?? row?.fundingRateTimestamp,
  })).filter(Boolean);
  return { current, history, warnings, source: 'bitget_official_public_funding_rest' };
}

async function fetchGate(symbol, limit) {
  const native = nativeSymbol('gate', symbol);
  const { currentRaw: contractRaw, historyRaw, warnings } = await fetchPair(
    `https://api.gateio.ws/api/v4/futures/usdt/contracts/${encodeURIComponent(native)}`,
    `https://api.gateio.ws/api/v4/futures/usdt/funding_rate?contract=${encodeURIComponent(native)}&limit=${limit}`,
  );
  const current = currentRow({
    provider: 'gate', symbol,
    rate: contractRaw?.funding_rate ?? contractRaw?.funding_rate_indicative,
    nextTime: contractRaw?.funding_next_apply,
    mark: contractRaw?.mark_price,
    index: contractRaw?.index_price,
    sourceTime: Date.now(),
    intervalHours: numberOrNull(contractRaw?.funding_interval) == null ? null : Number(contractRaw.funding_interval) / 3600,
  });
  const history = Array.isArray(historyRaw) ? historyRaw.map((row) => historyRow({
    provider: 'gate', symbol,
    rate: row?.funding_rate ?? row?.r ?? row?.rate,
    time: row?.funding_time ?? row?.t ?? row?.time,
    mark: row?.mark_price,
  })).filter(Boolean) : [];
  return { current, history, warnings, source: 'gate_official_public_funding_rest', native_symbol: native };
}

async function load(provider, symbol, limit) {
  switch (provider) {
    case 'binance': return fetchBinance(symbol, limit);
    case 'okx': return fetchOkx(symbol, limit);
    case 'bybit': return fetchBybit(symbol, limit);
    case 'bitget': return fetchBitget(symbol, limit);
    case 'gate': return fetchGate(symbol, limit);
    default: throw new Error('unsupported_provider');
  }
}

export async function handleContractFunding(req, res, url) {
  if (url.pathname === `${ROUTE}/health`) {
    sendJson(res, 200, {
      ok: true,
      version: VERSION,
      cache_entries: CACHE.size,
      inflight_entries: INFLIGHT.size,
      binance_history_refreshes: BINANCE_HISTORY_REFRESH.size,
      binance_current_transport: 'mark_price_websocket',
      binance_history_transport: 'authenticated_edge_relay_background',
      first_paint_waits_for_history: false,
      history_background_delay_ms: BINANCE_HISTORY_BACKGROUND_DELAY_MS,
      time: new Date().toISOString(),
    });
    return true;
  }
  if (url.pathname !== ROUTE) return false;
  if (req.method !== 'GET') {
    sendJson(res, 405, { ok: false, version: VERSION, error: 'method_not_allowed' });
    return true;
  }
  const provider = providerKey(url.searchParams.get('provider'));
  const symbol = canonicalSymbol(url.searchParams.get('symbol'));
  const limit = Math.max(1, Math.min(100, Number(url.searchParams.get('limit') || 24) || 24));
  if (!SUPPORTED.has(provider) || !symbol) {
    sendJson(res, 400, { ok: false, version: VERSION, error: 'invalid_provider_or_symbol' });
    return true;
  }
  const key = `${provider}|${symbol}|${limit}`;
  if (provider === 'binance') {
    await serveBinanceFunding(res, symbol, limit, key, {
      scheduleHistory: String(url.searchParams.get('history_mode') || '').toLowerCase() !== 'none',
    });
    return true;
  }
  const now = Date.now();
  const cached = CACHE.get(key);
  if (cached && now - cached.storedAt <= FRESH_MS) {
    sendJson(res, 200, { ...cached.payload, cache_state: 'fresh' });
    return true;
  }
  let pending = INFLIGHT.get(key);
  if (!pending) {
    pending = load(provider, symbol, limit)
      .then((data) => {
        const payload = {
          ok: true,
          version: VERSION,
          provider,
          market_type: 'contract',
          symbol,
          native_symbol: data.native_symbol || nativeSymbol(provider, symbol),
          source: data.source,
          current: data.current || null,
          history: Array.isArray(data.history) ? data.history.slice(0, limit) : [],
          warnings: Array.isArray(data.warnings) ? data.warnings : [],
          timestamp_ms: Date.now(),
        };
        CACHE.set(key, { storedAt: Date.now(), payload });
        return payload;
      })
      .finally(() => INFLIGHT.delete(key));
    INFLIGHT.set(key, pending);
  }
  try {
    const payload = await pending;
    sendJson(res, 200, { ...payload, cache_state: 'miss' });
  } catch (error) {
    if (cached && now - cached.storedAt <= STALE_MS) {
      sendJson(res, 200, { ...cached.payload, cache_state: 'stale', warning: String(error?.message || error) });
    } else {
      sendJson(res, 502, {
        ok: false,
        version: VERSION,
        provider,
        symbol,
        error: String(error?.message || error),
        reason: 'upstream_unavailable',
      });
    }
  }
  return true;
}
