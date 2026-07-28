import { fetchBinancePublicRestRelayJson } from './binance-contract-kline-relay.mjs';
import {
  ensureBinanceContractRealtimeMeta,
  getBinanceContractRealtimeMeta,
} from './binance-contract-market.mjs';
import { getMarketUniverseRows } from './market-rest.mjs';

const ROUTE = '/api/contract-funding';
const VERSION = '650.8.15.41';
const SUPPORTED = new Set(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const CACHE = new Map();
const INFLIGHT = new Map();
const FRESH_MS = 30_000;
const STALE_MS = 10 * 60_000;
const BINANCE_HISTORY_REFRESH_MS = 5 * 60_000;
const BINANCE_HISTORY_BACKGROUND_DELAY_MS = 10_000;
const BINANCE_REALTIME_WAIT_MS = 6_500;
const BINANCE_HISTORY_REFRESH = new Map();

// Step767.2: five-provider shared funding history persistence.
// Public history is fetched once by the backend, upserted by exact identity,
// retained with a bounded policy and served to all App users from Supabase.
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');
const FUNDING_PERSISTENCE_ENABLED = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const FUNDING_CURRENT_TABLE = 'app_funding_rate_current_cache';
const FUNDING_HISTORY_TABLE = 'app_funding_rate_history_cache';
const FUNDING_ROTATION_TABLE = 'app_contract_funding_rotation_state';
const FUNDING_CLEANUP_RPC = 'kaka_cleanup_contract_funding_cache';
const FUNDING_HISTORY_ROUTE = `${ROUTE}/history`;
const PERSISTED_HISTORY_CACHE = new Map();
const PERSISTED_HISTORY_INFLIGHT = new Map();
const PERSISTED_HISTORY_CACHE_TTL_MS = 5 * 60_000;
const PERSISTED_HISTORY_STALE_MS = 30 * 60_000;
const PERSISTED_HISTORY_CACHE_MAX = 256;
const FUNDING_PERSIST_QUEUE_MAX = 2500;
const fundingCurrentPersistQueue = new Map();
const fundingHistoryPersistQueue = new Map();
const fundingCurrentPersistGate = new Map();
const fundingHistoryPersistGate = new Map();
const FUNDING_CURRENT_PERSIST_MIN_MS = 5 * 60_000;
const FUNDING_HISTORY_PERSIST_MIN_MS = 6 * 60 * 60_000;
const FUNDING_PERSIST_GATE_MAX = 20000;
let fundingPersistTimer = null;
let fundingPersistInflight = null;
const FUNDING_ROTATION_INTERVAL_MS = 60 * 60_000;
const FUNDING_ROTATION_CATALOG_TTL_MS = 6 * 60 * 60_000;
const FUNDING_ROTATION_BATCH_PER_PROVIDER = 4;
const FUNDING_ROTATION_PINNED = ['BTCUSDT', 'ETHUSDT'];
const FUNDING_HISTORY_LIMIT = 24;
const fundingRotationState = {
  started: false,
  timer: null,
  running: null,
  cycle: 0,
  last_started_at: 0,
  last_completed_at: 0,
  last_error: '',
  last_cleanup_at: 0,
  cleanup_error: '',
  provider_errors: Object.fromEntries([...SUPPORTED].map((provider) => [provider, ''])),
  provider_success: Object.fromEntries([...SUPPORTED].map((provider) => [provider, 0])),
  provider_attempts: Object.fromEntries([...SUPPORTED].map((provider) => [provider, 0])),
  cursors: Object.fromEntries([...SUPPORTED].map((provider) => [provider, 0])),
  catalogs: Object.fromEntries([...SUPPORTED].map((provider) => [provider, []])),
  catalog_loaded_at: Object.fromEntries([...SUPPORTED].map((provider) => [provider, 0])),
  last_symbols: Object.fromEntries([...SUPPORTED].map((provider) => [provider, []])),
};


function fundingSupabaseHeaders(extra = {}) {
  return {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    ...extra,
  };
}

function exactFundingKey(provider, symbol) {
  return `${providerKey(provider)}|${canonicalSymbol(symbol)}`;
}

function prunePersistedHistoryCache(now = Date.now()) {
  for (const [key, entry] of PERSISTED_HISTORY_CACHE) {
    if (!entry || now - entry.cachedAt > PERSISTED_HISTORY_STALE_MS) {
      PERSISTED_HISTORY_CACHE.delete(key);
    }
  }
  while (PERSISTED_HISTORY_CACHE.size > PERSISTED_HISTORY_CACHE_MAX) {
    const oldestKey = [...PERSISTED_HISTORY_CACHE.entries()]
      .sort((a, b) => (a[1]?.cachedAt || 0) - (b[1]?.cachedAt || 0))[0]?.[0];
    if (!oldestKey) break;
    PERSISTED_HISTORY_CACHE.delete(oldestKey);
  }
}

function normalizePersistedCurrent(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const provider = providerKey(raw.provider);
  const symbol = canonicalSymbol(raw.symbol);
  if (!SUPPORTED.has(provider) || !symbol) return null;
  return currentRow({
    provider,
    symbol,
    rate: raw.last_funding_rate ?? raw.funding_rate,
    nextTime: raw.next_funding_time,
    mark: raw.mark_price,
    index: raw.index_price,
    sourceTime: raw.source_time ?? raw.cached_at,
    intervalHours: raw.funding_interval_hours,
  });
}

function normalizePersistedHistory(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const provider = providerKey(raw.provider);
  const symbol = canonicalSymbol(raw.symbol);
  if (!SUPPORTED.has(provider) || !symbol) return null;
  return historyRow({
    provider,
    symbol,
    rate: raw.funding_rate,
    time: raw.funding_time,
    mark: raw.mark_price,
  });
}

function mergeFundingHistoryRows(...groups) {
  const byKey = new Map();
  for (const group of groups) {
    for (const raw of Array.isArray(group) ? group : []) {
      const row = normalizePersistedHistory(raw);
      if (!row) continue;
      const key = `${row.provider}|${row.symbol}|${row.funding_time}`;
      const existing = byKey.get(key);
      byKey.set(key, existing ? { ...existing, ...row } : row);
    }
  }
  return [...byKey.values()].sort((a, b) =>
    (Date.parse(String(b.funding_time || '')) || 0) -
    (Date.parse(String(a.funding_time || '')) || 0));
}

async function readPersistedFundingBundle(provider, symbol, limit = 24) {
  if (!FUNDING_PERSISTENCE_ENABLED) {
    return { current: null, history: [], persistence_enabled: false };
  }
  const safeProvider = providerKey(provider);
  const safeSymbol = canonicalSymbol(symbol);
  const safeLimit = Math.max(1, Math.min(100, Number(limit) || 24));
  const key = `${exactFundingKey(safeProvider, safeSymbol)}|${safeLimit}`;
  const now = Date.now();
  prunePersistedHistoryCache(now);
  const cached = PERSISTED_HISTORY_CACHE.get(key);
  if (cached && now - cached.cachedAt < PERSISTED_HISTORY_CACHE_TTL_MS) {
    return { ...cached.payload, cache_hit: true, cache_age_ms: now - cached.cachedAt };
  }
  if (PERSISTED_HISTORY_INFLIGHT.has(key)) return await PERSISTED_HISTORY_INFLIGHT.get(key);
  const pending = (async () => {
    try {
      const currentQuery = new URLSearchParams({
        select: 'provider,market_type,symbol,mark_price,index_price,last_funding_rate,last_funding_rate_percent,next_funding_time,source_time,cached_at',
        provider: `eq.${safeProvider}`,
        symbol: `eq.${safeSymbol}`,
        order: 'cached_at.desc',
        limit: '1',
      });
      const historyQuery = new URLSearchParams({
        select: 'provider,market_type,symbol,funding_time,funding_rate,funding_rate_percent,mark_price,cached_at',
        provider: `eq.${safeProvider}`,
        symbol: `eq.${safeSymbol}`,
        order: 'funding_time.desc',
        limit: String(safeLimit),
      });
      const [currentResponse, historyResponse] = await Promise.all([
        fetch(`${SUPABASE_URL}/rest/v1/${FUNDING_CURRENT_TABLE}?${currentQuery}`, {
          headers: fundingSupabaseHeaders({ accept: 'application/json' }),
          signal: AbortSignal.timeout(12000),
        }),
        fetch(`${SUPABASE_URL}/rest/v1/${FUNDING_HISTORY_TABLE}?${historyQuery}`, {
          headers: fundingSupabaseHeaders({ accept: 'application/json' }),
          signal: AbortSignal.timeout(12000),
        }),
      ]);
      const currentText = await currentResponse.text();
      const historyText = await historyResponse.text();
      if (!currentResponse.ok) throw new Error(`funding_current_store_http_${currentResponse.status}:${currentText.slice(0, 180)}`);
      if (!historyResponse.ok) throw new Error(`funding_history_store_http_${historyResponse.status}:${historyText.slice(0, 180)}`);
      const currentDecoded = JSON.parse(currentText);
      const historyDecoded = JSON.parse(historyText);
      const payload = {
        current: normalizePersistedCurrent(Array.isArray(currentDecoded) ? currentDecoded[0] : null),
        history: mergeFundingHistoryRows(Array.isArray(historyDecoded) ? historyDecoded : []).slice(0, safeLimit),
        persistence_enabled: true,
        cache_hit: false,
        cache_age_ms: 0,
      };
      PERSISTED_HISTORY_CACHE.set(key, { cachedAt: Date.now(), payload });
      prunePersistedHistoryCache();
      return payload;
    } catch (error) {
      const stale = PERSISTED_HISTORY_CACHE.get(key) || cached;
      if (stale && Date.now() - stale.cachedAt < PERSISTED_HISTORY_STALE_MS) {
        return {
          ...stale.payload,
          cache_hit: true,
          cache_stale: true,
          cache_age_ms: Date.now() - stale.cachedAt,
          warning: 'persisted_funding_history_read_failed_stale_retained',
          read_error: String(error?.message || error).slice(0, 220),
        };
      }
      throw error;
    }
  })().finally(() => PERSISTED_HISTORY_INFLIGHT.delete(key));
  PERSISTED_HISTORY_INFLIGHT.set(key, pending);
  return await pending;
}

function capFundingPersistQueues() {
  while (fundingCurrentPersistQueue.size + fundingHistoryPersistQueue.size > FUNDING_PERSIST_QUEUE_MAX) {
    const firstHistoryKey = fundingHistoryPersistQueue.keys().next().value;
    if (firstHistoryKey) fundingHistoryPersistQueue.delete(firstHistoryKey);
    else {
      const firstCurrentKey = fundingCurrentPersistQueue.keys().next().value;
      if (!firstCurrentKey) break;
      fundingCurrentPersistQueue.delete(firstCurrentKey);
    }
  }
}

function pruneFundingPersistGates(now = Date.now()) {
  for (const [key, entry] of fundingCurrentPersistGate) {
    if (!entry || now - entry.at > 24 * 60 * 60_000) fundingCurrentPersistGate.delete(key);
  }
  for (const [key, at] of fundingHistoryPersistGate) {
    if (!at || now - at > 31 * 24 * 60 * 60_000) fundingHistoryPersistGate.delete(key);
  }
  while (fundingCurrentPersistGate.size + fundingHistoryPersistGate.size > FUNDING_PERSIST_GATE_MAX) {
    const currentOldest = [...fundingCurrentPersistGate.entries()].sort((a, b) => (a[1]?.at || 0) - (b[1]?.at || 0))[0];
    const historyOldest = [...fundingHistoryPersistGate.entries()].sort((a, b) => (a[1] || 0) - (b[1] || 0))[0];
    if (currentOldest && (!historyOldest || (currentOldest[1]?.at || 0) <= (historyOldest[1] || 0))) {
      fundingCurrentPersistGate.delete(currentOldest[0]);
    } else if (historyOldest) {
      fundingHistoryPersistGate.delete(historyOldest[0]);
    } else break;
  }
}

function currentPersistRow(raw) {
  const normalized = normalizePersistedCurrent(raw);
  if (!normalized) return null;
  return {
    provider: normalized.provider,
    market_type: 'contract',
    symbol: normalized.symbol,
    mark_price: numberOrNull(normalized.mark_price),
    index_price: numberOrNull(normalized.index_price),
    last_funding_rate: numberOrNull(normalized.last_funding_rate ?? normalized.funding_rate),
    last_funding_rate_percent: numberOrNull(normalized.last_funding_rate_percent ?? normalized.funding_rate_percent),
    next_funding_time: normalized.next_funding_time || null,
    source_time: normalized.source_time || new Date().toISOString(),
    cached_at: new Date().toISOString(),
  };
}

function queueFundingPersistence(current, history) {
  if (!FUNDING_PERSISTENCE_ENABLED) return;
  const now = Date.now();
  pruneFundingPersistGates(now);
  const normalizedCurrent = currentPersistRow(current);
  if (normalizedCurrent) {
    const key = exactFundingKey(normalizedCurrent.provider, normalizedCurrent.symbol);
    const signature = [
      normalizedCurrent.source_time || '',
      normalizedCurrent.last_funding_rate ?? '',
      normalizedCurrent.next_funding_time || '',
      normalizedCurrent.mark_price ?? '',
      normalizedCurrent.index_price ?? '',
    ].join('|');
    const previous = fundingCurrentPersistGate.get(key);
    if (!previous || previous.signature !== signature || now - previous.at >= FUNDING_CURRENT_PERSIST_MIN_MS) {
      fundingCurrentPersistQueue.set(key, normalizedCurrent);
      fundingCurrentPersistGate.set(key, { signature, at: now });
    }
  }
  for (const raw of Array.isArray(history) ? history : []) {
    const row = normalizePersistedHistory(raw);
    if (!row) continue;
    const key = `${exactFundingKey(row.provider, row.symbol)}|${row.funding_time}`;
    const previousAt = fundingHistoryPersistGate.get(key) || 0;
    if (now - previousAt < FUNDING_HISTORY_PERSIST_MIN_MS) continue;
    row.cached_at = new Date().toISOString();
    fundingHistoryPersistQueue.set(key, row);
    fundingHistoryPersistGate.set(key, now);
  }
  capFundingPersistQueues();
  if (!fundingPersistTimer) {
    fundingPersistTimer = setTimeout(() => {
      fundingPersistTimer = null;
      flushFundingPersistence().catch((error) => {
        console.error(`[Step${VERSION}] funding persistence flush failed: ${error?.message || error}`);
      });
    }, 1200);
    fundingPersistTimer.unref?.();
  }
}

async function upsertFundingRows(table, conflict, rows) {
  if (!rows.length) return;
  const response = await fetch(`${SUPABASE_URL}/rest/v1/${table}?on_conflict=${encodeURIComponent(conflict)}`, {
    method: 'POST',
    headers: fundingSupabaseHeaders({
      accept: 'application/json',
      'content-type': 'application/json',
      prefer: 'resolution=merge-duplicates,return=minimal',
    }),
    body: JSON.stringify(rows),
    signal: AbortSignal.timeout(15000),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`${table}_upsert_http_${response.status}:${text.slice(0, 220)}`);
}

async function flushFundingPersistence() {
  if (!FUNDING_PERSISTENCE_ENABLED || fundingPersistInflight ||
      (!fundingCurrentPersistQueue.size && !fundingHistoryPersistQueue.size)) return;
  const currentRows = [...fundingCurrentPersistQueue.values()].slice(0, 200);
  const historyRows = [...fundingHistoryPersistQueue.values()].slice(0, 500);
  for (const row of currentRows) fundingCurrentPersistQueue.delete(exactFundingKey(row.provider, row.symbol));
  for (const row of historyRows) fundingHistoryPersistQueue.delete(`${exactFundingKey(row.provider, row.symbol)}|${row.funding_time}`);
  fundingPersistInflight = (async () => {
    try {
      await upsertFundingRows(FUNDING_CURRENT_TABLE, 'provider,market_type,symbol', currentRows);
      await upsertFundingRows(FUNDING_HISTORY_TABLE, 'provider,market_type,symbol,funding_time', historyRows);
      for (const row of [...currentRows, ...historyRows]) {
        for (const key of [...PERSISTED_HISTORY_CACHE.keys()]) {
          if (key.startsWith(`${exactFundingKey(row.provider, row.symbol)}|`)) PERSISTED_HISTORY_CACHE.delete(key);
        }
      }
    } catch (error) {
      for (const row of currentRows) fundingCurrentPersistQueue.set(exactFundingKey(row.provider, row.symbol), row);
      for (const row of historyRows) fundingHistoryPersistQueue.set(`${exactFundingKey(row.provider, row.symbol)}|${row.funding_time}`, row);
      capFundingPersistQueues();
      throw error;
    }
  })().finally(() => { fundingPersistInflight = null; });
  await fundingPersistInflight;
  if (fundingCurrentPersistQueue.size || fundingHistoryPersistQueue.size) {
    if (!fundingPersistTimer) {
      fundingPersistTimer = setTimeout(() => {
        fundingPersistTimer = null;
        flushFundingPersistence().catch(() => {});
      }, 1500);
      fundingPersistTimer.unref?.();
    }
  }
}

async function fundingRpc(name, body = {}) {
  const response = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${name}`, {
    method: 'POST',
    headers: fundingSupabaseHeaders({ accept: 'application/json', 'content-type': 'application/json' }),
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(30000),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`${name}_http_${response.status}:${text.slice(0, 220)}`);
  if (!text.trim()) return null;
  try { return JSON.parse(text); } catch (_) { return text; }
}

async function loadFundingRotationState() {
  if (!FUNDING_PERSISTENCE_ENABLED) return;
  const query = new URLSearchParams({ select: 'provider,cursor,cycle', limit: '20' });
  const response = await fetch(`${SUPABASE_URL}/rest/v1/${FUNDING_ROTATION_TABLE}?${query}`, {
    headers: fundingSupabaseHeaders({ accept: 'application/json' }),
    signal: AbortSignal.timeout(12000),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`funding_rotation_state_http_${response.status}:${text.slice(0, 180)}`);
  const rows = JSON.parse(text);
  for (const row of Array.isArray(rows) ? rows : []) {
    const provider = providerKey(row?.provider);
    if (!SUPPORTED.has(provider)) continue;
    fundingRotationState.cursors[provider] = Math.max(0, Number(row?.cursor) || 0);
    fundingRotationState.cycle = Math.max(fundingRotationState.cycle, Number(row?.cycle) || 0);
  }
}

async function saveFundingRotationState(provider, cursor, symbols, catalogSize) {
  if (!FUNDING_PERSISTENCE_ENABLED) return;
  const row = {
    provider,
    cursor: Math.max(0, Number(cursor) || 0),
    cycle: fundingRotationState.cycle,
    catalog_size: Math.max(0, Number(catalogSize) || 0),
    last_symbols: symbols,
    last_started_at: fundingRotationState.last_started_at ? new Date(fundingRotationState.last_started_at).toISOString() : null,
    last_completed_at: new Date().toISOString(),
    last_error: fundingRotationState.provider_errors[provider] || '',
    updated_at: new Date().toISOString(),
  };
  await upsertFundingRows(FUNDING_ROTATION_TABLE, 'provider', [row]);
}

async function loadFundingCatalog(provider) {
  const now = Date.now();
  const existing = fundingRotationState.catalogs[provider] || [];
  if (existing.length && now - (fundingRotationState.catalog_loaded_at[provider] || 0) < FUNDING_ROTATION_CATALOG_TTL_MS) return existing;
  const rows = await getMarketUniverseRows(provider, 'contract', 'USDT');
  const symbols = [...new Set((Array.isArray(rows) ? rows : [])
    .map((row) => canonicalSymbol(row?.symbol))
    .filter((symbol) => symbol && symbol.endsWith('USDT') && supportsNativeContract(provider, symbol)))].sort();
  fundingRotationState.catalogs[provider] = symbols;
  fundingRotationState.catalog_loaded_at[provider] = now;
  if ((fundingRotationState.cursors[provider] || 0) >= symbols.length) fundingRotationState.cursors[provider] = 0;
  return symbols;
}

function nextFundingRotationBatch(provider, catalog) {
  if (!catalog.length) return [];
  const pinned = FUNDING_ROTATION_PINNED.filter((symbol) => catalog.includes(symbol));
  let cursor = Math.max(0, Number(fundingRotationState.cursors[provider]) || 0) % catalog.length;
  const rotating = [];
  for (let index = 0; index < Math.min(FUNDING_ROTATION_BATCH_PER_PROVIDER, catalog.length); index += 1) {
    rotating.push(catalog[(cursor + index) % catalog.length]);
  }
  fundingRotationState.cursors[provider] = (cursor + rotating.length) % catalog.length;
  return [...new Set([...pinned, ...rotating])];
}

async function fetchFundingHistoryOnly(provider, symbol, limit = FUNDING_HISTORY_LIMIT) {
  const safeLimit = Math.max(1, Math.min(100, Number(limit) || FUNDING_HISTORY_LIMIT));
  if (provider === 'binance') return await fetchBinanceFundingHistory(symbol, safeLimit);
  if (provider === 'okx') {
    const native = nativeSymbol(provider, symbol);
    const raw = await fetchJson(`https://www.okx.com/api/v5/public/funding-rate-history?instId=${encodeURIComponent(native)}&limit=${safeLimit}`);
    return (Array.isArray(raw?.data) ? raw.data : []).map((row) => historyRow({ provider, symbol, rate: row?.realizedRate || row?.fundingRate, time: row?.fundingTime })).filter(Boolean);
  }
  if (provider === 'bybit') {
    const native = nativeSymbol(provider, symbol);
    const raw = await fetchJson(`https://api.bybit.com/v5/market/funding/history?category=${bybitCategory(symbol)}&symbol=${encodeURIComponent(native)}&limit=${safeLimit}`);
    return (Array.isArray(raw?.result?.list) ? raw.result.list : []).map((row) => historyRow({ provider, symbol, rate: row?.fundingRate, time: row?.fundingRateTimestamp })).filter(Boolean);
  }
  if (provider === 'bitget') {
    const native = nativeSymbol(provider, symbol);
    const q = `symbol=${encodeURIComponent(native)}&productType=${encodeURIComponent(bitgetProductType(symbol))}`;
    const raw = await fetchJson(`https://api.bitget.com/api/v2/mix/market/history-fund-rate?${q}&pageSize=${safeLimit}`);
    const list = Array.isArray(raw?.data) ? raw.data : (Array.isArray(raw?.data?.list) ? raw.data.list : []);
    return list.map((row) => historyRow({ provider, symbol, rate: row?.fundingRate, time: row?.fundingTime ?? row?.fundingRateTimestamp })).filter(Boolean);
  }
  if (provider === 'gate') {
    const native = nativeSymbol(provider, symbol);
    const raw = await fetchJson(`https://api.gateio.ws/api/v4/futures/${gateSettle(symbol)}/funding_rate?contract=${encodeURIComponent(native)}&limit=${safeLimit}`);
    return (Array.isArray(raw) ? raw : []).map((row) => historyRow({ provider, symbol, rate: row?.funding_rate ?? row?.r ?? row?.rate, time: row?.funding_time ?? row?.t ?? row?.time, mark: row?.mark_price })).filter(Boolean);
  }
  return [];
}

async function runFundingHistoryRotationCycle() {
  if (!FUNDING_PERSISTENCE_ENABLED || fundingRotationState.running) return fundingRotationState.running;
  fundingRotationState.last_started_at = Date.now();
  fundingRotationState.cycle += 1;
  const pending = (async () => {
    for (const provider of SUPPORTED) {
      try {
        const catalog = await loadFundingCatalog(provider);
        const symbols = nextFundingRotationBatch(provider, catalog);
        fundingRotationState.last_symbols[provider] = symbols;
        fundingRotationState.provider_errors[provider] = '';
        for (const symbol of symbols) {
          fundingRotationState.provider_attempts[provider] += 1;
          try {
            const history = await fetchFundingHistoryOnly(provider, symbol, FUNDING_HISTORY_LIMIT);
            if (history.length) {
              queueFundingPersistence(null, history);
              fundingRotationState.provider_success[provider] += 1;
            }
          } catch (error) {
            fundingRotationState.provider_errors[provider] = String(error?.message || error).slice(0, 180);
          }
          await new Promise((resolve) => setTimeout(resolve, 350));
        }
        await saveFundingRotationState(provider, fundingRotationState.cursors[provider], symbols, catalog.length);
      } catch (error) {
        fundingRotationState.provider_errors[provider] = String(error?.message || error).slice(0, 220);
      }
    }
    await flushFundingPersistence();
    fundingRotationState.last_completed_at = Date.now();
    fundingRotationState.last_error = '';
  })().catch((error) => {
    fundingRotationState.last_error = String(error?.message || error).slice(0, 240);
  }).finally(() => { fundingRotationState.running = null; });
  fundingRotationState.running = pending;
  return await pending;
}

async function cleanupFundingPersistence() {
  if (!FUNDING_PERSISTENCE_ENABLED) return;
  try {
    await fundingRpc(FUNDING_CLEANUP_RPC, {});
    fundingRotationState.last_cleanup_at = Date.now();
    fundingRotationState.cleanup_error = '';
    prunePersistedHistoryCache();
  } catch (error) {
    fundingRotationState.cleanup_error = String(error?.message || error).slice(0, 220);
  }
}

function scheduleFundingHistoryRotation(delayMs = FUNDING_ROTATION_INTERVAL_MS) {
  if (!fundingRotationState.started) return;
  if (fundingRotationState.timer) clearTimeout(fundingRotationState.timer);
  fundingRotationState.timer = setTimeout(async () => {
    await runFundingHistoryRotationCycle();
    scheduleFundingHistoryRotation(FUNDING_ROTATION_INTERVAL_MS);
  }, Math.max(5000, delayMs));
  fundingRotationState.timer.unref?.();
}

export function startContractFundingHistoryMaintainer() {
  if (fundingRotationState.started || !FUNDING_PERSISTENCE_ENABLED) return;
  fundingRotationState.started = true;
  loadFundingRotationState().catch((error) => {
    fundingRotationState.last_error = String(error?.message || error).slice(0, 220);
  }).finally(() => scheduleFundingHistoryRotation(120_000));
  const cleanupTimer = setInterval(() => cleanupFundingPersistence(), 6 * 60 * 60_000);
  cleanupTimer.unref?.();
  const firstCleanup = setTimeout(() => cleanupFundingPersistence(), 90_000);
  firstCleanup.unref?.();
}

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

function supportsNativeContract(provider, symbol) {
  const { quote } = splitSymbol(symbol);
  if (quote === 'USDT') return SUPPORTED.has(provider);
  if (quote === 'USDC') {
    return provider === 'binance' ||
      provider === 'bybit' ||
      provider === 'bitget';
  }
  if (quote === 'USD') {
    return provider === 'okx' ||
      provider === 'bybit' ||
      provider === 'bitget' ||
      provider === 'gate';
  }
  return false;
}
function bybitCategory(symbol) {
  return splitSymbol(symbol).quote === 'USD'
    ? 'inverse'
    : 'linear';
}
function gateSettle(symbol) {
  return splitSymbol(symbol).quote === 'USD'
    ? 'btc'
    : 'usdt';
}

function nativeSymbol(provider, symbol) {
  const { base, quote } = splitSymbol(symbol);
  if (!supportsNativeContract(provider, symbol)) throw new Error('unsupported_native_contract_quote');
  if ((provider === 'bybit' || provider === 'bitget') &&
      quote === 'USDC') {
    return `${base}PERP`;
  }
  if ((provider === 'bybit' || provider === 'bitget') &&
      quote === 'USD') {
    return `${base}USD`;
  }
  if (provider === 'okx') return `${base}-${quote}-SWAP`;
  if (provider === 'gate') return `${base}_${quote}`;
  return symbol;
}

function bitgetProductType(symbol) {
  const quote = splitSymbol(symbol).quote;
  if (quote === 'USDC') return 'usdc-futures';
  if (quote === 'USD') return 'coin-futures';
  return 'usdt-futures';
}

function numberOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function positivePriceOrNull(value) {
  const n = numberOrNull(value);
  return n != null && n > 0 ? n : null;
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
    mark_price: positivePriceOrNull(mark),
    index_price: positivePriceOrNull(index),
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
    mark_price: positivePriceOrNull(mark),
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
    deferWhenBusy: options.deferWhenBusy === true,
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

async function fetchPair(currentUrl, historyUrl, { includeHistory = true } = {}) {
  const [currentResult, historyResult] = await Promise.allSettled([
    fetchJson(currentUrl),
    includeHistory && historyUrl ? fetchJson(historyUrl) : Promise.resolve(null),
  ]);
  if (currentResult.status === 'rejected' &&
      (includeHistory ? historyResult.status === 'rejected' : true)) {
    throw new Error(`current:${currentResult.reason?.message || currentResult.reason}${includeHistory ? `;history:${historyResult.reason?.message || historyResult.reason}` : ''}`);
  }
  return {
    currentRaw: currentResult.status === 'fulfilled' ? currentResult.value : null,
    historyRaw: historyResult.status === 'fulfilled' ? historyResult.value : null,
    warnings: [
      currentResult.status === 'rejected' ? `current:${currentResult.reason?.message || currentResult.reason}` : null,
      includeHistory && historyResult.status === 'rejected' ? `history:${historyResult.reason?.message || historyResult.reason}` : null,
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
  const immediateNext = Date.parse(String(immediate?.next_funding_time || ''));
  if (immediate && Number.isFinite(immediateNext) && immediateNext > Date.now()) {
    return immediate;
  }
  await ensureBinanceContractRealtimeMeta(symbol, {
    waitMs,
    requireFundingSchedule: true,
  });
  return binanceRealtimeCurrent(symbol);
}

async function fetchBinanceFundingHistory(symbol, limit) {
  const historyRaw = await fetchBinanceJson(
    `https://fapi.binance.com/fapi/v1/fundingRate?symbol=${encodeURIComponent(symbol)}&limit=${limit}`,
    8000,
    'funding:history_background',
    { lane: 'auxiliary', priority: -10, deferWhenBusy: true },
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
      queueFundingPersistence(null, history);
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
  let persisted = { current: null, history: [] };
  try { persisted = await readPersistedFundingBundle('binance', symbol, limit); } catch (_) {}
  const history = mergeFundingHistoryRows(
    Array.isArray(cached?.payload?.history) ? cached.payload.history : [],
    persisted.history,
  ).slice(0, limit);
  queueFundingPersistence(current, history);
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

async function fetchOkx(symbol, limit, { includeHistory = true } = {}) {
  const native = nativeSymbol('okx', symbol);
  const { currentRaw, historyRaw, warnings } = await fetchPair(
    `https://www.okx.com/api/v5/public/funding-rate?instId=${encodeURIComponent(native)}`,
    `https://www.okx.com/api/v5/public/funding-rate-history?instId=${encodeURIComponent(native)}&limit=${limit}`,
    { includeHistory },
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

async function fetchBybit(symbol, limit, { includeHistory = true } = {}) {
  const native = nativeSymbol('bybit', symbol);
  const { currentRaw, historyRaw, warnings } = await fetchPair(
    `https://api.bybit.com/v5/market/tickers?category=${bybitCategory(symbol)}&symbol=${encodeURIComponent(native)}`,
    `https://api.bybit.com/v5/market/funding/history?category=${bybitCategory(symbol)}&symbol=${encodeURIComponent(native)}&limit=${limit}`,
    { includeHistory },
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
  return { current, history, warnings, source: 'bybit_official_public_funding_rest', native_symbol: native };
}

async function fetchBitget(symbol, limit, { includeHistory = true } = {}) {
  const native = nativeSymbol('bitget', symbol);
  const q = `symbol=${encodeURIComponent(native)}&productType=${encodeURIComponent(bitgetProductType(symbol))}`;
  const [currentResult, fundingTimeResult, historyResult] = await Promise.allSettled([
    fetchJson(`https://api.bitget.com/api/v2/mix/market/current-fund-rate?${q}`),
    fetchJson(`https://api.bitget.com/api/v2/mix/market/funding-time?${q}`),
    includeHistory
      ? fetchJson(`https://api.bitget.com/api/v2/mix/market/history-fund-rate?${q}&pageSize=${limit}`)
      : Promise.resolve(null),
  ]);
  if (currentResult.status === 'rejected' &&
      fundingTimeResult.status === 'rejected' &&
      (!includeHistory || historyResult.status === 'rejected')) {
    throw new Error([
      `current:${currentResult.reason?.message || currentResult.reason}`,
      `funding_time:${fundingTimeResult.reason?.message || fundingTimeResult.reason}`,
      includeHistory ? `history:${historyResult.reason?.message || historyResult.reason}` : null,
    ].filter(Boolean).join(';'));
  }
  const currentRaw = currentResult.status === 'fulfilled' ? currentResult.value : null;
  const fundingTimeRaw = fundingTimeResult.status === 'fulfilled' ? fundingTimeResult.value : null;
  const historyRaw = historyResult.status === 'fulfilled' ? historyResult.value : null;
  const warnings = [
    currentResult.status === 'rejected' ? `current:${currentResult.reason?.message || currentResult.reason}` : null,
    fundingTimeResult.status === 'rejected' ? `funding_time:${fundingTimeResult.reason?.message || fundingTimeResult.reason}` : null,
    includeHistory && historyResult.status === 'rejected' ? `history:${historyResult.reason?.message || historyResult.reason}` : null,
  ].filter(Boolean);
  const item = Array.isArray(currentRaw?.data) ? currentRaw.data[0] : currentRaw?.data;
  const timeItem = Array.isArray(fundingTimeRaw?.data) ? fundingTimeRaw.data[0] : fundingTimeRaw?.data;
  const current = currentRow({
    provider: 'bitget', symbol,
    rate: item?.fundingRate,
    nextTime: timeItem?.nextFundingTime ?? item?.nextFundingTime ?? item?.nextUpdate,
    sourceTime: fundingTimeRaw?.requestTime ?? currentRaw?.requestTime,
    intervalHours: timeItem?.ratePeriod ?? item?.fundingRateInterval,
  });
  const list = Array.isArray(historyRaw?.data) ? historyRaw.data : (Array.isArray(historyRaw?.data?.list) ? historyRaw.data.list : []);
  const history = list.map((row) => historyRow({
    provider: 'bitget', symbol,
    rate: row?.fundingRate,
    time: row?.fundingTime ?? row?.fundingRateTimestamp,
  })).filter(Boolean);
  return {
    current,
    history,
    warnings,
    source: 'bitget_official_public_current_funding_plus_next_funding_time_rest',
    native_symbol: native,
  };
}

async function fetchGate(symbol, limit, { includeHistory = true } = {}) {
  const native = nativeSymbol('gate', symbol);
  const { currentRaw: contractRaw, historyRaw, warnings } = await fetchPair(
    `https://api.gateio.ws/api/v4/futures/${gateSettle(symbol)}/contracts/${encodeURIComponent(native)}`,
    `https://api.gateio.ws/api/v4/futures/${gateSettle(symbol)}/funding_rate?contract=${encodeURIComponent(native)}&limit=${limit}`,
    { includeHistory },
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

async function load(provider, symbol, limit, { includeHistory = true } = {}) {
  switch (provider) {
    case 'binance': return fetchBinance(symbol, limit);
    case 'okx': return fetchOkx(symbol, limit, { includeHistory });
    case 'bybit': return fetchBybit(symbol, limit, { includeHistory });
    case 'bitget': return fetchBitget(symbol, limit, { includeHistory });
    case 'gate': return fetchGate(symbol, limit, { includeHistory });
    default: throw new Error('unsupported_provider');
  }
}

export async function handleContractFunding(req, res, url) {
  if (url.pathname === `${ROUTE}/health`) {
    sendJson(res, 200, {
      ok: true,
      version: VERSION,
      persistence_enabled: FUNDING_PERSISTENCE_ENABLED,
      history_endpoint: FUNDING_HISTORY_ROUTE,
      current_storage_table: FUNDING_CURRENT_TABLE,
      history_storage_table: FUNDING_HISTORY_TABLE,
      history_cache_ttl_seconds: Math.round(PERSISTED_HISTORY_CACHE_TTL_MS / 1000),
      history_stale_seconds: Math.round(PERSISTED_HISTORY_STALE_MS / 1000),
      history_retention_days: 31,
      current_retention_days: 7,
      persisted_history_cache_entries: PERSISTED_HISTORY_CACHE.size,
      persisted_history_inflight_entries: PERSISTED_HISTORY_INFLIGHT.size,
      persist_current_queue: fundingCurrentPersistQueue.size,
      persist_history_queue: fundingHistoryPersistQueue.size,
      persistence_flush_active: Boolean(fundingPersistInflight),
      background_rotation: {
        enabled: FUNDING_PERSISTENCE_ENABLED,
        mode: 'backend_fixed_bounded_history_rotation',
        interval_ms: FUNDING_ROTATION_INTERVAL_MS,
        rotating_batch_per_provider: FUNDING_ROTATION_BATCH_PER_PROVIDER,
        pinned_symbols: FUNDING_ROTATION_PINNED,
        cycle: fundingRotationState.cycle,
        running: Boolean(fundingRotationState.running),
        last_started_at: fundingRotationState.last_started_at ? new Date(fundingRotationState.last_started_at).toISOString() : null,
        last_completed_at: fundingRotationState.last_completed_at ? new Date(fundingRotationState.last_completed_at).toISOString() : null,
        last_error: fundingRotationState.last_error,
        last_cleanup_at: fundingRotationState.last_cleanup_at ? new Date(fundingRotationState.last_cleanup_at).toISOString() : null,
        cleanup_error: fundingRotationState.cleanup_error,
        cursors: fundingRotationState.cursors,
        catalog_size_by_provider: Object.fromEntries([...SUPPORTED].map((provider) => [provider, fundingRotationState.catalogs[provider]?.length || 0])),
        last_symbols_by_provider: fundingRotationState.last_symbols,
        attempts_by_provider: fundingRotationState.provider_attempts,
        success_by_provider: fundingRotationState.provider_success,
        errors_by_provider: fundingRotationState.provider_errors,
      },
      old_binance_cron_parallel_observation_retained: true,
      history_reads_open_exchange_connection: false,
      persisted_history_read_market_type_mode: 'dedicated_funding_table_provider_symbol_compat_then_normalize_contract',
      missing_mark_and_index_price_zero_normalized_to_null: true,
      cache_entries: CACHE.size,
      inflight_entries: INFLIGHT.size,
      binance_history_refreshes: BINANCE_HISTORY_REFRESH.size,
      binance_current_transport: 'mark_price_websocket',
      binance_history_transport: 'authenticated_edge_relay_background',
      first_paint_waits_for_history: false,
      current_only_mode_skips_history_requests: true,
      binance_current_on_demand_mark_price_snapshot: true,
      bitget_next_funding_time_endpoint: '/api/v2/mix/market/funding-time',
      native_contract_quotes: {
        USDT: ['binance','okx','bybit','bitget','gate'],
        USDC: ['binance','bybit','bitget'],
        USD: ['okx','bybit','bitget','gate'],
      },
      okx_usdc_contract_retired: true,
      okx_current_contract_quotes: ['USDT', 'USD'],
      binance_coin_m_usd_enabled: false,
      history_background_delay_ms: BINANCE_HISTORY_BACKGROUND_DELAY_MS,
      time: new Date().toISOString(),
    });
    return true;
  }
  if (url.pathname === FUNDING_HISTORY_ROUTE) {
    if (req.method !== 'GET') {
      sendJson(res, 405, { ok: false, version: VERSION, error: 'method_not_allowed' });
      return true;
    }
    const provider = providerKey(url.searchParams.get('provider'));
    const symbol = canonicalSymbol(url.searchParams.get('symbol'));
    const limit = Math.max(1, Math.min(100, Number(url.searchParams.get('limit') || 24) || 24));
    if (!SUPPORTED.has(provider) || !symbol || !supportsNativeContract(provider, symbol)) {
      sendJson(res, 400, { ok: false, version: VERSION, error: 'invalid_provider_or_symbol' });
      return true;
    }
    if (!FUNDING_PERSISTENCE_ENABLED) {
      sendJson(res, 503, { ok: false, version: VERSION, provider, symbol, error: 'funding_history_persistence_disabled' });
      return true;
    }
    try {
      const bundle = await readPersistedFundingBundle(provider, symbol, limit);
      sendJson(res, 200, {
        ok: true, version: VERSION, provider, market_type: 'contract', symbol,
        source: 'render_shared_persisted_five_provider_funding_history',
        current: bundle.current || null,
        history: bundle.history || [],
        history_count: bundle.history?.length || 0,
        storage_table: FUNDING_HISTORY_TABLE,
        history_retention_days: 31,
        current_retention_days: 7,
        exchange_requests_started: 0,
        persisted_history_read_market_type_mode: 'provider_symbol_compat',
        cache_hit: bundle.cache_hit === true,
        cache_stale: bundle.cache_stale === true,
        cache_age_ms: bundle.cache_age_ms || 0,
        warning: bundle.warning || null,
        timestamp_ms: Date.now(),
      });
    } catch (error) {
      sendJson(res, 502, { ok: false, version: VERSION, provider, symbol, error: String(error?.message || error), reason: 'persisted_funding_history_unavailable' });
    }
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
  if (!SUPPORTED.has(provider) || !symbol || !supportsNativeContract(provider, symbol)) {
    sendJson(res, 400, { ok: false, version: VERSION, error: 'invalid_provider_or_symbol' });
    return true;
  }
  const includeHistory = String(url.searchParams.get('history_mode') || '').toLowerCase() !== 'none';
  const key = `${provider}|${symbol}|${limit}|${includeHistory ? 'history' : 'current'}`;
  if (provider === 'binance') {
    await serveBinanceFunding(res, symbol, limit, key, {
      scheduleHistory: includeHistory,
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
    pending = Promise.allSettled([
      load(provider, symbol, limit, { includeHistory }),
      includeHistory ? readPersistedFundingBundle(provider, symbol, limit) : Promise.resolve({ current: null, history: [] }),
    ])
      .then((results) => {
        if (results[0].status === 'rejected') throw results[0].reason;
        const data = results[0].value;
        const persisted = results[1].status === 'fulfilled' ? results[1].value : { current: null, history: [] };
        const history = includeHistory
          ? mergeFundingHistoryRows(data.history, persisted.history).slice(0, limit)
          : [];
        const current = data.current || persisted.current || null;
        const payload = {
          ok: true,
          version: VERSION,
          provider,
          market_type: 'contract',
          symbol,
          native_symbol: data.native_symbol || nativeSymbol(provider, symbol),
          source: data.source,
          current,
          history,
          warnings: Array.isArray(data.warnings) ? data.warnings : [],
          persisted_history_fallback_used: includeHistory && (!Array.isArray(data.history) || data.history.length === 0) && history.length > 0,
          timestamp_ms: Date.now(),
        };
        queueFundingPersistence(current, history);
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
