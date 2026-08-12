import { getContractFocusPoolInternalSnapshot } from './contract-focus-pool.mjs';
import { getMarketLightInternalSnapshot } from './market-light-snapshot.mjs';

const VERSION = '650.8.15.1';
const SNAPSHOT_ROUTE = '/api/bybit-advanced/current-snapshot';
const HEALTH_ROUTE = '/api/bybit-advanced/health';

const HOSTS = Object.freeze(['https://api.bybit.com', 'https://api.bytick.com']);
const FOCUS_TARGET = 15;
const HISTORY_INTERVALS = Object.freeze(['5min', '1h', '1d']);
const HISTORY_LIMIT = Object.freeze({ '5min': 120, '1h': 168, '1d': 90 });

const START_DELAY_MS = Math.max(5_000, Number(process.env.KAKA_BYBIT_ADVANCED_START_DELAY_MS || 12_000));
const STARTUP_RETRY_MS = Math.max(15_000, Number(process.env.KAKA_BYBIT_ADVANCED_STARTUP_RETRY_MS || 30_000));
const FULL_REFRESH_MS = Math.max(60 * 60_000, Number(process.env.KAKA_BYBIT_ADVANCED_FULL_REFRESH_MS || 6 * 60 * 60_000));
const INSURANCE_REFRESH_MS = Math.max(10 * 60_000, Number(process.env.KAKA_BYBIT_ADVANCED_INSURANCE_REFRESH_MS || 30 * 60_000));
const FOCUS_WATCH_MS = Math.max(15_000, Number(process.env.KAKA_BYBIT_ADVANCED_FOCUS_WATCH_MS || 30_000));
const HISTORY_STALE_MS = Math.max(6 * 60 * 60_000, Number(process.env.KAKA_BYBIT_ADVANCED_HISTORY_STALE_MS || 12 * 60 * 60_000));
const RISK_STALE_MS = Math.max(12 * 60 * 60_000, Number(process.env.KAKA_BYBIT_ADVANCED_RISK_STALE_MS || 24 * 60 * 60_000));
const INSURANCE_STALE_MS = Math.max(60 * 60_000, Number(process.env.KAKA_BYBIT_ADVANCED_INSURANCE_STALE_MS || 2 * 60 * 60_000));
const PER_REQUEST_GAP_MS = Math.max(220, Number(process.env.KAKA_BYBIT_ADVANCED_PER_REQUEST_GAP_MS || 300));
const RESPONSE_CACHE_TTL_MS = Math.max(3_000, Number(process.env.KAKA_BYBIT_ADVANCED_RESPONSE_CACHE_TTL_MS || 20_000));

let started = false;
let running = null;
let startupTimer = null;
let retryTimer = null;
let fullInterval = null;
let insuranceInterval = null;
let focusWatchInterval = null;
let lastFocusSignature = '';
let totalReads = 0;
let totalBuilds = 0;
let totalBuildFailures = 0;
let totalExchangeRequests = 0;
let totalExchangeSuccesses = 0;
let totalExchangeFailures = 0;
let lastStartedAt = null;
let lastCompletedAt = null;
let lastError = '';
let round = 0;

const historyBySymbol = new Map();
const riskBySymbol = new Map();
let insuranceState = {
  official_response: false,
  updated_at: null,
  source_time: null,
  pools: [],
  last_error: '',
};
const responseCache = new Map();
const laneStats = new Map();

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
function clone(value) {
  return value == null ? value : JSON.parse(JSON.stringify(value));
}
function compact(value) {
  return String(value || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}
function finite(value) {
  if (value == null) return null;
  if (typeof value === 'string' && value.trim() === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
function isoMs(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? new Date(n).toISOString() : null;
}
function isFresh(value, staleMs) {
  const ms = Date.parse(String(value || ''));
  return Number.isFinite(ms) && Date.now() - ms <= staleMs;
}
function setLane(name, patch) {
  laneStats.set(name, { ...(laneStats.get(name) || {}), ...patch });
}
function focusTargets() {
  const focus = getContractFocusPoolInternalSnapshot();
  const rows = (Array.isArray(focus?.rows) ? focus.rows : [])
    .filter((row) => row?.provider === 'bybit' && row?.market_type === 'contract')
    .map((row) => ({
      symbol: compact(row?.symbol),
      base_asset: compact(row?.base_asset),
      role: String(row?.role || ''),
      slot: Number(row?.slot || 0),
    }))
    .filter((row) => row.symbol.endsWith('USDT'));

  const unique = [];
  const seen = new Set();
  for (const row of rows) {
    if (!row.symbol || seen.has(row.symbol)) continue;
    seen.add(row.symbol);
    unique.push(row);
  }
  return {
    focus_ready: focus?.ready === true,
    focus_round: Number(focus?.round || 0),
    rows: unique.slice(0, FOCUS_TARGET),
  };
}
function focusSignature(rows) {
  return [...rows].map((row) => `${row.slot}:${row.symbol}`).sort().join('|');
}
function marketLightCurrentOiMap() {
  const snapshot = getMarketLightInternalSnapshot({ market: 'contract', provider: 'bybit' });
  const map = new Map();
  for (const row of Array.isArray(snapshot?.rows) ? snapshot.rows : []) {
    const symbol = compact(row?.symbol);
    if (!symbol) continue;
    map.set(symbol, {
      open_interest: finite(row?.open_interest),
      open_interest_value: finite(row?.open_interest_value),
      source_time: row?.source_time || row?.updated_at || null,
      source: row?.open_interest_source || 'bybit_official_linear_ticker_batch',
    });
  }
  return {
    ready: snapshot?.ok === true && snapshot?.stale !== true && !String(snapshot?.last_error || '').trim(),
    map,
  };
}

async function fetchJson(path, lane, timeoutMs = 18_000) {
  let last = null;
  for (const host of HOSTS) {
    totalExchangeRequests += 1;
    setLane(lane, {
      attempts: Number(laneStats.get(lane)?.attempts || 0) + 1,
      last_started_at: new Date().toISOString(),
      last_host: host,
    });
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    timer.unref?.();
    try {
      const response = await fetch(`${host}${path}`, {
        headers: {
          accept: 'application/json',
          'user-agent': 'KakaWeb3/650.8.15.106 bybit-advanced-shared',
        },
        signal: controller.signal,
      });
      const text = await response.text();
      let payload = null;
      try { payload = text ? JSON.parse(text) : null; } catch {}
      if (!response.ok || Number(payload?.retCode ?? -1) !== 0) {
        const error = new Error(`bybit_http_${response.status}:${lane}:retCode=${payload?.retCode ?? ''}:retMsg=${payload?.retMsg || ''}`);
        last = error;
        setLane(lane, { failures: Number(laneStats.get(lane)?.failures || 0) + 1, last_error: error.message });
        continue;
      }
      totalExchangeSuccesses += 1;
      setLane(lane, {
        successes: Number(laneStats.get(lane)?.successes || 0) + 1,
        last_completed_at: new Date().toISOString(),
        last_error: '',
      });
      return payload;
    } catch (error) {
      last = error;
      setLane(lane, { failures: Number(laneStats.get(lane)?.failures || 0) + 1, last_error: String(error?.message || error).slice(0, 240) });
    } finally {
      clearTimeout(timer);
    }
  }
  totalExchangeFailures += 1;
  throw last || new Error(`bybit_upstream_unavailable:${lane}`);
}

function parseHistory(payload, symbol, interval) {
  const list = Array.isArray(payload?.result?.list) ? payload.result.list : [];
  const rows = list.map((item) => {
    const openInterest = finite(item?.openInterest);
    const singleOpenInterest = finite(item?.singleOpenInterest);
    const timestampMs = Number(item?.timestamp || 0);
    if (!Number.isFinite(timestampMs) || timestampMs <= 0) return null;
    return {
      open_interest: openInterest,
      single_open_interest: singleOpenInterest,
      timestamp_ms: timestampMs,
      source_time: isoMs(timestampMs),
    };
  }).filter(Boolean).sort((a, b) => b.timestamp_ms - a.timestamp_ms);

  return {
    symbol,
    interval,
    official_response: true,
    official_empty: rows.length === 0,
    row_count: rows.length,
    rows,
    updated_at: new Date().toISOString(),
    source: 'bybit_official_public_open_interest_history',
    endpoint: '/v5/market/open-interest',
  };
}

function parseRisk(payload, symbol) {
  const list = Array.isArray(payload?.result?.list) ? payload.result.list : [];
  const tiers = list
    .filter((item) => compact(item?.symbol) === symbol)
    .map((item) => ({
      id: Number(item?.id || 0) || null,
      risk_limit_value: finite(item?.riskLimitValue),
      maintenance_margin: finite(item?.maintenanceMargin),
      initial_margin: finite(item?.initialMargin),
      is_lowest_risk: Number(item?.isLowestRisk || 0) === 1,
      max_leverage: finite(item?.maxLeverage),
      mm_deduction: finite(item?.mmDeduction),
    }))
    .filter((item) => item.id != null || item.risk_limit_value != null);

  return {
    symbol,
    official_response: true,
    official_empty: tiers.length === 0,
    tier_count: tiers.length,
    tiers,
    lowest_tier: tiers.find((tier) => tier.is_lowest_risk) || tiers[0] || null,
    updated_at: new Date().toISOString(),
    source: 'bybit_official_public_risk_limit',
    endpoint: '/v5/market/risk-limit',
  };
}

function parseInsurance(payload) {
  const list = Array.isArray(payload?.result?.list) ? payload.result.list : [];
  const pools = list.map((item, index) => {
    const symbols = String(item?.symbols || '')
      .split(',')
      .map((value) => compact(value))
      .filter(Boolean);
    return {
      pool_index: index + 1,
      coin: compact(item?.coin),
      symbols,
      balance: finite(item?.balance),
      value_usd: finite(item?.value),
    };
  });
  return {
    official_response: true,
    source_time: isoMs(payload?.result?.updatedTime),
    updated_at: new Date().toISOString(),
    pools,
    last_error: '',
  };
}

function historyStateFresh(symbol) {
  const state = historyBySymbol.get(symbol);
  if (!state) return false;
  return HISTORY_INTERVALS.every((interval) => {
    const item = state[interval];
    return item?.official_response === true && isFresh(item.updated_at, HISTORY_STALE_MS);
  });
}
function riskStateFresh(symbol) {
  const item = riskBySymbol.get(symbol);
  return item?.official_response === true && isFresh(item.updated_at, RISK_STALE_MS);
}
function insuranceFresh() {
  return insuranceState.official_response === true && isFresh(insuranceState.updated_at, INSURANCE_STALE_MS);
}
function insuranceForSymbol(symbol) {
  if (!insuranceFresh()) {
    return { official_checked: false, mapped: false, pool: null };
  }
  const pool = insuranceState.pools.find((item) => item.symbols.includes(symbol)) || null;
  return {
    official_checked: true,
    mapped: Boolean(pool),
    pool: pool ? clone(pool) : null,
  };
}

async function refreshHistoryAndRisk(reason = 'scheduled', { missingOnly = false } = {}) {
  if (running) return await running;
  const task = (async () => {
    lastStartedAt = new Date().toISOString();
    totalBuilds += 1;
    const focus = focusTargets();
    if (!focus.focus_ready || focus.rows.length !== FOCUS_TARGET) {
      lastError = `${reason}:bybit_focus_not_ready:${focus.rows.length}/${FOCUS_TARGET}`;
      totalBuildFailures += 1;
      return false;
    }

    for (const target of focus.rows) {
      const symbol = target.symbol;

      if (!missingOnly || !historyStateFresh(symbol)) {
        const state = { ...(historyBySymbol.get(symbol) || {}) };
        for (const interval of HISTORY_INTERVALS) {
          const previous = state[interval];
          if (missingOnly && previous?.official_response === true && isFresh(previous.updated_at, HISTORY_STALE_MS)) continue;
          try {
            const limit = HISTORY_LIMIT[interval];
            const payload = await fetchJson(
              `/v5/market/open-interest?category=linear&symbol=${encodeURIComponent(symbol)}&intervalTime=${encodeURIComponent(interval)}&limit=${limit}`,
              `oi_history_${interval}`,
            );
            state[interval] = parseHistory(payload, symbol, interval);
          } catch (error) {
            if (!previous?.official_response) {
              state[interval] = {
                symbol,
                interval,
                official_response: false,
                official_empty: false,
                row_count: 0,
                rows: [],
                updated_at: null,
                error: String(error?.message || error).slice(0, 240),
              };
            }
          }
          await sleep(PER_REQUEST_GAP_MS);
        }
        historyBySymbol.set(symbol, state);
      }

      if (!missingOnly || !riskStateFresh(symbol)) {
        const previous = riskBySymbol.get(symbol);
        try {
          const payload = await fetchJson(
            `/v5/market/risk-limit?category=linear&symbol=${encodeURIComponent(symbol)}`,
            'risk_limit',
          );
          riskBySymbol.set(symbol, parseRisk(payload, symbol));
        } catch (error) {
          if (!previous?.official_response) {
            riskBySymbol.set(symbol, {
              symbol,
              official_response: false,
              official_empty: false,
              tier_count: 0,
              tiers: [],
              lowest_tier: null,
              updated_at: null,
              error: String(error?.message || error).slice(0, 240),
            });
          }
        }
        await sleep(PER_REQUEST_GAP_MS);
      }
    }

    lastFocusSignature = focusSignature(focus.rows);
    round += 1;
    responseCache.clear();
    lastCompletedAt = new Date().toISOString();

    const status = buildSnapshot({ includeRows: false, useCache: false });
    if (!status.history_risk_ready) {
      lastError = `${reason}:bybit_history_risk_coverage:${status.history_complete_symbols}/${FOCUS_TARGET}:risk=${status.risk_official_coverage_rows}/${FOCUS_TARGET}`;
      totalBuildFailures += 1;
      return false;
    }
    lastError = '';
    return true;
  })();

  running = task;
  try {
    return await task;
  } finally {
    if (running === task) running = null;
  }
}

async function refreshInsurance(reason = 'scheduled') {
  try {
    const payload = await fetchJson('/v5/market/insurance?coin=USDT', 'insurance');
    insuranceState = parseInsurance(payload);
    responseCache.clear();
    return true;
  } catch (error) {
    insuranceState = {
      ...insuranceState,
      last_error: `${reason}:${String(error?.message || error)}`.slice(0, 240),
    };
    return false;
  }
}

function scheduleRetry() {
  if (!started || retryTimer) return;
  retryTimer = setTimeout(async () => {
    retryTimer = null;
    await refreshHistoryAndRisk('startup_recovery_missing_only', { missingOnly: true }).catch(() => false);
    if (!insuranceFresh()) await refreshInsurance('startup_recovery').catch(() => false);
    if (!getBybitAdvancedStatsHealth().ready) scheduleRetry();
  }, STARTUP_RETRY_MS);
  retryTimer.unref?.();
}

export function startBybitAdvancedStatsScanner() {
  if (started || process.env.KAKA_DISABLE_BYBIT_ADVANCED_STATS === '1') return;
  started = true;

  startupTimer = setTimeout(async () => {
    await refreshInsurance('startup').catch(() => false);
    await refreshHistoryAndRisk('startup', { missingOnly: false }).catch(() => false);
    if (!getBybitAdvancedStatsHealth().ready) scheduleRetry();
  }, START_DELAY_MS);
  startupTimer.unref?.();

  fullInterval = setInterval(async () => {
    await refreshHistoryAndRisk('interval', { missingOnly: false }).catch(() => false);
  }, FULL_REFRESH_MS);
  fullInterval.unref?.();

  insuranceInterval = setInterval(() => refreshInsurance('interval').catch(() => false), INSURANCE_REFRESH_MS);
  insuranceInterval.unref?.();

  focusWatchInterval = setInterval(async () => {
    const focus = focusTargets();
    if (!focus.focus_ready || focus.rows.length !== FOCUS_TARGET) return;
    const signature = focusSignature(focus.rows);
    if (signature !== lastFocusSignature) {
      await refreshHistoryAndRisk('focus_change_missing_only', { missingOnly: true }).catch(() => false);
      if (!getBybitAdvancedStatsHealth().ready) scheduleRetry();
    }
  }, FOCUS_WATCH_MS);
  focusWatchInterval.unref?.();
}

function buildSnapshot({ includeRows = true, useCache = true } = {}) {
  const cacheKey = includeRows ? 'full' : 'meta';
  const cached = useCache ? responseCache.get(cacheKey) : null;
  if (cached && Date.now() - cached.at <= RESPONSE_CACHE_TTL_MS) {
    return { ...clone(cached.payload), cache_hit: true, cache_age_ms: Date.now() - cached.at };
  }

  const focus = focusTargets();
  const currentOi = marketLightCurrentOiMap();
  const rows = focus.rows.map((target) => {
    const histories = historyBySymbol.get(target.symbol) || {};
    const risk = riskBySymbol.get(target.symbol) || null;
    const insurance = insuranceForSymbol(target.symbol);
    const oi = currentOi.map.get(target.symbol) || null;
    return {
      provider: 'bybit',
      market_type: 'contract',
      quote_asset: 'USDT',
      symbol: target.symbol,
      base_asset: target.base_asset,
      focus_role: target.role,
      focus_slot: target.slot,
      current_open_interest: oi?.open_interest ?? null,
      current_open_interest_value: oi?.open_interest_value ?? null,
      current_open_interest_source: oi?.source ?? null,
      current_open_interest_source_time: oi?.source_time ?? null,
      open_interest_history: Object.fromEntries(HISTORY_INTERVALS.map((interval) => [
        interval,
        histories[interval]?.official_response === true ? clone(histories[interval]) : null,
      ])),
      risk_limit: risk?.official_response === true ? clone(risk) : null,
      insurance_official_checked: insurance.official_checked,
      insurance_mapped: insurance.mapped,
      insurance_pool: insurance.pool,
    };
  });

  const historyCoverageByInterval = Object.fromEntries(HISTORY_INTERVALS.map((interval) => [
    interval,
    rows.filter((row) => row.open_interest_history[interval] != null).length,
  ]));
  const historyNonemptyByInterval = Object.fromEntries(HISTORY_INTERVALS.map((interval) => [
    interval,
    rows.filter((row) => Number(row.open_interest_history[interval]?.row_count || 0) > 0).length,
  ]));
  const historyCompleteSymbols = rows.filter((row) =>
    HISTORY_INTERVALS.every((interval) => row.open_interest_history[interval] != null)
  ).length;
  const riskCoverage = rows.filter((row) => row.risk_limit != null).length;
  const riskNonempty = rows.filter((row) => Number(row.risk_limit?.tier_count || 0) > 0).length;
  const insuranceCoverage = rows.filter((row) => row.insurance_official_checked).length;
  const insuranceMapped = rows.filter((row) => row.insurance_mapped).length;
  const currentOiCoverage = rows.filter((row) => row.current_open_interest != null || row.current_open_interest_value != null).length;

  const historyRiskReady = focus.focus_ready &&
    rows.length === FOCUS_TARGET &&
    historyCompleteSymbols === FOCUS_TARGET &&
    riskCoverage === FOCUS_TARGET;
  const ready = historyRiskReady &&
    insuranceFresh() &&
    insuranceCoverage === FOCUS_TARGET &&
    currentOi.ready &&
    currentOiCoverage === FOCUS_TARGET;

  const payload = {
    ok: true,
    version: VERSION,
    source: 'render_shared_bybit_official_public_advanced_statistics',
    ready,
    history_risk_ready: historyRiskReady,
    focus_target: FOCUS_TARGET,
    focus_round: focus.focus_round,
    focus_symbols: focus.rows.map((row) => row.symbol),
    row_count: rows.length,
    core_target_count: rows.filter((row) => row.focus_role === 'core').length,
    current_open_interest_rows: currentOiCoverage,
    current_open_interest_reused_from_market_light: true,
    current_open_interest_additional_requests: 0,
    history_intervals: [...HISTORY_INTERVALS],
    history_official_coverage_by_interval: historyCoverageByInterval,
    history_nonempty_symbols_by_interval: historyNonemptyByInterval,
    history_complete_symbols: historyCompleteSymbols,
    risk_official_coverage_rows: riskCoverage,
    risk_nonempty_rows: riskNonempty,
    insurance_official_coverage_rows: insuranceCoverage,
    insurance_mapped_rows: insuranceMapped,
    insurance_pool_count: insuranceFresh() ? insuranceState.pools.length : 0,
    insurance_source_time: insuranceState.source_time,
    history_refresh_seconds: Math.round(FULL_REFRESH_MS / 1000),
    insurance_refresh_seconds: Math.round(INSURANCE_REFRESH_MS / 1000),
    per_request_gap_ms: PER_REQUEST_GAP_MS,
    full_cycle_history_request_cap: FOCUS_TARGET * HISTORY_INTERVALS.length,
    full_cycle_risk_request_cap: FOCUS_TARGET,
    full_cycle_insurance_request_cap: 1,
    full_cycle_total_request_cap: FOCUS_TARGET * HISTORY_INTERVALS.length + FOCUS_TARGET + 1,
    focus_change_missing_only: true,
    official_empty_history_counts_as_official_coverage_without_fabricating_rows: true,
    official_empty_risk_counts_as_official_coverage_without_fabricating_tiers: true,
    insurance_unmapped_stays_null_but_counts_as_official_checked: true,
    provider_request_governor_reused: true,
    custom_provider_governor_created: false,
    exchange_requests_started_by_user_read: 0,
    exchange_connections_started_by_user_read: 0,
    reads_scale_with_users: false,
    last_started_at: lastStartedAt,
    last_completed_at: lastCompletedAt,
    last_error: lastError,
    round,
    lanes: Object.fromEntries([...laneStats.entries()].map(([key, value]) => [key, { ...value }])),
    rows: includeRows ? rows : undefined,
  };
  if (!includeRows) delete payload.rows;
  responseCache.set(cacheKey, { at: Date.now(), payload });
  return { ...clone(payload), cache_hit: false, cache_age_ms: 0 };
}

export function getBybitAdvancedStatsHealth() {
  const snapshot = buildSnapshot({ includeRows: false });
  return {
    ...snapshot,
    enabled: started || process.env.KAKA_DISABLE_BYBIT_ADVANCED_STATS !== '1',
    mode: 'shared_bybit_focus15_oi_history_risk_limit_plus_usdt_insurance_pool',
    snapshot_endpoint: SNAPSHOT_ROUTE,
    health_endpoint: HEALTH_ROUTE,
    total_reads: totalReads,
    total_builds: totalBuilds,
    total_build_failures: totalBuildFailures,
    total_exchange_requests: totalExchangeRequests,
    total_exchange_successes: totalExchangeSuccesses,
    total_exchange_failures: totalExchangeFailures,
    running: Boolean(running),
    startup_recovery_missing_only: true,
    history_direct_official_intervals_no_derived_relabel: true,
    risk_limit_per_focus_symbol: true,
    insurance_one_shared_usdt_request: true,
  };
}

function sendJson(res, status, payload) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET, OPTIONS',
    'content-length': String(body.length),
  });
  res.end(body);
}

export async function handleBybitAdvancedStats(req, res, url) {
  if (![SNAPSHOT_ROUTE, HEALTH_ROUTE].includes(url.pathname)) return false;
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET, OPTIONS',
      'cache-control': 'no-store',
    });
    res.end();
    return true;
  }
  if (req.method !== 'GET') {
    sendJson(res, 405, { ok: false, version: VERSION, error: 'method_not_allowed' });
    return true;
  }
  totalReads += 1;
  if (url.pathname === HEALTH_ROUTE) {
    sendJson(res, 200, getBybitAdvancedStatsHealth());
    return true;
  }
  sendJson(res, 200, buildSnapshot({ includeRows: true }));
  return true;
}

export const __bybitAdvancedTest = Object.freeze({
  parseHistory,
  parseRisk,
  parseInsurance,
});
