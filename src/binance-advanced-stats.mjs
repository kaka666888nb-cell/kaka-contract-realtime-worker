import { getContractFocusPoolInternalSnapshot } from './contract-focus-pool.mjs';
import {
  fetchBinancePublicRestRelayJson,
  getBinanceContractKlineRelayHealth,
} from './binance-contract-kline-relay.mjs';

const VERSION = '650.8.15.5';
const SNAPSHOT_ROUTE = '/api/binance-advanced/current-snapshot';
const HEALTH_ROUTE = '/api/binance-advanced/health';
const FUTURES_BASE = 'https://fapi.binance.com';
const FOCUS_TARGET = 15;
const CORE_TARGET = 10;

const START_DELAY_MS = Math.max(2_000, Number(process.env.KAKA_BINANCE_ADVANCED_START_DELAY_MS || 10_000));
const STARTUP_RETRY_MS = Math.max(10_000, Number(process.env.KAKA_BINANCE_ADVANCED_STARTUP_RETRY_MS || 15_000));
const REFRESH_MS = Math.max(2 * 60_000, Number(process.env.KAKA_BINANCE_ADVANCED_REFRESH_MS || 5 * 60_000));
const RESPONSE_CACHE_TTL_MS = Math.max(3_000, Number(process.env.KAKA_BINANCE_ADVANCED_RESPONSE_CACHE_TTL_MS || 20_000));
const OI_STALE_MS = Math.max(5 * 60_000, Number(process.env.KAKA_BINANCE_ADVANCED_OI_STALE_MS || 12 * 60_000));
const ADL_STALE_MS = Math.max(35 * 60_000, Number(process.env.KAKA_BINANCE_ADVANCED_ADL_STALE_MS || 55 * 60_000));
const ADL_REFRESH_MS = Math.max(30 * 60_000, Number(process.env.KAKA_BINANCE_ADVANCED_ADL_REFRESH_MS || 30 * 60_000));
const ADL_TARGET_RECOVERY_COOLDOWN_MS = Math.max(5 * 60_000, Number(process.env.KAKA_BINANCE_ADVANCED_ADL_TARGET_RECOVERY_COOLDOWN_MS || 5 * 60_000));
const ADL_TARGET_RECOVERY_MAX_PER_CYCLE = 1;
const DYNAMIC_FOCUS_WATCH_MS = Math.max(10_000, Number(process.env.KAKA_BINANCE_ADVANCED_FOCUS_WATCH_MS || 15_000));
const RELAY_LANE = 'critical';
const RELAY_PRIORITY = 18;

let started = false;
let running = null;
let startTimer = null;
let recoveryTimer = null;
let refreshInterval = null;
let focusWatchInterval = null;
let round = 0;
let totalReads = 0;
let totalBuilds = 0;
let totalFailures = 0;
let lastStartedAt = null;
let lastCompletedAt = null;
let lastError = '';
let lastAdlStartedAt = null;
let lastAdlCompletedAt = null;
let lastAdlError = '';
let lastOiStartedAt = null;
let lastOiCompletedAt = null;
let lastOiError = '';
let lastOiRecoveryCandidateCount = 0;
let lastAdlAllSymbolsRowCount = 0;
let lastAdlRecoveryCandidateCount = 0;
let lastAdlTargetRecoveryAttempted = 0;
let lastAdlTargetRecoverySucceeded = 0;
const adlTargetProbeAt = new Map();
const adlTargetProbeErrors = new Map();

const oiRows = new Map();
const adlRows = new Map();
const adlOfficialUnavailable = new Map();
const responseCache = new Map();
const laneStats = new Map();

function finite(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
function positive(value) {
  const n = finite(value);
  return n != null && n > 0 ? n : null;
}
function compact(raw) {
  return String(raw || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}
function isoMs(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? new Date(n).toISOString() : null;
}
function clone(value) {
  return value == null ? value : JSON.parse(JSON.stringify(value));
}
function isFresh(updatedAt, maxAgeMs) {
  const ms = Date.parse(String(updatedAt || ''));
  return Number.isFinite(ms) && Date.now() - ms <= maxAgeMs;
}
function setLane(name, patch) {
  const current = laneStats.get(name) || {
    name,
    attempts: 0,
    successes: 0,
    failures: 0,
    last_started_at: null,
    last_completed_at: null,
    last_error: '',
    last_rows: 0,
  };
  Object.assign(current, patch);
  laneStats.set(name, current);
}

function binanceFocusTargets() {
  const focus = getContractFocusPoolInternalSnapshot();
  const rows = (Array.isArray(focus?.rows) ? focus.rows : [])
    .filter((row) => row?.provider === 'binance' && row?.market_type === 'contract')
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

async function relayJson(url, laneName, { deferWhenBusy = true, priority = RELAY_PRIORITY } = {}) {
  const startedAt = Date.now();
  setLane(laneName, {
    attempts: Number(laneStats.get(laneName)?.attempts || 0) + 1,
    last_started_at: new Date(startedAt).toISOString(),
  });
  try {
    const payload = await fetchBinancePublicRestRelayJson(url, {
      source: `step993:${laneName}`,
      lane: RELAY_LANE,
      priority,
      deferWhenBusy,
    });
    setLane(laneName, {
      successes: Number(laneStats.get(laneName)?.successes || 0) + 1,
      last_completed_at: new Date().toISOString(),
      last_error: '',
    });
    return payload;
  } catch (error) {
    setLane(laneName, {
      failures: Number(laneStats.get(laneName)?.failures || 0) + 1,
      last_completed_at: new Date().toISOString(),
      last_error: String(error?.message || error),
    });
    throw error;
  }
}

function parseOpenInterest(payload, target) {
  if (!payload || Array.isArray(payload)) return null;
  const symbol = compact(payload?.symbol) || target.symbol;
  const openInterest = positive(payload?.openInterest);
  const timeMs = Number(payload?.time || 0) || null;
  if (symbol !== target.symbol || openInterest == null || timeMs == null) return null;
  return {
    provider: 'binance',
    market_type: 'contract',
    quote_asset: 'USDT',
    symbol,
    base_asset: target.base_asset,
    focus_role: target.role,
    focus_slot: target.slot,
    open_interest: openInterest,
    open_interest_time_ms: timeMs,
    open_interest_time: isoMs(timeMs),
    official_endpoint: '/fapi/v1/openInterest',
    source: 'binance_official_open_interest_supabase_edge_relay',
    updated_at: new Date().toISOString(),
  };
}

function normalizeAdlRisk(raw) {
  const value = String(raw || '').trim().toLowerCase();
  return ['high', 'medium', 'low'].includes(value) ? value : '';
}

function parseAdlRows(payload) {
  const list = Array.isArray(payload) ? payload : payload && typeof payload === 'object' ? [payload] : [];
  const out = new Map();
  for (const row of list) {
    const symbol = compact(row?.symbol);
    const adlRisk = normalizeAdlRisk(row?.adlRisk);
    const updateTimeMs = Number(row?.updateTime || 0) || null;
    if (!symbol || !adlRisk || updateTimeMs == null) continue;
    out.set(symbol, {
      provider: 'binance',
      market_type: 'contract',
      quote_asset: 'USDT',
      symbol,
      adl_risk: adlRisk,
      adl_update_time_ms: updateTimeMs,
      adl_update_time: isoMs(updateTimeMs),
      official_endpoint: '/fapi/v1/symbolAdlRisk',
      official_update_interval_minutes: 30,
      source: 'binance_official_symbol_adl_risk_supabase_edge_relay',
      updated_at: new Date().toISOString(),
    });
  }
  return out;
}

function classifyExactAdlPayload(payload, targetSymbol) {
  const symbol = compact(targetSymbol);
  const parsed = parseAdlRows(payload);
  const row = parsed.get(symbol) || null;
  if (row) return { status: 'rated', row, official_unrated: null };
  const first = Array.isArray(payload) ? payload?.[0] : payload;
  const payloadType = Array.isArray(payload) ? 'array' : payload && typeof payload === 'object' ? 'object' : typeof payload;
  const rawSymbol = compact(first?.symbol);
  const rawRisk = String(first?.adlRisk ?? '').trim().toLowerCase();
  return {
    status: 'official_unrated',
    row: null,
    official_unrated: {
      provider: 'binance',
      market_type: 'contract',
      quote_asset: 'USDT',
      symbol,
      status: 'official_unrated',
      official_endpoint: '/fapi/v1/symbolAdlRisk',
      source: 'binance_official_symbol_adl_risk_supabase_edge_relay',
      payload_type: payloadType,
      payload_symbol: rawSymbol || null,
      payload_adl_risk_raw: rawRisk || null,
      observed_at: new Date().toISOString(),
    },
  };
}

async function refreshAdl(targets, reason) {
  const now = Date.now();
  const lastCompletedMs = Date.parse(String(lastAdlCompletedAt || ''));
  const allSymbolsDue = !Number.isFinite(lastCompletedMs) || now - lastCompletedMs >= ADL_REFRESH_MS;
  let allSymbolsOk = true;

  // Step993.3: one successful all-symbol response is the official 30-minute
  // snapshot. A focus symbol omitted by that response is not fabricated. It is
  // eligible for one bounded per-symbol confirmation probe through the same
  // authenticated Edge relay.
  if (allSymbolsDue) {
    lastAdlStartedAt = new Date().toISOString();
    lastAdlError = '';
    try {
      const payload = await relayJson(`${FUTURES_BASE}/fapi/v1/symbolAdlRisk`, 'adl_risk_all_symbols');
      const parsed = parseAdlRows(payload);
      lastAdlAllSymbolsRowCount = parsed.size;

      // Step1004.1.3: the endpoint is already one official all-symbol request.
      // Retain every valid symbol from that same response instead of discarding
      // non-focus rows. Dynamic hot5 changes can then reuse the already-fetched
      // official 30-minute ADL snapshot with zero additional Binance requests.
      for (const [symbol, row] of parsed.entries()) {
        adlRows.set(symbol, {
          ...row,
          base_asset: symbol.endsWith('USDT') ? symbol.slice(0, -4) : null,
          focus_role: null,
          focus_slot: null,
        });
      }
      for (const target of targets) {
        if (!parsed.has(target.symbol)) continue;
        adlOfficialUnavailable.delete(target.symbol);
        adlTargetProbeErrors.delete(target.symbol);
      }
      setLane('adl_risk_all_symbols', { last_rows: targets.filter((target) => parsed.has(target.symbol)).length });
      lastAdlCompletedAt = new Date().toISOString();
      responseCache.clear();
    } catch (error) {
      allSymbolsOk = false;
      lastAdlError = String(error?.message || error);
    }
  }

  const missingTargets = targets.filter((target) => {
    const row = adlRows.get(target.symbol);
    const unavailable = adlOfficialUnavailable.get(target.symbol);
    const ratedFresh = row && isFresh(row.updated_at, ADL_STALE_MS);
    const unavailableFresh = unavailable && isFresh(unavailable.observed_at, ADL_STALE_MS);
    return !ratedFresh && !unavailableFresh;
  });
  lastAdlRecoveryCandidateCount = missingTargets.length;
  lastAdlTargetRecoveryAttempted = 0;
  lastAdlTargetRecoverySucceeded = 0;

  const hasSuccessfulAllSymbolsSnapshot = Number.isFinite(Date.parse(String(lastAdlCompletedAt || '')));
  const probeTargets = (hasSuccessfulAllSymbolsSnapshot ? missingTargets : [])
    .filter((target) => now - Number(adlTargetProbeAt.get(target.symbol) || 0) >= ADL_TARGET_RECOVERY_COOLDOWN_MS)
    .slice(0, ADL_TARGET_RECOVERY_MAX_PER_CYCLE);
  for (const target of probeTargets) {
    adlTargetProbeAt.set(target.symbol, now);
    lastAdlTargetRecoveryAttempted += 1;
    try {
      const query = new URLSearchParams({ symbol: target.symbol });
      const payload = await relayJson(
        `${FUTURES_BASE}/fapi/v1/symbolAdlRisk?${query.toString()}`,
        'adl_risk_missing_symbol',
        { deferWhenBusy: false, priority: RELAY_PRIORITY + 2 },
      );
      const classified = classifyExactAdlPayload(payload, target.symbol);
      if (classified.status === 'rated') {
        adlRows.set(target.symbol, {
          ...classified.row,
          base_asset: target.base_asset,
          focus_role: target.role,
          focus_slot: target.slot,
        });
        adlOfficialUnavailable.delete(target.symbol);
      } else {
        // The relay/Edge request succeeded, but Binance returned no valid
        // high/medium/low rating for this exact symbol. This is an official
        // availability result, not a transport failure. Keep adl_risk=null and
        // record the limitation explicitly instead of retrying/fabricating.
        adlOfficialUnavailable.set(target.symbol, classified.official_unrated);
      }
      adlTargetProbeErrors.delete(target.symbol);
      lastAdlTargetRecoverySucceeded += 1;
    } catch (error) {
      adlTargetProbeErrors.set(target.symbol, String(error?.message || error));
      lastAdlError = String(error?.message || error);
    }
  }
  setLane('adl_risk_missing_symbol', { last_rows: lastAdlTargetRecoverySucceeded });
  responseCache.clear();
  return allSymbolsOk || lastAdlTargetRecoverySucceeded > 0 || missingTargets.length === 0;
}

async function refreshOpenInterest(targets, reason) {
  lastOiStartedAt = new Date().toISOString();
  lastOiError = '';
  let successCount = 0;
  const errors = [];

  // Step993.1: during cold-start recovery retry only the missing/stale OI rows.
  // The first startup and normal five-minute interval still refresh the full
  // focus15. This prevents already-good early symbols from repeatedly taking
  // the single Binance relay slot ahead of a later missing symbol.
  const scanTargets = reason === 'startup_recovery'
    ? targets.filter((target) => {
        const row = oiRows.get(target.symbol);
        return !row || !isFresh(row.updated_at, OI_STALE_MS);
      })
    : targets;
  lastOiRecoveryCandidateCount = scanTargets.length;

  for (const target of scanTargets) {
    try {
      const query = new URLSearchParams({ symbol: target.symbol });
      const payload = await relayJson(`${FUTURES_BASE}/fapi/v1/openInterest?${query.toString()}`, 'open_interest_current');
      const parsed = parseOpenInterest(payload, target);
      if (!parsed) throw new Error(`binance_oi_payload_invalid:${target.symbol}`);
      oiRows.set(target.symbol, parsed);
      successCount += 1;
    } catch (error) {
      errors.push(`${target.symbol}:${String(error?.message || error)}`);
      // Keep a still-fresh last-good row. A transient refresh failure must not erase
      // verified official OI; normal freshness rules will expire it later.
    }
  }
  setLane('open_interest_current', { last_rows: successCount });
  lastOiCompletedAt = new Date().toISOString();
  lastOiError = errors.slice(0, 6).join('|');
  responseCache.clear();
  return successCount > 0 || scanTargets.length === 0;
}

function startupReady() {
  const focus = binanceFocusTargets();
  if (!focus.focus_ready || focus.rows.length !== FOCUS_TARGET) return false;
  const oiReady = focus.rows.every((target) => {
    const row = oiRows.get(target.symbol);
    return row && isFresh(row.updated_at, OI_STALE_MS);
  });
  const adlReady = focus.rows.every((target) => {
    const row = adlRows.get(target.symbol);
    const unavailable = adlOfficialUnavailable.get(target.symbol);
    return (row && isFresh(row.updated_at, ADL_STALE_MS)) ||
      (unavailable && isFresh(unavailable.observed_at, ADL_STALE_MS));
  });
  return oiReady && adlReady;
}

async function refreshCycle(reason = 'interval') {
  if (running) return running;
  const promise = (async () => {
    totalBuilds += 1;
    lastStartedAt = new Date().toISOString();
    lastError = '';
    try {
      const focus = binanceFocusTargets();
      if (!focus.focus_ready || focus.rows.length !== FOCUS_TARGET) {
        throw new Error(`binance_focus_not_ready:${focus.rows.length}/${FOCUS_TARGET}`);
      }
      await refreshAdl(focus.rows, reason);
      await refreshOpenInterest(focus.rows, reason);
      round += 1;
      lastCompletedAt = new Date().toISOString();
      const ready = startupReady();
      if (!ready) {
        const missingOi = focus.rows.filter((target) => !oiRows.get(target.symbol) || !isFresh(oiRows.get(target.symbol)?.updated_at, OI_STALE_MS)).length;
        const missingAdl = focus.rows.filter((target) => { const row = adlRows.get(target.symbol); const unavailable = adlOfficialUnavailable.get(target.symbol); return !((row && isFresh(row.updated_at, ADL_STALE_MS)) || (unavailable && isFresh(unavailable.observed_at, ADL_STALE_MS))); }).length;
        lastError = `binance_advanced_not_ready:oi_missing=${missingOi};adl_missing=${missingAdl}`;
      }
      responseCache.clear();
      return ready;
    } catch (error) {
      totalFailures += 1;
      lastError = String(error?.message || error);
      lastCompletedAt = new Date().toISOString();
      responseCache.clear();
      return false;
    }
  })();
  running = promise;
  try {
    return await promise;
  } finally {
    if (running === promise) running = null;
  }
}

function scheduleStartupRecovery() {
  if (startupReady() || recoveryTimer) return;
  recoveryTimer = setTimeout(async () => {
    recoveryTimer = null;
    await refreshCycle('startup_recovery').catch(() => false);
    if (!startupReady()) scheduleStartupRecovery();
  }, STARTUP_RETRY_MS);
  recoveryTimer.unref?.();
}

export function startBinanceAdvancedStatsScanner() {
  if (started || process.env.KAKA_DISABLE_BINANCE_ADVANCED_STATS === '1') return;
  started = true;
  startTimer = setTimeout(async () => {
    await refreshCycle('startup').catch(() => false);
    if (!startupReady()) scheduleStartupRecovery();
  }, START_DELAY_MS);
  startTimer.unref?.();

  refreshInterval = setInterval(async () => {
    const reason = startupReady() ? 'interval' : 'startup_recovery';
    await refreshCycle(reason).catch(() => false);
    if (!startupReady()) scheduleStartupRecovery();
  }, REFRESH_MS);
  refreshInterval.unref?.();

  // A focus hot5 can change independently of the five-minute advanced-stat
  // interval. Detect missing current-focus official fields quickly and recover
  // only those rows through the existing relay/governor. This is backend-only
  // and never triggered by a user read.
  focusWatchInterval = setInterval(async () => {
    if (startupReady()) return;
    await refreshCycle('startup_recovery').catch(() => false);
    if (!startupReady()) scheduleStartupRecovery();
  }, DYNAMIC_FOCUS_WATCH_MS);
  focusWatchInterval.unref?.();
}

function snapshotPayload({ includeRows = true } = {}) {
  const cacheKey = includeRows ? 'full' : 'meta';
  const cached = responseCache.get(cacheKey);
  if (cached && Date.now() - cached.at <= RESPONSE_CACHE_TTL_MS) {
    return { ...clone(cached.payload), cache_hit: true, cache_age_ms: Date.now() - cached.at };
  }

  const focus = binanceFocusTargets();
  const targets = focus.rows;
  const targetSymbols = targets.map((row) => row.symbol);
  const rows = targets.map((target) => {
    const oi = oiRows.get(target.symbol);
    const adl = adlRows.get(target.symbol);
    const unavailable = adlOfficialUnavailable.get(target.symbol);
    const adlRatedFresh = adl && isFresh(adl.updated_at, ADL_STALE_MS);
    const adlUnavailableFresh = unavailable && isFresh(unavailable.observed_at, ADL_STALE_MS);
    return {
      provider: 'binance',
      market_type: 'contract',
      quote_asset: 'USDT',
      symbol: target.symbol,
      base_asset: target.base_asset,
      focus_role: target.role,
      focus_slot: target.slot,
      open_interest: oi && isFresh(oi.updated_at, OI_STALE_MS) ? oi.open_interest : null,
      open_interest_time_ms: oi && isFresh(oi.updated_at, OI_STALE_MS) ? oi.open_interest_time_ms : null,
      open_interest_time: oi && isFresh(oi.updated_at, OI_STALE_MS) ? oi.open_interest_time : null,
      adl_risk: adlRatedFresh ? adl.adl_risk : null,
      adl_update_time_ms: adlRatedFresh ? adl.adl_update_time_ms : null,
      adl_update_time: adlRatedFresh ? adl.adl_update_time : null,
      open_interest_source: oi?.source || null,
      adl_risk_source: adlRatedFresh ? adl?.source || null : adlUnavailableFresh ? unavailable?.source || null : null,
      adl_availability: adlRatedFresh ? 'rated' : adlUnavailableFresh ? 'official_unrated' : 'missing',
      adl_officially_unrated: Boolean(adlUnavailableFresh),
      adl_official_unrated_observed_at: adlUnavailableFresh ? unavailable?.observed_at || null : null,
    };
  });
  const oiReadyRows = rows.filter((row) => row.open_interest != null).length;
  const adlReadyRows = rows.filter((row) => row.adl_risk != null).length;
  const adlOfficialUnratedRows = rows.filter((row) => row.adl_officially_unrated === true).length;
  const adlOfficialCoverageRows = adlReadyRows + adlOfficialUnratedRows;
  const coreRows = rows.filter((row) => row.focus_role === 'core');
  const hotRows = rows.filter((row) => row.focus_role === 'hot');
  const coreOiRows = coreRows.filter((row) => row.open_interest != null).length;
  const coreAdlRows = coreRows.filter((row) => row.adl_risk != null).length;
  const coreAdlOfficialCoverageRows = coreRows.filter((row) => row.adl_risk != null || row.adl_officially_unrated === true).length;
  const hotOiRows = hotRows.filter((row) => row.open_interest != null).length;
  const hotAdlRows = hotRows.filter((row) => row.adl_risk != null).length;
  const hotAdlOfficialCoverageRows = hotRows.filter((row) => row.adl_risk != null || row.adl_officially_unrated === true).length;
  const relay = getBinanceContractKlineRelayHealth();
  const missingOpenInterestSymbols = rows.filter((row) => row.open_interest == null).map((row) => row.symbol);
  const missingAdlSymbols = rows.filter((row) => row.adl_risk == null && row.adl_officially_unrated !== true).map((row) => row.symbol);
  const officialUnratedAdlSymbols = rows.filter((row) => row.adl_officially_unrated === true).map((row) => row.symbol);
  const adlProbeErrors = Object.fromEntries(missingAdlSymbols.map((symbol) => [symbol, adlTargetProbeErrors.get(symbol) || '']).filter((entry) => entry[1]));

  const payload = {
    ok: true,
    version: VERSION,
    source: 'render_shared_binance_official_focus15_advanced_risk_statistics',
    ready: focus.focus_ready && targetSymbols.length === FOCUS_TARGET &&
      coreRows.length === CORE_TARGET && coreOiRows === CORE_TARGET && coreAdlOfficialCoverageRows === CORE_TARGET &&
      oiReadyRows === FOCUS_TARGET && adlOfficialCoverageRows === FOCUS_TARGET,
    focus_target: FOCUS_TARGET,
    focus_round: focus.focus_round,
    focus_symbols: targetSymbols,
    row_count: rows.length,
    core_target_count: coreRows.length,
    hot_target_count: hotRows.length,
    open_interest_rows: oiReadyRows,
    adl_risk_rows: adlReadyRows,
    adl_official_unrated_rows: adlOfficialUnratedRows,
    adl_official_coverage_rows: adlOfficialCoverageRows,
    core_open_interest_rows: coreOiRows,
    core_adl_risk_rows: coreAdlRows,
    core_adl_official_coverage_rows: coreAdlOfficialCoverageRows,
    hot_open_interest_rows: hotOiRows,
    hot_adl_risk_rows: hotAdlRows,
    hot_adl_official_coverage_rows: hotAdlOfficialCoverageRows,
    missing_open_interest_symbols: missingOpenInterestSymbols,
    missing_adl_symbols: missingAdlSymbols,
    official_unrated_adl_symbols: officialUnratedAdlSymbols,
    last_oi_recovery_candidate_count: lastOiRecoveryCandidateCount,
    last_adl_all_symbols_row_count: lastAdlAllSymbolsRowCount,
    last_adl_recovery_candidate_count: lastAdlRecoveryCandidateCount,
    last_adl_target_recovery_attempted: lastAdlTargetRecoveryAttempted,
    last_adl_target_recovery_succeeded: lastAdlTargetRecoverySucceeded,
    adl_target_probe_errors: adlProbeErrors,
    adl_target_requires_successful_all_symbols_snapshot: true,
    adl_target_queue_wait_enabled: true,
    adl_target_defer_when_busy: false,
    adl_all_symbols_response_cache_retains_nonfocus_symbols: true,
    adl_dynamic_hot_focus_reuses_cached_all_symbols_snapshot: true,
    dynamic_focus_watch_seconds: Math.round(DYNAMIC_FOCUS_WATCH_MS / 1000),
    dynamic_focus_missing_only_recovery: true,
    dynamic_focus_recovery_user_read_triggered: false,
    adl_target_priority: RELAY_PRIORITY + 2,
    adl_target_recovery_per_cycle_max: ADL_TARGET_RECOVERY_MAX_PER_CYCLE,
    adl_target_probe_cooldown_seconds: Math.round(ADL_TARGET_RECOVERY_COOLDOWN_MS / 1000),
    official_endpoints: {
      open_interest_current: '/fapi/v1/openInterest',
      adl_risk: '/fapi/v1/symbolAdlRisk',
    },
    official_rate_policy: {
      open_interest_current: 'IP weight 1 per symbol; shared focus15 collector every 5 minutes',
      adl_risk: 'IP weight 1; one all-symbol shared request; Binance publishes rating updates every 30 minutes',
    },
    official_semantics: {
      open_interest_current: 'Binance official present open interest for the exact USDⓈ-M symbol',
      adl_risk: 'Binance official symbol-level automatic deleveraging risk rating: high/medium/low when published; a successful exact-symbol response without a valid rating is exposed as official_unrated with adl_risk=null',
      adl_risk_update_interval_minutes: 30,
    },
    official_stats_separate_from_derived: true,
    no_cross_provider_substitution: true,
    no_cross_quote_substitution: true,
    missing_stays_null: true,
    official_unrated_stays_null: true,
    transport: 'authenticated_supabase_edge_public_rest_relay_only',
    edge_relay_only: true,
    render_direct_binance_rest: false,
    automatic_render_rest_enabled: false,
    relay_protocol_version: relay?.edge_protocol_version || null,
    relay_edge_health_reachable: relay?.edge_health_reachable === true,
    relay_validation_completed: relay?.validation_completed === true,
    relay_active: relay?.active === true,
    relay_requests_started: Number(relay?.requests_started || 0),
    relay_requests_succeeded: Number(relay?.requests_succeeded || 0),
    relay_requests_failed: Number(relay?.requests_failed || 0),
    relay_queue_depth: Number(relay?.queue_depth || 0),
    collector_only_exchange_requests: true,
    exchange_requests_started_by_user_read: 0,
    exchange_connections_started_by_user_read: 0,
    user_reads_trigger_collector: false,
    reads_scale_with_users: false,
    refresh_seconds: Math.round(REFRESH_MS / 1000),
    oi_stale_seconds: Math.round(OI_STALE_MS / 1000),
    adl_stale_seconds: Math.round(ADL_STALE_MS / 1000),
    adl_official_update_interval_minutes: 30,
    last_started_at: lastStartedAt,
    last_completed_at: lastCompletedAt,
    last_error: lastError,
    last_oi_started_at: lastOiStartedAt,
    last_oi_completed_at: lastOiCompletedAt,
    last_oi_error: lastOiError,
    last_adl_started_at: lastAdlStartedAt,
    last_adl_completed_at: lastAdlCompletedAt,
    last_adl_error: lastAdlError,
    lane_stats: Object.fromEntries([...laneStats.entries()].map(([key, value]) => [key, { ...value }])),
    round,
    rows: includeRows ? rows.map(clone) : [],
    timestamp_ms: Date.now(),
  };
  responseCache.set(cacheKey, { at: Date.now(), payload });
  return { ...clone(payload), cache_hit: false, cache_age_ms: 0 };
}

export function getBinanceAdvancedStatsHealth() {
  const snapshot = snapshotPayload({ includeRows: false });
  return {
    ...snapshot,
    enabled: started || process.env.KAKA_DISABLE_BINANCE_ADVANCED_STATS !== '1',
    mode: 'shared_focus15_official_open_interest_plus_adl_risk',
    snapshot_endpoint: SNAPSHOT_ROUTE,
    health_endpoint: HEALTH_ROUTE,
    total_reads: totalReads,
    total_builds: totalBuilds,
    total_failures: totalFailures,
    running: Boolean(running),
    startup_lock_release_after_completion: true,
    startup_recovery_until_focus_and_official_stats_ready: true,
    transient_refresh_preserves_last_good_until_stale: true,
    adl_all_symbols_single_request: true,
    oi_startup_recovery_missing_only: true,
    adl_startup_recovery_does_not_repeat_all_symbols_inside_30m: true,
    adl_missing_symbol_targeted_recovery: true,
    adl_successful_exact_symbol_empty_means_official_unrated: true,
    adl_target_requires_successful_all_symbols_snapshot: true,
    adl_target_queue_wait_enabled: true,
    adl_target_defer_when_busy: false,
    open_interest_requests_per_refresh_max: FOCUS_TARGET,
    adl_all_symbols_requests_per_30m_max: 1,
    adl_missing_symbol_recovery_requests_per_cycle_max: ADL_TARGET_RECOVERY_MAX_PER_CYCLE,
    adl_missing_symbol_probe_cooldown_seconds: Math.round(ADL_TARGET_RECOVERY_COOLDOWN_MS / 1000),
    adl_requests_per_30m_max: 1 + Math.ceil(ADL_REFRESH_MS / ADL_TARGET_RECOVERY_COOLDOWN_MS) * ADL_TARGET_RECOVERY_MAX_PER_CYCLE,
    response_cache_ttl_seconds: RESPONSE_CACHE_TTL_MS / 1000,
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

export async function handleBinanceAdvancedStats(req, res, url) {
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
    sendJson(res, 200, getBinanceAdvancedStatsHealth());
    return true;
  }
  sendJson(res, 200, snapshotPayload({ includeRows: true }));
  return true;
}

export const __binanceAdvancedTest = Object.freeze({
  parseOpenInterest,
  parseAdlRows,
  classifyExactAdlPayload,
  normalizeAdlRisk,
});
