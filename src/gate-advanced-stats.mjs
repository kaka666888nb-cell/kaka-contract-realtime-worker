import { getContractFocusPoolInternalSnapshot } from './contract-focus-pool.mjs';

const VERSION = '650.8.15.1';
const SNAPSHOT_ROUTE = '/api/gate-advanced/current-snapshot';
const HEALTH_ROUTE = '/api/gate-advanced/health';
const BASES = Object.freeze([
  'https://fx-api.gateio.ws/api/v4',
  'https://api.gateio.ws/api/v4',
]);

const START_DELAY_MS = Math.max(2_000, Number(process.env.KAKA_GATE_ADVANCED_START_DELAY_MS || 9_000));
const STARTUP_RETRY_MS = Math.max(10_000, Number(process.env.KAKA_GATE_ADVANCED_STARTUP_RETRY_MS || 15_000));
const FOCUS_REFRESH_MS = Math.max(2 * 60_000, Number(process.env.KAKA_GATE_ADVANCED_FOCUS_REFRESH_MS || 5 * 60_000));
const INSURANCE_REFRESH_MS = Math.max(2 * 60_000, Number(process.env.KAKA_GATE_ADVANCED_INSURANCE_REFRESH_MS || 5 * 60_000));
const RESPONSE_CACHE_TTL_MS = Math.max(3_000, Number(process.env.KAKA_GATE_ADVANCED_RESPONSE_CACHE_TTL_MS || 20_000));
const STALE_MS = Math.max(5 * 60_000, Number(process.env.KAKA_GATE_ADVANCED_STALE_MS || 12 * 60_000));
const PER_SYMBOL_GAP_MS = Math.max(220, Number(process.env.KAKA_GATE_ADVANCED_PER_SYMBOL_GAP_MS || 260));
const FOCUS_TARGET = 15;

let started = false;
let focusRunning = null;
let insuranceRunning = null;
let focusTimer = null;
let insuranceTimer = null;
let focusInterval = null;
let insuranceInterval = null;
let round = 0;
let totalReads = 0;
let responseCacheHits = 0;
let responseCacheMisses = 0;
let lastFocusStartedAt = null;
let lastFocusCompletedAt = null;
let lastFocusError = '';
let lastInsuranceStartedAt = null;
let lastInsuranceCompletedAt = null;
let lastInsuranceError = '';
let totalFocusBuilds = 0;
let totalFocusFailures = 0;
let totalInsuranceBuilds = 0;
let totalInsuranceFailures = 0;

let contractRows = new Map();
let insuranceRows = [];
const responseCache = new Map();
const laneStats = new Map();

function finite(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
function integer(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}
function compact(raw) {
  return String(raw || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}
function clone(value) {
  return value == null ? value : JSON.parse(JSON.stringify(value));
}
function isoSeconds(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? new Date(n * 1000).toISOString() : null;
}
function sleep(ms) {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    timer.unref?.();
  });
}
function gateNativeSymbol(displaySymbol) {
  const symbol = compact(displaySymbol);
  if (!symbol.endsWith('USDT') || symbol.length <= 4) return '';
  return `${symbol.slice(0, -4)}_USDT`;
}
function setLane(name, patch) {
  const current = laneStats.get(name) || {
    name,
    attempts: 0,
    successes: 0,
    failures: 0,
    last_started_at: null,
    last_completed_at: null,
    last_status: 0,
    last_rows: 0,
    last_error: '',
  };
  Object.assign(current, patch);
  laneStats.set(name, current);
}

async function fetchGate(path, { lane = 'unknown', timeoutMs = 18_000 } = {}) {
  setLane(lane, {
    attempts: Number(laneStats.get(lane)?.attempts || 0) + 1,
    last_started_at: new Date().toISOString(),
  });
  let lastError = null;
  for (const base of BASES) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    timer.unref?.();
    try {
      const response = await fetch(`${base}${path}`, {
        headers: {
          accept: 'application/json',
          'user-agent': 'KakaWeb3/650.8.15.94 gate-advanced-shared',
        },
        signal: controller.signal,
      });
      const text = await response.text();
      let payload = null;
      try { payload = JSON.parse(text); } catch (_) {}
      if (!response.ok) throw new Error(`gate_http_${response.status}:${lane}:${new URL(base).hostname}`);
      if (payload && !Array.isArray(payload) && payload?.label) {
        throw new Error(`gate_label_${String(payload.label)}:${String(payload.message || '')}:${lane}`);
      }
      setLane(lane, {
        successes: Number(laneStats.get(lane)?.successes || 0) + 1,
        last_status: response.status,
        last_completed_at: new Date().toISOString(),
        last_error: '',
      });
      clearTimeout(timer);
      return payload;
    } catch (error) {
      clearTimeout(timer);
      lastError = error;
    }
  }
  setLane(lane, {
    failures: Number(laneStats.get(lane)?.failures || 0) + 1,
    last_completed_at: new Date().toISOString(),
    last_error: String(lastError?.message || lastError || 'gate_fetch_failed'),
  });
  throw lastError || new Error(`gate_fetch_failed:${lane}`);
}

function gateFocusTargets() {
  const focus = getContractFocusPoolInternalSnapshot();
  const rows = (Array.isArray(focus?.rows) ? focus.rows : [])
    .filter((row) => row?.provider === 'gate' && row?.market_type === 'contract')
    .map((row) => ({
      symbol: compact(row?.symbol),
      native_symbol: gateNativeSymbol(row?.symbol),
      base_asset: compact(row?.base_asset),
      role: String(row?.role || ''),
      slot: Number(row?.slot || 0),
    }))
    .filter((row) => row.symbol.endsWith('USDT') && row.native_symbol);
  const unique = [];
  const seen = new Set();
  for (const row of rows) {
    if (seen.has(row.symbol)) continue;
    seen.add(row.symbol);
    unique.push(row);
  }
  return {
    focus_ready: focus?.ready === true,
    focus_round: Number(focus?.round || 0),
    rows: unique.slice(0, FOCUS_TARGET),
  };
}

function latestByTime(payload) {
  const rows = Array.isArray(payload) ? payload : [];
  if (!rows.length) return null;
  return [...rows].sort((a, b) => Number(b?.time || 0) - Number(a?.time || 0))[0] || null;
}

function parseContractStat(payload, target) {
  const row = latestByTime(payload);
  if (!row) return null;
  const parsed = {
    symbol: target.symbol,
    native_symbol: target.native_symbol,
    focus_role: target.role,
    focus_slot: target.slot,
    source_time: isoSeconds(row?.time),
    source_time_s: integer(row?.time),
    interval: '5m',
    lsr_taker: finite(row?.lsr_taker),
    lsr_account: finite(row?.lsr_account),
    open_interest_contracts: finite(row?.open_interest),
    open_interest_usd: finite(row?.open_interest_usd),
    top_lsr_account: finite(row?.top_lsr_account),
    top_lsr_size: finite(row?.top_lsr_size),
    mark_price: finite(row?.mark_price),
    long_liq_size: finite(row?.long_liq_size),
    long_liq_amount: finite(row?.long_liq_amount),
    long_liq_usd: finite(row?.long_liq_usd_new ?? row?.long_liq_usd),
    short_liq_size: finite(row?.short_liq_size),
    short_liq_amount: finite(row?.short_liq_amount),
    short_liq_usd: finite(row?.short_liq_usd_new ?? row?.short_liq_usd),
    top_long_size: finite(row?.top_long_size),
    top_short_size: finite(row?.top_short_size),
    long_taker_size: finite(row?.long_taker_size),
    short_taker_size: finite(row?.short_taker_size),
    top_long_account: finite(row?.top_long_account),
    top_short_account: finite(row?.top_short_account),
    long_users: integer(row?.long_users),
    short_users: integer(row?.short_users),
    source: 'gate_official_public_futures_contract_stats',
  };
  const hasCore = parsed.open_interest_contracts != null || parsed.open_interest_usd != null || parsed.lsr_taker != null || parsed.lsr_account != null;
  return hasCore ? parsed : null;
}

function parseRiskTiers(payload, target) {
  const rows = Array.isArray(payload) ? payload : [];
  if (!rows.length) return null;
  const tiers = rows
    .filter((row) => !row?.contract || compact(row.contract) === compact(target.native_symbol))
    .map((row) => ({
      tier: integer(row?.tier),
      risk_limit: finite(row?.risk_limit),
      initial_margin_rate: finite(row?.initial_rate),
      maintenance_margin_rate: finite(row?.maintenance_rate),
      max_leverage: finite(row?.leverage_max),
      deduction: finite(row?.deduction),
      contract: String(row?.contract || target.native_symbol),
    }))
    .filter((row) => row.tier != null || row.risk_limit != null || row.max_leverage != null);
  if (!tiers.length) return null;
  return {
    symbol: target.symbol,
    native_symbol: target.native_symbol,
    tiers,
    source: 'gate_official_public_futures_risk_limit_tiers',
  };
}

function parseInsurance(payload) {
  const rows = Array.isArray(payload) ? payload : [];
  return rows.map((row) => ({
    time: integer(row?.time),
    source_time: isoSeconds(row?.time),
    balance: finite(row?.balance),
    source: 'gate_official_public_futures_insurance_fund_history',
  })).filter((row) => row.time != null || row.balance != null);
}

async function collectContractStats(targets) {
  const result = new Map();
  for (let i = 0; i < targets.length; i += 1) {
    const target = targets[i];
    try {
      const payload = await fetchGate(`/futures/usdt/contract_stats?contract=${encodeURIComponent(target.native_symbol)}&interval=5m&limit=1`, { lane: 'contract_stats' });
      const parsed = parseContractStat(payload, target);
      if (parsed) result.set(target.symbol, parsed);
    } catch (_) {}
    if (i < targets.length - 1) await sleep(PER_SYMBOL_GAP_MS);
  }
  setLane('contract_stats', { last_rows: result.size });
  return result;
}

async function collectRiskTiers(targets) {
  const result = new Map();
  for (let i = 0; i < targets.length; i += 1) {
    const target = targets[i];
    try {
      const payload = await fetchGate(`/futures/usdt/risk_limit_tiers?contract=${encodeURIComponent(target.native_symbol)}&limit=100`, { lane: 'risk_limit_tiers' });
      const parsed = parseRiskTiers(payload, target);
      if (parsed) result.set(target.symbol, parsed);
    } catch (_) {}
    if (i < targets.length - 1) await sleep(PER_SYMBOL_GAP_MS);
  }
  setLane('risk_limit_tiers', { last_rows: result.size });
  return result;
}

async function refreshFocusStats(reason = 'scheduled') {
  if (focusRunning) return await focusRunning;
  let task;
  task = (async () => {
    lastFocusStartedAt = new Date().toISOString();
    lastFocusError = '';
    totalFocusBuilds += 1;
    try {
      const focus = gateFocusTargets();
      if (!focus.focus_ready || focus.rows.length !== FOCUS_TARGET) {
        throw new Error(`gate_focus_not_ready:${focus.rows.length}/${FOCUS_TARGET}`);
      }
      const [stats, risks] = await Promise.all([
        collectContractStats(focus.rows),
        collectRiskTiers(focus.rows),
      ]);
      const updatedAt = new Date().toISOString();
      const next = new Map();
      for (const target of focus.rows) {
        const stat = stats.get(target.symbol) || null;
        const risk = risks.get(target.symbol) || null;
        if (!stat && !risk) continue;
        next.set(target.symbol, {
          provider: 'gate',
          market_type: 'contract',
          symbol: target.symbol,
          native_symbol: target.native_symbol,
          base_asset: target.base_asset,
          quote_asset: 'USDT',
          focus_role: target.role,
          focus_slot: target.slot,
          contract_stats: stat,
          risk_limit_tiers: risk?.tiers || null,
          official_contract_stats_available: Boolean(stat),
          official_risk_limit_tiers_available: Boolean(risk),
          updated_at: updatedAt,
          source: 'gate_official_public_advanced_shared_stats',
        });
      }
      if (!next.size) throw new Error('gate_advanced_focus_rows_empty');
      contractRows = next;
      round += 1;
      lastFocusCompletedAt = updatedAt;
      responseCache.clear();
      return true;
    } catch (error) {
      totalFocusFailures += 1;
      lastFocusError = `${reason}:${String(error?.message || error)}`.slice(0, 320);
      return false;
    } finally {
      if (focusRunning === task) focusRunning = null;
    }
  })();
  focusRunning = task;
  return await task;
}

async function refreshInsurance(reason = 'scheduled') {
  if (insuranceRunning) return await insuranceRunning;
  let task;
  task = (async () => {
    lastInsuranceStartedAt = new Date().toISOString();
    lastInsuranceError = '';
    totalInsuranceBuilds += 1;
    try {
      const payload = await fetchGate('/futures/usdt/insurance?limit=100', { lane: 'insurance_fund' });
      const rows = parseInsurance(payload);
      if (!rows.length) throw new Error('gate_insurance_rows_empty');
      insuranceRows = rows;
      setLane('insurance_fund', { last_rows: rows.length });
      lastInsuranceCompletedAt = new Date().toISOString();
      responseCache.clear();
      return true;
    } catch (error) {
      totalInsuranceFailures += 1;
      lastInsuranceError = `${reason}:${String(error?.message || error)}`.slice(0, 320);
      return false;
    } finally {
      if (insuranceRunning === task) insuranceRunning = null;
    }
  })();
  insuranceRunning = task;
  return await task;
}

export function startGateAdvancedStatsScanner() {
  if (started || process.env.KAKA_DISABLE_GATE_ADVANCED_STATS === '1') return;
  started = true;
  focusTimer = setTimeout(async () => {
    const ok = await refreshFocusStats('startup');
    if (!ok) {
      const retry = setTimeout(() => refreshFocusStats('startup_retry').catch(() => {}), STARTUP_RETRY_MS);
      retry.unref?.();
    }
  }, START_DELAY_MS);
  focusTimer.unref?.();
  insuranceTimer = setTimeout(() => refreshInsurance('startup').catch(() => {}), Math.min(START_DELAY_MS + 1_500, 14_000));
  insuranceTimer.unref?.();
  focusInterval = setInterval(() => refreshFocusStats('interval').catch(() => {}), FOCUS_REFRESH_MS);
  focusInterval.unref?.();
  insuranceInterval = setInterval(() => refreshInsurance('interval').catch(() => {}), INSURANCE_REFRESH_MS);
  insuranceInterval.unref?.();
}

function fresh(updatedAt, maxAge = STALE_MS) {
  const ms = Date.parse(String(updatedAt || ''));
  return Number.isFinite(ms) && Date.now() - ms <= maxAge;
}

function snapshotPayload({ includeRows = true } = {}) {
  const cacheKey = includeRows ? 'full' : 'meta';
  const cached = responseCache.get(cacheKey);
  if (cached && Date.now() - cached.at <= RESPONSE_CACHE_TTL_MS) {
    responseCacheHits += 1;
    return { ...clone(cached.payload), cache_hit: true, cache_age_ms: Date.now() - cached.at };
  }
  responseCacheMisses += 1;

  const focus = gateFocusTargets();
  const targetSymbols = focus.rows.map((row) => row.symbol);
  const rows = targetSymbols.map((symbol) => contractRows.get(symbol)).filter((row) => row && fresh(row.updated_at));
  const statsRows = rows.filter((row) => row.official_contract_stats_available);
  const riskRows = rows.filter((row) => row.official_risk_limit_tiers_available);
  const coreTargetCount = focus.rows.filter((row) => row.role === 'core').length;
  const hotTargetCount = focus.rows.filter((row) => row.role === 'hot').length;
  const coreStatsRows = statsRows.filter((row) => row.focus_role === 'core').length;
  const hotStatsRows = statsRows.filter((row) => row.focus_role === 'hot').length;
  const topTraderRows = statsRows.filter((row) => {
    const stat = row.contract_stats || {};
    return stat.top_lsr_account != null || stat.top_lsr_size != null || stat.top_long_size != null || stat.top_short_size != null || stat.top_long_account != null || stat.top_short_account != null;
  }).length;
  const takerRows = statsRows.filter((row) => {
    const stat = row.contract_stats || {};
    return stat.lsr_taker != null || stat.long_taker_size != null || stat.short_taker_size != null;
  }).length;
  const accountRows = statsRows.filter((row) => {
    const stat = row.contract_stats || {};
    return stat.lsr_account != null || stat.long_users != null || stat.short_users != null;
  }).length;
  const oiRows = statsRows.filter((row) => row.contract_stats?.open_interest_contracts != null || row.contract_stats?.open_interest_usd != null).length;
  const liquidationReferenceRows = statsRows.filter((row) => {
    const stat = row.contract_stats || {};
    return stat.long_liq_size != null || stat.short_liq_size != null || stat.long_liq_usd != null || stat.short_liq_usd != null;
  }).length;
  const insuranceFresh = insuranceRows.length > 0 && fresh(lastInsuranceCompletedAt, Math.max(STALE_MS, INSURANCE_REFRESH_MS * 3));
  const lane = Object.fromEntries([...laneStats.entries()].map(([key, value]) => [key, { ...value }]));

  const payload = {
    ok: true,
    version: VERSION,
    source: 'render_shared_gate_official_public_advanced_statistics',
    ready: focus.focus_ready &&
      targetSymbols.length === FOCUS_TARGET &&
      coreTargetCount === 10 &&
      coreStatsRows === coreTargetCount &&
      riskRows.length >= coreTargetCount &&
      oiRows >= coreTargetCount &&
      insuranceFresh,
    focus_target: FOCUS_TARGET,
    focus_round: focus.focus_round,
    focus_symbols: targetSymbols,
    contract_row_count: rows.length,
    contract_stats_rows: statsRows.length,
    contract_core_target_count: coreTargetCount,
    contract_hot_target_count: hotTargetCount,
    contract_core_stats_rows: coreStatsRows,
    contract_hot_stats_rows: hotStatsRows,
    risk_limit_rows: riskRows.length,
    open_interest_rows: oiRows,
    top_trader_rows: topTraderRows,
    taker_stats_rows: takerRows,
    account_stats_rows: accountRows,
    liquidation_reference_rows: liquidationReferenceRows,
    insurance_record_count: insuranceFresh ? insuranceRows.length : 0,
    focus_refresh_seconds: Math.round(FOCUS_REFRESH_MS / 1000),
    insurance_refresh_seconds: Math.round(INSURANCE_REFRESH_MS / 1000),
    per_symbol_gap_ms: PER_SYMBOL_GAP_MS,
    official_endpoint_policy: {
      contract_stats: 'Gate official public per-contract 5m ContractStat; focus15 only because endpoint is per contract',
      risk_limit_tiers: 'Gate official public per-contract risk tiers; focus15 shared slow stats',
      insurance_fund: 'Gate official public futures insurance fund history; one shared low-frequency request',
      liquidation_history: 'Gate official liq_orders exists but intentionally deferred to Step997 unified liquidation event/history pipeline',
    },
    official_semantics: {
      open_interest: 'ContractStat open_interest is contract count; open_interest_usd is quote-currency notional; never relabel futures ticker total_size as OI',
      lsr_taker: 'Gate official long/short taker ratio from ContractStat',
      lsr_account: 'Gate official long/short position user ratio from ContractStat',
      top_trader: 'Gate official top_lsr_account/top_lsr_size plus newer top-long/top-short size/account fields when supplied by current API',
      taker_size: 'Gate official long_taker_size/short_taker_size when supplied by current API',
      user_counts: 'Gate official long_users/short_users when supplied by current API',
      liquidation_reference: 'ContractStat 5m liquidation aggregate fields are preserved as Gate official statistics only; they do not replace Step997 unified liquidation event/history pipeline',
      risk_limit_tiers: 'Gate official position risk limit, initial/maintenance margin rates, max leverage and deduction',
      insurance_fund: 'Gate official futures insurance fund history',
    },
    separate_from_derived_data: true,
    no_cross_provider_substitution: true,
    no_cross_quote_substitution: true,
    missing_stays_null: true,
    collector_only_exchange_requests: true,
    exchange_requests_started_by_user_read: 0,
    exchange_connections_started_by_user_read: 0,
    user_reads_trigger_collector: false,
    reads_scale_with_users: false,
    liquidation_history_deferred_to_step997: true,
    last_focus_started_at: lastFocusStartedAt,
    last_focus_completed_at: lastFocusCompletedAt,
    last_focus_error: lastFocusError,
    last_insurance_started_at: lastInsuranceStartedAt,
    last_insurance_completed_at: lastInsuranceCompletedAt,
    last_insurance_error: lastInsuranceError,
    round,
    lane_stats: lane,
    contract_rows: includeRows ? rows.map(clone) : [],
    insurance_rows: includeRows && insuranceFresh ? clone(insuranceRows) : [],
    timestamp_ms: Date.now(),
  };
  responseCache.set(cacheKey, { at: Date.now(), payload });
  return { ...clone(payload), cache_hit: false, cache_age_ms: 0 };
}

export function getGateAdvancedStatsHealth() {
  const snapshot = snapshotPayload({ includeRows: false });
  return {
    ...snapshot,
    enabled: started || process.env.KAKA_DISABLE_GATE_ADVANCED_STATS !== '1',
    mode: 'shared_gate_focus15_contract_stats_risk_plus_insurance',
    snapshot_endpoint: SNAPSHOT_ROUTE,
    health_endpoint: HEALTH_ROUTE,
    total_reads: totalReads,
    total_focus_builds: totalFocusBuilds,
    total_focus_failures: totalFocusFailures,
    total_insurance_builds: totalInsuranceBuilds,
    total_insurance_failures: totalInsuranceFailures,
    response_cache_ttl_seconds: RESPONSE_CACHE_TTL_MS / 1000,
    response_cache_hits: responseCacheHits,
    response_cache_misses: responseCacheMisses,
    focus_running: Boolean(focusRunning),
    insurance_running: Boolean(insuranceRunning),
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

export async function handleGateAdvancedStats(req, res, url) {
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
    sendJson(res, 200, getGateAdvancedStatsHealth());
    return true;
  }
  sendJson(res, 200, snapshotPayload({ includeRows: true }));
  return true;
}

export const __gateAdvancedTest = Object.freeze({
  gateNativeSymbol,
  parseContractStat,
  parseRiskTiers,
  parseInsurance,
});
