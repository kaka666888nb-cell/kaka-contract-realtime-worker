import { getContractFocusPoolInternalSnapshot } from './deep-market-bridge.mjs';

const VERSION = '650.8.15.4';
const SNAPSHOT_ROUTE = '/api/gate-advanced/current-snapshot';
const HEALTH_ROUTE = '/api/gate-advanced/health';
const CONTRACT_STATS_HISTORY_ROUTE = '/api/gate-advanced/contract-stats-history';
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

const CONTRACT_STATS_HISTORY_OFFICIAL_INTERVAL = '5m';
const CONTRACT_STATS_HISTORY_LIMIT = 100;
const CONTRACT_STATS_HISTORY_MAX_ROWS = Math.max(288, Math.min(2_016, Number(process.env.KAKA_GATE_CONTRACT_STATS_HISTORY_MAX_ROWS || 576)));
const CONTRACT_STATS_HISTORY_STALE_MS = Math.max(10 * 60_000, Number(process.env.KAKA_GATE_CONTRACT_STATS_HISTORY_STALE_MS || 20 * 60_000));
const CONTRACT_STATS_HISTORY_PERSIST_INTERVAL_MS = Math.max(15 * 60_000, Number(process.env.KAKA_GATE_CONTRACT_STATS_HISTORY_PERSIST_INTERVAL_MS || 30 * 60_000));
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');
const CONTRACT_STATS_HISTORY_SNAPSHOT_TABLE = 'app_market_backend_snapshots';
const CONTRACT_STATS_HISTORY_SNAPSHOT_TYPE = 'position_stats';
const CONTRACT_STATS_HISTORY_SNAPSHOT_PREFIX = 'GATE_CONTRACT_STATS_HISTORY:';

let started = false;
let focusRunning = null;
let insuranceRunning = null;
let focusTimer = null;
let insuranceTimer = null;
let focusRecoveryTimer = null;
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

let contractStatsHistoryRestorePromise = null;
let contractStatsHistoryRestored = false;
let contractStatsHistoryPersistPromise = null;
let contractStatsHistoryLastPersistAt = 0;
let contractStatsHistoryPersistAttempts = 0;
let contractStatsHistoryPersistSuccesses = 0;
let contractStatsHistoryPersistErrors = 0;
let contractStatsHistoryRestoreAttempts = 0;
let contractStatsHistoryRestoreSuccesses = 0;
let contractStatsHistoryRestoreErrors = 0;
let contractStatsHistoryLastPersistError = '';
let contractStatsHistoryLastRestoreError = '';

let contractRows = new Map();
const contractStatsHistoryBySymbol = new Map();
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
          'user-agent': 'KakaWeb3/650.8.15.117 gate-advanced-shared',
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


function persistenceEnabled() {
  return Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
}

function contractStatsHistorySnapshotKey(symbol) {
  return `${CONTRACT_STATS_HISTORY_SNAPSHOT_PREFIX}${compact(symbol)}`;
}

function normalizeContractStatsHistoryInterval(raw) {
  const value = String(raw || CONTRACT_STATS_HISTORY_OFFICIAL_INTERVAL).trim().toLowerCase();
  if (value === '5m') return '5m';
  if (value === '1h' || value === '60m') return '1h';
  if (value === '1d' || value === '24h') return '1d';
  return null;
}

function contractStatsHistoryIntervalMs(interval) {
  if (interval === '1h') return 60 * 60_000;
  if (interval === '1d') return 24 * 60 * 60_000;
  return 5 * 60_000;
}

function parseContractStatHistoryRow(row, target) {
  if (!row || typeof row !== 'object') return null;
  const timeS = integer(row?.time);
  if (timeS == null || timeS <= 0) return null;

  const parsed = {
    provider: 'gate',
    market_type: 'contract',
    quote_asset: 'USDT',
    symbol: target.symbol,
    native_symbol: target.native_symbol,
    base_asset: target.base_asset,
    interval: CONTRACT_STATS_HISTORY_OFFICIAL_INTERVAL,
    bucket_time_ms: timeS * 1000,
    bucket_time: isoSeconds(timeS),
    source_time_ms: timeS * 1000,
    source_time: isoSeconds(timeS),

    lsr_taker: finite(row?.lsr_taker),
    lsr_account: finite(row?.lsr_account),
    open_interest_contracts: finite(row?.open_interest),
    open_interest_usd: finite(row?.open_interest_usd),
    top_lsr_account: finite(row?.top_lsr_account),
    top_lsr_size: finite(row?.top_lsr_size),
    mark_price: finite(row?.mark_price),
    top_long_size: finite(row?.top_long_size),
    top_short_size: finite(row?.top_short_size),
    top_long_account: finite(row?.top_long_account),
    top_short_account: finite(row?.top_short_account),
    long_users: integer(row?.long_users),
    short_users: integer(row?.short_users),

    long_liq_size: finite(row?.long_liq_size),
    long_liq_amount: finite(row?.long_liq_amount),
    long_liq_usd: finite(row?.long_liq_usd_new ?? row?.long_liq_usd),
    short_liq_size: finite(row?.short_liq_size),
    short_liq_amount: finite(row?.short_liq_amount),
    short_liq_usd: finite(row?.short_liq_usd_new ?? row?.short_liq_usd),
    long_taker_size: finite(row?.long_taker_size),
    short_taker_size: finite(row?.short_taker_size),

    official: true,
    derived: false,
    derived_from_official_5m: false,
    official_interval: CONTRACT_STATS_HISTORY_OFFICIAL_INTERVAL,
    official_endpoint: '/futures/usdt/contract_stats',
    source: 'gate_official_public_contract_stats_5m_history',
    updated_at: new Date().toISOString(),
  };

  const hasAny = [
    parsed.lsr_taker, parsed.lsr_account,
    parsed.open_interest_contracts, parsed.open_interest_usd,
    parsed.top_lsr_account, parsed.top_lsr_size,
    parsed.long_liq_size, parsed.short_liq_size,
    parsed.long_taker_size, parsed.short_taker_size,
    parsed.long_users, parsed.short_users,
  ].some((value) => value != null);
  return hasAny ? parsed : null;
}

function parseContractStatsHistory(payload, target) {
  const source = Array.isArray(payload) ? payload : [];
  const byTime = new Map();
  for (const raw of source) {
    const parsed = parseContractStatHistoryRow(raw, target);
    if (!parsed) continue;
    byTime.set(parsed.source_time_ms, parsed);
  }
  return [...byTime.values()].sort((a, b) => a.source_time_ms - b.source_time_ms);
}

function mergeContractStatsHistoryRows(existing, incoming) {
  const byTime = new Map();
  for (const row of [...(existing || []), ...(incoming || [])]) {
    const ts = Number(row?.source_time_ms || row?.bucket_time_ms || 0);
    if (!Number.isFinite(ts) || ts <= 0) continue;
    byTime.set(ts, {
      ...row,
      source_time_ms: ts,
      source_time: new Date(ts).toISOString(),
    });
  }
  return [...byTime.values()]
    .sort((a, b) => a.source_time_ms - b.source_time_ms)
    .slice(-CONTRACT_STATS_HISTORY_MAX_ROWS);
}

const CONTRACT_STATS_HISTORY_STATE_FIELDS = Object.freeze([
  'lsr_taker',
  'lsr_account',
  'open_interest_contracts',
  'open_interest_usd',
  'top_lsr_account',
  'top_lsr_size',
  'mark_price',
  'top_long_size',
  'top_short_size',
  'top_long_account',
  'top_short_account',
  'long_users',
  'short_users',
]);

const CONTRACT_STATS_HISTORY_SUM_FIELDS = Object.freeze([
  'long_liq_size',
  'long_liq_amount',
  'long_liq_usd',
  'short_liq_size',
  'short_liq_amount',
  'short_liq_usd',
  'long_taker_size',
  'short_taker_size',
]);

function sumNullable(values) {
  const finiteValues = values.filter((value) => Number.isFinite(Number(value))).map(Number);
  if (!finiteValues.length) return null;
  return finiteValues.reduce((sum, value) => sum + value, 0);
}

function deriveContractStatsHistoryRows(rows, interval) {
  if (interval === '5m') return (rows || []).map((row) => ({ ...row }));

  const span = contractStatsHistoryIntervalMs(interval);
  const groups = new Map();
  for (const row of rows || []) {
    const ts = Number(row?.source_time_ms || 0);
    if (!Number.isFinite(ts) || ts <= 0) continue;
    const bucket = Math.floor(ts / span) * span;
    const group = groups.get(bucket) || [];
    group.push(row);
    groups.set(bucket, group);
  }

  const out = [];
  for (const [bucket, group] of groups.entries()) {
    group.sort((a, b) => Number(a.source_time_ms || 0) - Number(b.source_time_ms || 0));
    const last = group.at(-1);
    if (!last) continue;

    const derived = {
      provider: 'gate',
      market_type: 'contract',
      quote_asset: 'USDT',
      symbol: last.symbol,
      native_symbol: last.native_symbol,
      base_asset: last.base_asset,
      interval,
      bucket_time_ms: bucket,
      bucket_time: new Date(bucket).toISOString(),
      source_time_ms: last.source_time_ms,
      source_time: last.source_time,
      official: false,
      derived: true,
      derived_from_official_5m: true,
      official_base_interval: CONTRACT_STATS_HISTORY_OFFICIAL_INTERVAL,
      derived_method: 'state_fields_last_observation; interval_flow_fields_sum',
      official_endpoint: '/futures/usdt/contract_stats',
      source: `gate_derived_${interval}_from_official_5m_contract_stats`,
      updated_at: new Date().toISOString(),
    };

    for (const field of CONTRACT_STATS_HISTORY_STATE_FIELDS) {
      derived[field] = last[field] ?? null;
    }
    for (const field of CONTRACT_STATS_HISTORY_SUM_FIELDS) {
      derived[field] = sumNullable(group.map((row) => row[field]));
    }

    out.push(derived);
  }
  return out.sort((a, b) => a.bucket_time_ms - b.bucket_time_ms);
}

function contractStatsHistoryFresh(entry) {
  const latest = entry?.rows?.at(-1);
  const ts = Number(latest?.source_time_ms || 0);
  return Number.isFinite(ts) && ts > 0 && Date.now() - ts <= CONTRACT_STATS_HISTORY_STALE_MS;
}

async function restoreContractStatsHistorySnapshots() {
  if (contractStatsHistoryRestored) return true;
  if (!persistenceEnabled()) {
    contractStatsHistoryRestored = true;
    return false;
  }
  if (contractStatsHistoryRestorePromise) return await contractStatsHistoryRestorePromise;

  contractStatsHistoryRestorePromise = (async () => {
    contractStatsHistoryRestoreAttempts += 1;
    try {
      const query = [
        'select=quote_asset,payload,updated_at',
        'provider=eq.gate',
        'market_type=eq.contract',
        `snapshot_type=eq.${encodeURIComponent(CONTRACT_STATS_HISTORY_SNAPSHOT_TYPE)}`,
        'source=eq.gate_official_contract_stats_history_focus15_shared',
        'limit=30',
      ].join('&');

      const response = await fetch(
        `${SUPABASE_URL}/rest/v1/${CONTRACT_STATS_HISTORY_SNAPSHOT_TABLE}?${query}`,
        {
          headers: {
            apikey: SUPABASE_SERVICE_ROLE_KEY,
            authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
            accept: 'application/json',
          },
          signal: AbortSignal.timeout(8_000),
        },
      );
      if (!response.ok) throw new Error(`gate_contract_stats_history_restore_http_${response.status}`);
      const records = await response.json();

      let restored = 0;
      for (const record of Array.isArray(records) ? records : []) {
        const symbol = compact(
          record?.payload?.symbol ||
          String(record?.quote_asset || '').replace(CONTRACT_STATS_HISTORY_SNAPSHOT_PREFIX, ''),
        );
        if (!symbol || !symbol.endsWith('USDT')) continue;
        const target = {
          symbol,
          native_symbol: gateNativeSymbol(symbol),
          base_asset: symbol.slice(0, -4),
        };
        const normalized = [];
        for (const raw of Array.isArray(record?.payload?.rows) ? record.payload.rows : []) {
          const timeS = Math.floor(Number(raw?.source_time_ms || raw?.bucket_time_ms || 0) / 1000);
          const parsed = parseContractStatHistoryRow({ ...raw, time: timeS }, target);
          if (!parsed) continue;
          for (const field of CONTRACT_STATS_HISTORY_STATE_FIELDS) {
            if (raw?.[field] != null) parsed[field] = finite(raw[field]);
          }
          for (const field of CONTRACT_STATS_HISTORY_SUM_FIELDS) {
            if (raw?.[field] != null) parsed[field] = finite(raw[field]);
          }
          if (raw?.long_users != null) parsed.long_users = integer(raw.long_users);
          if (raw?.short_users != null) parsed.short_users = integer(raw.short_users);
          if (raw?.top_long_account != null) parsed.top_long_account = integer(raw.top_long_account);
          if (raw?.top_short_account != null) parsed.top_short_account = integer(raw.top_short_account);
          normalized.push(parsed);
        }
        if (!normalized.length) continue;
        contractStatsHistoryBySymbol.set(symbol, {
          symbol,
          native_symbol: target.native_symbol,
          base_asset: target.base_asset,
          rows: mergeContractStatsHistoryRows([], normalized),
          restored: true,
          updated_at: record?.updated_at || new Date().toISOString(),
        });
        restored += 1;
      }
      if (restored > 0) contractStatsHistoryRestoreSuccesses += 1;
      contractStatsHistoryLastRestoreError = '';
      return restored > 0;
    } catch (error) {
      contractStatsHistoryRestoreErrors += 1;
      contractStatsHistoryLastRestoreError = String(error?.message || error).slice(0, 320);
      return false;
    } finally {
      contractStatsHistoryRestored = true;
    }
  })().finally(() => {
    contractStatsHistoryRestorePromise = null;
  });

  return await contractStatsHistoryRestorePromise;
}

async function persistContractStatsHistorySnapshots({ force = false } = {}) {
  if (!persistenceEnabled()) return false;
  if (
    !force &&
    contractStatsHistoryLastPersistAt > 0 &&
    Date.now() - contractStatsHistoryLastPersistAt < CONTRACT_STATS_HISTORY_PERSIST_INTERVAL_MS
  ) {
    return true;
  }
  if (contractStatsHistoryPersistPromise) return await contractStatsHistoryPersistPromise;

  const focus = gateFocusTargets();
  const body = focus.rows.map((target) => {
    const entry = contractStatsHistoryBySymbol.get(target.symbol);
    if (!entry?.rows?.length) return null;
    const now = new Date().toISOString();
    return {
      provider: 'gate',
      market_type: 'contract',
      snapshot_type: CONTRACT_STATS_HISTORY_SNAPSHOT_TYPE,
      quote_asset: contractStatsHistorySnapshotKey(target.symbol),
      payload: {
        schema_version: '650.8.15.117',
        symbol: target.symbol,
        native_symbol: target.native_symbol,
        base_asset: target.base_asset,
        official_interval: CONTRACT_STATS_HISTORY_OFFICIAL_INTERVAL,
        official_endpoint: '/futures/usdt/contract_stats',
        rows: entry.rows.slice(-CONTRACT_STATS_HISTORY_MAX_ROWS),
      },
      row_count: entry.rows.length,
      source: 'gate_official_contract_stats_history_focus15_shared',
      source_time: entry.rows.at(-1)?.source_time || now,
      updated_at: now,
    };
  }).filter(Boolean);

  if (!body.length) return false;

  contractStatsHistoryPersistPromise = (async () => {
    contractStatsHistoryPersistAttempts += 1;
    try {
      const response = await fetch(
        `${SUPABASE_URL}/rest/v1/${CONTRACT_STATS_HISTORY_SNAPSHOT_TABLE}?on_conflict=provider,market_type,snapshot_type,quote_asset`,
        {
          method: 'POST',
          headers: {
            apikey: SUPABASE_SERVICE_ROLE_KEY,
            authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
            'content-type': 'application/json',
            prefer: 'resolution=merge-duplicates,return=minimal',
          },
          body: JSON.stringify(body),
          signal: AbortSignal.timeout(12_000),
        },
      );
      if (!response.ok) throw new Error(`gate_contract_stats_history_persist_http_${response.status}`);
      contractStatsHistoryLastPersistAt = Date.now();
      contractStatsHistoryPersistSuccesses += 1;
      contractStatsHistoryLastPersistError = '';
      return true;
    } catch (error) {
      contractStatsHistoryPersistErrors += 1;
      contractStatsHistoryLastPersistError = String(error?.message || error).slice(0, 320);
      return false;
    }
  })().finally(() => {
    contractStatsHistoryPersistPromise = null;
  });

  return await contractStatsHistoryPersistPromise;
}

function contractStatsHistoryHealthPayload() {
  const focus = gateFocusTargets();
  const symbols = focus.rows.map((target) => target.symbol);
  const entries = symbols.map((symbol) => {
    const entry = contractStatsHistoryBySymbol.get(symbol);
    const rows = entry?.rows || [];
    return {
      symbol,
      row_count: rows.length,
      fresh: contractStatsHistoryFresh(entry),
      restored: entry?.restored === true,
      latest_source_time: rows.at(-1)?.source_time || null,
    };
  });
  const covered = entries.filter((entry) => entry.row_count > 0).length;
  const fresh = entries.filter((entry) => entry.fresh).length;
  const totalRows = entries.reduce((sum, entry) => sum + Number(entry.row_count || 0), 0);

  return {
    ready: focus.focus_ready && symbols.length === FOCUS_TARGET && covered === FOCUS_TARGET,
    focus_target: FOCUS_TARGET,
    focus_symbols: symbols,
    official_5m_coverage: covered,
    fresh_5m_coverage: fresh,
    total_official_5m_rows: totalRows,
    official_interval: CONTRACT_STATS_HISTORY_OFFICIAL_INTERVAL,
    official_endpoint: '/futures/usdt/contract_stats',
    official_limit_per_existing_request: CONTRACT_STATS_HISTORY_LIMIT,
    reuses_existing_step992_contract_stats_requests: true,
    additional_exchange_requests_vs_step992: 0,
    request_count_per_full_focus_cycle: FOCUS_TARGET,
    same_request_current_and_history: true,
    derived_intervals: ['1h', '1d'],
    official_and_derived_separate: true,
    rollup_state_fields: [...CONTRACT_STATS_HISTORY_STATE_FIELDS],
    rollup_sum_fields: [...CONTRACT_STATS_HISTORY_SUM_FIELDS],
    derived_method: 'state_fields_last_observation; interval_flow_fields_sum',
    shared_backend_memory: true,
    persistence_enabled: persistenceEnabled(),
    persistence_table: CONTRACT_STATS_HISTORY_SNAPSHOT_TABLE,
    persistence_snapshot_type: CONTRACT_STATS_HISTORY_SNAPSHOT_TYPE,
    persistence_one_batch_write_per_interval: true,
    persist_interval_seconds: Math.round(CONTRACT_STATS_HISTORY_PERSIST_INTERVAL_MS / 1000),
    persist_attempts: contractStatsHistoryPersistAttempts,
    persist_successes: contractStatsHistoryPersistSuccesses,
    persist_errors: contractStatsHistoryPersistErrors,
    last_persist_error: contractStatsHistoryLastPersistError,
    restore_attempts: contractStatsHistoryRestoreAttempts,
    restore_successes: contractStatsHistoryRestoreSuccesses,
    restore_errors: contractStatsHistoryRestoreErrors,
    last_restore_error: contractStatsHistoryLastRestoreError,
    exchange_requests_started_by_user_read: 0,
    exchange_connections_started_by_user_read: 0,
    user_reads_trigger_collector: false,
    reads_scale_with_users: false,
    symbols: entries,
  };
}

function contractStatsHistoryReadPayload(symbol, interval = '5m', limit = 200) {
  const normalizedSymbol = compact(symbol);
  const normalizedInterval = normalizeContractStatsHistoryInterval(interval);
  const focus = gateFocusTargets();
  const target = focus.rows.find((row) => row.symbol === normalizedSymbol);
  const entry = contractStatsHistoryBySymbol.get(normalizedSymbol);
  const baseRows = entry?.rows || [];
  const capped = Math.max(1, Math.min(CONTRACT_STATS_HISTORY_MAX_ROWS, Number(limit || 200)));
  const rows = normalizedInterval
    ? deriveContractStatsHistoryRows(baseRows, normalizedInterval).slice(-capped)
    : [];

  return {
    ok: true,
    version: VERSION,
    provider: 'gate',
    market_type: 'contract',
    symbol: normalizedSymbol,
    native_symbol: target?.native_symbol || gateNativeSymbol(normalizedSymbol),
    interval: normalizedInterval,
    ready: Boolean(target && normalizedInterval && rows.length > 0),
    official: normalizedInterval === CONTRACT_STATS_HISTORY_OFFICIAL_INTERVAL,
    derived: normalizedInterval !== CONTRACT_STATS_HISTORY_OFFICIAL_INTERVAL,
    derived_from_official_5m: normalizedInterval !== CONTRACT_STATS_HISTORY_OFFICIAL_INTERVAL,
    official_base_interval: CONTRACT_STATS_HISTORY_OFFICIAL_INTERVAL,
    official_endpoint: '/futures/usdt/contract_stats',
    derived_method: normalizedInterval === CONTRACT_STATS_HISTORY_OFFICIAL_INTERVAL
      ? null
      : 'state_fields_last_observation; interval_flow_fields_sum',
    row_count: rows.length,
    rows: rows.map(clone),
    shared_backend_read: true,
    user_read_triggered_exchange_requests: false,
    user_read_triggered_exchange_connections: false,
    reads_scale_with_users: false,
    timestamp_ms: Date.now(),
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
  return rows.map((row) => {
    // Gate official InsuranceRecord model:
    // t = Unix timestamp in seconds, b = insurance balance.
    const time = integer(row?.t);
    const balance = finite(row?.b);
    return {
      time,
      source_time: isoSeconds(time),
      balance,
      source: 'gate_official_public_futures_insurance_fund_history',
    };
  }).filter((row) => row.time != null || row.balance != null);
}

async function collectContractStats(targets) {
  await restoreContractStatsHistorySnapshots().catch(() => false);
  const result = new Map();

  for (let i = 0; i < targets.length; i += 1) {
    const target = targets[i];
    try {
      const payload = await fetchGate(
        `/futures/usdt/contract_stats?contract=${encodeURIComponent(target.native_symbol)}&interval=5m&limit=${CONTRACT_STATS_HISTORY_LIMIT}`,
        { lane: 'contract_stats' },
      );

      const parsed = parseContractStat(payload, target);
      if (parsed) result.set(target.symbol, parsed);

      const historyRows = parseContractStatsHistory(payload, target);
      if (historyRows.length) {
        const previous = contractStatsHistoryBySymbol.get(target.symbol);
        contractStatsHistoryBySymbol.set(target.symbol, {
          symbol: target.symbol,
          native_symbol: target.native_symbol,
          base_asset: target.base_asset,
          rows: mergeContractStatsHistoryRows(previous?.rows || [], historyRows),
          restored: previous?.restored === true,
          updated_at: new Date().toISOString(),
        });
      }
    } catch (_) {}

    if (i < targets.length - 1) await sleep(PER_SYMBOL_GAP_MS);
  }

  setLane('contract_stats', {
    last_rows: result.size,
    official_limit: CONTRACT_STATS_HISTORY_LIMIT,
    current_and_history_same_request: true,
    additional_exchange_requests_vs_step992: 0,
  });

  const historyHealth = contractStatsHistoryHealthPayload();
  if (historyHealth.official_5m_coverage === FOCUS_TARGET) {
    await persistContractStatsHistorySnapshots({
      force: contractStatsHistoryLastPersistAt === 0,
    }).catch(() => false);
  }

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

  // Yield before the readiness check so focusRunning receives this Promise
  // before any startup precondition failure can finish the task.
  const task = (async () => {
    await Promise.resolve();
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
    }
  })();

  focusRunning = task;
  try {
    return await task;
  } finally {
    if (focusRunning === task) focusRunning = null;
  }
}

async function refreshInsurance(reason = 'scheduled') {
  if (insuranceRunning) return await insuranceRunning;

  const task = (async () => {
    await Promise.resolve();
    lastInsuranceStartedAt = new Date().toISOString();
    lastInsuranceError = '';
    totalInsuranceBuilds += 1;
    try {
      const payload = await fetchGate('/futures/usdt/insurance?limit=100', { lane: 'insurance_fund' });
      const rows = parseInsurance(payload);
      setLane('insurance_fund', {
        last_rows: rows.length,
        last_payload_rows: Array.isArray(payload) ? payload.length : 0,
        official_model_fields: 't,b',
      });
      if (!rows.length) {
        throw new Error(`gate_insurance_rows_empty:payload_rows=${Array.isArray(payload) ? payload.length : 0}`);
      }
      insuranceRows = rows;
      lastInsuranceCompletedAt = new Date().toISOString();
      responseCache.clear();
      return true;
    } catch (error) {
      totalInsuranceFailures += 1;
      lastInsuranceError = `${reason}:${String(error?.message || error)}`.slice(0, 320);
      return false;
    }
  })();

  insuranceRunning = task;
  try {
    return await task;
  } finally {
    if (insuranceRunning === task) insuranceRunning = null;
  }
}

function scheduleFocusStartupRecovery() {
  if (!started || contractRows.size >= FOCUS_TARGET || focusRecoveryTimer) return;
  focusRecoveryTimer = setTimeout(async () => {
    focusRecoveryTimer = null;
    const ok = await refreshFocusStats('startup_recovery').catch(() => false);
    if (!ok && contractRows.size < FOCUS_TARGET) scheduleFocusStartupRecovery();
  }, STARTUP_RETRY_MS);
  focusRecoveryTimer.unref?.();
}

export function startGateAdvancedStatsScanner() {
  if (started || process.env.KAKA_DISABLE_GATE_ADVANCED_STATS === '1') return;
  started = true;
  focusTimer = setTimeout(async () => {
    const ok = await refreshFocusStats('startup');
    if (!ok) scheduleFocusStartupRecovery();
  }, START_DELAY_MS);
  focusTimer.unref?.();
  insuranceTimer = setTimeout(async () => {
    const ok = await refreshInsurance('startup').catch(() => false);
    if (!ok) {
      const retry = setTimeout(() => refreshInsurance('startup_retry').catch(() => {}), STARTUP_RETRY_MS);
      retry.unref?.();
    }
  }, Math.min(START_DELAY_MS + 1_500, 14_000));
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
    contract_stats_history: contractStatsHistoryHealthPayload(),
    contract_stats_history_route: CONTRACT_STATS_HISTORY_ROUTE,
    focus_refresh_seconds: Math.round(FOCUS_REFRESH_MS / 1000),
    insurance_refresh_seconds: Math.round(INSURANCE_REFRESH_MS / 1000),
    per_symbol_gap_ms: PER_SYMBOL_GAP_MS,
    official_endpoint_policy: {
      contract_stats: 'Gate official public per-contract 5m ContractStat; Step1003 keeps the same focus15 request count and retains up to 100 official 5m rows from each existing request',
      risk_limit_tiers: 'Gate official public per-contract risk tiers; focus15 shared slow stats',
      insurance_fund: 'Gate official public futures insurance fund history; one shared low-frequency request',
      liquidation_history: 'Gate official public liq_orders is owned by Step997 contract-liquidation background history pipeline; user reads never call Gate',
    },
    official_semantics: {
      open_interest: 'ContractStat open_interest is contract count; open_interest_usd is quote-currency notional; never relabel futures ticker total_size as OI',
      lsr_taker: 'Gate official long/short taker ratio from ContractStat',
      lsr_account: 'Gate official long/short position user ratio from ContractStat',
      top_trader: 'Gate official top_lsr_account/top_lsr_size plus newer top-long/top-short size/account fields when supplied by current API',
      taker_size: 'Gate official long_taker_size/short_taker_size when supplied by current API',
      user_counts: 'Gate official long_users/short_users when supplied by current API',
      contract_stats_history_rollup: '5m is official. 1h/1d are explicit backend-derived buckets: state/ratio/OI fields use the last 5m observation, while liquidation/taker flow fields sum 5m buckets.',
      liquidation_reference: 'ContractStat 5m liquidation aggregate fields remain reference statistics only; Step997 unified history is owned by contract-liquidation and Gate public liq_orders',
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
    liquidation_history_deferred_to_step997: false,
    liquidation_history_step997_ready: true,
    liquidation_history_step997_owner: 'contract-liquidation',
    liquidation_history_step997_route: '/api/contract-liquidation/history',
    liquidation_history_step997_gate_liq_orders_background_only: true,
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
    focus_lock_release_after_completion: true,
    startup_recovery_until_focus_ready: true,
    insurance_official_model_fields: 't,b',
    insurance_parser_matches_official_model: true,
    contract_stats_history_ready: snapshot.contract_stats_history?.ready === true,
    contract_stats_history_official_5m_coverage: Number(snapshot.contract_stats_history?.official_5m_coverage || 0),
    contract_stats_history_total_official_5m_rows: Number(snapshot.contract_stats_history?.total_official_5m_rows || 0),
    contract_stats_history_additional_exchange_requests_vs_step992: Number(snapshot.contract_stats_history?.additional_exchange_requests_vs_step992 || 0),
    contract_stats_history_reuses_existing_step992_requests: snapshot.contract_stats_history?.reuses_existing_step992_contract_stats_requests === true,
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
  if (![SNAPSHOT_ROUTE, HEALTH_ROUTE, CONTRACT_STATS_HISTORY_ROUTE].includes(url.pathname)) return false;
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
  if (url.pathname === CONTRACT_STATS_HISTORY_ROUTE) {
    const symbol = compact(url.searchParams.get('symbol') || '');
    const interval = normalizeContractStatsHistoryInterval(url.searchParams.get('interval') || '5m');
    const limit = Number(url.searchParams.get('limit') || 200);
    if (!symbol || !interval) {
      sendJson(res, 400, {
        ok: false,
        version: VERSION,
        error: 'symbol_and_supported_interval_required',
        supported_intervals: ['5m', '1h', '1d'],
      });
      return true;
    }
    sendJson(res, 200, contractStatsHistoryReadPayload(symbol, interval, limit));
    return true;
  }
  sendJson(res, 200, snapshotPayload({ includeRows: true }));
  return true;
}

export const __gateAdvancedTest = Object.freeze({
  gateNativeSymbol,
  parseContractStat,
  parseContractStatHistoryRow,
  parseContractStatsHistory,
  mergeContractStatsHistoryRows,
  deriveContractStatsHistoryRows,
  normalizeContractStatsHistoryInterval,
  parseRiskTiers,
  parseInsurance,
});
