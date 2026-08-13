import WebSocket from 'ws';
import { getContractFocusPoolInternalSnapshot } from './contract-focus-pool.mjs';
import { getMarketLightInternalSnapshot } from './market-light-snapshot.mjs';

const VERSION = '650.8.15.3';
const SNAPSHOT_ROUTE = '/api/okx-advanced/current-snapshot';
const HEALTH_ROUTE = '/api/okx-advanced/health';
const OI_HISTORY_ROUTE = '/api/okx-advanced/open-interest-history';
const BASE = 'https://www.okx.com';
const WS_URL = 'wss://ws.okx.com:8443/ws/v5/public';

const START_DELAY_MS = Math.max(2_000, Number(process.env.KAKA_OKX_ADVANCED_START_DELAY_MS || 10_000));
const STARTUP_RETRY_MS = Math.max(10_000, Number(process.env.KAKA_OKX_ADVANCED_STARTUP_RETRY_MS || 15_000));
const FOCUS_REFRESH_MS = Math.max(2 * 60_000, Number(process.env.KAKA_OKX_ADVANCED_FOCUS_REFRESH_MS || 5 * 60_000));
const SECURITY_FUND_REFRESH_MS = Math.max(60 * 60_000, Number(process.env.KAKA_OKX_ADVANCED_SECURITY_FUND_REFRESH_MS || 6 * 60 * 60_000));
const RESPONSE_CACHE_TTL_MS = Math.max(3_000, Number(process.env.KAKA_OKX_ADVANCED_RESPONSE_CACHE_TTL_MS || 20_000));
const FOCUS_STALE_MS = Math.max(5 * 60_000, Number(process.env.KAKA_OKX_ADVANCED_FOCUS_STALE_MS || 12 * 60_000));
const SECURITY_FUND_STALE_MS = Math.max(12 * 60 * 60_000, Number(process.env.KAKA_OKX_ADVANCED_SECURITY_FUND_STALE_MS || 36 * 60 * 60_000));
const PER_REQUEST_GAP_MS = Math.max(220, Number(process.env.KAKA_OKX_ADVANCED_PER_REQUEST_GAP_MS || 260));
const ADL_ACTIVE_TTL_MS = Math.max(3_000, Number(process.env.KAKA_OKX_ADVANCED_ADL_ACTIVE_TTL_MS || 6_000));
const ADL_HEARTBEAT_MS = Math.max(10_000, Number(process.env.KAKA_OKX_ADVANCED_ADL_HEARTBEAT_MS || 20_000));
const ADL_RECONNECT_MIN_MS = Math.max(1_000, Number(process.env.KAKA_OKX_ADVANCED_ADL_RECONNECT_MIN_MS || 3_000));
const ADL_RECONNECT_MAX_MS = Math.max(10_000, Number(process.env.KAKA_OKX_ADVANCED_ADL_RECONNECT_MAX_MS || 60_000));
const ADL_SUBSCRIPTION_REFRESH_MS = Math.max(10_000, Number(process.env.KAKA_OKX_ADVANCED_ADL_SUBSCRIPTION_REFRESH_MS || 30_000));
const FOCUS_TARGET = 15;

const OI_HISTORY_NATIVE_PERIOD = '5m';
const OI_HISTORY_OFFICIAL_LIMIT = 100;
const OI_HISTORY_START_DELAY_MS = Math.max(30_000, Number(process.env.KAKA_OKX_OI_HISTORY_START_DELAY_MS || 45_000));
const OI_HISTORY_REFRESH_MS = Math.max(10 * 60_000, Number(process.env.KAKA_OKX_OI_HISTORY_REFRESH_MS || 10 * 60_000));
const OI_HISTORY_STALE_MS = Math.max(10 * 60_000, Number(process.env.KAKA_OKX_OI_HISTORY_STALE_MS || 20 * 60_000));
const OI_HISTORY_RECOVERY_BASE_MS = Math.max(60_000, Number(process.env.KAKA_OKX_OI_HISTORY_RECOVERY_BASE_MS || 60_000));
const OI_HISTORY_RECOVERY_MAX_MS = Math.max(OI_HISTORY_RECOVERY_BASE_MS, Number(process.env.KAKA_OKX_OI_HISTORY_RECOVERY_MAX_MS || 15 * 60_000));
const OI_HISTORY_MAX_5M_ROWS = Math.max(288, Math.min(2_016, Number(process.env.KAKA_OKX_OI_HISTORY_MAX_5M_ROWS || 576)));
const OI_HISTORY_PERSIST_INTERVAL_MS = Math.max(15 * 60_000, Number(process.env.KAKA_OKX_OI_HISTORY_PERSIST_INTERVAL_MS || 30 * 60_000));
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');
const OI_HISTORY_SNAPSHOT_TABLE = 'app_market_backend_snapshots';
const OI_HISTORY_SNAPSHOT_TYPE = 'position_stats';
const OI_HISTORY_SNAPSHOT_PREFIX = 'OKX_OI_HISTORY:';

let started = false;
let focusRunning = null;
let securityFundRunning = null;
let focusTimer = null;
let focusRecoveryTimer = null;
let focusInterval = null;
let securityFundTimer = null;
let securityFundRecoveryTimer = null;
let securityFundInterval = null;
let adlSubscriptionTimer = null;
let adlHeartbeatTimer = null;
let adlReconnectTimer = null;
let adlSocket = null;
let adlReconnectDelayMs = ADL_RECONNECT_MIN_MS;
let adlDesiredSignature = '';
let adlLastSubscribeAt = null;
let adlLastMessageAt = null;
let adlLastPongAt = null;
let adlLastError = '';
let adlConnectAttempts = 0;
let adlReconnects = 0;
let adlMessages = 0;
let adlWarningMessages = 0;
let adlSubscriptionAcks = 0;
let adlSubscriptionErrors = 0;
let totalReads = 0;
let responseCacheHits = 0;
let responseCacheMisses = 0;
let totalFocusBuilds = 0;
let totalFocusFailures = 0;
let totalSecurityFundBuilds = 0;
let totalSecurityFundFailures = 0;
let lastFocusStartedAt = null;
let lastFocusCompletedAt = null;
let lastFocusError = '';
let lastSecurityFundStartedAt = null;
let lastSecurityFundCompletedAt = null;
let lastSecurityFundError = '';
let round = 0;

let oiHistoryRunning = null;
let oiHistoryTimer = null;
let oiHistoryInterval = null;
let oiHistoryRecoveryTimer = null;
let oiHistoryRestorePromise = null;
let oiHistoryRestored = false;
let oiHistoryLastStartedAt = null;
let oiHistoryLastCompletedAt = null;
let oiHistoryLastError = '';
let oiHistoryBuilds = 0;
let oiHistoryFailures = 0;
let oiHistoryRecoveryFailures = 0;
let oiHistoryRecoveryAttempts = 0;
let oiHistoryNextRecoveryAt = 0;
let oiHistoryLastPersistAt = 0;
let oiHistoryPersistAttempts = 0;
let oiHistoryPersistSuccesses = 0;
let oiHistoryPersistErrors = 0;
let oiHistoryRestoreAttempts = 0;
let oiHistoryRestoreSuccesses = 0;
let oiHistoryRestoreErrors = 0;
let oiHistoryLastPersistError = '';
let oiHistoryLastRestoreError = '';

const fundingRows = new Map();
const priceLimitRows = new Map();
const securityFundRows = new Map();
const adlWarningRows = new Map();
const adlSubscribedFamilies = new Set();
const oiHistoryBySymbol = new Map();
const laneStats = new Map();
const responseCache = new Map();

function finite(value) {
  if (value == null) return null;
  if (typeof value === 'string' && value.trim() === '') return null;
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
function sleep(ms) {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    timer.unref?.();
  });
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
    last_status: 0,
    last_rows: 0,
  };
  Object.assign(current, patch);
  laneStats.set(name, current);
}
function splitSymbol(symbol) {
  const normalized = compact(symbol);
  if (normalized.endsWith('USDT')) return { base: normalized.slice(0, -4), quote: 'USDT' };
  return { base: '', quote: '' };
}
function nativeSymbol(symbol) {
  const { base, quote } = splitSymbol(symbol);
  return base && quote ? `${base}-${quote}-SWAP` : '';
}
function instFamily(symbol) {
  const { base, quote } = splitSymbol(symbol);
  return base && quote ? `${base}-${quote}` : '';
}

function okxFocusTargets() {
  const focus = getContractFocusPoolInternalSnapshot();
  const rows = (Array.isArray(focus?.rows) ? focus.rows : [])
    .filter((row) => row?.provider === 'okx' && row?.market_type === 'contract')
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
    unique.push({
      ...row,
      native_symbol: nativeSymbol(row.symbol),
      inst_family: instFamily(row.symbol),
    });
  }
  return {
    focus_ready: focus?.ready === true,
    focus_round: Number(focus?.round || 0),
    rows: unique.slice(0, FOCUS_TARGET),
  };
}

function okxOpenInterestMap() {
  const market = getMarketLightInternalSnapshot({ market: 'contract', provider: 'okx' });
  const out = new Map();
  for (const row of Array.isArray(market?.rows) ? market.rows : []) {
    const symbol = compact(row?.symbol);
    if (!symbol || !symbol.endsWith('USDT')) continue;
    const oi = finite(row?.open_interest);
    const oiValue = finite(row?.open_interest_value);
    if (oi == null && oiValue == null) continue;
    out.set(symbol, {
      open_interest: oi,
      open_interest_value: oiValue,
      open_interest_unit: row?.open_interest_unit || null,
      open_interest_value_unit: row?.open_interest_value_unit || null,
      open_interest_source: row?.open_interest_source || 'okx_public_open_interest_batch',
      source_time: row?.source_time || row?.updated_at || null,
    });
  }
  return {
    ready: market?.ok === true && market?.stale !== true && !String(market?.last_error || '').trim(),
    shared_round: Number(market?.shared_round || 0),
    map: out,
  };
}


function persistenceEnabled() {
  return Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
}

function oiHistorySnapshotKey(symbol) {
  return `${OI_HISTORY_SNAPSHOT_PREFIX}${compact(symbol)}`;
}

function normalizeOiHistoryInterval(raw) {
  const value = String(raw || OI_HISTORY_NATIVE_PERIOD).trim().toLowerCase();
  if (value === '5m') return '5m';
  if (value === '15m') return '15m';
  if (value === '1h' || value === '60m') return '1h';
  if (value === '4h' || value === '240m') return '4h';
  return null;
}

function oiHistoryIntervalMs(interval) {
  if (interval === '15m') return 15 * 60_000;
  if (interval === '1h') return 60 * 60_000;
  if (interval === '4h') return 4 * 60 * 60_000;
  return 5 * 60_000;
}

function parseOiHistoryRow(raw, target) {
  if (!raw) return null;
  let ts = null;
  let oi = null;
  let oiCcy = null;
  let oiUsd = null;
  let responseShape = 'unknown';

  if (Array.isArray(raw)) {
    responseShape = 'array';
    ts = Number(raw[0] || 0) || null;
    oi = finite(raw[1]);
    oiCcy = finite(raw[2]);
    oiUsd = finite(raw[3]);
  } else if (typeof raw === 'object') {
    responseShape = 'object';
    ts = Number(raw.ts ?? raw.timestamp ?? raw.time ?? 0) || null;
    oi = finite(raw.oi ?? raw.openInterest ?? raw.open_interest);
    oiCcy = finite(raw.oiCcy ?? raw.openInterestCcy ?? raw.open_interest_ccy ?? raw.open_interest_base);
    oiUsd = finite(raw.oiUsd ?? raw.openInterestUsd ?? raw.open_interest_usd ?? raw.open_interest_value);
  }

  if (!Number.isFinite(ts) || ts <= 0) return null;
  if (ts < 10_000_000_000) ts *= 1000;
  if (oi == null && oiCcy == null && oiUsd == null) return null;

  return {
    provider: 'okx',
    market_type: 'contract',
    quote_asset: 'USDT',
    symbol: target.symbol,
    native_symbol: target.native_symbol,
    base_asset: target.base_asset,
    interval: OI_HISTORY_NATIVE_PERIOD,
    bucket_time_ms: ts,
    bucket_time: isoMs(ts),
    source_time_ms: ts,
    source_time: isoMs(ts),
    open_interest_contracts: oi,
    open_interest_base: oiCcy,
    open_interest_usd: oiUsd,
    open_interest_contracts_unit: oi != null ? 'contracts' : null,
    open_interest_base_unit: oiCcy != null ? target.base_asset : null,
    open_interest_usd_unit: oiUsd != null ? 'USD' : null,
    official: true,
    derived: false,
    derived_from_official_5m: false,
    official_period: OI_HISTORY_NATIVE_PERIOD,
    official_endpoint: '/api/v5/rubik/stat/contracts/open-interest-history',
    response_shape: responseShape,
    source: 'okx_official_contract_open_interest_history_5m',
    updated_at: new Date().toISOString(),
  };
}

function parseOiHistoryPayload(payload, target) {
  const source = Array.isArray(payload?.data) ? payload.data : [];
  const byTime = new Map();
  const shapes = new Set();
  for (const raw of source) {
    const row = parseOiHistoryRow(raw, target);
    if (!row) continue;
    byTime.set(row.bucket_time_ms, row);
    shapes.add(row.response_shape);
  }
  const rows = [...byTime.values()].sort((a, b) => a.bucket_time_ms - b.bucket_time_ms);
  return {
    rows,
    response_shape: shapes.size === 1 ? [...shapes][0] : shapes.size > 1 ? 'mixed' : 'unknown',
  };
}

function mergeOiHistoryRows(existing, incoming) {
  const byTime = new Map();
  for (const row of [...(existing || []), ...(incoming || [])]) {
    const ts = Number(row?.bucket_time_ms || row?.source_time_ms || 0);
    if (!Number.isFinite(ts) || ts <= 0) continue;
    byTime.set(ts, { ...row, bucket_time_ms: ts, bucket_time: isoMs(ts) });
  }
  return [...byTime.values()]
    .sort((a, b) => a.bucket_time_ms - b.bucket_time_ms)
    .slice(-OI_HISTORY_MAX_5M_ROWS);
}

function oiHistoryEntryFresh(entry) {
  const latest = entry?.rows?.at(-1);
  const ts = Number(latest?.source_time_ms || 0);
  return Number.isFinite(ts) && ts > 0 && Date.now() - ts <= OI_HISTORY_STALE_MS;
}

function deriveOiHistoryRows(rows, interval) {
  if (interval === '5m') return (rows || []).map((row) => ({ ...row }));
  const span = oiHistoryIntervalMs(interval);
  const buckets = new Map();
  for (const row of rows || []) {
    const ts = Number(row?.source_time_ms || row?.bucket_time_ms || 0);
    if (!Number.isFinite(ts) || ts <= 0) continue;
    const bucketStart = Math.floor(ts / span) * span;
    const current = buckets.get(bucketStart);
    if (!current || ts >= Number(current.source_time_ms || 0)) {
      buckets.set(bucketStart, {
        ...row,
        interval,
        bucket_time_ms: bucketStart,
        bucket_time: isoMs(bucketStart),
        official: false,
        derived: true,
        derived_from_official_5m: true,
        official_period: OI_HISTORY_NATIVE_PERIOD,
        derived_method: 'last_official_5m_observation_in_bucket',
        source: `okx_derived_${interval}_from_official_5m_open_interest_history`,
      });
    }
  }
  return [...buckets.values()].sort((a, b) => a.bucket_time_ms - b.bucket_time_ms);
}

function oiHistoryRecoveryDelayMs(failures) {
  const safe = Math.max(1, Number(failures || 1));
  const factor = 2 ** Math.min(4, safe - 1);
  return Math.min(OI_HISTORY_RECOVERY_MAX_MS, OI_HISTORY_RECOVERY_BASE_MS * factor);
}

function oiHistoryTargets() {
  const focus = okxFocusTargets();
  return {
    focus_ready: focus.focus_ready,
    focus_round: focus.focus_round,
    rows: focus.rows,
  };
}

function oiHistoryCoverage(targets = oiHistoryTargets()) {
  const covered = targets.rows.filter((target) => {
    const entry = oiHistoryBySymbol.get(target.symbol);
    return Array.isArray(entry?.rows) && entry.rows.length > 0;
  });
  const fresh = targets.rows.filter((target) => oiHistoryEntryFresh(oiHistoryBySymbol.get(target.symbol)));
  return {
    target_count: targets.rows.length,
    covered_count: covered.length,
    fresh_count: fresh.length,
    missing_symbols: targets.rows.filter((target) => !oiHistoryBySymbol.get(target.symbol)?.rows?.length).map((target) => target.symbol),
    stale_symbols: targets.rows.filter((target) => {
      const entry = oiHistoryBySymbol.get(target.symbol);
      return entry?.rows?.length && !oiHistoryEntryFresh(entry);
    }).map((target) => target.symbol),
  };
}

async function restoreOiHistorySnapshots() {
  if (oiHistoryRestored) return true;
  if (!persistenceEnabled()) {
    oiHistoryRestored = true;
    return false;
  }
  if (oiHistoryRestorePromise) return await oiHistoryRestorePromise;
  oiHistoryRestorePromise = (async () => {
    oiHistoryRestoreAttempts += 1;
    try {
      const query = [
        'select=quote_asset,payload,updated_at',
        'provider=eq.okx',
        'market_type=eq.contract',
        `snapshot_type=eq.${encodeURIComponent(OI_HISTORY_SNAPSHOT_TYPE)}`,
        'source=eq.okx_official_open_interest_history_focus15_shared',
        'limit=30',
      ].join('&');
      const response = await fetch(`${SUPABASE_URL}/rest/v1/${OI_HISTORY_SNAPSHOT_TABLE}?${query}`, {
        headers: {
          apikey: SUPABASE_SERVICE_ROLE_KEY,
          authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          accept: 'application/json',
        },
        signal: AbortSignal.timeout(8_000),
      });
      if (!response.ok) throw new Error(`okx_oi_history_restore_http_${response.status}`);
      const records = await response.json();
      let restored = 0;
      for (const record of Array.isArray(records) ? records : []) {
        const symbol = compact(record?.payload?.symbol || String(record?.quote_asset || '').replace(OI_HISTORY_SNAPSHOT_PREFIX, ''));
        if (!symbol || !symbol.endsWith('USDT')) continue;
        const target = {
          symbol,
          native_symbol: nativeSymbol(symbol),
          base_asset: splitSymbol(symbol).base,
        };
        const normalized = [];
        for (const raw of Array.isArray(record?.payload?.rows) ? record.payload.rows : []) {
          const row = parseOiHistoryRow(raw, target);
          if (row) normalized.push(row);
        }
        if (!normalized.length) continue;
        oiHistoryBySymbol.set(symbol, {
          symbol,
          native_symbol: target.native_symbol,
          base_asset: target.base_asset,
          rows: mergeOiHistoryRows([], normalized),
          response_shape: String(record?.payload?.response_shape || 'persisted_normalized'),
          updated_at: record?.updated_at || new Date().toISOString(),
          restored: true,
        });
        restored += 1;
      }
      oiHistoryRestoreSuccesses += restored > 0 ? 1 : 0;
      oiHistoryLastRestoreError = '';
      return restored > 0;
    } catch (error) {
      oiHistoryRestoreErrors += 1;
      oiHistoryLastRestoreError = String(error?.message || error).slice(0, 320);
      return false;
    } finally {
      oiHistoryRestored = true;
    }
  })().finally(() => {
    oiHistoryRestorePromise = null;
  });
  return await oiHistoryRestorePromise;
}

async function persistOiHistorySnapshots({ force = false } = {}) {
  if (!persistenceEnabled()) return false;
  if (!force && oiHistoryLastPersistAt > 0 && Date.now() - oiHistoryLastPersistAt < OI_HISTORY_PERSIST_INTERVAL_MS) return true;
  const targets = oiHistoryTargets();
  const body = targets.rows.map((target) => {
    const entry = oiHistoryBySymbol.get(target.symbol);
    if (!entry?.rows?.length) return null;
    const now = new Date().toISOString();
    return {
      provider: 'okx',
      market_type: 'contract',
      snapshot_type: OI_HISTORY_SNAPSHOT_TYPE,
      quote_asset: oiHistorySnapshotKey(target.symbol),
      payload: {
        schema_version: '650.8.15.116',
        symbol: target.symbol,
        native_symbol: target.native_symbol,
        base_asset: target.base_asset,
        official_period: OI_HISTORY_NATIVE_PERIOD,
        official_endpoint: '/api/v5/rubik/stat/contracts/open-interest-history',
        response_shape: entry.response_shape || 'unknown',
        rows: entry.rows.slice(-OI_HISTORY_MAX_5M_ROWS),
      },
      row_count: entry.rows.length,
      source: 'okx_official_open_interest_history_focus15_shared',
      source_time: entry.rows.at(-1)?.source_time || now,
      updated_at: now,
    };
  }).filter(Boolean);
  if (!body.length) return false;

  oiHistoryPersistAttempts += 1;
  try {
    const response = await fetch(
      `${SUPABASE_URL}/rest/v1/${OI_HISTORY_SNAPSHOT_TABLE}?on_conflict=provider,market_type,snapshot_type,quote_asset`,
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
    if (!response.ok) throw new Error(`okx_oi_history_persist_http_${response.status}`);
    oiHistoryLastPersistAt = Date.now();
    oiHistoryPersistSuccesses += 1;
    oiHistoryLastPersistError = '';
    return true;
  } catch (error) {
    oiHistoryPersistErrors += 1;
    oiHistoryLastPersistError = String(error?.message || error).slice(0, 320);
    return false;
  }
}

function scheduleOiHistoryRecovery() {
  const coverage = oiHistoryCoverage();
  if (coverage.target_count === FOCUS_TARGET && coverage.fresh_count === FOCUS_TARGET) {
    oiHistoryRecoveryFailures = 0;
    oiHistoryNextRecoveryAt = 0;
    if (oiHistoryRecoveryTimer) clearTimeout(oiHistoryRecoveryTimer);
    oiHistoryRecoveryTimer = null;
    return;
  }
  if (oiHistoryRecoveryTimer) return;
  oiHistoryRecoveryFailures += 1;
  const delay = oiHistoryRecoveryDelayMs(oiHistoryRecoveryFailures);
  oiHistoryNextRecoveryAt = Date.now() + delay;
  oiHistoryRecoveryTimer = setTimeout(async () => {
    oiHistoryRecoveryTimer = null;
    oiHistoryRecoveryAttempts += 1;
    await refreshOiHistory('recovery_missing_only', { missingOnly: true }).catch(() => false);
    const next = oiHistoryCoverage();
    if (next.target_count !== FOCUS_TARGET || next.fresh_count !== FOCUS_TARGET) scheduleOiHistoryRecovery();
  }, delay);
  oiHistoryRecoveryTimer.unref?.();
}

async function refreshOiHistory(reason = 'scheduled', { missingOnly = true } = {}) {
  if (oiHistoryRunning) return await oiHistoryRunning;
  const task = (async () => {
    oiHistoryLastStartedAt = new Date().toISOString();
    oiHistoryBuilds += 1;
    await restoreOiHistorySnapshots().catch(() => false);

    const targets = oiHistoryTargets();
    if (!targets.focus_ready || targets.rows.length !== FOCUS_TARGET) {
      oiHistoryFailures += 1;
      oiHistoryLastError = `${reason}:okx_oi_history_focus_not_ready:${targets.rows.length}/${FOCUS_TARGET}`;
      scheduleOiHistoryRecovery();
      return false;
    }

    const requestTargets = missingOnly
      ? targets.rows.filter((target) => !oiHistoryEntryFresh(oiHistoryBySymbol.get(target.symbol)))
      : targets.rows;

    let successes = 0;
    const errors = [];
    for (const target of requestTargets) {
      try {
        const path = `/api/v5/rubik/stat/contracts/open-interest-history?instId=${encodeURIComponent(target.native_symbol)}&period=${encodeURIComponent(OI_HISTORY_NATIVE_PERIOD)}&limit=${OI_HISTORY_OFFICIAL_LIMIT}`;
        const payload = await fetchJson(path, { lane: 'open_interest_history_5m', timeoutMs: 20_000 });
        const parsed = parseOiHistoryPayload(payload, target);
        if (!parsed.rows.length) throw new Error(`okx_oi_history_payload_empty:${target.symbol}`);
        const previous = oiHistoryBySymbol.get(target.symbol);
        oiHistoryBySymbol.set(target.symbol, {
          symbol: target.symbol,
          native_symbol: target.native_symbol,
          base_asset: target.base_asset,
          rows: mergeOiHistoryRows(previous?.rows || [], parsed.rows),
          response_shape: parsed.response_shape,
          updated_at: new Date().toISOString(),
          restored: previous?.restored === true,
        });
        successes += 1;
      } catch (error) {
        errors.push(`${target.symbol}:${String(error?.message || error)}`);
      }
      await sleep(PER_REQUEST_GAP_MS);
    }

    const coverage = oiHistoryCoverage(targets);
    setLane('open_interest_history_5m', {
      last_rows: coverage.covered_count,
      last_error: errors.join(' | ').slice(0, 700),
    });

    oiHistoryLastCompletedAt = new Date().toISOString();
    oiHistoryLastError = errors.join(' | ').slice(0, 700);
    if (errors.length > 0 && successes === 0 && requestTargets.length > 0) oiHistoryFailures += 1;
    if (coverage.covered_count === FOCUS_TARGET) {
      await persistOiHistorySnapshots({ force: oiHistoryLastPersistAt === 0 }).catch(() => false);
    }
    responseCache.clear();

    if (coverage.fresh_count !== FOCUS_TARGET) scheduleOiHistoryRecovery();
    else {
      oiHistoryRecoveryFailures = 0;
      oiHistoryNextRecoveryAt = 0;
    }
    return coverage.covered_count === FOCUS_TARGET;
  })();
  oiHistoryRunning = task;
  try {
    return await task;
  } finally {
    if (oiHistoryRunning === task) oiHistoryRunning = null;
  }
}

function oiHistoryHealthPayload() {
  const targets = oiHistoryTargets();
  const coverage = oiHistoryCoverage(targets);
  const entries = targets.rows.map((target) => {
    const entry = oiHistoryBySymbol.get(target.symbol);
    const rows = entry?.rows || [];
    const latest = rows.at(-1) || null;
    return {
      symbol: target.symbol,
      native_symbol: target.native_symbol,
      row_count: rows.length,
      fresh: oiHistoryEntryFresh(entry),
      response_shape: entry?.response_shape || null,
      latest_source_time: latest?.source_time || null,
      latest_open_interest_contracts: latest?.open_interest_contracts ?? null,
      latest_open_interest_base: latest?.open_interest_base ?? null,
      latest_open_interest_usd: latest?.open_interest_usd ?? null,
      restored: entry?.restored === true,
    };
  });
  const totalRows = entries.reduce((sum, entry) => sum + Number(entry.row_count || 0), 0);
  return {
    ready: targets.focus_ready && targets.rows.length === FOCUS_TARGET && coverage.covered_count === FOCUS_TARGET,
    focus_target: FOCUS_TARGET,
    focus_ready: targets.focus_ready,
    focus_round: targets.focus_round,
    focus_symbols: targets.rows.map((target) => target.symbol),
    official_5m_coverage: coverage.covered_count,
    fresh_5m_coverage: coverage.fresh_count,
    total_official_5m_rows: totalRows,
    official_endpoint: '/api/v5/rubik/stat/contracts/open-interest-history',
    official_period: OI_HISTORY_NATIVE_PERIOD,
    official_limit_per_request: OI_HISTORY_OFFICIAL_LIMIT,
    official_rate_limit: '10 requests per 2 seconds / IP + Instrument ID',
    full_cycle_request_cap: FOCUS_TARGET,
    request_model: 'one official 5m request per stale focus symbol; higher intervals are backend rollups',
    derived_intervals: ['15m', '1h', '4h'],
    derived_method: 'last_official_5m_observation_in_bucket',
    official_and_derived_separate: true,
    refresh_seconds: Math.round(OI_HISTORY_REFRESH_MS / 1000),
    stale_seconds: Math.round(OI_HISTORY_STALE_MS / 1000),
    per_request_gap_ms: PER_REQUEST_GAP_MS,
    startup_delay_seconds: Math.round(OI_HISTORY_START_DELAY_MS / 1000),
    recovery_base_seconds: Math.round(OI_HISTORY_RECOVERY_BASE_MS / 1000),
    recovery_max_seconds: Math.round(OI_HISTORY_RECOVERY_MAX_MS / 1000),
    recovery_attempts: oiHistoryRecoveryAttempts,
    recovery_failures: oiHistoryRecoveryFailures,
    next_recovery_at: oiHistoryNextRecoveryAt > 0 ? new Date(oiHistoryNextRecoveryAt).toISOString() : null,
    missing_symbols: coverage.missing_symbols,
    stale_symbols: coverage.stale_symbols,
    shared_background_collector: true,
    shared_process_memory: true,
    persistence_enabled: persistenceEnabled(),
    persistence_table: OI_HISTORY_SNAPSHOT_TABLE,
    persistence_snapshot_type: OI_HISTORY_SNAPSHOT_TYPE,
    persistence_one_batch_write_per_interval: true,
    persist_interval_seconds: Math.round(OI_HISTORY_PERSIST_INTERVAL_MS / 1000),
    persist_attempts: oiHistoryPersistAttempts,
    persist_successes: oiHistoryPersistSuccesses,
    persist_errors: oiHistoryPersistErrors,
    last_persist_error: oiHistoryLastPersistError,
    restore_attempts: oiHistoryRestoreAttempts,
    restore_successes: oiHistoryRestoreSuccesses,
    restore_errors: oiHistoryRestoreErrors,
    last_restore_error: oiHistoryLastRestoreError,
    exchange_requests_started_by_user_read: 0,
    exchange_connections_started_by_user_read: 0,
    user_reads_trigger_collector: false,
    reads_scale_with_users: false,
    last_started_at: oiHistoryLastStartedAt,
    last_completed_at: oiHistoryLastCompletedAt,
    last_error: oiHistoryLastError,
    builds: oiHistoryBuilds,
    failures: oiHistoryFailures,
    symbols: entries,
  };
}

function oiHistoryReadPayload(symbol, interval = '5m', limit = 200) {
  const normalized = compact(symbol);
  const normalizedInterval = normalizeOiHistoryInterval(interval);
  const cappedLimit = Math.max(1, Math.min(OI_HISTORY_MAX_5M_ROWS, Number(limit || 200)));
  const target = okxFocusTargets().rows.find((row) => row.symbol === normalized);
  const entry = oiHistoryBySymbol.get(normalized);
  const baseRows = entry?.rows || [];
  const rows = normalizedInterval ? deriveOiHistoryRows(baseRows, normalizedInterval).slice(-cappedLimit) : [];
  return {
    ok: true,
    version: VERSION,
    provider: 'okx',
    market_type: 'contract',
    symbol: normalized,
    native_symbol: target?.native_symbol || nativeSymbol(normalized),
    interval: normalizedInterval,
    ready: Boolean(target && normalizedInterval && rows.length > 0),
    official: normalizedInterval === '5m',
    derived: normalizedInterval !== '5m',
    derived_from_official_5m: normalizedInterval !== '5m',
    official_base_period: OI_HISTORY_NATIVE_PERIOD,
    official_endpoint: '/api/v5/rubik/stat/contracts/open-interest-history',
    row_count: rows.length,
    rows: rows.map(clone),
    shared_backend_read: true,
    user_read_triggered_exchange_requests: false,
    user_read_triggered_exchange_connections: false,
    reads_scale_with_users: false,
    timestamp_ms: Date.now(),
  };
}

async function fetchJson(path, { lane = 'unknown', timeoutMs = 18_000 } = {}) {
  const startedAt = Date.now();
  setLane(lane, {
    attempts: Number(laneStats.get(lane)?.attempts || 0) + 1,
    last_started_at: new Date(startedAt).toISOString(),
  });
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  timer.unref?.();
  try {
    const response = await fetch(`${BASE}${path}`, {
      headers: {
        accept: 'application/json',
        'user-agent': 'KakaWeb3/650.8.15.116 okx-advanced-shared',
      },
      signal: controller.signal,
    });
    const text = await response.text();
    let payload = null;
    try { payload = JSON.parse(text); } catch (_) {}
    if (!response.ok) throw new Error(`okx_http_${response.status}:${lane}`);
    if (String(payload?.code ?? '0') !== '0') {
      throw new Error(`okx_code_${payload?.code ?? 'unknown'}:${String(payload?.msg || '')}:${lane}`);
    }
    setLane(lane, {
      successes: Number(laneStats.get(lane)?.successes || 0) + 1,
      last_status: response.status,
      last_completed_at: new Date().toISOString(),
      last_error: '',
    });
    return payload;
  } catch (error) {
    setLane(lane, {
      failures: Number(laneStats.get(lane)?.failures || 0) + 1,
      last_completed_at: new Date().toISOString(),
      last_error: String(error?.message || error),
    });
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

function parseFunding(payload, target) {
  const row = Array.isArray(payload?.data) ? payload.data[0] : null;
  if (!row || compact(row?.instId) !== compact(target.native_symbol)) return null;
  const fundingRate = finite(row?.fundingRate);
  const fundingTimeMs = Number(row?.fundingTime || 0) || null;
  const sourceTimeMs = Number(row?.ts || 0) || null;
  if (fundingRate == null || fundingTimeMs == null) return null;
  return {
    provider: 'okx',
    market_type: 'contract',
    quote_asset: 'USDT',
    symbol: target.symbol,
    native_symbol: target.native_symbol,
    inst_family: target.inst_family,
    base_asset: target.base_asset,
    focus_role: target.role,
    focus_slot: target.slot,
    funding_rate: fundingRate,
    funding_time_ms: fundingTimeMs,
    funding_time: isoMs(fundingTimeMs),
    next_funding_rate: finite(row?.nextFundingRate),
    next_funding_time_ms: Number(row?.nextFundingTime || 0) || null,
    next_funding_time: isoMs(row?.nextFundingTime),
    settled_funding_rate: finite(row?.settFundingRate),
    settlement_state: String(row?.settState || '') || null,
    interest_rate: finite(row?.interestRate),
    premium: finite(row?.premium),
    min_funding_rate: finite(row?.minFundingRate),
    max_funding_rate: finite(row?.maxFundingRate),
    formula_type: String(row?.formulaType || '') || null,
    method: String(row?.method || '') || null,
    source_time_ms: sourceTimeMs,
    source_time: isoMs(sourceTimeMs),
    official_endpoint: '/api/v5/public/funding-rate',
    source: 'okx_official_public_funding_rate_focus15',
    updated_at: new Date().toISOString(),
  };
}

function parsePriceLimit(payload, target) {
  const row = Array.isArray(payload?.data) ? payload.data[0] : null;
  if (!row || compact(row?.instId) !== compact(target.native_symbol)) return null;
  const enabled = row?.enabled === true || String(row?.enabled).toLowerCase() === 'true';
  const buyLimit = finite(row?.buyLmt);
  const sellLimit = finite(row?.sellLmt);
  const sourceTimeMs = Number(row?.ts || 0) || null;
  if (enabled && (buyLimit == null || sellLimit == null)) return null;
  return {
    provider: 'okx',
    market_type: 'contract',
    quote_asset: 'USDT',
    symbol: target.symbol,
    native_symbol: target.native_symbol,
    inst_family: target.inst_family,
    base_asset: target.base_asset,
    focus_role: target.role,
    focus_slot: target.slot,
    enabled,
    highest_buy_limit: buyLimit,
    lowest_sell_limit: sellLimit,
    source_time_ms: sourceTimeMs,
    source_time: isoMs(sourceTimeMs),
    official_endpoint: '/api/v5/public/price-limit',
    source: 'okx_official_public_price_limit_focus15',
    updated_at: new Date().toISOString(),
  };
}

function parseSecurityFund(payload, requestedFamily = '') {
  const rows = Array.isArray(payload?.data) ? payload.data : [];
  const requested = String(requestedFamily || '').trim().toUpperCase();
  if (!requested || rows.length === 0) return null;

  const row = rows.find((item) => {
    const family = String(item?.instFamily || item?.uly || '').trim().toUpperCase();
    return family && family === requested;
  }) || rows[0];

  const type = String(row?.instType || 'SWAP').toUpperCase();
  if (type && type !== 'SWAP') return null;

  const responseFamily = String(row?.instFamily || row?.uly || '').trim().toUpperCase();
  if (responseFamily && responseFamily !== requested) return null;

  const details = (Array.isArray(row?.details) ? row.details : [])
    .map((item) => ({
      balance: finite(item?.balance),
      amount_change: finite(item?.amt),
      currency: String(item?.ccy || '') || null,
      type: String(item?.type || '') || null,
      ts: Number(item?.ts || 0) || null,
      source_time: isoMs(item?.ts),
    }))
    .filter((item) => item.balance != null || item.amount_change != null || item.ts != null)
    .sort((a, b) => Number(b.ts || 0) - Number(a.ts || 0));

  const latest = details[0] || null;
  const total = finite(row?.total);
  if (total == null && details.length === 0) return null;

  return {
    inst_type: 'SWAP',
    inst_family: requested,
    requested_uly: requested,
    response_family: responseFamily || null,
    total_balance: total,
    total_usd: total,
    latest_balance: latest?.balance ?? null,
    latest_amount_change: latest?.amount_change ?? null,
    latest_currency: latest?.currency ?? null,
    latest_type: latest?.type ?? null,
    latest_time_ms: latest?.ts ?? null,
    latest_time: latest?.source_time ?? null,
    detail_count: details.length,
    details,
    official_endpoint: '/api/v5/public/insurance-fund',
    official_query_scope: 'instType=SWAP + uly=<focus instFamily>',
    source: 'okx_official_public_security_fund_focus_family_slow_shared',
    updated_at: new Date().toISOString(),
  };
}

function securityFundFocusState() {
  const focus = okxFocusTargets();
  const targets = focus.rows;
  const readyTargets = targets.filter((target) =>
    isFresh(securityFundRows.get(target.inst_family)?.updated_at, SECURITY_FUND_STALE_MS)
  );
  return {
    focus_ready: focus.focus_ready,
    target_count: targets.length,
    ready_count: readyTargets.length,
    missing_targets: targets.filter((target) =>
      !isFresh(securityFundRows.get(target.inst_family)?.updated_at, SECURITY_FUND_STALE_MS)
    ),
  };
}

function parseAdlWarningPush(payload) {
  const arg = payload?.arg || {};
  if (String(arg?.channel || '') !== 'adl-warning') return [];
  const rows = Array.isArray(payload?.data) ? payload.data : [];
  return rows.map((row) => {
    const family = String(row?.instFamily || arg?.instFamily || '').trim().toUpperCase();
    const state = String(row?.state || '').trim().toLowerCase();
    if (!family || !['warning', 'adl'].includes(state)) return null;
    const ts = Number(row?.ts || 0) || Date.now();
    return {
      inst_type: String(row?.instType || arg?.instType || 'SWAP').toUpperCase(),
      inst_family: family,
      state,
      security_fund_balance: finite(row?.bal),
      ts,
      source_time: isoMs(ts),
      official_channel: 'adl-warning',
      source: 'okx_official_public_adl_warning_channel',
      observed_at: new Date().toISOString(),
    };
  }).filter(Boolean);
}

function missingFocusTargets(targets) {
  return targets.filter((target) => {
    const funding = fundingRows.get(target.symbol);
    const limit = priceLimitRows.get(target.symbol);
    return !funding || !isFresh(funding.updated_at, FOCUS_STALE_MS) || !limit || !isFresh(limit.updated_at, FOCUS_STALE_MS);
  });
}

async function refreshFocusStats(reason = 'scheduled', { missingOnly = false } = {}) {
  if (focusRunning) return await focusRunning;
  const task = (async () => {
    lastFocusStartedAt = new Date().toISOString();
    lastFocusError = '';
    totalFocusBuilds += 1;
    const focus = okxFocusTargets();
    if (!focus.focus_ready || focus.rows.length !== FOCUS_TARGET) {
      const error = new Error(`okx_focus_not_ready:${focus.rows.length}/${FOCUS_TARGET}`);
      lastFocusError = error.message;
      totalFocusFailures += 1;
      throw error;
    }
    const targets = missingOnly ? missingFocusTargets(focus.rows) : focus.rows;
    let successes = 0;
    let failures = 0;
    for (const target of targets) {
      try {
        const payload = await fetchJson(`/api/v5/public/funding-rate?instId=${encodeURIComponent(target.native_symbol)}`, { lane: 'funding_rate' });
        const parsed = parseFunding(payload, target);
        if (!parsed) throw new Error(`okx_funding_payload_invalid:${target.symbol}`);
        fundingRows.set(target.symbol, parsed);
        successes += 1;
      } catch (error) {
        failures += 1;
        lastFocusError = String(error?.message || error);
      }
      await sleep(PER_REQUEST_GAP_MS);
      try {
        const payload = await fetchJson(`/api/v5/public/price-limit?instId=${encodeURIComponent(target.native_symbol)}`, { lane: 'price_limit' });
        const parsed = parsePriceLimit(payload, target);
        if (!parsed) throw new Error(`okx_price_limit_payload_invalid:${target.symbol}`);
        priceLimitRows.set(target.symbol, parsed);
        successes += 1;
      } catch (error) {
        failures += 1;
        lastFocusError = String(error?.message || error);
      }
      await sleep(PER_REQUEST_GAP_MS);
    }
    setLane('funding_rate', { last_rows: focus.rows.filter((target) => isFresh(fundingRows.get(target.symbol)?.updated_at, FOCUS_STALE_MS)).length });
    setLane('price_limit', { last_rows: focus.rows.filter((target) => isFresh(priceLimitRows.get(target.symbol)?.updated_at, FOCUS_STALE_MS)).length });
    if (failures > 0 && successes === 0) totalFocusFailures += 1;
    lastFocusCompletedAt = new Date().toISOString();
    round += 1;
    if (!securityFundStartupReady()) scheduleSecurityFundStartupRecovery();
    responseCache.clear();
    return successes > 0 || targets.length === 0;
  })();
  focusRunning = task;
  try {
    return await task;
  } finally {
    if (focusRunning === task) focusRunning = null;
  }
}

async function refreshSecurityFund(reason = 'scheduled', { missingOnly = false } = {}) {
  if (securityFundRunning) return await securityFundRunning;
  const task = (async () => {
    lastSecurityFundStartedAt = new Date().toISOString();
    lastSecurityFundError = '';
    totalSecurityFundBuilds += 1;

    const focus = okxFocusTargets();
    if (!focus.focus_ready || focus.rows.length !== FOCUS_TARGET) {
      const error = new Error(`okx_security_fund_focus_not_ready:${focus.rows.length}/${FOCUS_TARGET}`);
      totalSecurityFundFailures += 1;
      lastSecurityFundError = `${reason}:${error.message}`;
      return false;
    }

    const targets = missingOnly
      ? focus.rows.filter((target) =>
          !isFresh(securityFundRows.get(target.inst_family)?.updated_at, SECURITY_FUND_STALE_MS)
        )
      : focus.rows;

    let successes = 0;
    let failures = 0;
    for (const target of targets) {
      try {
        const path = `/api/v5/public/insurance-fund?instType=SWAP&uly=${encodeURIComponent(target.inst_family)}`;
        const payload = await fetchJson(path, { lane: 'security_fund', timeoutMs: 20_000 });
        const parsed = parseSecurityFund(payload, target.inst_family);
        if (!parsed) throw new Error(`okx_security_fund_payload_invalid:${target.inst_family}`);
        securityFundRows.set(target.inst_family, parsed);
        successes += 1;
      } catch (error) {
        failures += 1;
        lastSecurityFundError = `${reason}:${String(error?.message || error)}`.slice(0, 320);
      }
      await sleep(PER_REQUEST_GAP_MS);
    }

    const state = securityFundFocusState();
    setLane('security_fund', { last_rows: state.ready_count });
    if (failures > 0 && successes === 0) totalSecurityFundFailures += 1;
    if (successes > 0) {
      lastSecurityFundCompletedAt = new Date().toISOString();
      responseCache.clear();
    }

    if (!state.focus_ready || state.target_count !== FOCUS_TARGET || state.ready_count !== FOCUS_TARGET) {
      if (!lastSecurityFundError) {
        lastSecurityFundError = `${reason}:okx_security_fund_focus_coverage:${state.ready_count}/${FOCUS_TARGET}`;
      }
      return false;
    }

    lastSecurityFundError = '';
    return true;
  })();

  securityFundRunning = task;
  try {
    return await task;
  } finally {
    if (securityFundRunning === task) securityFundRunning = null;
  }
}

function adlDesiredFamilies() {
  return okxFocusTargets().rows.map((row) => row.inst_family).filter(Boolean);
}
function adlSignature(families) {
  return [...new Set(families)].sort().join('|');
}
function wsOpen() {
  return adlSocket && adlSocket.readyState === WebSocket.OPEN;
}
function clearAdlHeartbeat() {
  if (adlHeartbeatTimer) clearInterval(adlHeartbeatTimer);
  adlHeartbeatTimer = null;
}
function scheduleAdlReconnect() {
  if (!started || process.env.KAKA_DISABLE_OKX_ADVANCED_ADL_WS === '1' || adlReconnectTimer) return;
  const delay = adlReconnectDelayMs;
  adlReconnectTimer = setTimeout(() => {
    adlReconnectTimer = null;
    adlReconnects += 1;
    connectAdlWarningWs();
  }, delay);
  adlReconnectTimer.unref?.();
  adlReconnectDelayMs = Math.min(ADL_RECONNECT_MAX_MS, Math.max(ADL_RECONNECT_MIN_MS, delay * 2));
}
function sendWs(payload) {
  if (!wsOpen()) return false;
  try {
    adlSocket.send(JSON.stringify(payload));
    return true;
  } catch (error) {
    adlLastError = String(error?.message || error);
    return false;
  }
}
function syncAdlSubscriptions() {
  if (!wsOpen()) return false;
  const desiredFamilies = adlDesiredFamilies();
  if (desiredFamilies.length !== FOCUS_TARGET) return false;
  const desired = new Set(desiredFamilies);
  const remove = [...adlSubscribedFamilies].filter((family) => !desired.has(family));
  const add = [...desired].filter((family) => !adlSubscribedFamilies.has(family));
  if (remove.length) {
    sendWs({ op: 'unsubscribe', args: remove.map((family) => ({ channel: 'adl-warning', instType: 'SWAP', instFamily: family })) });
  }
  if (add.length) {
    sendWs({ op: 'subscribe', args: add.map((family) => ({ channel: 'adl-warning', instType: 'SWAP', instFamily: family })) });
    adlLastSubscribeAt = new Date().toISOString();
  }
  adlDesiredSignature = adlSignature(desiredFamilies);
  return true;
}
function connectAdlWarningWs() {
  if (!started || process.env.KAKA_DISABLE_OKX_ADVANCED_ADL_WS === '1') return;
  if (adlSocket && [WebSocket.OPEN, WebSocket.CONNECTING].includes(adlSocket.readyState)) return;
  adlConnectAttempts += 1;
  const ws = new WebSocket(WS_URL, { handshakeTimeout: 15_000 });
  adlSocket = ws;
  ws.on('open', () => {
    adlReconnectDelayMs = ADL_RECONNECT_MIN_MS;
    adlLastError = '';
    adlSubscribedFamilies.clear();
    syncAdlSubscriptions();
    clearAdlHeartbeat();
    adlHeartbeatTimer = setInterval(() => {
      if (!wsOpen()) return;
      try { adlSocket.send('ping'); } catch (_) {}
    }, ADL_HEARTBEAT_MS);
    adlHeartbeatTimer.unref?.();
  });
  ws.on('message', (buffer) => {
    const text = String(buffer || '');
    adlLastMessageAt = new Date().toISOString();
    adlMessages += 1;
    if (text === 'pong') {
      adlLastPongAt = adlLastMessageAt;
      return;
    }
    let payload = null;
    try { payload = JSON.parse(text); } catch (_) { return; }
    if (payload?.event === 'subscribe') {
      const family = String(payload?.arg?.instFamily || '').toUpperCase();
      if (payload?.arg?.channel === 'adl-warning' && family) {
        adlSubscribedFamilies.add(family);
        adlSubscriptionAcks += 1;
        responseCache.clear();
      }
      return;
    }
    if (payload?.event === 'unsubscribe') {
      const family = String(payload?.arg?.instFamily || '').toUpperCase();
      if (family) adlSubscribedFamilies.delete(family);
      responseCache.clear();
      return;
    }
    if (payload?.event === 'error') {
      adlSubscriptionErrors += 1;
      adlLastError = `okx_adl_ws_${payload?.code || 'error'}:${String(payload?.msg || '')}`;
      return;
    }
    const rows = parseAdlWarningPush(payload);
    for (const row of rows) {
      adlWarningRows.set(row.inst_family, row);
      adlWarningMessages += 1;
    }
    if (rows.length) responseCache.clear();
  });
  ws.on('error', (error) => {
    adlLastError = String(error?.message || error);
  });
  ws.on('close', () => {
    if (adlSocket === ws) adlSocket = null;
    adlSubscribedFamilies.clear();
    clearAdlHeartbeat();
    responseCache.clear();
    scheduleAdlReconnect();
  });
}

function focusStartupReady() {
  const focus = okxFocusTargets();
  if (!focus.focus_ready || focus.rows.length !== FOCUS_TARGET) return false;
  return missingFocusTargets(focus.rows).length === 0;
}
function scheduleFocusStartupRecovery() {
  if (focusRecoveryTimer || focusStartupReady()) return;
  focusRecoveryTimer = setTimeout(async () => {
    focusRecoveryTimer = null;
    await refreshFocusStats('startup_recovery_missing_only', { missingOnly: true }).catch(() => false);
    if (!focusStartupReady()) scheduleFocusStartupRecovery();
  }, STARTUP_RETRY_MS);
  focusRecoveryTimer.unref?.();
}
function securityFundStartupReady() {
  const state = securityFundFocusState();
  return state.focus_ready && state.target_count === FOCUS_TARGET && state.ready_count === FOCUS_TARGET;
}
function scheduleSecurityFundStartupRecovery() {
  if (securityFundRecoveryTimer || securityFundStartupReady()) return;
  securityFundRecoveryTimer = setTimeout(async () => {
    securityFundRecoveryTimer = null;
    await refreshSecurityFund('startup_recovery_missing_only', { missingOnly: true }).catch(() => false);
    if (!securityFundStartupReady()) scheduleSecurityFundStartupRecovery();
  }, Math.max(30_000, STARTUP_RETRY_MS * 2));
  securityFundRecoveryTimer.unref?.();
}

export function startOkxAdvancedStatsScanner() {
  if (started || process.env.KAKA_DISABLE_OKX_ADVANCED_STATS === '1') return;
  started = true;

  focusTimer = setTimeout(async () => {
    await refreshFocusStats('startup').catch(() => false);
    if (!focusStartupReady()) scheduleFocusStartupRecovery();
  }, START_DELAY_MS);
  focusTimer.unref?.();

  securityFundTimer = setTimeout(async () => {
    await refreshSecurityFund('startup').catch(() => false);
    if (!securityFundStartupReady()) scheduleSecurityFundStartupRecovery();
  }, START_DELAY_MS + 1_500);
  securityFundTimer.unref?.();

  focusInterval = setInterval(() => refreshFocusStats('interval').catch(() => {}), FOCUS_REFRESH_MS);
  focusInterval.unref?.();
  securityFundInterval = setInterval(() => refreshSecurityFund('interval').catch(() => {}), SECURITY_FUND_REFRESH_MS);
  securityFundInterval.unref?.();

  oiHistoryTimer = setTimeout(async () => {
    await refreshOiHistory('startup_missing_only', { missingOnly: true }).catch(() => false);
    const coverage = oiHistoryCoverage();
    if (coverage.target_count !== FOCUS_TARGET || coverage.fresh_count !== FOCUS_TARGET) scheduleOiHistoryRecovery();
  }, OI_HISTORY_START_DELAY_MS);
  oiHistoryTimer.unref?.();

  oiHistoryInterval = setInterval(async () => {
    await refreshOiHistory('interval_missing_only', { missingOnly: true }).catch(() => false);
    await persistOiHistorySnapshots().catch(() => false);
  }, OI_HISTORY_REFRESH_MS);
  oiHistoryInterval.unref?.();

  const adlStart = setTimeout(() => connectAdlWarningWs(), START_DELAY_MS + 2_500);
  adlStart.unref?.();
  adlSubscriptionTimer = setInterval(() => {
    if (!wsOpen()) connectAdlWarningWs();
    else syncAdlSubscriptions();
  }, ADL_SUBSCRIPTION_REFRESH_MS);
  adlSubscriptionTimer.unref?.();
}

function currentAdlWarning(family) {
  const row = adlWarningRows.get(family);
  if (!row) return null;
  const ts = Number(row.ts || 0);
  return Number.isFinite(ts) && Date.now() - ts <= ADL_ACTIVE_TTL_MS ? row : null;
}

function snapshotPayload({ includeRows = true } = {}) {
  const cacheKey = includeRows ? 'full' : 'meta';
  const cached = responseCache.get(cacheKey);
  if (cached && Date.now() - cached.at <= RESPONSE_CACHE_TTL_MS) {
    responseCacheHits += 1;
    return { ...clone(cached.payload), cache_hit: true, cache_age_ms: Date.now() - cached.at };
  }
  responseCacheMisses += 1;

  const focus = okxFocusTargets();
  const oiState = okxOpenInterestMap();
  const focusRows = focus.rows.map((target) => {
    const funding = fundingRows.get(target.symbol);
    const limit = priceLimitRows.get(target.symbol);
    const oi = oiState.map.get(target.symbol) || null;
    const fund = securityFundRows.get(target.inst_family) || null;
    const adl = currentAdlWarning(target.inst_family);
    return {
      provider: 'okx',
      market_type: 'contract',
      quote_asset: 'USDT',
      symbol: target.symbol,
      native_symbol: target.native_symbol,
      inst_family: target.inst_family,
      base_asset: target.base_asset,
      focus_role: target.role,
      focus_slot: target.slot,
      open_interest: oi?.open_interest ?? null,
      open_interest_value: oi?.open_interest_value ?? null,
      open_interest_unit: oi?.open_interest_unit ?? null,
      open_interest_value_unit: oi?.open_interest_value_unit ?? null,
      open_interest_source: oi?.open_interest_source ?? null,
      open_interest_source_time: oi?.source_time ?? null,
      funding_rate: isFresh(funding?.updated_at, FOCUS_STALE_MS) ? funding?.funding_rate ?? null : null,
      funding: isFresh(funding?.updated_at, FOCUS_STALE_MS) ? clone(funding) : null,
      price_limit: isFresh(limit?.updated_at, FOCUS_STALE_MS) ? clone(limit) : null,
      security_fund: securityFundStartupReady() && fund ? clone(fund) : null,
      adl_warning_state: adl?.state ?? null,
      adl_warning: adl ? clone(adl) : null,
    };
  });

  const coreRows = focusRows.filter((row) => row.focus_role === 'core');
  const oiRowsCount = focusRows.filter((row) => row.open_interest != null || row.open_interest_value != null).length;
  const fundingRowsCount = focusRows.filter((row) => row.funding != null).length;
  const priceLimitCoverageRows = focusRows.filter((row) => row.price_limit != null).length;
  const priceLimitEnabledRows = focusRows.filter((row) => row.price_limit?.enabled === true).length;
  const securityFundFocusRows = focusRows.filter((row) => row.security_fund != null).length;
  const activeAdlRows = focusRows.filter((row) => ['warning', 'adl'].includes(row.adl_warning_state)).length;
  const coreOiRows = coreRows.filter((row) => row.open_interest != null || row.open_interest_value != null).length;
  const coreFundingRows = coreRows.filter((row) => row.funding != null).length;
  const corePriceLimitRows = coreRows.filter((row) => row.price_limit != null).length;
  const desiredFamilies = focus.rows.map((row) => row.inst_family).filter(Boolean);
  const currentSignature = adlSignature(desiredFamilies);
  const subscribedCurrent = desiredFamilies.length === FOCUS_TARGET && desiredFamilies.every((family) => adlSubscribedFamilies.has(family));
  const adlChannelReady = wsOpen() && subscribedCurrent && adlDesiredSignature === currentSignature;
  const lane = Object.fromEntries([...laneStats.entries()].map(([key, value]) => [key, { ...value }]));
  const ready = focus.focus_ready &&
    focusRows.length === FOCUS_TARGET &&
    coreRows.length === 10 &&
    oiState.ready && oiRowsCount === FOCUS_TARGET && coreOiRows === 10 &&
    fundingRowsCount === FOCUS_TARGET && coreFundingRows === 10 &&
    priceLimitCoverageRows === FOCUS_TARGET && corePriceLimitRows === 10 &&
    securityFundStartupReady() && securityFundRows.size > 0 &&
    adlChannelReady;

  const payload = {
    ok: true,
    version: VERSION,
    source: 'render_shared_okx_official_public_advanced_statistics',
    ready,
    focus_target: FOCUS_TARGET,
    focus_round: focus.focus_round,
    focus_symbols: focus.rows.map((row) => row.symbol),
    row_count: focusRows.length,
    core_target_count: coreRows.length,
    open_interest_rows: oiRowsCount,
    core_open_interest_rows: coreOiRows,
    funding_rows: fundingRowsCount,
    core_funding_rows: coreFundingRows,
    price_limit_coverage_rows: priceLimitCoverageRows,
    price_limit_enabled_rows: priceLimitEnabledRows,
    core_price_limit_coverage_rows: corePriceLimitRows,
    security_fund_record_count: securityFundStartupReady() ? securityFundRows.size : 0,
    security_fund_focus_rows: securityFundFocusRows,
    adl_warning_channel_ready: adlChannelReady,
    adl_warning_active_rows: activeAdlRows,
    adl_warning_subscribed_families: adlSubscribedFamilies.size,
    adl_warning_desired_families: desiredFamilies.length,
    focus_refresh_seconds: Math.round(FOCUS_REFRESH_MS / 1000),
    security_fund_refresh_seconds: Math.round(SECURITY_FUND_REFRESH_MS / 1000),
    security_fund_official_update_policy: 'daily_after_settlement_around_08:00_UTC_per_OKX_2026_update',
    per_request_gap_ms: PER_REQUEST_GAP_MS,
    official_endpoint_rate_policy: {
      funding_rate: 'public REST; IP + instrument ID; shared focus15 every 5m',
      price_limit: 'public REST; shared focus15 every 5m',
      security_fund: '10 requests/2s/IP; official SWAP query requires uly; focus15 families every 6h with missing-only recovery',
      open_interest: 'reused from market-light official SWAP batch; zero duplicate Step994 requests',
      adl_warning: 'one shared public websocket connection; focus15 instFamily subscriptions; no push in normal state',
    },
    official_semantics: {
      open_interest: 'OKX official public open-interest SWAP batch already owned by market-light',
      funding_rate: 'OKX official current funding rate; nextFundingRate may be empty and stays null',
      price_limit: 'OKX official highest buy limit / lowest sell limit; enabled=false is official coverage and limits may be null',
      security_fund: 'OKX official security fund queried by instType=SWAP + uly per focus family; regular_update removed in 2026; daily balance-change events remain around 08:00 UTC',
      adl_warning: 'OKX public adl-warning pushes only warning/adl once per second; silence is not fabricated as a per-symbol normal value',
    },
    separate_from_derived_data: true,
    no_cross_provider_substitution: true,
    no_cross_quote_substitution: true,
    missing_stays_null: true,
    collector_only_exchange_requests: true,
    provider_request_governor_reused: true,
    custom_provider_governor_created: false,
    exchange_requests_started_by_user_read: 0,
    exchange_connections_started_by_user_read: 0,
    user_reads_trigger_collector: false,
    reads_scale_with_users: false,
    market_light_oi_reused_only: true,
    market_light_oi_additional_exchange_requests: 0,
    adl_warning_normal_state_not_fabricated: true,
    adl_warning_public_no_auth: true,
    adl_warning_one_shared_connection: true,
    open_interest_history: oiHistoryHealthPayload(),
    open_interest_history_route: OI_HISTORY_ROUTE,
    last_focus_started_at: lastFocusStartedAt,
    last_focus_completed_at: lastFocusCompletedAt,
    last_focus_error: lastFocusError,
    last_security_fund_started_at: lastSecurityFundStartedAt,
    last_security_fund_completed_at: lastSecurityFundCompletedAt,
    last_security_fund_error: lastSecurityFundError,
    adl_ws: {
      url: WS_URL,
      connected: wsOpen(),
      desired_signature: currentSignature,
      subscribed_current_focus: subscribedCurrent,
      desired_families: desiredFamilies.length,
      subscribed_families: adlSubscribedFamilies.size,
      last_subscribe_at: adlLastSubscribeAt,
      last_message_at: adlLastMessageAt,
      last_pong_at: adlLastPongAt,
      last_error: adlLastError,
      connect_attempts: adlConnectAttempts,
      reconnects: adlReconnects,
      messages: adlMessages,
      warning_messages: adlWarningMessages,
      subscription_acks: adlSubscriptionAcks,
      subscription_errors: adlSubscriptionErrors,
      active_ttl_ms: ADL_ACTIVE_TTL_MS,
    },
    round,
    lane_stats: lane,
    rows: includeRows ? focusRows.map(clone) : [],
    security_fund_rows: includeRows && securityFundStartupReady() ? [...securityFundRows.values()].map(clone) : [],
    timestamp_ms: Date.now(),
  };
  responseCache.set(cacheKey, { at: Date.now(), payload });
  return { ...clone(payload), cache_hit: false, cache_age_ms: 0 };
}

export function getOkxAdvancedStatsHealth() {
  const snapshot = snapshotPayload({ includeRows: false });
  return {
    ...snapshot,
    enabled: started || process.env.KAKA_DISABLE_OKX_ADVANCED_STATS !== '1',
    mode: 'shared_okx_focus15_funding_price_limit_plus_batch_oi_security_fund_and_event_adl',
    snapshot_endpoint: SNAPSHOT_ROUTE,
    health_endpoint: HEALTH_ROUTE,
    total_reads: totalReads,
    total_focus_builds: totalFocusBuilds,
    total_focus_failures: totalFocusFailures,
    total_security_fund_builds: totalSecurityFundBuilds,
    total_security_fund_failures: totalSecurityFundFailures,
    response_cache_ttl_seconds: RESPONSE_CACHE_TTL_MS / 1000,
    response_cache_hits: responseCacheHits,
    response_cache_misses: responseCacheMisses,
    focus_running: Boolean(focusRunning),
    security_fund_running: Boolean(securityFundRunning),
    focus_lock_release_after_completion: true,
    security_fund_lock_release_after_completion: true,
    startup_recovery_until_focus_ready: true,
    startup_recovery_missing_only: true,
    security_fund_startup_recovery: true,
    security_fund_startup_recovery_missing_only: true,
    security_fund_uly_parameter_required: true,
    security_fund_all_swap_query_disabled: true,
    security_fund_requests_per_full_cycle_max: FOCUS_TARGET,
    open_interest_history_ready: snapshot.open_interest_history?.ready === true,
    open_interest_history_official_5m_coverage: Number(snapshot.open_interest_history?.official_5m_coverage || 0),
    open_interest_history_total_official_5m_rows: Number(snapshot.open_interest_history?.total_official_5m_rows || 0),
    open_interest_history_full_cycle_request_cap: Number(snapshot.open_interest_history?.full_cycle_request_cap || 0),
    open_interest_history_user_reads_trigger_collector: snapshot.open_interest_history?.user_reads_trigger_collector === true,
    open_interest_history_reads_scale_with_users: snapshot.open_interest_history?.reads_scale_with_users === true,
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

export async function handleOkxAdvancedStats(req, res, url) {
  if (![SNAPSHOT_ROUTE, HEALTH_ROUTE, OI_HISTORY_ROUTE].includes(url.pathname)) return false;
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
    sendJson(res, 200, getOkxAdvancedStatsHealth());
    return true;
  }
  if (url.pathname === OI_HISTORY_ROUTE) {
    const symbol = compact(url.searchParams.get('symbol') || '');
    const interval = normalizeOiHistoryInterval(url.searchParams.get('interval') || '5m');
    const limit = Number(url.searchParams.get('limit') || 200);
    if (!symbol || !interval) {
      sendJson(res, 400, { ok: false, version: VERSION, error: 'symbol_and_supported_interval_required' });
      return true;
    }
    sendJson(res, 200, oiHistoryReadPayload(symbol, interval, limit));
    return true;
  }
  sendJson(res, 200, snapshotPayload({ includeRows: true }));
  return true;
}

export const __okxAdvancedTest = Object.freeze({
  parseFunding,
  parsePriceLimit,
  parseSecurityFund,
  securityFundFocusState,
  parseAdlWarningPush,
  parseOiHistoryRow,
  parseOiHistoryPayload,
  mergeOiHistoryRows,
  deriveOiHistoryRows,
  normalizeOiHistoryInterval,
  oiHistoryRecoveryDelayMs,
  nativeSymbol,
  instFamily,
});
