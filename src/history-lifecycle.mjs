// Step1004.8 / original Step1000: unified history lifecycle + bounded 1D layer.
// This module never contacts an exchange. It only asks Supabase to aggregate
// already-shared persistent tables into a small provider/day rollup.

const VERSION = '650.8.15.130';
const SNAPSHOT_ROUTE = '/api/history-lifecycle/current-snapshot';
const HEALTH_ROUTE = '/api/history-lifecycle/health';
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');
const PERSISTENCE_ENABLED = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const REFRESH_RPC = 'kaka_refresh_contract_lifecycle_1d_cache';
const CLEANUP_RPC = 'kaka_cleanup_contract_lifecycle_1d_cache';
const STATUS_RPC = 'kaka_contract_lifecycle_1d_status';
const START_DELAY_MS = Math.max(20_000, Number(process.env.KAKA_HISTORY_LIFECYCLE_START_DELAY_MS || 75_000));
const REFRESH_MS = Math.max(60 * 60_000, Number(process.env.KAKA_HISTORY_LIFECYCLE_REFRESH_MS || 6 * 60 * 60_000));
const STATUS_REFRESH_MS = Math.max(5 * 60_000, Number(process.env.KAKA_HISTORY_LIFECYCLE_STATUS_REFRESH_MS || 15 * 60_000));
const BACKFILL_DAYS = Math.max(2, Math.min(31, Number(process.env.KAKA_HISTORY_LIFECYCLE_BACKFILL_DAYS || 8)));

const LIFECYCLE_MATRIX = Object.freeze([
  Object.freeze({ dataset: 'contract_flow_raw', resolution: '5m', storage: 'app_contract_flow_5m_cache', retention: '8d', next_layer: '15m', raw_exchange_events_persisted: false }),
  Object.freeze({ dataset: 'contract_position_metrics', resolution: '5m', storage: 'app_contract_position_5m_cache', retention: '8d', next_layer: '1D', raw_exchange_events_persisted: false }),
  Object.freeze({ dataset: 'contract_flow_aggregate', resolution: '15m', storage: 'app_contract_flow_15m_cache', retention: '31d', next_layer: '1D', raw_exchange_events_persisted: false }),
  Object.freeze({ dataset: 'contract_funding_current', resolution: 'event/current', storage: 'app_funding_rate_current_cache', retention: '7d', next_layer: 'funding_history', raw_exchange_events_persisted: false }),
  Object.freeze({ dataset: 'contract_funding_history', resolution: 'official_event', storage: 'app_funding_rate_history_cache', retention: '31d', next_layer: '1D', raw_exchange_events_persisted: false }),
  Object.freeze({ dataset: 'contract_liquidation_raw_events', resolution: 'event', storage: 'process_memory_only', retention: '24h bounded recent window', next_layer: '1m', raw_exchange_events_persisted: false }),
  Object.freeze({ dataset: 'contract_liquidation_minute', resolution: '1m', storage: 'app_contract_liquidation_1m_cache', retention: '30h', next_layer: '1H', raw_exchange_events_persisted: false }),
  Object.freeze({ dataset: 'contract_liquidation_hour', resolution: '1H', storage: 'app_contract_liquidation_1h_cache', retention: '15d', next_layer: '1D', raw_exchange_events_persisted: false }),
  Object.freeze({ dataset: 'realized_liquidation_heatmap', resolution: '25bps/minute bins', storage: 'collector_memory_aggregate_only', retention: '24h', next_layer: 'none', raw_exchange_events_persisted: false }),
  Object.freeze({ dataset: 'contract_daily_lifecycle', resolution: '1D', storage: 'app_contract_lifecycle_1d_cache', retention: '370d', next_layer: 'long_term_daily', raw_exchange_events_persisted: false }),
  Object.freeze({ dataset: 'advanced_focus_histories', resolution: 'official_5m_or_native', storage: 'app_market_backend_snapshots_bounded_rows', retention: 'bounded row windows; no unbounded raw stream', next_layer: 'none', raw_exchange_events_persisted: false }),
]);

let started = false;
let timer = null;
let statusTimer = null;
let inflight = null;
let status = null;
let lastAttemptAt = 0;
let lastSuccessAt = 0;
let lastError = '';
let refreshAttempts = 0;
let refreshSuccesses = 0;
let refreshFailures = 0;
let cleanupAttempts = 0;
let cleanupSuccesses = 0;
let cleanupFailures = 0;
let statusAttempts = 0;
let statusSuccesses = 0;
let statusFailures = 0;
let backgroundDbRequests = 0;
let userReads = 0;

function sendJson(res, statusCode, payload) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(statusCode, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': String(body.length),
  });
  res.end(body);
}

async function rpc(name, body = {}, timeoutMs = 20_000) {
  if (!PERSISTENCE_ENABLED) throw new Error('history_lifecycle_persistence_disabled');
  backgroundDbRequests += 1;
  const response = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${name}`, {
    method: 'POST',
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      accept: 'application/json',
      'content-type': 'application/json',
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`${name}_http_${response.status}:${text.slice(0, 260)}`);
  if (!text.trim()) return null;
  try { return JSON.parse(text); } catch (_) { return text; }
}

async function refreshStatus() {
  statusAttempts += 1;
  try {
    const next = await rpc(STATUS_RPC, {}, 12_000);
    status = next && typeof next === 'object' ? next : null;
    statusSuccesses += 1;
    return status;
  } catch (error) {
    statusFailures += 1;
    throw error;
  }
}

async function runMaintenance() {
  if (!PERSISTENCE_ENABLED) return false;
  if (inflight) return await inflight;
  inflight = (async () => {
    lastAttemptAt = Date.now();
    refreshAttempts += 1;
    try {
      await rpc(REFRESH_RPC, { p_days: BACKFILL_DAYS }, 45_000);
      refreshSuccesses += 1;
      cleanupAttempts += 1;
      try {
        await rpc(CLEANUP_RPC, {}, 20_000);
        cleanupSuccesses += 1;
      } catch (error) {
        cleanupFailures += 1;
        throw error;
      }
      await refreshStatus();
      lastSuccessAt = Date.now();
      lastError = '';
      return true;
    } catch (error) {
      refreshFailures += 1;
      lastError = String(error?.message || error).slice(0, 400);
      return false;
    }
  })().finally(() => { inflight = null; });
  return await inflight;
}

export function startHistoryLifecycleMaintainer() {
  if (started) return;
  started = true;
  const firstStatus = setTimeout(() => {
    refreshStatus().catch((error) => { lastError = String(error?.message || error).slice(0, 400); });
  }, Math.min(20_000, Math.max(3_000, Math.trunc(START_DELAY_MS / 3))));
  firstStatus.unref?.();
  const first = setTimeout(() => runMaintenance().catch(() => {}), START_DELAY_MS);
  first.unref?.();
  timer = setInterval(() => runMaintenance().catch(() => {}), REFRESH_MS);
  timer.unref?.();
  statusTimer = setInterval(() => {
    refreshStatus().catch((error) => { lastError = String(error?.message || error).slice(0, 400); });
  }, STATUS_REFRESH_MS);
  statusTimer.unref?.();
}

function statusNumber(key) {
  const n = Number(status?.[key]);
  return Number.isFinite(n) ? n : 0;
}

export function getHistoryLifecycleHealth() {
  const providerCount = statusNumber('provider_count');
  const rows = statusNumber('rows');
  const completedRows = statusNumber('completed_rows');
  const oneDayRetentionDays = Number(status?.one_day_retention_days || 370);
  const oneDayReady = PERSISTENCE_ENABLED && rows > 0 && providerCount === 5 && oneDayRetentionDays === 370;
  const matrixReady = LIFECYCLE_MATRIX.length >= 10 &&
    LIFECYCLE_MATRIX.some((row) => row.resolution === '5m' && row.retention === '8d') &&
    LIFECYCLE_MATRIX.some((row) => row.resolution === '1H' && row.retention === '15d') &&
    LIFECYCLE_MATRIX.some((row) => row.resolution === '1D' && row.retention === '370d');
  return {
    ok: true,
    version: VERSION,
    ready: oneDayReady && matrixReady,
    step1000_ready: oneDayReady && matrixReady,
    persistence_enabled: PERSISTENCE_ENABLED,
    lifecycle_matrix_declared: matrixReady,
    lifecycle_matrix: LIFECYCLE_MATRIX.map((row) => ({ ...row })),
    one_day_persistence_present: oneDayReady,
    one_day_storage_table: 'app_contract_lifecycle_1d_cache',
    one_day_retention_days: 370,
    one_day_rows: rows,
    one_day_provider_count: providerCount,
    one_day_completed_rows: completedRows,
    one_day_provisional_rows: statusNumber('provisional_rows'),
    one_day_oldest_date: status?.oldest_date || null,
    one_day_latest_date: status?.latest_date || null,
    one_day_flow_rows: statusNumber('flow_rows'),
    one_day_position_rows: statusNumber('position_rows'),
    one_day_liquidation_rows: statusNumber('liquidation_rows'),
    one_day_funding_rows: statusNumber('funding_rows'),
    backfill_days: BACKFILL_DAYS,
    refresh_every_hours: REFRESH_MS / 3_600_000,
    refresh_rpc: REFRESH_RPC,
    cleanup_rpc: CLEANUP_RPC,
    status_rpc: STATUS_RPC,
    refresh_attempts: refreshAttempts,
    refresh_successes: refreshSuccesses,
    refresh_failures: refreshFailures,
    cleanup_attempts: cleanupAttempts,
    cleanup_successes: cleanupSuccesses,
    cleanup_failures: cleanupFailures,
    status_attempts: statusAttempts,
    status_successes: statusSuccesses,
    status_failures: statusFailures,
    background_db_requests: backgroundDbRequests,
    last_attempt_at: lastAttemptAt ? new Date(lastAttemptAt).toISOString() : null,
    last_success_at: lastSuccessAt ? new Date(lastSuccessAt).toISOString() : null,
    last_error: lastError,
    maintenance_inflight: Boolean(inflight),
    user_reads: userReads,
    user_reads_trigger_database_requests: false,
    user_reads_trigger_exchange_requests: false,
    exchange_requests_started: 0,
    exchange_connections_started: 0,
    reads_scale_with_users: false,
    raw_exchange_events_persisted: false,
  };
}

export async function handleHistoryLifecycle(req, res, url) {
  if (url.pathname !== SNAPSHOT_ROUTE && url.pathname !== HEALTH_ROUTE) return false;
  if (req.method !== 'GET') {
    sendJson(res, 405, { ok: false, version: VERSION, error: 'method_not_allowed' });
    return true;
  }
  userReads += 1;
  const health = getHistoryLifecycleHealth();
  if (url.pathname === HEALTH_ROUTE) {
    sendJson(res, 200, health);
    return true;
  }
  sendJson(res, health.ready ? 200 : 503, {
    ok: health.ready,
    version: VERSION,
    ready: health.ready,
    source: 'shared_persisted_existing_history_lifecycle_1d',
    lifecycle_matrix: health.lifecycle_matrix,
    one_day: {
      storage_table: health.one_day_storage_table,
      rows: health.one_day_rows,
      provider_count: health.one_day_provider_count,
      completed_rows: health.one_day_completed_rows,
      provisional_rows: health.one_day_provisional_rows,
      oldest_date: health.one_day_oldest_date,
      latest_date: health.one_day_latest_date,
      retention_days: health.one_day_retention_days,
      flow_rows: health.one_day_flow_rows,
      position_rows: health.one_day_position_rows,
      liquidation_rows: health.one_day_liquidation_rows,
      funding_rows: health.one_day_funding_rows,
    },
    raw_exchange_events_persisted: false,
    user_reads_trigger_database_requests: false,
    user_reads_trigger_exchange_requests: false,
    exchange_requests_started: 0,
    generated_at: new Date().toISOString(),
  });
  return true;
}

export const __historyLifecycleTest = Object.freeze({
  matrix: LIFECYCLE_MATRIX,
});
