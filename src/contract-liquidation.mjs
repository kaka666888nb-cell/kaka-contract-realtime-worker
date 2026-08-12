import { getMarketUniverseRows } from './market-rest.mjs';

const STEP_VERSION = '650.8.15.49';
const SUPPORTED_PROVIDERS = new Set(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const GLOBAL_FEED_PROVIDERS = new Set(['binance', 'okx', 'bitget', 'gate']);
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


// Step768: shared bounded liquidation hour buckets.
// Raw public liquidation events remain process-memory only. Only exact
// provider+symbol hourly aggregates are persisted and served to all users.
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');
const LIQUIDATION_PERSISTENCE_ENABLED = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const LIQUIDATION_HOUR_TABLE = 'app_contract_liquidation_1h_cache';
// Step997: persist exact non-zero one-minute event buckets as the short-resolution
// history base. 5m/15m are derived from 1m; 1H/6H/24H remain based on the
// long-lived 1h aggregate store. Raw events remain process-memory only.
const LIQUIDATION_MINUTE_TABLE = 'app_contract_liquidation_1m_cache';
const LIQUIDATION_GATE_COVERAGE_TABLE = 'app_contract_liquidation_gate_1m_coverage';
const LIQUIDATION_CLEANUP_RPC = 'kaka_cleanup_contract_liquidation_step997_cache';
const LIQUIDATION_STEP997_HISTORY_RPC = 'kaka_contract_liquidation_history_step997';
const LIQUIDATION_MINUTE_RETENTION_HOURS = 30;
const STEP997_HISTORY_INTERVALS = Object.freeze({
  '1m': { canonical: '1m', durationMs: MINUTE_BUCKET_MS, base: 'minute', maxHours: 24 },
  '5m': { canonical: '5m', durationMs: 5 * MINUTE_BUCKET_MS, base: 'minute', maxHours: 24 },
  '15m': { canonical: '15m', durationMs: 15 * MINUTE_BUCKET_MS, base: 'minute', maxHours: 24 },
  '1h': { canonical: '1H', durationMs: HOUR_BUCKET_MS, base: 'hour', maxHours: 336 },
  '6h': { canonical: '6H', durationMs: 6 * HOUR_BUCKET_MS, base: 'hour', maxHours: 336 },
  '24h': { canonical: '24H', durationMs: 24 * HOUR_BUCKET_MS, base: 'hour', maxHours: 336 },
});
const GATE_LIQ_ORDERS_POLL_MS = 60_000;
const GATE_LIQ_ORDERS_LIMIT = 100;
const GATE_OFFICIAL_FINALIZED_MINUTE_RETENTION_MS = 3 * HOUR_BUCKET_MS;
const LIQUIDATION_HISTORY_ROUTE = '/api/contract-liquidation/history';
const LIQUIDATION_HEALTH_ROUTE = '/api/contract-liquidation/health';
const LIQUIDATION_CURRENT_ROUTE = '/api/contract-liquidation/current-snapshot';
const LIQUIDATION_MARKET_ROUTE = '/api/contract-liquidation/market-snapshot';
const LIQUIDATION_MARKET_UNIVERSE_REFRESH_MS = 10 * 60_000;
const LIQUIDATION_MARKET_STARTUP_RETRY_MS = 15_000;
const LIQUIDATION_MARKET_CACHE_TTL_MS = 5_000;
const LIQUIDATION_MARKET_MAX_EVENT_ROWS = 2000;
const BYBIT_SHARD_ARG_CHAR_BUDGET = 9000;
const LIQUIDATION_MARKET_PROVIDER_ORDER = Object.freeze(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const liquidationMarketUniverse = new Map();
const liquidationMarketUniverseMeta = new Map();
const bybitMarketShardKeys = new Set();
let liquidationMarketCurrentCache = { cachedAt: 0, payload: null };
let liquidationMarketUniverseRefreshInflight = null;
let liquidationMarketUniverseRetryTimer = null;
const liquidationMarketHealth = {
  universe_refresh_attempts: 0,
  universe_refresh_successes: 0,
  universe_refresh_failures: 0,
  last_universe_refresh_at: 0,
  last_universe_refresh_error: '',
  bybit_shard_rebuilds: 0,
  bybit_shard_count: 0,
  bybit_subscribed_symbols: 0,
  bybit_subscription_chars: 0,
  bybit_max_shard_subscription_chars: 0,
};
const LIQUIDATION_SHARED_TARGETS_PER_PROVIDER = 3;
const LIQUIDATION_SHARED_ROTATION_MS = 30 * 60_000;
const LIQUIDATION_SHARED_CACHE_TTL_MS = 5_000;
const LIQUIDATION_SHARED_PROVIDER_ORDER = Object.freeze(['binance', 'okx', 'bybit', 'bitget', 'gate']);
let liquidationSharedCurrentCache = { cachedAt: 0, payload: null };
const LIQUIDATION_HOUR_RETENTION_DAYS = 15;
const LIQUIDATION_CLOSE_GRACE_MS = 2 * 60_000;
const LIQUIDATION_PERSIST_FLUSH_MS = 60_000;
const LIQUIDATION_PERSIST_QUEUE_MAX = 5000;
const LIQUIDATION_HISTORY_CACHE_TTL_MS = 5 * 60_000;
const LIQUIDATION_HISTORY_STALE_MS = 30 * 60_000;
const LIQUIDATION_HISTORY_CACHE_MAX = 64;
const liquidationPersistQueue = new Map();
const liquidationPersistGate = new Map();
const liquidationMinutePersistQueue = new Map();
const liquidationMinutePersistGate = new Map();
const gateOfficialFinalizedMinuteStarts = new Map();
const liquidationHistoryCache = new Map();
const liquidationHistoryInflight = new Map();
let liquidationPersistInflight = null;
let liquidationMinutePersistInflight = null;
let gateLiqOrdersInflight = null;
let gateLiqOrdersLastWindowStart = 0;
const gateLiqOrdersHealth = {
  enabled: LIQUIDATION_PERSISTENCE_ENABLED,
  endpoint: '/futures/{settle}/liq_orders',
  public_no_auth: true,
  polling_interval_seconds: Math.trunc(GATE_LIQ_ORDERS_POLL_MS / 1000),
  max_rows_per_closed_minute: GATE_LIQ_ORDERS_LIMIT,
  max_time_window_seconds: 60,
  user_reads_trigger_requests: false,
  reads_scale_with_users: false,
  left_field_ignored: true,
  size_decimal_header: true,
  contract_multiplier_source: 'shared_market_universe_only_no_per_event_rest',
  additional_contract_metadata_requests: 0,
  max_transport_attempts_per_closed_minute: 2,
  signed_position_semantics: 'size>0 short_position_liquidation; size<0 long_position_liquidation',
  attempts: 0,
  successes: 0,
  failures: 0,
  complete_windows: 0,
  truncated_windows: 0,
  normalization_incomplete_windows: 0,
  parsed_events: 0,
  skipped_rows: 0,
  last_rows: 0,
  last_window_start: null,
  last_window_end: null,
  last_completed_at: null,
  last_error: '',
};
const liquidationPersistenceHealth = {
  last_flush_at: 0,
  last_flush_rows: 0,
  flush_error: '',
  last_cleanup_at: 0,
  cleanup_error: '',
  persisted_rows_total: 0,
  minute_last_flush_at: 0,
  minute_last_flush_rows: 0,
  minute_flush_error: '',
  minute_persisted_rows_total: 0,
};

function liquidationSupabaseHeaders(extra = {}) {
  return {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    ...extra,
  };
}

function liquidationIso(timeMs) {
  const value = Number(timeMs || 0);
  return value > 0 ? new Date(value).toISOString() : null;
}

function liquidationBucketCoverage(state, bucket, now = Date.now()) {
  const startMs = Number(bucket?.start_ms || 0);
  const endMs = Number(bucket?.end_ms || 0);
  const observedSinceMs = Number(state?.observedSinceMs || state?.createdAt || now);
  const lastGapAtMs = Number(state?.lastGapAtMs || 0);
  const bucketClosed = endMs > 0 && now >= endMs + LIQUIDATION_CLOSE_GRACE_MS;
  const startedBeforeBucket = observedSinceMs > 0 && observedSinceMs <= startMs;
  const gapInsideBucket = lastGapAtMs >= startMs && lastGapAtMs <= endMs + LIQUIDATION_CLOSE_GRACE_MS;
  return {
    bucket_closed: bucketClosed,
    provisional: !bucketClosed,
    coverage_complete: bucketClosed && startedBeforeBucket && !gapInsideBucket,
    observed_since_ms: observedSinceMs || null,
    last_gap_at_ms: lastGapAtMs || null,
  };
}

function liquidationPersistRow(state, bucket, now = Date.now(), source = 'render_public_liquidation_ws_hour_bucket_v1') {
  if (!state || !bucket || Number(bucket.event_count || 0) <= 0) return null;
  const provider = normalizeProvider(state.provider);
  const symbol = compactSymbol(state.symbol);
  const startMs = Number(bucket.start_ms || 0);
  const endMs = Number(bucket.end_ms || 0);
  if (!SUPPORTED_PROVIDERS.has(provider) || !symbol || startMs <= 0 || endMs <= startMs) return null;
  const largest = bucket.largest_event || null;
  const coverage = liquidationBucketCoverage(state, bucket, now);
  return {
    provider,
    market_type: 'contract',
    symbol,
    quote_asset: quoteFromCompact(symbol),
    bucket_start: liquidationIso(startMs),
    bucket_end: liquidationIso(endMs),
    long_notional: Math.max(0, Number(bucket.long_notional || 0)),
    short_notional: Math.max(0, Number(bucket.short_notional || 0)),
    total_notional: Math.max(0, Number(bucket.total_notional || 0)),
    long_count: Math.max(0, Math.trunc(Number(bucket.long_count || 0))),
    short_count: Math.max(0, Math.trunc(Number(bucket.short_count || 0))),
    event_count: Math.max(0, Math.trunc(Number(bucket.event_count || 0))),
    largest_event_id: largest ? String(largest.id || '') || null : null,
    largest_event_side: largest && ['long', 'short'].includes(String(largest.liquidation_side || '').toLowerCase())
      ? String(largest.liquidation_side).toLowerCase()
      : null,
    largest_event_notional: largest ? positiveNumber(largest.notional) : null,
    largest_event_price: largest ? positiveNumber(largest.price) : null,
    largest_event_time: largest ? liquidationIso(largest.time_ms) : null,
    latest_event_time: liquidationIso(bucket.latest_event_time_ms || largest?.time_ms || endMs),
    bucket_closed: coverage.bucket_closed,
    provisional: coverage.provisional,
    coverage_complete: coverage.coverage_complete,
    observed_since: liquidationIso(coverage.observed_since_ms),
    last_gap_at: liquidationIso(coverage.last_gap_at_ms),
    source,
    cached_at: new Date(now).toISOString(),
  };
}

function liquidationPersistSignature(row) {
  return JSON.stringify([
    row.event_count,
    row.long_count,
    row.short_count,
    Number(row.total_notional || 0).toFixed(8),
    row.largest_event_id || '',
    row.bucket_closed === true,
    row.coverage_complete === true,
  ]);
}

function capLiquidationMinutePersistQueue() {
  while (liquidationMinutePersistQueue.size > LIQUIDATION_PERSIST_QUEUE_MAX) {
    const first = liquidationMinutePersistQueue.keys().next().value;
    if (!first) break;
    liquidationMinutePersistQueue.delete(first);
  }
}

function pruneGateOfficialFinalizedMinutes(now = Date.now()) {
  for (const [startMs, finalizedAt] of [...gateOfficialFinalizedMinuteStarts.entries()]) {
    if (now - Number(finalizedAt || 0) > GATE_OFFICIAL_FINALIZED_MINUTE_RETENTION_MS) {
      gateOfficialFinalizedMinuteStarts.delete(startMs);
    }
  }
}

function queueLiquidationMinuteBucket(state, bucket, now = Date.now()) {
  if (!LIQUIDATION_PERSISTENCE_ENABLED) return;
  if (normalizeProvider(state?.provider) === 'gate') {
    // Step997 Gate 1m history is owned by the public liq_orders closed-minute
    // collector. Never let the lower-fidelity websocket aggregate overwrite it.
    return;
  }
  const row = liquidationPersistRow(state, bucket, now, 'render_public_liquidation_ws_minute_bucket_v1');
  if (!row) return;
  const key = `${row.provider}|${row.symbol}|${row.bucket_start}`;
  const signature = liquidationPersistSignature(row);
  const gate = liquidationMinutePersistGate.get(key);
  if (gate?.signature === signature && now - Number(gate.at || 0) < 3 * 60_000) return;
  liquidationMinutePersistQueue.set(key, { row, signature });
  capLiquidationMinutePersistQueue();
}

async function upsertLiquidationMinuteRows(rows) {
  if (!LIQUIDATION_PERSISTENCE_ENABLED || rows.length === 0) return 0;
  let written = 0;
  for (let index = 0; index < rows.length; index += 250) {
    const chunk = rows.slice(index, index + 250);
    const response = await fetch(
      `${SUPABASE_URL}/rest/v1/${LIQUIDATION_MINUTE_TABLE}?on_conflict=provider,market_type,symbol,bucket_start`,
      {
        method: 'POST',
        headers: liquidationSupabaseHeaders({
          'content-type': 'application/json',
          prefer: 'resolution=merge-duplicates,return=minimal',
        }),
        body: JSON.stringify(chunk),
        signal: AbortSignal.timeout(15000),
      },
    );
    const responseText = await response.text();
    if (!response.ok) throw new Error(`liquidation_minute_upsert_http_${response.status}:${responseText.slice(0, 220)}`);
    written += chunk.length;
  }
  return written;
}

async function flushLiquidationMinutePersistQueue() {
  if (!LIQUIDATION_PERSISTENCE_ENABLED || liquidationMinutePersistInflight || liquidationMinutePersistQueue.size === 0) {
    return liquidationMinutePersistInflight;
  }
  const batch = [...liquidationMinutePersistQueue.entries()].slice(0, 1000);
  for (const [key] of batch) liquidationMinutePersistQueue.delete(key);
  liquidationMinutePersistInflight = (async () => {
    try {
      const written = await upsertLiquidationMinuteRows(batch.map((entry) => entry[1].row));
      const now = Date.now();
      for (const [key, entry] of batch) liquidationMinutePersistGate.set(key, { signature: entry.signature, at: now });
      liquidationPersistenceHealth.minute_last_flush_at = now;
      liquidationPersistenceHealth.minute_last_flush_rows = written;
      liquidationPersistenceHealth.minute_persisted_rows_total += written;
      liquidationPersistenceHealth.minute_flush_error = '';
      liquidationHistoryCache.clear();
      return written;
    } catch (error) {
      for (const [key, entry] of batch) {
        if (!liquidationMinutePersistQueue.has(key)) liquidationMinutePersistQueue.set(key, entry);
      }
      capLiquidationMinutePersistQueue();
      liquidationPersistenceHealth.minute_flush_error = String(error?.message || error).slice(0, 300);
      throw error;
    } finally {
      liquidationMinutePersistInflight = null;
    }
  })();
  return liquidationMinutePersistInflight;
}

function capLiquidationPersistQueue() {
  while (liquidationPersistQueue.size > LIQUIDATION_PERSIST_QUEUE_MAX) {
    const first = liquidationPersistQueue.keys().next().value;
    if (!first) break;
    liquidationPersistQueue.delete(first);
  }
}

function queueLiquidationHourBucket(state, bucket, now = Date.now()) {
  if (!LIQUIDATION_PERSISTENCE_ENABLED) return;
  const row = liquidationPersistRow(state, bucket, now);
  if (!row) return;
  const key = `${row.provider}|${row.symbol}|${row.bucket_start}`;
  const signature = liquidationPersistSignature(row);
  const gate = liquidationPersistGate.get(key);
  if (gate?.signature === signature && now - Number(gate.at || 0) < 5 * 60_000) return;
  liquidationPersistQueue.set(key, { row, signature });
  capLiquidationPersistQueue();
}

async function upsertLiquidationHourRows(rows) {
  if (!LIQUIDATION_PERSISTENCE_ENABLED || rows.length === 0) return 0;
  let written = 0;
  for (let index = 0; index < rows.length; index += 250) {
    const chunk = rows.slice(index, index + 250);
    const response = await fetch(
      `${SUPABASE_URL}/rest/v1/${LIQUIDATION_HOUR_TABLE}?on_conflict=provider,market_type,symbol,bucket_start`,
      {
        method: 'POST',
        headers: liquidationSupabaseHeaders({
          'content-type': 'application/json',
          prefer: 'resolution=merge-duplicates,return=minimal',
        }),
        body: JSON.stringify(chunk),
        signal: AbortSignal.timeout(15000),
      },
    );
    const responseText = await response.text();
    if (!response.ok) throw new Error(`liquidation_hour_upsert_http_${response.status}:${responseText.slice(0, 220)}`);
    written += chunk.length;
  }
  return written;
}

async function flushLiquidationPersistQueue() {
  if (!LIQUIDATION_PERSISTENCE_ENABLED || liquidationPersistInflight || liquidationPersistQueue.size === 0) {
    return liquidationPersistInflight;
  }
  const batch = [...liquidationPersistQueue.entries()].slice(0, 1000);
  for (const [key] of batch) liquidationPersistQueue.delete(key);
  liquidationPersistInflight = (async () => {
    try {
      const written = await upsertLiquidationHourRows(batch.map((entry) => entry[1].row));
      const now = Date.now();
      for (const [key, entry] of batch) liquidationPersistGate.set(key, { signature: entry.signature, at: now });
      liquidationPersistenceHealth.last_flush_at = now;
      liquidationPersistenceHealth.last_flush_rows = written;
      liquidationPersistenceHealth.persisted_rows_total += written;
      liquidationPersistenceHealth.flush_error = '';
      liquidationHistoryCache.clear();
      return written;
    } catch (error) {
      for (const [key, entry] of batch) {
        if (!liquidationPersistQueue.has(key)) liquidationPersistQueue.set(key, entry);
      }
      capLiquidationPersistQueue();
      liquidationPersistenceHealth.flush_error = String(error?.message || error).slice(0, 300);
      throw error;
    } finally {
      liquidationPersistInflight = null;
    }
  })();
  return liquidationPersistInflight;
}

function queueRecentLiquidationMinuteBuckets(now = Date.now()) {
  if (!LIQUIDATION_PERSISTENCE_ENABLED) return;
  const cutoff = now - 5 * MINUTE_BUCKET_MS;
  for (const state of STATS.values()) {
    for (const bucket of state.minuteBuckets.values()) {
      if (Number(bucket.end_ms || 0) >= cutoff) queueLiquidationMinuteBucket(state, bucket, now);
    }
  }
}

function queueRecentLiquidationHourBuckets(now = Date.now()) {
  if (!LIQUIDATION_PERSISTENCE_ENABLED) return;
  const cutoff = now - 3 * HOUR_BUCKET_MS;
  for (const state of STATS.values()) {
    for (const bucket of state.hourBuckets.values()) {
      if (Number(bucket.end_ms || 0) >= cutoff) queueLiquidationHourBucket(state, bucket, now);
    }
  }
}

async function cleanupLiquidationPersistence() {
  if (!LIQUIDATION_PERSISTENCE_ENABLED) return null;
  const response = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${LIQUIDATION_CLEANUP_RPC}`, {
    method: 'POST',
    headers: liquidationSupabaseHeaders({ 'content-type': 'application/json', accept: 'application/json' }),
    body: '{}',
    signal: AbortSignal.timeout(15000),
  });
  const responseText = await response.text();
  if (!response.ok) throw new Error(`liquidation_cleanup_http_${response.status}:${responseText.slice(0, 220)}`);
  liquidationPersistenceHealth.last_cleanup_at = Date.now();
  liquidationPersistenceHealth.cleanup_error = '';
  try { return JSON.parse(responseText); } catch (_) { return responseText; }
}

function pruneLiquidationHistoryCache(now = Date.now()) {
  for (const [key, entry] of liquidationHistoryCache) {
    if (!entry || now - Number(entry.cachedAt || 0) > LIQUIDATION_HISTORY_STALE_MS) liquidationHistoryCache.delete(key);
  }
  while (liquidationHistoryCache.size > LIQUIDATION_HISTORY_CACHE_MAX) {
    const oldest = [...liquidationHistoryCache.entries()].sort((a, b) => Number(a[1]?.cachedAt || 0) - Number(b[1]?.cachedAt || 0))[0]?.[0];
    if (!oldest) break;
    liquidationHistoryCache.delete(oldest);
  }
}

function normalizePersistedLiquidationHour(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const provider = normalizeProvider(raw.provider);
  const symbol = compactSymbol(raw.symbol);
  const startMs = Date.parse(String(raw.bucket_start || ''));
  const endMs = Date.parse(String(raw.bucket_end || ''));
  if (!SUPPORTED_PROVIDERS.has(provider) || !symbol || !Number.isFinite(startMs) || !Number.isFinite(endMs)) return null;
  const row = {
    provider,
    market_type: 'contract',
    symbol,
    quote_asset: String(raw.quote_asset || quoteFromCompact(symbol)).toUpperCase(),
    bucket_start: new Date(startMs).toISOString(),
    bucket_end: new Date(endMs).toISOString(),
    bucket_start_ms: startMs,
    bucket_end_ms: endMs,
    long_notional: Math.max(0, Number(raw.long_notional || 0)),
    short_notional: Math.max(0, Number(raw.short_notional || 0)),
    total_notional: Math.max(0, Number(raw.total_notional || 0)),
    long_count: Math.max(0, Math.trunc(Number(raw.long_count || 0))),
    short_count: Math.max(0, Math.trunc(Number(raw.short_count || 0))),
    event_count: Math.max(0, Math.trunc(Number(raw.event_count || 0))),
    largest_event_id: raw.largest_event_id ? String(raw.largest_event_id) : null,
    largest_event_side: ['long', 'short'].includes(String(raw.largest_event_side || '').toLowerCase())
      ? String(raw.largest_event_side).toLowerCase()
      : null,
    largest_event_notional: positiveNumber(raw.largest_event_notional),
    largest_event_price: positiveNumber(raw.largest_event_price),
    largest_event_time: raw.largest_event_time || null,
    latest_event_time: raw.latest_event_time || null,
    bucket_closed: raw.bucket_closed === true,
    provisional: raw.provisional === true,
    coverage_complete: raw.coverage_complete === true,
    observed_since: raw.observed_since || null,
    last_gap_at: raw.last_gap_at || null,
    source: String(raw.source || 'render_public_liquidation_ws_hour_bucket_v1'),
    cached_at: raw.cached_at || null,
  };
  return row.event_count > 0 && row.total_notional > 0 ? row : null;
}

async function readPersistedLiquidationMinutes({ hours = 6, provider = '', symbol = '', limit = 5000 } = {}) {
  if (!LIQUIDATION_PERSISTENCE_ENABLED) throw new Error('liquidation_history_persistence_disabled');
  const safeHours = Math.max(1, Math.min(LIQUIDATION_MINUTE_RETENTION_HOURS, Math.trunc(Number(hours) || 6)));
  const safeProvider = normalizeProvider(provider);
  const safeSymbol = compactSymbol(symbol);
  const safeLimit = Math.max(1, Math.min(15000, Math.trunc(Number(limit) || 5000)));
  const now = Date.now();
  const query = new URLSearchParams({
    select: 'provider,market_type,symbol,quote_asset,bucket_start,bucket_end,long_notional,short_notional,total_notional,long_count,short_count,event_count,largest_event_id,largest_event_side,largest_event_notional,largest_event_price,largest_event_time,latest_event_time,bucket_closed,provisional,coverage_complete,observed_since,last_gap_at,source,cached_at',
    bucket_start: `gte.${new Date(now - safeHours * HOUR_BUCKET_MS).toISOString()}`,
    order: 'bucket_start.desc,provider.asc,symbol.asc',
    limit: String(safeLimit),
  });
  if (safeProvider) query.set('provider', `eq.${safeProvider}`);
  if (safeSymbol) query.set('symbol', `eq.${safeSymbol}`);
  const response = await fetch(`${SUPABASE_URL}/rest/v1/${LIQUIDATION_MINUTE_TABLE}?${query}`, {
    headers: liquidationSupabaseHeaders({ accept: 'application/json' }),
    signal: AbortSignal.timeout(15000),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`liquidation_minute_history_store_http_${response.status}:${text.slice(0, 220)}`);
  const decoded = JSON.parse(text);
  return (Array.isArray(decoded) ? decoded : []).map(normalizePersistedLiquidationHour).filter(Boolean);
}

function canonicalStep997HistoryInterval(raw) {
  const key = String(raw || '').trim().toLowerCase();
  return STEP997_HISTORY_INTERVALS[key] || null;
}

function persistedHistorySummary(rows) {
  return rows.reduce((acc, row) => {
    acc.long_notional += Number(row.long_notional || 0);
    acc.short_notional += Number(row.short_notional || 0);
    acc.total_notional += Number(row.total_notional || 0);
    acc.long_count += Number(row.long_count || 0);
    acc.short_count += Number(row.short_count || 0);
    acc.event_count += Number(row.event_count || 0);
    if (!acc.largest_event || Number(row.largest_event_notional || 0) > Number(acc.largest_event.largest_event_notional || 0)) {
      acc.largest_event = row.largest_event_notional ? {
        provider: row.provider,
        symbol: row.symbol,
        largest_event_id: row.largest_event_id,
        largest_event_side: row.largest_event_side,
        largest_event_notional: row.largest_event_notional,
        largest_event_price: row.largest_event_price,
        largest_event_time: row.largest_event_time,
      } : acc.largest_event;
    }
    return acc;
  }, { long_notional: 0, short_notional: 0, total_notional: 0, long_count: 0, short_count: 0, event_count: 0, largest_event: null });
}

function aggregatePersistedLiquidationRows(rows, spec) {
  if (!spec) return rows;
  const durationMs = Number(spec.durationMs || HOUR_BUCKET_MS);
  const grouped = new Map();
  for (const row of rows) {
    const startMs = Number(row.bucket_start_ms || Date.parse(row.bucket_start || ''));
    if (!Number.isFinite(startMs)) continue;
    const aggregateStart = Math.floor(startMs / durationMs) * durationMs;
    const key = `${row.provider}|${row.symbol}|${aggregateStart}`;
    let target = grouped.get(key);
    if (!target) {
      target = {
        provider: row.provider,
        market_type: 'contract',
        symbol: row.symbol,
        quote_asset: row.quote_asset || quoteFromCompact(row.symbol),
        bucket_start: new Date(aggregateStart).toISOString(),
        bucket_end: new Date(aggregateStart + durationMs).toISOString(),
        bucket_start_ms: aggregateStart,
        bucket_end_ms: aggregateStart + durationMs,
        long_notional: 0,
        short_notional: 0,
        total_notional: 0,
        long_count: 0,
        short_count: 0,
        event_count: 0,
        largest_event_id: null,
        largest_event_side: null,
        largest_event_notional: null,
        largest_event_price: null,
        largest_event_time: null,
        latest_event_time: null,
        bucket_closed: true,
        provisional: false,
        coverage_complete: true,
        observed_since: null,
        last_gap_at: null,
        source: `render_step997_unified_${spec.canonical}_from_${spec.base}_event_buckets_v1`,
        cached_at: null,
        source_bucket_count: 0,
      };
      grouped.set(key, target);
    }
    target.long_notional += Number(row.long_notional || 0);
    target.short_notional += Number(row.short_notional || 0);
    target.total_notional += Number(row.total_notional || 0);
    target.long_count += Number(row.long_count || 0);
    target.short_count += Number(row.short_count || 0);
    target.event_count += Number(row.event_count || 0);
    target.source_bucket_count += 1;
    target.bucket_closed = target.bucket_closed && row.bucket_closed === true;
    target.provisional = target.provisional || row.provisional === true;
    target.coverage_complete = target.coverage_complete && row.coverage_complete === true;
    if (!target.observed_since || (row.observed_since && Date.parse(row.observed_since) < Date.parse(target.observed_since))) target.observed_since = row.observed_since || target.observed_since;
    if (row.last_gap_at && (!target.last_gap_at || Date.parse(row.last_gap_at) > Date.parse(target.last_gap_at))) target.last_gap_at = row.last_gap_at;
    if (row.latest_event_time && (!target.latest_event_time || Date.parse(row.latest_event_time) > Date.parse(target.latest_event_time))) target.latest_event_time = row.latest_event_time;
    if (Number(row.largest_event_notional || 0) > Number(target.largest_event_notional || 0)) {
      target.largest_event_id = row.largest_event_id;
      target.largest_event_side = row.largest_event_side;
      target.largest_event_notional = row.largest_event_notional;
      target.largest_event_price = row.largest_event_price;
      target.largest_event_time = row.largest_event_time;
    }
    if (!target.cached_at || (row.cached_at && Date.parse(row.cached_at) > Date.parse(target.cached_at))) target.cached_at = row.cached_at || target.cached_at;
  }
  return [...grouped.values()].sort((a, b) => b.bucket_start_ms - a.bucket_start_ms || a.provider.localeCompare(b.provider) || a.symbol.localeCompare(b.symbol));
}

async function readStep997UnifiedHistory({ interval, hours = 6, provider = '', symbol = '', limit = 2500 } = {}) {
  if (!LIQUIDATION_PERSISTENCE_ENABLED) throw new Error('liquidation_history_persistence_disabled');
  const spec = canonicalStep997HistoryInterval(interval);
  if (!spec) throw new Error('unsupported_liquidation_history_interval');
  const safeHours = Math.max(1, Math.min(spec.maxHours, Math.trunc(Number(hours) || 6)));
  const safeProvider = normalizeProvider(provider);
  const safeSymbol = compactSymbol(symbol);
  const safeLimit = Math.max(1, Math.min(5000, Math.trunc(Number(limit) || 2500)));
  const key = `step997|${spec.canonical}|${safeHours}|${safeProvider}|${safeSymbol}|${safeLimit}`;
  const now = Date.now();
  pruneLiquidationHistoryCache(now);
  const cached = liquidationHistoryCache.get(key);
  if (cached && now - Number(cached.cachedAt || 0) <= LIQUIDATION_HISTORY_CACHE_TTL_MS) {
    return { ...cached.payload, cache_hit: true, cache_age_ms: now - cached.cachedAt };
  }
  if (liquidationHistoryInflight.has(key)) return await liquidationHistoryInflight.get(key);

  const inflight = (async () => {
    const response = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${LIQUIDATION_STEP997_HISTORY_RPC}`, {
      method: 'POST',
      headers: liquidationSupabaseHeaders({ 'content-type': 'application/json', accept: 'application/json' }),
      body: JSON.stringify({
        p_interval: spec.canonical,
        p_hours: safeHours,
        p_provider: safeProvider || null,
        p_symbol: safeSymbol || null,
        p_limit: safeLimit,
      }),
      signal: AbortSignal.timeout(20000),
    });
    const text = await response.text();
    if (!response.ok) throw new Error(`liquidation_step997_history_rpc_http_${response.status}:${text.slice(0, 220)}`);
    const decoded = JSON.parse(text);
    const rows = (Array.isArray(decoded) ? decoded : []).map((raw) => {
      const normalized = normalizePersistedLiquidationHour(raw);
      if (!normalized) return null;
      normalized.source_bucket_count = Math.max(1, Math.trunc(Number(raw?.source_bucket_count || 1)));
      normalized.source = String(raw?.source || `render_step997_unified_${spec.canonical}_from_${spec.base}_event_buckets_v1`);
      return normalized;
    }).filter(Boolean);
    const providers = [...new Set(rows.map((row) => row.provider))].sort();
    const pairs = new Set(rows.map((row) => `${row.provider}|${row.symbol}`));
    const payload = {
      rows,
      row_count: rows.length,
      provider_coverage: providers,
      provider_count: providers.length,
      pair_count: pairs.size,
      summary: persistedHistorySummary(rows),
      hours: safeHours,
      provider: safeProvider || null,
      symbol: safeSymbol || null,
      interval: spec.canonical,
      base_interval: spec.base === 'minute' ? '1m' : '1H',
      zero_event_rows_persisted: false,
      coverage_semantics: 'event_buckets_only_no_fabricated_zero_rows',
      persistence_enabled: true,
      aggregation_location: 'supabase_rpc_shared_server_side',
      aggregation_rpc: LIQUIDATION_STEP997_HISTORY_RPC,
      cache_hit: false,
      cache_age_ms: 0,
    };
    liquidationHistoryCache.set(key, { cachedAt: Date.now(), payload });
    pruneLiquidationHistoryCache();
    return payload;
  })().finally(() => liquidationHistoryInflight.delete(key));
  liquidationHistoryInflight.set(key, inflight);
  return await inflight;
}

async function readPersistedLiquidationHours({ hours = 6, provider = '', symbol = '', limit = 2500 } = {}) {
  if (!LIQUIDATION_PERSISTENCE_ENABLED) throw new Error('liquidation_history_persistence_disabled');
  const safeHours = Math.max(1, Math.min(336, Math.trunc(Number(hours) || 6)));
  const safeProvider = normalizeProvider(provider);
  const safeSymbol = compactSymbol(symbol);
  const safeLimit = Math.max(1, Math.min(5000, Math.trunc(Number(limit) || 2500)));
  const key = `${safeHours}|${safeProvider}|${safeSymbol}|${safeLimit}`;
  const now = Date.now();
  pruneLiquidationHistoryCache(now);
  const cached = liquidationHistoryCache.get(key);
  if (cached && now - cached.cachedAt < LIQUIDATION_HISTORY_CACHE_TTL_MS) {
    return { ...cached.payload, cache_hit: true, cache_age_ms: now - cached.cachedAt };
  }
  if (liquidationHistoryInflight.has(key)) return await liquidationHistoryInflight.get(key);
  const pending = (async () => {
    try {
      const query = new URLSearchParams({
        select: 'provider,market_type,symbol,quote_asset,bucket_start,bucket_end,long_notional,short_notional,total_notional,long_count,short_count,event_count,largest_event_id,largest_event_side,largest_event_notional,largest_event_price,largest_event_time,latest_event_time,bucket_closed,provisional,coverage_complete,observed_since,last_gap_at,source,cached_at',
        bucket_start: `gte.${new Date(now - safeHours * HOUR_BUCKET_MS).toISOString()}`,
        order: 'bucket_start.desc,provider.asc,symbol.asc',
        limit: String(safeLimit),
      });
      if (safeProvider) query.set('provider', `eq.${safeProvider}`);
      if (safeSymbol) query.set('symbol', `eq.${safeSymbol}`);
      const response = await fetch(`${SUPABASE_URL}/rest/v1/${LIQUIDATION_HOUR_TABLE}?${query}`, {
        headers: liquidationSupabaseHeaders({ accept: 'application/json' }),
        signal: AbortSignal.timeout(15000),
      });
      const responseText = await response.text();
      if (!response.ok) throw new Error(`liquidation_history_store_http_${response.status}:${responseText.slice(0, 220)}`);
      const decoded = JSON.parse(responseText);
      const rows = (Array.isArray(decoded) ? decoded : []).map(normalizePersistedLiquidationHour).filter(Boolean);
      const providers = [...new Set(rows.map((row) => row.provider))].sort();
      const pairs = new Set(rows.map((row) => `${row.provider}|${row.symbol}`));
      const summary = rows.reduce((acc, row) => {
        acc.long_notional += row.long_notional;
        acc.short_notional += row.short_notional;
        acc.total_notional += row.total_notional;
        acc.long_count += row.long_count;
        acc.short_count += row.short_count;
        acc.event_count += row.event_count;
        if (!acc.largest_event || Number(row.largest_event_notional || 0) > Number(acc.largest_event.largest_event_notional || 0)) {
          acc.largest_event = row.largest_event_notional ? {
            provider: row.provider,
            symbol: row.symbol,
            largest_event_id: row.largest_event_id,
            largest_event_side: row.largest_event_side,
            largest_event_notional: row.largest_event_notional,
            largest_event_price: row.largest_event_price,
            largest_event_time: row.largest_event_time,
          } : acc.largest_event;
        }
        return acc;
      }, { long_notional: 0, short_notional: 0, total_notional: 0, long_count: 0, short_count: 0, event_count: 0, largest_event: null });
      const payload = {
        rows,
        row_count: rows.length,
        provider_coverage: providers,
        provider_count: providers.length,
        pair_count: pairs.size,
        summary,
        hours: safeHours,
        provider: safeProvider || null,
        symbol: safeSymbol || null,
        persistence_enabled: true,
        cache_hit: false,
        cache_age_ms: 0,
      };
      liquidationHistoryCache.set(key, { cachedAt: Date.now(), payload });
      pruneLiquidationHistoryCache();
      return payload;
    } catch (error) {
      const stale = liquidationHistoryCache.get(key) || cached;
      if (stale && Date.now() - stale.cachedAt < LIQUIDATION_HISTORY_STALE_MS) {
        return {
          ...stale.payload,
          cache_hit: true,
          cache_stale: true,
          cache_age_ms: Date.now() - stale.cachedAt,
          warning: 'persisted_liquidation_history_read_failed_stale_retained',
          read_error: String(error?.message || error).slice(0, 260),
        };
      }
      throw error;
    }
  })().finally(() => liquidationHistoryInflight.delete(key));
  liquidationHistoryInflight.set(key, pending);
  return await pending;
}


function liquidationSharedRound(now = Date.now()) {
  return Math.max(0, Math.floor(Math.max(0, now - SERVICE_STARTED_AT_MS) / LIQUIDATION_SHARED_ROTATION_MS));
}

function liquidationSharedCandidateSymbols(provider, now = Date.now()) {
  const safePool = [...new Set(['BTCUSDT', 'ETHUSDT', 'SOLUSDT', ...CORE_SYMBOLS])]
    .map(compactSymbol)
    .filter(Boolean)
    .filter((symbol) => marketUniverseHasSymbol(provider, symbol));
  const connectedPool = safePool.filter((symbol) => {
    const feed = feedForSymbol(provider, symbol);
    return Boolean(feed && wsReady(feed.socket) && feed.ready);
  });
  const pool = connectedPool.length >= LIQUIDATION_SHARED_TARGETS_PER_PROVIDER
    ? connectedPool
    : safePool;
  const activity = pool.map((symbol) => {
    const state = getStats(provider, symbol, { create: false });
    const total = state ? Number(buildStatistics(state, '15m', now).total_notional || 0) : 0;
    return { symbol, total };
  }).sort((a, b) => b.total - a.total || a.symbol.localeCompare(b.symbol));
  const high = activity.find((entry) => entry.total > 0)?.symbol
    || (pool.includes('BTCUSDT') ? 'BTCUSDT' : pool[0]);
  const selected = [];
  if (high) selected.push({ symbol: high, role: 'high_activity' });
  const rotating = pool.filter((symbol) => symbol !== high);
  const round = liquidationSharedRound(now);
  const providerOffset = Math.max(0, LIQUIDATION_SHARED_PROVIDER_ORDER.indexOf(provider));
  const cursor = rotating.length > 0
    ? ((round * 2) + providerOffset * 3) % rotating.length
    : 0;
  for (let offset = 0; offset < rotating.length && selected.length < LIQUIDATION_SHARED_TARGETS_PER_PROVIDER; offset += 1) {
    const symbol = rotating[(cursor + offset) % rotating.length];
    if (!selected.some((entry) => entry.symbol === symbol)) {
      selected.push({ symbol, role: 'rotated_core' });
    }
  }
  for (const symbol of pool) {
    if (selected.length >= LIQUIDATION_SHARED_TARGETS_PER_PROVIDER) break;
    if (!selected.some((entry) => entry.symbol === symbol)) {
      selected.push({ symbol, role: 'core_fill' });
    }
  }
  return selected.slice(0, LIQUIDATION_SHARED_TARGETS_PER_PROVIDER);
}

function buildSharedCurrentLiquidationRow(provider, candidate, slot, round, now = Date.now()) {
  const symbol = compactSymbol(candidate?.symbol);
  const feed = feedForSymbol(provider, symbol);
  const observedSinceMs = Number(
    feed?.openedAt
      || (GLOBAL_FEED_PROVIDERS.has(provider) ? SERVICE_STARTED_AT_MS : now),
  );
  const state = getStats(provider, symbol, { observedSinceMs });
  const statistics = buildStatistics(state, '15m', now);
  const cutoff = now - PERIODS['15m'].durationMs;
  const items = (state?.recentEvents || feedEvents(feed || { eventsBySymbol: new Map() }, symbol))
    .filter((row) => integerValue(row?.time_ms) >= cutoff)
    .slice(0, 8)
    .map((row) => ({ ...row }));
  const connected = Boolean(feed && wsReady(feed.socket) && feed.ready);
  const info = feed || sourceInfo(provider);
  return {
    id: `contract_liquidation_shared:${provider}:${symbol}`,
    provider,
    market_type: 'contract',
    symbol,
    native_symbol: providerSymbol(provider, symbol),
    quote_asset: quoteFromCompact(symbol),
    quote_symbol: quoteFromCompact(symbol),
    period: '15m',
    connected,
    source: info.source || null,
    transport: info.transport || null,
    upstream_host: info.upstream_host || null,
    coverage: info.coverage || null,
    coverage_start_ms: statistics.coverage_start_ms,
    coverage_end_ms: statistics.coverage_end_ms,
    covered_ms: statistics.covered_ms,
    coverage_complete: statistics.coverage_complete === true,
    total_notional: Number(statistics.total_notional || 0),
    long_notional: Number(statistics.long_notional || 0),
    short_notional: Number(statistics.short_notional || 0),
    event_count: Math.max(0, Math.trunc(Number(statistics.event_count || 0))),
    long_count: Math.max(0, Math.trunc(Number(statistics.long_count || 0))),
    short_count: Math.max(0, Math.trunc(Number(statistics.short_count || 0))),
    last_event_at_ms: Number(state?.lastEventAt || 0) || null,
    session_started_at_ms: Number(feed?.openedAt || 0) || null,
    service_started_at_ms: SERVICE_STARTED_AT_MS,
    timestamp_ms: now,
    source_time: new Date(now).toISOString(),
    observed_at: new Date(now).toISOString(),
    items,
    backend_shared: true,
    shared_round: round,
    shared_slot: slot,
    selection_role: String(candidate?.role || 'core_fill'),
  };
}

function buildSharedCurrentLiquidationSnapshot(now = Date.now()) {
  const round = liquidationSharedRound(now);
  const rows = [];
  const providerCoverage = {};
  for (const provider of LIQUIDATION_SHARED_PROVIDER_ORDER) {
    const candidates = liquidationSharedCandidateSymbols(provider, now);
    const venueRows = candidates.map((candidate, index) =>
      buildSharedCurrentLiquidationRow(provider, candidate, index + 1, round, now));
    rows.push(...venueRows);
    providerCoverage[provider] = {
      rows: venueRows.length,
      connected: venueRows.filter((row) => row.connected === true).length,
      complete: venueRows.filter((row) => row.coverage_complete === true).length,
      event_pairs: venueRows.filter((row) => Number(row.event_count || 0) > 0).length,
      events: venueRows.reduce((sum, row) => sum + Number(row.event_count || 0), 0),
    };
  }
  return {
    rows,
    row_count: rows.length,
    provider_coverage: providerCoverage,
    provider_count: Object.values(providerCoverage).filter((item) => Number(item.rows || 0) > 0).length,
    connected_provider_count: Object.values(providerCoverage).filter((item) => Number(item.connected || 0) > 0).length,
    shared_round: round,
    targets_per_provider: LIQUIDATION_SHARED_TARGETS_PER_PROVIDER,
    rotation_minutes: Math.trunc(LIQUIDATION_SHARED_ROTATION_MS / 60_000),
    core_symbol_pool: [...new Set(['BTCUSDT', 'ETHUSDT', 'SOLUSDT', ...CORE_SYMBOLS])],
    generated_at: new Date(now).toISOString(),
  };
}

function getSharedCurrentLiquidationSnapshot(now = Date.now()) {
  const cached = liquidationSharedCurrentCache;
  if (cached.payload && now - Number(cached.cachedAt || 0) < LIQUIDATION_SHARED_CACHE_TTL_MS) {
    return {
      ...cached.payload,
      cache_hit: true,
      cache_age_ms: now - cached.cachedAt,
    };
  }
  const payload = buildSharedCurrentLiquidationSnapshot(now);
  liquidationSharedCurrentCache = { cachedAt: now, payload };
  return { ...payload, cache_hit: false, cache_age_ms: 0 };
}


function buildMarketLiquidationRow(provider, symbol, state, feed, statistics, now = Date.now()) {
  const info = feed || sourceInfo(provider);
  const recentItems = (state?.recentEvents || feedEvents(feed || { eventsBySymbol: new Map() }, symbol))
    .filter((row) => integerValue(row?.time_ms) >= now - PERIODS['15m'].durationMs)
    .slice(0, 5)
    .map((row) => ({ ...row }));
  return {
    id: `contract_liquidation_market:${provider}:${symbol}`,
    provider,
    market_type: 'contract',
    symbol,
    native_symbol: (() => {
      try {
        return providerSymbol(provider, symbol);
      } catch (_) {
        return symbol;
      }
    })(),
    quote_asset: 'USDT',
    period: '15m',
    connected: Boolean(feed && wsReady(feed.socket) && feed.ready),
    source: info.source || null,
    transport: info.transport || null,
    upstream_host: info.upstream_host || null,
    coverage: info.coverage || null,
    coverage_start_ms: statistics.coverage_start_ms,
    coverage_end_ms: statistics.coverage_end_ms,
    covered_ms: statistics.covered_ms,
    coverage_complete: statistics.coverage_complete === true,
    total_notional: Number(statistics.total_notional || 0),
    long_notional: Number(statistics.long_notional || 0),
    short_notional: Number(statistics.short_notional || 0),
    event_count: Math.max(0, Math.trunc(Number(statistics.event_count || 0))),
    long_count: Math.max(0, Math.trunc(Number(statistics.long_count || 0))),
    short_count: Math.max(0, Math.trunc(Number(statistics.short_count || 0))),
    last_event_at_ms: Number(state?.lastEventAt || 0) || null,
    timestamp_ms: now,
    source_time: new Date(now).toISOString(),
    observed_at: new Date(now).toISOString(),
    items: recentItems,
    backend_shared: true,
    main_layer: true,
  };
}

function buildMarketLiquidationSnapshot(now = Date.now()) {
  const providerCoverage = {};
  const eventRows = [];
  let directorySymbolCount = 0;
  let connectedSymbolCount = 0;
  let completeSymbolCount = 0;
  let eventSymbolCount = 0;
  let totalEvents = 0;
  let totalNotional = 0;
  let totalLongNotional = 0;
  let totalShortNotional = 0;

  for (const provider of LIQUIDATION_MARKET_PROVIDER_ORDER) {
    const symbols = marketUniverseSymbols(provider);
    directorySymbolCount += symbols.length;
    const info = sourceInfo(provider);
    let providerConnected = 0;
    let providerComplete = 0;
    let providerEventSymbols = 0;
    let providerEvents = 0;
    let providerTotal = 0;
    let providerLong = 0;
    let providerShort = 0;
    let providerLastEventAt = 0;
    const providerFeedKeys = new Set();

    for (const symbol of symbols) {
      const feed = feedForSymbol(provider, symbol);
      const connected = Boolean(feed && wsReady(feed.socket) && feed.ready);
      if (feed?.key) providerFeedKeys.add(feed.key);
      if (!connected) continue;
      providerConnected += 1;
      connectedSymbolCount += 1;

      const state = getStats(provider, symbol, {
        observedSinceMs: Number(feed.openedAt || now),
      });
      const statistics = buildStatistics(state, '15m', now);
      if (statistics.coverage_complete === true) {
        providerComplete += 1;
        completeSymbolCount += 1;
      }

      const eventCount = Math.max(0, Math.trunc(Number(statistics.event_count || 0)));
      const notional = Number(statistics.total_notional || 0);
      const longNotional = Number(statistics.long_notional || 0);
      const shortNotional = Number(statistics.short_notional || 0);
      providerEvents += eventCount;
      providerTotal += notional;
      providerLong += longNotional;
      providerShort += shortNotional;
      totalEvents += eventCount;
      totalNotional += notional;
      totalLongNotional += longNotional;
      totalShortNotional += shortNotional;
      providerLastEventAt = Math.max(providerLastEventAt, Number(state?.lastEventAt || 0));

      if (eventCount > 0) {
        providerEventSymbols += 1;
        eventSymbolCount += 1;
        eventRows.push(buildMarketLiquidationRow(provider, symbol, state, feed, statistics, now));
      }
    }

    const directoryRows = symbols.length;
    const connectedPct = directoryRows > 0 ? (providerConnected * 100) / directoryRows : 0;
    const completePct = directoryRows > 0 ? (providerComplete * 100) / directoryRows : 0;
    providerCoverage[provider] = {
      provider,
      directory_symbols: directoryRows,
      connected_symbols: providerConnected,
      coverage_complete_symbols: providerComplete,
      event_symbols: providerEventSymbols,
      events: providerEvents,
      total_notional: providerTotal,
      long_notional: providerLong,
      short_notional: providerShort,
      connected_pct: connectedPct,
      coverage_complete_pct: completePct,
      feed_count: providerFeedKeys.size,
      last_event_at_ms: providerLastEventAt || null,
      source: info.source,
      transport: info.transport,
      coverage: info.coverage,
      universe_error: String(liquidationMarketUniverseMeta.get(provider)?.error || ''),
    };
  }

  eventRows.sort((a, b) =>
    Number(b.total_notional || 0) - Number(a.total_notional || 0) ||
    Number(b.event_count || 0) - Number(a.event_count || 0) ||
    String(a.provider).localeCompare(String(b.provider)) ||
    String(a.symbol).localeCompare(String(b.symbol)));

  const returnedRows = eventRows.slice(0, LIQUIDATION_MARKET_MAX_EVENT_ROWS);
  const providerCount = Object.values(providerCoverage)
    .filter((row) => Number(row.directory_symbols || 0) > 0).length;
  const connectedProviderCount = Object.values(providerCoverage)
    .filter((row) => Number(row.connected_symbols || 0) > 0).length;
  const universeErrorFree = LIQUIDATION_MARKET_PROVIDER_ORDER.every((provider) =>
    !String(providerCoverage[provider]?.universe_error || '').trim());
  const fullDirectoryConnected = providerCount === LIQUIDATION_MARKET_PROVIDER_ORDER.length &&
    connectedProviderCount === LIQUIDATION_MARKET_PROVIDER_ORDER.length &&
    directorySymbolCount > 0 &&
    connectedSymbolCount === directorySymbolCount &&
    universeErrorFree;
  const fullWindowComplete = fullDirectoryConnected &&
    completeSymbolCount === directorySymbolCount;

  return {
    rows: returnedRows,
    row_count: returnedRows.length,
    untruncated_event_row_count: eventRows.length,
    rows_truncated: eventRows.length > returnedRows.length,
    provider_coverage: providerCoverage,
    provider_count: providerCount,
    connected_provider_count: connectedProviderCount,
    directory_symbol_count: directorySymbolCount,
    connected_symbol_count: connectedSymbolCount,
    coverage_complete_symbol_count: completeSymbolCount,
    event_symbol_count: eventSymbolCount,
    event_count: totalEvents,
    total_notional: totalNotional,
    long_notional: totalLongNotional,
    short_notional: totalShortNotional,
    connected_coverage_pct: directorySymbolCount > 0
      ? (connectedSymbolCount * 100) / directorySymbolCount
      : 0,
    complete_window_coverage_pct: directorySymbolCount > 0
      ? (completeSymbolCount * 100) / directorySymbolCount
      : 0,
    full_directory_connected: fullDirectoryConnected,
    full_window_coverage_complete: fullWindowComplete,
    generated_at: new Date(now).toISOString(),
  };
}

function getMarketLiquidationSnapshot(now = Date.now()) {
  const cached = liquidationMarketCurrentCache;
  if (cached.payload && now - Number(cached.cachedAt || 0) < LIQUIDATION_MARKET_CACHE_TTL_MS) {
    return {
      ...cached.payload,
      cache_hit: true,
      cache_age_ms: now - cached.cachedAt,
    };
  }
  const payload = buildMarketLiquidationSnapshot(now);
  liquidationMarketCurrentCache = { cachedAt: now, payload };
  return { ...payload, cache_hit: false, cache_age_ms: 0 };
}

export function getContractLiquidationSharedCurrentHealth() {
  const now = Date.now();
  const snapshot = getSharedCurrentLiquidationSnapshot(now);
  const marketSnapshot = getMarketLiquidationSnapshot(now);
  const feeds = [...FEEDS.values()];
  const marketCollectorFeedCount = feeds.filter((feed) => feed.marketCollector === true).length;
  const focusedUsdtDuplicateFeedCount = feeds.filter((feed) => {
    if (feed.marketCollector === true) return false;
    return [...feed.accessBySymbol.keys()].some((symbol) => quoteFromCompact(compactSymbol(symbol)) === 'USDT');
  }).length;
  const expectedMarketCollectorFeedCount = 4 + liquidationMarketHealth.bybit_shard_count;
  return {
    current_snapshot_endpoint: LIQUIDATION_CURRENT_ROUTE,
    current_snapshot_role: 'focused_fallback',
    current_snapshot_targets_per_provider: LIQUIDATION_SHARED_TARGETS_PER_PROVIDER,
    current_snapshot_rotation_minutes: Math.trunc(LIQUIDATION_SHARED_ROTATION_MS / 60_000),
    current_snapshot_rows: snapshot.row_count,
    current_snapshot_connected_provider_count: snapshot.connected_provider_count,
    current_snapshot_shared_round: snapshot.shared_round,
    current_snapshot_reads_start_exchange_connections: false,
    current_snapshot_exchange_connections_started_per_read: 0,
    current_snapshot_scales_with_users: false,
    current_snapshot_cache_ttl_seconds: Math.trunc(LIQUIDATION_SHARED_CACHE_TTL_MS / 1000),
    market_snapshot_endpoint: LIQUIDATION_MARKET_ROUTE,
    market_snapshot_role: 'official_maximum_public_coverage_main_layer',
    market_snapshot_directory_symbols: marketSnapshot.directory_symbol_count,
    market_snapshot_connected_symbols: marketSnapshot.connected_symbol_count,
    market_snapshot_complete_symbols: marketSnapshot.coverage_complete_symbol_count,
    market_snapshot_event_symbols: marketSnapshot.event_symbol_count,
    market_snapshot_event_rows: marketSnapshot.row_count,
    market_snapshot_full_directory_connected: marketSnapshot.full_directory_connected,
    market_snapshot_full_window_coverage_complete: marketSnapshot.full_window_coverage_complete,
    market_snapshot_reads_start_exchange_connections: false,
    market_snapshot_exchange_connections_started_per_read: 0,
    market_snapshot_scales_with_users: false,
    market_snapshot_cache_ttl_seconds: Math.trunc(LIQUIDATION_MARKET_CACHE_TTL_MS / 1000),
    bybit_market_shard_count: liquidationMarketHealth.bybit_shard_count,
    bybit_market_subscribed_symbols: liquidationMarketHealth.bybit_subscribed_symbols,
    bybit_market_subscription_chars: liquidationMarketHealth.bybit_subscription_chars,
    gate_market_wide_subscription: true,
    market_collector_feed_count: marketCollectorFeedCount,
    market_collector_expected_feed_count: expectedMarketCollectorFeedCount,
    focused_usdt_duplicate_feed_count: focusedUsdtDuplicateFeedCount,
    market_collector_topology_exact:
      marketCollectorFeedCount === expectedMarketCollectorFeedCount && focusedUsdtDuplicateFeedCount === 0,
    market_universe_refresh_minutes: Math.trunc(LIQUIDATION_MARKET_UNIVERSE_REFRESH_MS / 60_000),
    ...liquidationMarketHealth,
  };
}

async function gateLiqOrdersJson(urls) {
  let lastError = null;
  for (const target of urls) {
    try {
      const response = await fetch(target, {
        headers: { accept: 'application/json', 'X-Gate-Size-Decimal': '1' },
        signal: AbortSignal.timeout(15000),
      });
      const text = await response.text();
      if (!response.ok) throw new Error(`gate_liq_orders_http_${response.status}:${text.slice(0, 220)}`);
      const decoded = JSON.parse(text);
      if (!Array.isArray(decoded)) throw new Error('gate_liq_orders_payload_not_array');
      return decoded;
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError || new Error('gate_liq_orders_unavailable');
}

async function upsertGateLiquidationCoverage({ startMs, endMs, rowCount, parsedRowCount = 0, skippedRowCount = 0, truncated, normalizationComplete, complete }) {
  const row = {
    settle: 'usdt',
    bucket_start: liquidationIso(startMs),
    bucket_end: liquidationIso(endMs),
    row_count: Math.max(0, Math.trunc(Number(rowCount || 0))),
    parsed_row_count: Math.max(0, Math.trunc(Number(parsedRowCount || 0))),
    skipped_row_count: Math.max(0, Math.trunc(Number(skippedRowCount || 0))),
    truncated: truncated === true,
    normalization_complete: normalizationComplete === true,
    complete: complete === true,
    source: 'gate_official_public_futures_liq_orders_closed_minute_v1',
    cached_at: new Date().toISOString(),
  };
  const response = await fetch(
    `${SUPABASE_URL}/rest/v1/${LIQUIDATION_GATE_COVERAGE_TABLE}?on_conflict=settle,bucket_start`,
    {
      method: 'POST',
      headers: liquidationSupabaseHeaders({
        'content-type': 'application/json',
        prefer: 'resolution=merge-duplicates,return=minimal',
      }),
      body: JSON.stringify([row]),
      signal: AbortSignal.timeout(15000),
    },
  );
  const text = await response.text();
  if (!response.ok) throw new Error(`gate_liq_coverage_upsert_http_${response.status}:${text.slice(0, 220)}`);
}

async function replaceGateOfficialMinuteRows(startMs, rows) {
  const startIso = liquidationIso(startMs);
  const deleteQuery = new URLSearchParams({ provider: 'eq.gate', bucket_start: `eq.${startIso}` });
  const del = await fetch(`${SUPABASE_URL}/rest/v1/${LIQUIDATION_MINUTE_TABLE}?${deleteQuery}`, {
    method: 'DELETE',
    headers: liquidationSupabaseHeaders({ prefer: 'return=minimal' }),
    signal: AbortSignal.timeout(15000),
  });
  const deleteText = await del.text();
  if (!del.ok) throw new Error(`gate_liq_minute_replace_delete_http_${del.status}:${deleteText.slice(0, 220)}`);
  if (rows.length) await upsertLiquidationMinuteRows(rows);
}

function gateOfficialMinuteRows(rawRows, startMs, endMs) {
  const buckets = new Map();
  let skipped = 0;
  for (const raw of rawRows) {
    const native = String(raw?.contract || '').trim().toUpperCase();
    const symbol = compactSymbol(native);
    const timeMs = integerValue(raw?.time) * 1000;
    const signedPositionSize = numberValue(raw?.size);
    const rawOrderSize = numberValue(raw?.order_size);
    const contracts = rawOrderSize != null && rawOrderSize !== 0
      ? Math.abs(rawOrderSize)
      : signedPositionSize != null ? Math.abs(signedPositionSize) : null;
    const price = positiveNumber(raw?.fill_price) ?? positiveNumber(raw?.order_price);
    if (!native || !symbol || timeMs < startMs || timeMs >= endMs || signedPositionSize == null || signedPositionSize === 0 || contracts == null || contracts <= 0 || price == null) {
      skipped += 1;
      continue;
    }
    let target = buckets.get(symbol);
    if (!target) {
      target = { state: { provider: 'gate', symbol, observedSinceMs: startMs, createdAt: startMs, lastGapAtMs: 0 }, bucket: createBucket(startMs, MINUTE_BUCKET_MS) };
      buckets.set(symbol, target);
    }
    target.events = target.events || [];
    target.events.push({ native, symbol, timeMs, signedPositionSize, contracts, price });
  }
  return { buckets, skipped };
}

async function materializeGateOfficialMinuteRows(rawRows, startMs, endMs) {
  const prepared = gateOfficialMinuteRows(rawRows, startMs, endMs);
  const rows = [];
  let parsed = 0;
  for (const { state, bucket, events = [] } of prepared.buckets.values()) {
    for (const event of events) {
      const multiplier = gateContractMultiplierFromSharedCache(event.native, 'usdt');
      const quantity = event.contracts * multiplier;
      const notional = event.price * quantity;
      if (!Number.isFinite(quantity) || quantity <= 0 || !Number.isFinite(notional) || notional <= 0) {
        prepared.skipped += 1;
        continue;
      }
      // Gate official SDK semantics: positive size = short position,
      // negative size = long position. `left` is intentionally ignored.
      applyEventToBucket(bucket, {
        id: `gate-liq-orders:${event.native}:${event.timeMs}:${event.signedPositionSize}:${event.contracts}:${event.price}`,
        provider: 'gate',
        symbol: event.symbol,
        time_ms: event.timeMs,
        price: event.price,
        quantity,
        quantity_contracts: event.contracts,
        quantity_unit: 'base_asset',
        notional,
        liquidation_side: event.signedPositionSize > 0 ? 'short' : 'long',
        order_side: event.signedPositionSize > 0 ? 'buy' : 'sell',
        price_type: 'official_liq_orders_fill_or_order_price',
      });
      parsed += 1;
    }
    const row = liquidationPersistRow(state, bucket, Date.now(), 'gate_official_public_futures_liq_orders_closed_minute_v1');
    if (!row) continue;
    row.bucket_closed = true;
    row.provisional = false;
    row.coverage_complete = true;
    row.observed_since = liquidationIso(startMs);
    row.last_gap_at = null;
    rows.push(row);
  }
  return { rows, parsed, skipped: prepared.skipped };
}

async function pollGateOfficialLiquidationMinute(now = Date.now(), { force = false } = {}) {
  if (!LIQUIDATION_PERSISTENCE_ENABLED || gateLiqOrdersInflight) return gateLiqOrdersInflight;
  const endMs = Math.floor(now / MINUTE_BUCKET_MS) * MINUTE_BUCKET_MS;
  const startMs = endMs - MINUTE_BUCKET_MS;
  if (startMs <= 0 || (!force && gateLiqOrdersLastWindowStart === startMs)) return null;
  gateLiqOrdersLastWindowStart = startMs;
  gateLiqOrdersInflight = (async () => {
    gateLiqOrdersHealth.attempts += 1;
    gateLiqOrdersHealth.last_window_start = liquidationIso(startMs);
    gateLiqOrdersHealth.last_window_end = liquidationIso(endMs);
    try {
      const from = Math.floor(startMs / 1000);
      const to = Math.floor(endMs / 1000) - 1;
      const query = `from=${from}&to=${to}&limit=${GATE_LIQ_ORDERS_LIMIT}`;
      const rawRows = await gateLiqOrdersJson([
        `https://fx-api.gateio.ws/api/v4/futures/usdt/liq_orders?${query}`,
        `https://api.gateio.ws/api/v4/futures/usdt/liq_orders?${query}`,
      ]);
      const truncated = rawRows.length >= GATE_LIQ_ORDERS_LIMIT;
      gateLiqOrdersHealth.last_rows = rawRows.length;
      if (truncated) gateLiqOrdersHealth.truncated_windows += 1;

      let materialized = { rows: [], parsed: 0, skipped: 0 };
      let normalizationComplete = false;
      let complete = false;
      if (!truncated) {
        materialized = await materializeGateOfficialMinuteRows(rawRows, startMs, endMs);
        normalizationComplete = materialized.skipped === 0;
        complete = normalizationComplete;
        gateLiqOrdersHealth.parsed_events += materialized.parsed;
        gateLiqOrdersHealth.skipped_rows += materialized.skipped;
        if (!normalizationComplete) gateLiqOrdersHealth.normalization_incomplete_windows += 1;
      }

      await upsertGateLiquidationCoverage({
        startMs,
        endMs,
        rowCount: rawRows.length,
        parsedRowCount: materialized.parsed,
        skippedRowCount: materialized.skipped,
        truncated,
        normalizationComplete,
        complete,
      });
      if (complete) {
        await replaceGateOfficialMinuteRows(startMs, materialized.rows);
        gateOfficialFinalizedMinuteStarts.set(startMs, Date.now());
        gateLiqOrdersHealth.complete_windows += 1;
        liquidationPersistenceHealth.minute_persisted_rows_total += materialized.rows.length;
        liquidationHistoryCache.clear();
      }
      gateLiqOrdersHealth.successes += 1;
      gateLiqOrdersHealth.last_completed_at = new Date().toISOString();
      gateLiqOrdersHealth.last_error = '';
      return { ok: true, startMs, endMs, rows: rawRows.length, complete, truncated };
    } catch (error) {
      gateLiqOrdersHealth.failures += 1;
      gateLiqOrdersHealth.last_error = String(error?.message || error).slice(0, 500);
      throw error;
    } finally {
      gateLiqOrdersInflight = null;
    }
  })();
  return gateLiqOrdersInflight;
}

export function getContractLiquidationPersistenceHealth() {
  return {
    ok: true,
    version: STEP_VERSION,
    persistence_enabled: LIQUIDATION_PERSISTENCE_ENABLED,
    history_endpoint: LIQUIDATION_HISTORY_ROUTE,
    current_snapshot_endpoint: LIQUIDATION_CURRENT_ROUTE,
    current_snapshot_role: 'focused_fallback',
    market_snapshot_endpoint: LIQUIDATION_MARKET_ROUTE,
    market_snapshot_role: 'official_maximum_public_coverage_main_layer',
    current_snapshot_targets_per_provider: LIQUIDATION_SHARED_TARGETS_PER_PROVIDER,
    current_snapshot_rotation_minutes: Math.trunc(LIQUIDATION_SHARED_ROTATION_MS / 60_000),
    current_snapshot_reads_start_exchange_connections: false,
    current_snapshot_scales_with_users: false,
    storage_table: LIQUIDATION_HOUR_TABLE,
    minute_storage_table: LIQUIDATION_MINUTE_TABLE,
    gate_minute_coverage_table: LIQUIDATION_GATE_COVERAGE_TABLE,
    step997_history_rpc: LIQUIDATION_STEP997_HISTORY_RPC,
    raw_events_persisted: false,
    aggregate_period: '1h',
    aggregate_retention_days: LIQUIDATION_HOUR_RETENTION_DAYS,
    step997_unified_history_ready: LIQUIDATION_PERSISTENCE_ENABLED,
    step997_history_intervals: ['1m', '5m', '15m', '1H', '6H', '24H'],
    step997_default_history_backward_compatible_1h: true,
    minute_aggregate_retention_hours: LIQUIDATION_MINUTE_RETENTION_HOURS,
    minute_persist_queue: liquidationMinutePersistQueue.size,
    minute_persist_inflight: Boolean(liquidationMinutePersistInflight),
    zero_event_rows_persisted: false,
    raw_events_process_memory_only: true,
    gate_liq_orders: { ...gateLiqOrdersHealth },
    close_grace_seconds: Math.trunc(LIQUIDATION_CLOSE_GRACE_MS / 1000),
    persist_flush_seconds: Math.trunc(LIQUIDATION_PERSIST_FLUSH_MS / 1000),
    persist_queue: liquidationPersistQueue.size,
    persist_inflight: Boolean(liquidationPersistInflight),
    history_cache_entries: liquidationHistoryCache.size,
    history_inflight_entries: liquidationHistoryInflight.size,
    history_cache_ttl_seconds: Math.trunc(LIQUIDATION_HISTORY_CACHE_TTL_MS / 1000),
    history_stale_seconds: Math.trunc(LIQUIDATION_HISTORY_STALE_MS / 1000),
    exchange_requests_started_by_history_reads: 0,
    shared_current_health: getContractLiquidationSharedCurrentHealth(),
    ...liquidationPersistenceHealth,
  };
}

if (LIQUIDATION_PERSISTENCE_ENABLED) {
  const persistTimer = setInterval(() => {
    queueRecentLiquidationMinuteBuckets();
    queueRecentLiquidationHourBuckets();
    flushLiquidationMinutePersistQueue().catch((error) => {
      console.error(`[Step${STEP_VERSION}] liquidation minute flush failed: ${error?.message || error}`);
    });
    flushLiquidationPersistQueue().catch((error) => {
      console.error(`[Step${STEP_VERSION}] liquidation hour flush failed: ${error?.message || error}`);
    });
  }, LIQUIDATION_PERSIST_FLUSH_MS);
  persistTimer.unref?.();
  const cleanupTimer = setInterval(() => {
    cleanupLiquidationPersistence().catch((error) => {
      liquidationPersistenceHealth.cleanup_error = String(error?.message || error).slice(0, 300);
      console.error(`[Step${STEP_VERSION}] liquidation cleanup failed: ${error?.message || error}`);
    });
  }, 6 * 60 * 60_000);
  cleanupTimer.unref?.();
  const startupPersistenceTimer = setTimeout(() => {
    queueRecentLiquidationMinuteBuckets();
    queueRecentLiquidationHourBuckets();
    flushLiquidationMinutePersistQueue().catch(() => {});
    flushLiquidationPersistQueue().catch(() => {});
    cleanupLiquidationPersistence().catch((error) => {
      liquidationPersistenceHealth.cleanup_error = String(error?.message || error).slice(0, 300);
    });
  }, 5_000);
  startupPersistenceTimer.unref?.();
}

if (LIQUIDATION_PERSISTENCE_ENABLED) {
  const gateStartupTimer = setTimeout(() => {
    pollGateOfficialLiquidationMinute(Date.now(), { force: true }).catch(() => {});
  }, 12_000);
  gateStartupTimer.unref?.();
  const gatePollTimer = setInterval(() => {
    pollGateOfficialLiquidationMinute().catch(() => {});
  }, GATE_LIQ_ORDERS_POLL_MS);
  gatePollTimer.unref?.();
}

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

function supportsNativeContract(provider, rawSymbol) {
  const compact = compactSymbol(rawSymbol);
  const quote = quoteFromCompact(compact);
  if (quote === 'USDT') return SUPPORTED_PROVIDERS.has(provider);
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

function providerSymbol(provider, rawSymbol) {
  const compact = compactSymbol(rawSymbol);
  const quote = quoteFromCompact(compact);
  const base = baseFromCompact(compact);
  if (!base || !quote) throw new Error('invalid_symbol');
  if (!supportsNativeContract(provider, compact)) {
    throw new Error('unsupported_native_contract_quote');
  }
  if ((provider === 'bybit' || provider === 'bitget') && quote === 'USDC') {
    return `${base}PERP`;
  }
  if ((provider === 'bybit' || provider === 'bitget') && quote === 'USD') {
    return `${base}USD`;
  }
  if (provider === 'okx') return `${base}-${quote}-SWAP`;
  if (provider === 'gate') return `${base}_${quote}`;
  return `${base}${quote}`;
}


function bybitCategory(symbol) {
  return quoteFromCompact(compactSymbol(symbol)) === 'USD' ? 'inverse' : 'linear';
}

function gateSettle(symbol) {
  return quoteFromCompact(compactSymbol(symbol)) === 'USD' ? 'btc' : 'usdt';
}

function bitgetInstType(symbol) {
  const quote = quoteFromCompact(compactSymbol(symbol));
  if (quote === 'USDC') return 'usdc-futures';
  if (quote === 'USD') return 'coin-futures';
  return 'usdt-futures';
}

function displaySymbolForNative(feed, nativeSymbol) {
  const normalizedNative = compactSymbol(nativeSymbol);
  const mapped = feed?.nativeToDisplay?.get(normalizedNative);
  if (mapped) return mapped;
  for (const display of feed.accessBySymbol.keys()) {
    try {
      if (compactSymbol(providerSymbol(feed.provider, display)) === normalizedNative) return display;
    } catch (_) {}
  }
  if (feed.requestedDisplaySymbol && compactSymbol(feed.requestedNativeSymbol) === normalizedNative) {
    return feed.requestedDisplaySymbol;
  }
  return normalizedNative;
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
  if (cached && Date.now() - cached.storedAt <= META_FRESH_MS) return cached;
  const decoded = await fetchJson(
    `https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId=${encodeURIComponent(instId)}`,
  );
  const row = Array.isArray(decoded?.data) ? decoded.data[0] : null;
  const ctVal = positiveNumber(row?.ctVal) ?? 1;
  const ctMult = positiveNumber(row?.ctMult) ?? 1;
  const faceValue = ctVal * ctMult;
  const valueCurrency = String(row?.ctValCcy || '').toUpperCase();
  const meta = { faceValue, valueCurrency, storedAt: Date.now() };
  META_CACHE.set(key, meta);
  return meta;
}

async function gateContractMultiplier(contract, settle = 'usdt') {
  const key = `gate:${settle}:${contract}`;
  const cached = META_CACHE.get(key);
  if (cached && Date.now() - cached.storedAt <= META_FRESH_MS) {
    return cached.multiplier;
  }
  const decoded = await fetchFirstJson([
    `https://fx-api.gateio.ws/api/v4/futures/${settle}/contracts/${encodeURIComponent(contract)}`,
    `https://api.gateio.ws/api/v4/futures/${settle}/contracts/${encodeURIComponent(contract)}`,
  ]);
  const multiplier = positiveNumber(decoded?.quanto_multiplier) ?? 1;
  META_CACHE.set(key, { multiplier, storedAt: Date.now() });
  return multiplier;
}

function gateContractMultiplierFromSharedCache(contract, settle = 'usdt') {
  const cached = META_CACHE.get(`gate:${settle}:${contract}`);
  if (!cached || Date.now() - Number(cached.storedAt || 0) > META_FRESH_MS) return null;
  return positiveNumber(cached.multiplier);
}

function feedKey(provider, symbol) {
  if (provider === 'gate') return `${provider}|all|${gateSettle(symbol)}`;
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

function createFeed(provider, symbol, {
  keyOverride = '',
  subscriptionSymbols = null,
  marketCollector = false,
  marketWide = false,
} = {}) {
  const key = keyOverride || feedKey(provider, symbol);
  const info = sourceInfo(provider);
  const feed = {
    key,
    provider,
    requestedDisplaySymbol: compactSymbol(symbol),
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
    nativeToDisplay: new Map(),
    subscriptionSymbols: Array.isArray(subscriptionSymbols)
      ? [...new Set(subscriptionSymbols.map(compactSymbol).filter(Boolean))]
      : [],
    waiters: new Set(),
    heartbeatTimer: null,
    persistent: GLOBAL_FEED_PROVIDERS.has(provider) || marketCollector,
    core: false,
    marketCollector,
    marketWide: marketWide || GLOBAL_FEED_PROVIDERS.has(provider),
    lastAccessAt: Date.now(),
    ...info,
  };
  for (const display of feed.subscriptionSymbols) {
    feed.accessBySymbol.set(display, Date.now());
    try {
      feed.nativeToDisplay.set(compactSymbol(providerSymbol(provider, display)), display);
    } catch (_) {}
  }
  if (feed.requestedDisplaySymbol) {
    feed.nativeToDisplay.set(compactSymbol(feed.requestedNativeSymbol), feed.requestedDisplaySymbol);
  }
  FEEDS.set(key, feed);
  return feed;
}

function feedForSymbol(provider, symbol) {
  const compact = compactSymbol(symbol);
  if (!compact) return null;
  if (provider === 'bybit') {
    for (const feed of FEEDS.values()) {
      if (feed.provider === 'bybit' &&
          feed.marketCollector === true &&
          feed.accessBySymbol.has(compact)) {
        return feed;
      }
    }
  }
  return FEEDS.get(feedKey(provider, compact)) || null;
}

function getFeed(provider, symbol, { allowDynamic = true } = {}) {
  const existing = feedForSymbol(provider, symbol);
  if (existing) return existing;
  if (!allowDynamic) return null;
  return createFeed(provider, symbol);
}


function marketUniverseRows(provider) {
  const rows = liquidationMarketUniverse.get(provider);
  return rows instanceof Map ? [...rows.values()] : [];
}

function marketUniverseSymbols(provider) {
  const rows = liquidationMarketUniverse.get(provider);
  return rows instanceof Map ? [...rows.keys()] : [];
}

function marketUniverseHasSymbol(provider, symbol) {
  const rows = liquidationMarketUniverse.get(provider);
  if (!(rows instanceof Map) || rows.size === 0) return true;
  return rows.has(compactSymbol(symbol));
}

function normalizeMarketUniverseRows(provider, rows) {
  const normalized = new Map();
  for (const raw of Array.isArray(rows) ? rows : []) {
    const symbol = compactSymbol(raw?.symbol);
    const quote = String(raw?.quote_asset || quoteFromCompact(symbol)).toUpperCase();
    if (!symbol || quote !== 'USDT' || !supportsNativeContract(provider, symbol)) continue;
    normalized.set(symbol, {
      ...raw,
      provider,
      market_type: 'contract',
      symbol,
      quote_asset: 'USDT',
    });
  }
  return normalized;
}


function seedLiquidationMetaFromSharedUniverse(provider, rows) {
  if (!(rows instanceof Map)) return;
  const now = Date.now();
  for (const [symbol, row] of rows) {
    if (provider === 'gate') {
      const multiplier = positiveNumber(row?.contract_multiplier);
      const native = String(row?.native_symbol || providerSymbol('gate', symbol));
      if (multiplier != null && native) {
        META_CACHE.set(`gate:${gateSettle(symbol)}:${native}`, {
          multiplier,
          storedAt: now,
          source: 'shared_market_universe',
        });
      }
    } else if (provider === 'okx') {
      const faceValue = positiveNumber(row?.contract_value);
      const valueCurrency = String(row?.contract_value_currency || '').toUpperCase();
      const native = String(row?.native_symbol || providerSymbol('okx', symbol));
      if (faceValue != null && native) {
        META_CACHE.set(`okx:${native}`, {
          faceValue,
          valueCurrency,
          storedAt: now,
          source: 'shared_market_universe',
        });
      }
    }
  }
}

function bybitSubscriptionTopic(symbol) {
  return `allLiquidation.${providerSymbol('bybit', symbol)}`;
}

function partitionBybitMarketSymbols(symbols) {
  const chunks = [];
  let current = [];
  let currentChars = 2;
  for (const symbol of symbols) {
    const topic = bybitSubscriptionTopic(symbol);
    const topicChars = JSON.stringify(topic).length + (current.length ? 1 : 0);
    if (current.length > 0 && currentChars + topicChars > BYBIT_SHARD_ARG_CHAR_BUDGET) {
      chunks.push(current);
      current = [];
      currentChars = 2;
    }
    current.push(symbol);
    currentChars += JSON.stringify(topic).length + (current.length > 1 ? 1 : 0);
  }
  if (current.length) chunks.push(current);
  return chunks;
}

function closeBybitMarketShards() {
  for (const key of [...bybitMarketShardKeys]) {
    const feed = FEEDS.get(key);
    if (feed) {
      closeFeed(feed);
      FEEDS.delete(key);
    }
    bybitMarketShardKeys.delete(key);
  }
}

let bybitMarketUniverseSignature = '';

function rebuildBybitMarketShards(symbols) {
  const safeSymbols = [...new Set(symbols.map(compactSymbol).filter(Boolean))].sort();
  const signature = safeSymbols.join(',');
  if (signature === bybitMarketUniverseSignature && bybitMarketShardKeys.size > 0) return false;

  closeBybitMarketShards();
  bybitMarketUniverseSignature = signature;

  const chunks = partitionBybitMarketSymbols(safeSymbols);
  let totalChars = 0;
  let maxShardChars = 0;
  chunks.forEach((chunk, index) => {
    if (!chunk.length) return;
    const key = `bybit|market|${index}`;
    const feed = createFeed('bybit', chunk[0], {
      keyOverride: key,
      subscriptionSymbols: chunk,
      marketCollector: true,
      marketWide: false,
    });
    feed.core = true;
    feed.persistent = true;
    feed.marketShardIndex = index;
    feed.marketShardCount = chunks.length;
    bybitMarketShardKeys.add(key);
    const shardChars = JSON.stringify(chunk.map(bybitSubscriptionTopic)).length;
    totalChars += shardChars;
    maxShardChars = Math.max(maxShardChars, shardChars);
    ensureFeed(feed).catch(() => {});
  });

  liquidationMarketHealth.bybit_shard_rebuilds += 1;
  liquidationMarketHealth.bybit_shard_count = chunks.length;
  liquidationMarketHealth.bybit_subscribed_symbols = safeSymbols.length;
  liquidationMarketHealth.bybit_subscription_chars = totalChars;
  liquidationMarketHealth.bybit_max_shard_subscription_chars = maxShardChars;
  return true;
}

function ensureGlobalLiquidationFeed(provider) {
  const feed = markCoreFeed(getFeed(provider, 'BTCUSDT'));
  feed.marketCollector = true;
  feed.marketWide = true;
  ensureFeed(feed).catch(() => {});
  return feed;
}

async function refreshLiquidationMarketUniverse({ reason = 'scheduled' } = {}) {
  if (liquidationMarketUniverseRefreshInflight) return await liquidationMarketUniverseRefreshInflight;
  liquidationMarketUniverseRefreshInflight = (async () => {
    liquidationMarketHealth.universe_refresh_attempts += 1;
    const errors = [];
    for (const provider of LIQUIDATION_MARKET_PROVIDER_ORDER) {
      try {
        const rows = await getMarketUniverseRows(provider, 'contract', 'USDT');
        const normalized = normalizeMarketUniverseRows(provider, rows);
        if (normalized.size <= 0) throw new Error('empty_usdt_contract_universe');
        liquidationMarketUniverse.set(provider, normalized);
        seedLiquidationMetaFromSharedUniverse(provider, normalized);
        liquidationMarketUniverseMeta.set(provider, {
          provider,
          rows: normalized.size,
          refreshed_at: new Date().toISOString(),
          reason,
          error: '',
        });
      } catch (error) {
        errors.push(`${provider}:${String(error?.message || error)}`);
        const existing = liquidationMarketUniverse.get(provider);
        if (!(existing instanceof Map) || existing.size === 0) {
          const fallback = normalizeMarketUniverseRows(
            provider,
            CORE_SYMBOLS.map((symbol) => ({
              provider,
              market_type: 'contract',
              symbol,
              quote_asset: 'USDT',
              source: 'core_fallback_until_shared_universe_ready',
            })),
          );
          liquidationMarketUniverse.set(provider, fallback);
        }
        liquidationMarketUniverseMeta.set(provider, {
          provider,
          rows: liquidationMarketUniverse.get(provider)?.size || 0,
          refreshed_at: new Date().toISOString(),
          reason,
          error: String(error?.message || error),
        });
      }
    }

    for (const provider of ['binance', 'okx', 'bitget', 'gate']) {
      ensureGlobalLiquidationFeed(provider);
    }
    rebuildBybitMarketShards(marketUniverseSymbols('bybit'));

    liquidationMarketHealth.last_universe_refresh_at = Date.now();
    if (errors.length) {
      liquidationMarketHealth.universe_refresh_failures += 1;
      liquidationMarketHealth.last_universe_refresh_error = errors.join('|').slice(0, 1000);
    } else {
      liquidationMarketHealth.universe_refresh_successes += 1;
      liquidationMarketHealth.last_universe_refresh_error = '';
    }
    liquidationMarketCurrentCache = { cachedAt: 0, payload: null };
    liquidationSharedCurrentCache = { cachedAt: 0, payload: null };
    return {
      ok: errors.length === 0,
      errors,
      provider_rows: Object.fromEntries(
        LIQUIDATION_MARKET_PROVIDER_ORDER.map((provider) => [
          provider,
          liquidationMarketUniverse.get(provider)?.size || 0,
        ]),
      ),
    };
  })().finally(() => {
    liquidationMarketUniverseRefreshInflight = null;
  });
  return await liquidationMarketUniverseRefreshInflight;
}

function touchFeed(feed, symbol) {
  const now = Date.now();
  const display = compactSymbol(symbol);
  feed.lastAccessAt = now;
  feed.accessBySymbol.set(display, now);
  try {
    feed.nativeToDisplay?.set(compactSymbol(providerSymbol(feed.provider, display)), display);
  } catch (_) {}
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
    latest_event_time_ms: 0,
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
  bucket.latest_event_time_ms = Math.max(Number(bucket.latest_event_time_ms || 0), integerValue(row?.time_ms));
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
  target.latest_event_time_ms = Math.max(Number(target.latest_event_time_ms || 0), Number(bucket.latest_event_time_ms || 0));
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
      if (state.provider !== feed.provider) continue;
      if (feed.provider === 'gate') {
        const expectedSettle = gateSettle(feed.requestedDisplaySymbol);
        if (gateSettle(state.symbol) !== expectedSettle) continue;
      }
      state.lastGapAtMs = now;
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
  const minuteBucket = bucketFor(state.minuteBuckets, timeMs, MINUTE_BUCKET_MS);
  applyEventToBucket(minuteBucket, row);
  queueLiquidationMinuteBucket(state, minuteBucket);
  applyEventToBucket(bucketFor(state.quarterBuckets, timeMs, QUARTER_BUCKET_MS), row);
  const hourBucket = bucketFor(state.hourBuckets, timeMs, HOUR_BUCKET_MS);
  applyEventToBucket(hourBucket, row);
  queueLiquidationHourBucket(state, hourBucket);
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
  if (feed.provider === 'binance') {
    return 'wss://fstream.binance.com/market/ws/!forceOrder@arr';
  }
  if (feed.provider === 'okx') return 'wss://ws.okx.com:8443/ws/v5/public';
  if (feed.provider === 'bybit') {
    return `wss://stream.bybit.com/v5/public/${bybitCategory(feed.requestedDisplaySymbol)}`;
  }
  if (feed.provider === 'bitget') return 'wss://ws.bitget.com/v3/ws/public';
  if (feed.provider === 'gate') {
    return `wss://fx-ws.gateio.ws/v4/ws/${gateSettle(feed.requestedDisplaySymbol)}`;
  }
  throw new Error('unsupported_provider');
}

function subscribeFeed(feed) {
  const socket = feed.socket;
  if (feed.provider === 'binance') return true;
  if (feed.provider === 'okx') {
    return sendWs(socket, {
      id: 'kaka657',
      op: 'subscribe',
      args: [{ channel: 'liquidation-orders', instType: 'SWAP' }],
    });
  }
  if (feed.provider === 'bybit') {
    const symbols = feed.subscriptionSymbols.length
      ? feed.subscriptionSymbols
      : [feed.requestedDisplaySymbol];
    const args = symbols
      .map((symbol) => {
        try {
          return bybitSubscriptionTopic(symbol);
        } catch (_) {
          return '';
        }
      })
      .filter(Boolean);
    return args.length > 0 && sendWs(socket, {
      op: 'subscribe',
      args,
    });
  }
  if (feed.provider === 'bitget') {
    const types = ['usdt-futures', 'usdc-futures', 'coin-futures'];
    return types
      .map((instType) => sendWs(socket, {
        op: 'subscribe',
        args: [{ instType, topic: 'liquidation' }],
      }))
      .some(Boolean);
  }
  if (feed.provider === 'gate') {
    return sendWs(socket, {
      time: Math.floor(Date.now() / 1000),
      channel: 'futures.public_liquidates',
      event: 'subscribe',
      payload: [feed.marketWide ? '!all' : feed.requestedNativeSymbol],
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
    const contractMeta = await okxContractMultiplier(native);
    const displaySymbol = compactSymbol(native);
    const base = baseFromCompact(displaySymbol);
    const quote = quoteFromCompact(displaySymbol);
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
      let quantity = null;
      let notional = null;
      if (contracts != null) {
        if (!contractMeta.valueCurrency || contractMeta.valueCurrency === base) {
          quantity = contracts * contractMeta.faceValue;
          notional = price == null ? null : price * quantity;
        } else if (contractMeta.valueCurrency === quote) {
          notional = contracts * contractMeta.faceValue;
          quantity = price == null ? null : notional / price;
        }
      }
      if (price == null || quantity == null || quantity <= 0 || notional == null || !liquidationSide) continue;
      addEvent(feed, {
        id: `okx:${native}:${timeMs}:${liquidationSide}:${contracts}`,
        symbol,
        native_symbol: native,
        time_ms: timeMs,
        price,
        quantity,
        quantity_contracts: contracts,
        quantity_unit: 'base_asset',
        notional,
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
    const native = compactSymbol(row?.s);
    const symbol = displaySymbolForNative(feed, native);
    const side = String(row?.S || '').toLowerCase();
    const price = positiveNumber(row?.p);
    const rawQuantity = positiveNumber(row?.v);
    const timeMs = integerValue(row?.T) || integerValue(data?.ts) || Date.now();
    const inverse = quoteFromCompact(symbol) === 'USD';
    const notional = inverse ? rawQuantity : (price != null && rawQuantity != null ? price * rawQuantity : null);
    const quantity = inverse && price != null && rawQuantity != null ? rawQuantity / price : rawQuantity;
    if (!symbol || !['buy', 'sell'].includes(side) || price == null || quantity == null || notional == null) continue;
    addEvent(feed, {
      id: `bybit:${symbol}:${timeMs}:${side}:${rawQuantity}`,
      symbol,
      native_symbol: String(row?.s || native),
      time_ms: timeMs,
      price,
      quantity,
      notional,
      quantity_unit: 'base_asset',
      liquidation_side: side === 'buy' ? 'long' : 'short',
      order_side: side,
      price_type: 'bankruptcy',
    });
  }
}

async function handleBitget(feed, data) {
  if (String(data?.arg?.topic || '') !== 'liquidation' || !Array.isArray(data?.data)) return;
  for (const row of data.data) {
    const native = compactSymbol(row?.symbol);
    const symbol = displaySymbolForNative(feed, native);
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
      native_symbol: String(row?.symbol || native),
      time_ms: timeMs,
      price,
      quantity,
      notional,
      quantity_unit: 'base_asset',
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
    const multiplier = await gateContractMultiplier(native, gateSettle(symbol));
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
      quantity_unit: 'base_asset',
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

function scheduleLiquidationMarketUniverseRetry(reason = 'startup_retry') {
  if (liquidationMarketUniverseRetryTimer) return;
  liquidationMarketUniverseRetryTimer = setTimeout(async () => {
    liquidationMarketUniverseRetryTimer = null;
    try {
      const result = await refreshLiquidationMarketUniverse({ reason });
      if (result?.ok !== true) {
        scheduleLiquidationMarketUniverseRetry('startup_retry');
      }
    } catch (error) {
      liquidationMarketHealth.last_universe_refresh_error =
        `retry:${String(error?.message || error)}`.slice(0, 1000);
      scheduleLiquidationMarketUniverseRetry('startup_retry');
    }
  }, LIQUIDATION_MARKET_STARTUP_RETRY_MS);
  liquidationMarketUniverseRetryTimer.unref?.();
}

async function bootstrapCollection() {
  const startupUniverse = await refreshLiquidationMarketUniverse({ reason: 'startup' });
  if (startupUniverse?.ok !== true) {
    scheduleLiquidationMarketUniverseRetry('startup_retry');
  }

  // Keep the original 5 x 3 focused snapshot as a compatibility/fallback layer,
  // but reuse the already-running market collectors. No focused read creates
  // a second exchange connection.
  const focusedSymbols = [...new Set(['BTCUSDT', 'ETHUSDT', 'SOLUSDT', ...CORE_SYMBOLS])];
  for (const provider of LIQUIDATION_MARKET_PROVIDER_ORDER) {
    for (const symbol of focusedSymbols) {
      if (!marketUniverseHasSymbol(provider, symbol)) continue;
      const feed = feedForSymbol(provider, symbol);
      if (!feed) continue;
      touchFeed(feed, symbol);
      getStats(provider, symbol, {
        observedSinceMs: Number(feed.openedAt || Date.now()),
      });
    }
  }
}

const bootstrapTimer = setTimeout(() => {
  bootstrapCollection().catch((error) => {
    liquidationMarketHealth.last_universe_refresh_error =
      `startup:${String(error?.message || error)}`.slice(0, 1000);
  });
}, 900);
bootstrapTimer.unref?.();

const marketUniverseRefreshTimer = setInterval(() => {
  refreshLiquidationMarketUniverse({ reason: 'scheduled' })
    .then((result) => {
      if (result?.ok !== true) scheduleLiquidationMarketUniverseRetry('scheduled_retry');
    })
    .catch((error) => {
      liquidationMarketHealth.last_universe_refresh_error =
        `scheduled:${String(error?.message || error)}`.slice(0, 1000);
      scheduleLiquidationMarketUniverseRetry('scheduled_retry');
    });
}, LIQUIDATION_MARKET_UNIVERSE_REFRESH_MS);
marketUniverseRefreshTimer.unref?.();

export async function handleContractLiquidation(req, res, url) {
  if (!['/api/contract-liquidation', LIQUIDATION_HISTORY_ROUTE, LIQUIDATION_HEALTH_ROUTE, LIQUIDATION_CURRENT_ROUTE, LIQUIDATION_MARKET_ROUTE].includes(url.pathname)) return false;
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

  if (url.pathname === LIQUIDATION_HEALTH_ROUTE) {
    sendJson(res, 200, {
      ...getContractLiquidationPersistenceHealth(),
      streams: FEEDS.size,
      stats_states: STATS.size,
      time: new Date().toISOString(),
    });
    return true;
  }

  if (url.pathname === LIQUIDATION_CURRENT_ROUTE) {
    const snapshot = getSharedCurrentLiquidationSnapshot(Date.now());
    sendJson(res, 200, {
      ok: true,
      version: STEP_VERSION,
      source: 'render_shared_bounded_five_provider_liquidation_current_snapshot',
      market_type: 'contract',
      period: '15m',
      ...snapshot,
      aggregation_scope: 'backend_shared_five_provider_three_core_targets',
      raw_events_persisted: false,
      exchange_connections_started: 0,
      exchange_requests_started: 0,
      shared_feeds_do_not_scale_with_users: true,
      timestamp_ms: Date.now(),
    });
    return true;
  }

  if (url.pathname === LIQUIDATION_MARKET_ROUTE) {
    const snapshot = getMarketLiquidationSnapshot(Date.now());
    sendJson(res, 200, {
      ok: true,
      version: STEP_VERSION,
      source: 'render_shared_official_maximum_five_provider_liquidation_market_snapshot',
      market_type: 'contract',
      period: '15m',
      ...snapshot,
      aggregation_scope: 'backend_shared_official_maximum_public_coverage',
      legacy_focused_fallback_endpoint: LIQUIDATION_CURRENT_ROUTE,
      legacy_focused_targets_per_provider: LIQUIDATION_SHARED_TARGETS_PER_PROVIDER,
      public_stream_semantics: {
        binance: 'largest_liquidation_per_symbol_within_1000ms',
        okx: 'recent_liquidation_orders_not_total_market_count',
        bybit: 'all_liquidation_stream_500ms',
        bitget: 'largest_long_and_short_liquidation_per_pair_per_second',
        gate: 'up_to_one_liquidation_order_per_contract_per_second',
      },
      raw_events_persisted: false,
      exchange_connections_started: 0,
      exchange_requests_started: 0,
      shared_feeds_do_not_scale_with_users: true,
      user_reads_start_exchange_connections: false,
      user_reads_start_exchange_requests: false,
      timestamp_ms: Date.now(),
    });
    return true;
  }

  if (url.pathname === LIQUIDATION_HISTORY_ROUTE) {
    if (!LIQUIDATION_PERSISTENCE_ENABLED) {
      sendJson(res, 503, { ok: false, version: STEP_VERSION, error: 'liquidation_history_persistence_disabled' });
      return true;
    }
    const providerFilter = normalizeProvider(url.searchParams.get('provider'));
    const symbolFilter = compactSymbol(url.searchParams.get('symbol'));
    if (providerFilter && !SUPPORTED_PROVIDERS.has(providerFilter)) {
      sendJson(res, 400, { ok: false, version: STEP_VERSION, error: 'unsupported_provider', provider: providerFilter });
      return true;
    }
    try {
      const requestedIntervalRaw = url.searchParams.get('interval');
      if (requestedIntervalRaw) {
        const spec = canonicalStep997HistoryInterval(requestedIntervalRaw);
        if (!spec) {
          sendJson(res, 400, { ok: false, version: STEP_VERSION, error: 'unsupported_liquidation_history_interval', supported_intervals: ['1m', '5m', '15m', '1H', '6H', '24H'] });
          return true;
        }
        const unified = await readStep997UnifiedHistory({
          interval: requestedIntervalRaw,
          hours: url.searchParams.get('hours'),
          provider: providerFilter,
          symbol: symbolFilter,
          limit: url.searchParams.get('limit'),
        });
        sendJson(res, 200, {
          ok: true,
          version: STEP_VERSION,
          source: 'render_step997_unified_five_provider_liquidation_history',
          market_type: 'contract',
          ...unified,
          storage_tables: { minute: LIQUIDATION_MINUTE_TABLE, hour: LIQUIDATION_HOUR_TABLE },
          aggregate_period: spec.canonical,
          available_intervals: ['1m', '5m', '15m', '1H', '6H', '24H'],
          raw_events_persisted: false,
          exchange_requests_started: 0,
          user_reads_start_exchange_requests: false,
          reads_scale_with_users: false,
          timestamp_ms: Date.now(),
        });
        return true;
      }
      const persisted = await readPersistedLiquidationHours({
        hours: url.searchParams.get('hours'),
        provider: providerFilter,
        symbol: symbolFilter,
        limit: url.searchParams.get('limit'),
      });
      sendJson(res, 200, {
        ok: true,
        version: STEP_VERSION,
        source: 'render_shared_persisted_five_provider_liquidation_hour_buckets',
        market_type: 'contract',
        ...persisted,
        storage_table: LIQUIDATION_HOUR_TABLE,
        aggregate_period: '1h',
        step997_default_backward_compatible: true,
        available_intervals: ['1m', '5m', '15m', '1H', '6H', '24H'],
        aggregate_retention_days: LIQUIDATION_HOUR_RETENTION_DAYS,
        raw_events_persisted: false,
        exchange_requests_started: 0,
        user_reads_start_exchange_requests: false,
        reads_scale_with_users: false,
        timestamp_ms: Date.now(),
      });
    } catch (error) {
      sendJson(res, 502, {
        ok: false,
        version: STEP_VERSION,
        error: String(error?.message || error),
        reason: 'persisted_liquidation_history_unavailable',
        exchange_requests_started: 0,
      });
    }
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
  if (!symbol || !supportsNativeContract(provider, symbol)) {
    sendJson(res, 400, { ok: false, version: STEP_VERSION, error: 'invalid_symbol_or_quote' });
    return true;
  }
  if (quoteFromCompact(symbol) === 'USDT' && !marketUniverseHasSymbol(provider, symbol)) {
    sendJson(res, 404, {
      ok: false,
      version: STEP_VERSION,
      error: 'symbol_not_in_current_shared_contract_universe',
      provider,
      market_type: 'contract',
      symbol,
    });
    return true;
  }

  const quote = quoteFromCompact(symbol);
  let feed = null;
  if (quote === 'USDT') {
    feed = getFeed(provider, symbol, { allowDynamic: false });
    if (!feed) {
      // The shared market collector may still be in its startup universe phase.
      // A user read must never create an extra USDT liquidation connection.
      sendJson(res, 503, {
        ok: false,
        version: STEP_VERSION,
        provider,
        market_type: 'contract',
        symbol,
        error: 'shared_market_liquidation_feed_not_ready',
        exchange_connections_started: 0,
        exchange_requests_started: 0,
        reads_scale_with_users: false,
      });
      return true;
    }
  } else {
    // Preserve legacy non-USDT exact-symbol support. Global-capable providers
    // still collapse to one shared venue/settle feed instead of one per user.
    feed = getFeed(provider, symbol, { allowDynamic: true });
  }
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
      contract_quote_support: {
        binance: ['USDT', 'USDC'],
        okx: ['USDT', 'USD'],
        bybit: ['USDT', 'USDC', 'USD'],
        bitget: ['USDT', 'USDC', 'USD'],
        gate: ['USDT', 'USD'],
      },
      okx_usdc_contract_retired: true,
      okx_current_contract_quotes: ['USDT', 'USD'],
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
        process_memory_only_for_raw_events: true,
        persisted_hour_bucket_table: LIQUIDATION_HOUR_TABLE,
        persisted_hour_bucket_retention_days: LIQUIDATION_HOUR_RETENTION_DAYS,
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
