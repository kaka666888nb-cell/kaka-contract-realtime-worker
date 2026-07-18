import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import {
  isBinanceValidationAdminAuthorized,
  isBinanceValidationAdminConfigured,
} from './binance-rest-guard.mjs';

const VERSION = '650.8.15';
const EDGE_PROTOCOL_VERSION = '650.8.11';
const SCHEMA_VERSION = '650.8.11';
const PROVIDER = 'binance';
const MARKET_TYPE = 'contract';
const SNAPSHOT_TABLE = 'app_market_backend_snapshots';
const SNAPSHOT_TYPE = 'klines';
const SNAPSHOT_KEY = 'BINANCE_KLINE_RELAY_GUARD';
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');
const RELAY_FUNCTION_NAME = 'kaka-binance-contract-kline-relay';
const RELAY_URL = SUPABASE_URL ? `${SUPABASE_URL}/functions/v1/${RELAY_FUNCTION_NAME}` : '';
const RELAY_TIMEOUT_MS = 20_000;
const SNAPSHOT_IO_TIMEOUT_MS = 8_000;
const MIN_REQUEST_GAP_MS = 12_000;
const KLINE_MIN_REQUEST_GAP_MS = 3_000;
const CRITICAL_AUX_MIN_REQUEST_GAP_MS = 2_500;
const GLOBAL_MIN_REQUEST_GAP_MS = 1_000;
const MAX_QUEUE_WAIT_MS = 25_000;
const MAX_PENDING = 6;
const RESTRICTED_FALLBACK_MS = 30 * 60_000;
const BAN_SAFETY_MS = 90_000;
const VALIDATION_TTL_MS = 30 * 60_000;
const VALIDATION_INTERVAL = '15m';
const VALIDATION_LIMIT = 240;
const PRIOR_VALIDATED_SYMBOLS = Object.freeze(['1000SHIBUSDT', 'ARCUSDT']);
const VALIDATION_SEQUENCE = Object.freeze(['BANANAS31USDT', 'BCHUSDT']);
const RESTRICTED_STATUSES = new Set([403, 418, 429, 451]);

let initialized = false;
let initializingPromise = null;
let activeRequest = false;
let lastRequestStartedAt = 0;
const lastRequestStartedAtByLane = { kline: 0, critical: 0, auxiliary: 0 };
const waiters = [];
let state = defaultState();

const stats = {
  restore_attempts: 0,
  restore_success: 0,
  restore_errors: 0,
  persistence_attempts: 0,
  persistence_success: 0,
  persistence_errors: 0,
  requests_started: 0,
  requests_succeeded: 0,
  requests_failed: 0,
  restricted_responses: 0,
  queue_waits: 0,
  queue_timeouts: 0,
  queue_rejections: 0,
  client_abort_blocks: 0,
  validation_starts: 0,
  validation_start_blocks: 0,
  validation_calls_authorized: 0,
  validation_calls_completed: 0,
  validation_calls_failed: 0,
  validation_sequence_blocks: 0,
  validation_token_blocks: 0,
  edge_health_checks: 0,
  edge_health_success: 0,
  edge_health_errors: 0,
  edge_health_last_at: 0,
  edge_health_last_error: '',
  edge_health_last_version: '',
  edge_health_last_upstream_called: null,
  last_error: '',
  last_success_at: 0,
  last_used_weight_1m: null,
};

function nowIso(value = Date.now()) {
  return new Date(value).toISOString();
}

function defaultState() {
  return {
    schema_version: SCHEMA_VERSION,
    edge_protocol_version: EDGE_PROTOCOL_VERSION,
    until: 0,
    status: 200,
    reason: 'step650_8_11_edge_relay_ready',
    source: 'supabase_edge_kline_relay',
    error: '',
    updated_at: Date.now(),
    last_success_at: 0,
    last_used_weight_1m: null,
    last_upstream_status: null,
    last_upstream_host: '',
    validation_session_hash: '',
    validation_created_at: 0,
    validation_expires_at: 0,
    validation_next_index: 0,
    validation_inflight: null,
    validation_completed: false,
  };
}

function relayConfigured() {
  return Boolean(RELAY_URL && SUPABASE_SERVICE_ROLE_KEY);
}

function hashToken(value) {
  return createHash('sha256').update(String(value || '')).digest('hex');
}

function safeTokenMatches(value, expectedHash) {
  const actual = Buffer.from(hashToken(value), 'hex');
  const expected = Buffer.from(String(expectedHash || '').padStart(64, '0').slice(-64), 'hex');
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

function activeState() {
  return Number(state?.until || 0) > Date.now();
}

function expectedValidationSymbol() {
  const index = Math.max(0, Math.min(VALIDATION_SEQUENCE.length, Number(state?.validation_next_index || 0)));
  return index < VALIDATION_SEQUENCE.length ? VALIDATION_SEQUENCE[index] : null;
}

function validationExpired() {
  const expires = Number(state?.validation_expires_at || 0);
  return Boolean(expires && expires <= Date.now());
}

function normalizeRestored(raw) {
  if (!raw || typeof raw !== 'object' || raw.schema_version !== SCHEMA_VERSION) return defaultState();
  const nextIndex = Math.max(0, Math.min(VALIDATION_SEQUENCE.length, Number(raw.validation_next_index || 0)));
  const restored = {
    ...defaultState(),
    ...raw,
    schema_version: SCHEMA_VERSION,
    validation_next_index: nextIndex,
    validation_inflight: null,
  };
  if (Number(restored.until || 0) <= Date.now()) restored.until = 0;
  if (Number(restored.validation_expires_at || 0) <= Date.now()) {
    restored.validation_session_hash = '';
    restored.validation_created_at = 0;
    restored.validation_expires_at = 0;
  }
  restored.validation_completed = nextIndex >= VALIDATION_SEQUENCE.length;
  return restored;
}

async function fetchWithTimeout(url, options, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function restoreState() {
  stats.restore_attempts += 1;
  if (!relayConfigured()) {
    stats.restore_errors += 1;
    stats.last_error = 'relay_not_configured';
    throw new Error('binance_kline_relay_not_configured');
  }
  const key = encodeURIComponent(SNAPSHOT_KEY);
  const url = `${SUPABASE_URL}/rest/v1/${SNAPSHOT_TABLE}` +
    `?select=payload&provider=eq.${PROVIDER}&market_type=eq.${MARKET_TYPE}` +
    `&snapshot_type=eq.${SNAPSHOT_TYPE}&quote_asset=eq.${key}&limit=1`;
  try {
    const response = await fetchWithTimeout(url, {
      headers: {
        apikey: SUPABASE_SERVICE_ROLE_KEY,
        authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        accept: 'application/json',
      },
    }, SNAPSHOT_IO_TIMEOUT_MS);
    if (!response.ok) throw new Error(`relay_guard_restore_${response.status}`);
    const payload = await response.json();
    state = normalizeRestored(Array.isArray(payload) ? payload[0]?.payload : null);
    stats.restore_success += 1;
  } catch (error) {
    stats.restore_errors += 1;
    stats.last_error = String(error?.message || error);
    throw error;
  }
}

async function persistStateStrict() {
  stats.persistence_attempts += 1;
  if (!relayConfigured()) {
    stats.persistence_errors += 1;
    throw new Error('binance_kline_relay_persistence_not_configured');
  }
  state = { ...state, schema_version: SCHEMA_VERSION, updated_at: Date.now() };
  const body = [{
    provider: PROVIDER,
    market_type: MARKET_TYPE,
    snapshot_type: SNAPSHOT_TYPE,
    quote_asset: SNAPSHOT_KEY,
    payload: state,
    row_count: 1,
    source: 'step650_8_11_binance_kline_edge_relay_guard',
    source_time: nowIso(),
    updated_at: nowIso(),
  }];
  try {
    const response = await fetchWithTimeout(
      `${SUPABASE_URL}/rest/v1/${SNAPSHOT_TABLE}?on_conflict=provider,market_type,snapshot_type,quote_asset`,
      {
        method: 'POST',
        headers: {
          apikey: SUPABASE_SERVICE_ROLE_KEY,
          authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          'content-type': 'application/json',
          prefer: 'resolution=merge-duplicates,return=minimal',
        },
        body: JSON.stringify(body),
      },
      SNAPSHOT_IO_TIMEOUT_MS,
    );
    if (!response.ok) throw new Error(`relay_guard_persist_${response.status}`);
    stats.persistence_success += 1;
  } catch (error) {
    stats.persistence_errors += 1;
    stats.last_error = String(error?.message || error);
    throw error;
  }
}

export async function ensureBinanceContractKlineRelayInitialized() {
  if (initialized) return;
  if (initializingPromise) return await initializingPromise;
  initializingPromise = (async () => {
    await restoreState();
    initialized = true;
  })().finally(() => { initializingPromise = null; });
  return await initializingPromise;
}

export async function checkBinanceContractKlineRelayDeployment() {
  await ensureBinanceContractKlineRelayInitialized();
  stats.edge_health_checks += 1;
  stats.edge_health_last_at = Date.now();
  try {
    const response = await fetchWithTimeout(RELAY_URL, {
      method: 'GET',
      headers: {
        apikey: SUPABASE_SERVICE_ROLE_KEY,
        authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        accept: 'application/json',
        'x-kaka-relay-client': VERSION,
      },
    }, SNAPSHOT_IO_TIMEOUT_MS);
    const text = await response.text();
    let payload = null;
    try { payload = text ? JSON.parse(text) : null; } catch (_) {}
    const healthy = response.ok && payload?.ok === true &&
      payload?.relay_ready === true && payload?.upstream_called === false &&
      String(payload?.version || '') === EDGE_PROTOCOL_VERSION;
    if (!healthy) {
      throw new Error(`edge_relay_health_${response.status}:${String(payload?.error || text || 'invalid_payload').slice(0, 180)}`);
    }
    stats.edge_health_success += 1;
    stats.edge_health_last_error = '';
    stats.edge_health_last_version = String(payload.version || '');
    stats.edge_health_last_upstream_called = payload.upstream_called === true;
    return {
      ok: true,
      reachable: true,
      version: stats.edge_health_last_version,
      upstream_called: false,
      checked_at: nowIso(stats.edge_health_last_at),
    };
  } catch (error) {
    stats.edge_health_errors += 1;
    stats.edge_health_last_error = String(error?.message || error);
    stats.edge_health_last_version = '';
    stats.edge_health_last_upstream_called = null;
    throw error;
  }
}

function parseBanUntil(payload, text = '') {
  const values = [
    payload?.ban_until,
    payload?.ban_until_ms,
    payload?.parsed_ban_until,
    payload?.upstream_ban_until,
  ];
  for (const value of values) {
    const parsed = Number(value);
    if (Number.isFinite(parsed) && parsed > Date.now()) return parsed;
  }
  const match = String(text || '').match(/banned\s+until\s+(\d{10,16})/i);
  if (match) {
    let parsed = Number(match[1]);
    if (parsed < 10_000_000_000) parsed *= 1000;
    if (Number.isFinite(parsed) && parsed > Date.now()) return parsed;
  }
  return 0;
}

async function markRestricted({ status, reason, source, error, banUntil = 0 } = {}) {
  const until = Math.max(Date.now() + RESTRICTED_FALLBACK_MS, Number(banUntil || 0) + BAN_SAFETY_MS);
  state = {
    ...state,
    until,
    status: Number(status || 429),
    reason: String(reason || 'edge_relay_upstream_restricted'),
    source: String(source || 'supabase_edge_kline_relay'),
    error: String(error || ''),
    validation_session_hash: '',
    validation_created_at: 0,
    validation_expires_at: 0,
    validation_inflight: null,
  };
  stats.restricted_responses += 1;
  stats.last_error = state.error || state.reason;
  await persistStateStrict();
}

function laneGapMs(lane) {
  if (lane === 'kline') return KLINE_MIN_REQUEST_GAP_MS;
  if (lane === 'critical') return CRITICAL_AUX_MIN_REQUEST_GAP_MS;
  return MIN_REQUEST_GAP_MS;
}

function nextWaiterIndex() {
  let bestIndex = -1;
  let bestPriority = -Infinity;
  for (let index = 0; index < waiters.length; index += 1) {
    const waiter = waiters[index];
    if (waiter.done) continue;
    const priority = Number(waiter.priority || 0);
    if (bestIndex < 0 || priority > bestPriority) {
      bestIndex = index;
      bestPriority = priority;
    }
  }
  return bestIndex;
}

function releaseNext() {
  if (activeRequest) return;
  while (waiters.length) {
    const index = nextWaiterIndex();
    if (index < 0) {
      waiters.splice(0, waiters.length);
      return;
    }
    const [waiter] = waiters.splice(index, 1);
    if (waiter.done) continue;
    const now = Date.now();
    const lane = waiter.lane === 'kline' ? 'kline' : waiter.lane === 'critical' ? 'critical' : 'auxiliary';
    const laneDelay = laneGapMs(lane) - (now - Number(lastRequestStartedAtByLane[lane] || 0));
    const globalDelay = GLOBAL_MIN_REQUEST_GAP_MS - (now - lastRequestStartedAt);
    const delay = Math.max(0, laneDelay, globalDelay);
    const grant = () => {
      if (waiter.done || activeRequest) return;
      waiter.done = true;
      clearTimeout(waiter.timeout);
      waiter.signal?.removeEventListener('abort', waiter.abort);
      activeRequest = true;
      lastRequestStartedAt = Date.now();
      lastRequestStartedAtByLane[lane] = lastRequestStartedAt;
      stats.requests_started += 1;
      waiter.resolve(() => {
        if (!activeRequest) return;
        activeRequest = false;
        releaseNext();
      });
    };
    if (delay > 0) setTimeout(grant, delay).unref?.(); else grant();
    return;
  }
}

async function acquireSlot(signal = null, { lane = 'auxiliary', priority = 0 } = {}) {
  await ensureBinanceContractKlineRelayInitialized();
  if (activeState()) {
    const error = new Error('binance_kline_edge_relay_cooling_down');
    error.internalBinanceRelayGuard = true;
    error.guardState = state;
    throw error;
  }
  if (signal?.aborted) {
    stats.client_abort_blocks += 1;
    throw new Error('binance_kline_relay_client_aborted_before_queue');
  }
  const safeLane = lane === 'kline' ? 'kline' : lane === 'critical' ? 'critical' : 'auxiliary';
  const now = Date.now();
  const laneReady = now - Number(lastRequestStartedAtByLane[safeLane] || 0) >= laneGapMs(safeLane);
  const globalReady = now - lastRequestStartedAt >= GLOBAL_MIN_REQUEST_GAP_MS;
  if (!activeRequest && waiters.length === 0 && laneReady && globalReady) {
    activeRequest = true;
    lastRequestStartedAt = Date.now();
    lastRequestStartedAtByLane[safeLane] = lastRequestStartedAt;
    stats.requests_started += 1;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      activeRequest = false;
      releaseNext();
    };
  }
  if (waiters.length >= MAX_PENDING) {
    stats.queue_rejections += 1;
    throw new Error('binance_kline_relay_queue_full');
  }
  stats.queue_waits += 1;
  return await new Promise((resolve, reject) => {
    const waiter = { resolve, reject, done: false, signal, timeout: null, abort: null, lane: safeLane, priority: Number(priority || 0) };
    waiter.abort = () => {
      if (waiter.done) return;
      waiter.done = true;
      stats.client_abort_blocks += 1;
      clearTimeout(waiter.timeout);
      reject(new Error('binance_kline_relay_client_aborted_in_queue'));
    };
    waiter.timeout = setTimeout(() => {
      if (waiter.done) return;
      waiter.done = true;
      stats.queue_timeouts += 1;
      signal?.removeEventListener('abort', waiter.abort);
      reject(new Error('binance_kline_relay_queue_timeout'));
    }, MAX_QUEUE_WAIT_MS);
    waiter.timeout.unref?.();
    signal?.addEventListener('abort', waiter.abort, { once: true });
    waiters.push(waiter);
    releaseNext();
  });
}

function normalizeRelayRows(rawRows, symbol, interval) {
  const rows = [];
  for (const raw of Array.isArray(rawRows) ? rawRows : []) {
    if (!raw || typeof raw !== 'object') continue;
    const openTimeMs = Number(raw.open_time_ms ?? raw.openTime ?? Date.parse(raw.open_time || ''));
    const open = Number(raw.open ?? raw.open_price);
    const high = Number(raw.high ?? raw.high_price);
    const low = Number(raw.low ?? raw.low_price);
    const close = Number(raw.close ?? raw.close_price);
    if (![openTimeMs, open, high, low, close].every(Number.isFinite)) continue;
    rows.push({
      provider: PROVIDER,
      market_type: MARKET_TYPE,
      symbol,
      interval,
      open_time: nowIso(openTimeMs),
      open_time_ms: openTimeMs,
      close_time: raw.close_time || raw.closeTime || nowIso(Number(raw.close_time_ms || openTimeMs)),
      open,
      high,
      low,
      close,
      volume: Number(raw.volume || 0),
      quote_volume: Number(raw.quote_volume ?? raw.quoteVolume ?? 0),
      trade_count: Math.max(0, Math.trunc(Number(raw.trade_count ?? raw.trades ?? 0) || 0)),
      taker_buy_volume: Number.isFinite(Number(raw.taker_buy_volume)) ? Number(raw.taker_buy_volume) : null,
      taker_buy_quote_volume: Number.isFinite(Number(raw.taker_buy_quote_volume)) ? Number(raw.taker_buy_quote_volume) : null,
      source: raw.source || 'binance_official_public_kline_supabase_edge_relay_fapi_klines',
      cached_at: raw.cached_at || nowIso(),
    });
  }
  return [...new Map(rows.map((row) => [row.open_time_ms, row])).values()].sort((a, b) => a.open_time_ms - b.open_time_ms);
}

async function invokeAuthenticatedEdgeRelay(body, { signal = null, sourceLabel = 'public_rest', lane = 'auxiliary', priority = 0 } = {}) {
  const release = await acquireSlot(signal, { lane, priority });
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), RELAY_TIMEOUT_MS);
  try {
    const response = await fetch(RELAY_URL, {
      method: 'POST',
      signal: controller.signal,
      headers: {
        apikey: SUPABASE_SERVICE_ROLE_KEY,
        authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        'content-type': 'application/json',
        accept: 'application/json',
        'x-kaka-relay-client': VERSION,
      },
      body: JSON.stringify(body),
    });
    const bodyText = await response.text();
    let payload = null;
    try { payload = bodyText ? JSON.parse(bodyText) : null; } catch (_) {}
    const upstreamStatus = Number(payload?.upstream_status ?? response.status);
    const usedWeight = Number(payload?.used_weight_1m);
    if (Number.isFinite(usedWeight)) {
      stats.last_used_weight_1m = usedWeight;
      state.last_used_weight_1m = usedWeight;
    }
    state.last_upstream_status = Number.isFinite(upstreamStatus) ? upstreamStatus : null;
    state.last_upstream_host = String(payload?.upstream_host || 'binance_public_rest');
    if (!response.ok || payload?.ok === false) {
      const message = String(payload?.error || payload?.upstream_error || bodyText || `${response.status} ${response.statusText}`);
      if (RESTRICTED_STATUSES.has(upstreamStatus) || payload?.restricted === true) {
        await markRestricted({
          status: upstreamStatus,
          reason: 'edge_relay_upstream_restricted',
          source: `supabase_edge:${sourceLabel}`,
          error: message,
          banUntil: parseBanUntil(payload, message),
        });
      }
      stats.requests_failed += 1;
      stats.last_error = message;
      const error = new Error(`binance_public_edge_relay_failed:${message}`);
      error.status = response.status;
      error.upstreamStatus = upstreamStatus;
      error.internalBinanceRelayGuard = RESTRICTED_STATUSES.has(upstreamStatus) || payload?.restricted === true;
      error.guardState = state;
      throw error;
    }
    stats.requests_succeeded += 1;
    stats.last_success_at = Date.now();
    stats.last_error = '';
    state = {
      ...state,
      until: 0,
      status: 200,
      reason: 'edge_relay_success',
      source: `supabase_edge:${sourceLabel}`,
      error: '',
      last_success_at: Date.now(),
    };
    // Serialize telemetry before a validation-completion write can advance the
    // durable sequence. A late older write must never roll validation_next_index back.
    try {
      await persistStateStrict();
    } catch (error) {
      stats.last_error = String(error?.message || error);
    }
    return payload;
  } catch (error) {
    if (error?.name === 'AbortError') {
      stats.requests_failed += 1;
      stats.last_error = 'binance_public_edge_relay_timeout';
    }
    throw error;
  } finally {
    clearTimeout(timer);
    release();
  }
}

export async function fetchBinanceContractKlineRelayRows({
  symbol, interval, startTime, endTime, limit, signal = null, validationAuthorized = false,
} = {}) {
  await ensureBinanceContractKlineRelayInitialized();
  if (state.validation_completed !== true && validationAuthorized !== true) {
    const error = new Error('binance_kline_edge_relay_validation_not_completed');
    error.internalBinanceRelayGuard = true;
    error.guardState = state;
    throw error;
  }
  const payload = await invokeAuthenticatedEdgeRelay({
    kind: 'kline',
    symbol,
    interval,
    start_time: Math.max(0, Math.trunc(Number(startTime) || 0)),
    end_time: Math.max(1, Math.trunc(Number(endTime) || Date.now())),
    limit: Math.max(1, Math.min(1000, Math.trunc(Number(limit) || 240))),
  }, {
    signal,
    sourceLabel: `kline:${String(symbol || '').toUpperCase()}:${String(interval || '15m')}`,
    lane: 'kline',
    priority: 100,
  });
  return normalizeRelayRows(payload?.rows, String(symbol || '').toUpperCase(), String(interval || '15m'));
}

export async function fetchBinancePublicRestRelayJson(url, {
  source = 'public_rest', signal = null, allowBeforeValidation = false, lane = 'auxiliary', priority = 0,
} = {}) {
  await ensureBinanceContractKlineRelayInitialized();
  if (state.validation_completed !== true && allowBeforeValidation !== true) {
    const error = new Error('binance_public_edge_relay_validation_not_completed');
    error.internalBinanceRelayGuard = true;
    error.guardState = state;
    throw error;
  }
  const parsed = new URL(String(url || ''));
  const payload = await invokeAuthenticatedEdgeRelay({
    kind: 'public_rest',
    upstream_url: parsed.toString(),
  }, { signal, sourceLabel: String(source || 'public_rest'), lane, priority });
  return payload?.data;
}

export async function startBinanceContractKlineRelayValidation(adminKey) {
  await ensureBinanceContractKlineRelayInitialized();
  if (!isBinanceValidationAdminConfigured()) throw new Error('validation_admin_key_not_configured');
  if (!isBinanceValidationAdminAuthorized(adminKey)) throw new Error('validation_admin_key_invalid');
  await checkBinanceContractKlineRelayDeployment();
  if (activeState()) {
    stats.validation_start_blocks += 1;
    const error = new Error('binance_kline_edge_relay_cooling_down');
    error.internalBinanceRelayGuard = true;
    error.guardState = state;
    throw error;
  }
  if (activeRequest || state.validation_inflight) {
    stats.validation_start_blocks += 1;
    throw new Error('binance_kline_relay_not_idle');
  }
  if (validationExpired()) {
    state.validation_session_hash = '';
    state.validation_created_at = 0;
    state.validation_expires_at = 0;
  }
  const token = randomBytes(32).toString('hex');
  const now = Date.now();
  state = {
    ...state,
    status: 200,
    reason: state.validation_completed ? 'relay_validation_already_completed' : 'relay_validation_started',
    source: 'relay_validation_start',
    error: '',
    validation_session_hash: hashToken(token),
    validation_created_at: now,
    validation_expires_at: now + VALIDATION_TTL_MS,
    validation_inflight: null,
  };
  await persistStateStrict();
  stats.validation_starts += 1;
  return {
    validation_token: token,
    next_symbol: expectedValidationSymbol(),
    remaining: Math.max(0, VALIDATION_SEQUENCE.length - Number(state.validation_next_index || 0)),
    completed: state.validation_completed === true,
    expires_at: nowIso(state.validation_expires_at),
  };
}

export async function runWithBinanceContractKlineRelayValidation(token, fn, {
  provider = '', market = '', symbol = '', interval = '', limit = 0, endTimeProvided = false,
} = {}) {
  await ensureBinanceContractKlineRelayInitialized();
  if (activeState()) {
    const error = new Error('binance_kline_edge_relay_cooling_down');
    error.internalBinanceRelayGuard = true;
    error.guardState = state;
    throw error;
  }
  if (validationExpired()) {
    state.validation_session_hash = '';
    state.validation_created_at = 0;
    state.validation_expires_at = 0;
    await persistStateStrict();
    throw new Error('binance_kline_relay_validation_expired');
  }
  if (!safeTokenMatches(token, state.validation_session_hash)) {
    stats.validation_token_blocks += 1;
    throw new Error('binance_kline_relay_validation_token_invalid');
  }
  const normalizedSymbol = String(symbol || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  const expected = expectedValidationSymbol();
  const matches = String(provider).toLowerCase() === PROVIDER &&
    String(market).toLowerCase() === MARKET_TYPE &&
    normalizedSymbol === expected &&
    String(interval) === VALIDATION_INTERVAL &&
    Number(limit) === VALIDATION_LIMIT &&
    endTimeProvided !== true;
  if (!matches) {
    stats.validation_sequence_blocks += 1;
    throw new Error(`binance_kline_relay_validation_sequence_mismatch:expected=${expected};requested=${normalizedSymbol}`);
  }
  if (state.validation_inflight) throw new Error('binance_kline_relay_validation_inflight');
  state = {
    ...state,
    validation_inflight: { symbol: normalizedSymbol, interval: VALIDATION_INTERVAL, started_at: Date.now() },
  };
  await persistStateStrict();
  stats.validation_calls_authorized += 1;
  try {
    return await fn();
  } catch (error) {
    state = { ...state, validation_inflight: null };
    try { await persistStateStrict(); } catch (_) {}
    throw error;
  }
}

export async function completeBinanceContractKlineRelayValidation({ token, symbol, interval } = {}) {
  await ensureBinanceContractKlineRelayInitialized();
  if (!safeTokenMatches(token, state.validation_session_hash)) throw new Error('binance_kline_relay_validation_token_invalid');
  const expected = expectedValidationSymbol();
  const normalizedSymbol = String(symbol || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  if (normalizedSymbol !== expected || String(interval) !== VALIDATION_INTERVAL) throw new Error('binance_kline_relay_validation_complete_mismatch');
  const nextIndex = Math.min(VALIDATION_SEQUENCE.length, Number(state.validation_next_index || 0) + 1);
  const completed = nextIndex >= VALIDATION_SEQUENCE.length;
  state = {
    ...state,
    validation_next_index: nextIndex,
    validation_inflight: null,
    validation_completed: completed,
    validation_session_hash: completed ? '' : state.validation_session_hash,
    validation_created_at: completed ? 0 : state.validation_created_at,
    validation_expires_at: completed ? 0 : state.validation_expires_at,
    reason: completed ? 'relay_validation_completed' : 'relay_validation_symbol_completed',
    source: `relay_validation:${normalizedSymbol}`,
    error: '',
  };
  await persistStateStrict();
  stats.validation_calls_completed += 1;
  return getBinanceContractKlineRelayHealth();
}

export async function failBinanceContractKlineRelayValidation({ token, symbol, interval, reason = '' } = {}) {
  await ensureBinanceContractKlineRelayInitialized();
  if (safeTokenMatches(token, state.validation_session_hash)) {
    state = {
      ...state,
      validation_inflight: null,
      reason: 'relay_validation_coverage_failed',
      source: `relay_validation:${String(symbol || '')}:${String(interval || '')}`,
      error: String(reason || ''),
    };
    await persistStateStrict();
  }
  stats.validation_calls_failed += 1;
}

export async function resetBinanceContractKlineRelayValidation(adminKey) {
  await ensureBinanceContractKlineRelayInitialized();
  if (!isBinanceValidationAdminConfigured()) throw new Error('validation_admin_key_not_configured');
  if (!isBinanceValidationAdminAuthorized(adminKey)) throw new Error('validation_admin_key_invalid');
  // Never clear the validation token while an upstream relay request still owns the
  // slot. Recovery is only for a stranded persisted inflight marker after the real
  // network call has ended.
  if (activeRequest) throw new Error('binance_kline_relay_request_active');
  state = {
    ...state,
    validation_session_hash: '',
    validation_created_at: 0,
    validation_expires_at: 0,
    validation_inflight: null,
    reason: 'relay_validation_session_reset',
    source: 'admin_relay_validation_reset',
    error: '',
  };
  await persistStateStrict();
  return getBinanceContractKlineRelayHealth();
}

export function getBinanceContractKlineRelayHealth() {
  const now = Date.now();
  return {
    ok: initialized && relayConfigured() && stats.restore_errors === 0,
    version: VERSION,
    schema_version: SCHEMA_VERSION,
    relay_function: RELAY_FUNCTION_NAME,
    relay_configured: relayConfigured(),
    public_rest_relay_enabled: true,
    normal_public_rest_requires_validation_complete: true,
    relayed_upstream_hosts: ['fapi.binance.com', 'data-api.binance.vision'],
    edge_health_reachable: stats.edge_health_success > 0 && !stats.edge_health_last_error,
    edge_health_last_at: stats.edge_health_last_at ? nowIso(stats.edge_health_last_at) : null,
    edge_health_last_error: stats.edge_health_last_error || null,
    edge_health_last_version: stats.edge_health_last_version || null,
    edge_health_last_upstream_called: stats.edge_health_last_upstream_called,
    direct_binance_rest_used_by_kline: false,
    edge_relay_only: true,
    active: activeState(),
    next_allowed_at: activeState() ? nowIso(state.until) : null,
    status: state.status,
    reason: state.reason,
    source: state.source,
    last_error: state.error || stats.last_error || null,
    last_success_at: state.last_success_at ? nowIso(state.last_success_at) : null,
    last_used_weight_1m: state.last_used_weight_1m,
    last_upstream_status: state.last_upstream_status,
    last_upstream_host: state.last_upstream_host || null,
    initialized,
    restore_healthy: initialized && stats.restore_errors === 0,
    persistence_healthy: stats.persistence_errors === 0,
    queue_depth: waiters.filter((waiter) => !waiter.done).length,
    active_request: activeRequest,
    min_request_gap_ms: MIN_REQUEST_GAP_MS,
    kline_min_request_gap_ms: KLINE_MIN_REQUEST_GAP_MS,
    critical_aux_min_request_gap_ms: CRITICAL_AUX_MIN_REQUEST_GAP_MS,
    global_min_request_gap_ms: GLOBAL_MIN_REQUEST_GAP_MS,
    kline_priority_enabled: true,
    max_pending: MAX_PENDING,
    prior_validated_symbols: PRIOR_VALIDATED_SYMBOLS,
    validation_sequence: VALIDATION_SEQUENCE,
    validation_interval: VALIDATION_INTERVAL,
    validation_limit: VALIDATION_LIMIT,
    validation_next_index: Number(state.validation_next_index || 0),
    validation_next_symbol: expectedValidationSymbol(),
    validation_remaining: Math.max(0, VALIDATION_SEQUENCE.length - Number(state.validation_next_index || 0)),
    validation_completed: state.validation_completed === true,
    validation_session_active: Boolean(state.validation_session_hash) && !validationExpired(),
    validation_expires_at: state.validation_expires_at ? nowIso(state.validation_expires_at) : null,
    validation_inflight: state.validation_inflight,
    ...stats,
    last_success_at_counter: stats.last_success_at ? nowIso(stats.last_success_at) : null,
    time: nowIso(now),
  };
}
