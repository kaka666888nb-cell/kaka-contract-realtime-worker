import { AsyncLocalStorage } from 'node:async_hooks';
import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import { lookup as dnsLookup } from 'node:dns/promises';

const PROVIDER = 'binance';
const MARKET_TYPE = 'contract';
const SNAPSHOT_TABLE = 'app_market_backend_snapshots';
const SNAPSHOT_TYPE = 'klines';
const SNAPSHOT_KEY = 'REST_GUARD:BINANCE_CONTRACT';
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');
const PROCESS_REST_DISABLED = process.env.KAKA_DISABLE_BINANCE_REST === '1';
const VALIDATION_ADMIN_KEY = String(process.env.KAKA_BINANCE_VALIDATION_KEY || '').trim();
const SUPABASE_IO_TIMEOUT_MS = 8_000;
const RENDER_RUNTIME = process.env.RENDER === 'true';
const RENDER_INSTANCE_ID = String(process.env.RENDER_INSTANCE_ID || '');
const RENDER_DISCOVERY_SERVICE = String(process.env.RENDER_DISCOVERY_SERVICE || '');
const INSTANCE_SAFETY_REFRESH_MS = 30_000;
const INSTANCE_DISCOVERY_TIMEOUT_MS = 3_000;
const INSTANCE_STARTUP_REST_GRACE_MS = process.env.NODE_ENV === 'test'
  ? Math.max(0, Number(process.env.KAKA_BINANCE_TEST_INSTANCE_GRACE_MS || 0))
  : 90_000;
const PROCESS_STARTED_AT = Date.now();

// The last observed Binance USD-M Futures IP ban ended at this exact UTC time.
// Step650.8.8 keeps the observed-ban migration record and requires
// one explicit low-weight /fapi/v1/ping probe before any normal Binance contract
// REST request can leave this process. This prevents an App page, background metric
// refresh, or another user from becoming the first post-ban caller.
const OBSERVED_BAN_UNTIL_MS = 1_784_319_886_570;
const INITIAL_QUARANTINE_UNTIL_MS = OBSERVED_BAN_UNTIL_MS + 15 * 60_000;
const BAN_SAFETY_MS = 90_000;
const RESTRICTED_FALLBACK_MS = 30 * 60_000;

// Conservative production pacing. Binance's published limits are much higher, but
// this worker has one shared cloud egress IP and recently accumulated repeat bans.
const TEST_GAP_OVERRIDE_ENABLED = process.env.NODE_ENV === 'test' && process.env.KAKA_BINANCE_GUARD_TEST_MODE === '1';
const MIN_REQUEST_GAP_MS = TEST_GAP_OVERRIDE_ENABLED
  ? Math.max(1, Math.min(100, Number(process.env.KAKA_BINANCE_GUARD_TEST_GAP_MS || 1)))
  : 10_000;
const MAX_PENDING_REQUESTS = 6;
const DEFAULT_MAX_QUEUE_WAIT_MS = 25_000;
const PROBE_TIMEOUT_MS = 6_000;
const PROBE_URL = 'https://fapi.binance.com/fapi/v1/ping';
// Fail closed if the first post-ban response does not expose a low shared-IP
// request weight. This is deliberately far below Binance's normal capacity; the
// purpose of the probe is safety verification, not throughput.
const PROBE_MAX_USED_WEIGHT_1M = 100;
const PROBE_UNSAFE_COOLDOWN_MS = 10 * 60_000;
const NORMAL_UNSAFE_COOLDOWN_MS = 10 * 60_000;
const VALIDATION_MAX_USED_WEIGHT_1M = 150;
const NORMAL_MAX_USED_WEIGHT_1M = 600;
const MIGRATION_STATE_UPDATED_AT_MS = OBSERVED_BAN_UNTIL_MS;
const VALIDATION_ALLOWED_SOURCE_PREFIXES = ['kline_bridge:'];
const VALIDATION_SEQUENCE = ['1000SHIBUSDT', 'ARCUSDT', 'BANANAS31USDT', 'BCHUSDT'];
const VALIDATION_INTERVAL = '15m';
const VALIDATION_LIMIT = 240;
const VALIDATION_REST_BUDGET = VALIDATION_SEQUENCE.length;
const VALIDATION_SESSION_TTL_MS = 2 * 60 * 60_000;
const VALIDATION_RECOVERY_COOLDOWN_MS = 10 * 60_000;
const GUARD_SCHEMA_VERSION = '650.8.8';
const VALIDATION_ADMIN_KEY_VALID = /^[a-f0-9]{64}$/i.test(VALIDATION_ADMIN_KEY);
const VALIDATION_ADMIN_KEY_FINGERPRINT = VALIDATION_ADMIN_KEY_VALID
  ? createHash('sha256').update(`kaka-binance-admin-v1:${VALIDATION_ADMIN_KEY}`, 'utf8').digest('hex')
  : '';
const validationContext = new AsyncLocalStorage();
const requestSignalContext = new AsyncLocalStorage();

let initialized = false;
let initPromise = null;
let requestChain = Promise.resolve();
let lastRequestStartedAt = 0;
let pendingRequests = 0;
let activeRequest = false;
let persistPromise = Promise.resolve();
let probePromise = null;
let activeProbeController = null;
let lastPersistenceError = null;
let lastRestoreError = null;
let instanceSafety = {
  checked_at: 0,
  healthy: !RENDER_RUNTIME,
  instance_count: RENDER_RUNTIME ? 0 : 1,
  error: '',
};
let processRestShuttingDown = false;

let state = {
  until: INITIAL_QUARANTINE_UNTIL_MS,
  status: 418,
  reason: 'observed_binance_ip_ban_migration_quarantine',
  source: 'step650.8.8_migration_guard',
  error: '',
  parsed_ban_until: OBSERVED_BAN_UNTIL_MS,
  retry_after_seconds: null,
  updated_at: MIGRATION_STATE_UPDATED_AT_MS,
  probe_required: true,
  probe_passed_at: 0,
  probe_source: '',
  operating_mode: 'probe_required',
  validation_session_hash: '',
  validation_budget: 0,
  validation_created_at: 0,
  validation_expires_at: 0,
  validation_next_index: 0,
  validation_inflight: null,
  validation_admin_key_fingerprint: VALIDATION_ADMIN_KEY_FINGERPRINT,
  validation_control_epoch: 0,
  schema_version: GUARD_SCHEMA_VERSION,
};

const stats = {
  initialized_at: 0,
  restore_attempts: 0,
  restore_success: 0,
  restore_errors: 0,
  instance_safety_checks: 0,
  instance_safety_blocks: 0,
  instance_safety_errors: 0,
  restored_from_snapshot: 0,
  snapshot_persist_success: 0,
  snapshot_persist_errors: 0,
  requests_started: 0,
  request_slot_waits: 0,
  blocked_requests: 0,
  probe_required_blocks: 0,
  queue_rejections: 0,
  queue_timeouts: 0,
  queue_release_on_guard_error: 0,
  max_queue_depth: 0,
  restricted_responses: 0,
  ban_until_parsed: 0,
  retry_after_parsed: 0,
  successes: 0,
  probes_started: 0,
  probes_succeeded: 0,
  probes_restricted: 0,
  probes_failed: 0,
  validation_session_blocks: 0,
  validation_budget_exhausted: 0,
  validation_calls_authorized: 0,
  validation_sequence_blocks: 0,
  validation_admin_auth_failures: 0,
  probe_weight_missing: 0,
  probe_weight_unsafe: 0,
  probe_state_persist_failures: 0,
  validation_state_persist_failures: 0,
  validation_calls_completed: 0,
  validation_calls_failed: 0,
  validation_sessions_completed: 0,
  validation_inflight_recovered: 0,
  validation_session_expired: 0,
  validation_admin_resets: 0,
  validation_control_epoch_advances: 0,
  probe_results_superseded: 0,
  duplicate_probe_blocks: 0,
  probe_reset_wait_timeouts: 0,
  client_abort_blocks: 0,
  validation_reservations_cancelled_before_network: 0,
  probe_uncertain_failures: 0,
  admin_key_rotation_resets: 0,
  weight_header_missing: 0,
  weight_limit_unsafe: 0,
  normal_guarded_entries: 0,
  last_success_at: 0,
  last_success_source: '',
  last_used_weight_1m: null,
  last_error: '',
};

function iso(value) {
  return value ? new Date(value).toISOString() : null;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));
}

async function refreshInstanceSafety({ force = false } = {}) {
  const now = Date.now();
  if (!RENDER_RUNTIME) {
    instanceSafety = { checked_at: now, healthy: true, instance_count: 1, error: '' };
    return instanceSafety;
  }
  if (!force && now - Number(instanceSafety.checked_at || 0) < INSTANCE_SAFETY_REFRESH_MS) return instanceSafety;
  stats.instance_safety_checks += 1;
  if (!RENDER_DISCOVERY_SERVICE) {
    instanceSafety = { checked_at: now, healthy: false, instance_count: 0, error: 'render_discovery_service_missing' };
    stats.instance_safety_errors += 1;
    return instanceSafety;
  }
  try {
    // Render explicitly recommends the OS-backed lookup API for private discovery
    // so its internal resolver configuration is honored. Bound the lookup so a
    // DNS half-open state fails closed instead of hanging the Binance REST queue.
    const addresses = await Promise.race([
      dnsLookup(RENDER_DISCOVERY_SERVICE, { all: true, family: 4 }),
      new Promise((_, reject) => {
        const timer = setTimeout(() => reject(new Error('render_discovery_timeout')), INSTANCE_DISCOVERY_TIMEOUT_MS);
        timer.unref?.();
      }),
    ]);
    const unique = new Set((addresses || []).map((item) => String(item?.address || '')).filter(Boolean));
    const count = unique.size;
    instanceSafety = {
      checked_at: now,
      healthy: count === 1,
      instance_count: count,
      error: count === 1 ? '' : `render_instance_count_${count}`,
    };
  } catch (error) {
    instanceSafety = { checked_at: now, healthy: false, instance_count: 0, error: String(error?.message || error) };
    stats.instance_safety_errors += 1;
  }
  return instanceSafety;
}

async function ensureSingleInstanceRestSafety(source = '') {
  if (RENDER_RUNTIME && Date.now() - PROCESS_STARTED_AT < INSTANCE_STARTUP_REST_GRACE_MS) {
    stats.instance_safety_blocks += 1;
    throw guardError('binance_rest_instance_startup_grace', 'BINANCE_REST_INSTANCE_STARTUP_GRACE', {
      binanceRestInstanceStartupGrace: true,
      source,
      retryAt: PROCESS_STARTED_AT + INSTANCE_STARTUP_REST_GRACE_MS,
    });
  }
  // Force an OS-resolver-backed discovery check before every Binance REST start.
  // A cached singleton result could otherwise leak one request from the old
  // instance during Render's zero-downtime overlap window.
  const current = await refreshInstanceSafety({ force: true });
  if (current.healthy) return;
  stats.instance_safety_blocks += 1;
  throw guardError('binance_rest_multi_instance_blocked', 'BINANCE_REST_MULTI_INSTANCE_BLOCKED', {
    binanceRestMultiInstanceBlocked: true,
    source,
    instanceCount: current.instance_count,
    instanceSafetyError: current.error,
  });
}

function supabaseEnabled() {
  return !PROCESS_REST_DISABLED && Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
}

function headers(prefer = '') {
  const result = {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    accept: 'application/json',
    'content-type': 'application/json',
  };
  if (prefer) result.prefer = prefer;
  return result;
}

function toFiniteMs(value) {
  let parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  if (parsed > 10_000_000_000_000) parsed = Math.floor(parsed / 1000);
  return Math.floor(parsed);
}

function positiveNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

export function parseBinanceBanUntil(message) {
  const match = String(message || '').match(/banned\s+until\s+(\d{12,16})/i);
  return match ? toFiniteMs(match[1]) : null;
}

function parseRetryAfterSeconds(value) {
  if (value == null || String(value).trim() === '') return null;
  const numeric = positiveNumber(value);
  if (numeric != null) return numeric;
  const absolute = Date.parse(String(value));
  if (!Number.isFinite(absolute)) return null;
  return Math.max(1, Math.ceil((absolute - Date.now()) / 1000));
}

export function isBinanceRestrictedResponse(status = 0, bodyText = '') {
  const code = Number(status || 0);
  const text = String(bodyText || '').toLowerCase();
  return [403, 418, 429, 451].includes(code) ||
    text.includes('way too many requests') ||
    text.includes('too many requests') ||
    text.includes('banned until') ||
    text.includes('waf') ||
    text.includes('restricted') ||
    text.includes('ip ban');
}

function weightThresholdForSource(source = '') {
  const normalized = String(source || '');
  if (normalized === 'post_ban_probe') return PROBE_MAX_USED_WEIGHT_1M;
  if (normalized.startsWith('kline_bridge:') && state?.operating_mode === 'validation_only') {
    return VALIDATION_MAX_USED_WEIGHT_1M;
  }
  return NORMAL_MAX_USED_WEIGHT_1M;
}

function currentControlEpoch() {
  return Math.max(0, Number.parseInt(String(state?.validation_control_epoch ?? 0), 10) || 0);
}

function advanceControlEpoch() {
  const next = currentControlEpoch() + 1;
  stats.validation_control_epoch_advances += 1;
  return next;
}

function abortError(source = '') {
  return guardError('binance_rest_client_aborted', 'BINANCE_REST_CLIENT_ABORTED', {
    binanceRestClientAborted: true,
    source,
  });
}

function throwIfAborted(signal, source = '') {
  if (signal?.aborted) {
    stats.client_abort_blocks += 1;
    throw abortError(source);
  }
}

async function cancellableSleep(ms, signal, source = '') {
  throwIfAborted(signal, source);
  if (!signal) return sleep(ms);
  await new Promise((resolve, reject) => {
    const timer = setTimeout(done, Math.max(0, ms));
    function cleanup() { signal.removeEventListener('abort', onAbort); }
    function done() { cleanup(); resolve(); }
    function onAbort() { clearTimeout(timer); cleanup(); stats.client_abort_blocks += 1; reject(abortError(source)); }
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

function resetToProbeRequired({
  reason,
  message = '',
  cooldownMs = NORMAL_UNSAFE_COOLDOWN_MS,
  status = 200,
  source = 'binance_rest_guard',
} = {}) {
  const now = Date.now();
  state = {
    ...state,
    until: Math.max(Number(state?.until || 0), now + Math.max(0, Number(cooldownMs) || 0)),
    status: Number(status || 200),
    reason: String(reason || 'binance_rest_probe_required'),
    source: String(source || 'binance_rest_guard'),
    error: String(message || reason || ''),
    parsed_ban_until: null,
    retry_after_seconds: null,
    updated_at: now,
    probe_required: true,
    probe_passed_at: 0,
    probe_source: '',
    operating_mode: 'probe_required',
    validation_session_hash: '',
    validation_budget: 0,
    validation_created_at: 0,
    validation_expires_at: 0,
    validation_next_index: 0,
    validation_inflight: null,
    validation_admin_key_fingerprint: VALIDATION_ADMIN_KEY_FINGERPRINT,
    validation_control_epoch: advanceControlEpoch(),
    schema_version: GUARD_SCHEMA_VERSION,
  };
}

export async function observeBinanceRestResponse({
  response,
  bodyText = '',
  source = 'unknown_binance_rest_caller',
  allowMissingWeight = false,
} = {}) {
  if (!response) {
    throw guardError('binance_response_required', 'BINANCE_RESPONSE_REQUIRED', { source });
  }
  if (!response.ok) {
    const message = `${response.status} ${response.statusText || ''} ${String(bodyText || '').slice(0, 360)}`.trim();
    if (isBinanceRestrictedResponse(response.status, bodyText)) {
      markBinanceRestRestricted({
        status: response.status,
        message,
        source,
        retryAfterSeconds: response.headers?.get?.('retry-after'),
      });
      await flushBinanceRestGuardPersistence();
      return { restricted: true, message };
    }
    return { restricted: false, message };
  }

  stats.successes += 1;
  stats.last_success_at = Date.now();
  stats.last_success_source = String(source || '');
  stats.last_error = '';

  const rawWeight = response.headers?.get?.('x-mbx-used-weight-1m');
  const weight = rawWeight == null || String(rawWeight).trim() === '' ? Number.NaN : Number(rawWeight);
  if (!Number.isFinite(weight)) {
    if (allowMissingWeight) return { restricted: false, usedWeight1m: null, weightSafe: true };
    stats.weight_header_missing += 1;
    resetToProbeRequired({
      reason: 'binance_rest_weight_header_missing',
      message: `source:${source}`,
      cooldownMs: NORMAL_UNSAFE_COOLDOWN_MS,
      source,
    });
    await persistSnapshotStrict();
    return { restricted: false, usedWeight1m: null, weightSafe: false, reason: 'weight_header_missing' };
  }

  stats.last_used_weight_1m = weight;
  const threshold = weightThresholdForSource(source);
  if (weight > threshold) {
    stats.weight_limit_unsafe += 1;
    resetToProbeRequired({
      reason: 'binance_rest_weight_unsafe',
      message: `used_weight_1m:${weight};max:${threshold};source:${source}`,
      cooldownMs: NORMAL_UNSAFE_COOLDOWN_MS,
      source,
    });
    await persistSnapshotStrict();
    return { restricted: false, usedWeight1m: weight, weightSafe: false, reason: 'weight_unsafe', threshold };
  }
  return { restricted: false, usedWeight1m: weight, weightSafe: true, threshold };
}

function hashValidationToken(value) {
  return createHash('sha256').update(String(value || ''), 'utf8').digest('hex');
}

function safeTokenMatches(token, expectedHash) {
  const actual = Buffer.from(hashValidationToken(token), 'hex');
  const expected = Buffer.from(String(expectedHash || ''), 'hex');
  if (actual.length !== 32 || expected.length !== 32) return false;
  return timingSafeEqual(actual, expected);
}

function safeAdminKeyMatches(value) {
  if (!VALIDATION_ADMIN_KEY || !value) return false;
  const actual = Buffer.from(hashValidationToken(value), 'hex');
  const expected = Buffer.from(hashValidationToken(VALIDATION_ADMIN_KEY), 'hex');
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

export function isBinanceValidationAdminConfigured() {
  return VALIDATION_ADMIN_KEY_VALID;
}

export function isBinanceValidationAdminAuthorized(value) {
  const ok = isBinanceValidationAdminConfigured() && safeAdminKeyMatches(String(value || '').trim());
  if (!ok) stats.validation_admin_auth_failures += 1;
  return ok;
}

function expectedValidationSymbol(index = Number(state?.validation_next_index || 0)) {
  return VALIDATION_SEQUENCE[index] || null;
}

function validationSourceFor(symbol, interval) {
  return `kline_bridge:fapi_klines:${String(symbol || '').toUpperCase()}:${String(interval || '')}`;
}

function validationSourceAllowed(source = '') {
  const normalized = String(source || '');
  return VALIDATION_ALLOWED_SOURCE_PREFIXES.some((prefix) => normalized.startsWith(prefix));
}

function currentValidationContext() {
  return validationContext.getStore() || null;
}

function validationRequestAuthorized(source = '') {
  if (state?.operating_mode !== 'validation_only') return false;
  if (!validationSourceAllowed(source)) return false;
  if (Number(state?.validation_budget || 0) <= 0) return false;
  const context = currentValidationContext();
  if (!context || Number(context.remainingCalls || 0) <= 0) return false;
  if (!safeTokenMatches(context.token, state?.validation_session_hash)) return false;
  if (String(source) !== String(context.expectedSource || '')) return false;
  return String(context.symbol || '') === String(expectedValidationSymbol() || '');
}

function activeState() {
  if (Number(state?.until || 0) > Date.now()) return state;
  return null;
}

function validationSessionExpired() {
  return state?.operating_mode === 'validation_only' &&
    Number(state?.validation_expires_at || 0) > 0 &&
    Number(state.validation_expires_at) <= Date.now();
}

async function expireValidationSessionIfNeeded(source = 'validation_session_expiry_check') {
  if (!validationSessionExpired()) return false;
  stats.validation_session_expired += 1;
  resetToProbeRequired({
    reason: 'validation_session_expired',
    message: `source:${source}`,
    cooldownMs: VALIDATION_RECOVERY_COOLDOWN_MS,
    source,
  });
  await persistSnapshotStrict();
  return true;
}

function probeGateActive() {
  return !activeState() && state?.probe_required === true;
}

function validationOnlyBlocks(source = '') {
  if (activeState() || state?.probe_required === true) return false;
  if (state?.operating_mode !== 'validation_only') return false;
  return !validationRequestAuthorized(source);
}

function normalizedRestoredState(restored, record = null) {
  const restoredUntil = toFiniteMs(restored?.until) || 0;
  const probePassedAt = toFiniteMs(restored?.probe_passed_at) || 0;
  const validationSessionHash = String(restored?.validation_session_hash || '').toLowerCase();
  const storedAdminFingerprint = String(restored?.validation_admin_key_fingerprint || '').toLowerCase();
  const schemaMatches = String(restored?.schema_version || '') === GUARD_SCHEMA_VERSION;
  // Use a direct timing-safe comparison for already-hashed fingerprints.
  const fingerprintMatches = (() => {
    if (!VALIDATION_ADMIN_KEY_FINGERPRINT || !storedAdminFingerprint) return false;
    const actual = Buffer.from(storedAdminFingerprint, 'hex');
    const expected = Buffer.from(VALIDATION_ADMIN_KEY_FINGERPRINT, 'hex');
    return actual.length === 32 && expected.length === 32 && timingSafeEqual(actual, expected);
  })();

  const restoredMode = String(restored?.operating_mode || 'probe_required');
  const hasValidationSession = schemaMatches &&
    fingerprintMatches &&
    /^[a-f0-9]{64}$/.test(validationSessionHash);
  const restoredBudget = Math.max(0, Math.min(
    VALIDATION_REST_BUDGET,
    Number.parseInt(String(restored?.validation_budget ?? 0), 10) || 0,
  ));
  const restoredNextIndex = Math.max(0, Math.min(
    VALIDATION_SEQUENCE.length,
    Number.parseInt(String(restored?.validation_next_index ?? 0), 10) || 0,
  ));
  const restoredExpiresAt = toFiniteMs(restored?.validation_expires_at) || 0;
  const restoredControlEpoch = Math.max(0, Number.parseInt(String(restored?.validation_control_epoch ?? 0), 10) || 0);
  const restoredInflight = restored?.validation_inflight && typeof restored.validation_inflight === 'object'
    ? {
        symbol: String(restored.validation_inflight.symbol || '').toUpperCase(),
        interval: String(restored.validation_inflight.interval || ''),
        source: String(restored.validation_inflight.source || ''),
        started_at: toFiniteMs(restored.validation_inflight.started_at) || 0,
      }
    : null;

  let probeRequired = restored?.probe_required !== false;
  let operatingMode = restoredMode;
  let validationInflight = restoredInflight;
  let until = restoredUntil;
  let reason = String(restored?.reason || 'persisted_binance_rest_guard');

  if (!schemaMatches || !fingerprintMatches) {
    probeRequired = true;
    operatingMode = 'probe_required';
    validationInflight = null;
    reason = fingerprintMatches ? 'guard_schema_changed' : 'validation_admin_key_rotated';
    if (!fingerprintMatches) stats.admin_key_rotation_resets += 1;
  } else if (restoredMode === 'validation_only' && restoredExpiresAt > 0 && restoredExpiresAt <= Date.now()) {
    probeRequired = true;
    operatingMode = 'probe_required';
    validationInflight = null;
    until = Math.max(until, Date.now() + VALIDATION_RECOVERY_COOLDOWN_MS);
    reason = 'validation_session_expired';
    stats.validation_session_expired += 1;
  } else if (restoredInflight) {
    // A process restart while one validation call was reserved means we cannot know
    // whether the upstream request left the old process. Fail closed and require a
    // fresh probe rather than replaying the same Binance request.
    probeRequired = true;
    operatingMode = 'probe_required';
    validationInflight = null;
    until = Math.max(until, Date.now() + PROBE_UNSAFE_COOLDOWN_MS);
    reason = 'validation_inflight_recovered_require_probe';
    stats.validation_inflight_recovered += 1;
  } else if (restoredMode === 'normal_guarded') {
    probeRequired = false;
    operatingMode = 'normal_guarded';
  } else if (!hasValidationSession || restoredBudget <= 0 || restoredNextIndex >= VALIDATION_SEQUENCE.length) {
    probeRequired = true;
    operatingMode = 'probe_required';
  } else {
    probeRequired = false;
    operatingMode = 'validation_only';
  }

  return {
    until,
    status: Number(restored?.status || 418),
    reason,
    source: String(restored?.source || record?.source || 'persisted_binance_rest_guard'),
    error: String(restored?.error || ''),
    parsed_ban_until: toFiniteMs(restored?.parsed_ban_until),
    retry_after_seconds: parseRetryAfterSeconds(restored?.retry_after_seconds),
    updated_at: toFiniteMs(restored?.updated_at) || Date.now(),
    probe_required: probeRequired,
    probe_passed_at: probeRequired ? 0 : probePassedAt,
    probe_source: probeRequired ? '' : String(restored?.probe_source || ''),
    operating_mode: operatingMode,
    validation_session_hash: operatingMode === 'validation_only' ? validationSessionHash : '',
    validation_budget: operatingMode === 'validation_only' ? restoredBudget : 0,
    validation_created_at: operatingMode === 'validation_only'
      ? (toFiniteMs(restored?.validation_created_at) || probePassedAt)
      : 0,
    validation_expires_at: operatingMode === 'validation_only' ? restoredExpiresAt : 0,
    validation_next_index: operatingMode === 'validation_only' ? restoredNextIndex : (
      operatingMode === 'normal_guarded' ? VALIDATION_SEQUENCE.length : 0
    ),
    validation_inflight: operatingMode === 'validation_only' ? validationInflight : null,
    validation_admin_key_fingerprint: VALIDATION_ADMIN_KEY_FINGERPRINT,
    validation_control_epoch: restoredControlEpoch,
    schema_version: GUARD_SCHEMA_VERSION,
  };
}

async function restoreSnapshot() {
  if (!supabaseEnabled()) return;
  const query = new URLSearchParams({
    provider: `eq.${PROVIDER}`,
    market_type: `eq.${MARKET_TYPE}`,
    snapshot_type: `eq.${SNAPSHOT_TYPE}`,
    quote_asset: `eq.${SNAPSHOT_KEY}`,
    select: 'payload,updated_at,source,source_time',
    limit: '1',
  });
  const response = await fetch(`${SUPABASE_URL}/rest/v1/${SNAPSHOT_TABLE}?${query.toString()}`, {
    headers: headers(),
    signal: AbortSignal.timeout(SUPABASE_IO_TIMEOUT_MS),
  });
  if (!response.ok) throw new Error(`binance_rest_guard_restore_${response.status}`);
  const payload = await response.json();
  const record = Array.isArray(payload) ? payload[0] : null;
  const restored = record?.payload?.state;
  if (!restored || typeof restored !== 'object') return;

  const candidate = normalizedRestoredState(restored, record);
  const candidateIsNewer = candidate.updated_at > Number(state.updated_at || 0);
  const candidateBanIsLater = candidate.until > Number(state.until || 0);
  const candidateStillNeedsProbe = candidate.probe_required === true && state.probe_required !== true;
  if (candidateIsNewer || candidateBanIsLater || candidateStillNeedsProbe) {
    state = candidate;
    stats.restored_from_snapshot += 1;
  }
}

async function persistSnapshot() {
  if (!supabaseEnabled()) return;
  const now = Date.now();
  const body = [{
    provider: PROVIDER,
    market_type: MARKET_TYPE,
    snapshot_type: SNAPSHOT_TYPE,
    quote_asset: SNAPSHOT_KEY,
    payload: { state },
    row_count: 1,
    source: 'binance_shared_rest_guard',
    source_time: iso(Math.max(Number(state.until || 0), Number(state.updated_at || now))),
    updated_at: iso(now),
  }];
  const response = await fetch(
    `${SUPABASE_URL}/rest/v1/${SNAPSHOT_TABLE}?on_conflict=provider,market_type,snapshot_type,quote_asset`,
    {
      method: 'POST',
      headers: headers('resolution=merge-duplicates,return=minimal'),
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(SUPABASE_IO_TIMEOUT_MS),
    },
  );
  if (!response.ok) throw new Error(`binance_rest_guard_persist_${response.status}`);
  lastPersistenceError = null;
  stats.snapshot_persist_success += 1;
}

function queuePersistence() {
  persistPromise = persistPromise
    .catch(() => {})
    .then(() => persistSnapshot())
    .catch((error) => {
      lastPersistenceError = error;
      stats.snapshot_persist_errors += 1;
      stats.last_error = String(error?.message || error);
    });
  return persistPromise;
}

async function persistSnapshotStrict() {
  const operation = persistPromise
    .catch(() => {})
    .then(() => persistSnapshot());
  persistPromise = operation.catch((error) => {
    lastPersistenceError = error;
  });
  return operation;
}

export async function flushBinanceRestGuardPersistence() {
  await persistPromise.catch(() => {});
  if (lastPersistenceError) throw lastPersistenceError;
}

export async function ensureBinanceRestGuardInitialized() {
  if (initialized) return;
  if (PROCESS_REST_DISABLED) {
    initialized = true;
    stats.initialized_at = Date.now();
    return;
  }
  if (initPromise) return initPromise;
  initPromise = (async () => {
    stats.restore_attempts += 1;
    try {
      await restoreSnapshot();
      lastRestoreError = null;
      stats.restore_success += 1;
    } catch (error) {
      lastRestoreError = error;
      stats.restore_errors += 1;
      stats.last_error = String(error?.message || error);
      // Fail closed: never persist the local fallback over a possibly newer
      // remote ban/validation state, and never allow Binance REST until the
      // durable guard snapshot has been read successfully.
      throw guardError(
        'binance_rest_guard_restore_failed',
        'BINANCE_REST_GUARD_RESTORE_FAILED',
        {
          binanceRestGuardRestoreFailed: true,
          cause: String(error?.message || error),
        },
      );
    }
    await refreshInstanceSafety({ force: true });
    initialized = true;
    stats.initialized_at = Date.now();
    if (supabaseEnabled()) queuePersistence();
  })().finally(() => { initPromise = null; });
  return initPromise;
}

export function isBinanceRestBlocked() {
  return activeState();
}

export function isBinanceRestProbeRequired() {
  return probeGateActive();
}

function guardError(message, code, extra = {}) {
  const error = new Error(message);
  // Internal guard decisions must never masquerade as an upstream Binance 418.
  // Otherwise callers may feed the local gate error back into the restriction
  // detector and create a false 30-minute/IP-ban cooldown without any network call.
  error.status = 409;
  error.code = code;
  error.internalBinanceRestGuard = true;
  Object.assign(error, extra);
  return error;
}

function persistenceHealthyOrRetry(source = '') {
  if (!lastPersistenceError) return true;
  queuePersistence();
  stats.blocked_requests += 1;
  throw guardError('binance_rest_persistence_unhealthy', 'BINANCE_REST_PERSISTENCE_UNHEALTHY', {
    binanceRestPersistenceUnhealthy: true,
    source,
  });
}

export function runWithBinanceRequestSignal(signal, fn) {
  if (typeof fn !== 'function') throw new TypeError('fn required');
  return requestSignalContext.run(signal || null, fn);
}

export function beginBinanceRestShutdown(reason = 'process_shutdown') {
  processRestShuttingDown = true;
  stats.last_error = String(reason || 'process_shutdown');
  if (activeProbeController && !activeProbeController.signal.aborted) {
    try { activeProbeController.abort(); } catch (_) {}
  }
}

function throwIfRestShuttingDown(source = '') {
  if (!processRestShuttingDown) return;
  stats.blocked_requests += 1;
  throw guardError('binance_rest_process_shutting_down', 'BINANCE_REST_PROCESS_SHUTTING_DOWN', {
    binanceRestProcessShuttingDown: true,
    source,
  });
}

export async function acquireBinanceRestRequestSlot({
  source = 'unknown_binance_rest_caller',
  probe = false,
  maxQueueWaitMs = DEFAULT_MAX_QUEUE_WAIT_MS,
  signal = null,
} = {}) {
  const effectiveSignal = signal || requestSignalContext.getStore() || null;
  throwIfRestShuttingDown(source);
  throwIfAborted(effectiveSignal, source);
  await ensureBinanceRestGuardInitialized();
  throwIfAborted(effectiveSignal, source);
  await ensureSingleInstanceRestSafety(source);
  await expireValidationSessionIfNeeded(source);

  if (PROCESS_REST_DISABLED) {
    stats.blocked_requests += 1;
    throw guardError('binance_rest_disabled_in_child_process', 'BINANCE_REST_PROCESS_DISABLED', {
      binanceRestProcessDisabled: true,
      source,
    });
  }
  persistenceHealthyOrRetry(source);

  const blockedBeforeQueue = activeState();
  if (blockedBeforeQueue) {
    stats.blocked_requests += 1;
    throw guardError(`binance_rest_blocked_until:${blockedBeforeQueue.until}`, 'BINANCE_REST_BLOCKED', {
      binanceRestBlocked: true,
      guardState: blockedBeforeQueue,
      source,
    });
  }
  if (!probe && probeGateActive()) {
    stats.probe_required_blocks += 1;
    throw guardError('binance_rest_probe_required', 'BINANCE_REST_PROBE_REQUIRED', {
      binanceRestProbeRequired: true,
      guardState: state,
      source,
    });
  }
  if (!probe && validationOnlyBlocks(source)) {
    stats.blocked_requests += 1;
    stats.validation_session_blocks += 1;
    if (Number(state?.validation_budget || 0) <= 0) stats.validation_budget_exhausted += 1;
    throw guardError('binance_rest_validation_session_required', 'BINANCE_REST_VALIDATION_SESSION_REQUIRED', {
      binanceRestValidationSessionRequired: true,
      guardState: state,
      source,
    });
  }
  if (pendingRequests >= MAX_PENDING_REQUESTS) {
    stats.queue_rejections += 1;
    throw guardError('binance_rest_queue_full', 'BINANCE_REST_QUEUE_FULL', {
      binanceRestQueueFull: true,
      source,
    });
  }

  let releaseCurrent;
  const previous = requestChain;
  const current = new Promise((resolve) => { releaseCurrent = resolve; });
  requestChain = current;
  pendingRequests += 1;
  stats.max_queue_depth = Math.max(stats.max_queue_depth, pendingRequests);

  let queueTimer = null;
  let queueTimedOut = false;
  try {
    const safeWaitMs = Math.max(1_000, Number(maxQueueWaitMs) || DEFAULT_MAX_QUEUE_WAIT_MS);
    const waiters = [
      previous,
      new Promise((_, reject) => {
        queueTimer = setTimeout(() => {
          queueTimedOut = true;
          reject(guardError('binance_rest_queue_wait_timeout', 'BINANCE_REST_QUEUE_TIMEOUT', {
            binanceRestQueueTimeout: true,
            source,
          }));
        }, safeWaitMs);
        queueTimer.unref?.();
      }),
    ];
    if (effectiveSignal) {
      waiters.push(new Promise((_, reject) => {
        if (effectiveSignal.aborted) { stats.client_abort_blocks += 1; reject(abortError(source)); return; }
        effectiveSignal.addEventListener('abort', () => {
          stats.client_abort_blocks += 1;
          reject(abortError(source));
        }, { once: true });
      }));
    }
    await Promise.race(waiters);
  } catch (error) {
    pendingRequests = Math.max(0, pendingRequests - 1);
    if (queueTimedOut) stats.queue_timeouts += 1;
    // Keep the FIFO chain healthy: once the previous holder releases, immediately
    // release this abandoned node so later callers are not deadlocked.
    previous.finally(() => releaseCurrent()).catch(() => releaseCurrent());
    throw error;
  } finally {
    if (queueTimer) clearTimeout(queueTimer);
  }

  pendingRequests = Math.max(0, pendingRequests - 1);
  try { throwIfRestShuttingDown(source); } catch (error) { releaseCurrent(); throw error; }
  try { throwIfAborted(effectiveSignal, source); } catch (error) { releaseCurrent(); throw error; }

  // Step650.8.8: after this caller owns the FIFO node, every guard/persistence
  // failure must release it. Otherwise one transient Supabase error can leave
  // requestChain unresolved forever and deadlock all later Binance REST work.
  try {
    persistenceHealthyOrRetry(source);
  } catch (error) {
    stats.queue_release_on_guard_error += 1;
    releaseCurrent();
    throw error;
  }
  const blockedAfterQueue = activeState();
  if (blockedAfterQueue) {
    stats.blocked_requests += 1;
    releaseCurrent();
    throw guardError(`binance_rest_blocked_until:${blockedAfterQueue.until}`, 'BINANCE_REST_BLOCKED', {
      binanceRestBlocked: true,
      guardState: blockedAfterQueue,
      source,
    });
  }
  if (!probe && probeGateActive()) {
    stats.probe_required_blocks += 1;
    releaseCurrent();
    throw guardError('binance_rest_probe_required', 'BINANCE_REST_PROBE_REQUIRED', {
      binanceRestProbeRequired: true,
      guardState: state,
      source,
    });
  }

  if (!probe && validationOnlyBlocks(source)) {
    stats.blocked_requests += 1;
    stats.validation_session_blocks += 1;
    if (Number(state?.validation_budget || 0) <= 0) stats.validation_budget_exhausted += 1;
    releaseCurrent();
    throw guardError('binance_rest_validation_session_required', 'BINANCE_REST_VALIDATION_SESSION_REQUIRED', {
      binanceRestValidationSessionRequired: true,
      guardState: state,
      source,
    });
  }

  const waitMs = Math.max(0, MIN_REQUEST_GAP_MS - (Date.now() - lastRequestStartedAt));
  if (waitMs > 0) {
    stats.request_slot_waits += 1;
    try { await cancellableSleep(waitMs, effectiveSignal, source); } catch (error) { releaseCurrent(); throw error; }
  }

  try {
    throwIfAborted(effectiveSignal, source);
    persistenceHealthyOrRetry(source);
  } catch (error) {
    stats.queue_release_on_guard_error += 1;
    releaseCurrent();
    throw error;
  }
  const blockedAfterWait = activeState();
  if (blockedAfterWait) {
    stats.blocked_requests += 1;
    releaseCurrent();
    throw guardError(`binance_rest_blocked_until:${blockedAfterWait.until}`, 'BINANCE_REST_BLOCKED', {
      binanceRestBlocked: true,
      guardState: blockedAfterWait,
      source,
    });
  }
  if (!probe && probeGateActive()) {
    stats.probe_required_blocks += 1;
    releaseCurrent();
    throw guardError('binance_rest_probe_required', 'BINANCE_REST_PROBE_REQUIRED', {
      binanceRestProbeRequired: true,
      guardState: state,
      source,
    });
  }

  if (!probe && validationOnlyBlocks(source)) {
    stats.blocked_requests += 1;
    stats.validation_session_blocks += 1;
    if (Number(state?.validation_budget || 0) <= 0) stats.validation_budget_exhausted += 1;
    releaseCurrent();
    throw guardError('binance_rest_validation_session_required', 'BINANCE_REST_VALIDATION_SESSION_REQUIRED', {
      binanceRestValidationSessionRequired: true,
      guardState: state,
      source,
    });
  }

  if (!probe && state?.operating_mode === 'validation_only') {
    const context = currentValidationContext();
    if (!validationRequestAuthorized(source) || state?.validation_inflight) {
      stats.blocked_requests += 1;
      stats.validation_session_blocks += 1;
      if (Number(state?.validation_budget || 0) <= 0) stats.validation_budget_exhausted += 1;
      releaseCurrent();
      throw guardError('binance_rest_validation_session_required', 'BINANCE_REST_VALIDATION_SESSION_REQUIRED', {
        binanceRestValidationSessionRequired: true,
        guardState: state,
        source,
      });
    }
    context.remainingCalls = Math.max(0, Number(context.remainingCalls || 0) - 1);
    state = {
      ...state,
      validation_inflight: {
        symbol: String(context.symbol || ''),
        interval: String(context.interval || ''),
        source: String(source || ''),
        started_at: Date.now(),
      },
      updated_at: Date.now(),
      validation_admin_key_fingerprint: VALIDATION_ADMIN_KEY_FINGERPRINT,
      schema_version: GUARD_SCHEMA_VERSION,
    };
    stats.validation_calls_authorized += 1;

    // Persist an in-flight reservation before the real Binance request leaves.
    // On a crash/restart, restoreSnapshot() sees this reservation and requires a
    // new probe instead of replaying an uncertain upstream request.
    if (!supabaseEnabled()) {
      stats.validation_state_persist_failures += 1;
      state = { ...state, validation_inflight: null };
      releaseCurrent();
      throw guardError(
        'binance_validation_persistence_required',
        'BINANCE_VALIDATION_PERSISTENCE_REQUIRED',
        { binanceValidationPersistenceRequired: true, source },
      );
    }
    try {
      await persistSnapshotStrict();
    } catch (error) {
      stats.snapshot_persist_errors += 1;
      stats.validation_state_persist_failures += 1;
      stats.last_error = String(error?.message || error);
      state = { ...state, validation_inflight: null };
      releaseCurrent();
      throw guardError(
        'binance_validation_state_persist_failed',
        'BINANCE_VALIDATION_STATE_PERSIST_FAILED',
        { binanceValidationStatePersistFailed: true, source },
      );
    }
  }

  try {
    throwIfAborted(effectiveSignal, source);
  } catch (error) {
    const inflight = state?.validation_inflight;
    if (
      state?.operating_mode === 'validation_only' &&
      inflight &&
      String(inflight.source || '') === String(source || '')
    ) {
      state = { ...state, validation_inflight: null, updated_at: Date.now() };
      try {
        await persistSnapshotStrict();
        stats.validation_reservations_cancelled_before_network += 1;
      } catch (persistError) {
        resetToProbeRequired({
          reason: 'validation_abort_cleanup_not_durable',
          message: String(persistError?.message || persistError),
          cooldownMs: PROBE_UNSAFE_COOLDOWN_MS,
          source,
        });
        queuePersistence();
      }
    }
    releaseCurrent();
    throw error;
  }
  lastRequestStartedAt = Date.now();
  activeRequest = true;
  stats.requests_started += 1;
  let released = false;
  return () => {
    if (released) return;
    released = true;
    activeRequest = false;
    releaseCurrent();
  };
}

export function markBinanceRestRestricted({ status = 0, message = '', source = '', retryAfterSeconds = null } = {}) {
  const parsedBanUntil = parseBinanceBanUntil(message);
  if (parsedBanUntil) stats.ban_until_parsed += 1;
  const retrySeconds = parseRetryAfterSeconds(retryAfterSeconds);
  const retryUntil = retrySeconds != null ? Date.now() + Math.ceil(retrySeconds * 1000) : 0;
  if (retryUntil) stats.retry_after_parsed += 1;

  const until = Math.max(
    Number(state?.until || 0),
    Date.now() + RESTRICTED_FALLBACK_MS,
    Number(parsedBanUntil || 0) + BAN_SAFETY_MS,
    retryUntil + BAN_SAFETY_MS,
  );
  state = {
    until,
    status: Number(status || 418),
    reason: 'exchange_rate_limit_or_region_block',
    source: String(source || 'unknown_binance_rest_caller'),
    error: String(message || ''),
    parsed_ban_until: parsedBanUntil,
    retry_after_seconds: retrySeconds,
    updated_at: Date.now(),
    probe_required: true,
    probe_passed_at: 0,
    probe_source: '',
    operating_mode: 'probe_required',
    validation_session_hash: '',
    validation_budget: 0,
    validation_created_at: 0,
    validation_expires_at: 0,
    validation_next_index: 0,
    validation_inflight: null,
    validation_admin_key_fingerprint: VALIDATION_ADMIN_KEY_FINGERPRINT,
    validation_control_epoch: advanceControlEpoch(),
    schema_version: GUARD_SCHEMA_VERSION,
  };
  stats.restricted_responses += 1;
  stats.last_error = String(message || 'binance_rest_restricted');
  queuePersistence();
  return state;
}

export function markBinanceRestSuccess({ source = '', usedWeight1m = null, authorizeProbe = false } = {}) {
  stats.successes += 1;
  stats.last_success_at = Date.now();
  stats.last_success_source = String(source || '');
  const weight = Number(usedWeight1m);
  if (Number.isFinite(weight)) stats.last_used_weight_1m = weight;
  stats.last_error = '';
  if (!authorizeProbe) return null;

  const validationToken = randomBytes(32).toString('hex');
  const now = Date.now();
  state = {
    ...state,
    until: 0,
    status: 200,
    reason: 'post_ban_probe_passed',
    source: String(source || 'binance_rest_probe'),
    error: '',
    parsed_ban_until: null,
    retry_after_seconds: null,
    updated_at: now,
    probe_required: false,
    probe_passed_at: now,
    probe_source: String(source || 'binance_rest_probe'),
    operating_mode: 'validation_only',
    validation_session_hash: hashValidationToken(validationToken),
    validation_budget: VALIDATION_REST_BUDGET,
    validation_created_at: now,
    validation_expires_at: now + VALIDATION_SESSION_TTL_MS,
    validation_next_index: 0,
    validation_inflight: null,
    validation_admin_key_fingerprint: VALIDATION_ADMIN_KEY_FINGERPRINT,
    validation_control_epoch: currentControlEpoch(),
    schema_version: GUARD_SCHEMA_VERSION,
  };
  return {
    validation_token: validationToken,
    validation_budget: VALIDATION_REST_BUDGET,
  };
}

async function persistProbeStateOrFail({ reason = 'probe_state_persist_failed' } = {}) {
  if (!supabaseEnabled()) {
    stats.probe_state_persist_failures += 1;
    throw guardError(
      'binance_probe_persistence_required',
      'BINANCE_PROBE_PERSISTENCE_REQUIRED',
      { binanceProbePersistenceRequired: true },
    );
  }
  try {
    await persistSnapshotStrict();
  } catch (error) {
    stats.snapshot_persist_errors += 1;
    stats.probe_state_persist_failures += 1;
    stats.last_error = String(error?.message || error);
    throw guardError(reason, 'BINANCE_PROBE_STATE_PERSIST_FAILED', {
      binanceProbeStatePersistFailed: true,
    });
  }
}

function putProbeBackBehindLocalCooldown(reason, message = '') {
  const now = Date.now();
  state = {
    ...state,
    until: now + PROBE_UNSAFE_COOLDOWN_MS,
    status: 200,
    reason,
    source: 'post_ban_probe',
    error: String(message || reason),
    parsed_ban_until: null,
    retry_after_seconds: null,
    updated_at: now,
    probe_required: true,
    probe_passed_at: 0,
    probe_source: '',
    operating_mode: 'probe_required',
    validation_session_hash: '',
    validation_budget: 0,
    validation_created_at: 0,
    validation_expires_at: 0,
    validation_next_index: 0,
    validation_inflight: null,
    validation_admin_key_fingerprint: VALIDATION_ADMIN_KEY_FINGERPRINT,
    validation_control_epoch: advanceControlEpoch(),
    schema_version: GUARD_SCHEMA_VERSION,
  };
}

async function runBinanceRestProbeOnce(adminKey) {
  await ensureBinanceRestGuardInitialized();
  await expireValidationSessionIfNeeded('post_ban_probe');
  if (PROCESS_REST_DISABLED) {
    throw guardError('binance_rest_probe_disabled_in_child_process', 'BINANCE_REST_PROCESS_DISABLED', {
      binanceRestProcessDisabled: true,
    });
  }
  if (!isBinanceValidationAdminConfigured()) {
    throw guardError('binance_validation_admin_key_not_configured', 'BINANCE_VALIDATION_ADMIN_KEY_NOT_CONFIGURED', {
      binanceValidationAdminKeyNotConfigured: true,
    });
  }
  if (!isBinanceValidationAdminAuthorized(adminKey)) {
    throw guardError('binance_validation_admin_key_invalid', 'BINANCE_VALIDATION_ADMIN_KEY_INVALID', {
      binanceValidationAdminKeyInvalid: true,
    });
  }
  if (!supabaseEnabled()) {
    throw guardError('binance_probe_persistence_required', 'BINANCE_PROBE_PERSISTENCE_REQUIRED', {
      binanceProbePersistenceRequired: true,
    });
  }
  const blocked = activeState();
  if (blocked) {
    throw guardError(`binance_rest_blocked_until:${blocked.until}`, 'BINANCE_REST_BLOCKED', {
      binanceRestBlocked: true,
      guardState: blocked,
      source: 'post_ban_probe',
    });
  }
  if (!state.probe_required) {
    return {
      ok: true,
      skipped: true,
      reason: 'probe_already_passed',
      guard: getBinanceRestGuardHealth(),
    };
  }

  stats.probes_started += 1;
  const probeEpoch = currentControlEpoch();
  const controller = new AbortController();
  activeProbeController = controller;
  let release;
  try {
    release = await acquireBinanceRestRequestSlot({
      source: 'post_ban_probe',
      probe: true,
      maxQueueWaitMs: 5_000,
      signal: controller.signal,
    });
  } catch (error) {
    if (activeProbeController === controller) activeProbeController = null;
    throw error;
  }
  // Another explicit probe may have completed while this caller waited in the
  // bounded FIFO. Re-check before touching Binance so concurrent validation
  // clicks still result in at most one real probe request.
  if (!state.probe_required) {
    release();
    return {
      ok: true,
      skipped: true,
      reason: 'probe_completed_while_queued',
      guard: getBinanceRestGuardHealth(),
    };
  }

  const timer = setTimeout(() => controller.abort(), PROBE_TIMEOUT_MS);
  timer.unref?.();
  try {
    const response = await fetch(PROBE_URL, {
      signal: controller.signal,
      headers: {
        accept: 'application/json',
        'user-agent': 'KakaWeb3-Binance-Rest-Probe/650.8.8',
      },
    });
    const bodyText = await response.text();
    if (!response.ok) {
      const message = `${response.status} ${response.statusText} ${bodyText.slice(0, 360)}`.trim();
      if (isBinanceRestrictedResponse(response.status, bodyText)) {
        stats.probes_restricted += 1;
        markBinanceRestRestricted({
          status: response.status,
          message,
          source: 'post_ban_probe',
          retryAfterSeconds: response.headers.get('retry-after'),
        });
        await flushBinanceRestGuardPersistence();
      } else {
        stats.probes_failed += 1;
        stats.probe_uncertain_failures += 1;
        putProbeBackBehindLocalCooldown('post_ban_probe_http_failure', message);
        await persistProbeStateOrFail({ reason: 'binance_probe_failure_state_persist_failed' });
      }
      const error = new Error(message);
      error.status = response.status;
      throw error;
    }

    const rawUsedWeight = response.headers.get('x-mbx-used-weight-1m');
    const usedWeight = rawUsedWeight == null || String(rawUsedWeight).trim() === ''
      ? Number.NaN
      : Number(rawUsedWeight);
    if (!Number.isFinite(usedWeight)) {
      stats.probe_weight_missing += 1;
      putProbeBackBehindLocalCooldown('post_ban_probe_weight_header_missing');
      await persistProbeStateOrFail({ reason: 'binance_probe_weight_state_persist_failed' });
      throw guardError('binance_probe_weight_header_missing', 'BINANCE_PROBE_WEIGHT_HEADER_MISSING', {
        binanceProbeWeightHeaderMissing: true,
      });
    }
    if (usedWeight > PROBE_MAX_USED_WEIGHT_1M) {
      stats.probe_weight_unsafe += 1;
      putProbeBackBehindLocalCooldown('post_ban_probe_weight_unsafe', `used_weight_1m:${usedWeight}`);
      await persistProbeStateOrFail({ reason: 'binance_probe_weight_state_persist_failed' });
      throw guardError('binance_probe_weight_unsafe', 'BINANCE_PROBE_WEIGHT_UNSAFE', {
        binanceProbeWeightUnsafe: true,
        usedWeight1m: usedWeight,
        maxUsedWeight1m: PROBE_MAX_USED_WEIGHT_1M,
      });
    }

    if (currentControlEpoch() !== probeEpoch || state?.probe_required !== true || activeState()) {
      stats.probe_results_superseded += 1;
      throw guardError('binance_probe_result_superseded', 'BINANCE_PROBE_RESULT_SUPERSEDED', {
        binanceProbeResultSuperseded: true,
        probeEpoch,
        currentEpoch: currentControlEpoch(),
      });
    }

    const authorization = markBinanceRestSuccess({
      source: 'post_ban_probe',
      usedWeight1m: usedWeight,
      authorizeProbe: true,
    });
    try {
      await persistProbeStateOrFail();
    } catch (error) {
      putProbeBackBehindLocalCooldown('post_ban_probe_state_not_durable', String(error?.message || error));
      throw error;
    }
    if (
      currentControlEpoch() !== probeEpoch ||
      state?.operating_mode !== 'validation_only' ||
      !safeTokenMatches(authorization?.validation_token || '', state?.validation_session_hash)
    ) {
      stats.probe_results_superseded += 1;
      throw guardError('binance_probe_result_superseded_after_persist', 'BINANCE_PROBE_RESULT_SUPERSEDED', {
        binanceProbeResultSuperseded: true,
        probeEpoch,
        currentEpoch: currentControlEpoch(),
      });
    }
    stats.probes_succeeded += 1;
    return {
      ok: true,
      skipped: false,
      status: response.status,
      used_weight_1m: usedWeight,
      max_safe_used_weight_1m: PROBE_MAX_USED_WEIGHT_1M,
      validation_token: authorization?.validation_token || null,
      validation_budget: authorization?.validation_budget || 0,
      guard: getBinanceRestGuardHealth(),
    };
  } catch (error) {
    const status = Number(error?.status || 0);
    const uncertainNetworkFailure = error?.name === 'AbortError' || status === 0;
    if (uncertainNetworkFailure) {
      stats.probes_failed += 1;
      stats.probe_uncertain_failures += 1;
      stats.last_error = error?.name === 'AbortError'
        ? 'binance_rest_probe_timeout'
        : String(error?.message || 'binance_rest_probe_network_failure');
      putProbeBackBehindLocalCooldown(
        error?.name === 'AbortError' ? 'post_ban_probe_timeout' : 'post_ban_probe_network_failure',
        stats.last_error,
      );
      try {
        await persistProbeStateOrFail({ reason: 'binance_probe_uncertain_state_persist_failed' });
      } catch (_) {}
    }
    throw error;
  } finally {
    clearTimeout(timer);
    if (activeProbeController === controller) activeProbeController = null;
    release();
  }
}


export function runBinanceRestProbe(adminKey) {
  if (!isBinanceValidationAdminConfigured()) {
    return Promise.reject(guardError(
      'binance_validation_admin_key_not_configured',
      'BINANCE_VALIDATION_ADMIN_KEY_NOT_CONFIGURED',
      { binanceValidationAdminKeyNotConfigured: true },
    ));
  }
  if (!isBinanceValidationAdminAuthorized(adminKey)) {
    return Promise.reject(guardError(
      'binance_validation_admin_key_invalid',
      'BINANCE_VALIDATION_ADMIN_KEY_INVALID',
      { binanceValidationAdminKeyInvalid: true },
    ));
  }
  if (probePromise) {
    stats.duplicate_probe_blocks += 1;
    return Promise.reject(guardError('binance_probe_already_in_progress', 'BINANCE_PROBE_ALREADY_IN_PROGRESS', {
      binanceProbeAlreadyInProgress: true,
    }));
  }
  probePromise = runBinanceRestProbeOnce(adminKey).finally(() => {
    probePromise = null;
  });
  return probePromise;
}

export async function runWithBinanceValidationSession(
  token,
  fn,
  {
    maxRestCalls = 1,
    provider = '',
    market = '',
    symbol = '',
    interval = '',
    limit = 0,
    endTimeProvided = false,
  } = {},
) {
  await ensureBinanceRestGuardInitialized();
  await expireValidationSessionIfNeeded('validation_request');
  if (PROCESS_REST_DISABLED) {
    throw guardError('binance_rest_disabled_in_child_process', 'BINANCE_REST_PROCESS_DISABLED', {
      binanceRestProcessDisabled: true,
    });
  }
  if (activeState()) {
    throw guardError('binance_rest_blocked', 'BINANCE_REST_BLOCKED', {
      binanceRestBlocked: true,
      guardState: state,
    });
  }
  if (state?.probe_required === true || state?.operating_mode !== 'validation_only') {
    throw guardError('binance_rest_probe_required', 'BINANCE_REST_PROBE_REQUIRED', {
      binanceRestProbeRequired: true,
      guardState: state,
    });
  }
  if (!safeTokenMatches(token, state?.validation_session_hash)) {
    stats.validation_session_blocks += 1;
    throw guardError('binance_rest_validation_token_invalid', 'BINANCE_REST_VALIDATION_TOKEN_INVALID', {
      binanceRestValidationTokenInvalid: true,
      guardState: state,
    });
  }
  if (Number(state?.validation_budget || 0) <= 0) {
    stats.validation_budget_exhausted += 1;
    throw guardError('binance_rest_validation_budget_exhausted', 'BINANCE_REST_VALIDATION_BUDGET_EXHAUSTED', {
      binanceRestValidationBudgetExhausted: true,
      guardState: state,
    });
  }

  const normalizedProvider = String(provider || '').toLowerCase();
  const normalizedMarket = String(market || '').toLowerCase();
  const normalizedSymbol = String(symbol || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  const normalizedInterval = String(interval || '');
  const normalizedLimit = Number.parseInt(String(limit), 10) || 0;
  const expectedSymbol = expectedValidationSymbol();
  const sequenceMatches =
    normalizedProvider === 'binance' &&
    normalizedMarket === 'contract' &&
    normalizedInterval === VALIDATION_INTERVAL &&
    normalizedSymbol === expectedSymbol &&
    normalizedLimit === VALIDATION_LIMIT &&
    endTimeProvided !== true;

  if (!sequenceMatches) {
    stats.validation_sequence_blocks += 1;
    throw guardError('binance_rest_validation_sequence_mismatch', 'BINANCE_REST_VALIDATION_SEQUENCE_MISMATCH', {
      binanceRestValidationSequenceMismatch: true,
      expectedSymbol,
      expectedInterval: VALIDATION_INTERVAL,
      requestedSymbol: normalizedSymbol,
      requestedInterval: normalizedInterval,
      expectedLimit: VALIDATION_LIMIT,
      requestedLimit: normalizedLimit,
      endTimeProvided: endTimeProvided === true,
      guardState: state,
    });
  }

  const safeMaxCalls = Math.max(1, Math.min(1, Number.parseInt(String(maxRestCalls), 10) || 1));
  try {
    return await validationContext.run({
      token: String(token || ''),
      remainingCalls: safeMaxCalls,
      symbol: normalizedSymbol,
      interval: normalizedInterval,
      limit: normalizedLimit,
      expectedSource: validationSourceFor(normalizedSymbol, normalizedInterval),
    }, fn);
  } catch (error) {
    const inflight = state?.validation_inflight;
    if (
      state?.operating_mode === 'validation_only' &&
      inflight &&
      String(inflight.symbol || '') === normalizedSymbol &&
      String(inflight.interval || '') === normalizedInterval
    ) {
      await failBinanceValidationCall({
        token,
        symbol: normalizedSymbol,
        interval: normalizedInterval,
        reason: `validation_request_failed:${String(error?.message || error).slice(0, 180)}`,
      }).catch(() => {});
    }
    throw error;
  }
}

export async function completeBinanceValidationCall({ token, symbol, interval } = {}) {
  await ensureBinanceRestGuardInitialized();
  const normalizedSymbol = String(symbol || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  const normalizedInterval = String(interval || '');
  const inflight = state?.validation_inflight;
  if (
    state?.operating_mode !== 'validation_only' ||
    !safeTokenMatches(token, state?.validation_session_hash) ||
    !inflight ||
    String(inflight.symbol || '') !== normalizedSymbol ||
    String(inflight.interval || '') !== normalizedInterval
  ) {
    throw guardError('binance_validation_completion_mismatch', 'BINANCE_VALIDATION_COMPLETION_MISMATCH', {
      binanceValidationCompletionMismatch: true,
      guardState: state,
    });
  }

  const nextIndex = Math.min(
    VALIDATION_SEQUENCE.length,
    Number(state.validation_next_index || 0) + 1,
  );
  const remainingBudget = Math.max(0, Number(state.validation_budget || 0) - 1);
  const completed = nextIndex >= VALIDATION_SEQUENCE.length;
  const previousState = state;
  const nextState = {
    ...state,
    until: 0,
    status: 200,
    reason: completed ? 'staged_validation_passed' : 'validation_symbol_passed',
    source: completed ? 'validation_complete' : `validation_complete:${normalizedSymbol}`,
    error: '',
    updated_at: Date.now(),
    probe_required: false,
    operating_mode: completed ? 'normal_guarded' : 'validation_only',
    validation_session_hash: completed ? '' : state.validation_session_hash,
    validation_budget: completed ? 0 : remainingBudget,
    validation_created_at: completed ? 0 : state.validation_created_at,
    validation_expires_at: completed ? 0 : state.validation_expires_at,
    validation_next_index: nextIndex,
    validation_inflight: null,
    validation_admin_key_fingerprint: VALIDATION_ADMIN_KEY_FINGERPRINT,
    schema_version: GUARD_SCHEMA_VERSION,
  };
  state = nextState;
  try {
    await persistSnapshotStrict();
  } catch (error) {
    // A successful upstream validation is not allowed to open normal traffic unless
    // the transition is durable. Fail closed in memory and require a fresh probe.
    state = previousState;
    resetToProbeRequired({
      reason: 'validation_completion_state_not_durable',
      message: String(error?.message || error),
      cooldownMs: PROBE_UNSAFE_COOLDOWN_MS,
      source: `validation_complete:${normalizedSymbol}`,
    });
    queuePersistence();
    stats.validation_state_persist_failures += 1;
    throw guardError(
      'binance_validation_completion_persist_failed',
      'BINANCE_VALIDATION_COMPLETION_PERSIST_FAILED',
      { binanceValidationCompletionPersistFailed: true },
    );
  }
  stats.validation_calls_completed += 1;
  if (completed) {
    stats.validation_sessions_completed += 1;
    stats.normal_guarded_entries += 1;
  }
  return getBinanceRestGuardHealth();
}

export async function failBinanceValidationCall({
  token,
  symbol,
  interval,
  reason = 'validation_result_failed',
} = {}) {
  await ensureBinanceRestGuardInitialized();
  const normalizedSymbol = String(symbol || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  const normalizedInterval = String(interval || '');
  const tokenMatches = safeTokenMatches(token, state?.validation_session_hash);
  const inflight = state?.validation_inflight;
  if (
    state?.operating_mode !== 'validation_only' ||
    !tokenMatches ||
    !inflight ||
    String(inflight.symbol || '') !== normalizedSymbol ||
    String(inflight.interval || '') !== normalizedInterval
  ) {
    return getBinanceRestGuardHealth();
  }
  stats.validation_calls_failed += 1;
  resetToProbeRequired({
    reason: 'validation_result_failed',
    message: String(reason || 'validation_result_failed'),
    cooldownMs: PROBE_UNSAFE_COOLDOWN_MS,
    source: `validation_failed:${normalizedSymbol}`,
  });
  await persistSnapshotStrict();
  return getBinanceRestGuardHealth();
}


export async function resetBinanceValidationSession(adminKey, {
  reason = 'admin_validation_reset',
} = {}) {
  await ensureBinanceRestGuardInitialized();
  if (!isBinanceValidationAdminConfigured()) {
    throw guardError(
      'binance_validation_admin_key_not_configured',
      'BINANCE_VALIDATION_ADMIN_KEY_NOT_CONFIGURED',
      { binanceValidationAdminKeyNotConfigured: true },
    );
  }
  if (!isBinanceValidationAdminAuthorized(adminKey)) {
    throw guardError(
      'binance_validation_admin_key_invalid',
      'BINANCE_VALIDATION_ADMIN_KEY_INVALID',
      { binanceValidationAdminKeyInvalid: true },
    );
  }
  stats.validation_admin_resets += 1;
  // Invalidate the current probe result before waiting for it. This closes both
  // races: reset-during-fetch and reset-after-probe-success-before-persistence.
  state = {
    ...state,
    validation_control_epoch: advanceControlEpoch(),
    updated_at: Date.now(),
  };
  if (activeProbeController && !activeProbeController.signal.aborted) {
    try { activeProbeController.abort(); } catch (_) {}
  }
  const pendingProbe = probePromise;
  if (pendingProbe) {
    let settled = false;
    await Promise.race([
      pendingProbe.catch(() => {}).finally(() => { settled = true; }),
      sleep(PROBE_TIMEOUT_MS + 1_000),
    ]);
    if (!settled) stats.probe_reset_wait_timeouts += 1;
  }
  resetToProbeRequired({
    reason: String(reason || 'admin_validation_reset'),
    message: 'validation session cleared without a Binance request',
    cooldownMs: VALIDATION_RECOVERY_COOLDOWN_MS,
    source: 'admin_validation_reset',
  });
  await persistSnapshotStrict();
  return getBinanceRestGuardHealth();
}

export function getBinanceRestGuardHealth() {
  const active = activeState();
  return {
    active: Boolean(active),
    next_allowed_at: active ? iso(active.until) : null,
    status: active?.status || null,
    reason: active?.reason || state?.reason || null,
    source: active?.source || state?.source || null,
    parsed_ban_until: active?.parsed_ban_until ? iso(active.parsed_ban_until) : null,
    retry_after_seconds: active?.retry_after_seconds ?? null,
    last_error: active?.error || stats.last_error || null,
    probe_required: state?.probe_required === true,
    normal_requests_blocked_until_probe: Boolean(active) || probeGateActive(),
    normal_requests_blocked_by_validation_mode: validationOnlyBlocks('normal_health_check'),
    probe_passed_at: state?.probe_passed_at ? iso(state.probe_passed_at) : null,
    probe_source: state?.probe_source || null,
    operating_mode: state?.operating_mode || null,
    validation_only: state?.operating_mode === 'validation_only',
    normal_guarded: state?.operating_mode === 'normal_guarded',
    validation_session_required: state?.operating_mode === 'validation_only',
    validation_budget_remaining: Math.max(0, Number(state?.validation_budget || 0)),
    validation_sequence: [...VALIDATION_SEQUENCE],
    validation_interval: VALIDATION_INTERVAL,
    validation_limit: VALIDATION_LIMIT,
    validation_control_epoch: currentControlEpoch(),
    active_probe_inflight: Boolean(activeProbeController && !activeProbeController.signal.aborted),
    validation_next_index: Math.max(0, Number(state?.validation_next_index || 0)),
    validation_next_symbol: expectedValidationSymbol(),
    validation_admin_key_configured: isBinanceValidationAdminConfigured(),
    validation_admin_key_format: '64_hex_chars',
    validation_admin_key_fingerprint_matches: Boolean(
      VALIDATION_ADMIN_KEY_FINGERPRINT &&
      state?.validation_admin_key_fingerprint === VALIDATION_ADMIN_KEY_FINGERPRINT
    ),
    validation_inflight: state?.validation_inflight ? {
      symbol: state.validation_inflight.symbol,
      interval: state.validation_inflight.interval,
      started_at: iso(state.validation_inflight.started_at),
    } : null,
    probe_max_used_weight_1m: PROBE_MAX_USED_WEIGHT_1M,
    validation_max_used_weight_1m: VALIDATION_MAX_USED_WEIGHT_1M,
    normal_max_used_weight_1m: NORMAL_MAX_USED_WEIGHT_1M,
    probe_unsafe_cooldown_ms: PROBE_UNSAFE_COOLDOWN_MS,
    process_rest_disabled: PROCESS_REST_DISABLED,
    process_rest_shutting_down: processRestShuttingDown,
    guard_schema_version: GUARD_SCHEMA_VERSION,
    validation_created_at: state?.validation_created_at ? iso(state.validation_created_at) : null,
    validation_expires_at: state?.validation_expires_at ? iso(state.validation_expires_at) : null,
    validation_session_expired: validationSessionExpired(),
    validation_session_ttl_ms: VALIDATION_SESSION_TTL_MS,
    validation_recovery_cooldown_ms: VALIDATION_RECOVERY_COOLDOWN_MS,
    validation_allowed_source_prefixes: [...VALIDATION_ALLOWED_SOURCE_PREFIXES],
    persistence_enabled: supabaseEnabled(),
    persistence_timeout_ms: SUPABASE_IO_TIMEOUT_MS,
    persistence_last_error: lastPersistenceError ? String(lastPersistenceError?.message || lastPersistenceError) : null,
    restore_healthy: initialized && !lastRestoreError,
    single_instance_rest_healthy: instanceSafety.healthy,
    render_instance_count: instanceSafety.instance_count,
    render_instance_safety_error: instanceSafety.error || null,
    render_instance_safety_checked_at: iso(instanceSafety.checked_at),
    render_instance_id_present: Boolean(RENDER_INSTANCE_ID),
    multi_instance_rest_supported: false,
    render_discovery_api: 'dns_lookup_os_resolver',
    render_discovery_timeout_ms: INSTANCE_DISCOVERY_TIMEOUT_MS,
    render_instance_startup_rest_grace_ms: INSTANCE_STARTUP_REST_GRACE_MS,
    render_instance_startup_rest_grace_remaining_ms: RENDER_RUNTIME
      ? Math.max(0, INSTANCE_STARTUP_REST_GRACE_MS - (Date.now() - PROCESS_STARTED_AT))
      : 0,
    instance_check_forced_before_every_rest: true,
    restore_last_error: lastRestoreError ? String(lastRestoreError?.message || lastRestoreError) : null,
    initialized,
    min_request_gap_ms: MIN_REQUEST_GAP_MS,
    max_pending_requests: MAX_PENDING_REQUESTS,
    default_max_queue_wait_ms: DEFAULT_MAX_QUEUE_WAIT_MS,
    queue_depth: pendingRequests,
    active_request: activeRequest,
    migration_quarantine_until: iso(INITIAL_QUARANTINE_UNTIL_MS),
    probe_endpoint: '/api/binance-contract-rest-probe',
    validation_reset_endpoint: '/api/binance-contract-validation-reset',
    ...stats,
    initialized_at: stats.initialized_at ? iso(stats.initialized_at) : null,
    last_success_at: stats.last_success_at ? iso(stats.last_success_at) : null,
  };
}

// Restore immediately in the parent REST process, but never let the WS-only child
// become a second persistence writer or a second Binance REST caller.
if (!PROCESS_REST_DISABLED) ensureBinanceRestGuardInitialized().catch(() => {});

export const _test = {
  parseBinanceBanUntil,
  parseRetryAfterSeconds,
  toFiniteMs,
  activeState,
  probeGateActive,
  validationOnlyBlocks,
  forceReadyForQueueTest() {
    if (process.env.NODE_ENV !== 'test') throw new Error('test_hook_disabled');
    initialized = true;
    initPromise = null;
    lastRestoreError = null;
    lastPersistenceError = null;
    instanceSafety = { checked_at: Date.now(), healthy: true, instance_count: 1, error: '' };
    requestChain = Promise.resolve();
    pendingRequests = 0;
    activeRequest = false;
    lastRequestStartedAt = 0;
    state = {
      ...state,
      until: 0,
      status: 200,
      reason: 'test_ready',
      source: 'test',
      error: '',
      updated_at: Date.now(),
      probe_required: false,
      operating_mode: 'normal_guarded',
      validation_session_hash: '',
      validation_budget: 0,
      validation_inflight: null,
      validation_control_epoch: currentControlEpoch(),
      schema_version: GUARD_SCHEMA_VERSION,
    };
  },
  forceProbeReadyForTest() {
    if (process.env.NODE_ENV !== 'test') throw new Error('test_hook_disabled');
    initialized = true;
    initPromise = null;
    lastRestoreError = null;
    lastPersistenceError = null;
    instanceSafety = { checked_at: Date.now(), healthy: true, instance_count: 1, error: '' };
    requestChain = Promise.resolve();
    pendingRequests = 0;
    activeRequest = false;
    lastRequestStartedAt = 0;
    state = {
      ...state,
      until: 0,
      status: 200,
      reason: 'test_probe_ready',
      source: 'test',
      error: '',
      updated_at: Date.now(),
      probe_required: true,
      operating_mode: 'probe_required',
      validation_session_hash: '',
      validation_budget: 0,
      validation_created_at: 0,
      validation_expires_at: 0,
      validation_next_index: 0,
      validation_inflight: null,
      validation_control_epoch: currentControlEpoch(),
      validation_admin_key_fingerprint: VALIDATION_ADMIN_KEY_FINGERPRINT,
      schema_version: GUARD_SCHEMA_VERSION,
    };
  },
  setPersistenceError(message = 'test_persistence_error') {
    if (process.env.NODE_ENV !== 'test') throw new Error('test_hook_disabled');
    lastPersistenceError = new Error(String(message));
  },
  clearPersistenceError() {
    if (process.env.NODE_ENV !== 'test') throw new Error('test_hook_disabled');
    lastPersistenceError = null;
  },
  queueState() {
    if (process.env.NODE_ENV !== 'test') throw new Error('test_hook_disabled');
    return { pendingRequests, activeRequest, queue_release_on_guard_error: stats.queue_release_on_guard_error };
  },
};
