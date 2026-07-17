const PROVIDER = 'binance';
const MARKET_TYPE = 'contract';
const SNAPSHOT_TABLE = 'app_market_backend_snapshots';
const SNAPSHOT_TYPE = 'klines';
const SNAPSHOT_KEY = 'REST_GUARD:BINANCE_CONTRACT';
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');

// The last observed Binance USD-M Futures IP ban ended at this exact UTC time.
// Step650.8.1 keeps the existing 15-minute migration quarantine and then requires
// one explicit low-weight /fapi/v1/ping probe before any normal Binance contract
// REST request can leave this process. This prevents an App page, background metric
// refresh, or another user from becoming the first post-ban caller.
const OBSERVED_BAN_UNTIL_MS = 1_784_319_886_570;
const INITIAL_QUARANTINE_UNTIL_MS = OBSERVED_BAN_UNTIL_MS + 15 * 60_000;
const BAN_SAFETY_MS = 90_000;
const RESTRICTED_FALLBACK_MS = 30 * 60_000;

// Conservative production pacing. Binance's published limits are much higher, but
// this worker has one shared cloud egress IP and recently accumulated repeat bans.
const MIN_REQUEST_GAP_MS = 10_000;
const MAX_PENDING_REQUESTS = 6;
const DEFAULT_MAX_QUEUE_WAIT_MS = 25_000;
const PROBE_TIMEOUT_MS = 6_000;
const PROBE_URL = 'https://fapi.binance.com/fapi/v1/ping';
const MIGRATION_STATE_UPDATED_AT_MS = OBSERVED_BAN_UNTIL_MS;
const VALIDATION_ALLOWED_SOURCE_PREFIXES = ['kline_bridge:'];

let initialized = false;
let initPromise = null;
let requestChain = Promise.resolve();
let lastRequestStartedAt = 0;
let pendingRequests = 0;
let activeRequest = false;
let persistPromise = Promise.resolve();
let probePromise = null;

let state = {
  until: INITIAL_QUARANTINE_UNTIL_MS,
  status: 418,
  reason: 'observed_binance_ip_ban_migration_quarantine',
  source: 'step650.8.1_migration_guard',
  error: '',
  parsed_ban_until: OBSERVED_BAN_UNTIL_MS,
  retry_after_seconds: null,
  updated_at: MIGRATION_STATE_UPDATED_AT_MS,
  probe_required: true,
  probe_passed_at: 0,
  probe_source: '',
  operating_mode: 'probe_required',
};

const stats = {
  initialized_at: 0,
  restored_from_snapshot: 0,
  snapshot_persist_success: 0,
  snapshot_persist_errors: 0,
  requests_started: 0,
  request_slot_waits: 0,
  blocked_requests: 0,
  probe_required_blocks: 0,
  queue_rejections: 0,
  queue_timeouts: 0,
  max_queue_depth: 0,
  restricted_responses: 0,
  ban_until_parsed: 0,
  retry_after_parsed: 0,
  successes: 0,
  probes_started: 0,
  probes_succeeded: 0,
  probes_restricted: 0,
  probes_failed: 0,
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

function supabaseEnabled() {
  return Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
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

function activeState() {
  if (Number(state?.until || 0) > Date.now()) return state;
  return null;
}

function probeGateActive() {
  return !activeState() && state?.probe_required === true;
}

function validationOnlyBlocks(source = '') {
  if (activeState() || state?.probe_required === true) return false;
  if (state?.operating_mode !== 'validation_only') return false;
  const normalized = String(source || '');
  return !VALIDATION_ALLOWED_SOURCE_PREFIXES.some((prefix) => normalized.startsWith(prefix));
}

function normalizedRestoredState(restored, record = null) {
  const restoredUntil = toFiniteMs(restored?.until) || 0;
  const probePassedAt = toFiniteMs(restored?.probe_passed_at) || 0;
  return {
    until: restoredUntil,
    status: Number(restored?.status || 418),
    reason: String(restored?.reason || 'persisted_binance_rest_guard'),
    source: String(restored?.source || record?.source || 'persisted_binance_rest_guard'),
    error: String(restored?.error || ''),
    parsed_ban_until: toFiniteMs(restored?.parsed_ban_until),
    retry_after_seconds: parseRetryAfterSeconds(restored?.retry_after_seconds),
    updated_at: toFiniteMs(restored?.updated_at) || Date.now(),
    probe_required: restored?.probe_required !== false,
    probe_passed_at: probePassedAt,
    probe_source: String(restored?.probe_source || ''),
    operating_mode: String(restored?.operating_mode || (restored?.probe_required === false ? 'validation_only' : 'probe_required')),
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
    },
  );
  if (!response.ok) throw new Error(`binance_rest_guard_persist_${response.status}`);
  stats.snapshot_persist_success += 1;
}

function queuePersistence() {
  persistPromise = persistPromise
    .catch(() => {})
    .then(() => persistSnapshot())
    .catch((error) => {
      stats.snapshot_persist_errors += 1;
      stats.last_error = String(error?.message || error);
    });
  return persistPromise;
}

export async function flushBinanceRestGuardPersistence() {
  await persistPromise.catch(() => {});
}

export async function ensureBinanceRestGuardInitialized() {
  if (initialized) return;
  if (initPromise) return initPromise;
  initPromise = (async () => {
    try {
      await restoreSnapshot();
    } catch (error) {
      stats.last_error = String(error?.message || error);
    }
    initialized = true;
    stats.initialized_at = Date.now();
    if (activeState() || state.probe_required === true) queuePersistence();
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
  error.status = 418;
  error.code = code;
  Object.assign(error, extra);
  return error;
}

export async function acquireBinanceRestRequestSlot({
  source = 'unknown_binance_rest_caller',
  probe = false,
  maxQueueWaitMs = DEFAULT_MAX_QUEUE_WAIT_MS,
} = {}) {
  await ensureBinanceRestGuardInitialized();

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
    throw guardError('binance_rest_validation_only', 'BINANCE_REST_VALIDATION_ONLY', {
      binanceRestValidationOnly: true,
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
    await Promise.race([
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
    ]);
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
    releaseCurrent();
    throw guardError('binance_rest_validation_only', 'BINANCE_REST_VALIDATION_ONLY', {
      binanceRestValidationOnly: true,
      guardState: state,
      source,
    });
  }

  const waitMs = Math.max(0, MIN_REQUEST_GAP_MS - (Date.now() - lastRequestStartedAt));
  if (waitMs > 0) {
    stats.request_slot_waits += 1;
    await sleep(waitMs);
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
    releaseCurrent();
    throw guardError('binance_rest_validation_only', 'BINANCE_REST_VALIDATION_ONLY', {
      binanceRestValidationOnly: true,
      guardState: state,
      source,
    });
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
  if (authorizeProbe) {
    state = {
      ...state,
      until: 0,
      status: 200,
      reason: 'post_ban_probe_passed',
      source: String(source || 'binance_rest_probe'),
      error: '',
      parsed_ban_until: null,
      retry_after_seconds: null,
      updated_at: Date.now(),
      probe_required: false,
      probe_passed_at: Date.now(),
      probe_source: String(source || 'binance_rest_probe'),
      operating_mode: 'validation_only',
    };
    queuePersistence();
  }
}

async function runBinanceRestProbeOnce() {
  await ensureBinanceRestGuardInitialized();
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
  const release = await acquireBinanceRestRequestSlot({
    source: 'post_ban_probe',
    probe: true,
    maxQueueWaitMs: 5_000,
  });
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

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), PROBE_TIMEOUT_MS);
  timer.unref?.();
  try {
    const response = await fetch(PROBE_URL, {
      signal: controller.signal,
      headers: {
        accept: 'application/json',
        'user-agent': 'KakaWeb3-Binance-Rest-Probe/650.8.1',
      },
    });
    const bodyText = await response.text();
    if (!response.ok) {
      const message = `${response.status} ${response.statusText} ${bodyText.slice(0, 360)}`.trim();
      if ([418, 429, 451].includes(response.status) || /too many requests|banned until|restricted/i.test(bodyText)) {
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
      }
      const error = new Error(message);
      error.status = response.status;
      throw error;
    }

    markBinanceRestSuccess({
      source: 'post_ban_probe',
      usedWeight1m: response.headers.get('x-mbx-used-weight-1m'),
      authorizeProbe: true,
    });
    stats.probes_succeeded += 1;
    await flushBinanceRestGuardPersistence();
    return {
      ok: true,
      skipped: false,
      status: response.status,
      used_weight_1m: response.headers.get('x-mbx-used-weight-1m'),
      guard: getBinanceRestGuardHealth(),
    };
  } catch (error) {
    if (!Number(error?.status || 0) && error?.name === 'AbortError') {
      stats.probes_failed += 1;
      stats.last_error = 'binance_rest_probe_timeout';
    }
    throw error;
  } finally {
    clearTimeout(timer);
    release();
  }
}


export function runBinanceRestProbe() {
  if (probePromise) return probePromise;
  probePromise = runBinanceRestProbeOnce().finally(() => {
    probePromise = null;
  });
  return probePromise;
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
    validation_allowed_source_prefixes: [...VALIDATION_ALLOWED_SOURCE_PREFIXES],
    persistence_enabled: supabaseEnabled(),
    initialized,
    min_request_gap_ms: MIN_REQUEST_GAP_MS,
    max_pending_requests: MAX_PENDING_REQUESTS,
    default_max_queue_wait_ms: DEFAULT_MAX_QUEUE_WAIT_MS,
    queue_depth: pendingRequests,
    active_request: activeRequest,
    migration_quarantine_until: iso(INITIAL_QUARANTINE_UNTIL_MS),
    probe_endpoint: '/api/binance-contract-rest-probe',
    ...stats,
    initialized_at: stats.initialized_at ? iso(stats.initialized_at) : null,
    last_success_at: stats.last_success_at ? iso(stats.last_success_at) : null,
  };
}

// Restore immediately, but never block module loading or server startup.
ensureBinanceRestGuardInitialized().catch(() => {});

export const _test = {
  parseBinanceBanUntil,
  parseRetryAfterSeconds,
  toFiniteMs,
  activeState,
  probeGateActive,
  validationOnlyBlocks,
};
