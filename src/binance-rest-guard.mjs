const PROVIDER = 'binance';
const MARKET_TYPE = 'contract';
const SNAPSHOT_TABLE = 'app_market_backend_snapshots';
const SNAPSHOT_TYPE = 'klines';
const SNAPSHOT_KEY = 'REST_GUARD:BINANCE_CONTRACT';
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');

// Step650.8 migration quarantine:
// Binance returned an exact auto-ban deadline of 1784319886570 ms
// (2026-07-17T20:24:46.570Z). Keep an extra 15-minute migration guard so
// deploying/restarting this worker cannot immediately hit REST again.
const OBSERVED_BAN_UNTIL_MS = 1_784_319_886_570;
const INITIAL_QUARANTINE_UNTIL_MS = OBSERVED_BAN_UNTIL_MS + 15 * 60_000;
const BAN_SAFETY_MS = 90_000;
const RESTRICTED_FALLBACK_MS = 30 * 60_000;
const MIN_REQUEST_GAP_MS = 5_000;

let initialized = false;
let initPromise = null;
let requestChain = Promise.resolve();
let lastRequestStartedAt = 0;
let state = {
  until: INITIAL_QUARANTINE_UNTIL_MS,
  status: 418,
  reason: 'observed_binance_ip_ban_migration_quarantine',
  source: 'step650.8_migration_guard',
  error: '',
  parsed_ban_until: OBSERVED_BAN_UNTIL_MS,
  retry_after_seconds: null,
  updated_at: Date.now(),
};

const stats = {
  initialized_at: 0,
  restored_from_snapshot: 0,
  snapshot_persist_success: 0,
  snapshot_persist_errors: 0,
  requests_started: 0,
  request_slot_waits: 0,
  blocked_requests: 0,
  restricted_responses: 0,
  ban_until_parsed: 0,
  retry_after_parsed: 0,
  successes: 0,
  last_success_at: 0,
  last_success_source: '',
  last_used_weight_1m: null,
  last_error: '',
};

function iso(value) {
  return value ? new Date(value).toISOString() : null;
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

export function parseBinanceBanUntil(message) {
  const match = String(message || '').match(/banned\s+until\s+(\d{12,16})/i);
  return match ? toFiniteMs(match[1]) : null;
}

function activeState() {
  if (Number(state?.until || 0) > Date.now()) return state;
  return null;
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
  const restoredUntil = toFiniteMs(restored.until);
  if (restoredUntil && restoredUntil > Number(state.until || 0)) {
    state = {
      until: restoredUntil,
      status: Number(restored.status || 418),
      reason: String(restored.reason || 'persisted_binance_rest_guard'),
      source: String(restored.source || record?.source || 'persisted_binance_rest_guard'),
      error: String(restored.error || ''),
      parsed_ban_until: toFiniteMs(restored.parsed_ban_until),
      retry_after_seconds: restored.retry_after_seconds == null || String(restored.retry_after_seconds).trim() === ''
        ? null
        : (Number.isFinite(Number(restored.retry_after_seconds)) && Number(restored.retry_after_seconds) > 0
            ? Number(restored.retry_after_seconds)
            : null),
      updated_at: toFiniteMs(restored.updated_at) || Date.now(),
    };
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
    source_time: iso(Number(state.until || now)),
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

function persistSnapshotInBackground() {
  persistSnapshot().catch((error) => {
    stats.snapshot_persist_errors += 1;
    stats.last_error = String(error?.message || error);
  });
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
    // Persist the migration quarantine so a later process restart cannot forget it.
    if (activeState()) persistSnapshotInBackground();
  })().finally(() => { initPromise = null; });
  return initPromise;
}

export function isBinanceRestBlocked() {
  return activeState();
}

export async function acquireBinanceRestRequestSlot() {
  await ensureBinanceRestGuardInitialized();
  let release;
  const previous = requestChain;
  requestChain = new Promise((resolve) => { release = resolve; });
  await previous;

  const blocked = activeState();
  if (blocked) {
    stats.blocked_requests += 1;
    release();
    const error = new Error(`binance_rest_blocked_until:${blocked.until}`);
    error.status = 418;
    error.binanceRestBlocked = true;
    error.guardState = blocked;
    throw error;
  }

  const waitMs = Math.max(0, MIN_REQUEST_GAP_MS - (Date.now() - lastRequestStartedAt));
  if (waitMs > 0) {
    stats.request_slot_waits += 1;
    await new Promise((resolve) => setTimeout(resolve, waitMs));
  }

  // The guard may have been updated while this request was waiting.
  const blockedAfterWait = activeState();
  if (blockedAfterWait) {
    stats.blocked_requests += 1;
    release();
    const error = new Error(`binance_rest_blocked_until:${blockedAfterWait.until}`);
    error.status = 418;
    error.binanceRestBlocked = true;
    error.guardState = blockedAfterWait;
    throw error;
  }

  lastRequestStartedAt = Date.now();
  stats.requests_started += 1;
  return release;
}

export function markBinanceRestRestricted({ status = 0, message = '', source = '', retryAfterSeconds = null } = {}) {
  const parsedBanUntil = parseBinanceBanUntil(message);
  if (parsedBanUntil) stats.ban_until_parsed += 1;
  const parsedRetrySeconds = retryAfterSeconds == null || String(retryAfterSeconds).trim() === ''
    ? null
    : Number(retryAfterSeconds);
  const retrySeconds = Number.isFinite(parsedRetrySeconds) && parsedRetrySeconds > 0
    ? parsedRetrySeconds
    : null;
  const retryUntil = retrySeconds != null
    ? Date.now() + Math.ceil(retrySeconds * 1000)
    : 0;
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
  };
  stats.restricted_responses += 1;
  stats.last_error = String(message || 'binance_rest_restricted');
  persistSnapshotInBackground();
  return state;
}

export function markBinanceRestSuccess({ source = '', usedWeight1m = null } = {}) {
  stats.successes += 1;
  stats.last_success_at = Date.now();
  stats.last_success_source = String(source || '');
  const weight = Number(usedWeight1m);
  if (Number.isFinite(weight)) stats.last_used_weight_1m = weight;
  stats.last_error = '';
}

export function getBinanceRestGuardHealth() {
  const active = activeState();
  return {
    active: Boolean(active),
    next_allowed_at: active ? iso(active.until) : null,
    status: active?.status || null,
    reason: active?.reason || null,
    source: active?.source || null,
    parsed_ban_until: active?.parsed_ban_until ? iso(active.parsed_ban_until) : null,
    retry_after_seconds: active?.retry_after_seconds ?? null,
    last_error: active?.error || null,
    persistence_enabled: supabaseEnabled(),
    initialized,
    min_request_gap_ms: MIN_REQUEST_GAP_MS,
    migration_quarantine_until: iso(INITIAL_QUARANTINE_UNTIL_MS),
    ...stats,
    initialized_at: stats.initialized_at ? iso(stats.initialized_at) : null,
    last_success_at: stats.last_success_at ? iso(stats.last_success_at) : null,
  };
}

// Start restoration immediately, but never block module loading or server startup.
ensureBinanceRestGuardInitialized().catch(() => {});

export const _test = {
  parseBinanceBanUntil,
  toFiniteMs,
};
