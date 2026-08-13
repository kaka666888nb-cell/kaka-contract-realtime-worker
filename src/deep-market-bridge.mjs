import { requestIsolatedJson } from './collector-isolation.mjs';

const VERSION = '650.8.15.124';
const POLL_MS = Math.max(1_000, Number(process.env.KAKA_DEEP_MARKET_BRIDGE_POLL_MS || 3_000));
const STALE_MS = Math.max(8_000, Number(process.env.KAKA_DEEP_MARKET_BRIDGE_STALE_MS || 18_000));
const CONSUMER_ROLE = String(process.env.KAKA_ISOLATED_COLLECTOR_ROLE || 'parent').trim() || 'parent';
const STATE_SCOPE = CONSUMER_ROLE === 'slow-stats' ? 'slow-stats' : 'parent';

let timer = null;
let running = false;
let lastSuccessAt = 0;
let lastAttemptAt = 0;
let lastError = '';
let focusHealth = null;
let focusSnapshot = null;
let flowHealth = null;
let deepHealth = null;

async function poll() {
  if (running) return;
  running = true;
  lastAttemptAt = Date.now();
  try {
    const payload = await requestIsolatedJson(
      'deep-market',
      `/_isolated/state?scope=${encodeURIComponent(STATE_SCOPE)}`,
      8_000,
    );
    if (!payload?.ok || !payload?.focus_health || !payload?.focus_snapshot) {
      throw new Error('deep_market_bridge_invalid_focus_payload');
    }
    focusHealth = payload.focus_health;
    focusSnapshot = payload.focus_snapshot;
    if (STATE_SCOPE === 'parent') {
      if (!payload?.flow_health || !payload?.deep_health) {
        throw new Error('deep_market_bridge_invalid_parent_payload');
      }
      flowHealth = payload.flow_health;
      deepHealth = payload.deep_health;
    }
    lastSuccessAt = Date.now();
    lastError = '';
  } catch (error) {
    lastError = String(error?.message || error).slice(0, 400);
  } finally {
    running = false;
  }
}

export function startDeepMarketBridge() {
  if (timer) return;
  poll().catch(() => {});
  timer = setInterval(() => poll().catch(() => {}), POLL_MS);
  timer.unref?.();
}

function metadata() {
  const age = lastSuccessAt > 0 ? Date.now() - lastSuccessAt : null;
  return {
    isolated_bridge: true,
    isolated_bridge_role: 'deep-market',
    isolated_bridge_consumer_role: CONSUMER_ROLE,
    isolated_bridge_state_scope: STATE_SCOPE,
    isolated_bridge_version: VERSION,
    isolated_bridge_poll_ms: POLL_MS,
    isolated_bridge_stale_ms: STALE_MS,
    isolated_bridge_age_ms: age,
    isolated_bridge_fresh: age != null && age <= STALE_MS,
    isolated_bridge_last_attempt_at: lastAttemptAt ? new Date(lastAttemptAt).toISOString() : null,
    isolated_bridge_last_success_at: lastSuccessAt ? new Date(lastSuccessAt).toISOString() : null,
    isolated_bridge_last_error: lastError,
    parent_starts_focus_pool_scanner: false,
    parent_starts_contract_flow_scanner: false,
    parent_starts_deep_shared_scanner: false,
    reads_scale_with_users: false,
  };
}

export function getContractFocusPoolInternalSnapshot() {
  return focusSnapshot
    ? { ...focusSnapshot, ...metadata() }
    : {
        ok: false,
        version: focusHealth?.version || null,
        ready: false,
        row_count: 0,
        rows: [],
        error: lastError || 'deep_market_focus_bridge_not_ready',
        ...metadata(),
      };
}

export function getContractFocusPoolHealth() {
  return { ...(focusHealth || { ok: false, version: null, ready: false }), ...metadata() };
}

export function getContractFlowHealth() {
  return {
    ...(flowHealth || { ok: false, version: null }),
    ...metadata(),
    unavailable_in_slow_stats_scope: STATE_SCOPE !== 'parent',
  };
}

export function getContractDeepSharedHealth() {
  return {
    ...(deepHealth || { ok: false, version: null, ready: false }),
    ...metadata(),
    unavailable_in_slow_stats_scope: STATE_SCOPE !== 'parent',
  };
}

export function getDeepMarketBridgeHealth() {
  const meta = metadata();
  return {
    ok: true,
    version: VERSION,
    ready:
      meta.isolated_bridge_fresh === true &&
      focusHealth != null &&
      focusSnapshot != null &&
      (STATE_SCOPE !== 'parent' || (flowHealth != null && deepHealth != null)),
    focus_ready: focusHealth?.ready === true,
    focus_rows: Number(focusHealth?.row_count || 0),
    deep_ready: STATE_SCOPE === 'parent' ? deepHealth?.ready === true : null,
    deep_rows: STATE_SCOPE === 'parent' ? Number(deepHealth?.row_count || 0) : null,
    scoped_payload_reduction: STATE_SCOPE === 'slow-stats',
    ...meta,
  };
}
