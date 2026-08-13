import { requestIsolatedJson } from './collector-isolation.mjs';

const VERSION = '650.8.15.119';
const POLL_MS = Math.max(500, Number(process.env.KAKA_MARKET_LIGHT_BRIDGE_POLL_MS || 1_500));
const STALE_MS = Math.max(5_000, Number(process.env.KAKA_MARKET_LIGHT_BRIDGE_STALE_MS || 10_000));

let timer = null;
let running = false;
let lastSuccessAt = 0;
let lastAttemptAt = 0;
let lastError = '';
let remoteHealth = null;
const providers = new Map();

async function poll() {
  if (running) return;
  running = true;
  lastAttemptAt = Date.now();
  try {
    const payload = await requestIsolatedJson('market-light', '/_isolated/state', 8_000);
    if (!payload?.ok || !payload?.health) throw new Error('market_light_bridge_invalid_payload');
    remoteHealth = payload.health;
    providers.clear();
    for (const [key, value] of Object.entries(payload.providers || {})) {
      providers.set(key, value);
    }
    lastSuccessAt = Date.now();
    lastError = '';
  } catch (error) {
    lastError = String(error?.message || error).slice(0, 400);
  } finally {
    running = false;
  }
}

export function startMarketLightBridge() {
  if (timer) return;
  poll().catch(() => {});
  timer = setInterval(() => poll().catch(() => {}), POLL_MS);
  timer.unref?.();
}

export function getMarketLightInternalSnapshot({ market = '', provider = '' } = {}) {
  const key = `${String(market || '').toLowerCase()}:${String(provider || '').toLowerCase()}`;
  const payload = providers.get(key);
  if (payload) {
    return {
      ...payload,
      isolated_bridge: true,
      isolated_bridge_version: VERSION,
      isolated_bridge_age_ms: lastSuccessAt > 0 ? Date.now() - lastSuccessAt : null,
    };
  }
  return {
    ok: false,
    version: remoteHealth?.version || null,
    error: 'isolated_market_light_bridge_not_ready',
    market_type: String(market || '').toLowerCase(),
    provider: String(provider || '').toLowerCase(),
    row_count: 0,
    directory_count: 0,
    stale: true,
    last_error: lastError || 'bridge_not_ready',
    rows: [],
    reads_scale_with_users: false,
    isolated_bridge: true,
  };
}

export function getMarketLightSnapshotHealth() {
  const age = lastSuccessAt > 0 ? Date.now() - lastSuccessAt : null;
  const bridgeFresh = age != null && age <= STALE_MS;
  return {
    ...(remoteHealth || {
      ok: false,
      version: null,
      provider_coverage: {},
      last_error: lastError || 'bridge_not_ready',
    }),
    isolated_bridge: true,
    isolated_bridge_version: VERSION,
    isolated_bridge_poll_ms: POLL_MS,
    isolated_bridge_stale_ms: STALE_MS,
    isolated_bridge_age_ms: age,
    isolated_bridge_fresh: bridgeFresh,
    isolated_bridge_last_attempt_at: lastAttemptAt ? new Date(lastAttemptAt).toISOString() : null,
    isolated_bridge_last_success_at: lastSuccessAt ? new Date(lastSuccessAt).toISOString() : null,
    isolated_bridge_last_error: lastError,
    parent_starts_market_light_scanner: false,
    reads_scale_with_users: false,
  };
}
