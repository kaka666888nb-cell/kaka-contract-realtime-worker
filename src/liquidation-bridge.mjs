import { requestIsolatedJson } from './collector-isolation.mjs';

const VERSION = '650.8.15.119';
const POLL_MS = Math.max(500, Number(process.env.KAKA_LIQUIDATION_BRIDGE_POLL_MS || 1_500));
const STALE_MS = Math.max(5_000, Number(process.env.KAKA_LIQUIDATION_BRIDGE_STALE_MS || 10_000));

let timer = null;
let running = false;
let lastSuccessAt = 0;
let lastAttemptAt = 0;
let lastError = '';
let persistence = null;
let wsHealth = null;

async function poll() {
  if (running) return;
  running = true;
  lastAttemptAt = Date.now();
  try {
    const payload = await requestIsolatedJson('liquidation', '/_isolated/state', 8_000);
    if (!payload?.ok || !payload?.liquidation_persistence) {
      throw new Error('liquidation_bridge_invalid_payload');
    }
    persistence = payload.liquidation_persistence;
    wsHealth = payload.binance_liquidation_ws || null;
    lastSuccessAt = Date.now();
    lastError = '';
  } catch (error) {
    lastError = String(error?.message || error).slice(0, 400);
  } finally {
    running = false;
  }
}

export function startLiquidationBridge() {
  if (timer) return;
  poll().catch(() => {});
  timer = setInterval(() => poll().catch(() => {}), POLL_MS);
  timer.unref?.();
}

function metadata() {
  const age = lastSuccessAt > 0 ? Date.now() - lastSuccessAt : null;
  return {
    isolated_bridge: true,
    isolated_bridge_version: VERSION,
    isolated_bridge_poll_ms: POLL_MS,
    isolated_bridge_stale_ms: STALE_MS,
    isolated_bridge_age_ms: age,
    isolated_bridge_fresh: age != null && age <= STALE_MS,
    isolated_bridge_last_attempt_at: lastAttemptAt ? new Date(lastAttemptAt).toISOString() : null,
    isolated_bridge_last_success_at: lastSuccessAt ? new Date(lastSuccessAt).toISOString() : null,
    isolated_bridge_last_error: lastError,
    parent_loads_liquidation_collector_module: false,
    reads_scale_with_users: false,
  };
}

export function getContractLiquidationPersistenceHealth() {
  return {
    ...(persistence || { ok: false, version: null }),
    ...metadata(),
  };
}

export function getBinanceLiquidationWsHealth() {
  return {
    ...(wsHealth || { ok: false, version: null }),
    ...metadata(),
  };
}
