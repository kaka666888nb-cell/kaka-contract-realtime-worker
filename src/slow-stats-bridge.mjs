import { requestIsolatedJson } from './collector-isolation.mjs';

const VERSION = '650.8.15.124';
const POLL_MS = Math.max(1_000, Number(process.env.KAKA_SLOW_STATS_BRIDGE_POLL_MS || 5_000));
const STALE_MS = Math.max(10_000, Number(process.env.KAKA_SLOW_STATS_BRIDGE_STALE_MS || 30_000));

let timer = null;
let running = false;
let lastSuccessAt = 0;
let lastAttemptAt = 0;
let lastError = '';
const state = { binance: null, bitget: null, gate: null, okx: null, bybit: null };

async function poll() {
  if (running) return;
  running = true;
  lastAttemptAt = Date.now();
  try {
    const payload = await requestIsolatedJson('slow-stats', '/_isolated/state', 8_000);
    if (!payload?.ok || !payload?.binance_advanced || !payload?.bitget_advanced || !payload?.gate_advanced || !payload?.okx_advanced || !payload?.bybit_advanced) {
      throw new Error('slow_stats_bridge_invalid_payload');
    }
    state.binance = payload.binance_advanced;
    state.bitget = payload.bitget_advanced;
    state.gate = payload.gate_advanced;
    state.okx = payload.okx_advanced;
    state.bybit = payload.bybit_advanced;
    lastSuccessAt = Date.now();
    lastError = '';
  } catch (error) {
    lastError = String(error?.message || error).slice(0, 400);
  } finally {
    running = false;
  }
}

export function startSlowStatsBridge() {
  if (timer) return;
  poll().catch(() => {});
  timer = setInterval(() => poll().catch(() => {}), POLL_MS);
  timer.unref?.();
}

function metadata() {
  const age = lastSuccessAt > 0 ? Date.now() - lastSuccessAt : null;
  return {
    isolated_bridge: true,
    isolated_bridge_role: 'slow-stats',
    isolated_bridge_version: VERSION,
    isolated_bridge_poll_ms: POLL_MS,
    isolated_bridge_stale_ms: STALE_MS,
    isolated_bridge_age_ms: age,
    isolated_bridge_fresh: age != null && age <= STALE_MS,
    isolated_bridge_last_attempt_at: lastAttemptAt ? new Date(lastAttemptAt).toISOString() : null,
    isolated_bridge_last_success_at: lastSuccessAt ? new Date(lastSuccessAt).toISOString() : null,
    isolated_bridge_last_error: lastError,
    parent_loads_slow_stats_modules: false,
    reads_scale_with_users: false,
  };
}

function health(value) {
  return { ...(value || { ok: false, version: null, ready: false }), ...metadata() };
}

export function getBinanceAdvancedStatsHealth() { return health(state.binance); }
export function getBitgetAdvancedStatsHealth() { return health(state.bitget); }
export function getGateAdvancedStatsHealth() { return health(state.gate); }
export function getOkxAdvancedStatsHealth() { return health(state.okx); }
export function getBybitAdvancedStatsHealth() { return health(state.bybit); }

export function getSlowStatsBridgeHealth() {
  const meta = metadata();
  return {
    ok: true,
    version: VERSION,
    ready: meta.isolated_bridge_fresh === true && state.binance != null && state.bitget != null && state.gate != null && state.okx != null && state.bybit != null,
    binance_ready: state.binance?.ready === true,
    bitget_ready: state.bitget?.ready === true,
    gate_ready: state.gate?.ready === true,
    okx_ready: state.okx?.ready === true,
    bybit_ready: state.bybit?.ready === true,
    ...meta,
  };
}
