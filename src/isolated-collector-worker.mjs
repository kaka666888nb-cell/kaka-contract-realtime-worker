import http from 'node:http';
import { performance } from 'node:perf_hooks';
import { isMainThread, threadId, workerData } from 'node:worker_threads';
import { installProviderGovernorFetch, getProviderGovernorHealth } from './provider-request-governor.mjs';
import { projectMarketLightSnapshot, scopeTargets } from './market-light-bridge-projection.mjs';

const ROLE = String(workerData?.role || process.env.KAKA_ISOLATED_COLLECTOR_ROLE || '').trim();
const PORT = Number(workerData?.port || process.env.KAKA_ISOLATED_COLLECTOR_PORT || 0);
process.env.KAKA_ISOLATED_COLLECTOR_ROLE = ROLE;
process.env.KAKA_ISOLATED_COLLECTOR_PORT = String(PORT);
if (workerData?.disable_binance_rest === true) process.env.KAKA_DISABLE_BINANCE_REST = '1';
const VERSION = '650.8.15.192.2';

if (!ROLE || !PORT) {
  throw new Error('isolated_collector_role_and_port_required');
}

installProviderGovernorFetch({ role: `isolated-${ROLE}` });

function sendJson(res, status, payload) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': String(body.length),
  });
  res.end(body);
}

let handleRoleRoute = null;
let internalState = null;
let roleVersion = null;
let runtimeProfileExtra = null;

if (ROLE === 'market-light') {
  const module = await import('./market-light-snapshot.mjs');
  module.startMarketLightSnapshotScanner();
  roleVersion = module.getMarketLightSnapshotHealth().version || null;
  handleRoleRoute = module.handleMarketLightSnapshot;
  runtimeProfileExtra = () => {
    const health = module.getMarketLightSnapshotHealth();
    return {
      round: Number(health?.round || 0),
      running: health?.running === true,
      coinbase_messages: Number(health?.coinbase_ticker_batch?.messages || 0),
      coinbase_updates: Number(health?.coinbase_ticker_batch?.ticker_updates || 0),
      binance_spot_mini_messages: Number(health?.binance_spot_ticker_shared_ws?.messages || 0),
      binance_spot_mini_updates: Number(health?.binance_spot_ticker_shared_ws?.accepted_updates || 0),
      binance_spot_book_messages: Number(health?.binance_spot_book_ticker_shared_ws?.messages || 0),
      binance_spot_book_updates: Number(health?.binance_spot_book_ticker_shared_ws?.accepted_updates || 0),
      binance_contract_book_messages: Number(health?.binance_contract_all_book_ticker?.messages || 0),
      binance_contract_book_updates: Number(health?.binance_contract_all_book_ticker?.accepted_updates || 0),
    };
  };
  internalState = (url) => {
    const scope = String(url?.searchParams?.get('scope') || 'parent');
    const health = module.getMarketLightSnapshotHealth();
    const currentRound = Math.max(0, Number(health?.round || 0));
    const ifRound = Math.max(0, Number(url?.searchParams?.get('if_round') || 0));
    const common = {
      ok: true,
      collector_role: ROLE,
      collector_version: VERSION,
      module_version: health?.version || null,
      runtime: isMainThread ? 'child_process' : 'worker_thread',
      pid: process.pid,
      thread_id: isMainThread ? null : threadId,
      ppid: process.ppid,
      uptime_seconds: Math.round(process.uptime()),
      state_scope: scope,
      shared_round: currentRound,
      memory_usage: {
        rss_mb: Math.round(process.memoryUsage().rss / 1048576),
        heap_used_mb: Math.round(process.memoryUsage().heapUsed / 1048576),
      },
      provider_governor: getProviderGovernorHealth(),
      health,
      timestamp_ms: Date.now(),
    };
    // Step1073 R60: the projected provider rows only change when the shared
    // market-light round advances. Let bridges poll freshness without cloning,
    // projecting and serializing thousands of unchanged rows every time.
    if (ifRound > 0 && currentRound > 0 && ifRound === currentRound) {
      return {
        ...common,
        not_modified: true,
        provider_snapshot_count: 0,
        providers: {},
      };
    }
    const providers = {};
    const wanted = scopeTargets(scope);
    for (const [market, provider] of wanted) {
      const full = module.getMarketLightInternalSnapshot({ market, provider });
      providers[`${market}:${provider}`] = projectMarketLightSnapshot(full, { scope, market, provider });
    }
    return {
      ...common,
      not_modified: false,
      provider_snapshot_count: Object.keys(providers).length,
      providers,
    };
  };
} else if (ROLE === 'liquidation') {
  const module = await import('./contract-liquidation.mjs');
  roleVersion = module.getContractLiquidationPersistenceHealth().version || null;
  handleRoleRoute = module.handleContractLiquidation;
  internalState = () => ({
    ok: true,
    collector_role: ROLE,
    collector_version: VERSION,
    module_version: module.getContractLiquidationPersistenceHealth().version || null,
    runtime: 'child_process',
    pid: process.pid,
    thread_id: null,
    ppid: process.ppid,
    uptime_seconds: Math.round(process.uptime()),
    memory_usage: {
      rss_mb: Math.round(process.memoryUsage().rss / 1048576),
      heap_used_mb: Math.round(process.memoryUsage().heapUsed / 1048576),
    },
    provider_governor: getProviderGovernorHealth(),
    liquidation_persistence: module.getContractLiquidationPersistenceHealth(),
    binance_liquidation_ws: module.getBinanceLiquidationWsHealth(),
    timestamp_ms: Date.now(),
  });
} else if (ROLE === 'exchange-assets') {
  const assetMarket = await import('./exchange-asset-market.mjs');
  const assetKline = await import('./exchange-asset-kline.mjs');

  roleVersion = assetMarket.getAssetMarketHealth().version || '650.8.15.170';
  handleRoleRoute = async (req, res, url) => {
    const controller = new AbortController();
    const abortQueuedWork = () => {
      if (!res.writableEnded && !controller.signal.aborted) controller.abort();
    };
    req.once('aborted', abortQueuedWork);
    res.once('close', abortQueuedWork);
    try {
      if (await assetKline.handleAssetKline(req, res, url, controller.signal)) return true;
      if (await assetMarket.handleAssetMarket(req, res, url, controller.signal)) return true;
      return false;
    } finally {
      req.removeListener('aborted', abortQueuedWork);
      res.removeListener('close', abortQueuedWork);
    }
  };
  internalState = () => ({
    ok: true,
    collector_role: ROLE,
    collector_version: VERSION,
    module_version: roleVersion,
    runtime: 'child_process',
    pid: process.pid,
    thread_id: null,
    ppid: process.ppid,
    uptime_seconds: Math.round(process.uptime()),
    memory_usage: {
      rss_mb: Math.round(process.memoryUsage().rss / 1048576),
      heap_used_mb: Math.round(process.memoryUsage().heapUsed / 1048576),
    },
    provider_governor: getProviderGovernorHealth(),
    asset_market: assetMarket.getAssetMarketHealth(),
    asset_klines: assetKline.getAssetKlineHealth(),
    timestamp_ms: Date.now(),
  });
} else if (ROLE === 'onchain-market') {
  const module = await import('./onchain-market.mjs');
  module.startOnchainMarketCollector();
  roleVersion = module.getOnchainMarketHealth().version || null;
  handleRoleRoute = module.handleOnchainMarket;
  internalState = () => ({
    ok: true,
    collector_role: ROLE,
    collector_version: VERSION,
    module_version: module.getOnchainMarketHealth().version || null,
    runtime: 'child_process',
    pid: process.pid,
    thread_id: null,
    ppid: process.ppid,
    uptime_seconds: Math.round(process.uptime()),
    memory_usage: {
      rss_mb: Math.round(process.memoryUsage().rss / 1048576),
      heap_used_mb: Math.round(process.memoryUsage().heapUsed / 1048576),
    },
    provider_governor: getProviderGovernorHealth(),
    onchain_market: module.getOnchainMarketHealth(),
    timestamp_ms: Date.now(),
  });
} else if (ROLE === 'deep-market') {
  const marketBridge = await import('./market-light-bridge.mjs');
  const focusModule = await import('./contract-focus-pool.mjs');
  const flowModule = await import('./contract-flow.mjs');
  const deepModule = await import('./contract-deep-shared.mjs');
  const rpiModule = await import('./contract-rpi-shared.mjs');

  marketBridge.startMarketLightBridge();
  focusModule.startContractFocusPoolScanner();
  flowModule.startContractFlowUniverseScanner();
  deepModule.startContractDeepSharedScanner();
  rpiModule.startContractRpiSharedScanner();

  roleVersion = deepModule.getContractDeepSharedHealth().version || null;
  handleRoleRoute = async (req, res, url) => {
    if (await focusModule.handleContractFocusPool(req, res, url)) return true;
    if (await deepModule.handleContractDeepShared(req, res, url)) return true;
    if (await rpiModule.handleContractRpiShared(req, res, url)) return true;
    if (await flowModule.handleContractFlow(req, res, url)) return true;
    return false;
  };
  internalState = (url) => {
    const scope = String(url?.searchParams?.get('scope') || 'parent');
    const focusHealth = focusModule.getContractFocusPoolHealth();
    const focusSnapshot = focusModule.getContractFocusPoolInternalSnapshot();
    if (scope === 'slow-stats') {
      return {
        ok: true,
        collector_role: ROLE,
        collector_version: VERSION,
        runtime: 'worker_thread',
        pid: process.pid,
        thread_id: threadId,
        state_scope: scope,
        focus_health: focusHealth,
        focus_snapshot: focusSnapshot,
        timestamp_ms: Date.now(),
      };
    }
    return {
      ok: true,
      collector_role: ROLE,
      collector_version: VERSION,
      module_version: deepModule.getContractDeepSharedHealth().version || null,
      runtime: 'worker_thread',
      pid: process.pid,
      thread_id: threadId,
      uptime_seconds: Math.round(process.uptime()),
      state_scope: scope,
      memory_usage: {
        rss_mb: Math.round(process.memoryUsage().rss / 1048576),
        heap_used_mb: Math.round(process.memoryUsage().heapUsed / 1048576),
      },
      provider_governor: getProviderGovernorHealth(),
      market_light_bridge: marketBridge.getMarketLightSnapshotHealth(),
      focus_health: focusHealth,
      focus_snapshot: focusSnapshot,
      flow_health: flowModule.getContractFlowHealth(),
      deep_health: deepModule.getContractDeepSharedHealth(),
      rpi_health: rpiModule.getContractRpiSharedHealth(),
      timestamp_ms: Date.now(),
    };
  };
} else if (ROLE === 'slow-stats') {
  const marketBridge = await import('./market-light-bridge.mjs');
  const deepBridge = await import('./deep-market-bridge.mjs');
  const binance = await import('./binance-advanced-stats.mjs');
  const bitget = await import('./bitget-advanced-stats.mjs');
  const gate = await import('./gate-advanced-stats.mjs');
  const okx = await import('./okx-advanced-stats.mjs');
  const bybit = await import('./bybit-advanced-stats.mjs');
  const derivatives = await import('./derivatives-public.mjs');
  const lifecycle = await import('./history-lifecycle.mjs');

  marketBridge.startMarketLightBridge();
  deepBridge.startDeepMarketBridge();

  binance.startBinanceAdvancedStatsScanner();
  bitget.startBitgetAdvancedStatsScanner();
  gate.startGateAdvancedStatsScanner();
  okx.startOkxAdvancedStatsScanner();
  bybit.startBybitAdvancedStatsScanner();
  derivatives.startDerivativesPublicScanner();
  lifecycle.startHistoryLifecycleMaintainer();

  roleVersion = binance.getBinanceAdvancedStatsHealth().version || null;
  handleRoleRoute = async (req, res, url) => {
    if (await binance.handleBinanceAdvancedStats(req, res, url)) return true;
    if (await bitget.handleBitgetAdvancedStats(req, res, url)) return true;
    if (await gate.handleGateAdvancedStats(req, res, url)) return true;
    if (await okx.handleOkxAdvancedStats(req, res, url)) return true;
    if (await bybit.handleBybitAdvancedStats(req, res, url)) return true;
    if (await derivatives.handleDerivativesPublic(req, res, url)) return true;
    if (await lifecycle.handleHistoryLifecycle(req, res, url)) return true;
    return false;
  };
  internalState = () => ({
    ok: true,
    collector_role: ROLE,
    collector_version: VERSION,
    module_version: binance.getBinanceAdvancedStatsHealth().version || null,
    runtime: 'worker_thread',
    pid: process.pid,
    thread_id: threadId,
    uptime_seconds: Math.round(process.uptime()),
    memory_usage: {
      rss_mb: Math.round(process.memoryUsage().rss / 1048576),
      heap_used_mb: Math.round(process.memoryUsage().heapUsed / 1048576),
    },
    provider_governor: getProviderGovernorHealth(),
    market_light_bridge: marketBridge.getMarketLightSnapshotHealth(),
    deep_market_bridge: deepBridge.getDeepMarketBridgeHealth(),
    binance_advanced: binance.getBinanceAdvancedStatsHealth(),
    bitget_advanced: bitget.getBitgetAdvancedStatsHealth(),
    gate_advanced: gate.getGateAdvancedStatsHealth(),
    okx_advanced: okx.getOkxAdvancedStatsHealth(),
    bybit_advanced: bybit.getBybitAdvancedStatsHealth(),
    derivatives_public: derivatives.getDerivativesPublicHealth(),
    history_lifecycle: lifecycle.getHistoryLifecycleHealth(),
    timestamp_ms: Date.now(),
  });
} else {
  throw new Error(`unsupported_isolated_collector_role:${ROLE}`);
}

// Step1073 V102: temporary bounded role profiler. It observes only this
// isolated collector's event-loop utilization after boot; it does not change
// scanners, intervals, upstream requests, persistence, routing, or cache data.
// Sampling auto-stops after ~3 minutes so normal production logging/egress is
// unchanged outside the diagnostic window.
const RUNTIME_PROFILE_INTERVAL_MS = 5_000;
const RUNTIME_PROFILE_MAX_SAMPLES = 36;
let runtimeProfileSamples = 0;
let runtimeProfilePreviousElu = performance.eventLoopUtilization();
let runtimeProfilePreviousCpu = process.cpuUsage();
let runtimeProfilePreviousAt = performance.now();
let runtimeProfilePreviousExtra = runtimeProfileExtra ? runtimeProfileExtra() : null;
const runtimeProfileTimer = setInterval(() => {
  try {
    const now = performance.now();
    const elu = performance.eventLoopUtilization(runtimeProfilePreviousElu);
    runtimeProfilePreviousElu = performance.eventLoopUtilization();
    const cpu = process.cpuUsage(runtimeProfilePreviousCpu);
    runtimeProfilePreviousCpu = process.cpuUsage();
    const wallUs = Math.max(1, (now - runtimeProfilePreviousAt) * 1000);
    runtimeProfilePreviousAt = now;
    const memory = process.memoryUsage();
    const currentExtra = runtimeProfileExtra ? runtimeProfileExtra() : null;
    const extraDelta = currentExtra && runtimeProfilePreviousExtra
      ? Object.fromEntries(
          Object.entries(currentExtra)
            .filter(([key, value]) =>
              key !== 'running' &&
              key !== 'round' &&
              Number.isFinite(Number(value)) &&
              Number.isFinite(Number(runtimeProfilePreviousExtra?.[key]))
            )
            .map(([key, value]) => [`${key}_delta`, Number(value) - Number(runtimeProfilePreviousExtra[key])])
        )
      : null;
    const roundDelta = currentExtra && runtimeProfilePreviousExtra
      ? Number(currentExtra.round || 0) - Number(runtimeProfilePreviousExtra.round || 0)
      : null;
    runtimeProfilePreviousExtra = currentExtra;
    runtimeProfileSamples += 1;
    console.log('[Step1073 V102 runtime-profiler] ' + JSON.stringify({
      collector_role: ROLE,
      runtime: isMainThread ? 'child_process' : 'worker_thread',
      pid: process.pid,
      thread_id: isMainThread ? null : threadId,
      sample: runtimeProfileSamples,
      interval_ms: RUNTIME_PROFILE_INTERVAL_MS,
      event_loop_utilization_pct: Number((Number(elu?.utilization || 0) * 100).toFixed(2)),
      event_loop_active_ms: Number(Number(elu?.active || 0).toFixed(2)),
      event_loop_idle_ms: Number(Number(elu?.idle || 0).toFixed(2)),
      child_process_cpu_pct: isMainThread
        ? Number((((Number(cpu?.user || 0) + Number(cpu?.system || 0)) / wallUs) * 100).toFixed(2))
        : null,
      rss_mb: Math.round(memory.rss / 1048576),
      heap_used_mb: Math.round(memory.heapUsed / 1048576),
      market_light: currentExtra ? {
        ...currentExtra,
        round_delta: roundDelta,
        ...(extraDelta || {}),
      } : null,
      timestamp_ms: Date.now(),
    }));
  } catch (error) {
    console.log('[Step1073 V102 runtime-profiler] ' + JSON.stringify({
      collector_role: ROLE,
      runtime: isMainThread ? 'child_process' : 'worker_thread',
      sample: runtimeProfileSamples + 1,
      error: String(error?.message || error).slice(0, 240),
      timestamp_ms: Date.now(),
    }));
    runtimeProfileSamples += 1;
  }
  if (runtimeProfileSamples >= RUNTIME_PROFILE_MAX_SAMPLES) {
    clearInterval(runtimeProfileTimer);
  }
}, RUNTIME_PROFILE_INTERVAL_MS);
runtimeProfileTimer.unref?.();

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);

  if (url.pathname === '/_isolated/health') {
    sendJson(res, 200, {
      ok: true,
      collector_role: ROLE,
      collector_version: VERSION,
      module_version: roleVersion,
      runtime: isMainThread ? 'child_process' : 'worker_thread',
      pid: process.pid,
      thread_id: isMainThread ? null : threadId,
      ppid: process.ppid,
      uptime_seconds: Math.round(process.uptime()),
      timestamp_ms: Date.now(),
    });
    return;
  }

  if (url.pathname === '/_isolated/state') {
    sendJson(res, 200, internalState(url));
    return;
  }

  try {
    const handled = await handleRoleRoute(req, res, url);
    if (handled) return;
  } catch (error) {
    sendJson(res, 500, {
      ok: false,
      collector_role: ROLE,
      collector_version: VERSION,
      error: String(error?.message || error),
    });
    return;
  }

  sendJson(res, 404, {
    ok: false,
    collector_role: ROLE,
    collector_version: VERSION,
    error: 'route_not_owned_by_isolated_collector',
    path: url.pathname,
  });
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`[${VERSION}] isolated collector ${ROLE} listening on 127.0.0.1:${PORT} runtime=${isMainThread ? 'child_process' : 'worker_thread'} pid=${process.pid} thread=${isMainThread ? 0 : threadId}`);
});

function shutdown(signal) {
  console.log(`[${VERSION}] isolated collector ${ROLE} shutdown signal=${signal}`);
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2_500).unref?.();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
