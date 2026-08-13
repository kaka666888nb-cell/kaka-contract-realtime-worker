import http from 'node:http';
import { isMainThread, threadId, workerData } from 'node:worker_threads';
import { installProviderGovernorFetch, getProviderGovernorHealth } from './provider-request-governor.mjs';

const ROLE = String(workerData?.role || process.env.KAKA_ISOLATED_COLLECTOR_ROLE || '').trim();
const PORT = Number(workerData?.port || process.env.KAKA_ISOLATED_COLLECTOR_PORT || 0);
process.env.KAKA_ISOLATED_COLLECTOR_ROLE = ROLE;
process.env.KAKA_ISOLATED_COLLECTOR_PORT = String(PORT);
if (workerData?.disable_binance_rest === true) process.env.KAKA_DISABLE_BINANCE_REST = '1';
const VERSION = '650.8.15.123';

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

if (ROLE === 'market-light') {
  const module = await import('./market-light-snapshot.mjs');
  module.startMarketLightSnapshotScanner();
  roleVersion = module.getMarketLightSnapshotHealth().version || null;
  handleRoleRoute = module.handleMarketLightSnapshot;
  internalState = (url) => {
    const scope = String(url?.searchParams?.get('scope') || 'parent');
    const providers = {};
    const wanted = scope === 'deep-market'
      ? [
          ['contract', 'binance'], ['contract', 'okx'], ['contract', 'bybit'],
          ['contract', 'bitget'], ['contract', 'gate'],
        ]
      : scope === 'slow-stats'
        ? [
            ['spot', 'bitget'],
            ['contract', 'bitget'], ['contract', 'okx'], ['contract', 'bybit'],
          ]
        : [
            ['spot', 'binance'], ['spot', 'coinbase'], ['spot', 'okx'],
            ['spot', 'bybit'], ['spot', 'bitget'], ['spot', 'gate'],
            ['contract', 'binance'], ['contract', 'okx'], ['contract', 'bybit'],
            ['contract', 'bitget'], ['contract', 'gate'],
          ];

    for (const [market, provider] of wanted) {
      providers[`${market}:${provider}`] = module.getMarketLightInternalSnapshot({ market, provider });
    }
    return {
      ok: true,
      collector_role: ROLE,
      collector_version: VERSION,
      module_version: module.getMarketLightSnapshotHealth().version || null,
      runtime: isMainThread ? 'child_process' : 'worker_thread',
      pid: process.pid,
      thread_id: isMainThread ? null : threadId,
      ppid: process.ppid,
      uptime_seconds: Math.round(process.uptime()),
      state_scope: scope,
      provider_snapshot_count: Object.keys(providers).length,
      provider_governor: getProviderGovernorHealth(),
      health: module.getMarketLightSnapshotHealth(),
      providers,
      timestamp_ms: Date.now(),
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
    provider_governor: getProviderGovernorHealth(),
    liquidation_persistence: module.getContractLiquidationPersistenceHealth(),
    binance_liquidation_ws: module.getBinanceLiquidationWsHealth(),
    timestamp_ms: Date.now(),
  });
} else if (ROLE === 'deep-market') {
  const marketBridge = await import('./market-light-bridge.mjs');
  const focusModule = await import('./contract-focus-pool.mjs');
  const flowModule = await import('./contract-flow.mjs');
  const deepModule = await import('./contract-deep-shared.mjs');

  marketBridge.startMarketLightBridge();
  focusModule.startContractFocusPoolScanner();
  flowModule.startContractFlowUniverseScanner();
  deepModule.startContractDeepSharedScanner();

  roleVersion = deepModule.getContractDeepSharedHealth().version || null;
  handleRoleRoute = async (req, res, url) => {
    if (await focusModule.handleContractFocusPool(req, res, url)) return true;
    if (await deepModule.handleContractDeepShared(req, res, url)) return true;
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
      provider_governor: getProviderGovernorHealth(),
      market_light_bridge: marketBridge.getMarketLightSnapshotHealth(),
      focus_health: focusHealth,
      focus_snapshot: focusSnapshot,
      flow_health: flowModule.getContractFlowHealth(),
      deep_health: deepModule.getContractDeepSharedHealth(),
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

  marketBridge.startMarketLightBridge();
  deepBridge.startDeepMarketBridge();

  binance.startBinanceAdvancedStatsScanner();
  bitget.startBitgetAdvancedStatsScanner();
  gate.startGateAdvancedStatsScanner();
  okx.startOkxAdvancedStatsScanner();
  bybit.startBybitAdvancedStatsScanner();

  roleVersion = binance.getBinanceAdvancedStatsHealth().version || null;
  handleRoleRoute = async (req, res, url) => {
    if (await binance.handleBinanceAdvancedStats(req, res, url)) return true;
    if (await bitget.handleBitgetAdvancedStats(req, res, url)) return true;
    if (await gate.handleGateAdvancedStats(req, res, url)) return true;
    if (await okx.handleOkxAdvancedStats(req, res, url)) return true;
    if (await bybit.handleBybitAdvancedStats(req, res, url)) return true;
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
    provider_governor: getProviderGovernorHealth(),
    market_light_bridge: marketBridge.getMarketLightSnapshotHealth(),
    deep_market_bridge: deepBridge.getDeepMarketBridgeHealth(),
    binance_advanced: binance.getBinanceAdvancedStatsHealth(),
    bitget_advanced: bitget.getBitgetAdvancedStatsHealth(),
    gate_advanced: gate.getGateAdvancedStatsHealth(),
    okx_advanced: okx.getOkxAdvancedStatsHealth(),
    bybit_advanced: bybit.getBybitAdvancedStatsHealth(),
    timestamp_ms: Date.now(),
  });
} else {
  throw new Error(`unsupported_isolated_collector_role:${ROLE}`);
}

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
