import http from 'node:http';
import { installProviderGovernorFetch, getProviderGovernorHealth } from './provider-request-governor.mjs';

const ROLE = String(process.env.KAKA_ISOLATED_COLLECTOR_ROLE || '').trim();
const PORT = Number(process.env.KAKA_ISOLATED_COLLECTOR_PORT || 0);
const VERSION = '650.8.15.119';

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
  internalState = () => {
    const providers = {};
    for (const provider of ['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate']) {
      providers[`spot:${provider}`] = module.getMarketLightInternalSnapshot({
        market: 'spot',
        provider,
      });
    }
    for (const provider of ['binance', 'okx', 'bybit', 'bitget', 'gate']) {
      providers[`contract:${provider}`] = module.getMarketLightInternalSnapshot({
        market: 'contract',
        provider,
      });
    }
    return {
      ok: true,
      collector_role: ROLE,
      collector_version: VERSION,
      module_version: module.getMarketLightSnapshotHealth().version || null,
      pid: process.pid,
      ppid: process.ppid,
      uptime_seconds: Math.round(process.uptime()),
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
    pid: process.pid,
    ppid: process.ppid,
    uptime_seconds: Math.round(process.uptime()),
    provider_governor: getProviderGovernorHealth(),
    liquidation_persistence: module.getContractLiquidationPersistenceHealth(),
    binance_liquidation_ws: module.getBinanceLiquidationWsHealth(),
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
      pid: process.pid,
      ppid: process.ppid,
      uptime_seconds: Math.round(process.uptime()),
      timestamp_ms: Date.now(),
    });
    return;
  }

  if (url.pathname === '/_isolated/state') {
    sendJson(res, 200, internalState());
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
  console.log(`[${VERSION}] isolated collector ${ROLE} listening on 127.0.0.1:${PORT} pid=${process.pid}`);
});

function shutdown(signal) {
  console.log(`[${VERSION}] isolated collector ${ROLE} shutdown signal=${signal}`);
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2_500).unref?.();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
