import http from 'node:http';
import { spawn } from 'node:child_process';

const VERSION = '650.8.15.122';
const MARKET_LIGHT_PORT = Number(process.env.KAKA_MARKET_LIGHT_COLLECTOR_PORT || 10011);
const LIQUIDATION_PORT = Number(process.env.KAKA_LIQUIDATION_COLLECTOR_PORT || 10012);
const DEEP_MARKET_PORT = Number(process.env.KAKA_DEEP_MARKET_COLLECTOR_PORT || 10013);
const SLOW_STATS_PORT = Number(process.env.KAKA_SLOW_STATS_COLLECTOR_PORT || 10014);
const RESTART_BASE_MS = Math.max(1_000, Number(process.env.KAKA_COLLECTOR_RESTART_BASE_MS || 2_000));
const RESTART_MAX_MS = Math.max(RESTART_BASE_MS, Number(process.env.KAKA_COLLECTOR_RESTART_MAX_MS || 30_000));

const ROLES = Object.freeze({
  'market-light': Object.freeze({ port: MARKET_LIGHT_PORT }),
  liquidation: Object.freeze({ port: LIQUIDATION_PORT }),
  'deep-market': Object.freeze({ port: DEEP_MARKET_PORT }),
  'slow-stats': Object.freeze({ port: SLOW_STATS_PORT }),
});

const state = new Map();
let started = false;
let shuttingDown = false;

function roleState(role) {
  if (!state.has(role)) {
    state.set(role, {
      role,
      port: ROLES[role].port,
      child: null,
      pid: null,
      starts: 0,
      exits: 0,
      restart_count: 0,
      consecutive_failures: 0,
      last_started_at: null,
      last_exit_at: null,
      last_exit_code: null,
      last_exit_signal: '',
      next_restart_at: 0,
      restart_timer: null,
    });
  }
  return state.get(role);
}

function restartDelay(failures) {
  const factor = 2 ** Math.min(4, Math.max(0, Number(failures || 1) - 1));
  return Math.min(RESTART_MAX_MS, RESTART_BASE_MS * factor);
}

function spawnRole(role) {
  if (shuttingDown) return;
  const info = roleState(role);
  if (info.child && info.child.exitCode == null) return;

  if (info.restart_timer) {
    clearTimeout(info.restart_timer);
    info.restart_timer = null;
  }

  const child = spawn(process.execPath, ['src/isolated-collector-worker.mjs'], {
    env: {
      ...process.env,
      KAKA_ISOLATED_COLLECTOR_ROLE: role,
      KAKA_ISOLATED_COLLECTOR_PORT: String(info.port),
      KAKA_DISABLE_BINANCE_REST: '1',
    },
    stdio: 'inherit',
  });

  info.child = child;
  info.pid = child.pid || null;
  info.starts += 1;
  info.last_started_at = new Date().toISOString();
  info.next_restart_at = 0;

  child.on('spawn', () => {
    info.consecutive_failures = 0;
  });

  child.on('exit', (code, signal) => {
    info.exits += 1;
    info.last_exit_at = new Date().toISOString();
    info.last_exit_code = code;
    info.last_exit_signal = signal || '';
    info.child = null;
    info.pid = null;

    if (shuttingDown) return;

    info.restart_count += 1;
    info.consecutive_failures += 1;
    const delay = restartDelay(info.consecutive_failures);
    info.next_restart_at = Date.now() + delay;
    info.restart_timer = setTimeout(() => {
      info.restart_timer = null;
      spawnRole(role);
    }, delay);
    info.restart_timer.unref?.();
  });
}

export function startCollectorIsolationSupervisor() {
  if (started) return;
  started = true;
  for (const role of Object.keys(ROLES)) spawnRole(role);
}

export function isolatedCollectorPort(role) {
  return Number(ROLES[role]?.port || 0);
}

export function collectorRoleForPath(pathname) {
  const path = String(pathname || '');
  if (path === '/api/market-light/current-snapshot' || path === '/api/market-light/health') return 'market-light';
  if (path.startsWith('/api/contract-liquidation')) return 'liquidation';
  if (
    path.startsWith('/api/contract-focus-pool') ||
    path.startsWith('/api/contract-flow') ||
    path.startsWith('/api/contract-deep-shared') ||
    path === '/api/contract-meta' ||
    path === '/api/gate-usd-flow-self-test' ||
    path === '/api/gate-contract-stats-live-test'
  ) return 'deep-market';
  if (
    path.startsWith('/api/binance-advanced') ||
    path.startsWith('/api/bitget-advanced') ||
    path.startsWith('/api/gate-advanced') ||
    path.startsWith('/api/okx-advanced') ||
    path.startsWith('/api/bybit-advanced')
  ) return 'slow-stats';
  return null;
}

export function requestIsolatedJson(role, path, timeoutMs = 8_000) {
  const port = isolatedCollectorPort(role);
  if (!port) return Promise.reject(new Error(`isolated_role_not_configured:${role}`));
  return new Promise((resolve, reject) => {
    const req = http.request({
      hostname: '127.0.0.1',
      port,
      method: 'GET',
      path,
      headers: { accept: 'application/json' },
    }, (res) => {
      const chunks = [];
      let total = 0;
      res.on('data', (chunk) => {
        total += chunk.length;
        if (total > 32 * 1024 * 1024) {
          res.destroy(new Error('isolated_response_too_large'));
          return;
        }
        chunks.push(chunk);
      });
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString('utf8');
        if ((res.statusCode || 500) < 200 || (res.statusCode || 500) >= 300) {
          reject(new Error(`isolated_http_${res.statusCode}:${body.slice(0, 300)}`));
          return;
        }
        try {
          resolve(JSON.parse(body));
        } catch (error) {
          reject(new Error(`isolated_json_parse:${String(error?.message || error)}`));
        }
      });
      res.on('error', reject);
    });
    req.setTimeout(timeoutMs, () => req.destroy(new Error('isolated_request_timeout')));
    req.on('error', reject);
    req.end();
  });
}

export function proxyIsolatedCollectorRequest(req, res, url) {
  const role = collectorRoleForPath(url?.pathname);
  if (!role) return false;
  const port = isolatedCollectorPort(role);
  const upstream = http.request({
    hostname: '127.0.0.1',
    port,
    method: req.method,
    path: req.url,
    headers: {
      ...req.headers,
      host: `127.0.0.1:${port}`,
      connection: 'close',
    },
  }, (upstreamRes) => {
    const headers = { ...upstreamRes.headers };
    delete headers.connection;
    delete headers['keep-alive'];
    res.writeHead(upstreamRes.statusCode || 502, headers);
    upstreamRes.pipe(res);
  });
  upstream.setTimeout(45_000, () => upstream.destroy(new Error(`isolated_${role}_timeout`)));
  upstream.on('error', (error) => {
    if (res.headersSent) {
      res.destroy(error);
      return;
    }
    const body = Buffer.from(JSON.stringify({
      ok: false,
      error: 'isolated_collector_unavailable',
      collector_role: role,
      message: String(error?.message || error),
    }));
    res.writeHead(503, {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
      'content-length': String(body.length),
    });
    res.end(body);
  });
  req.pipe(upstream);
  return true;
}

export function getCollectorIsolationHealth() {
  const roles = {};
  for (const role of Object.keys(ROLES)) {
    const info = roleState(role);
    roles[role] = {
      port: info.port,
      pid: info.pid,
      alive: Boolean(info.child && info.child.exitCode == null),
      starts: info.starts,
      exits: info.exits,
      restart_count: info.restart_count,
      consecutive_failures: info.consecutive_failures,
      last_started_at: info.last_started_at,
      last_exit_at: info.last_exit_at,
      last_exit_code: info.last_exit_code,
      last_exit_signal: info.last_exit_signal,
      next_restart_at: info.next_restart_at > 0 ? new Date(info.next_restart_at).toISOString() : null,
      restart_scope: 'role_only',
    };
  }

  const roleNames = Object.keys(ROLES);
  const allPids = roleNames.map((role) => Number(roles[role]?.pid || 0)).filter((pid) => pid > 0);
  const firstBatchNames = ['market-light', 'liquidation'];
  const secondBatchNames = ['deep-market', 'slow-stats'];
  const firstBatchPids = firstBatchNames.map((role) => Number(roles[role]?.pid || 0)).filter((pid) => pid > 0);
  const secondBatchPids = secondBatchNames.map((role) => Number(roles[role]?.pid || 0)).filter((pid) => pid > 0);

  return {
    ok: true,
    version: VERSION,
    enabled: started,
    parent_pid: process.pid,
    role_count: roleNames.length,
    roles,
    all_roles_alive: roleNames.every((role) => roles[role]?.alive === true),
    child_pids_distinct: allPids.length === roleNames.length && new Set(allPids).size === allPids.length,
    parent_pid_distinct_from_children: allPids.every((pid) => pid !== process.pid),

    first_batch_roles: firstBatchNames,
    first_batch_roles_alive: firstBatchNames.every((role) => roles[role]?.alive === true),
    first_batch_child_pids_distinct: firstBatchPids.length === firstBatchNames.length && new Set(firstBatchPids).size === firstBatchPids.length,
    first_batch_parent_pid_distinct: firstBatchPids.every((pid) => pid !== process.pid),

    second_batch_roles: secondBatchNames,
    second_batch_roles_alive: secondBatchNames.every((role) => roles[role]?.alive === true),
    second_batch_child_pids_distinct: secondBatchPids.length === secondBatchNames.length && new Set(secondBatchPids).size === secondBatchPids.length,
    second_batch_parent_pid_distinct: secondBatchPids.every((pid) => pid !== process.pid),

    process_level_fault_domains: true,
    one_role_exit_does_not_exit_parent: true,
    role_scoped_supervisor_restart: true,

    market_light_parent_scanner_started: false,
    liquidation_parent_module_loaded: false,
    contract_focus_pool_parent_scanner_started: false,
    contract_flow_parent_scanner_started: false,
    contract_deep_parent_scanner_started: false,
    slow_stats_parent_modules_loaded: false,

    deep_market_owns_focus_pool: true,
    deep_market_owns_contract_flow: true,
    deep_market_owns_deep_shared: true,
    slow_stats_owns_advanced_modules: true,
    parent_routes_second_batch_to_children: true,

    timestamp_ms: Date.now(),
  };
}

export function stopCollectorIsolationSupervisor() {
  shuttingDown = true;
  for (const role of Object.keys(ROLES)) {
    const info = roleState(role);
    if (info.restart_timer) clearTimeout(info.restart_timer);
    info.restart_timer = null;
    info.child?.kill('SIGTERM');
  }
}
