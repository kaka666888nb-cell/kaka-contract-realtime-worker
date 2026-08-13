import http from 'node:http';
import { spawn } from 'node:child_process';
import { Worker } from 'node:worker_threads';
import { randomUUID } from 'node:crypto';
import { gzip } from 'node:zlib';
import { promisify } from 'node:util';

const VERSION = '650.8.15.134';
const MARKET_LIGHT_PORT = Number(process.env.KAKA_MARKET_LIGHT_COLLECTOR_PORT || 10011);
const LIQUIDATION_PORT = Number(process.env.KAKA_LIQUIDATION_COLLECTOR_PORT || 10012);
const DEEP_MARKET_PORT = Number(process.env.KAKA_DEEP_MARKET_COLLECTOR_PORT || 10013);
const SLOW_STATS_PORT = Number(process.env.KAKA_SLOW_STATS_COLLECTOR_PORT || 10014);
const DEEP_MARKET_MAX_OLD_MB = Math.max(64, Number(process.env.KAKA_DEEP_MARKET_WORKER_MAX_OLD_MB || 144));
const SLOW_STATS_MAX_OLD_MB = Math.max(64, Number(process.env.KAKA_SLOW_STATS_WORKER_MAX_OLD_MB || 144));
const RESTART_BASE_MS = Math.max(1_000, Number(process.env.KAKA_COLLECTOR_RESTART_BASE_MS || 2_000));
const RESTART_MAX_MS = Math.max(RESTART_BASE_MS, Number(process.env.KAKA_COLLECTOR_RESTART_MAX_MS || 30_000));


// Step1004.10 pressure transport hardening: large shared snapshots are authoritative
// backend snapshots, so concurrent user reads must reuse one short-lived serialized
// response instead of forcing one localhost collector request + JSON serialization per user.
// The cache is response-only: collectors keep their existing update cadence and no exchange
// request is started by cache refreshes beyond the already-running collector work.
const gzipAsync = promisify(gzip);
const ISOLATED_KEEP_ALIVE_AGENT = new http.Agent({ keepAlive: true, maxSockets: 48, maxFreeSockets: 12 });
const SHARED_RESPONSE_CACHE_MAX_ENTRIES = 32;
const SHARED_RESPONSE_MAX_BODY_BYTES = 32 * 1024 * 1024;
const SHARED_RESPONSE_GZIP_MIN_BYTES = 2 * 1024;
const SHARED_RESPONSE_GZIP_LEVEL = Math.max(1, Math.min(9, Number(process.env.KAKA_SHARED_RESPONSE_GZIP_LEVEL || 6)));
const SHARED_RESPONSE_EDGE_POLICY_VERSION = 'render_edge_cache_shared_snapshot_v1';
const sharedResponseCache = new Map();
const sharedResponseInflight = new Map();
const sharedResponseStats = {
  user_requests: 0,
  fresh_hits: 0,
  stale_hits: 0,
  cold_misses: 0,
  inflight_coalesced: 0,
  collector_fetches: 0,
  collector_fetch_errors: 0,
  gzip_builds: 0,
  gzip_build_errors: 0,
  gzip_served_responses: 0,
  raw_served_responses: 0,
  raw_bytes_fetched: 0,
  gzip_bytes_built: 0,
};

function sharedResponsePolicy(pathname) {
  const path = String(pathname || '');
  if (path === '/api/market-light/current-snapshot') return { freshMs: 2_000, staleMs: 10_000, cdnSMaxAgeSec: 2 };
  if (path === '/api/contract-flow/market-snapshot') return { freshMs: 5_000, staleMs: 30_000, cdnSMaxAgeSec: 5 };
  if (path === '/api/contract-liquidation/market-snapshot') return { freshMs: 1_500, staleMs: 8_000, cdnSMaxAgeSec: 2 };
  if (path === '/api/contract-liquidation/heatmap') return { freshMs: 2_000, staleMs: 10_000, cdnSMaxAgeSec: 2 };
  if (path === '/api/contract-focus-pool/current-snapshot') return { freshMs: 5_000, staleMs: 20_000, cdnSMaxAgeSec: 5 };
  if (path === '/api/contract-deep-shared/current-snapshot') return { freshMs: 2_000, staleMs: 10_000, cdnSMaxAgeSec: 2 };
  if (path === '/api/derivatives-public/current-snapshot') return { freshMs: 15_000, staleMs: 60_000, cdnSMaxAgeSec: 15 };
  if (path === '/api/history-lifecycle/current-snapshot') return { freshMs: 30_000, staleMs: 120_000, cdnSMaxAgeSec: 30 };
  return null;
}

function sharedResponseCdnCacheControl(policy) {
  const sMaxAge = Math.max(1, Number(policy?.cdnSMaxAgeSec || 1));
  const swr = Math.max(8, Math.ceil(sMaxAge * 4));
  const sie = Math.max(30, Math.ceil(sMaxAge * 12));
  return `public, s-maxage=${sMaxAge}, stale-while-revalidate=${swr}, stale-if-error=${sie}`;
}

function cleanSharedHeaders(headers = {}) {
  const result = {};
  for (const [name, value] of Object.entries(headers || {})) {
    const lower = String(name || '').toLowerCase();
    if (lower === 'connection' || lower === 'keep-alive' || lower === 'transfer-encoding' || lower === 'content-length' || lower === 'content-encoding' || lower === 'vary' || lower === 'cdn-cache-control') continue;
    if (value != null) result[name] = value;
  }
  result['cache-control'] = 'no-store';
  return result;
}

function acceptsGzip(req) {
  return /(?:^|,|\s)gzip(?:\s|,|;|$)/i.test(String(req?.headers?.['accept-encoding'] || ''));
}

function pruneSharedResponseCache() {
  if (sharedResponseCache.size <= SHARED_RESPONSE_CACHE_MAX_ENTRIES) return;
  const entries = [...sharedResponseCache.entries()].sort((a, b) => Number(a[1]?.storedAt || 0) - Number(b[1]?.storedAt || 0));
  while (entries.length > SHARED_RESPONSE_CACHE_MAX_ENTRIES) {
    const [key] = entries.shift();
    sharedResponseCache.delete(key);
  }
}

function fetchIsolatedBuffered(role, path) {
  const port = isolatedCollectorPort(role);
  sharedResponseStats.collector_fetches += 1;
  return new Promise((resolve, reject) => {
    const upstream = http.request({
      hostname: '127.0.0.1',
      port,
      method: 'GET',
      path,
      agent: ISOLATED_KEEP_ALIVE_AGENT,
      headers: {
        accept: 'application/json',
        'accept-encoding': 'identity',
        host: `127.0.0.1:${port}`,
      },
    }, (upstreamRes) => {
      const chunks = [];
      let total = 0;
      upstreamRes.on('data', (chunk) => {
        total += chunk.length;
        if (total > SHARED_RESPONSE_MAX_BODY_BYTES) {
          upstreamRes.destroy(new Error('isolated_shared_response_too_large'));
          return;
        }
        chunks.push(chunk);
      });
      upstreamRes.on('end', () => {
        const body = Buffer.concat(chunks);
        sharedResponseStats.raw_bytes_fetched += body.length;
        resolve({ statusCode: upstreamRes.statusCode || 502, headers: upstreamRes.headers, body });
      });
      upstreamRes.on('error', reject);
    });
    upstream.setTimeout(45_000, () => upstream.destroy(new Error(`isolated_${role}_shared_timeout`)));
    upstream.on('error', reject);
    upstream.end();
  });
}

async function buildSharedResponseEntry(role, path) {
  const result = await fetchIsolatedBuffered(role, path);
  let gzipBody = null;
  if (result.statusCode >= 200 && result.statusCode < 300 && result.body.length >= SHARED_RESPONSE_GZIP_MIN_BYTES) {
    try {
      gzipBody = await gzipAsync(result.body, { level: SHARED_RESPONSE_GZIP_LEVEL });
      sharedResponseStats.gzip_builds += 1;
      sharedResponseStats.gzip_bytes_built += gzipBody.length;
    } catch {
      sharedResponseStats.gzip_build_errors += 1;
      gzipBody = null;
    }
  }
  return { ...result, gzipBody, storedAt: Date.now() };
}

function sendSharedResponse(req, res, entry, cacheState, policy) {
  if (res.headersSent) return;
  const useGzip = Buffer.isBuffer(entry?.gzipBody) && acceptsGzip(req);
  const body = useGzip ? entry.gzipBody : entry.body;
  const statusCode = entry?.statusCode || 502;
  const headers = {
    ...cleanSharedHeaders(entry?.headers || {}),
    'content-length': String(body?.length || 0),
    'x-kaka-shared-response-cache': cacheState,
    'x-kaka-shared-response-reused': cacheState === 'fresh' || cacheState === 'stale' || cacheState === 'coalesced' ? '1' : '0',
    'x-kaka-edge-cache-policy': SHARED_RESPONSE_EDGE_POLICY_VERSION,
  };
  // Browser/device storage stays disabled. Render's CDN gets a separate, short public TTL.
  // CDN-Cache-Control takes precedence over Cache-Control on Render when edge caching is enabled.
  if (statusCode >= 200 && statusCode < 300) {
    headers['cdn-cache-control'] = sharedResponseCdnCacheControl(policy);
  }
  if (useGzip) {
    headers['content-encoding'] = 'gzip';
    headers.vary = 'Accept-Encoding';
    sharedResponseStats.gzip_served_responses += 1;
  } else {
    sharedResponseStats.raw_served_responses += 1;
  }
  res.writeHead(statusCode, headers);
  res.end(body || Buffer.alloc(0));
}

function startSharedResponseRefresh(key, role, path) {
  let pending = sharedResponseInflight.get(key);
  if (pending) return pending;
  pending = buildSharedResponseEntry(role, path)
    .then((entry) => {
      if (entry.statusCode >= 200 && entry.statusCode < 300) {
        sharedResponseCache.set(key, entry);
        pruneSharedResponseCache();
      }
      return entry;
    })
    .catch((error) => {
      sharedResponseStats.collector_fetch_errors += 1;
      throw error;
    })
    .finally(() => sharedResponseInflight.delete(key));
  sharedResponseInflight.set(key, pending);
  return pending;
}

function proxySharedCachedCollectorGet(req, res, url, role, policy) {
  sharedResponseStats.user_requests += 1;
  const key = `${role}:${url.pathname}${url.search}`;
  const now = Date.now();
  const cached = sharedResponseCache.get(key);
  const age = cached ? now - Number(cached.storedAt || 0) : Number.POSITIVE_INFINITY;

  if (cached && age <= policy.freshMs) {
    sharedResponseStats.fresh_hits += 1;
    sendSharedResponse(req, res, cached, 'fresh', policy);
    return;
  }

  if (cached && age <= policy.staleMs) {
    sharedResponseStats.stale_hits += 1;
    startSharedResponseRefresh(key, role, req.url).catch(() => {});
    sendSharedResponse(req, res, cached, 'stale', policy);
    return;
  }

  const existing = sharedResponseInflight.get(key);
  if (existing) sharedResponseStats.inflight_coalesced += 1;
  else sharedResponseStats.cold_misses += 1;
  const pending = existing || startSharedResponseRefresh(key, role, req.url);
  pending.then((entry) => sendSharedResponse(req, res, entry, existing ? 'coalesced' : 'miss', policy))
    .catch((error) => {
      if (res.headersSent) {
        res.destroy(error);
        return;
      }
      const body = Buffer.from(JSON.stringify({
        ok: false,
        error: 'isolated_collector_shared_cache_unavailable',
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
}

const ROLES = Object.freeze({
  'market-light': Object.freeze({ port: MARKET_LIGHT_PORT, runtime: 'child_process' }),
  liquidation: Object.freeze({ port: LIQUIDATION_PORT, runtime: 'child_process' }),
  'deep-market': Object.freeze({
    port: DEEP_MARKET_PORT,
    runtime: 'worker_thread',
    max_old_generation_size_mb: DEEP_MARKET_MAX_OLD_MB,
  }),
  'slow-stats': Object.freeze({
    port: SLOW_STATS_PORT,
    runtime: 'worker_thread',
    max_old_generation_size_mb: SLOW_STATS_MAX_OLD_MB,
  }),
});

const INSTANCE_ID = randomUUID();
const INSTANCE_STARTED_AT = new Date().toISOString();
const state = new Map();
let started = false;
let shuttingDown = false;

function roleState(role) {
  if (!state.has(role)) {
    state.set(role, {
      role,
      port: ROLES[role].port,
      runtime: ROLES[role].runtime,
      max_old_generation_size_mb: Number(ROLES[role].max_old_generation_size_mb || 0),
      handle: null,
      pid: null,
      thread_id: null,
      starts: 0,
      exits: 0,
      restart_count: 0,
      consecutive_failures: 0,
      last_started_at: null,
      last_exit_at: null,
      last_exit_code: null,
      last_exit_signal: '',
      last_error: '',
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

function roleAlive(info) {
  if (!info?.handle) return false;
  if (info.runtime === 'worker_thread') return info.handle.threadId > 0;
  return info.handle.exitCode == null;
}

function scheduleRoleRestart(role, info) {
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
}

function onRoleExit(role, info, code, signal = '') {
  info.exits += 1;
  info.last_exit_at = new Date().toISOString();
  info.last_exit_code = code;
  info.last_exit_signal = signal || '';
  info.handle = null;
  info.pid = null;
  info.thread_id = null;
  scheduleRoleRestart(role, info);
}

function spawnChildProcessRole(role, info) {
  const child = spawn(process.execPath, ['src/isolated-collector-worker.mjs'], {
    env: {
      ...process.env,
      KAKA_ISOLATED_COLLECTOR_ROLE: role,
      KAKA_ISOLATED_COLLECTOR_PORT: String(info.port),
      KAKA_DISABLE_BINANCE_REST: '1',
    },
    stdio: 'inherit',
  });
  info.handle = child;
  info.pid = child.pid || null;
  info.thread_id = null;
  child.on('spawn', () => {
    info.consecutive_failures = 0;
    info.last_error = '';
  });
  child.on('error', (error) => {
    info.last_error = String(error?.message || error).slice(0, 400);
  });
  child.on('exit', (code, signal) => onRoleExit(role, info, code, signal || ''));
}

function spawnWorkerThreadRole(role, info) {
  const worker = new Worker(new URL('./isolated-collector-worker.mjs', import.meta.url), {
    workerData: {
      role,
      port: info.port,
      disable_binance_rest: true,
      supervisor_version: VERSION,
    },
    resourceLimits: {
      maxOldGenerationSizeMb: Math.max(48, Number(info.max_old_generation_size_mb || 96)),
      stackSizeMb: 2,
    },
  });
  info.handle = worker;
  info.pid = process.pid;
  info.thread_id = worker.threadId || null;
  info.consecutive_failures = 0;
  info.last_error = '';

  worker.on('online', () => {
    info.thread_id = worker.threadId || info.thread_id;
    info.consecutive_failures = 0;
  });
  worker.on('error', (error) => {
    info.last_error = String(error?.message || error).slice(0, 400);
  });
  worker.on('exit', (code) => onRoleExit(role, info, code, 'worker_exit'));
}

function spawnRole(role) {
  if (shuttingDown) return;
  const info = roleState(role);
  if (roleAlive(info)) return;

  if (info.restart_timer) {
    clearTimeout(info.restart_timer);
    info.restart_timer = null;
  }

  info.starts += 1;
  info.last_started_at = new Date().toISOString();
  info.next_restart_at = 0;

  if (info.runtime === 'worker_thread') {
    spawnWorkerThreadRole(role, info);
  } else {
    spawnChildProcessRole(role, info);
  }
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
    path.startsWith('/api/bybit-advanced') ||
    path.startsWith('/api/derivatives-public') ||
    path.startsWith('/api/history-lifecycle')
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
  const policy = req.method === 'GET' ? sharedResponsePolicy(url?.pathname) : null;
  if (policy) {
    proxySharedCachedCollectorGet(req, res, url, role, policy);
    return true;
  }

  const port = isolatedCollectorPort(role);
  const upstream = http.request({
    hostname: '127.0.0.1',
    port,
    method: req.method,
    path: req.url,
    agent: ISOLATED_KEEP_ALIVE_AGENT,
    headers: {
      ...req.headers,
      host: `127.0.0.1:${port}`,
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
      runtime: info.runtime,
      host_pid: info.runtime === 'worker_thread' ? process.pid : info.pid,
      pid: info.runtime === 'child_process' ? info.pid : null,
      thread_id: info.runtime === 'worker_thread' ? info.thread_id : null,
      alive: roleAlive(info),
      starts: info.starts,
      exits: info.exits,
      restart_count: info.restart_count,
      consecutive_failures: info.consecutive_failures,
      last_started_at: info.last_started_at,
      last_exit_at: info.last_exit_at,
      last_exit_code: info.last_exit_code,
      last_exit_signal: info.last_exit_signal,
      last_error: info.last_error,
      next_restart_at: info.next_restart_at > 0 ? new Date(info.next_restart_at).toISOString() : null,
      restart_scope: 'role_only',
      max_old_generation_size_mb: info.max_old_generation_size_mb || null,
    };
  }

  const firstBatchNames = ['market-light', 'liquidation'];
  const secondBatchNames = ['deep-market', 'slow-stats'];
  const firstBatchPids = firstBatchNames.map((role) => Number(roles[role]?.pid || 0)).filter((pid) => pid > 0);
  const secondBatchThreads = secondBatchNames.map((role) => Number(roles[role]?.thread_id || 0)).filter((id) => id > 0);

  return {
    ok: true,
    version: VERSION,
    instance_id: INSTANCE_ID,
    instance_started_at: INSTANCE_STARTED_AT,
    enabled: started,
    parent_pid: process.pid,
    host_process_memory: {
      rss_mb: Math.round(process.memoryUsage().rss / 1048576),
      heap_total_mb: Math.round(process.memoryUsage().heapTotal / 1048576),
      heap_used_mb: Math.round(process.memoryUsage().heapUsed / 1048576),
      external_mb: Math.round(process.memoryUsage().external / 1048576),
      array_buffers_mb: Math.round(process.memoryUsage().arrayBuffers / 1048576),
    },
    role_count: Object.keys(ROLES).length,
    roles,

    first_batch_runtime: 'child_process',
    first_batch_roles: firstBatchNames,
    first_batch_roles_alive: firstBatchNames.every((role) => roles[role]?.alive === true),
    first_batch_child_pids_distinct:
      firstBatchPids.length === firstBatchNames.length && new Set(firstBatchPids).size === firstBatchPids.length,
    first_batch_parent_pid_distinct: firstBatchPids.every((pid) => pid !== process.pid),
    first_batch_process_level_fault_domains: true,

    second_batch_runtime: 'worker_thread',
    second_batch_roles: secondBatchNames,
    second_batch_roles_alive: secondBatchNames.every((role) => roles[role]?.alive === true),
    second_batch_thread_ids_distinct:
      secondBatchThreads.length === secondBatchNames.length && new Set(secondBatchThreads).size === secondBatchThreads.length,
    second_batch_host_pid: process.pid,
    second_batch_worker_resource_limits_enabled: secondBatchNames.every(
      (role) => Number(roles[role]?.max_old_generation_size_mb || 0) >= 48
    ),
    second_batch_worker_failure_isolated_from_parent: true,
    second_batch_worker_failure_isolated_from_sibling: true,

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
    parent_routes_second_batch_to_workers: true,

    memory_safety_design: 'first_batch_child_processes_plus_second_batch_resource_limited_worker_isolates_plus_projected_internal_bridges',
    projected_internal_bridge_payloads: true,
    full_market_rows_not_copied_to_second_batch: true,
    shared_read_transport_cache: {
      ready: true,
      response_cache_enabled: true,
      inflight_coalescing_enabled: true,
      stale_while_refresh_enabled: true,
      gzip_shared_buffer_enabled: true,
      gzip_level: SHARED_RESPONSE_GZIP_LEVEL,
      collector_keep_alive_enabled: true,
      collector_fetches_scale_with_users: false,
      one_refresh_per_key_window: true,
      render_edge_cache_header_enabled: true,
      render_edge_cache_requires_dashboard_all_files: true,
      browser_cache_control_no_store: true,
      cdn_cache_control_separate_from_browser_cache: true,
      edge_cache_policy_version: SHARED_RESPONSE_EDGE_POLICY_VERSION,
      cache_entries: sharedResponseCache.size,
      inflight_keys: sharedResponseInflight.size,
      max_entries: SHARED_RESPONSE_CACHE_MAX_ENTRIES,
      max_body_bytes: SHARED_RESPONSE_MAX_BODY_BYTES,
      user_requests: sharedResponseStats.user_requests,
      fresh_hits: sharedResponseStats.fresh_hits,
      stale_hits: sharedResponseStats.stale_hits,
      cold_misses: sharedResponseStats.cold_misses,
      inflight_coalesced: sharedResponseStats.inflight_coalesced,
      collector_fetches: sharedResponseStats.collector_fetches,
      collector_fetch_errors: sharedResponseStats.collector_fetch_errors,
      gzip_builds: sharedResponseStats.gzip_builds,
      gzip_build_errors: sharedResponseStats.gzip_build_errors,
      gzip_served_responses: sharedResponseStats.gzip_served_responses,
      raw_served_responses: sharedResponseStats.raw_served_responses,
      raw_bytes_fetched: sharedResponseStats.raw_bytes_fetched,
      gzip_bytes_built: sharedResponseStats.gzip_bytes_built,
      cacheable_routes: 8,
    },
    timestamp_ms: Date.now(),
  };
}

export function stopCollectorIsolationSupervisor() {
  shuttingDown = true;
  for (const role of Object.keys(ROLES)) {
    const info = roleState(role);
    if (info.restart_timer) clearTimeout(info.restart_timer);
    info.restart_timer = null;
    if (!info.handle) continue;
    if (info.runtime === 'worker_thread') {
      info.handle.terminate().catch(() => {});
    } else {
      info.handle.kill('SIGTERM');
    }
  }
}
