import http from 'node:http';
import { spawn } from 'node:child_process';
import { handleContractFlow } from './contract-flow.mjs';
import { handleContractDepth } from './contract-depth.mjs';
import { handleContractLiquidation } from './contract-liquidation.mjs';
import { handleContractFunding } from './contract-funding.mjs';

const PORT = Number(process.env.PORT || 10000);
const CHILD_PORT = Number(process.env.KAKA_CHILD_PORT || 10001);
const STEP_VERSION = '650.7';

const child = spawn(process.execPath, ['src/server.mjs'], {
  env: { ...process.env, PORT: String(CHILD_PORT) },
  stdio: 'inherit',
});

child.on('exit', (code, signal) => {
  console.error(`[Step${STEP_VERSION}] legacy worker exited code=${code} signal=${signal || ''}`);
  process.exit(code || 1);
});

const legacyCache = new Map();
const legacyInflight = new Map();
const legacyCircuit = new Map();
const LEGACY_MAX_BODY_BYTES = 24 * 1024 * 1024;

function legacyPolicy(url) {
  const provider = (url.searchParams.get('provider') || '').toLowerCase();
  const market = (url.searchParams.get('market_type') || url.searchParams.get('market') || '').toLowerCase();
  const isBinanceContractSnapshot = provider === 'binance' && /contract|future|perpetual|swap|linear/.test(market) &&
    ['/api/universe', '/api/tickers', '/api/klines'].includes(url.pathname);
  // Step650.7：这三条 Binance 合约路由已分别由 WebSocket 快照或官方归档+当前日桥接提供，
  // 不再经过旧 REST provider 级熔断。某个旧符号/归档文件暂缺不能连带封死全部正常币种。
  if (isBinanceContractSnapshot) return null;
  if (url.pathname === '/api/tickers') return { freshMs: 8_000, staleMs: 24 * 60 * 60_000 };
  if (url.pathname === '/api/klines') return { freshMs: 45_000, staleMs: 30 * 60_000 };
  if (url.pathname === '/api/universe') return { freshMs: 5 * 60_000, staleMs: 7 * 24 * 60 * 60_000 };
  return null;
}

function circuitKey(url) {
  const provider = (url.searchParams.get('provider') || '').toLowerCase();
  const market = (url.searchParams.get('market_type') || '').toLowerCase();
  return `${url.pathname}|${provider}|${market}`;
}

function isRestrictedFailure(statusCode, bodyText) {
  const text = String(bodyText || '').toLowerCase();
  return statusCode === 418 || statusCode === 429 || statusCode === 451 ||
    text.includes('way too many requests') ||
    (text.includes('ip(') && text.includes('banned until')) ||
    text.includes('too many requests');
}

function isUpstreamFailure(statusCode, bodyText) {
  const text = String(bodyText || '').toLowerCase();
  return statusCode >= 500 || statusCode === 408 || statusCode === 418 || statusCode === 429 || statusCode === 451 ||
    text.includes('502') || text.includes('bad gateway') || text.includes('legacy_worker_unavailable');
}

function openCircuit(key, statusCode, bodyText) {
  const restricted = isRestrictedFailure(statusCode, bodyText);
  const durationMs = restricted ? 30 * 60_000 : 90_000;
  const current = legacyCircuit.get(key);
  const until = Date.now() + durationMs;
  legacyCircuit.set(key, {
    until: Math.max(Number(current?.until || 0), until),
    reason: restricted ? 'exchange_rate_limit_or_region_block' : 'upstream_unavailable',
    statusCode,
  });
}

function cleanResponseHeaders(headers = {}) {
  const result = {};
  for (const [name, value] of Object.entries(headers)) {
    const lower = name.toLowerCase();
    if (lower === 'content-length' || lower === 'transfer-encoding' || lower === 'connection') continue;
    if (value != null) result[name] = value;
  }
  result['cache-control'] = 'no-store';
  return result;
}

function sendBuffered(res, result, extraHeaders = {}) {
  if (res.headersSent) return res.end();
  const body = Buffer.isBuffer(result.body) ? result.body : Buffer.from(String(result.body || ''));
  res.writeHead(result.statusCode || 200, {
    ...cleanResponseHeaders(result.headers),
    ...extraHeaders,
    'content-length': String(body.length),
  });
  res.end(body);
}

function sendCircuitJson(res, state) {
  const retryAfterSeconds = Math.max(1, Math.ceil((Number(state?.until || Date.now()) - Date.now()) / 1000));
  const body = Buffer.from(JSON.stringify({
    ok: false,
    error: 'legacy_rest_circuit_open',
    reason: state?.reason || 'upstream_unavailable',
    retry_after_seconds: retryAfterSeconds,
  }));
  res.writeHead(503, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'retry-after': String(retryAfterSeconds),
    'content-length': String(body.length),
  });
  res.end(body);
}

function fetchLegacyBuffered(req) {
  return new Promise((resolve, reject) => {
    const upstream = http.request({
      hostname: '127.0.0.1',
      port: CHILD_PORT,
      method: req.method,
      path: req.url,
      headers: { ...req.headers, host: `127.0.0.1:${CHILD_PORT}` },
    }, (upstreamRes) => {
      const chunks = [];
      let total = 0;
      upstreamRes.on('data', (chunk) => {
        total += chunk.length;
        if (total > LEGACY_MAX_BODY_BYTES) {
          upstreamRes.destroy(new Error('legacy_response_too_large'));
          return;
        }
        chunks.push(chunk);
      });
      upstreamRes.on('end', () => resolve({
        statusCode: upstreamRes.statusCode || 502,
        headers: upstreamRes.headers,
        body: Buffer.concat(chunks),
      }));
      upstreamRes.on('error', reject);
    });
    upstream.setTimeout(30_000, () => upstream.destroy(new Error('legacy_worker_timeout')));
    upstream.on('error', reject);
    req.pipe(upstream);
  });
}

async function proxyCachedGet(req, res, url, policy) {
  const now = Date.now();
  const key = `${req.method}:${url.pathname}${url.search}`;
  const groupKey = circuitKey(url);
  const cached = legacyCache.get(key);
  if (cached && now - cached.storedAt <= policy.freshMs) {
    sendBuffered(res, cached, { 'x-kaka-cache': 'fresh' });
    return;
  }
  const circuit = legacyCircuit.get(groupKey);
  if (circuit && circuit.until > now) {
    if (cached && now - cached.storedAt <= policy.staleMs) {
      sendBuffered(res, cached, { 'x-kaka-cache': 'stale-circuit' });
    } else {
      sendCircuitJson(res, circuit);
    }
    return;
  }
  if (circuit) legacyCircuit.delete(groupKey);

  let pending = legacyInflight.get(key);
  if (!pending) {
    pending = fetchLegacyBuffered(req)
      .then((result) => {
        const bodyText = result.body.toString('utf8', 0, Math.min(result.body.length, 4096));
        if (isUpstreamFailure(result.statusCode, bodyText)) {
          openCircuit(groupKey, result.statusCode, bodyText);
          const error = new Error('legacy_upstream_failure');
          error.result = result;
          throw error;
        }
        if (result.statusCode >= 200 && result.statusCode < 300) {
          legacyCache.set(key, { ...result, storedAt: Date.now() });
        }
        return result;
      })
      .finally(() => legacyInflight.delete(key));
    legacyInflight.set(key, pending);
  }

  try {
    const result = await pending;
    sendBuffered(res, result, { 'x-kaka-cache': 'miss' });
  } catch (_) {
    const fallback = legacyCache.get(key);
    if (fallback && Date.now() - fallback.storedAt <= policy.staleMs) {
      sendBuffered(res, fallback, { 'x-kaka-cache': 'stale-error' });
      return;
    }
    const state = legacyCircuit.get(groupKey) || { until: Date.now() + 90_000, reason: 'upstream_unavailable' };
    sendCircuitJson(res, state);
  }
}

function proxyHttp(req, res, url) {
  const policy = req.method === 'GET' ? legacyPolicy(url) : null;
  if (policy) {
    proxyCachedGet(req, res, url, policy).catch(() => {
      if (!res.headersSent) sendCircuitJson(res, { until: Date.now() + 90_000, reason: 'proxy_error' });
    });
    return;
  }
  const upstream = http.request({
    hostname: '127.0.0.1',
    port: CHILD_PORT,
    method: req.method,
    path: req.url,
    headers: { ...req.headers, host: `127.0.0.1:${CHILD_PORT}` },
  }, (upstreamRes) => {
    res.writeHead(upstreamRes.statusCode || 502, upstreamRes.headers);
    upstreamRes.pipe(res);
  });
  upstream.setTimeout(30_000, () => upstream.destroy(new Error('legacy_worker_timeout')));
  upstream.on('error', (error) => {
    if (res.headersSent) return res.end();
    res.writeHead(502, { 'content-type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ ok: false, error: `legacy_worker_unavailable:${error.message}` }));
  });
  req.pipe(upstream);
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  if (url.pathname === '/health') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(JSON.stringify({
      ok: true,
      service: 'kaka-contract-realtime-worker',
      version: STEP_VERSION,
      legacy_worker: '515.1.2',
      protocol: 'kaka.market.realtime.v1',
      providers: ['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate'],
      spot_providers: ['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate'],
      contract_providers: ['binance', 'okx', 'bybit', 'bitget', 'gate'],
      contract_flow: '/api/contract-flow',
      contract_flow_warm: '/api/contract-flow/warm',
      contract_meta: '/api/contract-meta',
      contract_depth: '/api/contract-depth',
      contract_depth_views: ['orderbook', 'trades'],
      contract_liquidation: '/api/contract-liquidation',
      contract_liquidation_periods: ['15m', '1h', '4h', '12h', '24h', '3d', '7d', '14d'],
      contract_liquidation_scope: 'single_provider_single_symbol',
      contract_funding: '/api/contract-funding',
      binance_contract_market_health: '/api/binance-contract-market-health',
      binance_contract_kline_seed_health: '/api/binance-contract-kline-seed-health',
      contract_funding_providers: ['binance', 'okx', 'bybit', 'bitget', 'gate'],
      contract_liquidation_providers: ['binance', 'okx', 'bybit', 'bitget', 'gate'],
      contract_flow_persistence: Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY),
      contract_position_metrics: '/api/contract-flow',
      risk_controls: {
        flow_memory: 'fixed_histogram',
        metric_refresh_seconds: 300,
        partial_retry_seconds: 60,
        partial_retry_limit: 2,
        retention_hours: 72,
        metric_merge: 'coalesce_non_null',
        strict_null_numeric: true,
        app_metric_merge: 'time_and_family_key',
        okx_contract_value: true,
        okx_unit_source: 'v2',
        gate_contract_multiplier: true,
        gate_unit_source: 'v2',
        legacy_rest_cache: true,
        legacy_rest_inflight_coalescing: true,
        legacy_rest_circuit_breaker: true,
        binance_contract_market_transport: 'official_websocket_ticker_bookticker_contract_info',
        binance_contract_market_persistent_snapshot: Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY),
        binance_contract_market_rest_role: 'low_frequency_metadata_refresh_only',
        binance_contract_market_empty_snapshot_never_overwrites: true,
        binance_contract_market_startup_restore: true,
        binance_contract_kline_seed_source: 'official_data_archive_daily_monthly_plus_current_http_and_live_websocket_bridge',
        binance_contract_kline_seed_persistent_snapshot: Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY),
        binance_contract_kline_partial_candidate_validation: true,
        binance_contract_kline_shared_ip_ban_guard: true,
        binance_contract_kline_exact_symbol_first: true,
        binance_contract_kline_http_min_request_gap_ms: 1200,
        binance_contract_kline_parse_official_ban_until: true,
        binance_contract_kline_partial_snapshot_never_persists: true,
        binance_contract_kline_current_day_bridge: true,
        binance_contract_kline_internal_gap_aware_repair: true,
        binance_contract_kline_memory_fast_path_requires_continuity: true,
        binance_contract_kline_live_bridge_on_demand: true,
        binance_contract_kline_gap_diagnostics: true,
        binance_contract_snapshot_routes_bypass_legacy_rest_circuit: true,
        binance_contract_kline_cold_start: 'exact_symbol_kline_first_then_bounded_archive_gap_repair',
        binance_contract_kline_failure_scope: 'symbol_interval_isolated',
        restricted_cooldown_seconds: 1800,
        transient_cooldown_seconds: 90,
        contract_meta_cache_seconds: 30,
        contract_depth_cache_ms: 1200,
        contract_depth_stale_seconds: 20,
        contract_depth_page_visible_only: true,
        binance_contract_depth_transport: 'websocket_public_depth20_100ms',
        binance_contract_trades_transport: 'websocket_market_aggTrade',
        binance_contract_rest_disabled_for_depth: true,
        binance_websocket_endpoint_split_2026: true,
        binance_websocket_hosts: ['fstream.binance.com', 'stream.binancefuture.com'],
        contract_liquidation_page_visible_polling: true,
        contract_liquidation_memory_aggregation: true,
        contract_liquidation_raw_persistence: false,
        contract_liquidation_short_bucket_minutes: 15,
        contract_liquidation_hour_bucket_retention_days: 14,
        contract_liquidation_max_period_days: 14,
        contract_liquidation_dynamic_feed_idle_hours: 24,
        contract_liquidation_dynamic_limit_per_provider: 12,
        liquidation_platform_strict_isolation: true,
        contract_funding_current_and_history: true,
        contract_funding_cache_seconds: 30,
        gate_next_funding_source: 'futures_contract_funding_next_apply',
        liquidation_public_feeds: {
          binance: 'all_market_forceOrder',
          okx: 'public_liquidation-orders',
          bybit: 'public_allLiquidation',
          bitget: 'public_liquidation',
          gate: 'public_liquidates',
        },
      },
      time: new Date().toISOString(),
    }));
    return;
  }

  try {
    if (await handleContractDepth(req, res, url)) return;
    if (await handleContractFunding(req, res, url)) return;
    if (await handleContractLiquidation(req, res, url)) return;
    if (await handleContractFlow(req, res, url)) return;
  } catch (error) {
    if (!res.headersSent) {
      res.writeHead(500, { 'content-type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: false, error: String(error?.message || error) }));
    }
    return;
  }
  proxyHttp(req, res, url);
});

server.on('upgrade', (req, socket, head) => {
  const upstream = http.request({
    hostname: '127.0.0.1',
    port: CHILD_PORT,
    method: 'GET',
    path: req.url,
    headers: { ...req.headers, host: `127.0.0.1:${CHILD_PORT}` },
  });
  upstream.on('upgrade', (upstreamRes, upstreamSocket, upstreamHead) => {
    let response = `HTTP/${upstreamRes.httpVersion} ${upstreamRes.statusCode} ${upstreamRes.statusMessage}\r\n`;
    for (const [name, value] of Object.entries(upstreamRes.headers)) {
      if (Array.isArray(value)) for (const item of value) response += `${name}: ${item}\r\n`;
      else if (value != null) response += `${name}: ${value}\r\n`;
    }
    response += '\r\n';
    socket.write(response);
    if (head?.length) upstreamSocket.write(head);
    if (upstreamHead?.length) socket.write(upstreamHead);
    socket.pipe(upstreamSocket).pipe(socket);
  });
  upstream.on('response', (upstreamRes) => {
    socket.write(`HTTP/1.1 ${upstreamRes.statusCode || 502} ${upstreamRes.statusMessage || 'Bad Gateway'}\r\n\r\n`);
    socket.destroy();
  });
  upstream.on('error', () => socket.destroy());
  upstream.end();
});

function shutdown(signal) {
  console.log(`[Step${STEP_VERSION}] shutdown ${signal}`);
  server.close(() => {
    child.kill('SIGTERM');
    process.exit(0);
  });
  setTimeout(() => process.exit(0), 5000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
server.listen(PORT, '0.0.0.0', () => {
  console.log(`[Step${STEP_VERSION}] proxy + persistent Binance contract market + contract flow + contract depth + single-venue liquidation statistics + five-platform funding listening on 0.0.0.0:${PORT}; legacy=${CHILD_PORT}`);
});
