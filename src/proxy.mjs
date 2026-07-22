import http from 'node:http';
import { spawn } from 'node:child_process';
import { getContractFlowHealth, handleContractFlow } from './contract-flow.mjs';
import { getContractDepthHealth, handleContractDepth } from './contract-depth.mjs';
import { getBinanceLiquidationWsHealth, handleContractLiquidation } from './contract-liquidation.mjs';
import { handleContractFunding } from './contract-funding.mjs';
import { beginBinanceRestShutdown, getBinanceRestGuardHealth, runWithBinanceRequestSignal } from './binance-rest-guard.mjs';
import { getBinanceContractKlineSeedHealth } from './binance-contract-kline-seed.mjs';
import { getBinanceContractKlineRelayHealth } from './binance-contract-kline-relay.mjs';
import { getBinanceMarketRestHealth, handleMarketApi } from './market-rest.mjs';
import { installProviderGovernorFetch, getProviderGovernorHealth, runProviderGovernorSelfTest } from './provider-request-governor.mjs';

const PORT = Number(process.env.PORT || 10000);
const CHILD_PORT = Number(process.env.KAKA_CHILD_PORT || 10001);
const STEP_VERSION = '650.8.15.28';
installProviderGovernorFetch({ role: 'parent-http-api' });
let shuttingDown = false;

const child = spawn(process.execPath, ['src/server.mjs'], {
  env: {
    ...process.env,
    PORT: String(CHILD_PORT),
    KAKA_DISABLE_MARKET_API: '1',
    KAKA_DISABLE_BINANCE_MARKET_START: '1',
    KAKA_DISABLE_BINANCE_REST: '1',
  },
  stdio: 'inherit',
});

child.on('exit', (code, signal) => {
  if (shuttingDown) return;
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
  // Step650.8.15.28：这三条 Binance 合约路由已分别由 WebSocket 快照或官方归档+共享REST守卫+实时桥接提供，
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
  return statusCode === 403 || statusCode === 418 || statusCode === 429 || statusCode === 451 ||
    text.includes('way too many requests') ||
    (text.includes('ip(') && text.includes('banned until')) ||
    text.includes('too many requests');
}

function isUpstreamFailure(statusCode, bodyText) {
  const text = String(bodyText || '').toLowerCase();
  return statusCode >= 500 || statusCode === 408 || statusCode === 403 || statusCode === 418 || statusCode === 429 || statusCode === 451 ||
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


function fetchChildJson(pathname, timeoutMs = 4_000) {
  return new Promise((resolve, reject) => {
    const request = http.get({
      hostname: '127.0.0.1',
      port: CHILD_PORT,
      path: pathname,
      headers: { accept: 'application/json' },
    }, (response) => {
      const chunks = [];
      response.on('data', (chunk) => chunks.push(chunk));
      response.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        if ((response.statusCode || 500) >= 400) {
          reject(new Error(`child_health_${response.statusCode}:${text.slice(0, 160)}`));
          return;
        }
        try { resolve(JSON.parse(text)); }
        catch (_) { reject(new Error('child_health_invalid_json')); }
      });
    });
    request.setTimeout(timeoutMs, () => request.destroy(new Error('child_health_timeout')));
    request.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  if (url.pathname === '/health') {
    const realtimeWsHealth = await fetchChildJson('/ws-health').catch((error) => ({
      ok: false,
      error: String(error?.message || error),
      binance_shared_ws: null,
    }));
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
      contract_depth_health: getContractDepthHealth(),
      contract_flow_health: getContractFlowHealth(),
      binance_liquidation_ws_health: getBinanceLiquidationWsHealth(),
      contract_depth_views: ['orderbook', 'trades'],
      contract_liquidation: '/api/contract-liquidation',
      contract_liquidation_periods: ['15m', '1h', '4h', '12h', '24h', '3d', '7d', '14d'],
      contract_liquidation_scope: 'single_provider_single_symbol',
      contract_funding: '/api/contract-funding',
      contract_funding_health: '/api/contract-funding/health',
      binance_contract_market_health: '/api/binance-contract-market-health',
      binance_contract_kline_seed_health: '/api/binance-contract-kline-seed-health',
      binance_contract_rest_probe: 'retired_step650_8_11',
      binance_contract_validation_reset: 'retired_step650_8_11',
      binance_contract_kline_relay_health: '/api/binance-contract-kline-relay-health',
      binance_contract_kline_relay_validation_start: '/api/binance-contract-kline-relay-validation-start',
      binance_contract_kline_relay_validation_reset: '/api/binance-contract-kline-relay-validation-reset',
      binance_contract_kline_relay: getBinanceContractKlineRelayHealth(),
      binance_rest_guard: getBinanceRestGuardHealth(),
      binance_market_rest_health: getBinanceMarketRestHealth(),
      realtime_ws_health: realtimeWsHealth,
      provider_request_governor: {
        parent: getProviderGovernorHealth(),
        child: realtimeWsHealth?.provider_request_governor || null,
      },
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
        non_binance_provider_request_governor: true,
        non_binance_provider_request_governor_version: '652.1C.2',
        non_binance_governed_providers: ['okx','bybit','bitget','gate','coinbase'],
        non_binance_provider_min_start_gap_ms: 220,
        non_binance_provider_max_concurrent: 2,
        non_binance_provider_max_queue: 96,
        non_binance_global_max_active: 6,
        non_binance_exact_get_inflight_merge: true,
        non_binance_retry_after_honored: true,
        non_binance_unsupported_market_negative_cache_minutes: 15,
        bybit_403_minimum_hard_cooldown_minutes: 10,
        okx_50011_rate_limit_detection: true,
        bitget_429_rate_limit_detection: true,
        gate_rate_limit_reset_header_detection: true,
        coinbase_public_rest_guard_below_official_10rps: true,
        binance_contract_market_transport: 'official_websocket_ticker_bookticker_contract_info_mark_price',
        binance_contract_market_persistent_snapshot: Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY),
        binance_contract_market_rest_role: 'automatic_rest_disabled_websocket_snapshot_only',
        binance_contract_market_empty_snapshot_never_overwrites: true,
        binance_contract_market_startup_restore: true,
        binance_contract_kline_seed_source: 'official_data_archive_plus_authenticated_supabase_edge_exact_kline_relay_plus_live_websocket',
        binance_contract_kline_seed_persistent_snapshot: Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY),
        binance_contract_kline_partial_candidate_validation: true,
        binance_contract_kline_edge_relay_guard: true,
        binance_contract_kline_single_upstream_relay: 'supabase_edge_kaka_binance_contract_kline_relay',
        binance_contract_kline_edge_relay_min_request_gap_ms: 3000,
        binance_contract_aux_edge_relay_min_request_gap_ms: 12000,
        binance_contract_critical_aux_edge_relay_min_request_gap_ms: 2500,
        binance_contract_long_short_first_paint: 'critical_edge_relay_global_first',
        binance_contract_long_short_first_paint_wait_ms: 3200,
        binance_contract_long_short_first_paint_limit: 3,
        binance_contract_long_short_fast_retry_without_app_restart: true,
        binance_contract_kline_edge_relay_priority: true,
        contract_api_route_ownership_fixed: true,
        generic_market_handler_intercepts_contract_routes: false,
        binance_contract_kline_first_paint_max_rows: 240,
        binance_contract_kline_parse_official_ban_until: true,
        binance_contract_rest_guard_persistent_snapshot: true,
        binance_rest_guard_process_scope: 'single_parent_process',
        binance_render_direct_rest_hard_disabled: true,
        binance_kline_edge_relay_only: true,
        binance_kline_edge_relay_configured: getBinanceContractKlineRelayHealth().relay_configured,
        binance_kline_edge_relay_validation_sequence: getBinanceContractKlineRelayHealth().validation_sequence,
        binance_validation_limit: 240,
        binance_validation_end_time_client_controlled: false,
        binance_probe_reset_race_cancelled_by_epoch: true,
        binance_duplicate_probe_token_sharing: false,
        binance_client_abort_blocks_queued_rest: true,
        binance_kline_snapshot_persisted_before_validation_advance: true,
        binance_kline_snapshot_io_timeout_ms: 8000,
        binance_max_bridge_rest_calls_per_api_request: 0,
        binance_max_edge_relay_calls_per_api_request: 1,
        binance_rest_single_instance_required: true,
        binance_rest_multi_instance_supported: false,
        binance_rest_single_instance_healthy: getBinanceRestGuardHealth().single_instance_rest_healthy,
        binance_rest_render_instance_count: getBinanceRestGuardHealth().render_instance_count,
        binance_rest_render_instance_count_verified_by_dns: getBinanceRestGuardHealth().render_instance_count_verified_by_dns,
        binance_rest_render_instance_safety_strategy: getBinanceRestGuardHealth().render_instance_safety_strategy,
        binance_rest_render_expected_plan: getBinanceRestGuardHealth().render_expected_plan,
        binance_rest_render_free_single_instance_guarantee: getBinanceRestGuardHealth().render_free_single_instance_guarantee,
        binance_rest_render_discovery_available: getBinanceRestGuardHealth().render_discovery_available,
        binance_rest_render_discovery_api: getBinanceRestGuardHealth().render_discovery_api,
        binance_rest_render_discovery_timeout_ms: getBinanceRestGuardHealth().render_discovery_timeout_ms,
        binance_rest_instance_startup_grace_ms: getBinanceRestGuardHealth().render_instance_startup_rest_grace_ms,
        binance_rest_instance_check_forced_before_every_request: getBinanceRestGuardHealth().instance_check_forced_before_every_rest,
        binance_rest_shutdown_blocks_new_requests: true,
        legacy_child_market_api_enabled: false,
        legacy_child_binance_rest_enabled: false,
        binance_rest_guard_all_callers: ['contract_kline','contract_funding','contract_meta','position_metrics','legacy_contract_agg_trades','spot_universe','spot_ticker','spot_kline','spot_agg_trades'],
        binance_contract_rest_migration_quarantine_until: '2026-07-17T20:39:46.570Z',
        binance_contract_rest_multi_host_retry_disabled: true,
        binance_contract_rest_post_ban_probe_required: true,
        binance_contract_rest_normal_callers_blocked_until_probe: true,
        binance_rest_validation_mode_after_probe: 'token_locked_kline_bridge_only_until_staged_validation_passes',
        binance_rest_validation_token_required: true,
        binance_rest_validation_admin_key_configured: getBinanceRestGuardHealth().validation_admin_key_configured,
        binance_rest_validation_sequence: getBinanceContractKlineRelayHealth().validation_sequence,
        binance_rest_validation_interval: getBinanceContractKlineRelayHealth().validation_interval,
        binance_rest_validation_session_budget: 2,
        binance_rest_validation_max_calls_per_api_request: 1,
        binance_rest_validation_session_ttl_ms: getBinanceRestGuardHealth().validation_session_ttl_ms,
        binance_rest_validation_admin_reset_enabled: true,
        binance_rest_probe_uncertain_failure_cooldown: true,
        binance_rest_probe_state_durable_before_token_return: true,
        binance_rest_validation_state_durable_before_network: true,
        binance_rest_probe_requires_persistence: true,
        binance_rest_validation_requires_persistence: true,
        binance_rest_probe_max_used_weight_1m: getBinanceRestGuardHealth().probe_max_used_weight_1m,
        binance_internal_guard_error_never_treated_as_upstream_418: true,
        binance_spot_rest_uses_same_shared_guard: true,
        binance_contract_rest_max_pending_requests: 6,
        binance_contract_rest_max_queue_wait_ms: 25000,
        binance_contract_rest_queue_is_bounded: true,
        binance_contract_rest_queue_releases_on_guard_error: true,
        binance_contract_rest_persistence_flush_on_restriction: true,
        binance_rest_persistence_failure_blocks_network: true,
        binance_rest_guard_persistence_timeout_ms: getBinanceRestGuardHealth().persistence_timeout_ms,
        binance_rest_guard_restore_failure_blocks_network: true,
        binance_rest_guard_restore_healthy: getBinanceRestGuardHealth().restore_healthy,
        binance_rest_guard_restore_errors: getBinanceRestGuardHealth().restore_errors,
        binance_validation_reset_method: 'POST',
        binance_contract_kline_partial_snapshot_never_persists: true,
        binance_contract_kline_current_day_bridge: true,
        binance_contract_kline_internal_gap_aware_repair: true,
        binance_contract_kline_memory_fast_path_requires_continuity: true,
        binance_contract_kline_live_bridge_on_demand: true,
        binance_contract_kline_live_ws_connect_gap_ms: getBinanceContractKlineSeedHealth().live_ws_connect_gap_ms,
        binance_contract_kline_live_ws_max_connect_attempts_5m: getBinanceContractKlineSeedHealth().live_ws_max_connect_attempts_5m,
        binance_contract_kline_gap_diagnostics: true,
        binance_contract_snapshot_routes_bypass_legacy_rest_circuit: true,
        binance_contract_kline_cold_start: 'persistent_snapshot_then_priority_exact_edge_240_first_paint_then_archive_pages_then_live_websocket',
        binance_contract_kline_failure_scope: 'symbol_interval_isolated',
        binance_rest_operating_modes: ['render_direct_rest_hard_disabled'],
        binance_rest_admin_key_rotation_invalidates_sessions: true,
        binance_rest_restricted_statuses: [403,418,429,451],
        binance_rest_success_weight_checked_on_every_response: true,
        binance_rest_normal_max_used_weight_1m: getBinanceRestGuardHealth().normal_max_used_weight_1m,
        binance_rest_validation_max_used_weight_1m: getBinanceRestGuardHealth().validation_max_used_weight_1m,
        binance_spot_market_data_host: 'data-api.binance.vision',
        binance_spot_rest_shared_cache: true,
        binance_contract_second_history_max_rest_pages: 1,
        one_second_empty_bucket_owner: 'app_local_visible_detail_only',
        one_second_render_ws_mode: 'official_real_trades_only',
        one_second_render_synthetic_heartbeat: false,
        binance_archive_global_max_active: 3,
        binance_archive_global_max_pending: 12,
        binance_one_second_synthetic_gap_fill: false,
        binance_app_ws_shared_by_market_symbol_interval: true,
        binance_app_ws_max_shared_streams: 64,
        binance_futures_ws_route_migration: 'market_public_split',
        binance_futures_ws_legacy_root_disabled: true,
        binance_futures_ws_market_path: '/market',
        binance_futures_ws_public_path: '/public',
        binance_app_ws_max_connect_attempts_5m: Number(realtimeWsHealth?.binance_shared_ws?.max_connect_attempts_5m || 60),
        binance_app_ws_max_total_clients: 1000,
        binance_app_ws_max_clients_per_stream: 250,
        binance_app_ws_max_client_buffered_bytes: Number(realtimeWsHealth?.binance_shared_ws?.max_client_buffered_bytes || 1000000),
        binance_app_ws_max_clients_per_ip: Number(realtimeWsHealth?.binance_shared_ws?.max_clients_per_ip || 50),
        binance_app_ws_max_streams_per_ip: Number(realtimeWsHealth?.binance_shared_ws?.max_streams_per_ip || 16),
        binance_app_ws_max_connect_attempts_per_ip_1m: Number(realtimeWsHealth?.binance_shared_ws?.max_connect_attempts_per_ip_1m || 60),
        binance_app_ws_client_ip_source: 'render_x_forwarded_for_first_entry',
        binance_app_ws_trade_1s_shared_aggregator: true,
        binance_depth_ws_max_symbols: getContractDepthHealth().binance_ws_max_symbols,
        binance_depth_ws_connect_gap_ms: getContractDepthHealth().binance_ws_connect_gap_ms,
        binance_depth_ws_max_connect_attempts_5m: getContractDepthHealth().binance_ws_max_connect_attempts_5m,
        restricted_cooldown_policy: 'official_ban_until_or_retry_after_plus_90_seconds',
        transient_cooldown_seconds: 90,
        contract_meta_cache_seconds: 30,
        binance_contract_meta_first_paint_transport: 'official_mark_price_websocket',
        binance_contract_open_interest_first_paint: 'stale_cache_then_critical_background_edge_relay',
        contract_flow_first_paint_waits_for_full_metrics: false,
        contract_flow_valid_symbol_partial_response_status: 200,
        contract_depth_cache_ms: 1200,
        contract_depth_stale_seconds: 20,
        contract_depth_page_visible_only: true,
        usdc_bottom_menu_native_identity: true,
        usdc_contract_depth_native_identity: true,
        usdc_contract_funding_native_identity: true,
        usdc_contract_flow_native_identity: true,
        usdc_contract_liquidation_native_identity: true,
        spot_depth_render_fallback_providers: ['coinbase','okx','bybit','bitget','gate'],
        spot_depth_render_binance_rest_unchanged: true,
        usdc_contract_native_providers: ['binance','bybit','bitget'],
        binance_contract_depth_transport: 'official_combined_websocket_depth20_100ms',
        binance_contract_trades_transport: 'official_combined_websocket_aggTrade',
        binance_contract_quiet_trade_stream_returns_empty_200: true,
        binance_contract_rest_disabled_for_depth: true,
        binance_websocket_endpoint_split_2026: true,
        binance_websocket_hosts: ['fstream.binance.com/market', 'fstream.binance.com/public', 'stream.binance.com:9443'],
        binance_websocket_production_only: true,
        binance_flow_ws_max_active_streams: getContractFlowHealth().binance_max_active_streams,
        binance_flow_ws_max_connect_attempts_5m: getContractFlowHealth().binance_ws_max_connect_attempts_5m,
        binance_market_ws_max_connect_attempts_5m: 15,
        binance_liquidation_ws_max_connect_attempts_5m: getBinanceLiquidationWsHealth().max_connect_attempts_5m,
        binance_ws_designed_aggregate_connect_attempts_5m: 185,
        binance_ws_official_ip_connect_attempt_reference_5m: 300,
        binance_ws_designed_headroom_attempts_5m: 115,
        binance_ws_designed_max_upstream_connections: 164,
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
        binance_contract_funding_current_transport: 'official_mark_price_websocket',
        binance_contract_funding_history_transport: 'authenticated_edge_relay_background',
        binance_contract_funding_first_paint_waits_for_history: false,
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

  if (url.pathname === '/api/provider-governor/health') {
    const childHealth = await fetchChildJson('/ws-health').catch((error) => ({
      ok: false,
      error: String(error?.message || error),
      provider_request_governor: null,
    }));
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(JSON.stringify({
      ok: true,
      version: STEP_VERSION,
      provider_request_governor: {
        parent: getProviderGovernorHealth(),
        child: childHealth?.provider_request_governor || null,
      },
      time: new Date().toISOString(),
    }));
    return;
  }

  if (url.pathname === '/api/provider-governor/self-test') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(JSON.stringify({
      ok: true,
      version: STEP_VERSION,
      self_test: runProviderGovernorSelfTest(),
      time: new Date().toISOString(),
    }));
    return;
  }

  if (url.pathname === '/api/realtime-ws-health') {
    try {
      const payload = await fetchChildJson('/ws-health');
      res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
      res.end(JSON.stringify(payload));
    } catch (error) {
      res.writeHead(503, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
      res.end(JSON.stringify({ ok: false, error: String(error?.message || error) }));
    }
    return;
  }

  const requestAbortController = new AbortController();
  const abortQueuedWork = () => {
    if (!res.writableEnded && !requestAbortController.signal.aborted) requestAbortController.abort();
  };
  req.once('aborted', abortQueuedWork);
  res.once('close', abortQueuedWork);
  try {
    // Step650.8.15.28: all HTTP market endpoints run in the parent process so Binance
    // Spot/Contract REST, probe, Kline validation, funding, and metrics share one
    // in-memory guard and one bounded queue. A disconnected client can cancel only
    // queued/paced work; an already-started upstream request is still fully observed.
    const handled = await runWithBinanceRequestSignal(requestAbortController.signal, async () => {
      if (await handleMarketApi(req, res, url)) return true;
      if (await handleContractDepth(req, res, url)) return true;
      if (await handleContractFunding(req, res, url)) return true;
      if (await handleContractLiquidation(req, res, url)) return true;
      if (await handleContractFlow(req, res, url)) return true;
      return false;
    });
    if (handled) return;
  } catch (error) {
    if (!res.headersSent) {
      res.writeHead(500, { 'content-type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: false, error: String(error?.message || error) }));
    }
    return;
  } finally {
    req.removeListener('aborted', abortQueuedWork);
    res.removeListener('close', abortQueuedWork);
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
  if (shuttingDown) return;
  shuttingDown = true;
  beginBinanceRestShutdown(`shutdown:${signal}`);
  console.log(`[Step${STEP_VERSION}] shutdown ${signal}; new Binance REST blocked immediately`);
  server.close(() => {
    child.kill('SIGTERM');
    process.exit(0);
  });
  setTimeout(() => {
    try { child.kill('SIGTERM'); } catch (_) {}
    process.exit(0);
  }, 5000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
server.listen(PORT, '0.0.0.0', () => {
  console.log(`[Step${STEP_VERSION}] proxy + persistent Binance contract market + contract flow + contract depth + single-venue liquidation statistics + five-platform funding listening on 0.0.0.0:${PORT}; legacy=${CHILD_PORT}`);
});
