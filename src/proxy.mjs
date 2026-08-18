import http from 'node:http';
import { spawn } from 'node:child_process';
import { getContractFlowHealth, getContractFocusPoolHealth, getContractDeepSharedHealth, startDeepMarketBridge } from './deep-market-bridge.mjs';
import { getContractDepthHealth, handleContractDepth } from './contract-depth.mjs';
import { getBinanceLiquidationWsHealth, getContractLiquidationPersistenceHealth, startLiquidationBridge } from './liquidation-bridge.mjs';
import { handleContractFunding, startContractFundingHistoryMaintainer } from './contract-funding.mjs';
import { beginBinanceRestShutdown, getBinanceRestGuardHealth, runWithBinanceRequestSignal } from './binance-rest-guard.mjs';
import { getBinanceContractKlineSeedHealth } from './binance-contract-kline-seed.mjs';
import { getBinanceContractKlineRelayHealth } from './binance-contract-kline-relay.mjs';
import { getBinanceMarketRestHealth, handleMarketApi } from './market-rest.mjs';
import { getSpotCurrentSnapshotHealth, handleSpotCurrentSnapshot, startSpotCurrentSnapshotScanner } from './spot-current-snapshot.mjs';
import { getSpotExactTickerHealth, handleSpotExactTicker } from './spot-exact-ticker.mjs';
import { getSpotFlowHistoryHealth, handleSpotFlowHistory } from './spot-flow-history.mjs';
import { getSpotFlowSnapshotHealth, handleSpotFlowSnapshot } from './spot-flow-snapshot.mjs';
import { getMarketLightSnapshotHealth, startMarketLightBridge } from './market-light-bridge.mjs';
import { getContractBasisHealth, handleContractBasis, startContractBasisScanner } from './contract-basis.mjs';
import { getSourceCapabilityRegistryHealth, handleSourceCapabilityRegistry } from './source-capability-registry.mjs';
import {
  getBinanceAdvancedStatsHealth,
  getBitgetAdvancedStatsHealth,
  getGateAdvancedStatsHealth,
  getOkxAdvancedStatsHealth,
  getBybitAdvancedStatsHealth,
  getDerivativesPublicHealth,
  getHistoryLifecycleHealth,
  startSlowStatsBridge,
} from './slow-stats-bridge.mjs';
import { installProviderGovernorFetch, getProviderGovernorHealth, runProviderGovernorSelfTest } from './provider-request-governor.mjs';
import { startCollectorIsolationSupervisor, proxyIsolatedCollectorRequest, requestIsolatedJson, getCollectorIsolationHealth, stopCollectorIsolationSupervisor } from './collector-isolation.mjs';

import { getCmeExpirySharedHealth, handleCmeExpirySharedCalendar, startCmeExpirySharedCollector } from './cme-expiry-shared-calendar.mjs';
const PORT = Number(process.env.PORT || 10000);
const CHILD_PORT = Number(process.env.KAKA_CHILD_PORT || 10001);
const STEP_VERSION = '650.8.15.165';
installProviderGovernorFetch({ role: 'parent-http-api' });
startCollectorIsolationSupervisor();
startMarketLightBridge();
startLiquidationBridge();
startDeepMarketBridge();
startSlowStatsBridge();
startContractFundingHistoryMaintainer();
startSpotCurrentSnapshotScanner();
startContractBasisScanner();
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
  // Step650.8.15.33：这三条 Binance 合约路由已分别由 WebSocket 快照或官方归档+共享REST守卫+实时桥接提供，
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
  // Step1004.12: Render Edge Caching will be enabled with Cacheable file types = All files.
  // Keep every route private/no-store by default; only explicitly approved shared snapshot
  // responses attach CDN-Cache-Control, which Render gives precedence over Cache-Control.
  if (!res.hasHeader('cache-control')) res.setHeader('cache-control', 'no-store');
  if (url.pathname === '/health') {
    const realtimeWsHealth = await fetchChildJson('/ws-health').catch((error) => ({
      ok: false,
      error: String(error?.message || error),
      binance_shared_ws: null,
    }));
    const exchangeAssetsState = await requestIsolatedJson('exchange-assets', '/_isolated/state', 750).catch((error) => ({
      ok: false,
      collector_role: 'exchange-assets',
      error: String(error?.message || error),
      asset_market: null,
      asset_klines: null,
    }));
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(JSON.stringify({
      ok: true,
      service: 'kaka-contract-realtime-worker',
      version: STEP_VERSION,
      step1028_7_http_transport: {
        keep_alive_timeout_ms: server.keepAliveTimeout,
        headers_timeout_ms: server.headersTimeout,
        max_requests_per_socket: server.maxRequestsPerSocket,
        inbound_tls_terminated_by_render: true,
        exchange_collectors_unchanged: true,
        user_reads_add_exchange_requests: false,
      },
      legacy_worker: '515.1.2',
      protocol: 'kaka.market.realtime.v1',
      providers: ['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate'],
      spot_providers: ['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate'],
      contract_providers: ['binance', 'okx', 'bybit', 'bitget', 'gate'],
      cme_expiry_shared_health: '/api/calendar/cme-expiry/health',
      cme_expiry_shared_refresh: '/api/calendar/cme-expiry/refresh',
      cme_expiry_shared_state: getCmeExpirySharedHealth(),
      contract_flow: '/api/contract-flow',
      contract_flow_warm: '/api/contract-flow/warm',
      contract_flow_market_snapshot: '/api/contract-flow/market-snapshot',
      contract_flow_current_snapshot: '/api/contract-flow/current-snapshot',
      spot_current_snapshot: '/api/spot-market/current-snapshot',
      spot_current_snapshot_health: '/api/spot-market/health',
      spot_current_snapshot_state: getSpotCurrentSnapshotHealth(),
      market_light_current_snapshot: '/api/market-light/current-snapshot',
      market_light_health: '/api/market-light/health',
      market_light_state: getMarketLightSnapshotHealth(),
      crypto_sector_professional_current_snapshot: '/api/crypto-sector-professional/current-snapshot',
      crypto_sector_professional_health: '/api/crypto-sector-professional/health',
      crypto_sector_professional_state:
        getMarketLightSnapshotHealth()?.crypto_sector_professional || null,
      collector_isolation: getCollectorIsolationHealth(),
      collector_isolation_first_batch: getCollectorIsolationHealth(),
      source_capabilities_current_snapshot: '/api/source-capabilities/current-snapshot',
      source_capabilities_health: '/api/source-capabilities/health',
      history_lifecycle_current_snapshot: '/api/history-lifecycle/current-snapshot',
      history_lifecycle_health: '/api/history-lifecycle/health',
      source_capabilities_state: getSourceCapabilityRegistryHealth(),
      derivatives_public_current_snapshot: '/api/derivatives-public/current-snapshot',
      derivatives_public_health: '/api/derivatives-public/health',
      derivatives_public_state: getDerivativesPublicHealth(),
      gate_advanced_current_snapshot: '/api/gate-advanced/current-snapshot',
      gate_advanced_health: '/api/gate-advanced/health',
      gate_advanced_state: getGateAdvancedStatsHealth(),
      okx_advanced_current_snapshot: '/api/okx-advanced/current-snapshot',
      okx_advanced_health: '/api/okx-advanced/health',
      okx_advanced_state: getOkxAdvancedStatsHealth(),
      bybit_advanced_current_snapshot: '/api/bybit-advanced/current-snapshot',
      bybit_advanced_health: '/api/bybit-advanced/health',
      bybit_advanced_state: getBybitAdvancedStatsHealth(),
      bybit_order_price_limit_current_snapshot: '/api/bybit-advanced/current-snapshot?symbol=BTCUSDT',
      bybit_order_price_limit_official_field_mapping: 'buyLmt=highest_bid_price;sellLmt=lowest_ask_price',
      bybit_order_price_limit_user_reads_scale_exchange_upstream: false,
      contract_basis_current_snapshot: '/api/contract-basis/current-snapshot',
      contract_basis_health: '/api/contract-basis/health',
      contract_basis_state: getContractBasisHealth(),
      contract_focus_pool_current_snapshot: '/api/contract-focus-pool/current-snapshot',
      contract_focus_pool_health: '/api/contract-focus-pool/health',
      contract_focus_pool_state: getContractFocusPoolHealth(),
      contract_deep_shared_current_snapshot: '/api/contract-deep-shared/current-snapshot',
      contract_deep_shared_health: '/api/contract-deep-shared/health',
      contract_deep_shared_state: getContractDeepSharedHealth(),
      contract_rpi_shared_current_snapshot: '/api/contract-rpi-shared/current-snapshot',
      contract_rpi_shared_health: '/api/contract-rpi-shared/health',
      contract_rpi_shared_providers: ['okx', 'bybit', 'bitget'],
      contract_rpi_shared_standard_depth_kept_separate: true,
      contract_rpi_shared_user_reads_scale_exchange_upstream: false,
      spot_exact_ticker: '/api/spot-market/exact-ticker',
      spot_exact_ticker_health: '/api/spot-market/exact-ticker-health',
      spot_exact_ticker_state: getSpotExactTickerHealth(),
      asset_klines: '/api/asset-klines',
      asset_klines_health: '/api/asset-klines/health',
      asset_klines_self_test: '/api/asset-klines/self-test',
      asset_klines_state: exchangeAssetsState?.asset_klines || null,
      asset_market: '/api/asset-market',
      asset_market_tickers: '/api/asset-market/tickers',
      asset_market_health: '/api/asset-market/health',
      asset_market_self_test: '/api/asset-market/self-test',
      asset_market_state: exchangeAssetsState?.asset_market || null,
      all_market_second_history_end_time_pagination: true,
      all_market_second_history_latest_audit_cases: 11,
      all_market_second_history_older_target_cases: 11,
      all_market_second_history_verified_older_cases_target: 11,
      nonexistent_pair_kline_identity_preflight: true,
      nonexistent_pair_kline_upstream_short_circuit: true,
      nonexistent_pair_kline_returns_exact_honest_empty: true,
      nonexistent_pair_kline_negative_ttl_seconds: 60,
      nonexistent_pair_kline_positive_ttl_seconds: 300,
      okx_second_history_cursor_mode: 'type_2_timestamp_after',
      bybit_second_history: '/api/bybit-second-history-health',
      bybit_second_history_state:
        realtimeWsHealth?.bybit_second_history || null,
      bybit_recent_trade_time_range_parameters_used: false,
      one_second_history_app_direct_left_backfill_required: true,
      coinbase_one_second_realtime_source: 'coinbase_exchange_ticker_per_match_plus_heartbeat',
      coinbase_one_second_empty_seconds_owned_by_app: true,
      coinbase_spot_usdt_realtime_supported: true,
      coinbase_all_directory_quotes_realtime_supported: true,
      all_provider_asset_quote_discovery_uses_shared_exact_quote_set: true,
    mixed_quote_ticker_requests_grouped_by_exact_quote: true,
    mixed_quote_ticker_max_quote_groups: 8,
    mixed_quote_ticker_merge_identity: 'provider_market_symbol',
    okx_usdc_contract_identity_retired_after_official_delisting: true,
    okx_current_contract_quotes: ['USDT', 'USD'],
      coinbase_usdt_directory_not_cross_aliased_to_usd: true,
      coinbase_exact_ticker_official_directory_preflight: true,
      coinbase_nonexistent_product_returns_honest_empty: true,
      coinbase_nonexistent_product_writes_negative_cache: true,
      coinbase_mixed_batch_absent_symbol_does_not_fail_valid_neighbors: true,
      coinbase_eur_realtime_supported: true,
      coinbase_trade_history_page_limit: 1000,
      coinbase_trade_history_max_pages_per_request: 12,
      coinbase_trade_history_cursor_checkpoint_reuse: true,
      coinbase_trade_history_no_three_minute_wall: true,
      coinbase_asset_ticker_shared_live_source: 'exchange_product_ticker_last_trade_plus_stats',
      asset_market_tab_count_uses_visible_rows: true,
      one_second_history_drag_window_target_seconds: 90,
      coinbase_one_second_history_drag_window_target_seconds: 300,
      coinbase_one_second_history_drag_max_pages: 8,
      bybit_second_history_indexed_bucket_updates: true,
      bybit_hot_targets_parallel_start: true,
      bybit_spot_recent_trade_rest_limit: 60,
      bybit_spot_recent_trade_public_older_cursor_available: false,
      bybit_shallow_latest_page_reserves_verified_older_rows: true,
      bybit_btc_eth_quote_pairs_prestarted_from_official_directory: true,
      one_second_history_window_passes_by_time_span_not_page_count: true,
      spot_flow_shared_history: '/api/spot-flow/history',
      spot_flow_shared_history_health: '/api/spot-flow/history-health',
      spot_flow_shared_history_state: getSpotFlowHistoryHealth(),
      spot_flow_shared_snapshot: '/api/spot-flow/snapshot',
      spot_flow_shared_snapshot_health: '/api/spot-flow/snapshot-health',
      spot_flow_shared_snapshot_state: getSpotFlowSnapshotHealth(),
      contract_meta: '/api/contract-meta',
      contract_depth: '/api/contract-depth',
      contract_depth_health: getContractDepthHealth(),
      contract_flow_health: getContractFlowHealth(),
      binance_liquidation_ws_health: getBinanceLiquidationWsHealth(),
      contract_depth_views: ['orderbook', 'trades'],
      contract_liquidation: '/api/contract-liquidation',
      contract_liquidation_history: '/api/contract-liquidation/history',
      contract_liquidation_current_snapshot: '/api/contract-liquidation/current-snapshot',
      contract_liquidation_market_snapshot: '/api/contract-liquidation/market-snapshot',
      contract_liquidation_heatmap: '/api/contract-liquidation/heatmap',
      contract_liquidation_health: '/api/contract-liquidation/health',
      contract_liquidation_persistence_health: getContractLiquidationPersistenceHealth(),
      contract_liquidation_periods: ['15m', '1h', '4h', '12h', '24h', '3d', '7d', '14d'],
      contract_liquidation_history_intervals_step997: ['1m', '5m', '15m', '1H', '6H', '24H'],
      contract_liquidation_history_default_backward_compatible: '1h',
      contract_liquidation_gate_liq_orders_background_only: true,
      contract_liquidation_scope: 'official_maximum_five_provider_market_layer_plus_focused_five_by_three_fallback_plus_single_symbol_plus_realized_price_heatmap',
      contract_liquidation_heatmap_semantics: 'actual_realized_events_only_not_estimated_risk',
      market_light_rollout: 'step980_6_3_4_1_binance_changed_only_ticker_fix_ws_api_latest_price_baseline_no_app_cutover',
      contract_funding: '/api/contract-funding',
      contract_funding_history: '/api/contract-funding/history',
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
        derivatives_public_shared_background: true,
        derivatives_public_collector_role: 'slow-stats',
        derivatives_public_user_reads_trigger_exchange_requests: false,
        derivatives_public_reads_scale_with_users: false,
        derivatives_public_official_only: true,
        derivatives_public_cross_provider_substitution: false,
        derivatives_public_cross_quote_substitution: false,
        derivatives_public_missing_stays_null: true,
        binance_option_direct_rest_added: false,
        binance_option_market_ws_added: false,
        bitget_stockplus_options_excluded_from_crypto_scope: true,
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
        nonexistent_pair_kline_identity_preflight: true,
        nonexistent_pair_kline_exact_key: 'provider_market_symbol',
        nonexistent_pair_kline_upstream_short_circuit: true,
        nonexistent_pair_kline_returns_exact_honest_empty: true,
        nonexistent_pair_kline_negative_ttl_seconds: 60,
        nonexistent_pair_kline_positive_ttl_seconds: 300,
        nonexistent_pair_directory_failure_never_written_as_negative: true,
        bybit_403_minimum_hard_cooldown_minutes: 10,
        okx_50011_rate_limit_detection: true,
        bitget_429_rate_limit_detection: true,
        gate_rate_limit_reset_header_detection: true,
        gate_spot_second_history_uses_bounded_backward_time_windows: true,
        gate_spot_second_history_strict_end_time_boundary: true,
        gate_spot_second_history_initial_window_hours: [6, 18],
        gate_spot_second_history_additional_daily_windows: 6,
        gate_spot_second_history_total_bounded_lookback_hours: 174,
        gate_spot_second_history_uses_one_minute_activity_anchors: true,
        gate_spot_second_history_activity_anchor_max: 8,
        gate_spot_second_history_one_minute_candles_never_promoted_to_one_second: true,
        gate_spot_second_history_last_id_cursor_retired_after_production_audit: true,
        gate_spot_second_history_large_from_to_range_retired_after_upstream_502: true,
        coinbase_public_rest_guard_below_official_10rps: true,
        coinbase_one_second_realtime_transport: 'official_exchange_ticker_per_match_plus_heartbeat',
        coinbase_spot_usdt_realtime_supported: true,
      coinbase_all_directory_quotes_realtime_supported: true,
      all_provider_asset_quote_discovery_uses_shared_exact_quote_set: true,
      mixed_quote_ticker_requests_grouped_by_exact_quote: true,
      mixed_quote_ticker_max_quote_groups: 8,
      mixed_quote_ticker_merge_identity: 'provider_market_symbol',
      okx_usdc_contract_identity_retired_after_official_delisting: true,
      okx_current_contract_quotes: ['USDT', 'USD'],
      coinbase_usdt_directory_not_cross_aliased_to_usd: true,
      coinbase_exact_ticker_official_directory_preflight: true,
      coinbase_nonexistent_product_returns_honest_empty: true,
      coinbase_nonexistent_product_writes_negative_cache: true,
      coinbase_mixed_batch_absent_symbol_does_not_fail_valid_neighbors: true,
      coinbase_eur_realtime_supported: true,
      coinbase_trade_history_page_limit: 1000,
      coinbase_trade_history_max_pages_per_request: 12,
      coinbase_trade_history_cursor_checkpoint_reuse: true,
      coinbase_trade_history_no_three_minute_wall: true,
        coinbase_asset_ticker_shared_live_source: 'exchange_product_ticker_last_trade_plus_stats',
        coinbase_asset_ticker_app_fresh_cache_ms: 1200,
        asset_market_tab_count_uses_visible_rows: true,
        coinbase_public_usdc_alias_to_usd_for_market_channels: true,
        one_second_wall_clock_transport_reconnect_independent: true,
        one_second_history_drag_window_target_seconds: 90,
      coinbase_one_second_history_drag_window_target_seconds: 300,
      coinbase_one_second_history_drag_max_pages: 8,
        one_second_history_drag_max_pages: 4,
        bybit_second_history_indexed_bucket_updates: true,
        bybit_hot_targets_parallel_start: true,
        bybit_spot_recent_trade_rest_limit: 60,
        bybit_spot_recent_trade_public_older_cursor_available: false,
        bybit_shallow_latest_page_reserves_verified_older_rows: true,
        bybit_btc_eth_quote_pairs_prestarted_from_official_directory: true,
        one_second_history_window_passes_by_time_span_not_page_count: true,
        binance_contract_market_transport: 'official_ws_api_all_symbol_latest_price_identity_plus_changed_ticker_contract_info_mark_price_enrichment',
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
        binance_edge_relay_dispatch_rechecks_priority_before_grant: true,
        binance_edge_relay_reserved_high_priority_pending: 2,
        binance_edge_relay_max_auxiliary_pending: 4,
        binance_edge_relay_background_work_deferred_while_busy: true,
        binance_edge_relay_high_priority_preempts_lower_priority_when_full: true,
        binance_edge_relay_background_metrics_priority: -20,
        binance_edge_relay_background_funding_priority: -10,
        binance_edge_relay_timeout_counters_by_lane_and_source: true,
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
        data_page_spot_current_snapshot_backend_shared: true,
        data_page_spot_current_snapshot_endpoint: '/api/spot-market/current-snapshot',
        market_light_full_directory_snapshot_endpoint: '/api/market-light/current-snapshot',
        market_light_snapshot_reads_scale_with_users: false,
        market_light_failed_refresh_retains_last_verified_rows: true,
        source_capability_registry_endpoint: '/api/source-capabilities/current-snapshot',
        source_capability_registry_official_first: true,
        source_capability_registry_cross_provider_substitution: false,
        source_capability_registry_cross_quote_substitution: false,
        source_capability_registry_user_reads_scale_exchange_upstream: false,
        binance_advanced_official_stats_endpoint: '/api/binance-advanced/current-snapshot',
        binance_advanced_official_stats_health_endpoint: '/api/binance-advanced/health',
        binance_advanced_official_stats_ready: getBinanceAdvancedStatsHealth().ready,
        binance_advanced_edge_relay_only: true,
        binance_advanced_render_direct_rest_enabled: false,
        binance_advanced_user_reads_scale_exchange_upstream: false,
        v46_binance_open_interest_history_formalized: true,
        v46_binance_official_taker_focus15_shared: true,
        v46_bitget_official_liquidation_history_reconcile: true,
        binance_advanced_adl_official_update_interval_minutes: 30,
        bitget_advanced_official_stats_endpoint: '/api/bitget-advanced/current-snapshot',
        bitget_advanced_official_stats_health_endpoint: '/api/bitget-advanced/health',
        bitget_advanced_official_stats_ready: getBitgetAdvancedStatsHealth().ready,
        bitget_advanced_user_reads_scale_exchange_upstream: false,
        gate_advanced_official_stats_endpoint: '/api/gate-advanced/current-snapshot',
        gate_advanced_official_stats_health_endpoint: '/api/gate-advanced/health',
        gate_advanced_official_stats_ready: getGateAdvancedStatsHealth().ready,
        gate_advanced_user_reads_scale_exchange_upstream: false,
        okx_advanced_official_stats_endpoint: '/api/okx-advanced/current-snapshot',
        okx_advanced_official_stats_health_endpoint: '/api/okx-advanced/health',
        okx_advanced_official_stats_ready: getOkxAdvancedStatsHealth().ready,
        okx_advanced_market_light_oi_reused_only: true,
        okx_advanced_market_light_oi_additional_exchange_requests: 0,
        okx_advanced_adl_warning_one_shared_connection: true,
        okx_advanced_adl_warning_normal_state_not_fabricated: true,
        okx_advanced_user_reads_scale_exchange_upstream: false,
        bybit_advanced_official_stats_endpoint: '/api/bybit-advanced/current-snapshot',
        bybit_advanced_official_stats_health_endpoint: '/api/bybit-advanced/health',
        bybit_advanced_official_stats_ready: getBybitAdvancedStatsHealth().ready,
        bybit_advanced_current_oi_reuses_market_light: true,
        bybit_advanced_user_reads_scale_exchange_upstream: false,
        step1024_okx_current_dynamic_price_limit: true,
        step1024_okx_option_official_iv_greeks: true,
        step1024_bybit_index_price_components_shared: true,
        step1024_bybit_option_historical_volatility_shared: true,
        step1024_bybit_spot_official_depth_limit: 1000,
        step1024_coinbase_product_status_restrictions_session: true,
        step1024_coinbase_product_facts_backend_secret_only: true,
        step1024_user_reads_scale_shared_collectors: false,
        step1024_missing_stays_null: true,
        step1026_all_asset_official_kline_route: '/api/asset-klines',
        step1026_exact_asset_identity_required: true,
        step1026_same_exact_key_cache_and_inflight: true,
        step1026_app_direct_exchange_requests: 0,
        step1026_gate_cash_equity_secondary_source_locked: true,
        step1026_coinbase_equity_secondary_source_locked: true,
        step1026_okx_event_sparse_bars_allowed: true,
        step1026_cross_provider_substitution: false,
        step1026_cross_product_substitution: false,
        step1026_cross_ticker_substitution: false,
        step1026_asset_market_route: '/api/asset-market',
        step1026_asset_market_batch_tickers_route: '/api/asset-market/tickers',
        step1026_asset_market_exact_identity_required: true,
        step1026_asset_market_batch_shared_reads: true,
        step1026_asset_market_app_direct_exchange_requests: 0,
        step1026_asset_market_cross_provider_substitution: false,
        step1026_asset_market_cross_product_substitution: false,
        step1026_asset_market_cross_ticker_substitution: false,
        step1026_asset_market_binance_supported: false,
        step1026_bitget_reality_dedicated_depth_requires_bd_whitelist: true,
        step1026_bitget_reality_dedicated_fills_requires_bd_whitelist: true,
        step1026_bybit_mt5_requires_tradfi_qualification: true,
        step1026_gate_cash_stock_public_market_trades_unavailable: true,
        step1026_gate_cash_equity_kline_second_source_locked: true,
        step1026_coinbase_equity_kline_second_source_locked: true,
        step1026_3_coinbase_equity_v50_capability_policy_restored: true,
        step1026_3_coinbase_equity_per_asset_book_trades_not_assumed: true,
        step1026_3_coinbase_equity_realtime_price_not_proven: true,
        step1026_3_coinbase_equity_rules_status_session_official: true,
        step1026_3_coinbase_equity_canonical_product_id_preserved: true,
        step1026_6_coinbase_equity_full_market_catalog_scan_removed: true,
        step1026_6_coinbase_equity_exact_product_shared_metadata: true,
        step1026_6_coinbase_equity_metadata_fresh_ttl_ms: 1800000,
        step1026_6_coinbase_equity_metadata_stale_ttl_ms: 86400000,
        step1026_6_coinbase_equity_batch_realtime_upstream_requests: 0,
        step1026_6_coinbase_equity_missing_exact_metadata_returns_partial_200: true,
        step1026_7_coinbase_equity_opaque_product_id_case_preserved: true,
        step1026_7_coinbase_equity_generic_symbol_uppercase_normalizer_bypassed: true,
        step1026_7_coinbase_equity_product_id_case_rewrite: false,
        step1026_8_okx_xperp_exact_ticker_uses_futures_tickers: true,
        step1026_8_okx_xperp_funding_rate_futures_supported: true,
        step1026_8_okx_asset_kline_uses_current_candles_route: '/api/v5/market/candles',
        step1026_8_okx_history_candles_not_used_for_latest_asset_window: true,
        step1024_cross_provider_substitution: false,
        step1024_cross_quote_substitution: false,
        coinbase_step995_ticker_batch_existing_shared: true,
        coinbase_step995_level2_existing_bounded_exact: true,
        coinbase_step995_market_trades_existing_public_exact: true,
        coinbase_step1022_product_book_official_fields: ['mid_market', 'spread_bps', 'spread_absolute'],
        coinbase_step1022_product_book_auth_required: true,
        coinbase_step1022_render_environment_secret_only: true,
        coinbase_step1022_official_fields_primary: true,
        coinbase_step1022_same_product_level2_ws_bbo_fallback_only: true,
        coinbase_step1022_user_read_direct_rest_requests: 0,
        coinbase_step1022_active_symbol_slot_cap: 12,
        gate_liquidation_history_deferred_to_step997: false,
        gate_liquidation_history_step997_ready: true,
        binance_contract_full_market_bbo_shared_ws: true,
        contract_basis_shared_current_endpoint: '/api/contract-basis/current-snapshot',
        contract_basis_reuses_market_light_only: true,
        contract_basis_additional_exchange_requests_per_build: 0,
        contract_basis_additional_exchange_connections_per_build: 0,
        contract_basis_reads_scale_with_users: false,
        data_page_spot_current_snapshot_targets_per_provider: 20,
        data_page_spot_current_snapshot_reads_open_exchange_request: false,
        data_page_spot_current_snapshot_scales_with_users: false,
        step1032_crypto_sector_professional_shared_route: '/api/crypto-sector-professional/current-snapshot',
        step1032_crypto_sector_derivation_reuses_market_light_only: true,
        step1032_crypto_sector_additional_exchange_requests: 0,
        step1032_crypto_sector_additional_exchange_connections: 0,
        step1032_crypto_sector_reads_scale_with_users: false,
        step1032_crypto_sector_quote_scope: 'five_venue_usdt_metrics_plus_coinbase_usd_presence_only',
        step1032_crypto_sector_cross_quote_aggregation: false,
        step1032_crypto_sector_tradeable_index: false,
        step1032_crypto_sector_membership_overlap_allowed: true,
        spot_capital_shared_history_endpoint: '/api/spot-flow/history',
        spot_capital_shared_history_cache_ttl_seconds: 120,
        spot_capital_shared_history_stale_seconds: 900,
        spot_capital_same_exact_key_reads_share_cache_and_inflight: true,
        spot_capital_app_three_direct_kline_history_reads_removed: true,
        spot_capital_shared_rpc_snapshot_endpoint: '/api/spot-flow/snapshot',
        spot_capital_shared_rpc_snapshot_cache_ttl_seconds: 40,
        spot_capital_shared_rpc_snapshot_stale_seconds: 300,
        spot_capital_direct_supabase_read_rpcs_removed: true,
        spot_capital_direct_activation_rpc_removed: true,
        spot_capital_shared_rpc_snapshot_reads_open_exchange_request: false,
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
        contract_liquidation_hour_bucket_persistence: true,
        contract_liquidation_hour_bucket_retention_days: 15,
        contract_liquidation_history_reads_open_exchange_connection: false,
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
        contract_funding_shared_history_persistence: true,
        contract_funding_shared_history_endpoint: '/api/contract-funding/history',
        contract_funding_history_retention_days: 31,
        contract_funding_current_retention_days: 7,
        contract_funding_background_rotation_interval_minutes: 60,
        contract_funding_background_rotation_batch_per_provider: 4,
        contract_funding_history_reads_open_exchange_connection: false,
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

  if (url.pathname === '/api/bybit-second-history-health') {
    try {
      const payload = await fetchChildJson(
        '/internal/bybit-second-history-health',
        8_000,
      );
      res.writeHead(200, {
        'content-type': 'application/json; charset=utf-8',
        'cache-control': 'no-store',
      });
      res.end(JSON.stringify(payload));
    } catch (error) {
      res.writeHead(503, {
        'content-type': 'application/json; charset=utf-8',
        'cache-control': 'no-store',
      });
      res.end(JSON.stringify({
        ok: false,
        version: STEP_VERSION,
        error: String(error?.message || error),
      }));
    }
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

  if (proxyIsolatedCollectorRequest(req, res, url)) return;

  const requestAbortController = new AbortController();
  const abortQueuedWork = () => {
    if (!res.writableEnded && !requestAbortController.signal.aborted) requestAbortController.abort();
  };
  req.once('aborted', abortQueuedWork);
  res.once('close', abortQueuedWork);
  try {
    // Step650.8.15.33: all HTTP market endpoints run in the parent process so Binance
    // Spot/Contract REST, probe, Kline validation, funding, and metrics share one
    // in-memory guard and one bounded queue. A disconnected client can cancel only
    // queued/paced work; an already-started upstream request is still fully observed.
    const handled = await runWithBinanceRequestSignal(requestAbortController.signal, async () => {
      if (await handleCmeExpirySharedCalendar(req, res, url)) return true;
      if (await handleSourceCapabilityRegistry(req, res, url)) return true;
      if (await handleContractBasis(req, res, url)) return true;
      if (await handleSpotCurrentSnapshot(req, res, url)) return true;
      if (await handleSpotExactTicker(req, res, url, requestAbortController.signal)) return true;
      if (await handleSpotFlowSnapshot(req, res, url, requestAbortController.signal)) return true;
      if (await handleSpotFlowHistory(req, res, url)) return true;
      if (await handleMarketApi(req, res, url)) return true;
      if (await handleContractDepth(req, res, url)) return true;
      if (await handleContractFunding(req, res, url)) return true;
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

// Step1028.7: Render inbound HTTP transport resilience for shared snapshot fan-out.
// No exchange collector/request path changes.
server.keepAliveTimeout = 120_000;
server.headersTimeout = 125_000;

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
  stopCollectorIsolationSupervisor();
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
startCmeExpirySharedCollector();
server.listen(PORT, '0.0.0.0', () => {
  console.log(`[Step${STEP_VERSION}] proxy + Step1031.2 Binance spot WebSocket market-light recovery + six-venue provider-isolated shared spot + Step1026 all-asset official market ticker/orderbook/trades/rules/status/hours shared cache + pre-landed all-asset Kline + persistent Binance contract market + contract flow + shared liquidation/basis/depth/flow/RPI/funding/current persistence listening on 0.0.0.0:${PORT}; legacy=${CHILD_PORT}`);
});
