import { getMarketLightSnapshotHealth } from './market-light-snapshot.mjs';
import { getBinanceAdvancedStatsHealth } from './binance-advanced-stats.mjs';
import { getBitgetAdvancedStatsHealth } from './bitget-advanced-stats.mjs';
import { getGateAdvancedStatsHealth } from './gate-advanced-stats.mjs';
import { getContractLiquidationPersistenceHealth } from './contract-liquidation.mjs';

const VERSION = '650.8.15.6';
const SNAPSHOT_ROUTE = '/api/source-capabilities/current-snapshot';
const HEALTH_ROUTE = '/api/source-capabilities/health';

const DECISION_POLICY = Object.freeze({
  official_first: true,
  derived_only_when_official_missing: true,
  official_limitations_must_be_exposed: true,
  cross_provider_substitution: false,
  cross_quote_substitution: false,
  missing_stays_null: true,
  user_reads_scale_exchange_upstream: false,
  full_market_equals_breadth: true,
  focus_15_equals_depth: true,
});

const CAPABILITIES = Object.freeze([
  // Binance
  { provider: 'binance', market: 'spot', capability: 'directory', official_available: true, official_scope: 'full_market', transport: 'REST', batch_mode: 'exchange_info', rate_limit_class: 'light_cached', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'none', source_url: 'https://developers.binance.com/en/docs/products/spot/rest-api' },
  { provider: 'binance', market: 'spot', capability: 'ticker_24h', official_available: true, official_scope: 'full_market', transport: 'REST', batch_mode: 'all_symbols_batch', rate_limit_class: 'light_cached', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'none', source_url: 'https://developers.binance.com/en/docs/products/spot/rest-api' },
  { provider: 'binance', market: 'spot', capability: 'bbo', official_available: true, official_scope: 'full_market', transport: 'REST/WS', batch_mode: 'shared_batch_or_stream', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'leave_null_if_source_missing', history_policy: 'none', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-streams/~' },
  { provider: 'binance', market: 'contract', capability: 'directory', official_available: true, official_scope: 'full_market', transport: 'WS/shared_identity', batch_mode: 'all_market_identity', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'none', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public' },
  { provider: 'binance', market: 'contract', capability: 'ticker_24h', official_available: true, official_scope: 'full_market', transport: 'WS', batch_mode: 'all_market_ticker', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'none', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public' },
  { provider: 'binance', market: 'contract', capability: 'mark_index_funding', official_available: true, official_scope: 'full_market', transport: 'WS', batch_mode: 'all_market_mark_price', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'funding_history_separate', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public' },
  { provider: 'binance', market: 'contract', capability: 'bbo', official_available: true, official_scope: 'full_market', transport: 'WS', batch_mode: 'all_book_tickers_stream', rate_limit_class: 'one_shared_connection_5s', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready_step990', fallback_policy: 'last_verified_shared_snapshot_or_null', history_policy: 'none', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public' },
  { provider: 'binance', market: 'contract', capability: 'open_interest_current', official_available: true, official_scope: 'per_symbol_focus15', transport: 'authenticated_edge_relay_to_official_REST', batch_mode: 'shared_focus15_5m', rate_limit_class: 'medium_shared', collector: 'binance-advanced-slow-stats', target_layer: 'slow_stats_or_focus', current_integration: 'ready_step993', fallback_policy: 'last_verified_until_stale_then_missing; never derived_as_official', history_policy: '5m_1h_1d_history_remains_separate', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data' },
  { provider: 'binance', market: 'contract', capability: 'adl_risk', official_available: true, official_scope: 'all_symbols_official_then_focus15_filter', transport: 'authenticated_edge_relay_to_official_REST', batch_mode: 'one_shared_all_symbol_request_per_30m', rate_limit_class: 'slow_shared', collector: 'binance-advanced-slow-stats', target_layer: 'risk', current_integration: 'ready_step993', fallback_policy: 'last_verified_until_stale_then_null; successful exact-symbol no-rating is official_unrated and remains null', history_policy: 'slow_snapshot', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data' },

  // OKX
  { provider: 'okx', market: 'spot', capability: 'ticker_bbo_24h', official_available: true, official_scope: 'instType_batch', transport: 'REST/WS', batch_mode: 'SPOT_batch', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'none', source_url: 'https://www.okx.com/docs-v5/en/' },
  { provider: 'okx', market: 'contract', capability: 'ticker_bbo_24h', official_available: true, official_scope: 'instType_batch', transport: 'REST/WS', batch_mode: 'SWAP_batch', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'none', source_url: 'https://www.okx.com/docs-v5/en/' },
  { provider: 'okx', market: 'contract', capability: 'mark_index_open_interest', official_available: true, official_scope: 'batch', transport: 'REST', batch_mode: 'SWAP_plus_USDT_index_plus_OI', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'oi_history_deferred', source_url: 'https://www.okx.com/docs-v5/en/' },
  { provider: 'okx', market: 'contract', capability: 'funding_rate', official_available: true, official_scope: 'per_symbol_or_ws_subscription', transport: 'REST/WS', batch_mode: 'not_full_market_batch', rate_limit_class: 'medium', collector: 'slow-stats-collector', target_layer: 'slow_stats', current_integration: 'deferred_step994', fallback_policy: 'missing', history_policy: '5m_1h_1d', source_url: 'https://www.okx.com/docs-v5/en/' },
  { provider: 'okx', market: 'contract', capability: 'price_limit_security_fund_adl', official_available: true, official_scope: 'public_symbol_or_channel', transport: 'REST/WS', batch_mode: 'slow_or_event', rate_limit_class: 'slow', collector: 'slow-stats-collector', target_layer: 'risk', current_integration: 'deferred_step994', fallback_policy: 'missing', history_policy: 'slow_snapshot_or_event', source_url: 'https://www.okx.com/docs-v5/en/' },

  // Bybit
  { provider: 'bybit', market: 'spot', capability: 'ticker_bbo_24h', official_available: true, official_scope: 'category_batch', transport: 'REST/WS', batch_mode: 'spot_batch', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'none', source_url: 'https://bybit-exchange.github.io/docs/v5/websocket/public/ticker' },
  { provider: 'bybit', market: 'contract', capability: 'linear_ticker_mark_index_oi_funding_bbo', official_available: true, official_scope: 'category_batch', transport: 'REST/WS', batch_mode: 'linear_batch', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'selected_fields_bucketed_elsewhere', source_url: 'https://bybit-exchange.github.io/docs/v5/websocket/public/ticker' },
  { provider: 'bybit', market: 'contract', capability: 'open_interest_history', official_available: true, official_scope: 'per_symbol', transport: 'REST', batch_mode: 'not_full_market_batch', rate_limit_class: 'medium', collector: 'slow-stats-collector', target_layer: 'history', current_integration: 'deferred_step995', fallback_policy: 'missing', history_policy: '5m_1h_1d', source_url: 'https://bybit-exchange.github.io/docs/v5/market/open-interest' },

  // Bitget
  { provider: 'bitget', market: 'spot', capability: 'ticker_bbo_24h', official_available: true, official_scope: 'product_batch', transport: 'REST/WS', batch_mode: 'SPOT_batch', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'leave_null_if_official_row_omits_quote', history_policy: 'none', source_url: 'https://www.bitget.com/api-doc/uta/changelog' },
  { provider: 'bitget', market: 'contract', capability: 'ticker_mark_index_oi_funding_bbo', official_available: true, official_scope: 'product_batch', transport: 'REST/WS', batch_mode: 'USDT_FUTURES_batch', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'selected_fields_bucketed_elsewhere', source_url: 'https://www.bitget.com/api-doc/uta/changelog' },
  { provider: 'bitget', market: 'contract', capability: 'next_funding_time_interval', official_available: true, official_scope: 'category_batch_symbol_optional', transport: 'REST', batch_mode: 'USDT-FUTURES_category_batch', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready_step991', fallback_policy: 'last_verified_shared_snapshot_or_null', history_policy: 'none', source_url: 'https://www.bitget.com/api-doc/uta/public/Get-Current-Funding-Rate' },
  { provider: 'bitget', market: 'spot', capability: 'whale_fund_net_capital_flow', official_available: true, official_scope: 'per_symbol_focus_intersection', transport: 'REST', batch_mode: 'shared_focus15_slow_stats', rate_limit_class: '1_per_sec_per_endpoint', collector: 'slow-stats-collector', target_layer: 'funds_official', current_integration: 'ready_step991', fallback_policy: 'keep_derived_separate_never_relabel', history_policy: 'official_periods_plus_future_bucket_rollup', source_url: 'https://www.bitget.com/api-doc/uta/public/trading-data/Get-Spot-Whale-Net-Flow' },
  { provider: 'bitget', market: 'contract', capability: 'active_buy_sell_long_short', official_available: true, official_scope: 'per_symbol_focus15', transport: 'REST', batch_mode: 'four_official_5m_focus_lanes', rate_limit_class: '1_per_sec_per_endpoint', collector: 'slow-stats-collector', target_layer: 'contract_stats', current_integration: 'ready_step991', fallback_policy: 'keep_derived_separate_never_relabel', history_policy: '5m_official_then_future_rollup', source_url: 'https://www.bitget.com/api-doc/uta/public/Get-Futures-Active-Buy-Sell' },
  { provider: 'bitget', market: 'contract', capability: 'risk_reserve_position_tier_oi_limit_index_components', official_available: true, official_scope: 'batch_plus_focus15', transport: 'REST', batch_mode: 'risk_reserve_all_and_oi_limit_batch_plus_focus_tier_index', rate_limit_class: 'slow_shared', collector: 'slow-stats-collector', target_layer: 'risk_reference', current_integration: 'ready_step991', fallback_policy: 'missing', history_policy: 'risk_reserve_future_history_step1000; current_reference_now', source_url: 'https://www.bitget.com/api-doc/uta/public/Get-Risk-Reserve-All' },

  // Gate
  { provider: 'gate', market: 'spot', capability: 'ticker_bbo_24h', official_available: true, official_scope: 'batch', transport: 'REST/WS', batch_mode: 'all_spot_tickers', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'none', source_url: 'https://www.gate.com/docs/developers/apiv4/en/' },
  { provider: 'gate', market: 'contract', capability: 'ticker_mark_index_funding_bbo', official_available: true, official_scope: 'batch', transport: 'REST/WS', batch_mode: 'USDT_futures_tickers', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'selected_fields_bucketed_elsewhere', source_url: 'https://www.gate.com/docs/developers/apiv4/en/' },
  { provider: 'gate', market: 'contract', capability: 'open_interest', official_available: true, official_scope: 'per_contract_contract_stats', transport: 'REST', batch_mode: 'focus15_5m_contract_stats', rate_limit_class: 'slow_shared', collector: 'slow-stats-collector', target_layer: 'contract_stats', current_integration: 'ready_step992_focus15_contract_stats', fallback_policy: 'missing_not_provider_total_size', history_policy: '5m_1h_1d_future_rollup', source_url: 'https://www.gate.com/docs/developers/apiv4/en/' },
  { provider: 'gate', market: 'contract', capability: 'contract_stats_top_trader_taker_account', official_available: true, official_scope: 'per_contract_contract_stats', transport: 'REST', batch_mode: 'focus15_5m_contract_stats', rate_limit_class: 'slow_shared', collector: 'slow-stats-collector', target_layer: 'contract_stats', current_integration: 'ready_step992', fallback_policy: 'missing_keep_derived_separate', history_policy: '5m_1h_1d_future_rollup', source_url: 'https://www.gate.com/docs/developers/apiv4/en/' },
  { provider: 'gate', market: 'contract', capability: 'risk_limit_tiers', official_available: true, official_scope: 'public_per_contract_or_top100_market', transport: 'REST', batch_mode: 'focus15_per_contract', rate_limit_class: 'slow_shared', collector: 'slow-stats-collector', target_layer: 'risk_reference', current_integration: 'ready_step992', fallback_policy: 'missing', history_policy: 'reference_snapshot', source_url: 'https://www.gate.com/docs/developers/apiv4/en/' },
  { provider: 'gate', market: 'contract', capability: 'insurance_fund', official_available: true, official_scope: 'public_settlement_history', transport: 'REST', batch_mode: 'one_shared_low_frequency_request', rate_limit_class: 'slow_shared', collector: 'slow-stats-collector', target_layer: 'risk_reference', current_integration: 'ready_step992', fallback_policy: 'last_verified_shared_snapshot_or_missing', history_policy: 'official_history', source_url: 'https://www.gate.com/docs/developers/apiv4/en/' },
  { provider: 'gate', market: 'contract', capability: 'liquidation_event_history', official_available: true, official_scope: 'public_all_usdt_closed_minute_liq_orders', transport: 'REST+existing_public_WS', batch_mode: 'one_closed_minute_liq_orders_request_per_60s_plus_shared_ws', rate_limit_class: 'bounded_background_event_history', collector: 'liquidation-collector', target_layer: 'liquidation_history', current_integration: 'ready_step997', fallback_policy: 'complete_liq_orders_minute_owns_gate_1m_history; truncated_window_never_overwrites; never_cross_provider_substitute', history_policy: '1m_base_plus_5m_15m_and_1h_base_plus_6h_24h', source_url: 'https://www.gate.com/docs/developers/apiv4/en/' },

  // Coinbase is project spot-only.
  { provider: 'coinbase', market: 'spot', capability: 'ticker_24h', official_available: true, official_scope: 'multi_product_public_ws', transport: 'WS', batch_mode: 'ticker_batch_5s', rate_limit_class: 'one_shared_connection', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'none', source_url: 'https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels' },
  { provider: 'coinbase', market: 'spot', capability: 'bbo', official_available: true, official_scope: 'ticker_or_level2_not_ticker_batch', transport: 'WS', batch_mode: 'no_light_batch_bbo', rate_limit_class: 'higher_bandwidth', collector: 'deep-market-collector', target_layer: 'focus_or_user_exact', current_integration: 'intentionally_not_market_light', fallback_policy: 'missing_in_full_market_light', history_policy: 'none', source_url: 'https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels' },
  { provider: 'coinbase', market: 'spot', capability: 'market_trades_level2_candles', official_available: true, official_scope: 'per_product_public_ws', transport: 'WS', batch_mode: 'deep_or_user_exact', rate_limit_class: 'deep', collector: 'deep-market-collector', target_layer: 'focus_or_user_exact', current_integration: 'deferred_step995', fallback_policy: 'missing', history_policy: 'on_demand', source_url: 'https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels' },
]);

let totalReads = 0;

function runtimeCoverage() {
  const health = getMarketLightSnapshotHealth();
  const out = {};
  for (const [key, value] of Object.entries(health?.provider_coverage || {})) {
    out[key] = {
      rows: Number(value?.row_count || 0),
      directory: Number(value?.directory_count || 0),
      exact: Number(value?.row_count || 0) > 0 && Number(value?.row_count || 0) === Number(value?.directory_count || 0) && value?.stale !== true && !String(value?.last_error || ''),
      stale: Boolean(value?.stale),
      last_error: String(value?.last_error || ''),
      field_coverage: { ...(value?.field_coverage || {}) },
    };
  }
  return { health, coverage: out };
}

function registrySnapshot({ includeCapabilities = true } = {}) {
  const { health, coverage } = runtimeCoverage();
  const binanceAdvanced = getBinanceAdvancedStatsHealth();
  const bitgetAdvanced = getBitgetAdvancedStatsHealth();
  const gateAdvanced = getGateAdvancedStatsHealth();
  const liquidationHistory = getContractLiquidationPersistenceHealth();
  const marketKeys = Object.keys(coverage);
  const allMarketExact = marketKeys.length === 11 && marketKeys.every((key) => coverage[key]?.exact === true);
  const binanceContract = coverage['contract:binance'] || {};
  const coinbaseSpot = coverage['spot:coinbase'] || {};
  const binanceBboReady = Number(binanceContract?.field_coverage?.best_bid || 0) === Number(binanceContract?.rows || 0) &&
    Number(binanceContract?.field_coverage?.best_ask || 0) === Number(binanceContract?.rows || 0) && Number(binanceContract?.rows || 0) > 0;
  const tradingStatusReady = marketKeys.length === 11 && marketKeys.every((key) => {
    const item = coverage[key] || {};
    return Number(item?.field_coverage?.trading_status || 0) === Number(item?.rows || 0) && Number(item?.rows || 0) > 0;
  });
  const payload = {
    ok: true,
    version: VERSION,
    schema_version: '1.0',
    source: 'kaka_runtime_official_public_source_capability_registry',
    source_checked_at: '2026-08-13',
    provider_keys: ['binance', 'okx', 'bybit', 'bitget', 'gate', 'coinbase'],
    contract_provider_keys: ['binance', 'okx', 'bybit', 'bitget', 'gate'],
    coinbase_project_scope: 'spot_only',
    decision_policy: DECISION_POLICY,
    collector_targets: ['market-light-collector', 'liquidation-collector', 'deep-market-collector', 'slow-stats-collector', 'binance-advanced-slow-stats'],
    runtime_market_light_version: health?.version || null,
    runtime_market_light_11_exact: allMarketExact,
    runtime_market_light_coverage: coverage,
    step990_light_gap_status: {
      binance_contract_all_market_bbo_ready: binanceBboReady,
      coinbase_ticker_batch_bbo_available: false,
      coinbase_bbo_policy: 'ticker_batch_officially_omits_best_bid_ask; keep full-market-light null; use focus/user-exact higher-bandwidth source only',
      okx_funding_policy: 'official_available_but_not_full-market-light-batch; Step994 slow-stats',
      bitget_next_funding_policy: 'official current-fund-rate supports optional symbol/category batch; ready Step991 market-light',
      gate_open_interest_policy: 'Gate ContractStat open_interest/open_interest_usd ready on focus15; never relabel ticker total_size as OI',
      missing_policy: 'never fabricate zero; never cross-provider or cross-quote substitute',
    },
    trading_status_full_market_ready: tradingStatusReady,
    binance_step993: {
      ready: binanceAdvanced.ready === true,
      version: binanceAdvanced.version || null,
      focus_target: Number(binanceAdvanced.focus_target || 0),
      row_count: Number(binanceAdvanced.row_count || 0),
      core_target_count: Number(binanceAdvanced.core_target_count || 0),
      open_interest_rows: Number(binanceAdvanced.open_interest_rows || 0),
      adl_risk_rows: Number(binanceAdvanced.adl_risk_rows || 0),
      adl_official_unrated_rows: Number(binanceAdvanced.adl_official_unrated_rows || 0),
      adl_official_coverage_rows: Number(binanceAdvanced.adl_official_coverage_rows || 0),
      core_open_interest_rows: Number(binanceAdvanced.core_open_interest_rows || 0),
      core_adl_risk_rows: Number(binanceAdvanced.core_adl_risk_rows || 0),
      core_adl_official_coverage_rows: Number(binanceAdvanced.core_adl_official_coverage_rows || 0),
      official_unrated_adl_symbols: Array.isArray(binanceAdvanced.official_unrated_adl_symbols) ? [...binanceAdvanced.official_unrated_adl_symbols] : [],
      adl_official_update_interval_minutes: Number(binanceAdvanced.adl_official_update_interval_minutes || 0),
      edge_relay_only: binanceAdvanced.edge_relay_only === true,
      render_direct_binance_rest: binanceAdvanced.render_direct_binance_rest === true,
      user_reads_scale_with_users: false,
    },
    bitget_step991: {
      ready: bitgetAdvanced.ready === true,
      version: bitgetAdvanced.version || null,
      contract_core_official_stats_rows: Number(bitgetAdvanced.contract_core_official_stats_rows || 0),
      contract_risk_reference_rows: Number(bitgetAdvanced.contract_risk_reference_rows || 0),
      spot_row_count: Number(bitgetAdvanced.spot_row_count || 0),
      funding_row_count: Number(bitgetAdvanced.funding_row_count || 0),
      risk_reserve_pool_count: Number(bitgetAdvanced.risk_reserve_pool_count || 0),
      oi_limit_row_count: Number(bitgetAdvanced.oi_limit_row_count || 0),
      user_reads_scale_with_users: false,
    },
    gate_step992: {
      ready: gateAdvanced.ready === true,
      version: gateAdvanced.version || null,
      contract_core_stats_rows: Number(gateAdvanced.contract_core_stats_rows || 0),
      contract_stats_rows: Number(gateAdvanced.contract_stats_rows || 0),
      risk_limit_rows: Number(gateAdvanced.risk_limit_rows || 0),
      open_interest_rows: Number(gateAdvanced.open_interest_rows || 0),
      top_trader_rows: Number(gateAdvanced.top_trader_rows || 0),
      taker_stats_rows: Number(gateAdvanced.taker_stats_rows || 0),
      account_stats_rows: Number(gateAdvanced.account_stats_rows || 0),
      liquidation_reference_rows: Number(gateAdvanced.liquidation_reference_rows || 0),
      insurance_record_count: Number(gateAdvanced.insurance_record_count || 0),
      liquidation_history_deferred_to_step997: gateAdvanced.liquidation_history_deferred_to_step997 === true,
      liquidation_history_step997_ready: gateAdvanced.liquidation_history_step997_ready === true,
      user_reads_scale_with_users: false,
    },
    step997_liquidation_history: {
      ready: liquidationHistory.persistence_enabled === true &&
        liquidationHistory.step997_unified_history_ready === true &&
        JSON.stringify(liquidationHistory.step997_history_intervals || []) === JSON.stringify(['1m', '5m', '15m', '1H', '6H', '24H']) &&
        liquidationHistory.step997_default_history_backward_compatible_1h === true &&
        liquidationHistory.raw_events_persisted === false &&
        liquidationHistory.zero_event_rows_persisted === false &&
        liquidationHistory.exchange_requests_started_by_history_reads === 0 &&
        liquidationHistory.gate_liq_orders?.public_no_auth === true &&
        liquidationHistory.gate_liq_orders?.user_reads_trigger_requests === false &&
        liquidationHistory.gate_liq_orders?.reads_scale_with_users === false &&
        liquidationHistory.gate_liq_orders?.left_field_ignored === true &&
        liquidationHistory.gate_liq_orders?.size_decimal_header === true &&
        Number(liquidationHistory.gate_liq_orders?.additional_contract_metadata_requests || 0) === 0,
      version: liquidationHistory.version || null,
      history_endpoint: liquidationHistory.history_endpoint || null,
      intervals: Array.isArray(liquidationHistory.step997_history_intervals) ? [...liquidationHistory.step997_history_intervals] : [],
      default_backward_compatible_1h: liquidationHistory.step997_default_history_backward_compatible_1h === true,
      minute_storage_table: liquidationHistory.minute_storage_table || null,
      hour_storage_table: liquidationHistory.storage_table || null,
      gate_coverage_table: liquidationHistory.gate_minute_coverage_table || null,
      aggregation_rpc: liquidationHistory.step997_history_rpc || null,
      raw_events_persisted: liquidationHistory.raw_events_persisted === true,
      zero_event_rows_persisted: liquidationHistory.zero_event_rows_persisted === true,
      gate_liq_orders_public_no_auth: liquidationHistory.gate_liq_orders?.public_no_auth === true,
      gate_liq_orders_poll_seconds: Number(liquidationHistory.gate_liq_orders?.polling_interval_seconds || 0),
      gate_liq_orders_successes: Number(liquidationHistory.gate_liq_orders?.successes || 0),
      gate_liq_orders_complete_windows: Number(liquidationHistory.gate_liq_orders?.complete_windows || 0),
      gate_liq_orders_truncated_windows: Number(liquidationHistory.gate_liq_orders?.truncated_windows || 0),
      gate_left_field_ignored: liquidationHistory.gate_liq_orders?.left_field_ignored === true,
      gate_size_decimal_header: liquidationHistory.gate_liq_orders?.size_decimal_header === true,
      gate_additional_contract_metadata_requests: Number(liquidationHistory.gate_liq_orders?.additional_contract_metadata_requests || 0),
      user_reads_scale_with_users: false,
    },
    capabilities: includeCapabilities ? CAPABILITIES : [],
    capability_count: CAPABILITIES.length,
    exchange_requests_started_by_user_read: 0,
    exchange_connections_started_by_user_read: 0,
    user_reads_trigger_collector: false,
    reads_scale_with_users: false,
    timestamp_ms: Date.now(),
  };
  return payload;
}

export function getSourceCapabilityRegistryHealth() {
  const payload = registrySnapshot({ includeCapabilities: false });
  return {
    ...payload,
    health_endpoint: HEALTH_ROUTE,
    snapshot_endpoint: SNAPSHOT_ROUTE,
    total_reads: totalReads,
    ready: payload.runtime_market_light_11_exact &&
      payload.step990_light_gap_status.binance_contract_all_market_bbo_ready &&
      payload.trading_status_full_market_ready &&
      payload.binance_step993.ready &&
      payload.bitget_step991.ready &&
      payload.gate_step992.ready &&
      payload.step997_liquidation_history.ready,
  };
}

function sendJson(res, status, payload) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET, OPTIONS',
    'content-length': String(body.length),
  });
  res.end(body);
}

export async function handleSourceCapabilityRegistry(req, res, url) {
  if (![SNAPSHOT_ROUTE, HEALTH_ROUTE].includes(url.pathname)) return false;
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET, OPTIONS',
      'cache-control': 'no-store',
    });
    res.end();
    return true;
  }
  if (req.method !== 'GET') {
    sendJson(res, 405, { ok: false, version: VERSION, error: 'method_not_allowed' });
    return true;
  }
  totalReads += 1;
  if (url.pathname === HEALTH_ROUTE) {
    sendJson(res, 200, getSourceCapabilityRegistryHealth());
    return true;
  }
  sendJson(res, 200, registrySnapshot({ includeCapabilities: true }));
  return true;
}
