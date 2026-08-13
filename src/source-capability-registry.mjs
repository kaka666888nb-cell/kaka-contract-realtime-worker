import { getMarketLightSnapshotHealth } from './market-light-bridge.mjs';
import {
  getBinanceAdvancedStatsHealth,
  getBitgetAdvancedStatsHealth,
  getGateAdvancedStatsHealth,
  getOkxAdvancedStatsHealth,
  getBybitAdvancedStatsHealth,
  getDerivativesPublicHealth,
  getSlowStatsBridgeHealth,
} from './slow-stats-bridge.mjs';
import { getContractDepthHealth } from './contract-depth.mjs';
import { getContractLiquidationPersistenceHealth } from './liquidation-bridge.mjs';
import { getContractFocusPoolHealth, getContractFlowHealth, getContractDeepSharedHealth, getDeepMarketBridgeHealth } from './deep-market-bridge.mjs';
import { getCollectorIsolationHealth } from './collector-isolation.mjs';
import { BUSINESS_SOURCE_POLICY_VERSION, BUSINESS_SOURCE_RULES, validateBusinessSourceRules } from './business-source-policy.mjs';

const VERSION = '650.8.15.31';
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
  { provider: 'kaka_backend', market: 'shared', capability: 'collector_isolation_first_batch', official_available: true, official_scope: 'backend_architecture', transport: 'separate_node_child_processes_plus_localhost_bridge', batch_mode: 'market_light_and_liquidation_distinct_process_fault_domains', rate_limit_class: 'local_internal_only', collector: 'collector-isolation-supervisor', target_layer: 'backend_runtime', current_integration: 'ready_step1004_1_catchup', fallback_policy: 'role_scoped_restart; sibling collector stays alive', history_policy: 'none', source_url: 'internal_architecture' },
  { provider: 'kaka_backend', market: 'shared', capability: 'collector_isolation_second_batch', official_available: true, official_scope: 'backend_architecture', transport: 'resource_limited_node_worker_isolates_plus_localhost_bridges', batch_mode: 'deep_market_and_slow_stats_distinct_worker_fault_domains', rate_limit_class: 'local_internal_only', collector: 'collector-isolation-supervisor', target_layer: 'backend_runtime', current_integration: 'ready_step1004_2_2_projected_bridge_memory_safe', fallback_policy: 'role_scoped worker restart; sibling worker and parent remain alive', history_policy: 'none', source_url: 'internal_architecture' },
  { provider: 'binance', market: 'option', capability: 'option_public_market', official_available: true, official_scope: 'official_public_crypto_options_market', transport: 'official_REST_and_market_WebSocket_exist_but_not_collected_by_current_Kaka_Render', batch_mode: 'none_in_step1004_5', rate_limit_class: 'blocked_by_existing_Kaka_Binance_safety_boundary', collector: 'none', target_layer: 'derivatives_public', current_integration: 'not_collected_step996_existing_binance_safety_policy_preserved', fallback_policy: 'missing; no cross-provider substitution', history_policy: 'none', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/ws-streams/market' },
  { provider: 'okx', market: 'option', capability: 'option_public_market', official_available: true, official_scope: 'current_public_crypto_option_instruments_tickers_open_interest', transport: 'official_public_REST_shared_background', batch_mode: 'all_option_instruments_plus_all_option_tickers_plus_bounded_official_instrument_family_OI', rate_limit_class: 'slow_shared_governed', collector: 'derivatives-public-slow-stats', target_layer: 'derivatives_public', current_integration: 'ready_step1004_5', fallback_policy: 'last_verified_shared_snapshot_until_refresh; missing fields stay null', history_policy: 'current_only_step1004_5', source_url: 'https://www.okx.com/docs-v5/en/' },
  { provider: 'bybit', market: 'option', capability: 'option_public_market', official_available: true, official_scope: 'current_public_crypto_option_instruments_and_tickers', transport: 'official_public_REST_shared_background', batch_mode: 'paginated_option_instruments_plus_option_tickers_per_official_baseCoin', rate_limit_class: 'slow_shared_governed', collector: 'derivatives-public-slow-stats', target_layer: 'derivatives_public', current_integration: 'ready_step1004_5', fallback_policy: 'last_verified_shared_snapshot_until_refresh; missing fields stay null', history_policy: 'current_only_step1004_5', source_url: 'https://bybit-exchange.github.io/docs/v5/market/instrument' },
  { provider: 'gate', market: 'option', capability: 'option_public_market', official_available: true, official_scope: 'current_public_crypto_option_underlyings_and_tickers', transport: 'official_public_REST_shared_background', batch_mode: 'official_underlyings_plus_tickers_per_official_underlying', rate_limit_class: 'slow_shared_governed', collector: 'derivatives-public-slow-stats', target_layer: 'derivatives_public', current_integration: 'ready_step1004_5', fallback_policy: 'last_verified_shared_snapshot_until_refresh; missing fields stay null', history_policy: 'current_only_step1004_5', source_url: 'https://www.gate.com/docs/developers/apiv4/en/' },
  { provider: 'bitget', market: 'option', capability: 'option_public_market', official_available: false, official_scope: 'none_in_current_public_crypto_UTA_product_categories', transport: 'none', batch_mode: 'none', rate_limit_class: 'none', collector: 'none', target_layer: 'derivatives_public', current_integration: 'crypto_options_not_in_current_public_crypto_product_family', fallback_policy: 'missing; Stock+ US stock options excluded from crypto derivatives scope', history_policy: 'none', source_url: 'https://www.bitget.com/api-doc/uta/public/Instruments' },
  // Binance
  { provider: 'binance', market: 'spot', capability: 'directory', official_available: true, official_scope: 'full_market', transport: 'public_market_data_only_REST', batch_mode: 'data-api ticker_24hr symbol_omitted TRADING full-market shared baseline', rate_limit_class: '80_weight_per_2m_shared', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready_step1001_7', fallback_policy: 'last_verified_shared_public_market_data_baseline; no authenticated REST or Edge relay', history_policy: 'none', source_url: 'https://developers.binance.com/en/docs/products/spot/faqs/market_data_only' },
  { provider: 'binance', market: 'spot', capability: 'ticker_24h', official_available: true, official_scope: 'full_market', transport: 'public_market_data_only_REST_plus_market_stream', batch_mode: 'data-api ticker_24hr TRADING baseline plus data-stream !miniTicker@arr changed updates', rate_limit_class: 'one_shared_stream_plus_80_weight_per_2m', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready_step1001_7', fallback_policy: 'last_verified_shared_public_market_data_baseline; no authenticated REST or Edge relay', history_policy: 'none', source_url: 'https://developers.binance.com/en/docs/products/spot/faqs/market_data_only' },
  { provider: 'binance', market: 'spot', capability: 'bbo', official_available: true, official_scope: 'full_market', transport: 'public_market_data_only_REST', batch_mode: 'data-api ticker_24hr TRADING baseline BBO', rate_limit_class: 'shared_baseline', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready_step1001_7', fallback_policy: 'last_verified_public_market_data_baseline_bbo_or_null; no authenticated REST or Edge relay', history_policy: 'none', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market' },
  { provider: 'binance', market: 'contract', capability: 'directory', official_available: true, official_scope: 'full_market', transport: 'WS/shared_identity', batch_mode: 'all_market_identity', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'none', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public' },
  { provider: 'binance', market: 'contract', capability: 'ticker_24h', official_available: true, official_scope: 'full_market', transport: 'WS', batch_mode: 'all_market_ticker', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'none', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public' },
  { provider: 'binance', market: 'contract', capability: 'mark_index_funding', official_available: true, official_scope: 'full_market', transport: 'WS', batch_mode: 'all_market_mark_price', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'funding_history_separate', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public' },
  { provider: 'binance', market: 'contract', capability: 'bbo', official_available: true, official_scope: 'full_market', transport: 'WS', batch_mode: 'all_book_tickers_stream', rate_limit_class: 'one_shared_connection_5s', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready_step990', fallback_policy: 'last_verified_shared_snapshot_or_null', history_policy: 'none', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public' },
  { provider: 'binance', market: 'contract', capability: 'open_interest_current', official_available: true, official_scope: 'per_symbol_focus15', transport: 'authenticated_edge_relay_to_official_REST', batch_mode: 'shared_focus15_5m', rate_limit_class: 'medium_shared', collector: 'binance-advanced-slow-stats', target_layer: 'slow_stats_or_focus', current_integration: 'ready_step993', fallback_policy: 'last_verified_until_stale_then_missing; never derived_as_official', history_policy: '5m_1h_1d_history_remains_separate', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data' },
  { provider: 'binance', market: 'contract', capability: 'adl_risk', official_available: true, official_scope: 'all_symbols_official_then_focus15_filter', transport: 'authenticated_edge_relay_to_official_REST', batch_mode: 'one_shared_all_symbol_request_per_30m', rate_limit_class: 'slow_shared', collector: 'binance-advanced-slow-stats', target_layer: 'risk', current_integration: 'ready_step993', fallback_policy: 'last_verified_until_stale_then_null; successful exact-symbol no-rating is official_unrated and remains null', history_policy: 'slow_snapshot', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data' },
  { provider: 'binance', market: 'contract', capability: 'open_interest_history', official_available: true, official_scope: 'per_symbol_shared_history', transport: 'authenticated_edge_relay_to_official_REST_plus_shared_supabase_cache', batch_mode: 'existing_contract_flow_5m_history_no_duplicate_collector', rate_limit_class: 'existing_shared', collector: 'contract-flow-metric-history', target_layer: 'history', current_integration: 'ready_v46_closure', fallback_policy: 'last_persisted_verified_history; no derived-as-official', history_policy: 'official_5m_openInterestHist_persisted_72h', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data' },
  { provider: 'binance', market: 'contract', capability: 'long_short_and_taker_stats', official_available: true, official_scope: 'per_symbol_shared_focus_and_history', transport: 'authenticated_edge_relay_to_official_REST', batch_mode: 'existing_contract_flow_long_short_plus_focus15_official_taker_5m', rate_limit_class: 'shared_bounded', collector: 'contract-flow-metric-history', target_layer: 'history_and_focus', current_integration: 'ready_v46_closure', fallback_policy: 'long_short_last_persisted; taker_normalized_focus15_shared_snapshot_until_stale; never substitute derived flow as official taker', history_policy: '5m_long_short_persisted_plus_5m_taker_normalized_shared_snapshot', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data' },
  { provider: 'binance', market: 'contract', capability: 'depth20_current_focus', official_available: true, official_scope: 'focus15_current', transport: 'public_orderbook_shared_backend', batch_mode: 'bounded_focus15_depth20', rate_limit_class: 'shared_bounded', collector: 'deep-market-collector', target_layer: 'depth', current_integration: 'ready_step1004_1_5', fallback_policy: 'last verified until stale then null; no cross-provider substitution', history_policy: 'current_only', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/web-socket-streams' },
  { provider: 'binance', market: 'contract', capability: 'liquidation_current', official_available: true, official_scope: 'public_event_stream_current', transport: 'public_WS_or_public_event_endpoint', batch_mode: 'shared_backend_event_collection', rate_limit_class: 'shared_bounded', collector: 'liquidation-collector', target_layer: 'liquidation_current', current_integration: 'ready_step997', fallback_policy: 'verified zero distinct from missing; no cross-provider substitution', history_policy: 'shared_event_buckets_and_history', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/web-socket-streams' },

  // OKX
  { provider: 'okx', market: 'spot', capability: 'ticker_bbo_24h', official_available: true, official_scope: 'instType_batch', transport: 'REST/WS', batch_mode: 'SPOT_batch', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'none', source_url: 'https://www.okx.com/docs-v5/en/' },
  { provider: 'okx', market: 'contract', capability: 'ticker_bbo_24h', official_available: true, official_scope: 'instType_batch', transport: 'REST/WS', batch_mode: 'SWAP_batch', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'none', source_url: 'https://www.okx.com/docs-v5/en/' },
  { provider: 'okx', market: 'contract', capability: 'directory', official_available: true, official_scope: 'USDT_perpetual_full_directory', transport: 'existing_market_light_official_source', batch_mode: 'shared_full_directory', rate_limit_class: 'light_shared', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last verified shared directory', history_policy: 'none', source_url: 'https://www.okx.com/docs-v5/en/' },
  { provider: 'okx', market: 'contract', capability: 'bbo', official_available: true, official_scope: 'USDT_perpetual_full_directory', transport: 'existing_market_light_official_source', batch_mode: 'shared_full_directory_bbo', rate_limit_class: 'light_shared', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready_step990', fallback_policy: 'last verified shared BBO or null', history_policy: 'none', source_url: 'https://www.okx.com/docs-v5/en/' },
  { provider: 'okx', market: 'contract', capability: 'mark_index_open_interest', official_available: true, official_scope: 'batch', transport: 'REST', batch_mode: 'SWAP_plus_USDT_index_plus_OI', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'open_interest_history_owned_by_step1002', source_url: 'https://www.okx.com/docs-v5/en/' },
  { provider: 'okx', market: 'contract', capability: 'open_interest_history', official_available: true, official_scope: 'per_symbol_focus15', transport: 'public_REST_plus_shared_backend_persistence', batch_mode: 'one_official_5m_request_per_stale_focus_symbol_then_backend_rollup', rate_limit_class: '10_requests_per_2s_IP_plus_instrument_but_project_uses_low_frequency_shared', collector: 'okx-advanced-slow-stats', target_layer: 'history', current_integration: 'ready_step1002', fallback_policy: 'last_verified_shared_persisted_5m_history; no cross-provider substitution; stale remains stale', history_policy: 'official_5m_plus_explicit_derived_15m_1h_4h_last_observation', source_url: 'https://www.okx.com/docs-v5/en/' },
  { provider: 'okx', market: 'contract', capability: 'global_long_short_ratio', official_available: true, official_scope: 'currency_5m_shared_focus_rotation', transport: 'REST', batch_mode: 'rubik_long_short_account_ratio_5m', rate_limit_class: 'shared_bounded', collector: 'contract-flow-metric-history', target_layer: 'current_and_history', current_integration: 'ready_existing_contract_flow', fallback_policy: 'missing stays null; no top-account/top-position synthesis', history_policy: '5m_metric_rows', source_url: 'https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio' },
  { provider: 'okx', market: 'contract', capability: 'funding_rate', official_available: true, official_scope: 'per_symbol_focus15', transport: 'public_REST', batch_mode: 'shared_focus15_5m', rate_limit_class: 'medium_shared', collector: 'okx-advanced-slow-stats', target_layer: 'slow_stats', current_integration: 'ready_step994', fallback_policy: 'last_verified_until_stale_then_missing; empty nextFundingRate stays null', history_policy: 'current_shared_5m; existing_history_endpoint_separate', source_url: 'https://www.okx.com/docs-v5/en/' },
  { provider: 'okx', market: 'contract', capability: 'price_limit_security_fund_adl', official_available: true, official_scope: 'focus15_price_limit_plus_focus15_family_security_fund_plus_public_adl_warning_channel', transport: 'public_REST_plus_public_WS', batch_mode: 'price_limit_focus15_5m; security_fund_focus_family_6h_missing_only_recovery; one_shared_adl_ws', rate_limit_class: 'slow_shared', collector: 'okx-advanced-slow-stats', target_layer: 'risk', current_integration: 'ready_step994', fallback_policy: 'last_verified_until_stale_then_missing; ADL normal silence remains null and is never fabricated', history_policy: 'slow_snapshot_or_event; active_ADL_event_memory_only', source_url: 'https://www.okx.com/docs-v5/en/' },
  { provider: 'okx', market: 'contract', capability: 'depth20_current_focus', official_available: true, official_scope: 'focus15_current', transport: 'public_orderbook_shared_backend', batch_mode: 'bounded_focus15_depth20', rate_limit_class: 'shared_bounded', collector: 'deep-market-collector', target_layer: 'depth', current_integration: 'ready_step1004_1_5', fallback_policy: 'last verified until stale then null; no cross-provider substitution', history_policy: 'current_only', source_url: 'https://www.okx.com/docs-v5/en/' },
  { provider: 'okx', market: 'contract', capability: 'liquidation_current', official_available: true, official_scope: 'public_event_stream_current', transport: 'public_WS_or_public_event_endpoint', batch_mode: 'shared_backend_event_collection', rate_limit_class: 'shared_bounded', collector: 'liquidation-collector', target_layer: 'liquidation_current', current_integration: 'ready_step997', fallback_policy: 'verified zero distinct from missing; no cross-provider substitution', history_policy: 'shared_event_buckets_and_history', source_url: 'https://www.okx.com/docs-v5/en/' },

  // Bybit
  { provider: 'bybit', market: 'spot', capability: 'ticker_bbo_24h', official_available: true, official_scope: 'category_batch', transport: 'REST/WS', batch_mode: 'spot_batch', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'none', source_url: 'https://bybit-exchange.github.io/docs/v5/websocket/public/ticker' },
  { provider: 'bybit', market: 'contract', capability: 'linear_ticker_mark_index_oi_funding_bbo', official_available: true, official_scope: 'category_batch', transport: 'REST/WS', batch_mode: 'linear_batch', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'selected_fields_bucketed_elsewhere', source_url: 'https://bybit-exchange.github.io/docs/v5/websocket/public/ticker' },
  { provider: 'bybit', market: 'contract', capability: 'directory', official_available: true, official_scope: 'USDT_perpetual_full_directory', transport: 'existing_market_light_official_source', batch_mode: 'shared_full_directory', rate_limit_class: 'light_shared', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last verified shared directory', history_policy: 'none', source_url: 'https://bybit-exchange.github.io/docs/v5/market/tickers' },
  { provider: 'bybit', market: 'contract', capability: 'bbo', official_available: true, official_scope: 'USDT_perpetual_full_directory', transport: 'existing_market_light_official_source', batch_mode: 'shared_full_directory_bbo', rate_limit_class: 'light_shared', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready_step990', fallback_policy: 'last verified shared BBO or null', history_policy: 'none', source_url: 'https://bybit-exchange.github.io/docs/v5/market/tickers' },
  { provider: 'bybit', market: 'contract', capability: 'open_interest_history', official_available: true, official_scope: 'per_symbol_focus15', transport: 'public_REST', batch_mode: 'focus15_direct_5min_1h_1d_shared_6h_missing_only_rotation_recovery', rate_limit_class: 'medium_shared', collector: 'bybit-advanced-slow-stats', target_layer: 'history', current_integration: 'ready_step995', fallback_policy: 'last_verified_until_stale_then_missing; official empty stays empty', history_policy: 'direct_official_5min_1h_1d_no_derived_relabel', source_url: 'https://bybit-exchange.github.io/docs/v5/market/open-interest' },
  { provider: 'bybit', market: 'contract', capability: 'global_long_short_ratio', official_available: true, official_scope: 'USDT_linear_per_symbol_5m', transport: 'REST', batch_mode: 'account_ratio_5m_shared_rotation', rate_limit_class: 'shared_bounded', collector: 'contract-flow-metric-history', target_layer: 'current_and_history', current_integration: 'ready_existing_contract_flow', fallback_policy: 'USDC/USD account ratio stays unavailable; no USDT substitution', history_policy: '5m_metric_rows', source_url: 'https://api.bybit.com/v5/market/account-ratio' },
  { provider: 'bybit', market: 'contract', capability: 'risk_limit_insurance_pool', official_available: true, official_scope: 'risk_per_focus15_plus_one_shared_USDT_insurance_request', transport: 'public_REST', batch_mode: 'risk_focus15_6h_plus_insurance_USDT_30m', rate_limit_class: 'slow_shared', collector: 'bybit-advanced-slow-stats', target_layer: 'risk_reference', current_integration: 'ready_step995', fallback_policy: 'last_verified_until_stale_then_missing; insurance unmapped stays null', history_policy: 'reference_snapshot', source_url: 'https://bybit-exchange.github.io/docs/v5/market/risk-limit' },
  { provider: 'bybit', market: 'contract', capability: 'depth20_current_focus', official_available: true, official_scope: 'focus15_current', transport: 'public_orderbook_shared_backend', batch_mode: 'bounded_focus15_depth20', rate_limit_class: 'shared_bounded', collector: 'deep-market-collector', target_layer: 'depth', current_integration: 'ready_step1004_1_5', fallback_policy: 'last verified until stale then null; no cross-provider substitution', history_policy: 'current_only', source_url: 'https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook' },
  { provider: 'bybit', market: 'contract', capability: 'liquidation_current', official_available: true, official_scope: 'public_event_stream_current', transport: 'public_WS_or_public_event_endpoint', batch_mode: 'shared_backend_event_collection', rate_limit_class: 'shared_bounded', collector: 'liquidation-collector', target_layer: 'liquidation_current', current_integration: 'ready_step997', fallback_policy: 'verified zero distinct from missing; no cross-provider substitution', history_policy: 'shared_event_buckets_and_history', source_url: 'https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook' },

  // Bitget
  { provider: 'bitget', market: 'spot', capability: 'ticker_bbo_24h', official_available: true, official_scope: 'product_batch', transport: 'REST/WS', batch_mode: 'SPOT_batch', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'leave_null_if_official_row_omits_quote', history_policy: 'none', source_url: 'https://www.bitget.com/api-doc/uta/changelog' },
  { provider: 'bitget', market: 'contract', capability: 'ticker_mark_index_oi_funding_bbo', official_available: true, official_scope: 'product_batch', transport: 'REST/WS', batch_mode: 'USDT_FUTURES_batch', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'selected_fields_bucketed_elsewhere', source_url: 'https://www.bitget.com/api-doc/uta/changelog' },
  { provider: 'bitget', market: 'contract', capability: 'directory', official_available: true, official_scope: 'USDT_perpetual_full_directory', transport: 'existing_market_light_official_source', batch_mode: 'shared_full_directory', rate_limit_class: 'light_shared', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last verified shared directory', history_policy: 'none', source_url: 'https://www.bitget.com/api-doc/contract/market/Get-All-Symbol-Ticker' },
  { provider: 'bitget', market: 'contract', capability: 'bbo', official_available: true, official_scope: 'USDT_perpetual_full_directory', transport: 'existing_market_light_official_source', batch_mode: 'shared_full_directory_bbo', rate_limit_class: 'light_shared', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready_step990', fallback_policy: 'last verified shared BBO or null', history_policy: 'none', source_url: 'https://www.bitget.com/api-doc/contract/market/Get-All-Symbol-Ticker' },
  { provider: 'bitget', market: 'contract', capability: 'next_funding_time_interval', official_available: true, official_scope: 'category_batch_symbol_optional', transport: 'REST', batch_mode: 'USDT-FUTURES_category_batch', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready_step991', fallback_policy: 'last_verified_shared_snapshot_or_null', history_policy: 'none', source_url: 'https://www.bitget.com/api-doc/uta/public/Get-Current-Funding-Rate' },
  { provider: 'bitget', market: 'spot', capability: 'whale_fund_net_capital_flow', official_available: true, official_scope: 'per_symbol_focus_intersection', transport: 'REST', batch_mode: 'shared_focus15_slow_stats', rate_limit_class: '1_per_sec_per_endpoint', collector: 'slow-stats-collector', target_layer: 'funds_official', current_integration: 'ready_step991_plus_step1004_timestamped_history', fallback_policy: 'keep_derived_separate_never_relabel', history_policy: 'whale_and_net_timestamped_response_array_history_owned_by_step1004; fund_flow_native_period_matrix_remains_separate', source_url: 'https://www.bitget.com/api-doc/uta/public/trading-data/Get-Spot-Whale-Net-Flow' },
  { provider: 'bitget', market: 'spot', capability: 'whale_net_timestamped_history', official_available: true, official_scope: 'current_spot_focus_intersection_existing_step991_response_arrays', transport: 'reuse_existing_public_REST_response_arrays', batch_mode: 'zero_additional_requests_retain_date_ts_rows', rate_limit_class: 'zero_additional_upstream', collector: 'bitget-advanced-slow-stats', target_layer: 'spot_flow_history', current_integration: 'ready_step1004', fallback_policy: 'official timestamped rows only; no synthetic interval; missing stays missing', history_policy: 'native_official_whale_date_rows_plus_native_official_24h_net_capital_ts_rows', source_url: 'https://www.bitget.com/api-doc/uta/public/trading-data/Get-Spot-Whale-Net-Flow' },
  { provider: 'bitget', market: 'contract', capability: 'active_buy_sell_long_short', official_available: true, official_scope: 'per_symbol_focus15', transport: 'REST', batch_mode: 'four_official_5m_focus_lanes', rate_limit_class: '1_per_sec_per_endpoint', collector: 'slow-stats-collector', target_layer: 'contract_stats', current_integration: 'ready_step991', fallback_policy: 'keep_derived_separate_never_relabel', history_policy: 'official_5m_response_arrays_retained_step1001; 15m_1h_4h_1d_backend_rollups_explicitly_derived', source_url: 'https://www.bitget.com/api-doc/uta/public/Get-Futures-Active-Buy-Sell' },
  { provider: 'bitget', market: 'contract', capability: 'active_buy_sell_long_short_history', official_available: true, official_scope: 'focus15_existing_5m_response_arrays', transport: 'reuse_existing_public_REST_responses', batch_mode: 'zero_additional_exchange_requests_capture_step991_response_arrays_plus_on_read_backend_rollups', rate_limit_class: 'zero_additional_upstream', collector: 'bitget-advanced-slow-stats', target_layer: 'contract_stats_history', current_integration: 'ready_step1001', fallback_policy: 'official_5m_missing_stays_missing; derived rollup missing stays missing; never cross-provider substitute', history_policy: 'official_5m_base_plus_explicitly_derived_15m_1h_4h_1d', source_url: 'https://www.bitget.com/api-doc/uta/public/Get-Futures-Active-Buy-Sell' },
  { provider: 'bitget', market: 'contract', capability: 'long_short_ratio_families', official_available: true, official_scope: 'per_symbol_5m_global_account_position_families', transport: 'REST', batch_mode: 'futures_long_short_plus_account_long_short_plus_position_long_short', rate_limit_class: 'shared_bounded', collector: 'contract-flow-metric-history', target_layer: 'current_and_history', current_integration: 'ready_existing_contract_flow', fallback_policy: 'each ratio family stays separate; no family substitution', history_policy: '5m_metric_rows', source_url: 'https://api.bitget.com/api/v3/market/futures-long-short' },
  { provider: 'bitget', market: 'contract', capability: 'risk_reserve_position_tier_oi_limit_index_components', official_available: true, official_scope: 'batch_plus_focus15', transport: 'REST', batch_mode: 'risk_reserve_all_and_oi_limit_batch_plus_focus_tier_index', rate_limit_class: 'slow_shared', collector: 'slow-stats-collector', target_layer: 'risk_reference', current_integration: 'ready_step991', fallback_policy: 'missing', history_policy: 'risk_reserve_history_now_owned_by_step1000; current_reference_and_history_separate', source_url: 'https://www.bitget.com/api-doc/uta/public/Get-Risk-Reserve-All' },
  { provider: 'bitget', market: 'contract', capability: 'risk_reserve_history', official_available: true, official_scope: 'focus15_mapped_to_current_official_reserve_pools', transport: 'public_REST', batch_mode: 'one_representative_focus_symbol_per_current_reserve_pool_for_daily_and_hourly_history', rate_limit_class: 'slow_shared_pool_dedup', collector: 'bitget-advanced-slow-stats', target_layer: 'risk_history', current_integration: 'ready_step1000', fallback_policy: 'last_shared_verified_until_stale_then_missing; official empty remains empty; no current-balance derivation', history_policy: 'native_official_daily_plus_native_official_hourly', source_url: 'https://www.bitget.com/api-doc/uta/public/Get-Risk-Reserve' },
  { provider: 'bitget', market: 'contract', capability: 'liquidation_history', official_available: true, official_scope: 'category_wide_public_history_three_product_types', transport: 'public_REST', batch_mode: 'shared_background_delayed_closed_minute_reconcile_with_nonblocking_deferred_retry_after_public_ws_live_ingress', rate_limit_class: '5_per_sec_ip_bounded_350ms_gap', collector: 'liquidation-collector', target_layer: 'unified_history', current_integration: 'ready_v46_closure_step997_2_1', fallback_policy: 'live_ws_provisional_then_official_rest_reconcile; official empty/delayed/conflicting REST never overwrites; failed windows defer without blocking newer windows', history_policy: 'official_last_3_days_source_reconciles_nonzero_1m_buckets', source_url: 'https://www.bitget.com/api-doc/uta/public/Get-Liquidations' },
  { provider: 'bitget', market: 'contract', capability: 'depth20_current_focus', official_available: true, official_scope: 'focus15_current', transport: 'public_orderbook_shared_backend', batch_mode: 'bounded_focus15_depth20', rate_limit_class: 'shared_bounded', collector: 'deep-market-collector', target_layer: 'depth', current_integration: 'ready_step1004_1_5', fallback_policy: 'last verified until stale then null; no cross-provider substitution', history_policy: 'current_only', source_url: 'https://www.bitget.com/api-doc/contract/websocket/public/Order-Book-Channel' },
  { provider: 'bitget', market: 'contract', capability: 'liquidation_current', official_available: true, official_scope: 'public_event_stream_current', transport: 'public_WS_or_public_event_endpoint', batch_mode: 'shared_backend_event_collection', rate_limit_class: 'shared_bounded', collector: 'liquidation-collector', target_layer: 'liquidation_current', current_integration: 'ready_step997', fallback_policy: 'verified zero distinct from missing; no cross-provider substitution', history_policy: 'shared_event_buckets_and_history', source_url: 'https://www.bitget.com/api-doc/contract/websocket/public/Order-Book-Channel' },

  // Gate
  { provider: 'gate', market: 'spot', capability: 'ticker_bbo_24h', official_available: true, official_scope: 'batch', transport: 'REST/WS', batch_mode: 'all_spot_tickers', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'none', source_url: 'https://www.gate.com/docs/developers/apiv4/en/' },
  { provider: 'gate', market: 'contract', capability: 'ticker_mark_index_funding_bbo', official_available: true, official_scope: 'batch', transport: 'REST/WS', batch_mode: 'USDT_futures_tickers', rate_limit_class: 'light', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'selected_fields_bucketed_elsewhere', source_url: 'https://www.gate.com/docs/developers/apiv4/en/' },
  { provider: 'gate', market: 'contract', capability: 'directory', official_available: true, official_scope: 'USDT_perpetual_full_directory', transport: 'existing_market_light_official_source', batch_mode: 'shared_full_directory', rate_limit_class: 'light_shared', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last verified shared directory', history_policy: 'none', source_url: 'https://www.gate.com/docs/developers/futures/' },
  { provider: 'gate', market: 'contract', capability: 'bbo', official_available: true, official_scope: 'USDT_perpetual_full_directory', transport: 'existing_market_light_official_source', batch_mode: 'shared_full_directory_bbo', rate_limit_class: 'light_shared', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready_step990', fallback_policy: 'last verified shared BBO or null', history_policy: 'none', source_url: 'https://www.gate.com/docs/developers/futures/' },
  { provider: 'gate', market: 'contract', capability: 'open_interest', official_available: true, official_scope: 'per_contract_contract_stats', transport: 'REST', batch_mode: 'focus15_existing_5m_contract_stats_requests_limit100', rate_limit_class: 'slow_shared_zero_additional_request_count', collector: 'slow-stats-collector', target_layer: 'contract_stats', current_integration: 'ready_step1003', fallback_policy: 'missing_not_provider_total_size', history_policy: 'official_5m_retained_same_step992_requests_plus_explicit_1h_1d_backend_rollups', source_url: 'https://www.gate.com/docs/developers/apiv4/en/' },
  { provider: 'gate', market: 'contract', capability: 'contract_stats_top_trader_taker_account', official_available: true, official_scope: 'per_contract_contract_stats', transport: 'REST', batch_mode: 'focus15_existing_5m_contract_stats_requests_limit100', rate_limit_class: 'slow_shared_zero_additional_request_count', collector: 'slow-stats-collector', target_layer: 'contract_stats_history', current_integration: 'ready_step1003', fallback_policy: 'official_5m_missing_stays_missing; derived missing stays missing; no cross-provider substitution', history_policy: 'official_5m_state_and_flow_fields_plus_explicit_1h_1d_semantic_rollups', source_url: 'https://www.gate.com/docs/developers/apiv4/en/' },
  { provider: 'gate', market: 'contract', capability: 'contract_stats_history', official_available: true, official_scope: 'focus15_existing_step992_requests', transport: 'reuse_existing_public_REST_response_arrays_plus_shared_persistence', batch_mode: 'same_15_requests_limit100_zero_additional_request_count', rate_limit_class: 'zero_additional_exchange_request_count', collector: 'gate-advanced-slow-stats', target_layer: 'contract_stats_history', current_integration: 'ready_step1003', fallback_policy: 'last_verified_shared_persisted_5m; no derived-as-official; no cross-provider substitution', history_policy: 'official_5m_plus_explicit_1h_1d_field_semantic_rollups', source_url: 'https://www.gate.com/docs/developers/apiv4/en/' },
  { provider: 'gate', market: 'contract', capability: 'risk_limit_tiers', official_available: true, official_scope: 'public_per_contract_or_top100_market', transport: 'REST', batch_mode: 'focus15_per_contract', rate_limit_class: 'slow_shared', collector: 'slow-stats-collector', target_layer: 'risk_reference', current_integration: 'ready_step992', fallback_policy: 'missing', history_policy: 'reference_snapshot', source_url: 'https://www.gate.com/docs/developers/apiv4/en/' },
  { provider: 'gate', market: 'contract', capability: 'insurance_fund', official_available: true, official_scope: 'public_settlement_history', transport: 'REST', batch_mode: 'one_shared_low_frequency_request', rate_limit_class: 'slow_shared', collector: 'slow-stats-collector', target_layer: 'risk_reference', current_integration: 'ready_step992', fallback_policy: 'last_verified_shared_snapshot_or_missing', history_policy: 'official_history', source_url: 'https://www.gate.com/docs/developers/apiv4/en/' },
  { provider: 'gate', market: 'contract', capability: 'liquidation_event_history', official_available: true, official_scope: 'public_all_usdt_closed_minute_liq_orders', transport: 'REST+existing_public_WS', batch_mode: 'one_closed_minute_liq_orders_request_per_60s_plus_shared_ws', rate_limit_class: 'bounded_background_event_history', collector: 'liquidation-collector', target_layer: 'liquidation_history', current_integration: 'ready_step997', fallback_policy: 'complete_liq_orders_minute_owns_gate_1m_history; truncated_window_never_overwrites; never_cross_provider_substitute', history_policy: '1m_base_plus_5m_15m_and_1h_base_plus_6h_24h', source_url: 'https://www.gate.com/docs/developers/apiv4/en/' },
  { provider: 'gate', market: 'contract', capability: 'depth20_current_focus', official_available: true, official_scope: 'focus15_current', transport: 'public_orderbook_shared_backend', batch_mode: 'bounded_focus15_depth20', rate_limit_class: 'shared_bounded', collector: 'deep-market-collector', target_layer: 'depth', current_integration: 'ready_step1004_1_5', fallback_policy: 'last verified until stale then null; no cross-provider substitution', history_policy: 'current_only', source_url: 'https://www.gate.com/docs/developers/futures/' },
  { provider: 'gate', market: 'contract', capability: 'liquidation_current', official_available: true, official_scope: 'public_event_stream_current', transport: 'public_WS_or_public_event_endpoint', batch_mode: 'shared_backend_event_collection', rate_limit_class: 'shared_bounded', collector: 'liquidation-collector', target_layer: 'liquidation_current', current_integration: 'ready_step997', fallback_policy: 'verified zero distinct from missing; no cross-provider substitution', history_policy: 'shared_event_buckets_and_history', source_url: 'https://www.gate.com/docs/developers/futures/' },

  // Step1004.6 / original Step998: realized liquidation heatmap is a same-venue derived layer
  // from already-shared official/public liquidation events. It never estimates future liquidation risk.
  { provider: 'binance', market: 'contract', capability: 'liquidation_realized_price_heatmap', official_available: true, official_scope: 'public_realized_liquidation_events_same_provider_same_symbol_USDT', transport: 'reuse_existing_shared_liquidation_WS', batch_mode: 'shared_25bps_price_buckets_up_to_24h_observed_session', rate_limit_class: 'zero_additional_exchange_requests', collector: 'liquidation-collector', target_layer: 'liquidation_heatmap', current_integration: 'ready_step1004_6', fallback_policy: 'missing_presession_distribution_stays_missing; no estimated-risk substitution', history_policy: 'process_memory_price_buckets; Step997 time history remains separate', source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/web-socket-streams' },
  { provider: 'okx', market: 'contract', capability: 'liquidation_realized_price_heatmap', official_available: true, official_scope: 'public_realized_liquidation_events_same_provider_same_symbol_USDT', transport: 'reuse_existing_shared_liquidation_WS', batch_mode: 'shared_25bps_price_buckets_up_to_24h_observed_session', rate_limit_class: 'zero_additional_exchange_requests', collector: 'liquidation-collector', target_layer: 'liquidation_heatmap', current_integration: 'ready_step1004_6', fallback_policy: 'missing_presession_distribution_stays_missing; no estimated-risk substitution', history_policy: 'process_memory_price_buckets; Step997 time history remains separate', source_url: 'https://www.okx.com/docs-v5/en/' },
  { provider: 'bybit', market: 'contract', capability: 'liquidation_realized_price_heatmap', official_available: true, official_scope: 'public_realized_liquidation_events_same_provider_same_symbol_USDT', transport: 'reuse_existing_shared_liquidation_WS', batch_mode: 'shared_25bps_price_buckets_up_to_24h_observed_session', rate_limit_class: 'zero_additional_exchange_requests', collector: 'liquidation-collector', target_layer: 'liquidation_heatmap', current_integration: 'ready_step1004_6', fallback_policy: 'missing_presession_distribution_stays_missing; no estimated-risk substitution', history_policy: 'process_memory_price_buckets; Step997 time history remains separate', source_url: 'https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation' },
  { provider: 'bitget', market: 'contract', capability: 'liquidation_realized_price_heatmap', official_available: true, official_scope: 'public_realized_liquidation_events_same_provider_same_symbol_USDT_plus_accepted_delayed_official_minutes', transport: 'reuse_existing_shared_liquidation_WS_plus_existing_official_history_reconcile', batch_mode: 'shared_25bps_price_buckets_up_to_24h_observed_session', rate_limit_class: 'zero_additional_exchange_requests', collector: 'liquidation-collector', target_layer: 'liquidation_heatmap', current_integration: 'ready_step1004_6', fallback_policy: 'official delayed minute replaces live minute only after existing Step997 reconciliation accepts coverage; otherwise live observation remains', history_policy: 'process_memory_price_buckets_with_existing_official_minute_reconcile; Step997 time history remains separate', source_url: 'https://www.bitget.com/api-doc/uta/public/Get-Liquidations' },
  { provider: 'gate', market: 'contract', capability: 'liquidation_realized_price_heatmap', official_available: true, official_scope: 'public_realized_liquidation_events_same_provider_same_symbol_USDT_plus_complete_official_closed_minutes', transport: 'reuse_existing_shared_liquidation_WS_plus_existing_liq_orders_reconcile', batch_mode: 'shared_25bps_price_buckets_up_to_24h_observed_session', rate_limit_class: 'zero_additional_exchange_requests', collector: 'liquidation-collector', target_layer: 'liquidation_heatmap', current_integration: 'ready_step1004_6', fallback_policy: 'complete official liq_orders minute replaces live minute; truncated/incomplete official minute never overwrites', history_policy: 'process_memory_price_buckets_with_existing_official_minute_reconcile; Step997 time history remains separate', source_url: 'https://www.gate.com/docs/developers/apiv4/en/' },

  // Coinbase is project spot-only.
  { provider: 'coinbase', market: 'spot', capability: 'ticker_24h', official_available: true, official_scope: 'multi_product_public_ws', transport: 'WS', batch_mode: 'ticker_batch_5s', rate_limit_class: 'one_shared_connection', collector: 'market-light-collector', target_layer: 'market_light', current_integration: 'ready', fallback_policy: 'last_verified_shared_snapshot', history_policy: 'none', source_url: 'https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels' },
  { provider: 'coinbase', market: 'spot', capability: 'bbo', official_available: true, official_scope: 'ticker_or_level2_not_ticker_batch', transport: 'WS', batch_mode: 'no_light_batch_bbo', rate_limit_class: 'higher_bandwidth', collector: 'deep-market-collector', target_layer: 'focus_or_user_exact', current_integration: 'intentionally_not_market_light', fallback_policy: 'missing_in_full_market_light', history_policy: 'none', source_url: 'https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels' },
  { provider: 'coinbase', market: 'spot', capability: 'market_trades_level2_candles', official_available: true, official_scope: 'per_product_public_exact', transport: 'public_WS_plus_public_REST', batch_mode: 'ticker_batch_full_market_plus_level2_exact_on_demand_plus_market_trades_exact', rate_limit_class: 'bounded_exact', collector: 'existing_market_light_plus_contract_depth_exact', target_layer: 'market_light_plus_user_exact', current_integration: 'ready_step995_existing_paths_formalized', fallback_policy: 'cache_inflight_circuit; no cross-product substitution', history_policy: 'existing_trade_pagination_and_candles_on_demand', source_url: 'https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels' },
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
  const businessValidation = validateBusinessSourceRules(CAPABILITIES);
  const binanceAdvanced = getBinanceAdvancedStatsHealth();
  const bitgetAdvanced = getBitgetAdvancedStatsHealth();
  const gateAdvanced = getGateAdvancedStatsHealth();
  const okxAdvanced = getOkxAdvancedStatsHealth();
  const bybitAdvanced = getBybitAdvancedStatsHealth();
  const derivativesPublic = getDerivativesPublicHealth();
  const contractDepth = getContractDepthHealth();
  const contractFocus = getContractFocusPoolHealth();
  const contractFlow = getContractFlowHealth();
  const contractDeep = getContractDeepSharedHealth();
  const deepMarketBridge = getDeepMarketBridgeHealth();
  const slowStatsBridge = getSlowStatsBridgeHealth();
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
    business_policy_version: BUSINESS_SOURCE_POLICY_VERSION,
    business_rules_ready: businessValidation.ready === true,
    business_rule_count: businessValidation.rule_count,
    business_rule_unique_count: businessValidation.unique_rule_count,
    business_rule_missing_capability_refs: businessValidation.missing_capability_refs,
    business_rule_duplicate_keys: businessValidation.duplicate_rule_keys,
    business_rules: includeCapabilities ? BUSINESS_SOURCE_RULES : [],
    collector_targets: ['market-light-collector', 'liquidation-collector', 'deep-market-collector', 'slow-stats-collector', 'binance-advanced-slow-stats', 'okx-advanced-slow-stats', 'derivatives-public-slow-stats'],
    runtime_market_light_version: health?.version || null,
    runtime_market_light_11_exact: allMarketExact,
    runtime_market_light_coverage: coverage,
    binance_spot_step1001_7: {
      ready: health.binance_spot_ticker_shared_ws?.ready === true &&
        Number(health.binance_spot_ticker_shared_ws?.cached_rows || 0) > 0 &&
        Number(health.binance_spot_ticker_shared_ws?.cached_rows || 0) === Number(health.binance_spot_ticker_shared_ws?.directory_rows || 0) &&
        health.binance_spot_ticker_shared_ws?.public_market_data_only_rest_used === true &&
        health.binance_spot_ticker_shared_ws?.authenticated_rest_used === false &&
        health.binance_spot_ticker_shared_ws?.edge_relay_used === false &&
        health.binance_spot_ticker_shared_ws?.directory_from_public_market_data_rest_baseline === true &&
        health.binance_spot_ticker_shared_ws?.background_baseline_only === true &&
        health.binance_spot_ticker_shared_ws?.user_reads_trigger_baseline_requests === false &&
        health.binance_spot_ticker_shared_ws?.reads_scale_with_users === false,
      public_market_data_rest_base: health.binance_spot_ticker_shared_ws?.public_market_data_rest_base || null,
      public_market_data_rest_method: health.binance_spot_ticker_shared_ws?.public_market_data_rest_method || null,
      baseline_rows: Number(health.binance_spot_ticker_shared_ws?.baseline_rows || 0),
      baseline_successes: Number(health.binance_spot_ticker_shared_ws?.baseline_successes || 0),
      baseline_failures: Number(health.binance_spot_ticker_shared_ws?.baseline_failures || 0),
      baseline_last_http_status: Number(health.binance_spot_ticker_shared_ws?.baseline_last_http_status || 0),
      stream_connected: health.binance_spot_ticker_shared_ws?.stream_connected === true,
      market_stream: health.binance_spot_ticker_shared_ws?.market_stream || null,
      cached_rows: Number(health.binance_spot_ticker_shared_ws?.cached_rows || 0),
      directory_rows: Number(health.binance_spot_ticker_shared_ws?.directory_rows || 0),
      public_market_data_only_rest_used: health.binance_spot_ticker_shared_ws?.public_market_data_only_rest_used === true,
      authenticated_rest_used: health.binance_spot_ticker_shared_ws?.authenticated_rest_used === true,
      edge_relay_used: health.binance_spot_ticker_shared_ws?.edge_relay_used === true,
      user_reads_trigger_baseline_requests: health.binance_spot_ticker_shared_ws?.user_reads_trigger_baseline_requests === true,
      reads_scale_with_users: health.binance_spot_ticker_shared_ws?.reads_scale_with_users === true,
    },
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
    okx_step994: {
      ready: okxAdvanced.ready === true,
      version: okxAdvanced.version || null,
      focus_target: Number(okxAdvanced.focus_target || 0),
      row_count: Number(okxAdvanced.row_count || 0),
      core_target_count: Number(okxAdvanced.core_target_count || 0),
      open_interest_rows: Number(okxAdvanced.open_interest_rows || 0),
      core_open_interest_rows: Number(okxAdvanced.core_open_interest_rows || 0),
      funding_rows: Number(okxAdvanced.funding_rows || 0),
      core_funding_rows: Number(okxAdvanced.core_funding_rows || 0),
      price_limit_coverage_rows: Number(okxAdvanced.price_limit_coverage_rows || 0),
      price_limit_enabled_rows: Number(okxAdvanced.price_limit_enabled_rows || 0),
      core_price_limit_coverage_rows: Number(okxAdvanced.core_price_limit_coverage_rows || 0),
      security_fund_record_count: Number(okxAdvanced.security_fund_record_count || 0),
      security_fund_focus_rows: Number(okxAdvanced.security_fund_focus_rows || 0),
      adl_warning_channel_ready: okxAdvanced.adl_warning_channel_ready === true,
      adl_warning_active_rows: Number(okxAdvanced.adl_warning_active_rows || 0),
      adl_warning_subscribed_families: Number(okxAdvanced.adl_warning_subscribed_families || 0),
      adl_warning_desired_families: Number(okxAdvanced.adl_warning_desired_families || 0),
      market_light_oi_reused_only: okxAdvanced.market_light_oi_reused_only === true,
      market_light_oi_additional_exchange_requests: Number(okxAdvanced.market_light_oi_additional_exchange_requests || 0),
      adl_warning_normal_state_not_fabricated: okxAdvanced.adl_warning_normal_state_not_fabricated === true,
      adl_warning_one_shared_connection: okxAdvanced.adl_warning_one_shared_connection === true,
      provider_request_governor_reused: okxAdvanced.provider_request_governor_reused === true,
      custom_provider_governor_created: okxAdvanced.custom_provider_governor_created === true,
      user_reads_scale_with_users: false,
    },
    okx_step1002: {
      ready: okxAdvanced.open_interest_history?.ready === true &&
        Number(okxAdvanced.open_interest_history?.focus_target || 0) === 15 &&
        Number(okxAdvanced.open_interest_history?.official_5m_coverage || 0) === 15 &&
        Number(okxAdvanced.open_interest_history?.total_official_5m_rows || 0) > 0 &&
        Number(okxAdvanced.open_interest_history?.full_cycle_request_cap || 0) === 15 &&
        okxAdvanced.open_interest_history?.official_endpoint === '/api/v5/rubik/stat/contracts/open-interest-history' &&
        okxAdvanced.open_interest_history?.official_period === '5m' &&
        okxAdvanced.open_interest_history?.official_and_derived_separate === true &&
        okxAdvanced.open_interest_history?.shared_background_collector === true &&
        okxAdvanced.open_interest_history?.user_reads_trigger_collector === false &&
        okxAdvanced.open_interest_history?.reads_scale_with_users === false,
      version: okxAdvanced.version || null,
      focus_target: Number(okxAdvanced.open_interest_history?.focus_target || 0),
      official_5m_coverage: Number(okxAdvanced.open_interest_history?.official_5m_coverage || 0),
      fresh_5m_coverage: Number(okxAdvanced.open_interest_history?.fresh_5m_coverage || 0),
      total_official_5m_rows: Number(okxAdvanced.open_interest_history?.total_official_5m_rows || 0),
      official_endpoint: okxAdvanced.open_interest_history?.official_endpoint || null,
      official_period: okxAdvanced.open_interest_history?.official_period || null,
      derived_intervals: Array.isArray(okxAdvanced.open_interest_history?.derived_intervals) ? [...okxAdvanced.open_interest_history.derived_intervals] : [],
      derived_method: okxAdvanced.open_interest_history?.derived_method || null,
      full_cycle_request_cap: Number(okxAdvanced.open_interest_history?.full_cycle_request_cap || 0),
      refresh_seconds: Number(okxAdvanced.open_interest_history?.refresh_seconds || 0),
      per_request_gap_ms: Number(okxAdvanced.open_interest_history?.per_request_gap_ms || 0),
      persistence_enabled: okxAdvanced.open_interest_history?.persistence_enabled === true,
      persistence_table: okxAdvanced.open_interest_history?.persistence_table || null,
      persist_successes: Number(okxAdvanced.open_interest_history?.persist_successes || 0),
      restore_successes: Number(okxAdvanced.open_interest_history?.restore_successes || 0),
      official_and_derived_separate: okxAdvanced.open_interest_history?.official_and_derived_separate === true,
      user_reads_trigger_collector: okxAdvanced.open_interest_history?.user_reads_trigger_collector === true,
      reads_scale_with_users: okxAdvanced.open_interest_history?.reads_scale_with_users === true,
    },
    bybit_step995: {
      ready: bybitAdvanced.ready === true,
      version: bybitAdvanced.version || null,
      focus_target: Number(bybitAdvanced.focus_target || 0),
      row_count: Number(bybitAdvanced.row_count || 0),
      current_open_interest_rows: Number(bybitAdvanced.current_open_interest_rows || 0),
      current_open_interest_reused_from_market_light: bybitAdvanced.current_open_interest_reused_from_market_light === true,
      current_open_interest_additional_requests: Number(bybitAdvanced.current_open_interest_additional_requests || 0),
      history_intervals: Array.isArray(bybitAdvanced.history_intervals) ? [...bybitAdvanced.history_intervals] : [],
      history_complete_symbols: Number(bybitAdvanced.history_complete_symbols || 0),
      history_official_coverage_by_interval: { ...(bybitAdvanced.history_official_coverage_by_interval || {}) },
      risk_official_coverage_rows: Number(bybitAdvanced.risk_official_coverage_rows || 0),
      insurance_official_coverage_rows: Number(bybitAdvanced.insurance_official_coverage_rows || 0),
      insurance_mapped_rows: Number(bybitAdvanced.insurance_mapped_rows || 0),
      insurance_pool_count: Number(bybitAdvanced.insurance_pool_count || 0),
      full_cycle_total_request_cap: Number(bybitAdvanced.full_cycle_total_request_cap || 0),
      focus_change_missing_only: bybitAdvanced.focus_change_missing_only === true,
      provider_request_governor_reused: bybitAdvanced.provider_request_governor_reused === true,
      custom_provider_governor_created: bybitAdvanced.custom_provider_governor_created === true,
      user_reads_scale_with_users: false,
    },
    coinbase_step995: {
      ready: health.coinbase_ticker_batch?.connected === true &&
        Number(health.coinbase_ticker_batch?.product_ids || 0) > 0 &&
        Number(health.coinbase_ticker_batch?.cached_rows || 0) > 0 &&
        health.coinbase_ticker_batch?.best_bid_ask_in_ticker_batch === false &&
        contractDepth.coinbase_level2_mode === 'advanced_trade_public_websocket_alias_aware' &&
        contractDepth.coinbase_level2_public_no_auth === true &&
        contractDepth.coinbase_level2_exact_on_demand_bounded === true &&
        contractDepth.coinbase_level2_full_market_always_on === false &&
        contractDepth.coinbase_market_trades_public_rest === true &&
        contractDepth.coinbase_market_trades_side_field_is_maker === true &&
        contractDepth.coinbase_market_trades_output_side_is_taker === true,
      ticker_batch_connected: health.coinbase_ticker_batch?.connected === true,
      ticker_batch_product_ids: Number(health.coinbase_ticker_batch?.product_ids || 0),
      ticker_batch_cached_rows: Number(health.coinbase_ticker_batch?.cached_rows || 0),
      ticker_batch_best_bid_ask_available: health.coinbase_ticker_batch?.best_bid_ask_in_ticker_batch === true,
      level2_mode: contractDepth.coinbase_level2_mode || null,
      level2_public_no_auth: contractDepth.coinbase_level2_public_no_auth === true,
      level2_max_symbols: Number(contractDepth.coinbase_level2_max_symbols || 0),
      level2_exact_on_demand_bounded: contractDepth.coinbase_level2_exact_on_demand_bounded === true,
      level2_full_market_always_on: contractDepth.coinbase_level2_full_market_always_on === true,
      market_trades_public_rest: contractDepth.coinbase_market_trades_public_rest === true,
      market_trades_side_field_is_maker: contractDepth.coinbase_market_trades_side_field_is_maker === true,
      market_trades_output_side_is_taker: contractDepth.coinbase_market_trades_output_side_is_taker === true,
      user_reads_scale_full_market_upstream: false,
    },
    v46_closure: {
      ready: binanceAdvanced.focus_target === 15 &&
        Number(binanceAdvanced.adl_official_coverage_rows || 0) === 15 &&
        binanceAdvanced.edge_relay_only === true &&
        binanceAdvanced.render_direct_binance_rest === false &&
        contractFlow.binance_open_interest_history_official_ready === true &&
        contractFlow.binance_open_interest_history_edge_relay_only === true &&
        contractFlow.binance_long_short_history_official_ready === true &&
        contractFlow.binance_long_short_history_edge_relay_only === true &&
        contractFlow.binance_official_taker?.ready === true &&
        Number(contractFlow.binance_official_taker?.official_coverage_rows || 0) === 15 &&
        contractFlow.binance_official_taker?.existing_edge_relay_governor_reused === true &&
        contractFlow.binance_official_taker?.normalized_shared_snapshot_persisted === true &&
        contractFlow.binance_official_taker?.render_direct_binance_rest === false &&
        contractFlow.binance_official_taker?.custom_provider_governor_created === false &&
        contractFlow.binance_official_taker?.focus_change_detection === 'symbol_signature_not_round_counter' &&
        contractFlow.binance_official_taker?.focus_round_is_not_used_as_symbol_change_signal === true &&
        contractFlow.binance_official_taker?.scheduled_same_signature_refresh_is_full_focus15 === true &&
        liquidationHistory.bitget_liquidations_history?.collector_ready === true &&
        liquidationHistory.bitget_liquidations_history?.official_endpoint_operational === true &&
        Number(liquidationHistory.bitget_liquidations_history?.official_response_cycles || 0) > 0 &&
        liquidationHistory.bitget_liquidations_history?.public_no_auth === true &&
        liquidationHistory.bitget_liquidations_history?.shared_background_collector === true &&
        liquidationHistory.bitget_liquidations_history?.provider_request_governor_reused === true &&
        liquidationHistory.bitget_liquidations_history?.coverage_state_process_memory_only === true &&
        liquidationHistory.bitget_liquidations_history?.coverage_state_persisted === false &&
        liquidationHistory.bitget_liquidations_history?.official_empty_does_not_overwrite_existing_ws === true &&
        liquidationHistory.bitget_liquidations_history?.failed_window_blocks_new_windows === false &&
        liquidationHistory.bitget_liquidations_history?.deferred_retry_enabled === true &&
        liquidationHistory.bitget_liquidations_history?.pending_window_cleared_after_every_attempt === true &&
        Number(liquidationHistory.bitget_liquidations_history?.reconciliation_delay_seconds || 0) >= 1800 &&
        Number(liquidationHistory.bitget_liquidations_history?.max_pages_per_category || 0) === 20 &&
        liquidationHistory.bitget_liquidations_history?.user_reads_trigger_requests === false &&
        liquidationHistory.bitget_liquidations_history?.reads_scale_with_users === false,
      binance_adl_capability_ready: Number(binanceAdvanced.adl_official_coverage_rows || 0) === 15 && binanceAdvanced.edge_relay_only === true && binanceAdvanced.render_direct_binance_rest === false,
      binance_open_interest_history_ready: contractFlow.binance_open_interest_history_official_ready === true && contractFlow.binance_open_interest_history_edge_relay_only === true,
      binance_open_interest_history_endpoint: contractFlow.binance_open_interest_history_endpoint || null,
      binance_open_interest_history_persistence_table: contractFlow.binance_open_interest_history_persistence_table || null,
      binance_long_short_history_ready: contractFlow.binance_long_short_history_official_ready === true && contractFlow.binance_long_short_history_edge_relay_only === true,
      binance_long_short_history_endpoints: contractFlow.binance_long_short_history_endpoints || [],
      binance_official_taker_ready: contractFlow.binance_official_taker?.ready === true,
      binance_official_taker_coverage_rows: Number(contractFlow.binance_official_taker?.official_coverage_rows || 0),
      binance_official_taker_endpoint: contractFlow.binance_official_taker?.official_endpoint || null,
      binance_official_taker_requests_per_focus_cycle_max: Number(contractFlow.binance_official_taker?.requests_per_full_focus_cycle_max || 0),
      binance_official_taker_existing_edge_relay_governor_reused: contractFlow.binance_official_taker?.existing_edge_relay_governor_reused === true,
      binance_official_taker_normalized_shared_snapshot_persisted: contractFlow.binance_official_taker?.normalized_shared_snapshot_persisted === true,
      binance_official_taker_shared_snapshot_table: contractFlow.binance_official_taker?.shared_snapshot_table || null,
      binance_official_taker_restore_successes: Number(contractFlow.binance_official_taker?.restore_successes || 0),
      binance_official_taker_persist_successes: Number(contractFlow.binance_official_taker?.persist_successes || 0),
      binance_official_taker_relay_guard_active: contractFlow.binance_official_taker?.relay_guard_active === true,
      binance_official_taker_relay_guard_next_allowed_at: contractFlow.binance_official_taker?.relay_guard_next_allowed_at || null,
      binance_official_taker_user_reads_trigger_exchange_requests: contractFlow.binance_official_taker?.user_reads_trigger_exchange_requests === true,
      binance_official_taker_focus_change_detection: contractFlow.binance_official_taker?.focus_change_detection || null,
      binance_official_taker_round_not_symbol_change_signal: contractFlow.binance_official_taker?.focus_round_is_not_used_as_symbol_change_signal === true,
      binance_official_taker_same_signature_full_refresh: contractFlow.binance_official_taker?.scheduled_same_signature_refresh_is_full_focus15 === true,
      bitget_liquidation_history_ready: liquidationHistory.bitget_liquidations_history?.collector_ready === true && liquidationHistory.bitget_liquidations_history?.official_endpoint_operational === true,
      bitget_liquidation_history_collector_ready: liquidationHistory.bitget_liquidations_history?.collector_ready === true,
      bitget_liquidation_history_official_endpoint_operational: liquidationHistory.bitget_liquidations_history?.official_endpoint_operational === true,
      bitget_liquidation_history_official_response_cycles: Number(liquidationHistory.bitget_liquidations_history?.official_response_cycles || 0),
      bitget_liquidation_history_complete_windows: Number(liquidationHistory.bitget_liquidations_history?.complete_windows || 0),
      bitget_liquidation_history_deferred_windows: Number(liquidationHistory.bitget_liquidations_history?.deferred_windows || 0),
      bitget_liquidation_history_endpoint: liquidationHistory.bitget_liquidations_history?.endpoint || null,
      bitget_liquidation_history_categories: liquidationHistory.bitget_liquidations_history?.categories || [],
      bitget_liquidation_history_shared_background_collector: liquidationHistory.bitget_liquidations_history?.shared_background_collector === true,
      bitget_liquidation_history_provider_governor_reused: liquidationHistory.bitget_liquidations_history?.provider_request_governor_reused === true,
      bitget_liquidation_history_reconciliation_delay_seconds: Number(liquidationHistory.bitget_liquidations_history?.reconciliation_delay_seconds || 0),
      bitget_liquidation_history_max_pages_per_category: Number(liquidationHistory.bitget_liquidations_history?.max_pages_per_category || 0),
      bitget_liquidation_history_pending_window_start: liquidationHistory.bitget_liquidations_history?.pending_window_start || null,
      bitget_liquidation_history_coverage_state_process_memory_only: liquidationHistory.bitget_liquidations_history?.coverage_state_process_memory_only === true,
      bitget_liquidation_history_coverage_state_persisted: liquidationHistory.bitget_liquidations_history?.coverage_state_persisted === true,
      bitget_liquidation_history_official_empty_does_not_overwrite_existing_ws: liquidationHistory.bitget_liquidations_history?.official_empty_does_not_overwrite_existing_ws === true,
      bitget_liquidation_history_failed_window_blocks_new_windows: liquidationHistory.bitget_liquidations_history?.failed_window_blocks_new_windows === true,
      bitget_liquidation_history_deferred_retry_enabled: liquidationHistory.bitget_liquidations_history?.deferred_retry_enabled === true,
      bitget_liquidation_history_pending_window_cleared_after_every_attempt: liquidationHistory.bitget_liquidations_history?.pending_window_cleared_after_every_attempt === true,
      bitget_liquidation_history_amount_unit_source: liquidationHistory.bitget_liquidations_history?.amount_unit_source || null,
      bitget_liquidation_history_user_reads_trigger_requests: liquidationHistory.bitget_liquidations_history?.user_reads_trigger_requests === true,
      bitget_liquidation_history_reads_scale_with_users: liquidationHistory.bitget_liquidations_history?.reads_scale_with_users === true,
      no_new_binance_direct_render_rest: true,
      user_reads_scale_exchange_upstream: false,
    },
    binance_step993: {
      ready: binanceAdvanced.ready === true &&
        binanceAdvanced.adl_all_symbols_response_cache_retains_nonfocus_symbols === true &&
        binanceAdvanced.adl_dynamic_hot_focus_reuses_cached_all_symbols_snapshot === true &&
        binanceAdvanced.dynamic_focus_missing_only_recovery === true &&
        binanceAdvanced.dynamic_focus_recovery_user_read_triggered === false,
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
      adl_all_symbols_response_cache_retains_nonfocus_symbols: binanceAdvanced.adl_all_symbols_response_cache_retains_nonfocus_symbols === true,
      adl_dynamic_hot_focus_reuses_cached_all_symbols_snapshot: binanceAdvanced.adl_dynamic_hot_focus_reuses_cached_all_symbols_snapshot === true,
      dynamic_focus_watch_seconds: Number(binanceAdvanced.dynamic_focus_watch_seconds || 0),
      dynamic_focus_missing_only_recovery: binanceAdvanced.dynamic_focus_missing_only_recovery === true,
      dynamic_focus_recovery_user_read_triggered: binanceAdvanced.dynamic_focus_recovery_user_read_triggered === true,
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
    bitget_step1000: {
      ready: bitgetAdvanced.risk_reserve_history?.ready === true &&
        Number(bitgetAdvanced.risk_reserve_history?.mapped_focus_rows || 0) === 15 &&
        Number(bitgetAdvanced.risk_reserve_history?.pool_target_count || 0) > 0 &&
        Number(bitgetAdvanced.risk_reserve_history?.daily_official_pool_coverage || 0) === Number(bitgetAdvanced.risk_reserve_history?.pool_target_count || 0) &&
        Number(bitgetAdvanced.risk_reserve_history?.hourly_official_pool_coverage || 0) === Number(bitgetAdvanced.risk_reserve_history?.pool_target_count || 0) &&
        bitgetAdvanced.risk_reserve_history?.representative_symbol_per_pool === true &&
        bitgetAdvanced.risk_reserve_history?.current_risk_reserve_all_mapping_reused === true &&
        Number(bitgetAdvanced.risk_reserve_history?.full_cycle_request_cap || 0) <= 30 &&
        bitgetAdvanced.risk_reserve_history?.shared_background_collector === true &&
        bitgetAdvanced.risk_reserve_history?.incomplete_pool_recovery_enabled === true &&
        bitgetAdvanced.risk_reserve_history?.incomplete_pool_recovery_missing_only === true &&
        bitgetAdvanced.risk_reserve_history?.incomplete_signature_never_marked_complete === true &&
        Number(bitgetAdvanced.risk_reserve_history?.startup_deconflicted_seconds || 0) >= 60 &&
        bitgetAdvanced.risk_reserve_history?.user_reads_trigger_exchange_requests === false &&
        bitgetAdvanced.risk_reserve_history?.reads_scale_with_users === false,
      ready_from_bitget_advanced: bitgetAdvanced.risk_reserve_history?.ready === true,
      mapped_focus_rows: Number(bitgetAdvanced.risk_reserve_history?.mapped_focus_rows || 0),
      pool_target_count: Number(bitgetAdvanced.risk_reserve_history?.pool_target_count || 0),
      pool_dedup_saved_symbol_queries: Number(bitgetAdvanced.risk_reserve_history?.pool_dedup_saved_symbol_queries || 0),
      daily_official_pool_coverage: Number(bitgetAdvanced.risk_reserve_history?.daily_official_pool_coverage || 0),
      hourly_official_pool_coverage: Number(bitgetAdvanced.risk_reserve_history?.hourly_official_pool_coverage || 0),
      daily_nonempty_pools: Number(bitgetAdvanced.risk_reserve_history?.daily_nonempty_pools || 0),
      hourly_nonempty_pools: Number(bitgetAdvanced.risk_reserve_history?.hourly_nonempty_pools || 0),
      full_cycle_request_cap: Number(bitgetAdvanced.risk_reserve_history?.full_cycle_request_cap || 0),
      symbol_naive_request_cap: Number(bitgetAdvanced.risk_reserve_history?.full_cycle_symbol_naive_request_cap || 0),
      representative_symbol_per_pool: bitgetAdvanced.risk_reserve_history?.representative_symbol_per_pool === true,
      current_pool_mapping_reused: bitgetAdvanced.risk_reserve_history?.current_risk_reserve_all_mapping_reused === true,
      official_daily_endpoint: bitgetAdvanced.risk_reserve_history?.official_daily_endpoint || null,
      official_hourly_endpoint: bitgetAdvanced.risk_reserve_history?.official_hourly_endpoint || null,
      shared_background_collector: bitgetAdvanced.risk_reserve_history?.shared_background_collector === true,
      incomplete_pool_recovery_enabled: bitgetAdvanced.risk_reserve_history?.incomplete_pool_recovery_enabled === true,
      incomplete_pool_recovery_missing_only: bitgetAdvanced.risk_reserve_history?.incomplete_pool_recovery_missing_only === true,
      incomplete_signature_never_marked_complete: bitgetAdvanced.risk_reserve_history?.incomplete_signature_never_marked_complete === true,
      startup_deconflicted_seconds: Number(bitgetAdvanced.risk_reserve_history?.startup_deconflicted_seconds || 0),
      request_gap_ms: Number(bitgetAdvanced.risk_reserve_history?.request_gap_ms || 0),
      provider_request_governor_reused: bitgetAdvanced.risk_reserve_history?.provider_request_governor_reused === true,
      user_reads_trigger_exchange_requests: bitgetAdvanced.risk_reserve_history?.user_reads_trigger_exchange_requests === true,
      reads_scale_with_users: bitgetAdvanced.risk_reserve_history?.reads_scale_with_users === true,
    },
    bitget_step1001: {
      ready: bitgetAdvanced.contract_history?.ready === true &&
        Number(bitgetAdvanced.contract_history?.focus_target || 0) === 15 &&
        Number(bitgetAdvanced.contract_history?.official_lane_count || 0) === 4 &&
        bitgetAdvanced.contract_history?.additional_exchange_requests === 0 &&
        bitgetAdvanced.contract_history?.reused_existing_step991_response_arrays === true &&
        bitgetAdvanced.contract_history?.shared_backend_memory === true &&
        bitgetAdvanced.contract_history?.user_reads_trigger_exchange_requests === false &&
        bitgetAdvanced.contract_history?.reads_scale_with_users === false &&
        bitgetAdvanced.contract_history?.official_and_derived_kept_separate === true,
      official_5m_history_ready: bitgetAdvanced.contract_history?.ready === true,
      focus_target: Number(bitgetAdvanced.contract_history?.focus_target || 0),
      official_lane_count: Number(bitgetAdvanced.contract_history?.official_lane_count || 0),
      additional_exchange_requests: Number(bitgetAdvanced.contract_history?.additional_exchange_requests || 0),
      reused_existing_step991_response_arrays: bitgetAdvanced.contract_history?.reused_existing_step991_response_arrays === true,
      shared_backend_memory: bitgetAdvanced.contract_history?.shared_backend_memory === true,
      user_reads_trigger_exchange_requests: bitgetAdvanced.contract_history?.user_reads_trigger_exchange_requests === true,
      reads_scale_with_users: bitgetAdvanced.contract_history?.reads_scale_with_users === true,
      derived_intervals: bitgetAdvanced.contract_history?.derived_intervals || [],
      official_and_derived_kept_separate: bitgetAdvanced.contract_history?.official_and_derived_kept_separate === true,
    },
    bitget_step1004: {
      ready: bitgetAdvanced.spot_timestamped_history?.ready === true &&
        Number(bitgetAdvanced.spot_timestamped_history?.spot_target || 0) > 0 &&
        Number(bitgetAdvanced.spot_timestamped_history?.official_lane_count || 0) === 2 &&
        Number(bitgetAdvanced.spot_timestamped_history?.additional_exchange_requests || 0) === 0 &&
        bitgetAdvanced.spot_timestamped_history?.reused_existing_step991_response_arrays === true &&
        bitgetAdvanced.spot_timestamped_history?.native_official_timestamps_only === true &&
        bitgetAdvanced.spot_timestamped_history?.no_synthetic_interval_rollup === true &&
        bitgetAdvanced.spot_timestamped_history?.user_reads_trigger_exchange_requests === false &&
        bitgetAdvanced.spot_timestamped_history?.reads_scale_with_users === false,
      version: bitgetAdvanced.version || null,
      spot_target: Number(bitgetAdvanced.spot_timestamped_history?.spot_target || 0),
      official_lane_count: Number(bitgetAdvanced.spot_timestamped_history?.official_lane_count || 0),
      lanes: bitgetAdvanced.spot_timestamped_history?.lanes || {},
      captures: Number(bitgetAdvanced.spot_timestamped_history?.captures || 0),
      rows_captured_total: Number(bitgetAdvanced.spot_timestamped_history?.rows_captured_total || 0),
      last_updated_at: bitgetAdvanced.spot_timestamped_history?.last_updated_at || null,
      additional_exchange_requests: Number(bitgetAdvanced.spot_timestamped_history?.additional_exchange_requests || 0),
      reused_existing_step991_response_arrays: bitgetAdvanced.spot_timestamped_history?.reused_existing_step991_response_arrays === true,
      native_official_timestamps_only: bitgetAdvanced.spot_timestamped_history?.native_official_timestamps_only === true,
      no_synthetic_interval_rollup: bitgetAdvanced.spot_timestamped_history?.no_synthetic_interval_rollup === true,
      fund_flow_period_matrix_separate: bitgetAdvanced.spot_timestamped_history?.fund_flow_period_matrix_separate === true,
      user_reads_trigger_exchange_requests: bitgetAdvanced.spot_timestamped_history?.user_reads_trigger_exchange_requests === true,
      reads_scale_with_users: bitgetAdvanced.spot_timestamped_history?.reads_scale_with_users === true,
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
    gate_step1003: {
      ready: gateAdvanced.contract_stats_history?.ready === true &&
        Number(gateAdvanced.contract_stats_history?.focus_target || 0) === 15 &&
        Number(gateAdvanced.contract_stats_history?.official_5m_coverage || 0) === 15 &&
        Number(gateAdvanced.contract_stats_history?.total_official_5m_rows || 0) > 0 &&
        Number(gateAdvanced.contract_stats_history?.additional_exchange_requests_vs_step992 || 0) === 0 &&
        gateAdvanced.contract_stats_history?.reuses_existing_step992_contract_stats_requests === true &&
        gateAdvanced.contract_stats_history?.same_request_current_and_history === true &&
        gateAdvanced.contract_stats_history?.official_and_derived_separate === true &&
        gateAdvanced.contract_stats_history?.user_reads_trigger_collector === false &&
        gateAdvanced.contract_stats_history?.reads_scale_with_users === false,
      version: gateAdvanced.version || null,
      focus_target: Number(gateAdvanced.contract_stats_history?.focus_target || 0),
      official_5m_coverage: Number(gateAdvanced.contract_stats_history?.official_5m_coverage || 0),
      fresh_5m_coverage: Number(gateAdvanced.contract_stats_history?.fresh_5m_coverage || 0),
      total_official_5m_rows: Number(gateAdvanced.contract_stats_history?.total_official_5m_rows || 0),
      official_interval: gateAdvanced.contract_stats_history?.official_interval || null,
      official_endpoint: gateAdvanced.contract_stats_history?.official_endpoint || null,
      official_limit_per_existing_request: Number(gateAdvanced.contract_stats_history?.official_limit_per_existing_request || 0),
      request_count_per_full_focus_cycle: Number(gateAdvanced.contract_stats_history?.request_count_per_full_focus_cycle || 0),
      additional_exchange_requests_vs_step992: Number(gateAdvanced.contract_stats_history?.additional_exchange_requests_vs_step992 || 0),
      reuses_existing_step992_contract_stats_requests: gateAdvanced.contract_stats_history?.reuses_existing_step992_contract_stats_requests === true,
      same_request_current_and_history: gateAdvanced.contract_stats_history?.same_request_current_and_history === true,
      derived_intervals: Array.isArray(gateAdvanced.contract_stats_history?.derived_intervals) ? [...gateAdvanced.contract_stats_history.derived_intervals] : [],
      derived_method: gateAdvanced.contract_stats_history?.derived_method || null,
      rollup_state_fields: Array.isArray(gateAdvanced.contract_stats_history?.rollup_state_fields) ? [...gateAdvanced.contract_stats_history.rollup_state_fields] : [],
      rollup_sum_fields: Array.isArray(gateAdvanced.contract_stats_history?.rollup_sum_fields) ? [...gateAdvanced.contract_stats_history.rollup_sum_fields] : [],
      persistence_enabled: gateAdvanced.contract_stats_history?.persistence_enabled === true,
      persistence_table: gateAdvanced.contract_stats_history?.persistence_table || null,
      persist_successes: Number(gateAdvanced.contract_stats_history?.persist_successes || 0),
      restore_successes: Number(gateAdvanced.contract_stats_history?.restore_successes || 0),
      user_reads_trigger_collector: gateAdvanced.contract_stats_history?.user_reads_trigger_collector === true,
      reads_scale_with_users: gateAdvanced.contract_stats_history?.reads_scale_with_users === true,
    },
    collector_isolation_first_batch: (() => {
      const isolation = getCollectorIsolationHealth();
      const marketRole = isolation.roles?.['market-light'] || {};
      const liquidationRole = isolation.roles?.liquidation || {};
      return {
        ready: isolation.enabled === true &&
          isolation.first_batch_roles_alive === true &&
          isolation.first_batch_child_pids_distinct === true &&
          isolation.first_batch_parent_pid_distinct === true &&
          isolation.first_batch_process_level_fault_domains === true &&
          isolation.one_role_exit_does_not_exit_parent === true &&
          isolation.role_scoped_supervisor_restart === true &&
          marketRole.alive === true &&
          liquidationRole.alive === true &&
          health?.isolated_bridge === true &&
          health?.isolated_bridge_fresh === true &&
          liquidationHistory?.isolated_bridge === true &&
          liquidationHistory?.isolated_bridge_fresh === true,
        version: isolation.version || null,
        parent_pid: isolation.parent_pid || null,
        market_light_pid: marketRole.pid || null,
        liquidation_pid: liquidationRole.pid || null,
        all_roles_alive: isolation.first_batch_roles_alive === true,
        child_pids_distinct: isolation.first_batch_child_pids_distinct === true,
        parent_pid_distinct_from_children: isolation.first_batch_parent_pid_distinct === true,
        process_level_fault_domains: isolation.process_level_fault_domains === true,
        role_scoped_supervisor_restart: isolation.role_scoped_supervisor_restart === true,
        market_light_bridge_fresh: health?.isolated_bridge_fresh === true,
        liquidation_bridge_fresh: liquidationHistory?.isolated_bridge_fresh === true,
        market_light_parent_scanner_started: isolation.market_light_parent_scanner_started === true,
        liquidation_parent_module_loaded: isolation.liquidation_parent_module_loaded === true,
      };
    })(),
    collector_isolation_second_batch: (() => {
      const isolation = getCollectorIsolationHealth();
      const deepRole = isolation.roles?.['deep-market'] || {};
      const slowRole = isolation.roles?.['slow-stats'] || {};
      return {
        ready:
          isolation.enabled === true &&
          isolation.second_batch_runtime === 'worker_thread' &&
          isolation.second_batch_roles_alive === true &&
          isolation.second_batch_thread_ids_distinct === true &&
          isolation.second_batch_worker_resource_limits_enabled === true &&
          isolation.projected_internal_bridge_payloads === true &&
          isolation.full_market_rows_not_copied_to_second_batch === true &&
          isolation.second_batch_worker_failure_isolated_from_parent === true &&
          isolation.second_batch_worker_failure_isolated_from_sibling === true &&
          isolation.role_scoped_supervisor_restart === true &&
          deepRole.alive === true &&
          slowRole.alive === true &&
          Number(deepRole.thread_id || 0) > 0 &&
          Number(slowRole.thread_id || 0) > 0 &&
          isolation.contract_focus_pool_parent_scanner_started === false &&
          isolation.contract_flow_parent_scanner_started === false &&
          isolation.contract_deep_parent_scanner_started === false &&
          isolation.slow_stats_parent_modules_loaded === false &&
          deepMarketBridge.isolated_bridge_fresh === true &&
          slowStatsBridge.isolated_bridge_fresh === true &&
          contractDeep.isolated_bridge === true &&
          contractDeep.isolated_bridge_fresh === true &&
          binanceAdvanced.isolated_bridge === true &&
          binanceAdvanced.isolated_bridge_fresh === true &&
          bitgetAdvanced.isolated_bridge === true &&
          bitgetAdvanced.isolated_bridge_fresh === true &&
          gateAdvanced.isolated_bridge === true &&
          gateAdvanced.isolated_bridge_fresh === true &&
          okxAdvanced.isolated_bridge === true &&
          okxAdvanced.isolated_bridge_fresh === true &&
          bybitAdvanced.isolated_bridge === true &&
          bybitAdvanced.isolated_bridge_fresh === true,
        version: isolation.version || null,
        instance_id: isolation.instance_id || null,
        instance_started_at: isolation.instance_started_at || null,
        host_process_memory: isolation.host_process_memory || null,
        runtime: isolation.second_batch_runtime || null,
        host_pid: isolation.second_batch_host_pid || null,
        deep_market_thread_id: deepRole.thread_id || null,
        slow_stats_thread_id: slowRole.thread_id || null,
        roles_alive: isolation.second_batch_roles_alive === true,
        thread_ids_distinct: isolation.second_batch_thread_ids_distinct === true,
        worker_resource_limits_enabled: isolation.second_batch_worker_resource_limits_enabled === true,
        worker_failure_isolated_from_parent: isolation.second_batch_worker_failure_isolated_from_parent === true,
        worker_failure_isolated_from_sibling: isolation.second_batch_worker_failure_isolated_from_sibling === true,
        role_scoped_supervisor_restart: isolation.role_scoped_supervisor_restart === true,
        deep_market_max_old_generation_size_mb: Number(deepRole.max_old_generation_size_mb || 0),
        slow_stats_max_old_generation_size_mb: Number(slowRole.max_old_generation_size_mb || 0),
        parent_focus_scanner_started: isolation.contract_focus_pool_parent_scanner_started === true,
        parent_flow_scanner_started: isolation.contract_flow_parent_scanner_started === true,
        parent_deep_scanner_started: isolation.contract_deep_parent_scanner_started === true,
        parent_slow_stats_modules_loaded: isolation.slow_stats_parent_modules_loaded === true,
        deep_market_bridge_fresh: deepMarketBridge.isolated_bridge_fresh === true,
        slow_stats_bridge_fresh: slowStatsBridge.isolated_bridge_fresh === true,
        deep_market_owns_focus_pool: isolation.deep_market_owns_focus_pool === true,
        deep_market_owns_contract_flow: isolation.deep_market_owns_contract_flow === true,
        deep_market_owns_deep_shared: isolation.deep_market_owns_deep_shared === true,
        slow_stats_owns_advanced_modules: isolation.slow_stats_owns_advanced_modules === true,
        deep_ready: contractDeep.ready === true,
        deep_rows: Number(contractDeep.row_count || 0),
        binance_ready: binanceAdvanced.ready === true,
        bitget_ready: bitgetAdvanced.ready === true,
        gate_ready: gateAdvanced.ready === true,
        okx_ready: okxAdvanced.ready === true,
        bybit_ready: bybitAdvanced.ready === true,
        scoped_bridge_payloads: true,
        projected_bridge_payloads: isolation.projected_internal_bridge_payloads === true,
        full_market_rows_not_copied_to_second_batch: isolation.full_market_rows_not_copied_to_second_batch === true,
        user_reads_trigger_collector: false,
        reads_scale_with_users: false,
      };
    })(),
    step996_derivatives_public: {
      ready: derivativesPublic.ready === true &&
        derivativesPublic.providers?.okx?.ready === true &&
        derivativesPublic.providers?.bybit?.ready === true &&
        derivativesPublic.providers?.gate?.ready === true &&
        Number(derivativesPublic.providers?.okx?.row_count || 0) > 0 &&
        Number(derivativesPublic.providers?.bybit?.row_count || 0) > 0 &&
        Number(derivativesPublic.providers?.gate?.row_count || 0) > 0 &&
        derivativesPublic.user_reads_trigger_exchange_requests === false &&
        derivativesPublic.reads_scale_with_users === false &&
        derivativesPublic.binance_direct_rest_added === false &&
        derivativesPublic.binance_option_ws_added === false &&
        derivativesPublic.bitget_stockplus_options_excluded_from_crypto_scope === true &&
        derivativesPublic.official_only === true &&
        derivativesPublic.derived_metrics_fabricated === false &&
        derivativesPublic.cross_provider_substitution === false &&
        derivativesPublic.cross_quote_substitution === false &&
        derivativesPublic.missing_stays_null === true &&
        derivativesPublic.current_scope_not_truncated === true,
      version: derivativesPublic.version || null,
      snapshot_endpoint: derivativesPublic.snapshot_endpoint || '/api/derivatives-public/current-snapshot',
      health_endpoint: derivativesPublic.health_endpoint || '/api/derivatives-public/health',
      collector_role: derivativesPublic.collector_role || 'slow-stats',
      supported_crypto_option_providers: Array.isArray(derivativesPublic.supported_crypto_option_providers) ? [...derivativesPublic.supported_crypto_option_providers] : [],
      explicit_non_collected_or_unsupported: Array.isArray(derivativesPublic.explicit_non_collected_or_unsupported) ? [...derivativesPublic.explicit_non_collected_or_unsupported] : [],
      provider_rows: {
        okx: Number(derivativesPublic.providers?.okx?.row_count || 0),
        bybit: Number(derivativesPublic.providers?.bybit?.row_count || 0),
        gate: Number(derivativesPublic.providers?.gate?.row_count || 0),
      },
      provider_ready: {
        binance: derivativesPublic.providers?.binance?.ready === true,
        okx: derivativesPublic.providers?.okx?.ready === true,
        bybit: derivativesPublic.providers?.bybit?.ready === true,
        gate: derivativesPublic.providers?.gate?.ready === true,
        bitget: derivativesPublic.providers?.bitget?.ready === true,
      },
      binance_official_available_but_safety_excluded: derivativesPublic.providers?.binance?.official_available === true && derivativesPublic.binance_direct_rest_added === false && derivativesPublic.binance_option_ws_added === false,
      bitget_crypto_options_unavailable_policy: derivativesPublic.providers?.bitget?.official_available === false && derivativesPublic.bitget_stockplus_options_excluded_from_crypto_scope === true,
      user_reads_trigger_exchange_requests: derivativesPublic.user_reads_trigger_exchange_requests === true,
      reads_scale_with_users: derivativesPublic.reads_scale_with_users === true,
      official_only: derivativesPublic.official_only === true,
      derived_metrics_fabricated: derivativesPublic.derived_metrics_fabricated === true,
      cross_provider_substitution: derivativesPublic.cross_provider_substitution === true,
      cross_quote_substitution: derivativesPublic.cross_quote_substitution === true,
      missing_stays_null: derivativesPublic.missing_stays_null === true,
      current_scope_not_truncated: derivativesPublic.current_scope_not_truncated === true,
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
    step998_liquidation_heatmap: {
      ready: liquidationHistory.step998_liquidation_heatmap_ready === true &&
        liquidationHistory.heatmap_endpoint === '/api/contract-liquidation/heatmap' &&
        liquidationHistory.step998_heatmap_semantics === 'realized_liquidation_events_only_not_estimated_risk' &&
        Number(liquidationHistory.step998_heatmap_bucket_bps || 0) === 25 &&
        liquidationHistory.step998_heatmap_quote_scope === 'USDT_perpetual_only' &&
        liquidationHistory.step998_heatmap_gate_official_minute_reconcile === true &&
        liquidationHistory.step998_heatmap_bitget_official_minute_reconcile === true &&
        liquidationHistory.step998_heatmap_missing_presession_distribution_fabricated === false &&
        liquidationHistory.step998_heatmap_cross_provider_substitution === false &&
        liquidationHistory.step998_heatmap_cross_quote_substitution === false &&
        liquidationHistory.step998_heatmap_user_reads_trigger_requests === false &&
        liquidationHistory.step998_heatmap_reads_scale_with_users === false &&
        Number(liquidationHistory.step998_heatmap_exchange_requests_started_by_reads || 0) === 0,
      endpoint: liquidationHistory.heatmap_endpoint || null,
      semantics: liquidationHistory.step998_heatmap_semantics || null,
      bucket_bps: Number(liquidationHistory.step998_heatmap_bucket_bps || 0),
      bucket_mode: liquidationHistory.step998_heatmap_bucket_mode || null,
      periods: Array.isArray(liquidationHistory.step998_heatmap_periods) ? [...liquidationHistory.step998_heatmap_periods] : [],
      retention_hours: Number(liquidationHistory.step998_heatmap_retention_hours || 0),
      quote_scope: liquidationHistory.step998_heatmap_quote_scope || null,
      process_memory_only: liquidationHistory.step998_heatmap_process_memory_only === true,
      gate_official_minute_reconcile: liquidationHistory.step998_heatmap_gate_official_minute_reconcile === true,
      bitget_official_minute_reconcile: liquidationHistory.step998_heatmap_bitget_official_minute_reconcile === true,
      missing_presession_distribution_fabricated: liquidationHistory.step998_heatmap_missing_presession_distribution_fabricated === true,
      user_reads_trigger_requests: liquidationHistory.step998_heatmap_user_reads_trigger_requests === true,
      reads_scale_with_users: liquidationHistory.step998_heatmap_reads_scale_with_users === true,
      exchange_requests_started_by_reads: Number(liquidationHistory.step998_heatmap_exchange_requests_started_by_reads || 0),
      reads: Number(liquidationHistory.step998_heatmap_reads || 0),
      runtime: { ...(liquidationHistory.step998_heatmap_health || {}) },
    },
    step999_focus_hot_score: {
      ready: contractFocus.ready === true &&
        contractFocus.step999_composite_hot_score_ready === true &&
        contractFocus.hot_rank_metric === 'composite_6_factor_same_venue_shared' &&
        contractFocus.step999_no_additional_exchange_requests === true &&
        contractFocus.step999_no_cross_provider_or_quote_substitution === true &&
        contractFocus.step999_realized_liquidation_not_estimated_risk === true &&
        Number(contractFocus.row_count || 0) === 75,
      version: contractFocus.version || null,
      row_count: Number(contractFocus.row_count || 0),
      hot_rank_metric: contractFocus.hot_rank_metric || null,
      hot_score_min_factors: Number(contractFocus.step999_hot_score_min_factors || 0),
      hot_score_weights: { ...(contractFocus.step999_hot_score_weights || {}) },
      hot_score_sources: Array.isArray(contractFocus.step999_hot_score_sources) ? [...contractFocus.step999_hot_score_sources] : [],
      flow_metric_health: { ...(contractFocus.step999_flow_metric_health || {}) },
      liquidation_local_cache: { ...(contractFocus.step999_liquidation_local_cache || {}) },
      no_additional_exchange_requests: contractFocus.step999_no_additional_exchange_requests === true,
      no_cross_provider_or_quote_substitution: contractFocus.step999_no_cross_provider_or_quote_substitution === true,
      realized_liquidation_not_estimated_risk: contractFocus.step999_realized_liquidation_not_estimated_risk === true,
      reads_scale_with_users: false,
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
    ready: payload.business_rules_ready === true &&
      payload.runtime_market_light_11_exact &&
      payload.binance_spot_step1001_7.ready &&
      payload.step990_light_gap_status.binance_contract_all_market_bbo_ready &&
      payload.trading_status_full_market_ready &&
      payload.v46_closure.ready &&
      payload.binance_step993.ready &&
      payload.okx_step994.ready &&
      payload.okx_step1002.ready &&
      payload.bybit_step995.ready &&
      payload.coinbase_step995.ready &&
      payload.bitget_step991.ready &&
      payload.bitget_step1000.ready &&
      payload.bitget_step1001.ready &&
      payload.bitget_step1004.ready &&
      payload.gate_step992.ready &&
      payload.gate_step1003.ready &&
      payload.collector_isolation_first_batch.ready &&
      payload.collector_isolation_second_batch.ready &&
      payload.step997_liquidation_history.ready &&
      payload.step998_liquidation_heatmap.ready &&
      payload.step999_focus_hot_score.ready,
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
