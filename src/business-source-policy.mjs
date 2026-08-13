export const BUSINESS_SOURCE_POLICY_VERSION = '650.8.15.128';

const RULES = [];
function add(provider, market, field, {
  resolution,
  capability = null,
  scope = 'current_verified',
  limitation = '',
  inputs = [],
  value_semantics = '',
  layers = [],
}) {
  RULES.push(Object.freeze({
    provider,
    market,
    field,
    resolution,
    source_capability: capability,
    scope,
    limitation,
    source_inputs: Object.freeze([...inputs]),
    value_semantics,
    allowed_layers: Object.freeze([...layers]),
    official: String(resolution).startsWith('official'),
    derived: String(resolution).startsWith('derived'),
    available: resolution !== 'unavailable',
    missing_policy: 'null',
    cross_provider_substitution: false,
    cross_quote_substitution: false,
  }));
}

const spotCaps = {
  binance: { directory:'directory', ticker:'ticker_24h', bbo:'bbo', quote:'USDT' },
  okx: { directory:'ticker_bbo_24h', ticker:'ticker_bbo_24h', bbo:'ticker_bbo_24h', quote:'USDT' },
  bybit: { directory:'ticker_bbo_24h', ticker:'ticker_bbo_24h', bbo:'ticker_bbo_24h', quote:'USDT' },
  bitget: { directory:'ticker_bbo_24h', ticker:'ticker_bbo_24h', bbo:'ticker_bbo_24h', quote:'USDT' },
  gate: { directory:'ticker_bbo_24h', ticker:'ticker_bbo_24h', bbo:'ticker_bbo_24h', quote:'USDT' },
  coinbase: { directory:'ticker_24h', ticker:'ticker_24h', bbo:'bbo', quote:'USD' },
};

for (const [provider, cap] of Object.entries(spotCaps)) {
  add(provider,'spot','directory',{
    resolution:'official_shared_limited',
    capability:cap.directory,
    scope:`current_primary_quote_${cap.quote}_directory`,
    layers:['market_light'],
    limitation:'Current shared primary-quote directory; not presented as the venue official total market count.',
  });
  add(provider,'spot','ticker_24h',{
    resolution:'official_shared',
    capability:cap.ticker,
    scope:`current_primary_quote_${cap.quote}_full_directory`,
    layers:['market_light'],
  });
  add(provider,'spot','bbo',{
    resolution:'official_shared',
    capability:cap.bbo,
    scope:`current_primary_quote_${cap.quote}_full_directory`,
    layers:['market_light'],
  });
}

const contractTickerCap = {
  binance:'ticker_24h',
  okx:'ticker_bbo_24h',
  bybit:'linear_ticker_mark_index_oi_funding_bbo',
  bitget:'ticker_mark_index_oi_funding_bbo',
  gate:'ticker_mark_index_funding_bbo',
};
const fundingCap = {
  binance:'mark_index_funding',
  okx:'funding_rate',
  bybit:'linear_ticker_mark_index_oi_funding_bbo',
  bitget:'ticker_mark_index_oi_funding_bbo',
  gate:'ticker_mark_index_funding_bbo',
};
const markIndexCap = {
  binance:'mark_index_funding',
  okx:'mark_index_open_interest',
  bybit:'linear_ticker_mark_index_oi_funding_bbo',
  bitget:'ticker_mark_index_oi_funding_bbo',
  gate:'ticker_mark_index_funding_bbo',
};
const oiCap = {
  binance:'open_interest_current',
  okx:'mark_index_open_interest',
  bybit:'linear_ticker_mark_index_oi_funding_bbo',
  bitget:'ticker_mark_index_oi_funding_bbo',
  gate:'open_interest',
};
const ratioCaps = {
  binance:{global:'long_short_and_taker_stats',top_account:'long_short_and_taker_stats',top_position:'long_short_and_taker_stats'},
  okx:{global:'global_long_short_ratio',top_account:null,top_position:null},
  bybit:{global:'global_long_short_ratio',top_account:null,top_position:null},
  bitget:{global:'long_short_ratio_families',top_account:'long_short_ratio_families',top_position:'long_short_ratio_families'},
  gate:{global:'contract_stats_top_trader_taker_account',top_account:'contract_stats_top_trader_taker_account',top_position:'contract_stats_top_trader_taker_account'},
};

for (const provider of ['binance','okx','bybit','bitget','gate']) {
  add(provider,'contract','directory',{
    resolution:'official_shared_limited',
    capability:'directory',
    scope:'current_USDT_perpetual_full_directory',
    layers:['market_light'],
    limitation:'Current shared USDT perpetual universe; not presented as an official venue-total count.',
  });
  add(provider,'contract','ticker_24h',{
    resolution:'official_shared',
    capability:contractTickerCap[provider],
    scope:'current_USDT_perpetual_full_directory',
    layers:['market_light'],
  });
  add(provider,'contract','bbo',{
    resolution:'official_shared',
    capability:'bbo',
    scope:'current_USDT_perpetual_full_directory',
    layers:['market_light'],
  });
  add(provider,'contract','funding_rate',{
    resolution:'official_shared',
    capability:fundingCap[provider],
    scope:'current_USDT_perpetual',
    layers:provider === 'okx'
      ? ['shared_current']
      : ['market_light','shared_current'],
  });
  add(provider,'contract','mark_price',{
    resolution:'official_shared',
    capability:markIndexCap[provider],
    scope:'current_USDT_perpetual',
    layers:['market_light','shared_current'],
  });
  add(provider,'contract','index_price',{
    resolution:'official_shared',
    capability:markIndexCap[provider],
    scope:'current_USDT_perpetual',
    layers:['market_light','shared_current'],
  });
  add(provider,'contract','open_interest',{
    resolution:'official_shared',
    capability:oiCap[provider],
    scope:provider === 'binance' ? 'focus15_current_protected_relay' : 'shared_current_rotation_and_focus',
    layers:(provider === 'binance' || provider === 'gate')
      ? ['shared_current']
      : ['market_light','shared_current'],
    limitation:provider === 'binance'
      ? 'Render direct Binance contract REST remains disabled; official OI comes through the protected shared relay.'
      : '',
  });

  for (const family of ['global','top_account','top_position']) {
    const cap = ratioCaps[provider][family];
    add(provider,'contract',`ratio_${family}`,{
      resolution:cap ? 'official_shared' : 'unavailable',
      capability:cap,
      scope:cap ? (provider === 'bybit' ? 'USDT_linear_only' : 'shared_current_focus_or_rotation') : 'none',
      layers:cap ? ['shared_current'] : [],
      limitation:provider === 'bybit' && family === 'global'
        ? 'Bybit account ratio is used only for the officially supported USDT linear scope; no USDC/USD substitution.'
        : cap ? '' : 'No matching official public ratio family is registered; do not synthesize it from another family.',
    });
  }

  add(provider,'contract','depth20',{
    resolution:'official_shared_limited',
    capability:'depth20_current_focus',
    scope:'focus15_current_depth',
    layers:['deep_shared'],
    limitation:'20-level book is current focus15 depth, not simultaneous full-directory depth.',
  });
  add(provider,'contract','liquidation_current',{
    resolution:'official_shared',
    capability:'liquidation_current',
    scope:'current_USDT_perpetual_public_events',
    layers:['liquidation'],
  });
  add(provider,'contract','liquidation_heatmap_actual',{
    resolution:'derived_same_venue',
    capability:'liquidation_realized_price_heatmap',
    inputs:['liquidation_current'],
    scope:'current_USDT_perpetual_realized_events_up_to_24h_observed_session',
    layers:['liquidation_heatmap'],
    limitation:'Actual realized liquidation events are aggregated into deterministic 25bps price bins. This is not an estimated liquidation-risk distribution; missing pre-session price distribution stays missing.',
    value_semantics:'sum same-provider same-symbol same-USDT-quote realized liquidation event notional/count into price buckets; cross-provider view merges only the exact same symbol and quote',
  });
  add(provider,'contract','basis_mark_index',{
    resolution:'derived_same_venue',
    inputs:['mark_price','index_price'],
    scope:'same_provider_same_symbol_same_quote_current',
    layers:['app_derived'],
    limitation:'Derived only from two official current prices for the exact venue/pair; never labeled as an official venue basis field.',
    value_semantics:'(mark-index)/index',
  });
  add(provider,'contract','basis_perp_spot',{
    resolution:'derived_same_venue',
    inputs:['mark_price','spot:ticker_24h'],
    scope:'same_provider_same_base_same_quote_current',
    layers:['app_derived'],
    limitation:'Derived only when same-venue perpetual mark and spot price with the same quote are both verifiable.',
    value_semantics:'(perpetual_mark-spot)/spot',
  });
  add(provider,'contract','taker_quote_flow_5m',{
    resolution:provider === 'gate' ? 'official_normalized' : 'derived_same_venue',
    capability:provider === 'gate' ? 'contract_stats_top_trader_taker_account' : null,
    inputs:provider === 'gate' ? [] : ['public_trade_stream'],
    scope:'focus15_5m',
    layers:['deep_shared'],
    limitation:provider === 'gate'
      ? 'Gate contract_stats taker size is normalized to quote notional using verified contract sizing; if the official aggregate is absent this business field stays missing.'
      : 'Quote taker flow is derived from the same venue public trades and is not labeled as an official long/short statistic.',
  });
  add(provider,'contract','large_trade_p95',{
    resolution:'derived_same_venue',
    inputs:['public_trade_stream'],
    scope:'focus15_5m',
    layers:['deep_shared'],
    limitation:'P95 is calculated from the exact pair five-minute public-trade notional distribution; it is not an official exchange large-order field.',
  });
}

add('binance','contract','risk_adl',{resolution:'official_shared',capability:'adl_risk',scope:'focus15_current',layers:['slow_stats']});
add('binance','contract','risk_limit',{resolution:'unavailable',scope:'none',layers:['slow_stats']});
add('binance','contract','insurance_fund',{resolution:'unavailable',scope:'none',layers:['slow_stats']});

add('okx','contract','risk_adl',{resolution:'official_shared',capability:'price_limit_security_fund_adl',scope:'official_public_available_scope',layers:['slow_stats']});
add('okx','contract','risk_limit',{resolution:'official_shared_limited',capability:'price_limit_security_fund_adl',scope:'official_public_available_scope',layers:['slow_stats']});
add('okx','contract','insurance_fund',{resolution:'official_shared',capability:'price_limit_security_fund_adl',scope:'official_public_available_scope',layers:['slow_stats']});

add('bybit','contract','risk_adl',{resolution:'unavailable',scope:'none',layers:['slow_stats']});
add('bybit','contract','risk_limit',{resolution:'official_shared',capability:'risk_limit_insurance_pool',scope:'official_public_available_scope',layers:['slow_stats']});
add('bybit','contract','insurance_fund',{resolution:'official_shared',capability:'risk_limit_insurance_pool',scope:'official_public_available_scope',layers:['slow_stats']});

add('bitget','contract','risk_adl',{resolution:'unavailable',scope:'none',layers:['slow_stats']});
add('bitget','contract','risk_limit',{resolution:'official_shared',capability:'risk_reserve_position_tier_oi_limit_index_components',scope:'official_public_available_scope',layers:['slow_stats']});
add('bitget','contract','insurance_fund',{resolution:'official_shared',capability:'risk_reserve_position_tier_oi_limit_index_components',scope:'risk_reserve_shared',layers:['slow_stats']});

add('gate','contract','risk_adl',{resolution:'unavailable',scope:'none',layers:['slow_stats']});
add('gate','contract','risk_limit',{resolution:'official_shared',capability:'risk_limit_tiers',scope:'official_public_available_scope',layers:['slow_stats']});
add('gate','contract','insurance_fund',{resolution:'official_shared',capability:'insurance_fund',scope:'official_public_available_scope',layers:['slow_stats']});

for (const field of ['whale_flow','fund_flow','net_capital_24h']) {
  add('bitget','spot',field,{resolution:'official_shared',capability:'whale_fund_net_capital_flow',scope:'official_spot_advanced_current',layers:['slow_stats']});
}
add('bitget','spot','whale_net_timestamped_history',{
  resolution:'official_shared',
  capability:'whale_net_timestamped_history',
  scope:'native_official_timestamped_history',
  layers:['slow_stats'],
  limitation:'Source-native timestamps only; no synthetic interval rollup.',
});


// Step1004.5 closes the original Step996 gap without weakening the established
// Binance network safety boundary or inventing a Bitget crypto-options family.
for (const provider of ['binance','okx','bybit','bitget','gate']) {
  const shared = ['okx','bybit','gate'].includes(provider);
  const unavailableReason = provider === 'binance'
    ? 'Official Binance Options public market data exists, but Kaka Render deliberately adds no direct Binance options REST or /market WebSocket path in Step1004.5; values stay missing until that safety boundary is separately redesigned and verified.'
    : provider === 'bitget'
      ? 'The current Bitget public crypto UTA product categories do not expose a crypto-options category. Stock+ U.S. stock options are outside this crypto-derivatives scope.'
      : '';
  add(provider,'option','directory',{
    resolution: shared ? 'official_shared' : 'unavailable',
    capability:'option_public_market',
    scope: shared ? 'current_public_crypto_options' : 'none_current_Kaka_integration',
    layers: shared ? ['derivatives_public'] : [],
    limitation: unavailableReason,
  });
  add(provider,'option','ticker',{
    resolution: shared ? 'official_shared' : 'unavailable',
    capability:'option_public_market',
    scope: shared ? 'current_public_crypto_options' : 'none_current_Kaka_integration',
    layers: shared ? ['derivatives_public'] : [],
    limitation: unavailableReason,
  });
  add(provider,'option','open_interest',{
    resolution: shared ? 'official_shared' : 'unavailable',
    capability:'option_public_market',
    scope: shared ? 'current_public_crypto_options_where_officially_returned' : 'none_current_Kaka_integration',
    layers: shared ? ['derivatives_public'] : [],
    limitation: unavailableReason || 'If an official option row does not return open interest, the field stays null.',
  });
  const greeksShared = provider === 'bybit' || provider === 'gate';
  add(provider,'option','iv_greeks',{
    resolution: greeksShared ? 'official_shared' : 'unavailable',
    capability:'option_public_market',
    scope: greeksShared ? 'current_public_crypto_options_where_officially_returned' : 'none_current_shared_field',
    layers: greeksShared ? ['derivatives_public'] : [],
    limitation: greeksShared
      ? 'Only source-native public option IV/Greeks are exposed; absent individual Greeks remain null.'
      : provider === 'okx'
        ? 'The Step1004.5 OKX public layer consumes instruments, tickers and official option-family open interest only; IV/Greeks are not fabricated from price inputs.'
        : unavailableReason,
  });
}

export const BUSINESS_SOURCE_RULES = Object.freeze(RULES);
const RULE_BY_KEY = new Map(
  BUSINESS_SOURCE_RULES.map((rule) => [`${rule.provider}|${rule.market}|${rule.field}`, rule]),
);

export function getBusinessSourceRule(provider, market, field) {
  return RULE_BY_KEY.get(
    `${String(provider || '').trim().toLowerCase()}|${String(market || '').trim().toLowerCase()}|${String(field || '').trim().toLowerCase()}`
  ) || null;
}

export function validateBusinessSourceRules(capabilities = []) {
  const capKeys = new Set(
    (Array.isArray(capabilities) ? capabilities : []).map(
      (item) => `${item.provider}|${item.market}|${item.capability}`,
    ),
  );
  const missingCapabilityRefs = [];
  const duplicateKeys = [];
  const seen = new Set();
  for (const rule of BUSINESS_SOURCE_RULES) {
    const key = `${rule.provider}|${rule.market}|${rule.field}`;
    if (seen.has(key)) duplicateKeys.push(key);
    seen.add(key);
    if (rule.source_capability) {
      const capKey = `${rule.provider}|${rule.market}|${rule.source_capability}`;
      if (!capKeys.has(capKey)) missingCapabilityRefs.push(`${key}->${capKey}`);
    }
  }
  return {
    ready: missingCapabilityRefs.length === 0 && duplicateKeys.length === 0,
    rule_count: BUSINESS_SOURCE_RULES.length,
    unique_rule_count: seen.size,
    missing_capability_refs: missingCapabilityRefs,
    duplicate_rule_keys: duplicateKeys,
  };
}
