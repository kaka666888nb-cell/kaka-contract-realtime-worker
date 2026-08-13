const COMMON_META_KEYS = Object.freeze([
  'ok', 'version', 'provider', 'market_type', 'primary_quote',
  'directory_count', 'row_count', 'shared_round', 'round',
  'updated_at', 'cached_at', 'last_error', 'stale',
  'source', 'source_mode',
]);

const DEEP_CONTRACT_KEYS = Object.freeze([
  'symbol', 'base_asset', 'quote_asset', 'quote_symbol',
  'quote_volume_24h', 'price_change_percent_24h',
  'open_interest', 'open_interest_value', 'open_interest_unit', 'open_interest_value_unit',
  'last_price', 'price', 'source_time', 'cached_at', 'updated_at',
]);

const PARENT_SPOT_KEYS = Object.freeze([
  'symbol', 'base_asset', 'quote_asset', 'quote_symbol',
  'last_price', 'price', 'source_time', 'cached_at', 'updated_at',
]);

const PARENT_CONTRACT_KEYS = Object.freeze([
  'symbol', 'base_asset', 'quote_asset', 'quote_symbol',
  'quote_volume_24h', 'price_change_percent_24h',
  'last_price', 'price', 'mark_price', 'index_price',
  'source_time', 'cached_at', 'updated_at',
]);

const SLOW_BITGET_SPOT_KEYS = Object.freeze(['symbol']);

const SLOW_BITGET_CONTRACT_KEYS = Object.freeze([
  'symbol',
  'funding_rate', 'funding_interval_hours',
  'next_funding_time', 'next_funding_time_ms',
  'min_funding_rate', 'max_funding_rate',
  'cash_dividend', 'cash_dividend_next_update',
  'funding_rate_source', 'source',
  'source_time', 'cached_at', 'updated_at',
]);

const SLOW_OI_CONTRACT_KEYS = Object.freeze([
  'symbol',
  'open_interest', 'open_interest_value',
  'open_interest_unit', 'open_interest_value_unit',
  'open_interest_source', 'source',
  'source_time', 'cached_at', 'updated_at',
]);

function pick(source, keys) {
  const out = {};
  for (const key of keys) {
    if (source?.[key] !== undefined) out[key] = source[key];
  }
  return out;
}

export function scopeTargets(scope = 'parent') {
  if (scope === 'deep-market') {
    return [
      ['contract', 'binance'], ['contract', 'okx'], ['contract', 'bybit'],
      ['contract', 'bitget'], ['contract', 'gate'],
    ];
  }
  if (scope === 'slow-stats') {
    return [
      ['spot', 'bitget'],
      ['contract', 'bitget'], ['contract', 'okx'], ['contract', 'bybit'],
    ];
  }
  // Parent only needs the five venues used by focus/basis. Coinbase health is
  // still present in the market-light health object, but no parent module reads
  // a Coinbase internal row snapshot.
  return [
    ['spot', 'binance'], ['spot', 'okx'], ['spot', 'bybit'],
    ['spot', 'bitget'], ['spot', 'gate'],
    ['contract', 'binance'], ['contract', 'okx'], ['contract', 'bybit'],
    ['contract', 'bitget'], ['contract', 'gate'],
  ];
}

export function projectMarketLightSnapshot(snapshot, { scope = 'parent', market = '', provider = '' } = {}) {
  const meta = pick(snapshot || {}, COMMON_META_KEYS);
  const rows = Array.isArray(snapshot?.rows) ? snapshot.rows : [];

  let keys = [];
  if (scope === 'deep-market') {
    keys = DEEP_CONTRACT_KEYS;
  } else if (scope === 'slow-stats') {
    if (market === 'spot' && provider === 'bitget') {
      keys = SLOW_BITGET_SPOT_KEYS;
    } else if (market === 'contract' && provider === 'bitget') {
      keys = SLOW_BITGET_CONTRACT_KEYS;
    } else {
      keys = SLOW_OI_CONTRACT_KEYS;
    }
  } else {
    keys = market === 'spot' ? PARENT_SPOT_KEYS : PARENT_CONTRACT_KEYS;
  }

  const projectedRows = rows.map((row) => pick(row, keys));
  return {
    ...meta,
    rows: projectedRows,
    bridge_projection: true,
    bridge_projection_scope: scope,
    bridge_projection_market: market,
    bridge_projection_provider: provider,
    bridge_projection_row_fields: keys,
    bridge_projection_row_field_count: keys.length,
    bridge_projection_row_count: projectedRows.length,
  };
}

export const __marketLightBridgeProjectionTest = Object.freeze({
  scopeTargets,
  projectMarketLightSnapshot,
});
