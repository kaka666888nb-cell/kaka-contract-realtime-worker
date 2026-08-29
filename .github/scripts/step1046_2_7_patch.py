from pathlib import Path

p = Path('src/onchain-market.mjs')
s = p.read_text(encoding='utf-8')
marker = 'historical_empty_page_pool_creation_boundary_returns_200'
if marker not in s:
    old = """async function buildKlinesWithExactPoolFallback(network, tokenAddress, pool, interval, limit, endTimeMs) {
  let moralisError = '';
  try {
    const primary = await buildMoralisKlines(network, tokenAddress, pool, interval, limit, endTimeMs);
    if (Array.isArray(primary?.rows) && primary.rows.length > 0) {
      return { ...primary, source: 'moralis_official_data_api_pair_ohlcv', fallback_used: false };
    }
    moralisError = 'moralis_exact_pool_ohlcv_empty';
  } catch (error) {
    moralisError = text(error?.message || error).slice(0, 240);
  }
  try {
    return { ...(await buildGeckoKlines(network, tokenAddress, pool, interval, limit, endTimeMs)), fallback_used: true, primary_error: moralisError };
  } catch (fallbackError) {
    const error = new Error(`exact_pool_kline_unavailable:primary=${moralisError};fallback=${text(fallbackError?.message || fallbackError).slice(0, 220)}`);
    error.statusCode = 503;
    throw error;
  }
}
"""
    new = """function historicalRangeReachesPoolCreation(pool, interval, limit, endTimeMs) {
  const end = Number(endTimeMs);
  const createdMs = Date.parse(text(pool?.pool_created_at));
  if (!Number.isFinite(end) || end <= 0 || !Number.isFinite(createdMs) || createdMs <= 0) return false;
  const range = klineRange(interval, limit, end);
  return Number.isFinite(range?.fromMs) && range.fromMs <= createdMs;
}
async function buildKlinesWithExactPoolFallback(network, tokenAddress, pool, interval, limit, endTimeMs) {
  let moralisError = '';
  try {
    const primary = await buildMoralisKlines(network, tokenAddress, pool, interval, limit, endTimeMs);
    if (Array.isArray(primary?.rows) && primary.rows.length > 0) {
      return { ...primary, source: 'moralis_official_data_api_pair_ohlcv', fallback_used: false, history_exhausted: false };
    }
    const exactProof = text(primary?.identity_proof);
    const exactPair = exactAddressEqual(network, primary?.source_pair_address, pool?.pool_address);
    if (endTimeMs && exactProof && exactPair && historicalRangeReachesPoolCreation(pool, interval, limit, endTimeMs)) {
      return {
        ...primary,
        source: 'moralis_official_data_api_pair_ohlcv',
        fallback_used: false,
        history_exhausted: true,
        history_exhausted_reason: 'exact_pool_primary_empty_and_requested_range_reaches_pool_creation',
      };
    }
    moralisError = 'moralis_exact_pool_ohlcv_empty';
  } catch (error) {
    moralisError = text(error?.message || error).slice(0, 240);
  }
  try {
    return { ...(await buildGeckoKlines(network, tokenAddress, pool, interval, limit, endTimeMs)), fallback_used: true, primary_error: moralisError, history_exhausted: false };
  } catch (fallbackError) {
    const error = new Error(`exact_pool_kline_unavailable:primary=${moralisError};fallback=${text(fallbackError?.message || fallbackError).slice(0, 220)}`);
    error.statusCode = 503;
    throw error;
  }
}
"""
    if s.count(old) != 1:
        raise SystemExit(f'fallback block count={s.count(old)}')
    s = s.replace(old, new, 1)

    old_cache = "if (value?.rows?.length) {"
    if s.count(old_cache) != 2:
        raise SystemExit(f'cache rows-only count={s.count(old_cache)}')
    s = s.replace(old_cache, "if (value?.rows?.length || value?.history_exhausted === true) {", 2)

    old_native = """  if (nativeInterval) {
    const built = await buildKlinesWithExactPoolFallback(network, tokenAddress, pool, interval, limit, endTimeMs);
    const continuity = deriveAndFillKlines(built.rows || [], interval, {
"""
    new_native = """  if (nativeInterval) {
    const built = await buildKlinesWithExactPoolFallback(network, tokenAddress, pool, interval, limit, endTimeMs);
    if (built.history_exhausted === true && (!Array.isArray(built.rows) || built.rows.length === 0)) {
      return {
        ...built,
        rows: [],
        kline_feature_schema_version: KLINE_FEATURE_SCHEMA_VERSION,
        interval_mode: 'native_shared_history_exhausted',
        derived_from_interval: null,
        zero_trade_fill_count: 0,
        zero_trade_fill_policy: 'disabled_history_exhausted',
        zero_trade_fill_tail_extrapolation: false,
      };
    }
    const continuity = deriveAndFillKlines(built.rows || [], interval, {
"""
    if s.count(old_native) != 1:
        raise SystemExit(f'native block count={s.count(old_native)}')
    s = s.replace(old_native, new_native, 1)

    old_derived = """  const base = baseResult.value || { rows: [] };
  if (!Array.isArray(base.rows) || base.rows.length === 0) {
    const error = new Error('onchain_derived_base_kline_empty');
"""
    new_derived = """  const base = baseResult.value || { rows: [] };
  if (base.history_exhausted === true && (!Array.isArray(base.rows) || base.rows.length === 0)) {
    return {
      ...base,
      rows: [],
      kline_feature_schema_version: KLINE_FEATURE_SCHEMA_VERSION,
      interval_mode: 'shared_derived_history_exhausted',
      derived_from_interval: plan.base,
      base_cache_status: baseResult.cache_status,
      zero_trade_fill_count: 0,
      zero_trade_fill_policy: 'disabled_history_exhausted',
      zero_trade_fill_tail_extrapolation: false,
    };
  }
  if (!Array.isArray(base.rows) || base.rows.length === 0) {
    const error = new Error('onchain_derived_base_kline_empty');
"""
    if s.count(old_derived) != 1:
        raise SystemExit(f'derived block count={s.count(old_derived)}')
    s = s.replace(old_derived, new_derived, 1)

    old_health = "      historical_pages_require_identity_proof: true,\n"
    new_health = "      historical_pages_require_identity_proof: true,\n      historical_empty_page_pool_creation_boundary_returns_200: true,\n      historical_exhaustion_requires_exact_identity_and_pool_creation_boundary: true,\n"
    if s.count(old_health) != 1:
        raise SystemExit(f'health marker count={s.count(old_health)}')
    s = s.replace(old_health, new_health, 1)

    old_test = "  t('moralis_15m_same_pool_derivation_only', MORALIS_TIMEFRAME['15m'] === '5min');\n"
    new_test = old_test + "  t('step1046_2_7_history_exhaustion_requires_creation_boundary', historicalRangeReachesPoolCreation({ pool_created_at: '2026-01-01T00:00:00Z' }, '15m', 300, Date.parse('2026-01-02T00:00:00Z')) === true && historicalRangeReachesPoolCreation({ pool_created_at: '2026-01-01T00:00:00Z' }, '15m', 300, Date.parse('2026-01-10T00:00:00Z')) === false);\n"
    if s.count(old_test) != 1:
        raise SystemExit(f'self-test insertion count={s.count(old_test)}')
    s = s.replace(old_test, new_test, 1)

    old_response = "        historical_end_time_ms: endTimeMs,\n"
    new_response = "        historical_end_time_ms: endTimeMs,\n        history_exhausted: built.history_exhausted === true,\n        history_exhausted_reason: built.history_exhausted_reason || null,\n"
    if s.count(old_response) != 1:
        raise SystemExit(f'response insertion count={s.count(old_response)}')
    s = s.replace(old_response, new_response, 1)
    p.write_text(s, encoding='utf-8')

proxy = Path('src/proxy.mjs')
q = proxy.read_text(encoding='utf-8')
q = q.replace("const STEP_VERSION = '650.8.15.197.3.3.26';", "const STEP_VERSION = '650.8.15.197.3.3.27';")
if "const STEP_VERSION = '650.8.15.197.3.3.27';" not in q:
    raise SystemExit('proxy version bump failed')
proxy.write_text(q, encoding='utf-8')

s = p.read_text(encoding='utf-8')
required = [
    'historical_empty_page_pool_creation_boundary_returns_200: true',
    'historical_exhaustion_requires_exact_identity_and_pool_creation_boundary: true',
    'history_exhausted_reason',
    "interval_mode: 'native_shared_history_exhausted'",
    "interval_mode: 'shared_derived_history_exhausted'",
    'value?.history_exhausted === true',
    'step1046_2_7_history_exhaustion_requires_creation_boundary',
]
missing = [x for x in required if x not in s]
if missing:
    raise SystemExit('missing markers: ' + repr(missing))
if s.count('value?.history_exhausted === true') != 2:
    raise SystemExit('exhausted cache count mismatch')
print('PASS patch Step1046.2.7 history exhaustion semantics')
