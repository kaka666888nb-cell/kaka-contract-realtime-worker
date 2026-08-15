import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { __bybitAdvancedTest } from '../src/bybit-advanced-stats.mjs';
import { __testDerivativesPublic } from '../src/derivatives-public.mjs';
import { __contractDepthStep1024Test } from '../src/contract-depth.mjs';
import { BUSINESS_SOURCE_RULES, validateBusinessSourceRules } from '../src/business-source-policy.mjs';

const indexComponents = __bybitAdvancedTest.parseIndexPriceComponents({
  retCode: 0,
  time: 1_755_000_000_000,
  result: {
    indexName: 'BTCUSDT',
    lastPrice: '100123.45',
    updateTime: '1755000000123',
    components: [
      { exchange: 'Binance', spotPair: 'BTCUSDT', equivalentPrice: '100100', multiplier: '1', price: '100100', weight: '0.45' },
      { exchange: 'OKX', spotPair: 'BTC-USDT', equivalentPrice: '100150', multiplier: '1', price: '100150', weight: '0.55' },
    ],
  },
}, 'BTCUSDT');
assert.equal(indexComponents.official_empty, false);
assert.equal(indexComponents.component_count, 2);
assert.equal(indexComponents.components[0].weight, 0.45);
assert.equal(indexComponents.source, 'bybit_official_public_index_price_components');

const wrongIndex = __bybitAdvancedTest.parseIndexPriceComponents({
  result: { indexName: 'ETHUSDT', lastPrice: '5000', components: [{ exchange: 'X', spotPair: 'ETHUSDT' }] },
}, 'BTCUSDT');
assert.equal(wrongIndex.official_empty, true);
assert.equal(wrongIndex.last_price, null);
assert.deepEqual(wrongIndex.components, []);

const hv = __testDerivativesPublic.parseBybitHistoricalVolatility({
  result: [
    { period: '7', value: '0.6123', time: '1755000000000' },
    { period: '7', value: '0.6000', time: '1754996400000' },
  ],
}, 'BTC');
assert.equal(hv.official_empty, false);
assert.equal(hv.latest.value, 0.6123);
assert.equal(hv.rows.length, 2);
assert.equal(hv.provider, 'bybit');
assert.equal(hv.source, 'bybit_official_public_option_historical_volatility');

const legacyHv = __testDerivativesPublic.parseBybitHistoricalVolatility({
  result: { list: [{ period: '30', value: '0.5000', time: '1755000000000' }] },
}, 'ETH');
assert.equal(legacyHv.official_empty, false);
assert.equal(legacyHv.latest.period, 30);

const okxRows = __testDerivativesPublic.parseOkxRows(
  { data: [{ instId: 'BTC-USD-260101-100000-C', uly: 'BTC-USD', baseCcy: 'BTC', quoteCcy: 'USD', optType: 'C', stk: '100000', state: 'live' }] },
  { data: [{ instId: 'BTC-USD-260101-100000-C', last: '1234', bidPx: '1230', askPx: '1240' }] },
  { data: [{ instId: 'BTC-USD-260101-100000-C', oi: '50', oiUsd: '50000' }] },
  { data: [{ instId: 'BTC-USD-260101-100000-C', bidVol: '0.51', askVol: '0.53', markVol: '0.52', delta: '0.4', gamma: '0.0001', vega: '12.5', theta: '-8.1', fwdPx: '100200' }] },
);
assert.equal(okxRows.length, 1);
assert.equal(okxRows[0].mark_iv, 0.52);
assert.equal(okxRows[0].delta, 0.4);
assert.equal(okxRows[0].forward_price, 100200);

assert.equal(__contractDepthStep1024Test.clampLimit('orderbook', 1000, 'bybit', 'spot'), 1000);
assert.equal(__contractDepthStep1024Test.clampLimit('orderbook', 1000, 'bybit', 'contract'), 20);
assert.equal(__contractDepthStep1024Test.clampLimit('orderbook', 1000, 'okx', 'spot'), 20);

const coinbaseFacts = __contractDepthStep1024Test.parseCoinbaseProductFacts({
  product_id: 'BTC-USD',
  status: 'online',
  product_type: 'SPOT',
  is_disabled: false,
  view_only: false,
  cancel_only: false,
  limit_only: true,
  post_only: false,
  trading_disabled: false,
  auction_mode: false,
  fcm_trading_session_details: { is_session_open: true, open_time: '2026-08-15T00:00:00Z', close_time: '2026-08-16T00:00:00Z' },
}, new Set(['BTC-USD']));
assert.equal(coinbaseFacts.product_id, 'BTC-USD');
assert.equal(coinbaseFacts.limit_only, true);
assert.equal(coinbaseFacts.fcm_trading_session_details.is_session_open, true);
assert.equal(coinbaseFacts.future_product_details, null);

const ruleKeys = new Set(BUSINESS_SOURCE_RULES.map((row) => `${row.provider}|${row.market}|${row.field}`));
for (const key of [
  'okx|contract|current_price_limit',
  'okx|option|iv_greeks',
  'bybit|contract|index_price_components',
  'bybit|option|historical_volatility',
  'bybit|spot|ultra_deep_orderbook',
  'coinbase|spot|product_status_restrictions_session',
]) assert.equal(ruleKeys.has(key), true, `missing business rule ${key}`);

const contractDepthSource = await readFile(new URL('../src/contract-depth.mjs', import.meta.url), 'utf8');
assert.match(contractDepthSource, /coinbase_product_facts_user_read_starts_rest_request:\s*false/);
assert.match(contractDepthSource, /bybit_spot_orderbook_official_max_depth:\s*1_000/);
assert.match(contractDepthSource, /coinbase_product_facts_secret_exposed_to_app:\s*false/);

const registrySource = await readFile(new URL('../src/source-capability-registry.mjs', import.meta.url), 'utf8');
const capabilities = [...registrySource.matchAll(/provider:\s*'([^']+)'\s*,\s*market:\s*'([^']+)'\s*,\s*capability:\s*'([^']+)'/g)]
  .map((match) => ({ provider: match[1], market: match[2], capability: match[3] }));
const validation = validateBusinessSourceRules(capabilities);
assert.equal(validation.ready, true, JSON.stringify(validation));
assert.match(registrySource, /step1024_official_capability_batch/);

console.log('Step1024.1 self-test passed');
