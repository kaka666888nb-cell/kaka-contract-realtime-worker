import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { __bybitAdvancedTest } from '../src/bybit-advanced-stats.mjs';

const parsed = __bybitAdvancedTest.parseOrderPriceLimit({
  retCode: 0,
  time: 1_754_000_000_000,
  result: {
    symbol: 'BTCUSDT',
    buyLmt: '105878.10',
    sellLmt: '103781.60',
    ts: '1754000000123',
  },
}, 'BTCUSDT');

assert.equal(parsed.symbol, 'BTCUSDT');
assert.equal(parsed.highest_bid_price, 105878.10);
assert.equal(parsed.lowest_ask_price, 103781.60);
assert.equal(parsed.timestamp_ms, 1_754_000_000_123);
assert.equal(parsed.official_response, true);
assert.equal(parsed.official_empty, false);
assert.equal(parsed.source, 'bybit_official_public_order_price_limit');
assert.equal(parsed.endpoint, '/v5/market/price-limit');

const noCrossSymbol = __bybitAdvancedTest.parseOrderPriceLimit({
  retCode: 0,
  result: { symbol: 'ETHUSDT', buyLmt: '5000', sellLmt: '3000' },
}, 'BTCUSDT');
assert.equal(noCrossSymbol.highest_bid_price, null);
assert.equal(noCrossSymbol.lowest_ask_price, null);
assert.equal(noCrossSymbol.official_empty, true);

const source = await readFile(new URL('../src/bybit-advanced-stats.mjs', import.meta.url), 'utf8');
assert.match(source, /exchange_requests_started_by_user_read:\s*0/);
assert.match(source, /order_price_limit_user_reads_trigger_exchange_requests:\s*false/);
assert.match(source, /order_price_limit_reads_scale_with_users:\s*false/);
assert.match(source, /PRICE_LIMIT_REFRESH_MS[\s\S]*15_000/);
assert.match(source, /FOCUS_TARGET\s*=\s*15/);

console.log('Step1023 self-test passed');
