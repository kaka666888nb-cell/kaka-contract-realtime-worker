import { readFileSync, writeFileSync } from 'node:fs';

const STEP = 'Step1072.9.32.3';
const restUrl = new URL('./market-rest.mjs', import.meta.url);

function countExact(text, needle) {
  if (!needle) return 0;
  let count = 0;
  let offset = 0;
  while (true) {
    const index = text.indexOf(needle, offset);
    if (index < 0) return count;
    count += 1;
    offset = index + needle.length;
  }
}

function replaceExactlyOnce(text, oldText, newText, label) {
  const count = countExact(text, oldText);
  if (count !== 1) {
    throw new Error(`${STEP} ${label} anchor_count_expected_1_actual_${count}`);
  }
  return text.replace(oldText, newText);
}

let rest = readFileSync(restUrl, 'utf8');

const oldNumBlock = [
  'function num(value) {',
  '  const parsed = Number(value);',
  '  return Number.isFinite(parsed) ? parsed : null;',
  '}',
].join('\n');
const newNumBlock = [
  oldNumBlock,
  `// ${STEP}: market prices must be strictly positive. Some providers, notably`,
  '// Gate spot, emit 0 for 24h high/low when there were no trades in the',
  '// rolling window. Preserve truthful zero volume/change, but never expose a',
  '// zero sentinel as a real market price.',
  'function positiveMarketPrice(value) {',
  '  const parsed = num(value);',
  '  return parsed !== null && parsed > 0 ? parsed : null;',
  '}',
].join('\n');

const oldRangeBlock = [
  '    high_24h: num(item.high_24h ?? item.highPrice ?? item.high24h ?? item.highPrice24h),',
  '    low_24h: num(item.low_24h ?? item.lowPrice ?? item.low24h ?? item.lowPrice24h),',
].join('\n');
const newRangeBlock = [
  '    high_24h: positiveMarketPrice(',
  '      item.high_24h ?? item.highPrice ?? item.high24h ?? item.highPrice24h,',
  '    ),',
  '    low_24h: positiveMarketPrice(',
  '      item.low_24h ?? item.lowPrice ?? item.low24h ?? item.lowPrice24h,',
  '    ),',
].join('\n');

const selfTestAnchor = [
  "  const coinbase = tickerVolumeSemantics(",
  "    'coinbase',",
  "    'spot',",
  "    { volume: '2.5', quote_volume_24h: '250' },",
].join('\n');

const selfTestInsert = [
  `  const gateZeroRangeSentinel = tickerRow(`,
  `    'gate',`,
  `    'spot',`,
  `    {`,
  `      last: '1',`,
  `      high_24h: '0',`,
  `      low_24h: '0',`,
  `      change_percentage: '0',`,
  `      base_volume: '0',`,
  `      quote_volume: '0',`,
  `    },`,
  `    'TEST_USDT',`,
  `  );`,
  `  add(`,
  `    'gate_spot_zero_24h_range_is_unavailable_but_zero_volume_is_preserved',`,
  `    gateZeroRangeSentinel?.high_24h === null &&`,
  `      gateZeroRangeSentinel?.low_24h === null &&`,
  `      gateZeroRangeSentinel?.base_volume_24h === 0 &&`,
  `      gateZeroRangeSentinel?.quote_volume_24h === 0 &&`,
  `      gateZeroRangeSentinel?.price_change_percent_24h === 0,`,
  `    gateZeroRangeSentinel,`,
  `  );`,
  ``,
  selfTestAnchor,
].join('\n');

const red = {
  num_anchor: countExact(rest, oldNumBlock),
  range_anchor: countExact(rest, oldRangeBlock),
  self_test_anchor: countExact(rest, selfTestAnchor),
  zero_range_guard_before: countExact(rest, 'function positiveMarketPrice(value) {'),
};
console.log(`${STEP} RED`, JSON.stringify(red));
if (
  red.num_anchor !== 1 ||
  red.range_anchor !== 1 ||
  red.self_test_anchor !== 1 ||
  red.zero_range_guard_before !== 0
) {
  throw new Error(`${STEP} RED baseline mismatch; refusing build-time mutation`);
}

rest = replaceExactlyOnce(rest, oldNumBlock, newNumBlock, 'positive_market_price_helper');
rest = replaceExactlyOnce(rest, oldRangeBlock, newRangeBlock, 'ticker_high_low_price_semantics');
rest = replaceExactlyOnce(rest, selfTestAnchor, selfTestInsert, 'gate_zero_range_self_test');

const green = {
  old_range_anchor_remaining: countExact(rest, oldRangeBlock),
  positive_market_price_helper: countExact(rest, 'function positiveMarketPrice(value) {'),
  high_positive_normalizer: countExact(rest, '    high_24h: positiveMarketPrice('),
  low_positive_normalizer: countExact(rest, '    low_24h: positiveMarketPrice('),
  self_test: countExact(rest, "'gate_spot_zero_24h_range_is_unavailable_but_zero_volume_is_preserved'"),
};
console.log(`${STEP} GREEN`, JSON.stringify(green));
if (
  green.old_range_anchor_remaining !== 0 ||
  green.positive_market_price_helper !== 1 ||
  green.high_positive_normalizer !== 1 ||
  green.low_positive_normalizer !== 1 ||
  green.self_test !== 1
) {
  throw new Error(`${STEP} GREEN invariant failed; refusing image build`);
}

writeFileSync(restUrl, rest, 'utf8');
console.log(`${STEP} BUILD_PATCH_PASS`);
