import { readFileSync, writeFileSync } from 'node:fs';

const STEP = 'Step1072.9.32.2';
const snapshotUrl = new URL('./market-light-snapshot.mjs', import.meta.url);
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

let snapshot = readFileSync(snapshotUrl, 'utf8');
let rest = readFileSync(restUrl, 'utf8');

const oldSnapshot = "    quote_volume_24h: volume != null ? volume * last : null,";
const newSnapshot = [
  `    // ${STEP}: Coinbase ticker_batch exposes exact 24h base volume, but`,
  '    // it does not expose exact 24h USD quote turnover. base volume * current',
  '    // last price is only an approximation, so fail closed for exact turnover.',
  '    quote_volume_24h: null,',
  '    quote_volume_unit: null,',
  "    quote_volume_source: 'coinbase_ticker_batch_exact_quote_turnover_unavailable',",
].join('\n');

const oldRest = [
  '    base_volume_24h: baseVolume,',
  '    quote_volume_24h:',
  '      last !== null && baseVolume !== null ? last * baseVolume : null,',
].join('\n');
const newRest = [
  '    base_volume_24h: baseVolume,',
  `    // ${STEP}: Coinbase /stats volume is base-asset volume. Do not`,
  '    // synthesize exact 24h USD turnover from current last * rolling base volume.',
].join('\n');

const oldRestProvenance = [
  '  row.best_bid = num(live?.bid);',
  '  row.best_ask = num(live?.ask);',
  '  coinbaseTickerCache.set(cacheKey, { at: Date.now(), row });',
].join('\n');
const newRestProvenance = [
  '  row.best_bid = num(live?.bid);',
  '  row.best_ask = num(live?.ask);',
  '  row.quote_volume_unit = null;',
  "  row.quote_volume_source = 'coinbase_stats_exact_quote_turnover_unavailable';",
  '  coinbaseTickerCache.set(cacheKey, { at: Date.now(), row });',
].join('\n');

const red = {
  market_light_approximation_anchor: countExact(snapshot, oldSnapshot),
  market_rest_approximation_anchor: countExact(rest, oldRest),
  market_rest_provenance_anchor: countExact(rest, oldRestProvenance),
};
console.log(`${STEP} RED`, JSON.stringify(red));
if (Object.values(red).some((count) => count !== 1)) {
  throw new Error(`${STEP} RED baseline mismatch; refusing build-time mutation`);
}

snapshot = replaceExactlyOnce(
  snapshot,
  oldSnapshot,
  newSnapshot,
  'market_light_coinbase_quote_turnover',
);
rest = replaceExactlyOnce(
  rest,
  oldRest,
  newRest,
  'market_rest_coinbase_quote_turnover',
);
rest = replaceExactlyOnce(
  rest,
  oldRestProvenance,
  newRestProvenance,
  'market_rest_coinbase_quote_turnover_provenance',
);

const green = {
  market_light_old_anchor_remaining: countExact(snapshot, oldSnapshot),
  market_rest_old_anchor_remaining: countExact(rest, oldRest),
  market_light_unavailable_marker: countExact(
    snapshot,
    "quote_volume_source: 'coinbase_ticker_batch_exact_quote_turnover_unavailable'",
  ),
  market_rest_unavailable_marker: countExact(
    rest,
    "row.quote_volume_source = 'coinbase_stats_exact_quote_turnover_unavailable';",
  ),
};
console.log(`${STEP} GREEN`, JSON.stringify(green));
if (
  green.market_light_old_anchor_remaining !== 0 ||
  green.market_rest_old_anchor_remaining !== 0 ||
  green.market_light_unavailable_marker !== 1 ||
  green.market_rest_unavailable_marker !== 1
) {
  throw new Error(`${STEP} GREEN invariant failed; refusing image build`);
}

writeFileSync(snapshotUrl, snapshot, 'utf8');
writeFileSync(restUrl, rest, 'utf8');

console.log(`${STEP} BUILD_PATCH_PASS`);
