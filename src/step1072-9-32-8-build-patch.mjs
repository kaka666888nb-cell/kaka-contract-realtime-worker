import { readFileSync, writeFileSync } from 'node:fs';

const STEP = 'Step1072.9.32.8';
const depthUrl = new URL('./contract-depth.mjs', import.meta.url);

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

function fixtureDecision({ hasData, officialEmpty, staleUsable }) {
  if (hasData) return 'live';
  if (!officialEmpty) return 'error';
  return staleUsable ? 'stale-preserve' : 'official-empty';
}

const semantic = {
  normal_book_stays_live: fixtureDecision({ hasData: true, officialEmpty: false, staleUsable: false }) === 'live',
  malformed_or_failed_empty_stays_error: fixtureDecision({ hasData: false, officialEmpty: false, staleUsable: false }) === 'error',
  official_empty_with_recent_verified_book_preserves_stale: fixtureDecision({ hasData: false, officialEmpty: true, staleUsable: true }) === 'stale-preserve',
  official_empty_without_recent_book_is_not_upstream_failure: fixtureDecision({ hasData: false, officialEmpty: true, staleUsable: false }) === 'official-empty',
};
console.log(`${STEP} SEMANTIC`, JSON.stringify(semantic));
if (Object.values(semantic).some((ok) => !ok)) {
  throw new Error(`${STEP} semantic fixture failed before mutation`);
}

let depth = readFileSync(depthUrl, 'utf8');

const loaderOld = [
  "  const data = await fetchJson(`https://api.bitget.com/api/v2/spot/market/orderbook?symbol=${encodeURIComponent(native)}&type=step0&limit=${Math.max(1, Math.min(limit, 50))}`);",
  "  if (String(data?.code || '') !== '00000' || !data?.data) throw new Error(`bitget_spot_orderbook_${data?.code ?? 'invalid'}`);",
  "  return { bids: normalizeLevels(data.data.bids, { side: 'bid' }).slice(0, limit), asks: normalizeLevels(data.data.asks, { side: 'ask' }).slice(0, limit), timestamp_ms: integerValue(data.data.ts) || integerValue(data?.requestTime) || Date.now(), upstream_host: 'api.bitget.com', native_symbol: native };",
].join('\n');

const loaderNew = [
  "  const data = await fetchJson(`https://api.bitget.com/api/v2/spot/market/orderbook?symbol=${encodeURIComponent(native)}&type=step0&limit=${Math.max(1, Math.min(limit, 50))}`);",
  "  if (String(data?.code || '') !== '00000' || !data?.data) throw new Error(`bitget_spot_orderbook_${data?.code ?? 'invalid'}`);",
  '  const bids = normalizeLevels(data.data.bids, { side: \'bid\' }).slice(0, limit);',
  '  const asks = normalizeLevels(data.data.asks, { side: \'ask\' }).slice(0, limit);',
  `  // ${STEP}: Bitget can successfully return an empty official spot book for`,
  '  // some products even while its ticker remains available. Preserve that exact',
  '  // capability signal; never turn a successful empty book into an upstream outage.',
  '  return {',
  '    bids,',
  '    asks,',
  '    timestamp_ms: integerValue(data.data.ts) || integerValue(data?.requestTime) || Date.now(),',
  "    upstream_host: 'api.bitget.com',",
  '    native_symbol: native,',
  '    official_orderbook_empty: bids.length === 0 || asks.length === 0,',
  '  };',
].join('\n');

const resolveOld = [
  "        const hasData = view === 'trades' ? payload.items.length > 0 : payload.bids.length > 0 && payload.asks.length > 0;",
  "        const quietBinanceTradeStream = provider === 'binance' && view === 'trades' && payload.connected === true;",
  '        if (!hasData && !quietBinanceTradeStream) throw new Error(`empty_${view}`);',
].join('\n');

const resolveNew = [
  "        const hasData = view === 'trades' ? payload.items.length > 0 : payload.bids.length > 0 && payload.asks.length > 0;",
  "        const quietBinanceTradeStream = provider === 'binance' && view === 'trades' && payload.connected === true;",
  '        const officialEmptyBitgetSpotBook =',
  "          provider === 'bitget' &&",
  "          marketType === 'spot' &&",
  "          view === 'orderbook' &&",
  '          data?.official_orderbook_empty === true;',
  '        if (!hasData && officialEmptyBitgetSpotBook) {',
  '          const staleUsable = Boolean(',
  '            cached &&',
  '            now - cached.storedAt <= STALE_MS &&',
  '            Array.isArray(cached.payload?.bids) &&',
  '            cached.payload.bids.length > 0 &&',
  '            Array.isArray(cached.payload?.asks) &&',
  '            cached.payload.asks.length > 0,',
  '          );',
  '          if (staleUsable) {',
  '            return {',
  '              ...cached.payload,',
  "              cache_state: 'stale-official-empty',",
  '              stale: true,',
  '              official_orderbook_empty: true,',
  "              empty_reason: 'official_orderbook_empty',",
  `              depth_policy_version: '${STEP}',`,
  '              official_empty_orderbook_is_upstream_failure: false,',
  '            };',
  '          }',
  '          payload.depth_available = false;',
  '          payload.official_orderbook_empty = true;',
  "          payload.empty_reason = 'official_orderbook_empty';",
  '          payload.partial = true;',
  `          payload.depth_policy_version = '${STEP}';`,
  '          payload.official_empty_orderbook_is_upstream_failure = false;',
  '        }',
  '        if (!hasData && !quietBinanceTradeStream && !officialEmptyBitgetSpotBook) {',
  '          throw new Error(`empty_${view}`);',
  '        }',
].join('\n');

const red = {
  loader_anchor: countExact(depth, loaderOld),
  resolve_anchor: countExact(depth, resolveOld),
  official_empty_loader_marker_before: countExact(depth, 'official_orderbook_empty: bids.length === 0 || asks.length === 0,'),
  policy_before: countExact(depth, `depth_policy_version: '${STEP}'`),
  stale_state_before: countExact(depth, "cache_state: 'stale-official-empty'"),
};
console.log(`${STEP} RED`, JSON.stringify(red));
if (
  red.loader_anchor !== 1 ||
  red.resolve_anchor !== 1 ||
  red.official_empty_loader_marker_before !== 0 ||
  red.policy_before !== 0 ||
  red.stale_state_before !== 0
) {
  throw new Error(`${STEP} RED baseline mismatch; refusing build-time mutation`);
}

depth = replaceExactlyOnce(depth, loaderOld, loaderNew, 'bitget_spot_official_empty_capability');
depth = replaceExactlyOnce(depth, resolveOld, resolveNew, 'official_empty_not_upstream_failure');

const green = {
  loader_marker: countExact(depth, 'official_orderbook_empty: bids.length === 0 || asks.length === 0,'),
  decision_marker: countExact(depth, 'const officialEmptyBitgetSpotBook ='),
  stale_state: countExact(depth, "cache_state: 'stale-official-empty'"),
  policy_assignment: countExact(depth, `payload.depth_policy_version = '${STEP}';`),
  policy_stale_field: countExact(depth, `depth_policy_version: '${STEP}',`),
  nonfailure_assignment: countExact(depth, 'payload.official_empty_orderbook_is_upstream_failure = false;'),
  nonfailure_stale_field: countExact(depth, 'official_empty_orderbook_is_upstream_failure: false,'),
  old_loader_remaining: countExact(depth, loaderOld),
  old_resolve_remaining: countExact(depth, resolveOld),
};
console.log(`${STEP} GREEN`, JSON.stringify(green));
if (
  green.loader_marker !== 1 ||
  green.decision_marker !== 1 ||
  green.stale_state !== 1 ||
  green.policy_assignment !== 1 ||
  green.policy_stale_field !== 1 ||
  green.nonfailure_assignment !== 1 ||
  green.nonfailure_stale_field !== 1 ||
  green.old_loader_remaining !== 0 ||
  green.old_resolve_remaining !== 0
) {
  throw new Error(`${STEP} GREEN invariant failed; refusing image build`);
}

writeFileSync(depthUrl, depth, 'utf8');
console.log(`${STEP} BUILD_PATCH_PASS`);
