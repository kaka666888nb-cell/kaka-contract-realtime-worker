import { readFileSync, writeFileSync } from 'node:fs';

const STEP = 'Step1072.9.32.9';
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

function fixtureNewestFirst(rows) {
  return [...rows].sort((a, b) => b.time_ms - a.time_ms);
}

function fixtureEmptyTradeDecision({ hasData, officialEmpty, staleUsable }) {
  if (hasData) return 'live';
  if (!officialEmpty) return 'error';
  return staleUsable ? 'stale-preserve' : 'official-empty';
}

const sortedFixture = fixtureNewestFirst([
  { id: 'older', time_ms: 100 },
  { id: 'newest', time_ms: 300 },
  { id: 'middle', time_ms: 200 },
]);
const semantic = {
  newest_first: sortedFixture.map((row) => row.time_ms).join(',') === '300,200,100',
  timestamp_uses_newest: sortedFixture[0]?.time_ms === 300,
  live_trade_rows_stay_live: fixtureEmptyTradeDecision({ hasData: true, officialEmpty: false, staleUsable: false }) === 'live',
  malformed_empty_stays_error: fixtureEmptyTradeDecision({ hasData: false, officialEmpty: false, staleUsable: false }) === 'error',
  official_empty_preserves_recent_verified_rows: fixtureEmptyTradeDecision({ hasData: false, officialEmpty: true, staleUsable: true }) === 'stale-preserve',
  official_empty_without_stale_is_not_upstream_failure: fixtureEmptyTradeDecision({ hasData: false, officialEmpty: true, staleUsable: false }) === 'official-empty',
};
console.log(`${STEP} SEMANTIC`, JSON.stringify(semantic));
if (Object.values(semantic).some((ok) => !ok)) {
  throw new Error(`${STEP} semantic fixture failed before mutation`);
}

let depth = readFileSync(depthUrl, 'utf8');

const bitgetTradesOld = [
  "  if (view === 'trades') {",
  "    const data = await fetchJson(`https://api.bitget.com/api/v2/spot/market/fills?symbol=${encodeURIComponent(native)}&limit=${Math.min(limit, 100)}`);",
  "    if (String(data?.code || '') !== '00000' || !Array.isArray(data?.data)) throw new Error(`bitget_spot_trades_${data?.code ?? 'invalid'}`);",
  '    const items = data.data.map((row) => {',
  '      const price = positiveNumber(row?.price);',
  '      const quantity = positiveNumber(row?.size);',
  '      const timeMs = integerValue(row?.ts);',
  "      const rawSide = String(row?.side || '').toLowerCase();",
  "      const side = rawSide === 'buy' ? 'buy' : rawSide === 'sell' ? 'sell' : '';",
  '      if (price == null || quantity == null || timeMs <= 0 || !side) return null;',
  '      return { id: String(row?.tradeId ?? `${timeMs}:${price}:${quantity}`), time_ms: timeMs, price, quantity, quote_amount: price * quantity, side };',
  '    }).filter(Boolean);',
  "    return { items, timestamp_ms: items[0]?.time_ms || integerValue(data?.requestTime) || Date.now(), upstream_host: 'api.bitget.com', native_symbol: native };",
  '  }',
].join('\n');

const bitgetTradesNew = [
  "  if (view === 'trades') {",
  `    // ${STEP}: Bitget unified public market data documents spot recent fills`,
  '    // on v3. Reality and normal crypto stay on the same exact requested symbol;',
  '    // do not use authenticated account/reality fills and do not substitute venues.',
  "    const data = await fetchJson(`https://api.bitget.com/api/v3/market/fills?category=SPOT&symbol=${encodeURIComponent(native)}&limit=${Math.min(limit, 100)}`);",
  "    if (String(data?.code || '') !== '00000' || !Array.isArray(data?.data)) throw new Error(`bitget_spot_trades_${data?.code ?? 'invalid'}`);",
  '    const items = data.data.map((row) => {',
  '      const price = positiveNumber(row?.price);',
  '      const quantity = positiveNumber(row?.size);',
  '      const timeMs = integerValue(row?.ts);',
  "      const rawSide = String(row?.side || '').toLowerCase();",
  "      const side = rawSide === 'buy' ? 'buy' : rawSide === 'sell' ? 'sell' : '';",
  '      if (price == null || quantity == null || timeMs <= 0 || !side) return null;',
  '      return { id: String(row?.execId ?? row?.tradeId ?? `${timeMs}:${price}:${quantity}`), time_ms: timeMs, price, quantity, quote_amount: price * quantity, side };',
  '    }).filter(Boolean);',
  '    items.sort((a, b) => b.time_ms - a.time_ms);',
  '    return {',
  '      items,',
  '      timestamp_ms: items[0]?.time_ms || integerValue(data?.requestTime) || Date.now(),',
  "      upstream_host: 'api.bitget.com',",
  '      native_symbol: native,',
  "      transport: 'rest_public_v3_market_fills',",
  '      official_trades_empty: items.length === 0,',
  '    };',
  '  }',
].join('\n');

const binanceReturnOld = [
  '      };',
  '    }).filter(Boolean);',
  '    return {',
  '      items,',
  '      timestamp_ms: items[0]?.time_ms || Date.now(),',
  '      upstream_host: upstreamHost,',
  '      native_symbol: native,',
  '      transport,',
  '    };',
].join('\n');

const binanceReturnNew = [
  '      };',
  '    }).filter(Boolean);',
  `    // ${STEP}: Binance aggregate-trade responses can arrive oldest-first.`,
  '    // Normalize the public API contract so items[0] and timestamp_ms are newest.',
  '    items.sort((a, b) => b.time_ms - a.time_ms);',
  '    return {',
  '      items,',
  '      timestamp_ms: items[0]?.time_ms || Date.now(),',
  '      upstream_host: upstreamHost,',
  '      native_symbol: native,',
  '      transport,',
  '    };',
].join('\n');

const emptyBookDecisionOld = [
  '        const officialEmptyBitgetSpotBook =',
  "          provider === 'bitget' &&",
  "          marketType === 'spot' &&",
  "          view === 'orderbook' &&",
  '          data?.official_orderbook_empty === true;',
  '        if (!hasData && officialEmptyBitgetSpotBook) {',
].join('\n');

const emptyBookDecisionNew = [
  '        const officialEmptyBitgetSpotBook =',
  "          provider === 'bitget' &&",
  "          marketType === 'spot' &&",
  "          view === 'orderbook' &&",
  '          data?.official_orderbook_empty === true;',
  '        const officialEmptyBitgetSpotTrades =',
  "          provider === 'bitget' &&",
  "          marketType === 'spot' &&",
  "          view === 'trades' &&",
  '          data?.official_trades_empty === true;',
  '        if (!hasData && officialEmptyBitgetSpotBook) {',
].join('\n');

const genericEmptyOld = [
  '        if (!hasData && !quietBinanceTradeStream && !officialEmptyBitgetSpotBook) {',
  '          throw new Error(`empty_${view}`);',
  '        }',
].join('\n');

const genericEmptyNew = [
  '        if (!hasData && officialEmptyBitgetSpotTrades) {',
  '          const staleTradesUsable = Boolean(',
  '            cached &&',
  '            now - cached.storedAt <= STALE_MS &&',
  '            Array.isArray(cached.payload?.items) &&',
  '            cached.payload.items.length > 0,',
  '          );',
  '          if (staleTradesUsable) {',
  '            return {',
  '              ...cached.payload,',
  "              cache_state: 'stale-official-empty-trades',",
  '              stale: true,',
  '              official_trades_empty: true,',
  "              empty_reason: 'official_trades_empty',",
  `              trades_policy_version: '${STEP}',`,
  '              official_empty_trades_is_upstream_failure: false,',
  '            };',
  '          }',
  '          payload.trades_available = false;',
  '          payload.official_trades_empty = true;',
  "          payload.empty_reason = 'official_trades_empty';",
  '          payload.partial = true;',
  `          payload.trades_policy_version = '${STEP}';`,
  '          payload.official_empty_trades_is_upstream_failure = false;',
  '        }',
  '        if (',
  '          !hasData &&',
  '          !quietBinanceTradeStream &&',
  '          !officialEmptyBitgetSpotBook &&',
  '          !officialEmptyBitgetSpotTrades',
  '        ) {',
  '          throw new Error(`empty_${view}`);',
  '        }',
].join('\n');

const red = {
  bitget_v2_block: countExact(depth, bitgetTradesOld),
  bitget_v3_endpoint_before: countExact(depth, '/api/v3/market/fills?category=SPOT'),
  binance_return_anchor: countExact(depth, binanceReturnOld),
  binance_sort_marker_before: countExact(depth, `${STEP}: Binance aggregate-trade responses can arrive oldest-first.`),
  step32_8_book_decision_anchor: countExact(depth, emptyBookDecisionOld),
  generic_empty_anchor: countExact(depth, genericEmptyOld),
  official_empty_trade_marker_before: countExact(depth, 'official_trades_empty: items.length === 0,'),
};
console.log(`${STEP} RED`, JSON.stringify(red));
if (
  red.bitget_v2_block !== 1 ||
  red.bitget_v3_endpoint_before !== 0 ||
  red.binance_return_anchor !== 1 ||
  red.binance_sort_marker_before !== 0 ||
  red.step32_8_book_decision_anchor !== 1 ||
  red.generic_empty_anchor !== 1 ||
  red.official_empty_trade_marker_before !== 0
) {
  throw new Error(`${STEP} RED baseline mismatch; refusing build-time mutation`);
}

depth = replaceExactlyOnce(depth, bitgetTradesOld, bitgetTradesNew, 'bitget_v3_public_spot_fills');
depth = replaceExactlyOnce(depth, binanceReturnOld, binanceReturnNew, 'binance_newest_first');
depth = replaceExactlyOnce(depth, emptyBookDecisionOld, emptyBookDecisionNew, 'bitget_official_empty_trade_capability');
depth = replaceExactlyOnce(depth, genericEmptyOld, genericEmptyNew, 'official_empty_trade_not_upstream_failure');

const green = {
  bitget_v2_endpoint_remaining: countExact(depth, '/api/v2/spot/market/fills?symbol='),
  bitget_v3_endpoint: countExact(depth, '/api/v3/market/fills?category=SPOT'),
  bitget_v3_transport: countExact(depth, "transport: 'rest_public_v3_market_fills'"),
  bitget_official_empty_loader: countExact(depth, 'official_trades_empty: items.length === 0,'),
  binance_sort_marker: countExact(depth, `${STEP}: Binance aggregate-trade responses can arrive oldest-first.`),
  newest_first_sort_total: countExact(depth, 'items.sort((a, b) => b.time_ms - a.time_ms);'),
  official_empty_trade_decision: countExact(depth, 'const officialEmptyBitgetSpotTrades ='),
  official_empty_trade_stale: countExact(depth, "cache_state: 'stale-official-empty-trades'"),
  trade_policy_assignment: countExact(depth, `payload.trades_policy_version = '${STEP}';`),
  trade_policy_stale_field: countExact(depth, `trades_policy_version: '${STEP}',`),
  old_generic_empty_remaining: countExact(depth, genericEmptyOld),
};
console.log(`${STEP} GREEN`, JSON.stringify(green));
if (
  green.bitget_v2_endpoint_remaining !== 0 ||
  green.bitget_v3_endpoint !== 1 ||
  green.bitget_v3_transport !== 1 ||
  green.bitget_official_empty_loader !== 1 ||
  green.binance_sort_marker !== 1 ||
  green.newest_first_sort_total < 2 ||
  green.official_empty_trade_decision !== 1 ||
  green.official_empty_trade_stale !== 1 ||
  green.trade_policy_assignment !== 1 ||
  green.trade_policy_stale_field !== 1 ||
  green.old_generic_empty_remaining !== 0
) {
  throw new Error(`${STEP} GREEN invariant failed; refusing image build`);
}

writeFileSync(depthUrl, depth, 'utf8');
console.log(`${STEP} BUILD_PATCH_PASS`);
