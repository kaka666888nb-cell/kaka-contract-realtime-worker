import fs from 'node:fs';

const MARKET_PATH = 'src/market-rest.mjs';
const HISTORY_PATH = 'src/spot-flow-history.mjs';

const MARKET_MARKER = '// Step1072.9.32.11 preserve Binance official taker-buy Kline fields';
const HISTORY_MARKER = '// Step1072.9.32.11 robust Kline history timestamp parsing';

function count(text, needle) {
  if (!needle) return 0;
  return text.split(needle).length - 1;
}

function replaceExactlyOnce(text, oldText, newText, label) {
  const hits = count(text, oldText);
  if (hits !== 1) {
    throw new Error(`Step1072.9.32.11 RED anchor mismatch ${label}: expected=1 actual=${hits}`);
  }
  return text.replace(oldText, newText);
}

let market = fs.readFileSync(MARKET_PATH, 'utf8');
let history = fs.readFileSync(HISTORY_PATH, 'utf8');

const oldHistoryVersion = "const VERSION = '650.8.15.163';";
const newHistoryVersion = "const VERSION = '650.8.15.163.2';";

const oldRowsWithTimes = `function rowsWithTimes(raw) {
  const rows = Array.isArray(raw) ? raw.filter((row) => row && typeof row === 'object') : [];
  rows.sort((a, b) => Number(a.open_time ?? a.openTime ?? a.time ?? 0) - Number(b.open_time ?? b.openTime ?? b.time ?? 0));
  const deduped = [];
  const seen = new Set();
  for (const row of rows) {
    const time = Number(row.open_time ?? row.openTime ?? row.time ?? 0);
    if (!Number.isFinite(time) || time <= 0 || seen.has(time)) continue;
    seen.add(time);
    deduped.push(row);
  }
  return deduped;
}`;

const newRowsWithTimes = `${HISTORY_MARKER}
function spotFlowHistoryRowTimeMs(row) {
  const raw = row?.open_time_ms ?? row?.openTimeMs ?? row?.open_time ?? row?.openTime ?? row?.time ?? 0;
  const numeric = Number(raw);
  if (Number.isFinite(numeric) && numeric > 0) return numeric;
  const parsed = Date.parse(String(raw || ''));
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

function rowsWithTimes(raw) {
  const rows = Array.isArray(raw) ? raw.filter((row) => row && typeof row === 'object') : [];
  rows.sort((a, b) => spotFlowHistoryRowTimeMs(a) - spotFlowHistoryRowTimeMs(b));
  const deduped = [];
  const seen = new Set();
  for (const row of rows) {
    const time = spotFlowHistoryRowTimeMs(row);
    if (!Number.isFinite(time) || time <= 0 || seen.has(time)) continue;
    seen.add(time);
    deduped.push(row);
  }
  return deduped;
}`;

const oldMarketProjection = `          sourceRows = (Array.isArray(rawWsRows) ? rawWsRows : [])
            .map((a) => krow(provider, market, symbol, sourceInterval, [a[0], a[1], a[2], a[3], a[4], a[5], a[7], a[8]]))
            .filter(Boolean)
            .map((row) => ({
              ...row,
              source: 'binance_official_spot_ws_api_kline_shared',
              transport: 'shared_websocket_api',
            }));`;

const newMarketProjection = `          ${MARKET_MARKER}
          sourceRows = (Array.isArray(rawWsRows) ? rawWsRows : [])
            .map((a) => {
              const row = krow(provider, market, symbol, sourceInterval, [a[0], a[1], a[2], a[3], a[4], a[5], a[7], a[8]]);
              if (!row) return null;
              return {
                ...row,
                taker_buy_base_volume: Math.max(0, num(a[9]) || 0),
                taker_buy_quote_volume: Math.max(0, num(a[10]) || 0),
              };
            })
            .filter(Boolean)
            .map((row) => ({
              ...row,
              source: 'binance_official_spot_ws_api_kline_shared',
              transport: 'shared_websocket_api',
            }));`;

const red = {
  history_version_anchor: count(history, oldHistoryVersion),
  history_rows_anchor: count(history, oldRowsWithTimes),
  market_projection_anchor: count(market, oldMarketProjection),
  history_marker_before: count(history, HISTORY_MARKER),
  market_marker_before: count(market, MARKET_MARKER),
};
console.log('Step1072.9.32.11 RED', JSON.stringify(red));
if (
  red.history_version_anchor !== 1 ||
  red.history_rows_anchor !== 1 ||
  red.market_projection_anchor !== 1 ||
  red.history_marker_before !== 0 ||
  red.market_marker_before !== 0
) {
  throw new Error('Step1072.9.32.11 RED invariant failed; refusing image build');
}

// Unit semantics before mutating source: ISO timestamps must remain usable and
// Binance official Kline indices 9/10 are the taker-buy base/quote fields.
const isoFixture = '2026-09-17T17:35:00.000Z';
const parsedFixture = Date.parse(isoFixture);
const rawKlineFixture = [1789666500000, '1', '2', '0.5', '1.5', '100', 1789666559999, '150', 42, '40', '60', '0'];
if (!Number.isFinite(parsedFixture) || parsedFixture <= 0 || rawKlineFixture[9] !== '40' || rawKlineFixture[10] !== '60') {
  throw new Error('Step1072.9.32.11 semantic fixture failed');
}

history = replaceExactlyOnce(history, oldHistoryVersion, newHistoryVersion, 'history_version');
history = replaceExactlyOnce(history, oldRowsWithTimes, newRowsWithTimes, 'history_rows_with_times');
market = replaceExactlyOnce(market, oldMarketProjection, newMarketProjection, 'binance_spot_ws_kline_projection');

const green = {
  history_version: count(history, newHistoryVersion),
  history_marker: count(history, HISTORY_MARKER),
  time_helper: count(history, 'function spotFlowHistoryRowTimeMs(row)'),
  open_time_ms_priority: count(history, 'row?.open_time_ms ?? row?.openTimeMs'),
  old_numeric_sort_remaining: count(history, 'Number(a.open_time ?? a.openTime ?? a.time ?? 0)'),
  market_marker: count(market, MARKET_MARKER),
  taker_buy_base_projection: count(market, 'taker_buy_base_volume: Math.max(0, num(a[9]) || 0)'),
  taker_buy_quote_projection: count(market, 'taker_buy_quote_volume: Math.max(0, num(a[10]) || 0)'),
  old_projection_remaining: count(market, oldMarketProjection),
};
console.log('Step1072.9.32.11 GREEN', JSON.stringify(green));
if (
  green.history_version !== 1 ||
  green.history_marker !== 1 ||
  green.time_helper !== 1 ||
  green.open_time_ms_priority !== 1 ||
  green.old_numeric_sort_remaining !== 0 ||
  green.market_marker !== 1 ||
  green.taker_buy_base_projection !== 1 ||
  green.taker_buy_quote_projection !== 1 ||
  green.old_projection_remaining !== 0
) {
  throw new Error('Step1072.9.32.11 GREEN invariant failed; refusing image build');
}

fs.writeFileSync(HISTORY_PATH, history);
fs.writeFileSync(MARKET_PATH, market);
console.log('Step1072.9.32.11 BUILD_PATCH_PASS');
