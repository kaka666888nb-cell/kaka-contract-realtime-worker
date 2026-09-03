from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def replace_once(path, old, new, label):
    p = ROOT / path
    s = p.read_text(encoding='utf-8')
    if old not in s:
        if new in s:
            print(f'SKIP {label}: already applied')
            return
        raise SystemExit(f'PATCH_MISMATCH {label} in {path}')
    s = s.replace(old, new, 1)
    p.write_text(s, encoding='utf-8')
    print(f'PASS {label}')

# ---- Binance merged public WS: admit st=2 COIN-M dated delivery rows into a separate map. ----
path = 'src/binance-contract-market.mjs'
replace_once(path, "const VERSION = '650.8.15.44';", "const VERSION = '650.8.15.44.1';", 'binance version')
replace_once(path,
"""const universeBySymbol = new Map();
const tickerBySymbol = new Map();
const realtimeMetaBySymbol = new Map();
const connectionState = new Map();
""",
"""const universeBySymbol = new Map();
const tickerBySymbol = new Map();
const realtimeMetaBySymbol = new Map();
// Step1060.33.5: reuse the existing merged Binance futures public WebSocket streams
// for COIN-M dated delivery contracts. No new socket and no Binance REST request is
// introduced. st=2 rows are kept separate from the existing USDⓈ-M perpetual maps.
const deliveryBySymbol = new Map();
let lastDeliveryEventAt = 0;
const connectionState = new Map();
""", 'binance delivery state')
replace_once(path,
"""function isUsdmPayload(item) {
  const unifiedType = finite(item?.st);
  return unifiedType === null || unifiedType === 1;
}

function normalizedPerpetual(item) {
""",
"""function isUsdmPayload(item) {
  const unifiedType = finite(item?.st);
  return unifiedType === null || unifiedType === 1;
}

function isCoinMPayload(item) {
  return finite(item?.st) === 2;
}

function deliveryExpiryFromSymbol(rawSymbol) {
  const match = String(rawSymbol || '').trim().toUpperCase().match(/_(\\d{2})(\\d{2})(\\d{2})$/);
  if (!match) return 0;
  const year = 2000 + Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isInteger(year) || month < 1 || month > 12 || day < 1 || day > 31) return 0;
  // Binance quarterly delivery symbols encode YYMMDD and expire at 08:00 UTC.
  // contractInfo.dt, when observed, always overrides this cold-start convention.
  const ms = Date.UTC(year, month - 1, day, 8, 0, 0, 0);
  const check = new Date(ms);
  if (check.getUTCFullYear() !== year || check.getUTCMonth() !== month - 1 || check.getUTCDate() !== day) return 0;
  return ms;
}

export function normalizeBinanceCoinMDeliveryPublicRow(item) {
  if (!item || typeof item !== 'object' || !isCoinMPayload(item)) return null;
  const rawSymbol = String(item.s ?? item.symbol ?? '').trim().toUpperCase();
  if (!/_\\d{6}$/.test(rawSymbol)) return null;
  const rawPair = String(item.ps ?? item.pair ?? rawSymbol.replace(/_\\d{6}$/, '')).trim().toUpperCase();
  const pair = compact(rawPair);
  const [base, quote] = splitQuote(pair);
  if (!rawSymbol || !pair || !base || !quote) return null;
  const expiryMs = finite(item.dt ?? item.deliveryDate ?? item.delivery_time_ms) || deliveryExpiryFromSymbol(rawSymbol);
  return { symbol: rawSymbol, rawSymbol, pair, base, quote, expiryMs };
}

function upsertCoinMDelivery(item, source, observedAt = Date.now()) {
  const identity = normalizeBinanceCoinMDeliveryPublicRow(item);
  if (!identity) return false;
  const status = String(item.cs ?? item.contractStatus ?? item.status ?? 'TRADING').trim().toUpperCase() || 'TRADING';
  if (!['TRADING', 'PRE_DELIVERING', 'PRE_SETTLE'].includes(status)) return deliveryBySymbol.delete(identity.symbol);
  const previous = deliveryBySymbol.get(identity.symbol) || {};
  const sourceTimeMs = finite(item.E ?? item.time ?? item.source_time) || observedAt;
  const expiryMs = finite(item.dt ?? item.deliveryDate) || previous.expiry_timestamp_ms || identity.expiryMs;
  const last = finite(item.c ?? item.lastPrice ?? item.last_price ?? item.price);
  const mark = finite(item.p ?? item.markPrice ?? item.mark_price);
  const index = finite(item.i ?? item.indexPrice ?? item.index_price);
  const contractType = String(item.ct ?? item.contractType ?? previous.contract_type ?? 'DELIVERY').trim().toUpperCase();
  const next = mergeNonNull(previous, {
    provider: PROVIDER, market_type: 'delivery', symbol: identity.symbol, raw_symbol: identity.rawSymbol,
    pair: identity.pair, base_asset: identity.base, quote_asset: identity.quote, quote_symbol: identity.quote,
    settle_asset: identity.base, contract_type: contractType, status, active: true,
    expiry_timestamp_ms: expiryMs && expiryMs > 0 ? expiryMs : null,
    expiry_at: expiryMs && expiryMs > 0 ? iso(expiryMs) : null,
    expiry_source: finite(item.dt ?? item.deliveryDate) ? 'binance_contract_info_dt' : (previous.expiry_source || 'binance_symbol_yymmdd_0800utc'),
    last_price: last, price: last, mark_price: mark, index_price: index,
    source_time: iso(sourceTimeMs), cached_at: iso(observedAt), source, symbol_type: 2,
  });
  if (!(finite(next.expiry_timestamp_ms) > Date.now())) {
    deliveryBySymbol.delete(identity.symbol);
    return false;
  }
  deliveryBySymbol.set(identity.symbol, next);
  lastDeliveryEventAt = Math.max(lastDeliveryEventAt, sourceTimeMs);
  return true;
}

function normalizedPerpetual(item) {
""", 'binance delivery parser')
replace_once(path,
"""  const now = Date.now();
  let accepted = 0;
  for (const item of rows) {
    const identity = normalizedPerpetual(item);
""",
"""  const now = Date.now();
  let accepted = 0;
  let deliveryAccepted = 0;
  for (const item of rows) {
    if (upsertCoinMDelivery(item, 'binance_official_public_merged_ticker_websocket', now)) deliveryAccepted += 1;
    const identity = normalizedPerpetual(item);
""", 'ticker delivery ingest')
replace_once(path,
"""  if (accepted) {
    lastTickerEventAt = now;
    schedulePersist();
  }
}

function handleBookTickerMessage(raw) {
""",
"""  if (accepted) {
    lastTickerEventAt = now;
    schedulePersist();
  }
  if (deliveryAccepted) lastDeliveryEventAt = now;
}

function handleBookTickerMessage(raw) {
""", 'ticker delivery timestamp')
replace_once(path,
"""  const now = Date.now();
  let accepted = 0;
  const currentSymbols = new Set();
  for (const item of rows) {
    const identity = normalizedPerpetual(item);
""",
"""  const now = Date.now();
  let accepted = 0;
  let deliveryAccepted = 0;
  const currentSymbols = new Set();
  for (const item of rows) {
    if (upsertCoinMDelivery(item, 'binance_official_public_merged_mark_price_websocket', now)) deliveryAccepted += 1;
    const identity = normalizedPerpetual(item);
""", 'mark-price delivery ingest')
replace_once(path,
"""    if (!currentPriceBaselineRunning && !currentPriceBaselineTimer) {
      scheduleCurrentPriceBaseline(750);
    }
  }
}

function handleContractInfoMessage(raw) {
""",
"""    if (!currentPriceBaselineRunning && !currentPriceBaselineTimer) {
      scheduleCurrentPriceBaseline(750);
    }
  }
  if (deliveryAccepted) lastDeliveryEventAt = now;
}

function handleContractInfoMessage(raw) {
""", 'mark-price delivery timestamp')
replace_once(path,
"""  for (const item of rows) {
    if (!item || typeof item !== 'object' || !isUsdmPayload(item)) continue;
    const symbol = compact(item.s ?? item.symbol);
""",
"""  for (const item of rows) {
    if (!item || typeof item !== 'object') continue;
    if (isCoinMPayload(item)) {
      if (upsertCoinMDelivery(item, 'binance_official_public_contract_info_websocket', now)) accepted += 1;
      continue;
    }
    if (!isUsdmPayload(item)) continue;
    const symbol = compact(item.s ?? item.symbol);
""", 'contractInfo delivery ingest')
replace_once(path,
"""  const currentSymbols = new Set();
  let seeded = 0;
  for (const item of rows) {
    const identity = normalizedPerpetual(item);
""",
"""  const currentSymbols = new Set();
  let seeded = 0;
  for (const item of rows) {
    upsertCoinMDelivery(item, 'binance_official_public_ws_api_ticker_price_all_symbols', observedAt);
    const identity = normalizedPerpetual(item);
""", 'price baseline delivery ingest')
replace_once(path,
"""  return rows;
}

export function getBinanceContractRealtimeMeta(symbol) {
""",
"""  return rows;
}

export function getBinanceDeliveryContractsSnapshot({ nowMs = Date.now() } = {}) {
  startBinanceContractMarket();
  const rows = [...deliveryBySymbol.values()]
    .filter((row) => {
      const expiry = finite(row?.expiry_timestamp_ms);
      const price = finite(row?.mark_price ?? row?.last_price ?? row?.price);
      return expiry != null && expiry > nowMs && price != null && price > 0 && row?.active !== false;
    })
    .sort((a, b) => Number(a.expiry_timestamp_ms || 0) - Number(b.expiry_timestamp_ms || 0) || String(a.symbol).localeCompare(String(b.symbol)));
  return {
    ok: true, version: VERSION, provider: PROVIDER, market_type: 'delivery',
    source: 'binance_existing_merged_public_futures_websocket_reuse', ready: rows.length > 0,
    row_count: rows.length, rows: rows.map((row) => ({ ...row })),
    last_delivery_event_at: lastDeliveryEventAt ? iso(lastDeliveryEventAt) : null,
    binance_contract_rest_requests: 0, additional_websocket_connections: 0,
    reuses_existing_contract_info_stream: true, reuses_existing_all_market_ticker_stream: true,
    reuses_existing_mark_price_stream: true, reads_scale_with_users: false,
  };
}

export function getBinanceContractRealtimeMeta(symbol) {
""", 'delivery snapshot export')
replace_once(path,
"""    snapshot_persist_stats: {
      ...snapshotPersistStats,
      last_attempt_at: snapshotPersistStats.last_attempt_at ? iso(snapshotPersistStats.last_attempt_at) : null,
    },
    websocket_ingress: Object.fromEntries(
""",
"""    snapshot_persist_stats: {
      ...snapshotPersistStats,
      last_attempt_at: snapshotPersistStats.last_attempt_at ? iso(snapshotPersistStats.last_attempt_at) : null,
    },
    delivery_ws_reuse: {
      mode: 'coin_m_delivery_from_existing_merged_public_futures_websockets', symbol_type_filter: 2,
      rows: deliveryBySymbol.size, last_delivery_event_at: lastDeliveryEventAt ? iso(lastDeliveryEventAt) : null,
      additional_websocket_connections: 0, additional_rest_requests: 0, user_reads_start_connections: false,
      reuses_contract_info_stream: true, reuses_all_market_ticker_stream: true, reuses_mark_price_stream: true,
    },
    websocket_ingress: Object.fromEntries(
""", 'delivery health diagnostics')

# ---- Contract basis: add Binance delivery as an internal shared-WS provider, never REST. ----
path = 'src/contract-basis.mjs'
replace_once(path,
"// Binance COIN-M delivery REST is intentionally NOT called here because KakaWeb3's existing Binance contract REST guard remains permanent.",
"// Binance COIN-M delivery reuses the already-running merged Binance public futures WebSocket streams; Binance REST remains permanently disabled.", 'basis header')
replace_once(path,
"""import { getMarketLightInternalSnapshot } from './market-light-bridge.mjs';

const STEP_VERSION = '650.8.15.197.3.3.32.4-basis-delivery-2';
""",
"""import { getMarketLightInternalSnapshot } from './market-light-bridge.mjs';
import { getBinanceDeliveryContractsSnapshot } from './binance-contract-market.mjs';

const STEP_VERSION = '650.8.15.197.3.3.32.5-basis-delivery-binance-ws';
""", 'basis import/version')
replace_once(path,
"const DELIVERY_SOURCE_PROVIDERS = Object.freeze(['okx', 'bybit', 'bitget', 'gate']);",
"const DELIVERY_SOURCE_PROVIDERS = Object.freeze(['binance', 'okx', 'bybit', 'bitget', 'gate']);", 'basis providers')
replace_once(path,
"""async function collectGateDelivery({ spotByBase, nowMs }) {
""",
"""async function collectBinanceDelivery({ spotByBase, nowMs }) {
  const snapshot = getBinanceDeliveryContractsSnapshot({ nowMs });
  if (!snapshot?.ok || snapshot?.ready !== true || !Array.isArray(snapshot?.rows) || !snapshot.rows.length) {
    throw new Error('binance_delivery_shared_ws_not_ready');
  }
  const rows = [];
  for (const item of snapshot.rows) {
    const row = buildDeliveryRow({
      provider: 'binance', symbol: item?.symbol, base: item?.base_asset,
      quote: item?.quote_asset || 'USD', settle: item?.settle_asset || item?.base_asset,
      contractType: item?.contract_type || 'COIN_M_DELIVERY', cycle: '', expiryMs: item?.expiry_timestamp_ms,
      lastPrice: item?.last_price ?? item?.price, markPrice: item?.mark_price, indexPrice: item?.index_price,
      sourceMs: timeMs(item?.source_time) || nowMs, sourceUrl: 'binance_existing_merged_public_futures_websocket',
      spotByBase, nowMs,
    });
    if (row) {
      row.delivery_identity_source = item?.expiry_source || 'binance_public_websocket';
      row.binance_contract_rest_requests = 0;
      row.binance_shared_ws_reuse = true;
      rows.push(row);
    }
  }
  return rows;
}

async function collectGateDelivery({ spotByBase, nowMs }) {
""", 'basis binance collector')
replace_once(path,
"""  const providerCoverage = {
    binance: {
      provider: 'binance',
      source_supported: false,
      delivery_contracts: 0,
      comparable_pairs: 0,
      status: 'guarded',
      reason: 'binance_contract_rest_guard_no_new_rest',
      upstream_requests: 0,
    },
  };
""",
"""  const providerCoverage = {};
""", 'basis remove binance guarded placeholder')
replace_once(path,
"""  const jobs = [
    ['okx', collectOkxDelivery],
""",
"""  const jobs = [
    ['binance', collectBinanceDelivery],
    ['okx', collectOkxDelivery],
""", 'basis binance job')
replace_once(path,
"""        status: result.ok ? 'ready' : 'error',
        reason: result.ok ? '' : result.error,
      };
""",
"""        status: result.ok ? 'ready' : 'error',
        reason: result.ok ? '' : result.error,
        upstream_requests: result.provider === 'binance' ? 0 : null,
        shared_ws_reuse: result.provider === 'binance',
      };
""", 'basis provider coverage')
replace_once(path,
"""      binance_contract_rest_requests: 0,
      binance_contract_rest_guard_preserved: true,
      reason,
""",
"""      binance_contract_rest_requests: 0,
      binance_contract_rest_guard_preserved: true,
      binance_delivery_source: 'existing_merged_public_futures_websocket_st_2',
      binance_delivery_additional_websocket_connections: 0,
      binance_delivery_user_reads_start_connections: false,
      reason,
""", 'basis snapshot safety metadata')
replace_once(path,
"""      binance_contract_rest_requests: 0,
      binance_contract_rest_guard_preserved: true,
    },
""",
"""      binance_contract_rest_requests: 0,
      binance_contract_rest_guard_preserved: true,
      binance_delivery_source: 'existing_merged_public_futures_websocket_st_2',
      binance_delivery_additional_websocket_connections: 0,
    },
""", 'basis health safety metadata')

# Self-test is deliberately small and pure: proves st=2 gating, USD identity and expiry parsing.
test = ROOT / 'tests/step1060_33_5_binance_delivery_selftest.mjs'
test.write_text("""import assert from 'node:assert/strict';
import { normalizeBinanceCoinMDeliveryPublicRow } from '../src/binance-contract-market.mjs';
const exactDt = Date.UTC(2026, 8, 25, 8, 0, 0, 0);
const exact = normalizeBinanceCoinMDeliveryPublicRow({s:'BTCUSD_260925',ps:'BTCUSD',ct:'CURRENT_QUARTER',dt:exactDt,cs:'TRADING',st:2});
assert.ok(exact); assert.equal(exact.base,'BTC'); assert.equal(exact.quote,'USD'); assert.equal(exact.expiryMs,exactDt);
const fallback = normalizeBinanceCoinMDeliveryPublicRow({s:'ETHUSD_261225',ps:'ETHUSD',c:'5000',st:2});
assert.ok(fallback); assert.equal(fallback.expiryMs,Date.UTC(2026,11,25,8,0,0,0));
assert.equal(normalizeBinanceCoinMDeliveryPublicRow({s:'BTCUSDT',ps:'BTCUSDT',st:1}),null);
assert.equal(normalizeBinanceCoinMDeliveryPublicRow({s:'BTCUSD_PERP',ps:'BTCUSD',st:2}),null);
console.log('PASS Step1060.33.5 Binance COIN-M delivery public WS parser');
""", encoding='utf-8')
print('PASS test file')
