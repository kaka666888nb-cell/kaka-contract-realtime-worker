import {
  getBinanceContractMarketHealth,
  getBinanceContractTickers,
  getBinanceContractUniverse,
  startBinanceContractMarket,
} from './binance-contract-market.mjs';
import {
  getBinanceContractKlineSeed,
  getBinanceContractKlineSeedHealth,
} from './binance-contract-kline-seed.mjs';
import {
  checkBinanceContractKlineRelayDeployment,
  completeBinanceContractKlineRelayValidation,
  ensureBinanceContractKlineRelayInitialized,
  failBinanceContractKlineRelayValidation,
  fetchBinancePublicRestRelayJson,
  getBinanceContractKlineRelayHealth,
  resetBinanceContractKlineRelayValidation,
  runWithBinanceContractKlineRelayValidation,
  startBinanceContractKlineRelayValidation,
} from './binance-contract-kline-relay.mjs';
import { getBinanceRestGuardHealth } from './binance-rest-guard.mjs';

if (process.env.KAKA_DISABLE_BINANCE_MARKET_START !== '1') {
  startBinanceContractMarket();
}

const SPOT_PROVIDER_LIST = Object.freeze([
  'binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate',
]);
const CONTRACT_PROVIDER_LIST = Object.freeze([
  'binance', 'okx', 'bybit', 'bitget', 'gate',
]);
const PROVIDERS = new Set(SPOT_PROVIDER_LIST);
const CONTRACT_PROVIDERS = new Set(CONTRACT_PROVIDER_LIST);
const COINBASE_BASE_URL = 'https://api.exchange.coinbase.com';
const BYBIT_PUBLIC_REST_HOSTS = [
  'https://api.bybit.com',
  'https://api.bytick.com',
];
const MARKET_UNIVERSE_CACHE_TTL_MS = 5 * 60_000;
const MARKET_TICKER_CACHE_TTL_MS = 10_000;
const marketUniverseCache = new Map();
const marketTickerCache = new Map();
const marketCacheInflight = new Map();
const coinbaseTickerCache = new Map();
const COINBASE_TICKER_TTL_MS = 5_000;
const BINANCE_SHARED_CACHE_MAX = 256;
const binanceSharedCache = new Map();
const binanceSharedInflight = new Map();
const binanceMarketRestStats = {
  cache_hits: 0,
  inflight_hits: 0,
  cache_misses: 0,
  cache_evictions: 0,
};

function pruneBinanceSharedCache() {
  const now = Date.now();
  for (const [key, entry] of binanceSharedCache.entries()) {
    if (Number(entry?.expiresAt || 0) <= now) binanceSharedCache.delete(key);
  }
  while (binanceSharedCache.size > BINANCE_SHARED_CACHE_MAX) {
    const oldest = binanceSharedCache.keys().next().value;
    if (oldest == null) break;
    binanceSharedCache.delete(oldest);
    binanceMarketRestStats.cache_evictions += 1;
  }
}

async function sharedBinanceResult(key, ttlMs, loader) {
  pruneBinanceSharedCache();
  const cached = binanceSharedCache.get(key);
  if (cached && cached.expiresAt > Date.now()) {
    binanceMarketRestStats.cache_hits += 1;
    return cached.value;
  }
  const running = binanceSharedInflight.get(key);
  if (running) {
    binanceMarketRestStats.inflight_hits += 1;
    return await running;
  }
  binanceMarketRestStats.cache_misses += 1;
  const task = Promise.resolve().then(loader);
  binanceSharedInflight.set(key, task);
  try {
    const value = await task;
    binanceSharedCache.set(key, {
      value,
      expiresAt: Date.now() + Math.max(500, Number(ttlMs) || 0),
    });
    pruneBinanceSharedCache();
    return value;
  } finally {
    if (binanceSharedInflight.get(key) === task) binanceSharedInflight.delete(key);
  }
}

async function sharedMarketResult(key, ttlMs, loader) {
  const now = Date.now();
  const cached = marketTickerCache.get(key) || marketUniverseCache.get(key);
  if (cached && cached.expiresAt > now) return cached.value;
  const running = marketCacheInflight.get(key);
  if (running) return await running;
  const task = Promise.resolve().then(loader);
  marketCacheInflight.set(key, task);
  try {
    const value = await task;
    const target = key.startsWith('universe:') ? marketUniverseCache : marketTickerCache;
    target.set(key, { value, expiresAt: Date.now() + Math.max(1000, Number(ttlMs) || 0) });
    return value;
  } finally {
    if (marketCacheInflight.get(key) === task) marketCacheInflight.delete(key);
  }
}

function providerKey(raw) {
  const value = String(raw || '').trim().toLowerCase().replaceAll('gate.io', 'gate');
  if (value === 'okex') return 'okx';
  return PROVIDERS.has(value) ? value : null;
}
function marketKey(raw) {
  const value = String(raw || '').trim().toLowerCase();
  return /contract|future|perpetual|swap|linear/.test(value) ? 'contract' : 'spot';
}
function assertProviderMarket(provider, market) {
  if (market === 'contract' && !CONTRACT_PROVIDERS.has(provider)) {
    throw new Error(`${provider} contract market is not supported`);
  }
}
function compact(raw) {
  return String(raw || '')
    .trim()
    .toUpperCase()
    .replace(/-SWAP$/i, '')
    .replace(/_UMCBL$/i, '')
    .replace(/[^A-Z0-9]/g, '');
}
function split(symbol) {
  for (const quote of ['FDUSD', 'USDT', 'USDC', 'USD1', 'USD', 'BTC', 'BNB', 'ETH', 'EUR', 'GBP', 'JPY', 'TRY', 'BRL', 'AUD', 'CAD']) {
    if (symbol.endsWith(quote)) return [symbol.slice(0, -quote.length), quote];
  }
  return [symbol, 'USDT'];
}
const SUPPORTED_EXACT_QUOTE_ASSETS = Object.freeze([
  'FDUSD', 'USDT', 'USDC', 'USD1', 'USD',
  'BTC', 'BNB', 'ETH', 'EUR', 'GBP', 'JPY',
  'TRY', 'BRL', 'AUD', 'CAD',
]);

function normalizedQuote(raw, fallback = 'USDT') {
  const value = String(raw || '').trim().toUpperCase();
  return SUPPORTED_EXACT_QUOTE_ASSETS.includes(value) ? value : fallback;
}
function quoteFromSymbols(symbols = [], fallback = 'USDT') {
  for (const symbol of symbols) {
    const [, quote] = split(compact(symbol));
    if (quote) return normalizedQuote(quote, fallback);
  }
  return normalizedQuote(fallback, 'USDT');
}
const CONTRACT_QUOTES_BY_PROVIDER = Object.freeze({
  binance: Object.freeze(['USDT', 'USDC']),
  okx: Object.freeze(['USDT', 'USDC', 'USD']),
  bybit: Object.freeze(['USDT', 'USDC', 'USD']),
  bitget: Object.freeze(['USDT', 'USDC', 'USD']),
  gate: Object.freeze(['USDT', 'USD']),
});

function contractQuoteSupported(provider, quote) {
  return (CONTRACT_QUOTES_BY_PROVIDER[provider] || []).includes(
    normalizedQuote(quote, 'USDT'),
  );
}
function bybitContractCategory(quote) {
  return normalizedQuote(quote, 'USDT') === 'USD' ? 'inverse' : 'linear';
}
function bitgetContractCategory(quote) {
  const safe = normalizedQuote(quote, 'USDT');
  if (safe === 'USDC') return 'USDC-FUTURES';
  if (safe === 'USD') return 'COIN-FUTURES';
  return 'USDT-FUTURES';
}
function gateContractSettle(quote) {
  return normalizedQuote(quote, 'USDT') === 'USD' ? 'btc' : 'usdt';
}
function contractDisplayIdentity(provider, base, requestedQuote, rawSymbol = '') {
  const quote = normalizedQuote(requestedQuote, 'USDT');
  if (!contractQuoteSupported(provider, quote)) return null;
  const safeBase = compact(base);
  if (!safeBase) return null;
  return {
    symbol: `${safeBase}${quote}`,
    base_asset: safeBase,
    quote_asset: quote,
    raw_symbol: String(rawSymbol || `${safeBase}${quote}`),
  };
}
function payloadRows(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload?.data?.list)) return payload.data.list;
  if (Array.isArray(payload?.result?.list)) return payload.result.list;
  return [];
}
function identityMap(rows = []) {
  const byDisplay = new Map();
  const byNative = new Map();
  for (const row of rows) {
    const display = compact(row?.symbol);
    const native = compact(row?.native_symbol || row?.raw_symbol || row?.symbol);
    if (display) byDisplay.set(display, row);
    if (native) byNative.set(native, row);
  }
  return { byDisplay, byNative };
}
function coinbaseProductId(symbol) {
  const [base, quote] = split(compact(symbol));
  return `${base}-${quote}`;
}
function okxId(symbol, market) {
  const [base, quote] = split(symbol);
  return `${base}-${quote}${market === 'contract' ? '-SWAP' : ''}`;
}
function gateId(symbol) {
  const [base, quote] = split(symbol);
  return `${base}_${quote}`;
}
function num(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}
function clamp(value, min, max, fallback) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(parsed) ? Math.max(min, Math.min(max, parsed)) : fallback;
}
function intervalMs(interval) {
  return ({
    '1s': 1_000,
    '1m': 60_000,
    '3m': 180_000,
    '5m': 300_000,
    '15m': 900_000,
    '30m': 1_800_000,
    '1h': 3_600_000,
    '2h': 7_200_000,
    '4h': 14_400_000,
    '6h': 21_600_000,
    '8h': 28_800_000,
    '12h': 43_200_000,
    '1d': 86_400_000,
    '3d': 259_200_000,
    '1w': 604_800_000,
    '1M': 2_592_000_000,
  })[interval] || 900_000;
}
function klineCoverage(rows, interval, endMs) {
  const sorted = [...new Map((Array.isArray(rows) ? rows : []).map((row) => [Number(row?.open_time_ms), row])).values()]
    .filter((row) => Number.isFinite(Number(row?.open_time_ms)))
    .sort((a, b) => Number(a.open_time_ms) - Number(b.open_time_ms));
  const step = intervalMs(interval);
  if (!sorted.length) {
    return {
      row_count: 0,
      first_open_time: null,
      last_open_time: null,
      gap_count: 0,
      missing_intervals: 0,
      lag_intervals_to_end: null,
      continuous_to_current: false,
    };
  }
  let gapCount = 0;
  let missingIntervals = 0;
  for (let index = 1; index < sorted.length; index += 1) {
    const difference = Number(sorted[index].open_time_ms) - Number(sorted[index - 1].open_time_ms);
    if (difference > step) {
      gapCount += 1;
      missingIntervals += Math.max(0, Math.round(difference / step) - 1);
    }
  }
  const lastOpenMs = Number(sorted.at(-1).open_time_ms);
  const targetOpenMs = Math.floor(Math.max(0, Number(endMs || Date.now()) - 1) / step) * step;
  const lagIntervals = Math.max(0, Math.round((targetOpenMs - lastOpenMs) / step));
  return {
    row_count: sorted.length,
    first_open_time: sorted[0].open_time || new Date(Number(sorted[0].open_time_ms)).toISOString(),
    last_open_time: sorted.at(-1).open_time || new Date(lastOpenMs).toISOString(),
    gap_count: gapCount,
    missing_intervals: missingIntervals,
    lag_intervals_to_end: lagIntervals,
    continuous_to_current: gapCount === 0 && lagIntervals <= 1,
  };
}

function okxBar(interval) {
  return ({
    '1m':'1m','3m':'3m','5m':'5m','15m':'15m','30m':'30m',
    '1h':'1H','2h':'2H','4h':'4H','6h':'6H','12h':'12H',
    '1d':'1Dutc','3d':'3Dutc','1w':'1Wutc','1M':'1Mutc',
  })[interval] || null;
}
function gateBar(interval, market) {
  const spot = {
    '1s':'1s','1m':'1m','5m':'5m','15m':'15m','30m':'30m','1h':'1h',
    '4h':'4h','8h':'8h','1d':'1d','1w':'7d','1M':'30d',
  };
  const contract = {
    '1m':'1m','5m':'5m','15m':'15m','30m':'30m','1h':'1h',
    '4h':'4h','8h':'8h','1d':'1d','1w':'7d',
  };
  return (market === 'contract' ? contract : spot)[interval] || null;
}
function bitgetBar(interval, market) {
  if (market === 'spot') {
    return ({
      '1m':'1min','3m':'3min','5m':'5min','15m':'15min','30m':'30min',
      '1h':'1h','4h':'4h','6h':'6h','12h':'12h',
      '1d':'1day','3d':'3day','1w':'1week','1M':'1M',
    })[interval] || null;
  }
  return ({
    '1m':'1m','3m':'3m','5m':'5m','15m':'15m','30m':'30m',
    '1h':'1H','4h':'4H','6h':'6H','12h':'12H',
    '1d':'1D','3d':'3D','1w':'1W','1M':'1M',
  })[interval] || null;
}
function bybitBar(interval) {
  return ({
    '1m':'1','3m':'3','5m':'5','15m':'15','30m':'30',
    '1h':'60','2h':'120','4h':'240','6h':'360','12h':'720',
    '1d':'D','1w':'W','1M':'M',
  })[interval] || null;
}
function sourceIntervalFor(provider, market, interval) {
  const fallback = {
    okx: { '8h':'4h' },
    bitget: { '2h':'1h', '8h':'4h' },
    bybit: { '8h':'4h', '3d':'1d' },
  };
  if (provider === 'gate') {
    const gateFallback = market === 'contract'
      ? { '3m':'1m', '2h':'1h', '6h':'1h', '12h':'4h', '3d':'1d', '1M':'1d' }
      : { '3m':'1m', '2h':'1h', '6h':'1h', '12h':'4h', '3d':'1d' };
    return gateFallback[interval] || interval;
  }
  return fallback[provider]?.[interval] || interval;
}
function coinbaseSourceGranularity(interval) {
  const targetSeconds = Math.max(60, Math.floor(intervalMs(interval) / 1000));
  const supported = [86_400, 21_600, 3_600, 900, 300, 60];
  for (const candidate of supported) {
    if (targetSeconds >= candidate && targetSeconds % candidate === 0) return candidate;
  }
  return 60;
}
async function jsonFetch(urls, timeout = 15_000) {
  const candidates = Array.isArray(urls) ? urls : [urls];
  let lastError;
  for (const url of candidates) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeout);
    try {
      const response = await fetch(url, {
        signal: controller.signal,
        headers: {
          accept: 'application/json',
          'user-agent': 'KakaWeb3-Market-Worker/515.1.2',
        },
      });
      const bodyText = await response.text();
      if (!response.ok) {
        const endpoint = (() => {
          try {
            const parsed = new URL(url);
            return `${parsed.host}${parsed.pathname}`;
          } catch (_) {
            return 'market-upstream';
          }
        })();
        throw new Error(`${response.status} ${response.statusText} ${endpoint} ${bodyText.slice(0, 240)}`.trim());
      }
      if (!bodyText) return null;
      try {
        return JSON.parse(bodyText);
      } catch (_) {
        throw new Error(`invalid JSON from market upstream: ${bodyText.slice(0, 240)}`);
      }
    } catch (error) {
      lastError = error;
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastError || new Error('upstream unavailable');
}
async function bybitPublicJson(pathAndQuery, { requireRows = false, timeout = 12_000 } = {}) {
  let lastError = null;
  for (const host of BYBIT_PUBLIC_REST_HOSTS) {
    try {
      const payload = await jsonFetch(`${host}${pathAndQuery}`, timeout);
      if (Number(payload?.retCode ?? 0) !== 0) {
        lastError = new Error(`bybit retCode=${payload?.retCode} retMsg=${payload?.retMsg || ''} host=${host}`);
        continue;
      }
      const rows = payloadRows(payload);
      if (requireRows && rows.length === 0) {
        lastError = new Error(`bybit empty result host=${host}`);
        continue;
      }
      return payload;
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError || new Error('Bybit official public API unavailable');
}

async function binanceRestJsonFetch(url, timeout = 15_000, source = 'legacy_market_rest') {
  // Step650.8.15.8: Binance Spot and Contract public HTTP both use the same
  // authenticated Edge relay and durable queue. Render direct REST is disabled.
  void timeout;
  return await fetchBinancePublicRestRelayJson(url, { source });
}


function send(res, status, body) {
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'access-control-allow-origin': '*',
    'cache-control': 'no-store',
  });
  res.end(JSON.stringify(body));
}
function marketRow(provider, market, symbol, base, quote, raw, extra = {}) {
  const displaySymbol = compact(symbol);
  const nativeSymbol = compact(extra.native_symbol || raw || symbol);
  return {
    provider,
    market_type: market,
    symbol: displaySymbol,
    raw_symbol: String(raw || nativeSymbol),
    native_symbol: nativeSymbol,
    base_asset: String(base).toUpperCase(),
    quote_asset: String(quote).toUpperCase(),
    settle_asset: String(extra.settle_asset || quote || '').toUpperCase(),
    contract_type: extra.contract_type || null,
    contract_multiplier: num(extra.contract_multiplier),
    contract_value_currency: String(extra.contract_value_currency || '').toUpperCase() || null,
    quantity_semantics: extra.quantity_semantics || (market === 'contract' ? 'base_asset' : 'base_asset'),
    status: 'TRADING',
    active: true,
    source: `${provider}_official_public_market_render`,
  };
}

async function fetchUniverse(
  provider,
  market,
  requestedQuote = 'USDT',
  filterQuote = true,
) {
  const quoteFilter = normalizedQuote(
    requestedQuote,
    provider === 'coinbase' ? 'USD' : 'USDT',
  );
  assertProviderMarket(provider, market);
  if (market === 'contract' &&
      !contractQuoteSupported(provider, quoteFilter)) {
    return [];
  }

  const rows = [];
  if (provider === 'binance') {
    if (market === 'contract') {
      const snapshotRows =
          await getBinanceContractUniverse({ quote: quoteFilter });
      if (!snapshotRows.length) return [];
      rows.push(...snapshotRows.map((row) => ({
        ...row,
        raw_symbol: row.raw_symbol || row.symbol,
        native_symbol: compact(
          row.native_symbol || row.raw_symbol || row.symbol,
        ),
      })));
    } else {
      const payload = await sharedBinanceResult(
        'spot_universe:exchange_info',
        6 * 60 * 60_000,
        () => binanceRestJsonFetch(
          'https://data-api.binance.vision/api/v3/exchangeInfo',
          15_000,
          'spot_universe:exchange_info',
        ),
      );
      for (const item of payload.symbols || []) {
        if (String(item.status).toUpperCase() !== 'TRADING') continue;
        rows.push(marketRow(
          provider,
          market,
          item.symbol,
          item.baseAsset,
          item.quoteAsset,
          item.symbol,
        ));
      }
    }
  } else if (provider === 'coinbase') {
    const payload = await jsonFetch(`${COINBASE_BASE_URL}/products`);
    for (const item of Array.isArray(payload) ? payload : []) {
      if (String(item.status || '').toLowerCase() !== 'online') continue;
      if (item.trading_disabled === true || item.cancel_only === true) {
        continue;
      }
      const raw = String(item.id || '').toUpperCase();
      const base = String(item.base_currency || '').toUpperCase();
      const quote = String(item.quote_currency || '').toUpperCase();
      if (!raw || !base || !quote) continue;
      rows.push(marketRow(
        provider,
        'spot',
        raw,
        base,
        quote,
        raw,
      ));
    }
  } else if (provider === 'okx') {
    const payload = await jsonFetch(
      `https://www.okx.com/api/v5/public/instruments?instType=${market === 'contract' ? 'SWAP' : 'SPOT'}`,
    );
    for (const item of payload.data || []) {
      if (item.state && item.state !== 'live') continue;
      if (market === 'contract') {
        const parts = String(item.instId || '').toUpperCase().split('-');
        const base = parts[0] || '';
        const quote = parts[1] || '';
        if (!base || !quote) continue;
        if (quoteFilter === 'USD' &&
            String(item.ctType || '').toLowerCase() !== 'inverse') {
          continue;
        }
        if (quoteFilter !== 'USD' &&
            String(item.ctType || '').toLowerCase() !== 'linear') {
          continue;
        }
        const ctVal = num(item.ctVal);
        const ctMult = num(item.ctMult) ?? 1;
        const valueCurrency =
            String(item.ctValCcy || '').toUpperCase();
        const contractValue =
            ctVal !== null && ctVal > 0 && ctMult > 0
                ? ctVal * ctMult
                : null;
        const baseMultiplier =
            contractValue !== null && valueCurrency === base
                ? contractValue
                : null;
        rows.push(marketRow(
          provider,
          market,
          `${base}${quote}`,
          base,
          quote,
          item.instId,
          {
            native_symbol: item.instId,
            settle_asset: item.settleCcy || quote,
            contract_type: item.ctType || null,
            contract_multiplier: baseMultiplier,
            contract_value: contractValue,
            contract_value_currency: valueCurrency,
            quantity_semantics: baseMultiplier
                ? 'contract_count_convertible_to_base'
                : contractValue !== null && valueCurrency === quote
                    ? 'contract_count_convertible_to_quote'
                    : 'contract_count',
          },
        ));
      } else {
        const base = item.baseCcy;
        const quote = item.quoteCcy;
        if (base && quote) {
          rows.push(marketRow(
            provider,
            market,
            item.instId,
            base,
            quote,
            item.instId,
          ));
        }
      }
    }
  } else if (provider === 'gate') {
    if (market === 'contract') {
      const settle = gateContractSettle(quoteFilter);
      const payload = await jsonFetch([
        `https://api.gateio.ws/api/v4/futures/${settle}/contracts`,
        `https://fx-api.gateio.ws/api/v4/futures/${settle}/contracts`,
      ]);
      for (const item of payload || []) {
        if (item.in_delisting === true ||
            String(item.status || 'trading').toLowerCase() !== 'trading') {
          continue;
        }
        const [base, nativeQuote = quoteFilter] =
            String(item.name || '').toUpperCase().split('_');
        const quote = nativeQuote || quoteFilter;
        if (!base || quote !== quoteFilter) continue;
        const multiplier = num(item.quanto_multiplier);
        rows.push(marketRow(
          provider,
          market,
          item.name,
          base,
          quote,
          item.name,
          {
            settle_asset: settle.toUpperCase(),
            contract_type: item.type || null,
            contract_multiplier:
                quote === 'USDT' && multiplier !== null && multiplier > 0
                    ? multiplier
                    : null,
            contract_value: multiplier,
            contract_value_currency:
                quote === 'USD' ? 'USD' : base,
            quantity_semantics: quote === 'USD'
                ? 'contract_count_inverse'
                : multiplier && multiplier > 0
                    ? 'contract_count_convertible_to_base'
                    : 'contract_count',
          },
        ));
      }
    } else {
      const payload = await jsonFetch(
        'https://api.gateio.ws/api/v4/spot/currency_pairs',
      );
      for (const item of payload || []) {
        if (String(item.trade_status || 'tradable').toLowerCase() !==
            'tradable') {
          continue;
        }
        const [base, quote] =
            String(item.id || '').toUpperCase().split('_');
        if (base && quote) {
          rows.push(marketRow(
            provider,
            market,
            item.id,
            item.base || base,
            item.quote || quote,
            item.id,
          ));
        }
      }
    }
  } else if (provider === 'bitget') {
    if (market === 'contract') {
      const category = bitgetContractCategory(quoteFilter);
      // Step658.2.3: Bitget当前公开合约目录以官方v2 contracts为准。
      // 不再让字段体系不同的v3响应在“非空”时提前截断v2回退。
      const urls = [
        `https://api.bitget.com/api/v2/mix/market/contracts?productType=${encodeURIComponent(category.toLowerCase())}`,
        `https://api.bitget.com/api/v2/mix/market/contracts?productType=${encodeURIComponent(category)}`,
      ];
      let lastError = null;
      let items = [];
      for (const url of urls) {
        try {
          const payload = await jsonFetch(url);
          if (String(payload?.code || '00000') !== '00000') {
            lastError = new Error(
              `bitget instruments code=${payload?.code} msg=${payload?.msg || ''}`,
            );
            continue;
          }
          items = payloadRows(payload);
          if (items.length) break;
        } catch (error) {
          lastError = error;
        }
      }
      if (!items.length && lastError) throw lastError;
      for (const item of items) {
        const status =
            String(item.symbolStatus || item.status || item.state || '')
                .toLowerCase();
        if (status &&
            !['normal', 'online', 'listed', 'live', 'trading']
                .includes(status)) {
          continue;
        }
        const type =
            String(item.symbolType || item.contractType || '')
                .toLowerCase();
        if (type && !type.includes('perpetual')) continue;
        const raw =
            String(item.symbol || item.symbolName || '').toUpperCase();
        const base =
            String(item.baseCoin || item.baseCurrency || '').toUpperCase();
        const quote =
            String(
              item.quoteCoin ||
                  item.quoteCurrency ||
                  (quoteFilter === 'USD' ? 'USD' : item.settleCoin) ||
                  quoteFilter,
            ).toUpperCase();
        if (!raw || !base || !quote || quote !== quoteFilter) continue;
        rows.push(marketRow(
          provider,
          market,
          `${base}${quote}`,
          base,
          quote,
          raw,
          {
            native_symbol: raw,
            settle_asset:
                item.settleCoin || item.marginCoin || quote,
            contract_type:
                item.symbolType || item.contractType || 'perpetual',
            quantity_semantics:
                quote === 'USD' ? 'provider_defined_coin_margined' : 'base_asset',
          },
        ));
      }
    } else {
      const payload = await jsonFetch(
        'https://api.bitget.com/api/v2/spot/public/symbols',
      );
      for (const item of payloadRows(payload)) {
        const status =
            String(item.symbolStatus || item.status || '').toLowerCase();
        if (status &&
            !['normal', 'online', 'listed'].includes(status)) {
          continue;
        }
        if (item.baseCoin && item.quoteCoin) {
          rows.push(marketRow(
            provider,
            market,
            item.symbol,
            item.baseCoin,
            item.quoteCoin,
            item.symbol,
          ));
        }
      }
    }
  } else if (provider === 'bybit') {
    const category =
        market === 'contract'
            ? bybitContractCategory(quoteFilter)
            : 'spot';
    let cursor = '';
    do {
      const contractFilter = market === 'contract'
          ? `&limit=1000${category === 'linear' && quoteFilter === 'USDC' ? '&settleCoin=USDC' : ''}${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`
          : '';
      const payload = await bybitPublicJson(
        `/v5/market/instruments-info?category=${category}${contractFilter}`,
        { requireRows: true },
      );
      const result = payload.result || {};
      for (const item of result.list || []) {
        if (String(item.status || 'Trading').toLowerCase() !== 'trading') {
          continue;
        }
        if (market === 'contract') {
          const expectedType =
              category === 'inverse'
                  ? 'inverseperpetual'
                  : 'linearperpetual';
          if (String(item.contractType || '').toLowerCase() !==
              expectedType) {
            continue;
          }
        }
        const raw = String(item.symbol || '').toUpperCase();
        const base = String(item.baseCoin || '').toUpperCase();
        const settle = String(item.settleCoin || '').toUpperCase();
        const nativeQuote = String(item.quoteCoin || '').toUpperCase();
        const displayQuote = market === 'contract'
            ? (category === 'inverse'
                ? 'USD'
                : (settle === 'USDC'
                    ? 'USDC'
                    : nativeQuote || settle || split(raw)[1]))
            : nativeQuote;
        if (!raw || !base || !displayQuote) continue;
        if (market === 'contract' && displayQuote !== quoteFilter) {
          continue;
        }
        rows.push(marketRow(
          provider,
          market,
          `${base}${displayQuote}`,
          base,
          displayQuote,
          raw,
          {
            native_symbol: raw,
            settle_asset: settle || displayQuote,
            contract_type: item.contractType || null,
            quantity_semantics:
                category === 'inverse'
                    ? 'quote_asset'
                    : 'base_asset',
          },
        ));
      }
      cursor = market === 'contract'
          ? String(result.nextPageCursor || '')
          : '';
    } while (cursor);
  }

  const deduped = [...new Map(
    rows.map((item) => [
      `${item.provider}:${item.market_type}:${item.symbol}`,
      item,
    ]),
  ).values()].sort((a, b) => a.symbol.localeCompare(b.symbol));
  return filterQuote
      ? deduped.filter((item) => item.quote_asset === quoteFilter)
      : deduped;
}

async function universeCatalog(provider, market) {
  assertProviderMarket(provider, market);
  return await sharedMarketResult(
    `universe_catalog:${provider}:${market}`,
    MARKET_UNIVERSE_CACHE_TTL_MS,
    async () => {
      if (market === 'spot') {
        return await fetchUniverse(
          provider,
          market,
          provider === 'coinbase' ? 'USD' : 'USDT',
          false,
        );
      }
      const quotes = CONTRACT_QUOTES_BY_PROVIDER[provider] || [];
      const groups = await Promise.all(
        quotes.map(async (quote) => {
          try {
            return await fetchUniverse(provider, market, quote, true);
          } catch (_) {
            return [];
          }
        }),
      );
      return [...new Map(
        groups.flat().map((row) => [
          `${row.provider}:${row.market_type}:${row.symbol}`,
          row,
        ]),
      ).values()].sort((a, b) => a.symbol.localeCompare(b.symbol));
    },
  );
}

async function universe(provider, market, requestedQuote = 'USDT') {
  const quote = normalizedQuote(
    requestedQuote,
    provider === 'coinbase' ? 'USD' : 'USDT',
  );
  if (market === 'contract' &&
      !contractQuoteSupported(provider, quote)) {
    return [];
  }
  const catalog = await universeCatalog(provider, market);
  return catalog.filter((row) => row.quote_asset === quote);
}

function tickerVolumeSemantics(provider, market, item, last, quote = 'USDT') {
  let baseVolume = null;
  let quoteVolume = null;
  let contractCount = null;
  let quoteVolumeUsd = null;
  let baseSource = null;
  let quoteSource = null;
  const safeQuote = normalizedQuote(quote, 'USDT');

  if (provider === 'binance') {
    baseVolume = num(item.base_volume_24h ?? item.volume_24h ?? item.volume);
    quoteVolume = num(item.quote_volume_24h ?? item.quoteVolume);
    contractCount = num(item.contract_count_24h);
    baseSource = item.base_volume_24h != null || item.volume_24h != null
      ? 'canonical_base_asset'
      : 'binance_volume';
    quoteSource = item.quote_volume_24h != null
      ? 'canonical_quote_asset'
      : 'binance_quoteVolume';
  } else if (provider === 'coinbase') {
    baseVolume = num(item.base_volume_24h ?? item.volume);
    quoteVolume = num(item.quote_volume_24h);
    baseSource = 'coinbase_base_volume';
    quoteSource = 'coinbase_quote_turnover';
  } else if (provider === 'bybit') {
    const inverse = market === 'contract' && safeQuote === 'USD';
    if (inverse) {
      // Bybit inverse: volume24h is quote/USD volume and turnover24h is
      // base-coin turnover. Linear contracts use the opposite unit order.
      baseVolume = num(item.base_volume_24h ?? item.turnover24h);
      quoteVolume = num(item.quote_volume_24h ?? item.volume24h);
      baseSource = 'bybit_inverse_turnover24h_base';
      quoteSource = 'bybit_inverse_volume24h_quote';
    } else {
      baseVolume = num(item.base_volume_24h ?? item.volume24h);
      quoteVolume = num(item.quote_volume_24h ?? item.turnover24h);
      baseSource = 'bybit_linear_or_spot_volume24h_base';
      quoteSource = 'bybit_linear_or_spot_turnover24h_quote';
    }
  } else if (provider === 'bitget') {
    baseVolume = num(item.base_volume_24h ?? item.baseVolume);
    quoteVolume = num(item.quote_volume_24h ?? item.quoteVolume);
    quoteVolumeUsd = num(item.usdtVolume);
    baseSource = 'bitget_baseVolume';
    quoteSource = 'bitget_quoteVolume';
  } else if (provider === 'okx') {
    if (market === 'contract') {
      // OKX derivatives: vol24h is number of contracts; volCcy24h is
      // underlying/base currency; volCcyQuote24h is quote turnover.
      contractCount = num(item.vol24h);
      baseVolume = num(item.base_volume_24h ?? item.volCcy24h);
      quoteVolume = num(
        item.quote_volume_24h ??
        item.volCcyQuote24h ??
        item.volCcyQuote
      );
      if (quoteVolume === null && baseVolume !== null && last !== null) {
        quoteVolume = baseVolume * last;
        quoteSource = 'okx_base_times_last_fallback';
      } else {
        quoteSource = 'okx_volCcyQuote24h';
      }
      baseSource = 'okx_volCcy24h';
    } else {
      // OKX spot: vol24h is base quantity and volCcy24h is quote turnover.
      baseVolume = num(item.base_volume_24h ?? item.vol24h);
      quoteVolume = num(
        item.quote_volume_24h ??
        item.volCcyQuote24h ??
        item.volCcy24h
      );
      baseSource = 'okx_spot_vol24h';
      quoteSource = item.volCcyQuote24h != null
        ? 'okx_spot_volCcyQuote24h'
        : 'okx_spot_volCcy24h';
    }
  } else if (provider === 'gate') {
    if (market === 'contract') {
      contractCount = num(item.volume_24h ?? item.volume);
      baseVolume = num(item.base_volume_24h ?? item.volume_24h_base);
      quoteVolume = num(item.quote_volume_24h ?? item.volume_24h_quote);
      quoteVolumeUsd = num(item.volume_24h_usd);
      baseSource = 'gate_volume_24h_base';
      quoteSource = 'gate_volume_24h_quote';
    } else {
      baseVolume = num(item.base_volume_24h ?? item.base_volume);
      quoteVolume = num(item.quote_volume_24h ?? item.quote_volume);
      baseSource = 'gate_spot_base_volume';
      quoteSource = 'gate_spot_quote_volume';
    }
  }

  // Canonical fallback is accepted only when the field name itself states
  // the unit. Raw "volume" or contract "vol24h" is never guessed here.
  baseVolume ??= num(item.base_volume_24h);
  quoteVolume ??= num(item.quote_volume_24h);
  contractCount ??= num(item.contract_count_24h);

  return {
    base_volume_24h: baseVolume,
    quote_volume_24h: quoteVolume,
    quote_volume_usd_24h: quoteVolumeUsd,
    contract_count_24h: contractCount,
    base_volume_unit: baseVolume === null ? null : 'base_asset',
    quote_volume_unit: quoteVolume === null ? null : 'quote_asset',
    contract_count_unit: contractCount === null ? null : 'contracts',
    base_volume_source: baseSource,
    quote_volume_source: quoteSource,
    quantity_semantics: baseVolume !== null
      ? 'base_asset'
      : contractCount !== null
        ? 'contract_count'
        : 'unavailable',
  };
}

function tickerOpenInterestSemantics(provider, market, quote, item, last) {
  let amount = num(item.openInterest ?? item.holdingAmount ?? item.open_interest);
  let value = num(item.openInterestValue ?? item.open_interest_value);
  let amountUnit = item.open_interest_unit || null;
  let valueUnit = item.open_interest_value_unit || null;
  const safeQuote = normalizedQuote(quote, 'USDT');

  if (market !== 'contract') {
    return {
      open_interest: amount,
      open_interest_value: value,
      open_interest_unit: amountUnit,
      open_interest_value_unit: valueUnit,
    };
  }

  if (provider === 'bybit') {
    if (safeQuote === 'USD') {
      // Bybit inverse: openInterest is USD/quote amount and
      // openInterestValue is the converted base-coin amount.
      amountUnit = 'quote_asset';
      valueUnit = 'base_asset';
      if (value === null && amount !== null && last !== null && last > 0) {
        value = amount / last;
      }
    } else {
      amountUnit = 'base_asset';
      valueUnit = 'quote_asset';
      if (value === null && amount !== null && last !== null && last > 0) {
        value = amount * last;
      }
    }
  } else if (provider === 'bitget') {
    // Bitget holdingAmount is documented as base-coin position size.
    amountUnit = 'base_asset';
    valueUnit = 'quote_asset';
    if (value === null && amount !== null && last !== null && last > 0) {
      value = amount * last;
    }
  } else if (amount !== null || value !== null) {
    // Binance, OKX and Gate normalized ticker/meta paths expose base amount
    // plus quote/USD value. Explicit upstream unit fields still take priority.
    amountUnit ||= 'base_asset';
    valueUnit ||= 'quote_asset';
  }

  return {
    open_interest: amount,
    open_interest_value: value,
    open_interest_unit: amount === null ? null : amountUnit,
    open_interest_value_unit: value === null ? null : valueUnit,
  };
}

function tickerRow(provider, market, item, rawSymbol, displaySymbol = null) {
  const nativeSymbol = compact(rawSymbol);
  const symbol = compact(displaySymbol || nativeSymbol);
  if (!symbol || !nativeSymbol) return null;
  const last = num(item.last_price ?? item.lastPrice ?? item.last ?? item.close ?? item.lastPr);
  const open = num(item.open_24h ?? item.openPrice ?? item.open ?? item.open24h ?? item.prevPrice24h);
  let percent = num(
    item.price_change_percent_24h ??
    item.priceChangePercent ??
    item.change_percentage ??
    item.change24h ??
    item.price24hPcnt
  );
  if (percent !== null &&
      (provider === 'bitget' || provider === 'bybit') &&
      Math.abs(percent) <= 2) {
    percent *= 100;
  }
  if (percent === null && last !== null && open) {
    percent = ((last - open) / open) * 100;
  }

  const [, quote] = split(symbol);
  const volumes = tickerVolumeSemantics(provider, market, item, last, quote);
  const openInterest = tickerOpenInterestSemantics(
    provider,
    market,
    quote,
    item,
    last,
  );
  return {
    provider,
    market_type: market,
    symbol,
    raw_symbol: nativeSymbol,
    native_symbol: nativeSymbol,
    last_price: last,
    price: last,
    price_change_percent_24h: percent,
    volume_24h: volumes.base_volume_24h,
    base_volume_24h: volumes.base_volume_24h,
    quote_volume_24h: volumes.quote_volume_24h,
    quote_volume_usd_24h: volumes.quote_volume_usd_24h,
    contract_count_24h: volumes.contract_count_24h,
    base_volume_unit: volumes.base_volume_unit,
    quote_volume_unit: volumes.quote_volume_unit,
    contract_count_unit: volumes.contract_count_unit,
    base_volume_source: volumes.base_volume_source,
    quote_volume_source: volumes.quote_volume_source,
    quantity_semantics: volumes.quantity_semantics,
    high_24h: num(item.high_24h ?? item.highPrice ?? item.high24h ?? item.highPrice24h),
    low_24h: num(item.low_24h ?? item.lowPrice ?? item.low24h ?? item.lowPrice24h),
    funding_rate: num(item.fundingRate),
    open_interest: openInterest.open_interest,
    open_interest_value: openInterest.open_interest_value,
    open_interest_unit: openInterest.open_interest_unit,
    open_interest_value_unit: openInterest.open_interest_value_unit,
    source: `${provider}_official_public_ticker_render`,
    cached_at: new Date().toISOString(),
  };
}

async function mapLimit(values, concurrency, mapper) {
  const results = new Array(values.length);
  let nextIndex = 0;
  async function worker() {
    while (true) {
      const index = nextIndex++;
      if (index >= values.length) return;
      results[index] = await mapper(values[index], index);
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, values.length) }, () => worker()));
  return results;
}

async function coinbaseTicker(symbol) {
  const normalized = compact(symbol);
  const cacheKey = normalized;
  const cached = coinbaseTickerCache.get(cacheKey);
  if (cached && Date.now() - cached.at < COINBASE_TICKER_TTL_MS) return cached.row;
  const productId = coinbaseProductId(normalized);
  const stats = await jsonFetch(`${COINBASE_BASE_URL}/products/${encodeURIComponent(productId)}/stats`);
  const last = num(stats.last);
  const open = num(stats.open);
  const baseVolume = num(stats.volume);
  const row = tickerRow('coinbase', 'spot', {
    last,
    open,
    high_24h: stats.high,
    low_24h: stats.low,
    base_volume_24h: baseVolume,
    quote_volume_24h: last !== null && baseVolume !== null ? last * baseVolume : null,
  }, normalized);
  if (!row) throw new Error(`Coinbase ticker unavailable for ${productId}`);
  coinbaseTickerCache.set(cacheKey, { at: Date.now(), row });
  return row;
}

async function tickers(provider, market, wantedSymbols = []) {
  assertProviderMarket(provider, market);
  const wanted = [...new Set(wantedSymbols.map(compact).filter(Boolean))].slice(0, 120);
  const requestedQuote = quoteFromSymbols(wanted, provider === 'coinbase' ? 'USD' : 'USDT');

  if (provider === 'coinbase') {
    if (!wanted.length) return [];
    let lastError = null;
    const rows = await mapLimit(wanted.slice(0, 48), 5, async (symbol) => {
      try {
        return await coinbaseTicker(symbol);
      } catch (error) {
        lastError = error;
        return null;
      }
    });
    const validRows = rows.filter(Boolean);
    if (!validRows.length && lastError) throw lastError;
    return validRows;
  }

  if (provider === 'binance') {
    if (market === 'contract') {
      return getBinanceContractTickers({ symbols: wanted });
    }
    const payload = await sharedBinanceResult(
      'spot_ticker:24hr_all',
      10_000,
      () => binanceRestJsonFetch(
        'https://data-api.binance.vision/api/v3/ticker/24hr',
        15_000,
        'spot_ticker:24hr_all',
      ),
    );
    return (Array.isArray(payload) ? payload : [])
      .map((item) => tickerRow(provider, market, item, item.symbol))
      .filter(Boolean);
  }

  if (provider === 'bybit' && market === 'contract') {
    const identities = await universe(provider, market, requestedQuote);
    const maps = identityMap(identities);
    const payload = await sharedMarketResult(
      `ticker:${provider}:${market}:${requestedQuote}`,
      MARKET_TICKER_CACHE_TTL_MS,
      () => bybitPublicJson(
        `/v5/market/tickers?category=${bybitContractCategory(requestedQuote)}`,
        { requireRows: true },
      ),
    );
    const rows = [];
    for (const item of payloadRows(payload)) {
      const native = compact(item.symbol);
      const identity = maps.byNative.get(native);
      if (!identity) continue;
      if (wanted.length && !wanted.includes(identity.symbol)) continue;
      const row = tickerRow(provider, market, item, native, identity.symbol);
      if (row) rows.push({ ...row, settle_asset: identity.settle_asset, contract_type: identity.contract_type });
    }
    return rows;
  }

  if (provider === 'bitget' && market === 'contract') {
    const identities = await universe(provider, market, requestedQuote);
    const maps = identityMap(identities);
    const category = bitgetContractCategory(requestedQuote);
    const payload = await sharedMarketResult(
      `ticker:${provider}:${market}:${requestedQuote}`,
      MARKET_TICKER_CACHE_TTL_MS,
      async () => {
        // Step658.2.3: Ticker与目录使用同一套官方v2 productType身份。
        const urls = [
          `https://api.bitget.com/api/v2/mix/market/tickers?productType=${encodeURIComponent(category.toLowerCase())}`,
          `https://api.bitget.com/api/v2/mix/market/tickers?productType=${encodeURIComponent(category)}`,
        ];
        let lastError = null;
        for (const url of urls) {
          try {
            const result = await jsonFetch(url);
            if (String(result?.code || '00000') !== '00000') {
              lastError = new Error(`bitget tickers code=${result?.code} msg=${result?.msg || ''}`);
              continue;
            }
            if (payloadRows(result).length) return result;
          } catch (error) {
            lastError = error;
          }
        }
        throw lastError || new Error(`bitget ${category} ticker rows unavailable`);
      },
    );
    const rows = [];
    for (const item of payloadRows(payload)) {
      const native = compact(item.symbol || item.symbolName);
      const identity = maps.byNative.get(native) || maps.byDisplay.get(native);
      if (!identity) continue;
      if (wanted.length && !wanted.includes(identity.symbol)) continue;
      const row = tickerRow(provider, market, item, native, identity.symbol);
      if (row) rows.push({ ...row, settle_asset: identity.settle_asset, contract_type: identity.contract_type });
    }
    return rows;
  }

  let items = [];
  if (provider === 'okx') {
    const payload = await jsonFetch(
      `https://www.okx.com/api/v5/market/tickers?instType=${market === 'contract' ? 'SWAP' : 'SPOT'}`,
    );
    items = payload.data || [];
  } else if (provider === 'gate') {
    const settle = gateContractSettle(requestedQuote);
    const payload = await jsonFetch(
      market === 'contract'
        ? [
            `https://api.gateio.ws/api/v4/futures/${settle}/tickers`,
            `https://fx-api.gateio.ws/api/v4/futures/${settle}/tickers`,
          ]
        : 'https://api.gateio.ws/api/v4/spot/tickers',
    );
    items = Array.isArray(payload) ? payload : [];
  } else if (provider === 'bitget') {
    const payload = await jsonFetch('https://api.bitget.com/api/v2/spot/market/tickers');
    items = payloadRows(payload);
  } else if (provider === 'bybit') {
    const payload = await bybitPublicJson('/v5/market/tickers?category=spot', { requireRows: true });
    items = payloadRows(payload);
  }
  return items
    .map((item) => tickerRow(
      provider,
      market,
      item,
      item.symbol ?? item.instId ?? item.contract ?? item.currency_pair,
    ))
    .filter(Boolean)
    .filter((row) => split(row.symbol)[1] === requestedQuote)
    .filter((row) => !wanted.length || wanted.includes(row.symbol));
}

function krow(provider, market, symbol, interval, values) {
  const timestamp = num(values[0]);
  const open = num(values[1]);
  const high = num(values[2]);
  const low = num(values[3]);
  const close = num(values[4]);
  if ([timestamp, open, high, low, close].some((value) => value === null)) return null;
  return {
    provider,
    market_type: market,
    symbol,
    interval,
    open_time: new Date(timestamp).toISOString(),
    open_time_ms: timestamp,
    close_time: new Date(timestamp + intervalMs(interval) - 1).toISOString(),
    open,
    high,
    low,
    close,
    volume: num(values[5]) || 0,
    quote_volume: num(values[6]) || 0,
    trade_count: num(values[7]) || 0,
    source: `${provider}_official_public_kline_render`,
  };
}

function aggregateCandles(sourceRows, provider, market, symbol, interval) {
  const targetMs = intervalMs(interval);
  const buckets = new Map();
  const sorted = [...sourceRows].sort((a, b) => a.open_time_ms - b.open_time_ms);
  for (const source of sorted) {
    const bucketStart = Math.floor(source.open_time_ms / targetMs) * targetMs;
    const current = buckets.get(bucketStart);
    const sourceVolume = num(source.volume) || 0;
    const sourceQuote = num(source.quote_volume) || sourceVolume * (num(source.close) || 0);
    if (!current) {
      buckets.set(bucketStart, {
        provider,
        market_type: market,
        symbol,
        interval,
        open_time: new Date(bucketStart).toISOString(),
        open_time_ms: bucketStart,
        close_time: new Date(bucketStart + targetMs - 1).toISOString(),
        open: source.open,
        high: source.high,
        low: source.low,
        close: source.close,
        volume: sourceVolume,
        quote_volume: sourceQuote,
        trade_count: 0,
        source: `${provider}_official_public_kline_render`,
      });
    } else {
      current.high = Math.max(Number(current.high), Number(source.high));
      current.low = Math.min(Number(current.low), Number(source.low));
      current.close = source.close;
      current.volume = Number(current.volume) + sourceVolume;
      current.quote_volume = Number(current.quote_volume) + sourceQuote;
    }
  }
  return [...buckets.values()].sort((a, b) => a.open_time_ms - b.open_time_ms);
}


// Step650.8.15.28: calendar-month aggregation for safe Binance contract daily seeds.
// A month is not a fixed 30-day duration, so use UTC year/month boundaries and never
// interpolate or fabricate missing source candles.
function aggregateCalendarMonths(sourceRows, provider, market, symbol) {
  const buckets = new Map();
  const sorted = [...sourceRows].sort((a, b) => a.open_time_ms - b.open_time_ms);
  for (const source of sorted) {
    const time = Number(source.open_time_ms);
    if (!Number.isFinite(time)) continue;
    const date = new Date(time);
    const bucketStart = Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1);
    const nextMonth = Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 1);
    const sourceVolume = num(source.volume) || 0;
    const sourceQuote = num(source.quote_volume) || sourceVolume * (num(source.close) || 0);
    const current = buckets.get(bucketStart);
    if (!current) {
      buckets.set(bucketStart, {
        provider,
        market_type: market,
        symbol,
        interval: '1M',
        open_time: new Date(bucketStart).toISOString(),
        open_time_ms: bucketStart,
        close_time: new Date(nextMonth - 1).toISOString(),
        open: source.open,
        high: source.high,
        low: source.low,
        close: source.close,
        volume: sourceVolume,
        quote_volume: sourceQuote,
        trade_count: num(source.trade_count) || 0,
        source: 'binance_official_safe_daily_to_calendar_month_render',
      });
    } else {
      current.high = Math.max(Number(current.high), Number(source.high));
      current.low = Math.min(Number(current.low), Number(source.low));
      current.close = source.close;
      current.volume = Number(current.volume) + sourceVolume;
      current.quote_volume = Number(current.quote_volume) + sourceQuote;
      current.trade_count = Number(current.trade_count) + (num(source.trade_count) || 0);
    }
  }
  return [...buckets.values()].sort((a, b) => a.open_time_ms - b.open_time_ms);
}

async function coinbaseKlines(symbol, interval, end, limit) {
  const productId = coinbaseProductId(symbol);
  const sourceGranularity = coinbaseSourceGranularity(interval);
  const targetMs = intervalMs(interval);
  const factor = Math.max(1, Math.ceil(targetMs / (sourceGranularity * 1000)));
  const sourceNeeded = Math.min(5_000, limit * factor + factor * 4);
  const sourceRows = [];
  let pageEndMs = end;
  let remaining = sourceNeeded;
  let pages = 0;
  while (remaining > 0 && pages < 20) {
    const pageSize = Math.min(300, remaining);
    const pageStartMs = Math.max(0, pageEndMs - Math.max(0, pageSize - 1) * sourceGranularity * 1000);
    const url = `${COINBASE_BASE_URL}/products/${encodeURIComponent(productId)}/candles` +
      `?granularity=${sourceGranularity}&start=${encodeURIComponent(new Date(pageStartMs).toISOString())}` +
      `&end=${encodeURIComponent(new Date(pageEndMs).toISOString())}`;
    const payload = await jsonFetch(url, 20_000);
    const data = Array.isArray(payload) ? payload : [];
    if (!data.length) break;
    let oldestMs = pageEndMs;
    for (const candle of data) {
      if (!Array.isArray(candle) || candle.length < 6) continue;
      const timestamp = Number(candle[0]) * 1000;
      const volume = num(candle[5]) || 0;
      const close = num(candle[4]) || 0;
      const row = krow('coinbase', 'spot', symbol, `${sourceGranularity}s`, [
        timestamp,
        candle[3],
        candle[2],
        candle[1],
        candle[4],
        volume,
        volume * close,
        0,
      ]);
      if (row) {
        row.close_time = new Date(timestamp + sourceGranularity * 1000 - 1).toISOString();
        row.source = 'coinbase_official_public_kline_render';
        sourceRows.push(row);
        oldestMs = Math.min(oldestMs, timestamp);
      }
    }
    if (oldestMs >= pageEndMs) break;
    pageEndMs = oldestMs - 1;
    remaining -= data.length;
    pages += 1;
  }
  const dedupedSource = [...new Map(sourceRows.map((item) => [item.open_time_ms, item])).values()];
  return aggregateCandles(dedupedSource, 'coinbase', 'spot', symbol, interval).slice(-limit);
}

export async function resolveNativeMarketIdentity(provider, market, symbol) {
  const displaySymbol = compact(symbol);
  if (market !== 'contract' || !['bybit', 'bitget'].includes(provider)) {
    return {
      symbol: displaySymbol,
      native_symbol: displaySymbol,
      quote_asset: split(displaySymbol)[1],
    };
  }
  const [, quote] = split(displaySymbol);
  const identities = await universe(provider, market, quote);
  const maps = identityMap(identities);
  const identity = maps.byDisplay.get(displaySymbol) || maps.byNative.get(displaySymbol);
  if (!identity) {
    throw new Error(`${provider} native contract identity unavailable for ${displaySymbol}`);
  }
  return identity;
}

function alignedKlineEnd(end, interval) {
  const now = Date.now();
  const requested = Number(end);
  const safe = Number.isFinite(requested) && requested > 0 ? Math.min(requested, now) : now;
  const step = intervalMs(interval);
  return Math.floor(safe / step) * step;
}

async function fetchBitgetContractKlines({ displaySymbol, nativeSymbol, quote, interval, end, limit }) {
  const bar = bitgetBar(interval, 'contract');
  if (!bar) throw new Error(`bitget interval ${interval} requires aggregation`);
  const productType = bitgetContractCategory(quote).toLowerCase();
  const safeLimit = Math.max(1, Math.min(1000, Number(limit) || 1000));
  const nearCurrent = Number(end) >= Date.now() - intervalMs(interval) * 2;
  const alignedEnd = alignedKlineEnd(end, interval);
  const urls = [
    `https://api.bitget.com/api/v2/mix/market/candles?symbol=${encodeURIComponent(nativeSymbol)}` +
      `&productType=${encodeURIComponent(productType)}&granularity=${encodeURIComponent(bar)}` +
      `${nearCurrent ? '' : `&endTime=${alignedEnd}`}&limit=${safeLimit}`,
    `https://api.bitget.com/api/v2/mix/market/history-candles?symbol=${encodeURIComponent(nativeSymbol)}` +
      `&productType=${encodeURIComponent(productType)}&granularity=${encodeURIComponent(bar)}` +
      `&endTime=${alignedEnd}&limit=${Math.min(200, safeLimit)}`,
  ];
  let lastError = null;
  for (const url of urls) {
    try {
      const payload = await jsonFetch(url, 15_000);
      if (String(payload?.code || '00000') !== '00000') {
        lastError = new Error(`bitget kline code=${payload?.code} msg=${payload?.msg || ''}`);
        continue;
      }
      const data = payloadRows(payload);
      if (!data.length) {
        lastError = new Error(`bitget empty kline result symbol=${nativeSymbol} productType=${productType}`);
        continue;
      }
      const rows = data
        .map((a) => krow('bitget', 'contract', displaySymbol, interval, [a[0],a[1],a[2],a[3],a[4],a[5],a[6],0]))
        .filter(Boolean)
        .map((row) => ({
          ...row,
          raw_symbol: nativeSymbol,
          native_symbol: nativeSymbol,
          quote_asset: quote,
          source: 'bitget_official_usdc_contract_kline_render',
        }));
      if (rows.length) return rows;
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError || new Error(`bitget contract kline unavailable for ${displaySymbol}`);
}

async function fetchBybitContractKlines({
  displaySymbol,
  nativeSymbol,
  quote,
  interval,
  end,
  limit,
}) {
  const bar = bybitBar(interval);
  if (!bar) {
    throw new Error(`bybit interval ${interval} requires aggregation`);
  }
  const category = bybitContractCategory(quote);
  const safeLimit = Math.max(1, Math.min(1000, Number(limit) || 1000));
  const nearCurrent = Number(end) >= Date.now() - intervalMs(interval) * 2;
  const safeEnd = Math.min(Number(end) || Date.now(), Date.now());
  const query =
      `/v5/market/kline?category=${category}` +
      `&symbol=${encodeURIComponent(nativeSymbol)}` +
      `&interval=${encodeURIComponent(bar)}` +
      `&limit=${safeLimit}` +
      `${nearCurrent ? '' : `&end=${safeEnd}`}`;
  const payload = await bybitPublicJson(
    query,
    { requireRows: true, timeout: 15_000 },
  );
  const rows = payloadRows(payload)
    .map((a) => {
      const inverse = category === 'inverse';
      // Bybit official Kline units: linear volume=base, turnover=quote;
      // inverse volume=quote(USD), turnover=base.
      const baseVolume = inverse ? a[6] : a[5];
      const quoteVolume = inverse ? a[5] : a[6];
      const row = krow(
        'bybit',
        'contract',
        displaySymbol,
        interval,
        [a[0], a[1], a[2], a[3], a[4], baseVolume, quoteVolume, 0],
      );
      if (!row) return null;
      row.quantity_semantics = 'base_asset';
      row.base_volume_unit = 'base_asset';
      row.quote_volume_unit = 'quote_asset';
      return row;
    })
    .filter(Boolean)
    .map((row) => ({
      ...row,
      raw_symbol: nativeSymbol,
      native_symbol: nativeSymbol,
      quote_asset: quote,
      source: `bybit_official_${category}_contract_kline_render`,
    }));
  if (!rows.length) {
    throw new Error(`bybit empty kline result symbol=${nativeSymbol}`);
  }
  return rows;
}

async function fetchNativeMarketKlines(provider, market, symbol, interval, end, limit) {
  let rows = [];
  if (provider === 'binance') {
    const base = market === 'contract'
      ? 'https://fapi.binance.com/fapi/v1/klines'
      : 'https://data-api.binance.vision/api/v3/klines';
    const url = `${base}?symbol=${symbol}&interval=${encodeURIComponent(interval)}&endTime=${end}&limit=${Math.min(1500, limit)}`;
    const payload = market === 'contract'
      ? await binanceRestJsonFetch(url, 15_000, 'legacy_contract_kline')
      : await binanceRestJsonFetch(url, 15_000, 'spot_kline');
    rows = (payload || []).map((a) => krow(provider, market, symbol, interval, [a[0],a[1],a[2],a[3],a[4],a[5],a[7],a[8]])).filter(Boolean);
  } else if (provider === 'okx') {
    const bar = okxBar(interval);
    if (!bar) throw new Error(`okx interval ${interval} requires aggregation`);
    let after = end + 1;
    while (rows.length < limit) {
      const count = Math.min(300, limit - rows.length);
      const payload = await jsonFetch(
        `https://www.okx.com/api/v5/market/history-candles?instId=${encodeURIComponent(okxId(symbol, market))}` +
        `&bar=${encodeURIComponent(bar)}&after=${after}&limit=${count}`,
      );
      const data = payload.data || [];
      if (!data.length) break;
      let oldest = after;
      for (const a of data) {
        const rawContractsOrBase = num(a[5]);
        const baseVolume = market === 'contract' ? num(a[6]) : rawContractsOrBase;
        const quoteVolume = market === 'contract'
          ? num(a[7])
          : (num(a[7]) ?? num(a[6]));
        const row = krow(
          provider,
          market,
          symbol,
          interval,
          [a[0], a[1], a[2], a[3], a[4], baseVolume, quoteVolume, 0],
        );
        if (row) {
          if (market === 'contract') {
            row.contract_count = rawContractsOrBase;
            row.quantity_semantics = 'base_asset';
            row.base_volume_unit = 'base_asset';
            row.quote_volume_unit = 'quote_asset';
          }
          rows.push(row);
          oldest = Math.min(oldest, Number(a[0]));
        }
      }
      if (data.length < count || oldest >= after) break;
      after = oldest;
    }
  } else if (provider === 'gate') {
    const bar = gateBar(interval, market);
    const contractQuote = split(compact(symbol))[1];
    const gateSettle = gateContractSettle(contractQuote);
    const contractMultiplier =
        market === 'contract' && contractQuote !== 'USD'
            ? await marketContractBaseMultiplier('gate', symbol)
            : null;
    if (!bar) throw new Error(`gate ${market} interval ${interval} requires aggregation`);
    const seconds = Math.max(1, Math.floor(intervalMs(interval) / 1000));
    const maxPoints = market === 'contract' ? 2000 : 1000;
    const maxPages = market === 'contract' ? 3 : 5;
    const endpointPaths = market === 'contract'
      ? [
          `https://api.gateio.ws/api/v4/futures/${gateSettle}/candlesticks`,
          `https://fx-api.gateio.ws/api/v4/futures/${gateSettle}/candlesticks`,
        ]
      : ['https://api.gateio.ws/api/v4/spot/candlesticks'];
    const key = market === 'contract' ? 'contract' : 'currency_pair';
    let pageTo = Math.max(1, Math.floor(end / 1000));
    let pages = 0;
    while (rows.length < limit && pages < maxPages) {
      const wanted = Math.min(maxPoints, Math.max(1, limit - rows.length));
      const pageFrom = Math.max(0, pageTo - (wanted + 5) * seconds);
      const urls = endpointPaths.map((base) =>
        `${base}?${key}=${encodeURIComponent(gateId(symbol))}` +
        `&interval=${encodeURIComponent(bar)}&from=${pageFrom}&to=${pageTo}`,
      );
      const payload = await jsonFetch(urls);
      const pageRows = (Array.isArray(payload) ? payload : []).map((a) => {
        if (Array.isArray(a)) {
          const close = num(a[2]);
          if (market === 'contract') {
            const contracts = Math.abs(num(a[1]) || 0);
            const officialQuoteVolume = num(a[6]);
            const baseVolume = contractQuote === 'USD'
              ? (officialQuoteVolume !== null && close !== null && close > 0
                  ? officialQuoteVolume / close
                  : null)
              : (contractMultiplier === null
                  ? null
                  : contracts * contractMultiplier);
            const quoteVolume = officialQuoteVolume ??
              (baseVolume !== null && close !== null
                  ? baseVolume * close
                  : null);
            const row = krow(
              provider,
              market,
              symbol,
              interval,
              [Number(a[0]) * 1000, a[5], a[3], a[4], a[2],
               baseVolume, quoteVolume, 0],
            );
            if (row) {
              row.contract_count = contracts;
              row.quantity_semantics =
                  baseVolume === null ? 'contract_count' : 'base_asset';
              row.base_volume_unit =
                  baseVolume === null ? null : 'base_asset';
              row.quote_volume_unit =
                  quoteVolume === null ? null : 'quote_asset';
            }
            return row;
          }
          // Gate spot arrays: [time, quote_volume, close, high, low, open, base_volume].
          return krow(
            provider,
            market,
            symbol,
            interval,
            [Number(a[0]) * 1000, a[5], a[3], a[4], a[2], a[6], a[1], 0],
          );
        }

        const close = num(a.c);
        if (market === 'contract') {
          const contracts = Math.abs(num(a.v) || 0);
          const officialQuoteVolume = num(a.sum ?? a.a);
          const baseVolume = contractQuote === 'USD'
            ? (officialQuoteVolume !== null && close !== null && close > 0
                ? officialQuoteVolume / close
                : null)
            : (contractMultiplier === null
                ? null
                : contracts * contractMultiplier);
          const quoteVolume = officialQuoteVolume ??
            (baseVolume !== null && close !== null
                ? baseVolume * close
                : null);
          const row = krow(
            provider,
            market,
            symbol,
            interval,
            [Number(a.t) * 1000, a.o, a.h, a.l, a.c,
             baseVolume, quoteVolume, a.n],
          );
          if (row) {
            row.contract_count = contracts;
            row.quantity_semantics =
                baseVolume === null ? 'contract_count' : 'base_asset';
            row.base_volume_unit =
                baseVolume === null ? null : 'base_asset';
            row.quote_volume_unit =
                quoteVolume === null ? null : 'quote_asset';
          }
          return row;
        }
        return krow(
          provider,
          market,
          symbol,
          interval,
          [Number(a.t) * 1000, a.o, a.h, a.l, a.c, a.v, a.a ?? a.sum, a.n],
        );
      }).filter(Boolean);
      if (!pageRows.length) break;
      rows.push(...pageRows);
      const oldestMs = Math.min(...pageRows.map((row) => Number(row.open_time_ms)));
      const nextTo = Math.floor(oldestMs / 1000) - 1;
      if (!Number.isFinite(nextTo) || nextTo >= pageTo || pageFrom <= 0) break;
      pageTo = nextTo;
      pages += 1;
    }
  } else if (provider === 'bitget') {
    const bar = bitgetBar(interval, market);
    if (!bar) throw new Error(`bitget interval ${interval} requires aggregation`);
    if (market === 'contract') {
      const identity = await resolveNativeMarketIdentity(provider, market, symbol);
      rows = await fetchBitgetContractKlines({
        displaySymbol: compact(symbol),
        nativeSymbol: compact(identity.native_symbol || identity.raw_symbol || symbol),
        quote: normalizedQuote(identity.quote_asset || split(compact(symbol))[1], 'USDC'),
        interval,
        end,
        limit,
      });
    } else {
      const payload = await jsonFetch(
        `https://api.bitget.com/api/v2/spot/market/candles?symbol=${symbol}` +
        `&granularity=${encodeURIComponent(bar)}&endTime=${end}&limit=${Math.min(200, limit)}`,
      );
      rows = (payload.data || []).map((a) => krow(provider, market, symbol, interval, [a[0],a[1],a[2],a[3],a[4],a[5],a[6],0])).filter(Boolean);
    }
  } else if (provider === 'bybit') {
    const bar = bybitBar(interval);
    if (!bar) throw new Error(`bybit interval ${interval} requires aggregation`);
    if (market === 'contract') {
      const identity = await resolveNativeMarketIdentity(provider, market, symbol);
      rows = await fetchBybitContractKlines({
        displaySymbol: compact(symbol),
        nativeSymbol: compact(identity.native_symbol || identity.raw_symbol || symbol),
        quote: normalizedQuote(identity.quote_asset || split(compact(symbol))[1], 'USDC'),
        interval,
        end,
        limit,
      });
    } else {
      const payload = await bybitPublicJson(
        `/v5/market/kline?category=spot&symbol=${encodeURIComponent(symbol)}` +
        `&interval=${encodeURIComponent(bar)}&end=${Math.min(Number(end) || Date.now(), Date.now())}` +
        `&limit=${Math.min(1000, limit)}`,
        { requireRows: true, timeout: 15_000 },
      );
      rows = payloadRows(payload).map((a) => krow(provider, market, symbol, interval, [a[0],a[1],a[2],a[3],a[4],a[5],a[6],0])).filter(Boolean);
    }
  }
  return [...new Map(rows.map((item) => [item.open_time_ms, item])).values()]
    .sort((a, b) => a.open_time_ms - b.open_time_ms)
    .slice(-limit);
}



async function marketContractBaseMultiplier(provider, symbol) {
  const display = compact(symbol);
  if (!display) return null;
  if (provider === 'okx') {
    return await sharedMarketResult(
      `contract_multiplier:okx:${display}`,
      6 * 60 * 60_000,
      async () => {
        const payload = await jsonFetch(
          `https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId=${encodeURIComponent(okxId(display, 'contract'))}`,
          12_000,
        );
        const row = Array.isArray(payload?.data) ? payload.data[0] : null;
        const [base] = split(display);
        const ctVal = num(row?.ctVal);
        const ctMult = num(row?.ctMult) ?? 1;
        const valueCurrency = String(row?.ctValCcy || '').toUpperCase();
        if (ctVal === null || ctVal <= 0 || ctMult <= 0) return null;
        if (valueCurrency && valueCurrency !== base) return null;
        return ctVal * ctMult;
      },
    );
  }
  if (provider === 'gate') {
    return await sharedMarketResult(
      `contract_multiplier:gate:${display}`,
      6 * 60 * 60_000,
      async () => {
        const payload = await jsonFetch([
          `https://api.gateio.ws/api/v4/futures/${gateContractSettle(split(display)[1])}/contracts/${encodeURIComponent(gateId(display))}`,
          `https://fx-api.gateio.ws/api/v4/futures/${gateContractSettle(split(display)[1])}/contracts/${encodeURIComponent(gateId(display))}`,
        ], 12_000);
        const multiplier = num(payload?.quanto_multiplier);
        return multiplier !== null && multiplier > 0 ? multiplier : null;
      },
    );
  }
  return 1;
}

function normalizeTradeTimestamp(value) {
  if (typeof value === 'string' && !/^\d+(?:\.\d+)?$/.test(value.trim())) {
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  if (parsed < 10_000_000_000) return Math.round(parsed * 1000);
  if (parsed > 10_000_000_000_000) return Math.round(parsed / 1000);
  return Math.round(parsed);
}

function publicTrade(timestamp, price, size, id = '') {
  const time = normalizeTradeTimestamp(timestamp);
  const px = num(price);
  const qty = Math.abs(num(size) || 0);
  if (time === null || px === null || px <= 0) return null;
  return { time, price: px, size: qty, id: String(id || '') };
}

function dedupePublicTrades(items) {
  const seen = new Set();
  const rows = [];
  for (const trade of items) {
    if (!trade) continue;
    const key = trade.id || `${trade.time}:${trade.price}:${trade.size}`;
    if (seen.has(key)) continue;
    seen.add(key);
    rows.push(trade);
  }
  rows.sort((a, b) => a.time - b.time || a.price - b.price);
  return rows;
}

async function recentPublicTrades(provider, market, symbol, end, limit) {
  const wanted = Math.max(100, Math.min(5000, Number(limit) || 1000));
  const trades = [];
  if (provider === 'binance') {
    const base = market === 'contract'
      ? 'https://fapi.binance.com/fapi/v1/aggTrades'
      : 'https://data-api.binance.vision/api/v3/aggTrades';
    let beforeId = null;
    let pages = 0;
    const maxPages = 1; // One bounded REST page; realtime seconds continue over shared WebSocket.
    while (trades.length < wanted && pages < maxPages) {
      const pageLimit = Math.min(1000, Math.max(100, wanted - trades.length));
      let url;
      if (beforeId == null) {
        url = `${base}?symbol=${symbol}&endTime=${Math.max(1, end)}&limit=${pageLimit}`;
      } else {
        const fromId = Math.max(0, beforeId - pageLimit);
        url = `${base}?symbol=${symbol}&fromId=${fromId}&limit=${pageLimit}`;
      }
      const payload = market === 'contract'
        ? await binanceRestJsonFetch(url, 20_000, 'legacy_contract_agg_trades')
        : await binanceRestJsonFetch(url, 20_000, 'spot_agg_trades');
      const page = Array.isArray(payload) ? payload : [];
      if (!page.length) break;
      let oldestId = null;
      for (const item of page) {
        const trade = publicTrade(item.T ?? item.E, item.p, item.q, item.a ?? item.id);
        if (trade && trade.time <= end + 5_000) trades.push(trade);
        const id = Number(item.a ?? item.id);
        if (Number.isFinite(id)) oldestId = oldestId == null ? id : Math.min(oldestId, id);
      }
      if (oldestId == null || oldestId <= 0 || oldestId === beforeId) break;
      beforeId = oldestId;
      pages += 1;
    }
  } else if (provider === 'coinbase') {
    const productId = coinbaseProductId(symbol);
    const payload = await jsonFetch(`${COINBASE_BASE_URL}/products/${encodeURIComponent(productId)}/trades`, 20_000);
    for (const item of Array.isArray(payload) ? payload : []) {
      const trade = publicTrade(item.time, item.price, item.size, item.trade_id);
      if (trade && trade.time <= end + 5_000) trades.push(trade);
    }
  } else if (provider === 'okx') {
    const payload = await jsonFetch(
      `https://www.okx.com/api/v5/market/trades?instId=${encodeURIComponent(okxId(symbol, market))}&limit=500`,
      20_000,
    );
    const multiplier = market === 'contract'
      ? await marketContractBaseMultiplier('okx', symbol)
      : 1;
    for (const item of payload.data || []) {
      const rawSize = num(item.sz);
      const baseSize = rawSize === null || multiplier === null
        ? null
        : rawSize * multiplier;
      const trade = publicTrade(item.ts, item.px, baseSize, item.tradeId);
      if (trade && trade.time <= end + 5_000) {
        trade.raw_contract_count = market === 'contract' ? Math.abs(rawSize || 0) : null;
        trade.quantity_unit = 'base_asset';
        trades.push(trade);
      }
    }
  } else if (provider === 'bybit') {
    const category = market === 'contract'
      ? bybitContractCategory(split(compact(symbol))[1])
      : 'spot';
    const maxLimit = market === 'contract' ? 1000 : 60;
    const identity = market === 'contract'
      ? await resolveNativeMarketIdentity(provider, market, symbol)
      : { native_symbol: symbol };
    const nativeSymbol = compact(identity.native_symbol || symbol);
    const payload = await bybitPublicJson(
      `/v5/market/recent-trade?category=${category}&symbol=${encodeURIComponent(nativeSymbol)}&limit=${maxLimit}`,
      { requireRows: true, timeout: 20_000 },
    );
    for (const item of payloadRows(payload)) {
      const price = num(item.price ?? item.p);
      const rawSize = num(item.size ?? item.v);
      const baseSize = category === 'inverse' &&
              price !== null && price > 0 && rawSize !== null
          ? Math.abs(rawSize) / price
          : rawSize;
      const trade = publicTrade(
        item.time ?? item.T,
        price,
        baseSize,
        item.execId ?? item.i,
      );
      if (trade && trade.time <= end + 5_000) {
        if (category === 'inverse') {
          trade.quote_amount = Math.abs(rawSize || 0);
          trade.quantity_unit = 'base_asset';
        }
        trades.push(trade);
      }
    }
  } else if (provider === 'bitget') {
    let nativeSymbol = symbol;
    let productType = 'USDT-FUTURES';
    if (market === 'contract') {
      const identity = await resolveNativeMarketIdentity(provider, market, symbol);
      nativeSymbol = compact(identity.native_symbol || symbol);
      const quote = normalizedQuote(identity.quote_asset || split(compact(symbol))[1], 'USDT');
      productType = bitgetContractCategory(quote);
    }
    const url = market === 'contract'
      ? `https://api.bitget.com/api/v2/mix/market/fills?symbol=${encodeURIComponent(nativeSymbol)}&productType=${encodeURIComponent(productType)}&limit=100`
      : `https://api.bitget.com/api/v2/spot/market/fills?symbol=${encodeURIComponent(nativeSymbol)}&limit=500`;
    let payload = await jsonFetch(url, 20_000);
    let items = payloadRows(payload);
    if (market === 'contract' && items.length === 0) {
      payload = await jsonFetch(
        `https://api.bitget.com/api/v3/market/fills?category=${encodeURIComponent(productType)}&symbol=${encodeURIComponent(nativeSymbol)}&limit=100`,
        20_000,
      );
      items = payloadRows(payload);
    }
    for (const item of items) {
      const trade = publicTrade(item.ts, item.price, item.size, item.tradeId ?? item.execId);
      if (trade && trade.time <= end + 5_000) trades.push(trade);
    }
  } else if (provider === 'gate') {
    const raw = gateId(symbol);
    const quote = split(compact(symbol))[1];
    const settle = gateContractSettle(quote);
    const url = market === 'contract'
      ? `https://api.gateio.ws/api/v4/futures/${settle}/trades?contract=${encodeURIComponent(raw)}&limit=1000&to=${Math.floor(end / 1000)}`
      : `https://api.gateio.ws/api/v4/spot/trades?currency_pair=${encodeURIComponent(raw)}&limit=1000`;
    const payload = await jsonFetch(url, 20_000);
    const multiplier =
      market === 'contract' && quote !== 'USD'
        ? await marketContractBaseMultiplier('gate', symbol)
        : 1;
    for (const item of Array.isArray(payload) ? payload : []) {
      const timestamp = item.create_time_ms ?? item.time_ms ?? item.create_time ?? item.time;
      const rawSize = num(item.amount ?? item.size);
      const price = num(item.price);
      const baseSize = rawSize === null || price === null || price <= 0
        ? null
        : (market === 'contract' && quote === 'USD'
            ? Math.abs(rawSize) / price
            : Math.abs(rawSize) * multiplier);
      const trade = publicTrade(timestamp, price, baseSize, item.id);
      if (trade && trade.time <= end + 5_000) {
        trade.raw_contract_count =
            market === 'contract' ? Math.abs(rawSize || 0) : null;
        trade.quantity_unit = 'base_asset';
        if (market === 'contract' && quote === 'USD') {
          trade.quote_amount = Math.abs(rawSize || 0);
        }
        trades.push(trade);
      }
    }
  }
  return dedupePublicTrades(trades).slice(-wanted);
}

function aggregateTradesToSecondRows(trades, provider, market, symbol, end, limit) {
  const buckets = new Map();
  for (const trade of trades) {
    if (!trade || trade.time > end + 5_000) continue;
    const start = Math.floor(trade.time / 1000) * 1000;
    const current = buckets.get(start);
    if (!current) {
      buckets.set(start, {
        provider,
        market_type: market,
        symbol,
        interval: '1s',
        open_time: new Date(start).toISOString(),
        open_time_ms: start,
        close_time: new Date(start + 999).toISOString(),
        open: trade.price,
        high: trade.price,
        low: trade.price,
        close: trade.price,
        volume: trade.size,
        quote_volume: num(trade.quote_amount) ??
          trade.size * trade.price,
        trade_count: 1,
        source: `${provider}_official_public_trade_1s_render`,
      });
    } else {
      current.high = Math.max(current.high, trade.price);
      current.low = Math.min(current.low, trade.price);
      current.close = trade.price;
      current.volume += trade.size;
      current.quote_volume +=
        num(trade.quote_amount) ?? trade.size * trade.price;
      current.trade_count += 1;
    }
  }
  // Step650.8.15.8: only seconds with real official trades become candles.
  // Empty seconds remain absent; timeline rendering may visually carry the last
  // price, but the API never fabricates zero-volume OHLC rows.
  return [...buckets.values()]
    .sort((a, b) => a.open_time_ms - b.open_time_ms)
    .slice(-limit);
}

async function fetchSecondMarketKlines(provider, market, symbol, end, limit) {
  // Binance Spot公开K线原生支持1s，直接读取最多1000根真实历史，避免以成交分页近似。
  if (provider === 'binance' && market === 'spot') {
    return fetchNativeMarketKlines(provider, market, symbol, '1s', end, Math.min(1000, limit));
  }
  const tradeLimit = Math.min(5000, Math.max(1000, limit * 8));
  const trades = await recentPublicTrades(provider, market, symbol, end, tradeLimit);
  return aggregateTradesToSecondRows(trades, provider, market, symbol, end, limit);
}

export async function fetchMarketKlines(provider, market, symbol, interval, end, limit, options = {}) {
  assertProviderMarket(provider, market);
  if (interval === '1s') return fetchSecondMarketKlines(provider, market, symbol, end, limit);
  if (interval === 'timeline') interval = '1m';
  if (provider === 'binance' && market === 'contract') {
    // Binance contract Kline remains archive + authenticated Edge relay + production WS only.
    // Render direct Futures REST is hard-disabled. Any candidate failure stays isolated and
    // never falls through to the generic native Binance REST path.
    let seedRows = await getBinanceContractKlineSeed({
      symbol,
      interval,
      end,
      limit,
      forceRestValidation: options.forceRestValidation === true,
      signal: options.signal || null,
      maxRestCalls: 1,
    });

    // Step650.8.15.28: a sparse/empty direct monthly seed can occur when the current monthly
    // archive is not yet available. Reuse the same safe seed chain for official daily candles
    // and aggregate them by real UTC calendar month. This sends no Render-direct Binance REST.
    if (interval === '1M' && seedRows.length < 3) {
      const dailyLimit = Math.min(720, Math.max(120, Number(limit || 80) * 35));
      const dailyRows = await getBinanceContractKlineSeed({
        symbol,
        interval: '1d',
        end,
        limit: dailyLimit,
        forceRestValidation: options.forceRestValidation === true,
        signal: options.signal || null,
        maxRestCalls: 1,
      });
      const monthlyRows = aggregateCalendarMonths(dailyRows, provider, market, symbol).slice(-limit);
      if (monthlyRows.length > seedRows.length) seedRows = monthlyRows;
    }
    return seedRows;
  }
  if (provider === 'coinbase') return coinbaseKlines(symbol, interval, end, limit);
  if (provider === 'binance' && market === 'spot') {
    const step = intervalMs(interval);
    const endBucket = Math.floor(Math.max(1, Number(end || Date.now())) / step) * step;
    const ttlMs = Math.max(1_000, Math.min(30_000, Math.floor(step / 4)));
    const key = `spot_kline:${symbol}:${interval}:${limit}:${endBucket}`;
    return await sharedBinanceResult(
      key,
      ttlMs,
      async () => {
        const sourceInterval = sourceIntervalFor(provider, market, interval);
        const targetMs = intervalMs(interval);
        const sourceMs = intervalMs(sourceInterval);
        const factor = Math.max(1, Math.ceil(targetMs / sourceMs));
        const sourceLimit = Math.min(5000, limit * factor + factor * 4);
        const sourceRows = await fetchNativeMarketKlines(provider, market, symbol, sourceInterval, endBucket + step - 1, sourceLimit);
        if (sourceInterval === interval) return sourceRows.slice(-limit);
        return aggregateCandles(sourceRows, provider, market, symbol, interval).slice(-limit);
      },
    );
  }
  const sourceInterval = sourceIntervalFor(provider, market, interval);
  const targetMs = intervalMs(interval);
  const sourceMs = intervalMs(sourceInterval);
  const factor = Math.max(1, Math.ceil(targetMs / sourceMs));
  const sourceLimit = Math.min(5000, limit * factor + factor * 4);
  const sourceRows = await fetchNativeMarketKlines(provider, market, symbol, sourceInterval, end, sourceLimit);
  if (sourceInterval === interval) return sourceRows.slice(-limit);
  return aggregateCandles(sourceRows, provider, market, symbol, interval).slice(-limit);
}


const BINANCE_COMMON_QUOTE_ASSETS = Object.freeze([
  'USDT', 'USDC', 'FDUSD', 'USD1', 'USD',
  'BTC', 'BNB', 'ETH', 'EUR', 'GBP', 'JPY',
  'TRY', 'BRL', 'AUD', 'CAD',
]);

const ASSET_QUOTE_CANDIDATES = Object.freeze([
  'USDT', 'USDC', 'FDUSD', 'USD1', 'USD',
  'BTC', 'BNB', 'ETH', 'EUR', 'GBP', 'JPY',
  'TRY', 'BRL', 'AUD', 'CAD',
]);

async function assetQuoteSummary(rawBase) {
  const base = compact(rawBase);
  if (!base) throw new Error('base required');

  return await sharedMarketResult(
    `asset_quote_summary:${base}`,
    5 * 60_000,
    async () => {
      const counts = new Map(
        ASSET_QUOTE_CANDIDATES.map((quote) => [
          quote,
          {
            quote_asset: quote,
            spot_count: 0,
            contract_count: 0,
            providers: {},
          },
        ]),
      );

      const spotGroups = await Promise.all(
        SPOT_PROVIDER_LIST.map(async (provider) => {
          try {
            return [provider, await universeCatalog(provider, 'spot')];
          } catch (_) {
            return [provider, []];
          }
        }),
      );
      for (const [provider, rows] of spotGroups) {
        for (const item of rows) {
          if (String(item.base_asset || '').toUpperCase() !== base) {
            continue;
          }
          const quote =
              String(item.quote_asset || '').toUpperCase();
          const target = counts.get(quote);
          if (!target) continue;
          target.spot_count += 1;
          target.providers[provider] = {
            ...(target.providers[provider] || {}),
            spot_count:
                Number(target.providers[provider]?.spot_count || 0) + 1,
            contract_count:
                Number(target.providers[provider]?.contract_count || 0),
          };
        }
      }

      const contractGroups = await Promise.all(
        CONTRACT_PROVIDER_LIST.map(async (provider) => {
          try {
            return [provider, await universeCatalog(provider, 'contract')];
          } catch (_) {
            return [provider, []];
          }
        }),
      );
      for (const [provider, rows] of contractGroups) {
        for (const item of rows) {
          if (String(item.base_asset || '').toUpperCase() !== base) {
            continue;
          }
          const quote =
              String(item.quote_asset || '').toUpperCase();
          const target = counts.get(quote);
          if (!target) continue;
          target.contract_count += 1;
          target.providers[provider] = {
            ...(target.providers[provider] || {}),
            spot_count:
                Number(target.providers[provider]?.spot_count || 0),
            contract_count:
                Number(target.providers[provider]?.contract_count || 0) + 1,
          };
        }
      }

      return [...counts.values()]
        .map((row) => ({
          ...row,
          total_count: row.spot_count + row.contract_count,
        }))
        .filter((row) => row.total_count > 0)
        .sort((a, b) =>
          (b.total_count - a.total_count) ||
          (ASSET_QUOTE_CANDIDATES.indexOf(a.quote_asset) -
            ASSET_QUOTE_CANDIDATES.indexOf(b.quote_asset))
        );
    },
  );
}

async function binanceAssetQuoteSummary(rawBase) {
  // Backward-compatible route. Step657 App uses the all-provider route.
  return assetQuoteSummary(rawBase);
}


function marketUnitSelfTest() {
  const tests = [];
  const add = (name, ok, actual = null) => tests.push({ name, ok: Boolean(ok), actual });

  const binance = tickerVolumeSemantics(
    'binance',
    'spot',
    { volume: '2', quoteVolume: '200' },
    100,
    'USDT',
  );
  add(
    'binance_spot_base_and_quote',
    binance.base_volume_24h === 2 && binance.quote_volume_24h === 200,
    binance,
  );

  const coinbase = tickerVolumeSemantics(
    'coinbase',
    'spot',
    { volume: '2.5', quote_volume_24h: '250' },
    100,
    'USD',
  );
  add(
    'coinbase_spot_base_and_quote',
    coinbase.base_volume_24h === 2.5 && coinbase.quote_volume_24h === 250,
    coinbase,
  );

  const bybitLinear = tickerVolumeSemantics(
    'bybit',
    'contract',
    { volume24h: '3', turnover24h: '300' },
    100,
    'USDT',
  );
  add(
    'bybit_linear_volume_and_turnover',
    bybitLinear.base_volume_24h === 3 &&
      bybitLinear.quote_volume_24h === 300 &&
      bybitLinear.base_volume_source === 'bybit_linear_or_spot_volume24h_base',
    bybitLinear,
  );

  const bybitInverse = tickerVolumeSemantics(
    'bybit',
    'contract',
    { volume24h: '300', turnover24h: '3' },
    100,
    'USD',
  );
  add(
    'bybit_inverse_volume_and_turnover_swapped',
    bybitInverse.base_volume_24h === 3 &&
      bybitInverse.quote_volume_24h === 300 &&
      bybitInverse.base_volume_source === 'bybit_inverse_turnover24h_base' &&
      bybitInverse.quote_volume_source === 'bybit_inverse_volume24h_quote',
    bybitInverse,
  );

  const bybitLinearOi = tickerOpenInterestSemantics(
    'bybit',
    'contract',
    'USDT',
    { openInterest: '2', openInterestValue: '200' },
    100,
  );
  add(
    'bybit_linear_open_interest_units',
    bybitLinearOi.open_interest === 2 &&
      bybitLinearOi.open_interest_value === 200 &&
      bybitLinearOi.open_interest_unit === 'base_asset' &&
      bybitLinearOi.open_interest_value_unit === 'quote_asset',
    bybitLinearOi,
  );

  const bybitInverseOi = tickerOpenInterestSemantics(
    'bybit',
    'contract',
    'USD',
    { openInterest: '200', openInterestValue: '2' },
    100,
  );
  add(
    'bybit_inverse_open_interest_units',
    bybitInverseOi.open_interest === 200 &&
      bybitInverseOi.open_interest_value === 2 &&
      bybitInverseOi.open_interest_unit === 'quote_asset' &&
      bybitInverseOi.open_interest_value_unit === 'base_asset',
    bybitInverseOi,
  );

  const bitget = tickerVolumeSemantics(
    'bitget',
    'contract',
    { baseVolume: '4', quoteVolume: '400', usdtVolume: '401' },
    100,
    'USD',
  );
  add(
    'bitget_base_quote_usdt_separated',
    bitget.base_volume_24h === 4 &&
      bitget.quote_volume_24h === 400 &&
      bitget.quote_volume_usd_24h === 401,
    bitget,
  );

  const okxSpot = tickerVolumeSemantics(
    'okx',
    'spot',
    { vol24h: '5', volCcy24h: '500' },
    100,
    'USD',
  );
  add(
    'okx_spot_base_and_quote',
    okxSpot.base_volume_24h === 5 && okxSpot.quote_volume_24h === 500,
    okxSpot,
  );

  const okxContract = tickerVolumeSemantics(
    'okx',
    'contract',
    { vol24h: '10', volCcy24h: '0.1', volCcyQuote24h: '10000' },
    100000,
    'USD',
  );
  add(
    'okx_contract_count_base_quote_separated',
    okxContract.contract_count_24h === 10 &&
      okxContract.base_volume_24h === 0.1 &&
      okxContract.quote_volume_24h === 10000,
    okxContract,
  );

  const gateSpot = tickerVolumeSemantics(
    'gate',
    'spot',
    { base_volume: '6', quote_volume: '600' },
    100,
    'USD',
  );
  add(
    'gate_spot_base_and_quote',
    gateSpot.base_volume_24h === 6 && gateSpot.quote_volume_24h === 600,
    gateSpot,
  );

  const gateContract = tickerVolumeSemantics(
    'gate',
    'contract',
    {
      volume_24h: '20',
      volume_24h_base: '0.002',
      volume_24h_quote: '200',
    },
    100000,
    'USD',
  );
  add(
    'gate_contract_count_base_quote_separated',
    gateContract.contract_count_24h === 20 &&
      gateContract.base_volume_24h === 0.002 &&
      gateContract.quote_volume_24h === 200,
    gateContract,
  );

  return {
    ok: tests.every((item) => item.ok),
    checks: tests.length,
    tests,
  };
}

export function getBinanceMarketRestHealth() {
  pruneBinanceSharedCache();
  return {
    spot_market_data_host: 'data-api.binance.vision',
    binance_asset_quote_discovery_enabled: true,
    all_provider_asset_quote_discovery_enabled: true,
    bitget_contract_official_v2_only: true,
    bitget_contract_product_types: Object.freeze([
      'USDT-FUTURES',
      'USDC-FUTURES',
      'COIN-FUTURES',
    ]),
    spot_provider_list: SPOT_PROVIDER_LIST,
    contract_provider_list: CONTRACT_PROVIDER_LIST,
    asset_quote_summary_provider_lists_ready:
      SPOT_PROVIDER_LIST.length === 6 &&
      CONTRACT_PROVIDER_LIST.length === 5,
    contract_quote_support: CONTRACT_QUOTES_BY_PROVIDER,
    binance_coin_m_usd_enabled: false,
    binance_asset_quote_candidates: BINANCE_COMMON_QUOTE_ASSETS,
    asset_quote_candidates: ASSET_QUOTE_CANDIDATES,
    exact_quote_normalization: SUPPORTED_EXACT_QUOTE_ASSETS,
    market_unit_self_test: marketUnitSelfTest(),
    shared_cache_entries: binanceSharedCache.size,
    shared_inflight_entries: binanceSharedInflight.size,
    shared_cache_max: BINANCE_SHARED_CACHE_MAX,
    contract_second_history_max_rest_pages: 1,
    synthetic_one_second_candles: false,
    ...binanceMarketRestStats,
  };
}

const OWNED_MARKET_API_PATHS = new Set([
  '/api/binance-asset-quotes',
  '/api/asset-quote-summary',
  '/api/contract-quote-self-test',
  '/api/market-unit-self-test',
  '/api/universe',
  '/api/tickers',
  '/api/klines',
  '/api/binance-contract-market-health',
  '/api/binance-contract-kline-seed-health',
  '/api/binance-contract-kline-relay-health',
  '/api/binance-contract-kline-relay-validation-start',
  '/api/binance-contract-kline-relay-validation-reset',
  '/api/binance-contract-validation-reset',
  '/api/binance-contract-rest-probe',
]);

export async function handleMarketApi(req, res, url) {
  // Step650.8.15.8: this generic market handler must claim only routes it owns.
  // Previously it claimed every /api/* path and returned "unknown market api"
  // before contract-meta/funding/depth/trades/flow/liquidation handlers could run.
  if (!OWNED_MARKET_API_PATHS.has(url.pathname)) return false;
  const validationResetPath = url.pathname === '/api/binance-contract-validation-reset';
  const relayValidationStartPath = url.pathname === '/api/binance-contract-kline-relay-validation-start';
  const relayValidationResetPath = url.pathname === '/api/binance-contract-kline-relay-validation-reset';
  const postRequiredPath = validationResetPath || relayValidationStartPath || relayValidationResetPath;
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET, POST, OPTIONS',
      'access-control-allow-headers': 'content-type,x-kaka-admin-key,x-kaka-validation-token',
    });
    res.end();
    return true;
  }
  if (postRequiredPath ? req.method !== 'POST' : req.method !== 'GET') {
    send(res, 405, {
      ok: false,
      error: postRequiredPath ? 'POST required' : 'GET required',
      rows: [],
    });
    return true;
  }
  const requestController = new AbortController();
  const abortRequest = () => { if (!res.writableEnded && !requestController.signal.aborted) requestController.abort(); };
  req.once('aborted', abortRequest);
  res.once('close', abortRequest);
  try {
    if (url.pathname === '/api/binance-contract-market-health') {
      send(res, 200, getBinanceContractMarketHealth());
      return true;
    }
    if (url.pathname === '/api/binance-contract-kline-seed-health') {
      send(res, 200, getBinanceContractKlineSeedHealth());
      return true;
    }
    if (url.pathname === '/api/binance-contract-kline-relay-health') {
      await ensureBinanceContractKlineRelayInitialized();
      let deployment = null;
      try {
        deployment = await checkBinanceContractKlineRelayDeployment();
      } catch (error) {
        deployment = {
          ok: false,
          reachable: false,
          version: null,
          upstream_called: false,
          error: String(error?.message || error),
        };
      }
      send(res, deployment.ok === true ? 200 : 503, {
        ...getBinanceContractKlineRelayHealth(),
        edge_deployment: deployment,
      });
      return true;
    }
    if (url.pathname === '/api/binance-contract-kline-relay-validation-start') {
      const adminKey = String(req.headers['x-kaka-admin-key'] || '').trim();
      const result = await startBinanceContractKlineRelayValidation(adminKey);
      send(res, 200, {
        ok: true,
        version: '650.8.15.28',
        relay_validation: result,
        health: getBinanceContractKlineRelayHealth(),
        cached_at: new Date().toISOString(),
      });
      return true;
    }
    if (url.pathname === '/api/binance-contract-kline-relay-validation-reset') {
      const adminKey = String(req.headers['x-kaka-admin-key'] || '').trim();
      const health = await resetBinanceContractKlineRelayValidation(adminKey);
      send(res, 200, {
        ok: true,
        version: '650.8.15.28',
        reset: true,
        health,
        cached_at: new Date().toISOString(),
      });
      return true;
    }
    if (url.pathname === '/api/binance-contract-validation-reset') {
      send(res, 410, {
        ok: false,
        version: '650.8.15.28',
        error: 'legacy direct-REST validation reset retired; use the Kline relay validation reset endpoint',
        direct_binance_rest_enabled: false,
      });
      return true;
    }
    if (url.pathname === '/api/binance-contract-rest-probe') {
      send(res, 410, {
        ok: false,
        version: '650.8.15.28',
        error: 'direct Binance REST probe retired; use the Supabase Edge Kline relay validation endpoint',
        direct_binance_rest_probe_enabled: false,
      });
      return true;
    }
    if (url.pathname === '/api/market-unit-self-test') {
      const selfTest = marketUnitSelfTest();
      send(res, selfTest.ok ? 200 : 500, {
        ok: selfTest.ok,
        version: '650.8.15.28',
        self_test: selfTest,
      });
      return true;
    }
    if (url.pathname === '/api/contract-quote-self-test') {
      const tests = [
        ['okx_usdc', contractQuoteSupported('okx', 'USDC')],
        ['okx_usd', contractQuoteSupported('okx', 'USD')],
        ['bybit_usd_inverse', bybitContractCategory('USD') === 'inverse'],
        ['bitget_usd_coin', bitgetContractCategory('USD') === 'COIN-FUTURES'],
        ['gate_usd_btc_settle', gateContractSettle('USD') === 'btc'],
        ['binance_usd_disabled', !contractQuoteSupported('binance', 'USD')],
      ].map(([name, ok]) => ({ name, ok: Boolean(ok) }));
      send(res, tests.every((item) => item.ok) ? 200 : 500, {
        ok: tests.every((item) => item.ok),
        version: '650.8.15.28',
        checks: tests.length,
        tests,
      });
      return true;
    }
    if (url.pathname === '/api/asset-quote-summary') {
      const base = compact(url.searchParams.get('base'));
      if (!base) {
        send(res, 400, { ok: false, error: 'base required', rows: [] });
        return true;
      }
      const rows = await assetQuoteSummary(base);
      send(res, 200, {
        ok: true,
        version: '650.8.15.28',
        base_asset: base,
        rows,
        total_quote_assets: rows.length,
        source: 'six_spot_and_five_contract_official_public_catalogs',
        binance_coin_m_usd_enabled: false,
        cached_at: new Date().toISOString(),
      });
      return true;
    }
    if (url.pathname === '/api/binance-asset-quotes') {
      const base = compact(url.searchParams.get('base'));
      if (!base) {
        send(res, 400, { ok: false, error: 'base required', rows: [] });
        return true;
      }
      const rows = await binanceAssetQuoteSummary(base);
      send(res, 200, {
        ok: true,
        version: '650.8.15.28',
        provider: 'binance',
        base_asset: base,
        rows,
        total_quote_assets: rows.length,
        source: 'six_spot_and_five_contract_official_public_catalogs_compat',
        cached_at: new Date().toISOString(),
      });
      return true;
    }
    const provider = providerKey(url.searchParams.get('provider'));
    const market = marketKey(url.searchParams.get('market_type') || url.searchParams.get('market'));
    if (!provider) {
      send(res, 400, { ok: false, error: 'unsupported provider', rows: [] });
      return true;
    }
    assertProviderMarket(provider, market);
    if (url.pathname === '/api/universe') {
      const quote = (url.searchParams.get('quote') || (provider === 'coinbase' ? 'USD' : 'USDT')).toUpperCase();
      const query = (url.searchParams.get('query') || '').toUpperCase();
      const limit = clamp(url.searchParams.get('limit'), 20, 1000, 120);
      const cursor = clamp(url.searchParams.get('cursor'), 0, 10_000_000, 0);
      const all = (await universe(provider, market, quote)).filter((item) =>
        item.quote_asset === quote && (!query || item.symbol.includes(query) || item.base_asset.includes(query)),
      );
      const rows = all.slice(cursor, cursor + limit);
      const next = cursor + rows.length;
      send(res, 200, {
        ok: true,
        provider,
        market_type: market,
        rows,
        total: all.length,
        next_cursor: next < all.length ? String(next) : '',
        has_more: next < all.length,
        provider_status: provider === 'binance' && market === 'contract'
          ? 'official_public_websocket_snapshot_ok'
          : (market === 'contract' && ['bybit', 'bitget'].includes(provider)
            ? 'official_public_native_identity_ok_render'
            : 'official_public_ok_render'),
        source: rows[0]?.source || `${provider}_official_public_market_render`,
        cached_at: rows[0]?.cached_at || new Date().toISOString(),
      });
      return true;
    }
    if (url.pathname === '/api/tickers') {
      const wanted = [...new Set(String(url.searchParams.get('symbols') || '').split(',').map(compact).filter(Boolean))];
      const all = await tickers(provider, market, wanted);
      const wantedSet = new Set(wanted);
      const rows = wantedSet.size ? all.filter((item) => wantedSet.has(item.symbol)) : all.slice(0, 120);
      send(res, 200, {
        ok: true,
        provider,
        market_type: market,
        rows,
        source: rows[0]?.source || `${provider}_official_public_ticker_render`,
        cached_at: rows[0]?.cached_at || new Date().toISOString(),
      });
      return true;
    }
    if (url.pathname === '/api/klines') {
      const symbol = compact(url.searchParams.get('symbol'));
      const interval = url.searchParams.get('interval') || '15m';
      const validationToken = String(req.headers['x-kaka-validation-token'] || '').trim();
      const validationRequest = provider === 'binance' && market === 'contract' && Boolean(validationToken);
      const endTimeProvided = url.searchParams.has('end_time');
      const requestedLimit = clamp(url.searchParams.get('limit'), 20, 1000, 1000);
      const end = validationRequest ? Date.now() : clamp(url.searchParams.get('end_time'), 1, Number.MAX_SAFE_INTEGER, Date.now());
      const limit = validationRequest ? 240 : requestedLimit;
      if (!symbol) {
        send(res, 400, { ok: false, error: 'symbol required' });
        return true;
      }
      if (validationRequest && (requestedLimit !== 240 || endTimeProvided)) {
        send(res, 409, { ok: false, error: 'validation requires limit=240 and no end_time', rows: [] });
        return true;
      }
      const rows = validationRequest
        ? await runWithBinanceContractKlineRelayValidation(
            validationToken,
            () => fetchMarketKlines(
              provider,
              market,
              symbol,
              interval,
              end,
              limit,
              { forceRestValidation: true, signal: requestController.signal },
            ),
            {
              maxRestCalls: 1,
              provider,
              market,
              symbol,
              interval,
              limit,
              endTimeProvided,
            },
          )
        : await fetchMarketKlines(provider, market, symbol, interval, end, limit, { signal: requestController.signal });
      const coverage = klineCoverage(rows, interval, end);
      if (validationRequest) {
        const validationPassed =
          rows.length === limit &&
          coverage.gap_count === 0 &&
          coverage.missing_intervals === 0 &&
          coverage.lag_intervals_to_end <= 1 &&
          coverage.continuous_to_current === true;
        if (validationPassed) {
          await completeBinanceContractKlineRelayValidation({ token: validationToken, symbol, interval });
        } else {
          await failBinanceContractKlineRelayValidation({
            token: validationToken,
            symbol,
            interval,
            reason: `coverage_failed:rows=${rows.length};gaps=${coverage.gap_count};missing=${coverage.missing_intervals};lag=${coverage.lag_intervals_to_end}`,
          });
        }
      }
      send(res, 200, {
        ok: true,
        version: '650.8.15.28',
        provider,
        market_type: market,
        symbol,
        interval,
        transport: provider === 'binance' && market === 'contract'
          ? 'official_archive_plus_priority_authenticated_edge_relay_plus_live_websocket'
          : 'official_public_market_rest',
        requested_limit: limit,
        returned_rows: rows.length,
        rows,
        coverage,
        source: rows.at(-1)?.source || rows[0]?.source || `${provider}_official_public_kline_render`,
        cached_at: rows.at(-1)?.cached_at || rows[0]?.cached_at || new Date().toISOString(),
      });
      return true;
    }
    send(res, 404, { ok: false, error: 'unknown market api' });
    return true;
  } catch (error) {
    const message = String(error?.message || error);
    const internalGuard = error?.internalBinanceRelayGuard === true || error?.internalBinanceRestGuard === true;
    const status = internalGuard
      ? 409
      : (message.includes('not supported') || message.includes('unsupported provider') ? 400 : 502);
    const guard = error?.internalBinanceRelayGuard === true
      ? getBinanceContractKlineRelayHealth()
      : (error?.internalBinanceRestGuard === true ? getBinanceRestGuardHealth() : null);
    send(res, status, {
      ok: false,
      error: message,
      error_code: error?.code || null,
      used_weight_1m: Number.isFinite(Number(error?.usedWeight1m))
        ? Number(error.usedWeight1m)
        : (guard?.last_probe_used_weight_1m ?? null),
      max_safe_used_weight_1m: Number.isFinite(Number(error?.maxUsedWeight1m))
        ? Number(error.maxUsedWeight1m)
        : (guard?.last_probe_max_used_weight_1m ?? null),
      guard: guard ? {
        active: guard.active,
        next_allowed_at: guard.next_allowed_at,
        reason: guard.reason,
        operating_mode: guard.operating_mode || (guard.edge_relay_only ? 'edge_relay_guarded' : null),
        last_probe_at: guard.last_probe_at,
        last_probe_http_status: guard.last_probe_http_status,
        last_probe_raw_weight_1m: guard.last_probe_raw_weight_1m,
        last_probe_used_weight_1m: guard.last_probe_used_weight_1m,
        last_probe_max_used_weight_1m: guard.last_probe_max_used_weight_1m,
        last_probe_weight_safe: guard.last_probe_weight_safe,
      } : null,
      rows: [],
      cached_at: new Date().toISOString(),
    });
    return true;
  } finally {
    req.removeListener('aborted', abortRequest);
    res.removeListener('close', abortRequest);
  }
}

export const _test = {
  aggregateTradesToSecondRows,
  identityMap,
  klineCoverage,
  marketRow,
  payloadRows,
  tickerRow,
};
