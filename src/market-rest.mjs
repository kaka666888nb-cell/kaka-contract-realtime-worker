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

const PROVIDERS = new Set(['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate']);
const CONTRACT_PROVIDERS = new Set(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const COINBASE_BASE_URL = 'https://api.exchange.coinbase.com';
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
  for (const quote of ['USDT', 'USDC', 'USD']) {
    if (symbol.endsWith(quote)) return [symbol.slice(0, -quote.length), quote];
  }
  return [symbol, 'USDT'];
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
async function binanceRestJsonFetch(url, timeout = 15_000, source = 'legacy_market_rest') {
  // Step650.8.13: Binance Spot and Contract public HTTP both use the same
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
function marketRow(provider, market, symbol, base, quote, raw) {
  return {
    provider,
    market_type: market,
    symbol: compact(symbol),
    raw_symbol: raw,
    base_asset: String(base).toUpperCase(),
    quote_asset: String(quote).toUpperCase(),
    status: 'TRADING',
    active: true,
    source: `${provider}_official_public_market_render`,
  };
}

async function universe(provider, market) {
  assertProviderMarket(provider, market);
  const rows = [];
  if (provider === 'binance') {
    if (market === 'contract') {
      const snapshotRows = await getBinanceContractUniverse({ quote: 'USDT' });
      if (!snapshotRows.length) throw new Error('binance contract universe snapshot unavailable');
      rows.push(...snapshotRows);
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
        rows.push(marketRow(provider, market, item.symbol, item.baseAsset, item.quoteAsset, item.symbol));
      }
    }
  } else if (provider === 'coinbase') {
    const payload = await jsonFetch(`${COINBASE_BASE_URL}/products`);
    for (const item of Array.isArray(payload) ? payload : []) {
      if (String(item.status || '').toLowerCase() !== 'online') continue;
      if (item.trading_disabled === true || item.cancel_only === true) continue;
      const raw = String(item.id || '').toUpperCase();
      const base = String(item.base_currency || '').toUpperCase();
      const quote = String(item.quote_currency || '').toUpperCase();
      if (!raw || !base || !quote) continue;
      rows.push(marketRow(provider, 'spot', raw, base, quote, raw));
    }
  } else if (provider === 'okx') {
    const payload = await jsonFetch(
      `https://www.okx.com/api/v5/public/instruments?instType=${market === 'contract' ? 'SWAP' : 'SPOT'}`,
    );
    for (const item of payload.data || []) {
      if (item.state && item.state !== 'live') continue;
      if (market === 'contract' && item.ctType !== 'linear') continue;
      const base = item.baseCcy || item.ctValCcy;
      const quote = item.quoteCcy || item.settleCcy;
      if (base && quote) rows.push(marketRow(provider, market, item.instId, base, quote, item.instId));
    }
  } else if (provider === 'gate') {
    if (market === 'contract') {
      const payload = await jsonFetch([
        'https://api.gateio.ws/api/v4/futures/usdt/contracts',
        'https://fx-api.gateio.ws/api/v4/futures/usdt/contracts',
      ]);
      for (const item of payload || []) {
        if (item.in_delisting === true) continue;
        const [base, quote = 'USDT'] = String(item.name || '').toUpperCase().split('_');
        if (base) rows.push(marketRow(provider, market, item.name, base, quote, item.name));
      }
    } else {
      const payload = await jsonFetch('https://api.gateio.ws/api/v4/spot/currency_pairs');
      for (const item of payload || []) {
        if (String(item.trade_status || 'tradable').toLowerCase() !== 'tradable') continue;
        const [base, quote] = String(item.id || '').toUpperCase().split('_');
        if (base && quote) rows.push(marketRow(provider, market, item.id, item.base || base, item.quote || quote, item.id));
      }
    }
  } else if (provider === 'bitget') {
    const payload = await jsonFetch(
      market === 'contract'
        ? 'https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES'
        : 'https://api.bitget.com/api/v2/spot/public/symbols',
    );
    for (const item of payload.data || []) {
      const status = String(item.symbolStatus || item.status || '').toLowerCase();
      if (status && !['normal', 'online', 'listed'].includes(status)) continue;
      if (item.baseCoin && item.quoteCoin) {
        rows.push(marketRow(provider, market, item.symbol, item.baseCoin, item.quoteCoin, item.symbol));
      }
    }
  } else if (provider === 'bybit') {
    const category = market === 'contract' ? 'linear' : 'spot';
    let cursor = '';
    do {
      const suffix = market === 'contract'
        ? `&limit=1000${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`
        : '';
      const payload = await jsonFetch(
        `https://api.bybit.com/v5/market/instruments-info?category=${category}${suffix}`,
      );
      const result = payload.result || {};
      for (const item of result.list || []) {
        if (String(item.status || 'Trading').toLowerCase() !== 'trading') continue;
        if (market === 'contract' && String(item.contractType || '').toLowerCase() !== 'linearperpetual') continue;
        const raw = String(item.symbol || '').toUpperCase();
        const [base, quote] = split(raw);
        if (base && quote) {
          rows.push(marketRow(provider, market, raw, item.baseCoin || base, item.quoteCoin || quote, raw));
        }
      }
      cursor = market === 'contract' ? String(result.nextPageCursor || '') : '';
    } while (cursor);
  }
  return [...new Map(rows.map((item) => [`${item.provider}:${item.symbol}`, item])).values()]
    .sort((a, b) => a.symbol.localeCompare(b.symbol));
}

function tickerRow(provider, market, item, rawSymbol) {
  const symbol = compact(rawSymbol);
  if (!symbol) return null;
  const last = num(item.last_price ?? item.lastPrice ?? item.last ?? item.close ?? item.lastPr);
  const open = num(item.open_24h ?? item.openPrice ?? item.open ?? item.open24h ?? item.prevPrice24h);
  let percent = num(
    item.price_change_percent_24h ?? item.priceChangePercent ?? item.change_percentage ?? item.change24h ?? item.price24hPcnt,
  );
  if (percent !== null && (provider === 'bitget' || provider === 'bybit') && Math.abs(percent) <= 2) {
    percent *= 100;
  }
  if (percent === null && last !== null && open) percent = ((last - open) / open) * 100;
  return {
    provider,
    market_type: market,
    symbol,
    last_price: last,
    price: last,
    price_change_percent_24h: percent,
    quote_volume_24h: num(
      item.quote_volume_24h ?? item.quoteVolume ?? item.quote_volume ?? item.volume_24h_quote ??
      item.volCcy24h ?? item.amount ?? item.quoteVolume24h ?? item.usdtVolume ?? item.turnover24h,
    ),
    base_volume_24h: num(
      item.base_volume_24h ?? item.volume ?? item.volume_24h ?? item.vol24h ?? item.baseVolume ??
      item.base_volume ?? item.baseVolume24h ?? item.volume24h,
    ),
    high_24h: num(item.high_24h ?? item.highPrice ?? item.high24h ?? item.highPrice24h),
    low_24h: num(item.low_24h ?? item.lowPrice ?? item.low24h ?? item.lowPrice24h),
    funding_rate: num(item.fundingRate),
    open_interest: num(item.openInterest),
    open_interest_value: num(item.openInterestValue),
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
    volume: baseVolume,
    quote_volume_24h: last !== null && baseVolume !== null ? last * baseVolume : null,
  }, normalized);
  if (!row) throw new Error(`Coinbase ticker unavailable for ${productId}`);
  coinbaseTickerCache.set(cacheKey, { at: Date.now(), row });
  return row;
}

async function tickers(provider, market, wantedSymbols = []) {
  assertProviderMarket(provider, market);
  if (provider === 'coinbase') {
    const symbols = [...new Set(wantedSymbols.map(compact).filter(Boolean))].slice(0, 48);
    if (!symbols.length) return [];
    let lastError = null;
    const rows = await mapLimit(symbols, 5, async (symbol) => {
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
  let items = [];
  if (provider === 'binance') {
    if (market === 'contract') {
      // Step650.2：单个旧/下架符号未命中时返回空数组，不把整个 Binance ticker 路由误判为上游故障，
      // 更不能因此打开 provider 级熔断，导致同批 BTC/BNB/BCH 等正常交易对一起变成破折号。
      return getBinanceContractTickers({ symbols: wantedSymbols });
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
    items = Array.isArray(payload) ? payload : [];
  } else if (provider === 'okx') {
    const payload = await jsonFetch(
      `https://www.okx.com/api/v5/market/tickers?instType=${market === 'contract' ? 'SWAP' : 'SPOT'}`,
    );
    items = payload.data || [];
  } else if (provider === 'gate') {
    const payload = await jsonFetch(
      market === 'contract'
        ? ['https://api.gateio.ws/api/v4/futures/usdt/tickers', 'https://fx-api.gateio.ws/api/v4/futures/usdt/tickers']
        : 'https://api.gateio.ws/api/v4/spot/tickers',
    );
    items = Array.isArray(payload) ? payload : [];
  } else if (provider === 'bitget') {
    const payload = await jsonFetch(
      market === 'contract'
        ? 'https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES'
        : 'https://api.bitget.com/api/v2/spot/market/tickers',
    );
    items = payload.data || [];
  } else if (provider === 'bybit') {
    const payload = await jsonFetch(
      `https://api.bybit.com/v5/market/tickers?category=${market === 'contract' ? 'linear' : 'spot'}`,
    );
    items = payload.result?.list || [];
  }
  return items
    .map((item) => tickerRow(provider, market, item, item.symbol ?? item.instId ?? item.contract ?? item.currency_pair))
    .filter(Boolean);
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
        const row = krow(provider, market, symbol, interval, [a[0],a[1],a[2],a[3],a[4],a[5],a[7],0]);
        if (row) { rows.push(row); oldest = Math.min(oldest, Number(a[0])); }
      }
      if (data.length < count || oldest >= after) break;
      after = oldest;
    }
  } else if (provider === 'gate') {
    const bar = gateBar(interval, market);
    if (!bar) throw new Error(`gate ${market} interval ${interval} requires aggregation`);
    const seconds = Math.max(1, Math.floor(intervalMs(interval) / 1000));
    const maxPoints = market === 'contract' ? 2000 : 1000;
    const maxPages = market === 'contract' ? 3 : 5;
    const endpointPaths = market === 'contract'
      ? [
          'https://api.gateio.ws/api/v4/futures/usdt/candlesticks',
          'https://fx-api.gateio.ws/api/v4/futures/usdt/candlesticks',
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
      const pageRows = (Array.isArray(payload) ? payload : []).map((a) => Array.isArray(a)
        ? krow(provider, market, symbol, interval, [Number(a[0]) * 1000,a[5],a[3],a[4],a[2],a[6],a[1],0])
        : krow(provider, market, symbol, interval, [Number(a.t) * 1000,a.o,a.h,a.l,a.c,a.v,a.a ?? a.sum,a.n]))
        .filter(Boolean);
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
    const base = market === 'contract'
      ? 'https://api.bitget.com/api/v2/mix/market/candles'
      : 'https://api.bitget.com/api/v2/spot/market/candles';
    const product = market === 'contract' ? '&productType=USDT-FUTURES' : '';
    const payload = await jsonFetch(
      `${base}?symbol=${symbol}${product}&granularity=${encodeURIComponent(bar)}` +
      `&endTime=${end}&limit=${Math.min(market === 'spot' ? 200 : 1000, limit)}`,
    );
    rows = (payload.data || []).map((a) => krow(provider, market, symbol, interval, [a[0],a[1],a[2],a[3],a[4],a[5],a[6],0])).filter(Boolean);
  } else if (provider === 'bybit') {
    const bar = bybitBar(interval);
    if (!bar) throw new Error(`bybit interval ${interval} requires aggregation`);
    const payload = await jsonFetch(
      `https://api.bybit.com/v5/market/kline?category=${market === 'contract' ? 'linear' : 'spot'}` +
      `&symbol=${symbol}&interval=${encodeURIComponent(bar)}&end=${end}&limit=${Math.min(1000, limit)}`,
    );
    rows = (payload.result?.list || []).map((a) => krow(provider, market, symbol, interval, [a[0],a[1],a[2],a[3],a[4],a[5],a[6],0])).filter(Boolean);
  }
  return [...new Map(rows.map((item) => [item.open_time_ms, item])).values()]
    .sort((a, b) => a.open_time_ms - b.open_time_ms)
    .slice(-limit);
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
    for (const item of payload.data || []) {
      const trade = publicTrade(item.ts, item.px, item.sz, item.tradeId);
      if (trade && trade.time <= end + 5_000) trades.push(trade);
    }
  } else if (provider === 'bybit') {
    const category = market === 'contract' ? 'linear' : 'spot';
    const maxLimit = market === 'contract' ? 1000 : 60;
    const payload = await jsonFetch(
      `https://api.bybit.com/v5/market/recent-trade?category=${category}&symbol=${symbol}&limit=${maxLimit}`,
      20_000,
    );
    for (const item of payload.result?.list || []) {
      const trade = publicTrade(item.time ?? item.T, item.price, item.size, item.execId ?? item.i);
      if (trade && trade.time <= end + 5_000) trades.push(trade);
    }
  } else if (provider === 'bitget') {
    const url = market === 'contract'
      ? `https://api.bitget.com/api/v2/mix/market/fills?symbol=${symbol}&productType=USDT-FUTURES&limit=100`
      : `https://api.bitget.com/api/v2/spot/market/fills?symbol=${symbol}&limit=500`;
    const payload = await jsonFetch(url, 20_000);
    for (const item of payload.data || []) {
      const trade = publicTrade(item.ts, item.price, item.size, item.tradeId);
      if (trade && trade.time <= end + 5_000) trades.push(trade);
    }
  } else if (provider === 'gate') {
    const raw = gateId(symbol);
    const url = market === 'contract'
      ? `https://api.gateio.ws/api/v4/futures/usdt/trades?contract=${encodeURIComponent(raw)}&limit=1000&to=${Math.floor(end / 1000)}`
      : `https://api.gateio.ws/api/v4/spot/trades?currency_pair=${encodeURIComponent(raw)}&limit=1000&to=${Math.floor(end / 1000)}`;
    const payload = await jsonFetch(url, 20_000);
    for (const item of Array.isArray(payload) ? payload : []) {
      const timestamp = item.create_time_ms ?? item.time_ms ?? item.create_time ?? item.time;
      const trade = publicTrade(timestamp, item.price, item.amount ?? item.size, item.id);
      if (trade && trade.time <= end + 5_000) trades.push(trade);
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
        quote_volume: trade.size * trade.price,
        trade_count: 1,
        source: `${provider}_official_public_trade_1s_render`,
      });
    } else {
      current.high = Math.max(current.high, trade.price);
      current.low = Math.min(current.low, trade.price);
      current.close = trade.price;
      current.volume += trade.size;
      current.quote_volume += trade.size * trade.price;
      current.trade_count += 1;
    }
  }
  // Step650.8.13: only seconds with real official trades become candles.
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
    // Step650.8.13：Binance 合约历史K线先读官方日/月归档；若持久快照尾部已有实时蜡烛但内部仍断层，则从第一个缺口开始补官方当前日HTTP桥接，再启动按需实时K线WebSocket。
    // 归档、当前桥接和实时流按open_time去重合并后持久化；任何候选失败都不跨平台、不插值、不造蜡烛。
    const seedRows = await getBinanceContractKlineSeed({ symbol, interval, end, limit, forceRestValidation: options.forceRestValidation === true, signal: options.signal || null, maxRestCalls: 1 });
    // Step650.8.13: never fall through to the generic native Binance REST path.
    // Binance contract Kline is archive + authenticated Edge relay + production WS only.
    // Falling through attempted a public_rest /fapi/v1/klines route that is intentionally
    // not allowlisted and converted a recoverable empty/partial seed into HTTP 502.
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

export function getBinanceMarketRestHealth() {
  pruneBinanceSharedCache();
  return {
    spot_market_data_host: 'data-api.binance.vision',
    shared_cache_entries: binanceSharedCache.size,
    shared_inflight_entries: binanceSharedInflight.size,
    shared_cache_max: BINANCE_SHARED_CACHE_MAX,
    contract_second_history_max_rest_pages: 1,
    synthetic_one_second_candles: false,
    ...binanceMarketRestStats,
  };
}

const OWNED_MARKET_API_PATHS = new Set([
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
  // Step650.8.13: this generic market handler must claim only routes it owns.
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
        version: '650.8.13',
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
        version: '650.8.13',
        reset: true,
        health,
        cached_at: new Date().toISOString(),
      });
      return true;
    }
    if (url.pathname === '/api/binance-contract-validation-reset') {
      send(res, 410, {
        ok: false,
        version: '650.8.13',
        error: 'legacy direct-REST validation reset retired; use the Kline relay validation reset endpoint',
        direct_binance_rest_enabled: false,
      });
      return true;
    }
    if (url.pathname === '/api/binance-contract-rest-probe') {
      send(res, 410, {
        ok: false,
        version: '650.8.13',
        error: 'direct Binance REST probe retired; use the Supabase Edge Kline relay validation endpoint',
        direct_binance_rest_probe_enabled: false,
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
      const all = (await universe(provider, market)).filter((item) =>
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
          : 'official_public_ok_render',
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
        version: '650.8.13',
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
  klineCoverage,
};
