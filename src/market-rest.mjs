const PROVIDERS = new Set(['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate']);
const CONTRACT_PROVIDERS = new Set(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const COINBASE_BASE_URL = 'https://api.exchange.coinbase.com';
const coinbaseTickerCache = new Map();
const COINBASE_TICKER_TTL_MS = 5_000;

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
function okxBar(interval) {
  return ({
    '1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m', '30m': '30m',
    '1h': '1H', '2h': '2H', '4h': '4H', '6h': '6H', '12h': '12H',
    '1d': '1Dutc', '3d': '3Dutc', '1w': '1Wutc', '1M': '1Mutc',
  })[interval] || '15m';
}
function gateBar(interval) {
  return ({
    '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m', '1h': '1h',
    '2h': '2h', '4h': '4h', '6h': '6h', '8h': '8h', '12h': '12h',
    '1d': '1d', '3d': '3d', '1w': '7d',
  })[interval] || '15m';
}
function bitgetBar(interval) {
  return ({
    '1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m', '30m': '30m',
    '1h': '1H', '2h': '2H', '4h': '4H', '6h': '6H', '12h': '12H',
    '1d': '1D', '3d': '3D', '1w': '1W', '1M': '1M',
  })[interval] || '15m';
}
function bybitBar(interval) {
  return ({
    '1m': '1', '3m': '3', '5m': '5', '15m': '15', '30m': '30',
    '1h': '60', '2h': '120', '4h': '240', '6h': '360', '12h': '720',
    '1d': 'D', '1w': 'W', '1M': 'M',
  })[interval] || '15';
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
          'user-agent': 'KakaWeb3-Market-Worker/514.0',
        },
      });
      if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
      return await response.json();
    } catch (error) {
      lastError = error;
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastError || new Error('upstream unavailable');
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
    const payload = await jsonFetch(
      market === 'contract'
        ? 'https://fapi.binance.com/fapi/v1/exchangeInfo'
        : ['https://api.binance.com/api/v3/exchangeInfo', 'https://data-api.binance.vision/api/v3/exchangeInfo'],
    );
    for (const item of payload.symbols || []) {
      if (String(item.status).toUpperCase() !== 'TRADING') continue;
      if (market === 'contract' && String(item.contractType).toUpperCase() !== 'PERPETUAL') continue;
      rows.push(marketRow(provider, market, item.symbol, item.baseAsset, item.quoteAsset, item.symbol));
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
    const payload = await jsonFetch(
      market === 'contract'
        ? 'https://fapi.binance.com/fapi/v1/ticker/24hr'
        : ['https://api.binance.com/api/v3/ticker/24hr', 'https://data-api.binance.vision/api/v3/ticker/24hr'],
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

function aggregateCoinbaseCandles(sourceRows, provider, market, symbol, interval) {
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
  return aggregateCoinbaseCandles(dedupedSource, 'coinbase', 'spot', symbol, interval).slice(-limit);
}

export async function fetchMarketKlines(provider, market, symbol, interval, end, limit) {
  assertProviderMarket(provider, market);
  let rows = [];
  if (provider === 'coinbase') {
    rows = await coinbaseKlines(symbol, interval, end, limit);
  } else if (provider === 'binance') {
    const base = market === 'contract'
      ? 'https://fapi.binance.com/fapi/v1/klines'
      : 'https://api.binance.com/api/v3/klines';
    const payload = await jsonFetch(
      `${base}?symbol=${symbol}&interval=${encodeURIComponent(interval)}&endTime=${end}&limit=${Math.min(1500, limit)}`,
    );
    rows = (payload || []).map((a) => krow(provider, market, symbol, interval, [a[0],a[1],a[2],a[3],a[4],a[5],a[7],a[8]])).filter(Boolean);
  } else if (provider === 'okx') {
    let after = end + 1;
    while (rows.length < limit) {
      const count = Math.min(300, limit - rows.length);
      const payload = await jsonFetch(
        `https://www.okx.com/api/v5/market/history-candles?instId=${encodeURIComponent(okxId(symbol, market))}` +
        `&bar=${encodeURIComponent(okxBar(interval))}&after=${after}&limit=${count}`,
      );
      const data = payload.data || [];
      if (!data.length) break;
      let oldest = after;
      for (const a of data) {
        const row = krow(provider, market, symbol, interval, [a[0],a[1],a[2],a[3],a[4],a[5],a[7],0]);
        if (row) {
          rows.push(row);
          oldest = Math.min(oldest, Number(a[0]));
        }
      }
      if (data.length < count || oldest >= after) break;
      after = oldest;
    }
  } else if (provider === 'gate') {
    const seconds = Math.max(60, Math.floor(intervalMs(interval) / 1000));
    const to = Math.floor(end / 1000);
    const from = Math.max(0, to - (limit + 5) * seconds);
    const url = market === 'contract'
      ? `https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract=${encodeURIComponent(gateId(symbol))}` +
        `&interval=${encodeURIComponent(gateBar(interval))}&from=${from}&to=${to}&limit=${Math.min(2000, limit)}`
      : `https://api.gateio.ws/api/v4/spot/candlesticks?currency_pair=${encodeURIComponent(gateId(symbol))}` +
        `&interval=${encodeURIComponent(gateBar(interval))}&from=${from}&to=${to}&limit=${Math.min(1000, limit)}`;
    const payload = await jsonFetch(url);
    rows = (payload || []).map((a) => Array.isArray(a)
      ? krow(provider, market, symbol, interval, [Number(a[0]) * 1000,a[5],a[3],a[4],a[2],a[6],a[1],0])
      : krow(provider, market, symbol, interval, [Number(a.t) * 1000,a.o,a.h,a.l,a.c,a.v,a.a ?? a.sum,a.n]))
      .filter(Boolean);
  } else if (provider === 'bitget') {
    const base = market === 'contract'
      ? 'https://api.bitget.com/api/v2/mix/market/candles'
      : 'https://api.bitget.com/api/v2/spot/market/candles';
    const product = market === 'contract' ? '&productType=USDT-FUTURES' : '';
    const payload = await jsonFetch(
      `${base}?symbol=${symbol}${product}&granularity=${encodeURIComponent(bitgetBar(interval))}` +
      `&endTime=${end}&limit=${Math.min(1000, limit)}`,
    );
    rows = (payload.data || []).map((a) => krow(provider, market, symbol, interval, [a[0],a[1],a[2],a[3],a[4],a[5],a[6],0])).filter(Boolean);
  } else if (provider === 'bybit') {
    const payload = await jsonFetch(
      `https://api.bybit.com/v5/market/kline?category=${market === 'contract' ? 'linear' : 'spot'}` +
      `&symbol=${symbol}&interval=${encodeURIComponent(bybitBar(interval))}&end=${end}&limit=${Math.min(1000, limit)}`,
    );
    rows = (payload.result?.list || []).map((a) => krow(provider, market, symbol, interval, [a[0],a[1],a[2],a[3],a[4],a[5],a[6],0])).filter(Boolean);
  }
  return [...new Map(rows.map((item) => [item.open_time_ms, item])).values()]
    .sort((a, b) => a.open_time_ms - b.open_time_ms)
    .slice(-limit);
}

export async function handleMarketApi(req, res, url) {
  if (!url.pathname.startsWith('/api/')) return false;
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET, OPTIONS',
      'access-control-allow-headers': 'content-type',
    });
    res.end();
    return true;
  }
  if (req.method !== 'GET') {
    send(res, 405, { ok: false, error: 'GET required', rows: [] });
    return true;
  }
  try {
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
        provider_status: 'official_public_ok_render',
        source: `${provider}_official_public_market_render`,
        cached_at: new Date().toISOString(),
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
        source: `${provider}_official_public_ticker_render`,
        cached_at: new Date().toISOString(),
      });
      return true;
    }
    if (url.pathname === '/api/klines') {
      const symbol = compact(url.searchParams.get('symbol'));
      const interval = url.searchParams.get('interval') || '15m';
      const end = clamp(url.searchParams.get('end_time'), 1, Number.MAX_SAFE_INTEGER, Date.now());
      const limit = clamp(url.searchParams.get('limit'), 20, 1000, 1000);
      if (!symbol) {
        send(res, 400, { ok: false, error: 'symbol required' });
        return true;
      }
      const rows = await fetchMarketKlines(provider, market, symbol, interval, end, limit);
      send(res, 200, {
        ok: true,
        provider,
        market_type: market,
        symbol,
        interval,
        rows,
        source: `${provider}_official_public_kline_render`,
        cached_at: new Date().toISOString(),
      });
      return true;
    }
    send(res, 404, { ok: false, error: 'unknown market api' });
    return true;
  } catch (error) {
    const message = String(error);
    const status = message.includes('not supported') || message.includes('unsupported provider') ? 400 : 502;
    send(res, status, { ok: false, error: message, rows: [], cached_at: new Date().toISOString() });
    return true;
  }
}
