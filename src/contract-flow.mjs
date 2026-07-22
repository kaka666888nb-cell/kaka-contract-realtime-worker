import { WebSocket } from 'ws';
import { fetchBinancePublicRestRelayJson } from './binance-contract-kline-relay.mjs';
import { getBinanceContractRealtimeMeta } from './binance-contract-market.mjs';

const VERSION = '650.8.15.28';
const PROVIDERS = new Set(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const states = new Map();
const MAX_TRADES_PER_STREAM = 120000;
const MAX_AGE_MS = 24 * 60 * 60 * 1000;
const IDLE_CLOSE_MS = 12 * 60 * 1000;
const RECONNECT_MAX_MS = 30000;

function providerKey(raw) {
  const value = String(raw || '').trim().toLowerCase().replaceAll('gate.io', 'gate');
  const normalized = value === 'okex' ? 'okx' : value;
  return PROVIDERS.has(normalized) ? normalized : null;
}

function symbolKey(raw) {
  return String(raw || '')
    .trim()
    .toUpperCase()
    .replace(/-SWAP$/i, '')
    .replace(/_UMCBL$/i, '')
    .replace(/[^A-Z0-9]/g, '');
}

function splitSymbol(symbol) {
  for (const quote of ['USDT', 'USDC', 'USD']) {
    if (symbol.endsWith(quote)) return [symbol.slice(0, -quote.length), quote];
  }
  return [symbol, 'USDT'];
}

function supportsNativeContract(provider, symbol) {
  const [, quote] = splitSymbol(symbol);
  if (quote === 'USDT') return PROVIDERS.has(provider);
  if (quote === 'USDC') {
    return provider === 'binance' ||
      provider === 'okx' ||
      provider === 'bybit' ||
      provider === 'bitget';
  }
  if (quote === 'USD') {
    return provider === 'okx' ||
      provider === 'bybit' ||
      provider === 'bitget' ||
      provider === 'gate';
  }
  return false;
}

function nativeContractSymbol(provider, symbol) {
  const [base, quote] = splitSymbol(symbol);
  if (!supportsNativeContract(provider, symbol)) {
    throw new Error('unsupported_native_contract_quote');
  }
  if ((provider === 'bybit' || provider === 'bitget') && quote === 'USDC') {
    return `${base}PERP`;
  }
  if ((provider === 'bybit' || provider === 'bitget') && quote === 'USD') {
    return `${base}USD`;
  }
  if (provider === 'okx') return `${base}-${quote}-SWAP`;
  if (provider === 'gate') return `${base}_${quote}`;
  return `${base}${quote}`;
}

function bitgetProductType(symbol) {
  const [, quote] = splitSymbol(symbol);
  if (quote === 'USDC') return 'USDC-FUTURES';
  if (quote === 'USD') return 'COIN-FUTURES';
  return 'USDT-FUTURES';
}


function bybitCategory(symbol) {
  return splitSymbol(symbol)[1] === 'USD' ? 'inverse' : 'linear';
}

function gateSettle(symbol) {
  return splitSymbol(symbol)[1] === 'USD' ? 'btc' : 'usdt';
}

function openInterestUnitSemantics(provider, symbol) {
  const [, quote] = splitSymbol(symbol);
  if (provider === 'bybit' && quote === 'USD') {
    return {
      open_interest_unit: 'quote_asset',
      open_interest_value_unit: 'base_asset',
    };
  }
  return {
    open_interest_unit: 'base_asset',
    open_interest_value_unit: 'quote_asset',
  };
}

function okxInstId(symbol) {
  const [base, quote] = splitSymbol(symbol);
  return `${base}-${quote}-SWAP`;
}

function gateSymbol(symbol) {
  const [base, quote] = splitSymbol(symbol);
  return `${base}_${quote}`;
}

function asNumber(value) {
  // Step615.3：Number(null) 与 Number('') 都会得到 0。
  // 资金指标的“缺失”不能被转换成 0，否则会覆盖数据库中的真实值并让卡片忽有忽无。
  if (value == null || typeof value === 'boolean') return null;
  if (typeof value === 'string' && value.trim() === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeTime(value) {
  if (value instanceof Date) {
    const parsedDate = value.getTime();
    return Number.isFinite(parsedDate) ? parsedDate : null;
  }
  if (typeof value === 'string') {
    const text = value.trim();
    if (!text) return null;
    const numeric = Number(text);
    if (Number.isFinite(numeric)) {
      if (numeric < 10_000_000_000) return Math.round(numeric * 1000);
      if (numeric > 10_000_000_000_000) return Math.round(numeric / 1000);
      return Math.round(numeric);
    }
    const parsedIso = Date.parse(text);
    return Number.isFinite(parsedIso) ? parsedIso : null;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  if (parsed < 10_000_000_000) return Math.round(parsed * 1000);
  if (parsed > 10_000_000_000_000) return Math.round(parsed / 1000);
  return Math.round(parsed);
}

function tradeItem(
  time,
  price,
  size,
  side,
  sizeMultiplier = 1,
  explicitQuote = null,
) {
  const ts = normalizeTime(time);
  const px = asNumber(price);
  const multiplier = Math.abs(asNumber(sizeMultiplier) ?? 1);
  const qty = Math.abs(asNumber(size) ?? 0) * multiplier;
  const normalizedSide = String(side || '').toLowerCase();
  const quote = Math.abs(asNumber(explicitQuote) ?? 0) || (px && qty ? px * qty : 0);
  if (!ts || !px || px <= 0 || !qty || qty <= 0 || !quote || quote <= 0) return null;
  if (normalizedSide !== 'buy' && normalizedSide !== 'sell') return null;
  return {
    time: ts,
    price: px,
    size: qty,
    quote,
    side: normalizedSide,
  };
}

function configFor(provider, symbol, quantityMeta = {}) {
  const native = nativeContractSymbol(provider, symbol);
  const [, quote] = splitSymbol(symbol);
  const baseMultiplier = asNumber(quantityMeta.baseMultiplier) ?? 1;
  const quoteMultiplier = asNumber(quantityMeta.quoteMultiplier);
  if (provider === 'binance') {
    return {
      url: `wss://fstream.binance.com/market/ws/${native.toLowerCase()}@aggTrade`,
      subscriptions: [],
      parse(raw) {
        const message = JSON.parse(raw.toString());
        const payload = message?.data ?? message;
        if (payload?.e !== 'aggTrade') return [];
        const item = tradeItem(
          payload.T ?? payload.E,
          payload.p,
          payload.q,
          payload.m === true ? 'sell' : 'buy',
        );
        return item ? [item] : [];
      },
    };
  }
  if (provider === 'okx') {
    return {
      url: 'wss://ws.okx.com:8443/ws/v5/public',
      subscriptions: [{ op: 'subscribe', args: [{ channel: 'trades', instId: native }] }],
      heartbeat: 'ping',
      parse(raw) {
        const text = raw.toString();
        if (text === 'pong') return [];
        const message = JSON.parse(text);
        if (message?.arg?.channel !== 'trades') return [];
        const items = [];
        for (const row of Array.isArray(message.data) ? message.data : []) {
          const price = asNumber(row.px);
          const contracts = Math.abs(asNumber(row.sz) ?? 0);
          let item = null;
          if (price && contracts > 0 && quoteMultiplier && quoteMultiplier > 0) {
            const quoteAmount = contracts * quoteMultiplier;
            item = tradeItem(row.ts, price, quoteAmount / price, row.side, 1, quoteAmount);
          } else {
            item = tradeItem(row.ts, price, contracts, row.side, baseMultiplier);
          }
          if (item) items.push(item);
        }
        return items;
      },
    };
  }
  if (provider === 'bybit') {
    return {
      url: `wss://stream.bybit.com/v5/public/${bybitCategory(symbol)}`,
      subscriptions: [{ op: 'subscribe', args: [`publicTrade.${native}`] }],
      heartbeat: { op: 'ping' },
      parse(raw) {
        const message = JSON.parse(raw.toString());
        if (!String(message?.topic || '').startsWith('publicTrade.')) return [];
        const items = [];
        for (const row of Array.isArray(message.data) ? message.data : []) {
          const price = asNumber(row.p);
          const rawSize = Math.abs(asNumber(row.v) ?? 0);
          const item = quote === 'USD' && price && rawSize > 0
            ? tradeItem(row.T ?? message.ts, price, rawSize / price, row.S, 1, rawSize)
            : tradeItem(row.T ?? message.ts, price, rawSize, row.S);
          if (item) items.push(item);
        }
        return items;
      },
    };
  }
  if (provider === 'bitget') {
    return {
      url: 'wss://ws.bitget.com/v2/ws/public',
      subscriptions: [{ op: 'subscribe', args: [{ instType: bitgetProductType(symbol), channel: 'trade', instId: native }] }],
      heartbeat: 'ping',
      parse(raw) {
        const text = raw.toString();
        if (text === 'pong') return [];
        const message = JSON.parse(text);
        if (message?.arg?.channel !== 'trade') return [];
        const items = [];
        for (const row of Array.isArray(message.data) ? message.data : []) {
          if (Array.isArray(row)) {
            const item = tradeItem(row[0], row[1], row[2], row[3]);
            if (item) items.push(item);
          } else if (row && typeof row === 'object') {
            const item = tradeItem(
              row.ts ?? message.ts,
              row.price ?? row.px,
              row.size ?? row.sz,
              row.side ?? row.S,
            );
            if (item) items.push(item);
          }
        }
        return items;
      },
    };
  }
  if (provider === 'gate') {
    return {
      url: `wss://fx-ws.gateio.ws/v4/ws/${gateSettle(symbol)}`,
      subscriptions: [{
        time: Math.floor(Date.now() / 1000),
        channel: 'futures.trades',
        event: 'subscribe',
        payload: [native],
      }],
      parse(raw) {
        const message = JSON.parse(raw.toString());
        if (message?.channel !== 'futures.trades' || message?.event !== 'update') return [];
        const rows = Array.isArray(message.result)
          ? message.result
          : (message.result ? [message.result] : []);
        const items = [];
        for (const row of rows) {
          const signedSize = asNumber(row.size ?? row.amount);
          const side = row.side ||
            (signedSize == null ? '' : signedSize >= 0 ? 'buy' : 'sell');
          const item = tradeItem(
            row.create_time_ms ?? row.create_time ?? row.time_ms ?? row.time,
            row.price,
            signedSize,
            side,
            baseMultiplier,
          );
          if (item) items.push(item);
        }
        return items;
      },
    };
  }
  throw new Error('unsupported_provider');
}


const FIVE_MIN_MS = 5 * 60 * 1000;
const HISTORY_MS = 24 * 60 * 60 * 1000;
const RETENTION_MS = 72 * 60 * 60 * 1000;
const FLOW_HISTOGRAM_MIN_LOG10 = -2;
const FLOW_HISTOGRAM_MAX_LOG10 = 10;
const FLOW_HISTOGRAM_STEP = 0.10;
const FLOW_HISTOGRAM_BINS = Math.ceil((FLOW_HISTOGRAM_MAX_LOG10 - FLOW_HISTOGRAM_MIN_LOG10) / FLOW_HISTOGRAM_STEP);
const MAX_RECENT_IDS = 3000;
const MAX_ACTIVE_STATES = 80;
const BINANCE_FLOW_MAX_STATES = 24;
const BINANCE_FLOW_CONNECT_GAP_MS = 2_500;
const BINANCE_FLOW_CONNECT_WINDOW_MS = 5 * 60_000;
const BINANCE_FLOW_MAX_CONNECT_ATTEMPTS_5M = 40;
const binanceFlowConnectAttempts = [];
let binanceFlowConnectChain = Promise.resolve();
let binanceFlowLastConnectAt = 0;
const binanceFlowWsStats = { attempts: 0, waits: 0, window_blocks: 0, capacity_rejections: 0, evictions: 0 };
const flowWsTrafficByProvider = Object.fromEntries(
  [...PROVIDERS].map((provider) => [provider, { messages: 0, bytes: 0 }]),
);

function flowRawByteLength(raw) {
  if (Buffer.isBuffer(raw)) return raw.length;
  if (raw instanceof ArrayBuffer) return raw.byteLength;
  if (ArrayBuffer.isView(raw)) return raw.byteLength;
  return Buffer.byteLength(String(raw ?? ''), 'utf8');
}
const CORE_SYMBOLS = String(process.env.KAKA_FLOW_CORE_SYMBOLS || 'BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,AVAXUSDT,LINKUSDT,TRXUSDT,DOTUSDT,LTCUSDT')
  .split(',').map(symbolKey).filter((value) => value && value.endsWith('USDT'));
const CORE_SYMBOL_SET = new Set(CORE_SYMBOLS);
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');
const PERSISTENCE_ENABLED = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const persistQueue = new Map();
let persistFlushPromise = null;
const METRIC_HISTORY_MS = 24 * 60 * 60 * 1000;
const METRIC_REFRESH_MS = 5 * 60 * 1000;
const METRIC_TABLE = 'app_contract_position_5m_cache';
const metricPersistQueue = new Map();
let metricPersistFlushPromise = null;
const BINANCE_API_KEY = String(process.env.BINANCE_API_KEY || '').trim();
const CONTRACT_META_TTL_MS = 30 * 1000;
const CONTRACT_META_STALE_MS = 30 * 60 * 1000;
const CONTRACT_META_RETRY_MS = 90 * 1000;
const CONTRACT_META_RESTRICTED_RETRY_MS = 30 * 60 * 1000;
const contractMetaCache = new Map();
const BINANCE_OI_CRITICAL_TTL_MS = 4 * 60 * 1000;
const BINANCE_OI_CRITICAL_DELAY_MS = 900;
const BINANCE_RATIO_CRITICAL_TTL_MS = 5 * 60 * 1000;
const BINANCE_RATIO_FIRST_PAINT_WAIT_MS = 3200;
const BINANCE_RATIO_CRITICAL_LIMIT = 3;


function percentile(sorted, percentileValue) {
  if (!sorted.length) return 0;
  const index = Math.max(0, Math.min(sorted.length - 1, Math.ceil(sorted.length * percentileValue) - 1));
  return sorted[index];
}

function emptyHistogram() {
  return {
    buyQuote: new Float64Array(FLOW_HISTOGRAM_BINS),
    sellQuote: new Float64Array(FLOW_HISTOGRAM_BINS),
    buyCount: new Uint32Array(FLOW_HISTOGRAM_BINS),
    sellCount: new Uint32Array(FLOW_HISTOGRAM_BINS),
  };
}

function flowHistogramIndex(quote) {
  if (!Number.isFinite(quote) || quote <= 0) return 0;
  const raw = Math.floor((Math.log10(quote) - FLOW_HISTOGRAM_MIN_LOG10) / FLOW_HISTOGRAM_STEP);
  return Math.max(0, Math.min(FLOW_HISTOGRAM_BINS - 1, raw));
}

function flowHistogramQuoteAt(index) {
  const center = FLOW_HISTOGRAM_MIN_LOG10 + (index + 0.5) * FLOW_HISTOGRAM_STEP;
  return 10 ** center;
}

function emptyBucket(start) {
  return {
    start,
    end: start + FIVE_MIN_MS,
    histogram: emptyHistogram(),
    buyQuote: 0,
    sellQuote: 0,
    buyCount: 0,
    sellCount: 0,
    tradeCount: 0,
  };
}

function addTradeToBucket(bucket, trade) {
  const index = flowHistogramIndex(trade.quote);
  const isBuy = trade.side === 'buy';
  if (isBuy) {
    bucket.buyQuote += trade.quote;
    bucket.buyCount += 1;
    bucket.histogram.buyQuote[index] += trade.quote;
    bucket.histogram.buyCount[index] += 1;
  } else {
    bucket.sellQuote += trade.quote;
    bucket.sellCount += 1;
    bucket.histogram.sellQuote[index] += trade.quote;
    bucket.histogram.sellCount[index] += 1;
  }
  bucket.tradeCount += 1;
}

function histogramPercentileIndex(bucket, fraction) {
  const target = Math.max(1, Math.ceil(bucket.tradeCount * fraction));
  let cumulative = 0;
  for (let i = 0; i < FLOW_HISTOGRAM_BINS; i += 1) {
    cumulative += bucket.histogram.buyCount[i] + bucket.histogram.sellCount[i];
    if (cumulative >= target) return i;
  }
  return FLOW_HISTOGRAM_BINS - 1;
}

function finalizeBucket(bucket, provider, symbol) {
  if (!bucket || !bucket.tradeCount) return null;
  const p70Index = histogramPercentileIndex(bucket, 0.70);
  const p95Index = histogramPercentileIndex(bucket, 0.95);
  const row = {
    provider, symbol, bucket_time: new Date(bucket.start).toISOString(), bucket_end_time: new Date(bucket.end).toISOString(),
    buy_quote: bucket.buyQuote, sell_quote: bucket.sellQuote,
    large_buy_quote: 0, large_sell_quote: 0,
    medium_buy_quote: 0, medium_sell_quote: 0,
    small_buy_quote: 0, small_sell_quote: 0,
    large_buy_count: 0, large_sell_count: 0,
    medium_buy_count: 0, medium_sell_count: 0,
    small_buy_count: 0, small_sell_count: 0,
    trade_count: bucket.tradeCount,
    p70_quote: flowHistogramQuoteAt(p70Index),
    p95_quote: flowHistogramQuoteAt(p95Index),
    source: provider === 'gate'
      ? 'render_exchange_websocket_histogram_gate_unit_v2'
      : (provider === 'okx'
          ? 'render_exchange_websocket_histogram_okx_unit_v2'
          : 'render_exchange_websocket_histogram'),
    updated_at: new Date().toISOString(),
  };
  for (let i = 0; i < FLOW_HISTOGRAM_BINS; i += 1) {
    const tier = i >= p95Index ? 'large' : (i >= p70Index ? 'medium' : 'small');
    row[`${tier}_buy_quote`] += bucket.histogram.buyQuote[i];
    row[`${tier}_sell_quote`] += bucket.histogram.sellQuote[i];
    row[`${tier}_buy_count`] += bucket.histogram.buyCount[i];
    row[`${tier}_sell_count`] += bucket.histogram.sellCount[i];
  }
  return row;
}

function normalizePersistedRow(row) {
  const start = normalizeTime(row.bucket_time);
  const end = normalizeTime(row.bucket_end_time) || (start ? start + FIVE_MIN_MS : 0);
  if (!start) return null;
  const numberKeys = [
    'buy_quote','sell_quote','large_buy_quote','large_sell_quote','medium_buy_quote','medium_sell_quote','small_buy_quote','small_sell_quote',
    'large_buy_count','large_sell_count','medium_buy_count','medium_sell_count','small_buy_count','small_sell_count','trade_count','p70_quote','p95_quote'
  ];
  const next = { ...row, start, end };
  for (const key of numberKeys) next[key] = asNumber(row[key]) ?? 0;
  return next;
}

function queuePersist(row) {
  if (!PERSISTENCE_ENABLED || !row) return;
  const key = `${row.provider}:${row.symbol}:${row.bucket_time}`;
  persistQueue.set(key, row);
}

async function flushPersistQueue() {
  if (!PERSISTENCE_ENABLED || persistFlushPromise || persistQueue.size === 0) return;
  const rows = [...persistQueue.values()].slice(0, 500);
  for (const row of rows) persistQueue.delete(`${row.provider}:${row.symbol}:${row.bucket_time}`);
  persistFlushPromise = (async () => {
    try {
      const response = await fetch(`${SUPABASE_URL}/rest/v1/app_contract_flow_5m_cache?on_conflict=provider,symbol,bucket_time`, {
        method: 'POST',
        headers: {
          apikey: SUPABASE_SERVICE_ROLE_KEY,
          authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          'content-type': 'application/json',
          prefer: 'resolution=merge-duplicates,return=minimal',
        },
        body: JSON.stringify(rows),
        signal: AbortSignal.timeout(15000),
      });
      if (!response.ok) throw new Error(`persist_http_${response.status}:${(await response.text()).slice(0, 180)}`);
    } catch (error) {
      for (const row of rows) persistQueue.set(`${row.provider}:${row.symbol}:${row.bucket_time}`, row);
      console.error(`[Step615.5] flow bucket persist failed: ${error?.message || error}`);
    }
  })().finally(() => { persistFlushPromise = null; });
  await persistFlushPromise;
}

async function loadPersistedHistory(state) {
  if (!PERSISTENCE_ENABLED || state.historyLoaded || state.historyLoading) return;
  state.historyLoading = true;
  try {
    const cutoff = new Date(Date.now() - HISTORY_MS - FIVE_MIN_MS).toISOString();
    const query = new URLSearchParams({
      select: '*', provider: `eq.${state.provider}`, symbol: `eq.${state.symbol}`,
      bucket_time: `gte.${cutoff}`, order: 'bucket_time.asc', limit: '400',
    });
    const response = await fetch(`${SUPABASE_URL}/rest/v1/app_contract_flow_5m_cache?${query}`, {
      headers: { apikey: SUPABASE_SERVICE_ROLE_KEY, authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}` },
      signal: AbortSignal.timeout(12000),
    });
    const text = await response.text();
    if (!response.ok) throw new Error(`history_http_${response.status}:${text.slice(0, 180)}`);
    const rows = JSON.parse(text);
    for (const raw of Array.isArray(rows) ? rows : []) {
      // Step615.3：615.3以前的Gate桶没有正确单位版本标记，可能仍含“张数当BTC”的旧金额。
      // 即使旧进程在SQL清理后又写回，也不会再被新版本加载。
      if (state.provider === 'gate' && String(raw?.source || '') !== 'render_exchange_websocket_histogram_gate_unit_v2') continue;
      // Step615.4：OKX trades 的 sz 是合约张数，旧桶误按 BTC 数量计算。
      if (state.provider === 'okx' && String(raw?.source || '') !== 'render_exchange_websocket_histogram_okx_unit_v2') continue;
      const row = normalizePersistedRow(raw);
      if (row) state.completedBuckets.set(row.start, row);
    }
    state.historyLoaded = true;
  } catch (error) {
    state.historyError = String(error?.message || error).slice(0, 180);
  } finally {
    state.historyLoading = false;
  }
}

function pruneState(state) {
  const cutoff = Date.now() - RETENTION_MS;
  for (const [start] of state.completedBuckets) if (start < cutoff) state.completedBuckets.delete(start);
  for (const [start] of state.openBuckets) if (start < Date.now() - FIVE_MIN_MS * 2) state.openBuckets.delete(start);
}

function finalizeReadyBuckets(state, now = Date.now()) {
  const readyBefore = Math.floor((now - 15000) / FIVE_MIN_MS) * FIVE_MIN_MS;
  for (const [start, bucket] of [...state.openBuckets.entries()]) {
    if (start >= readyBefore) continue;
    const row = finalizeBucket(bucket, state.provider, state.symbol);
    state.openBuckets.delete(start);
    if (!row) continue;
    const normalized = normalizePersistedRow(row);
    if (normalized) state.completedBuckets.set(start, normalized);
    queuePersist(row);
  }
  pruneState(state);
}

function ingest(state, items) {
  const now = Date.now();
  let added = 0;
  for (const item of items) {
    if (!item || item.time < now - FIVE_MIN_MS * 2 || item.time > now + 15000) continue;
    const signature = `${item.time}:${item.price}:${item.size}:${item.side}`;
    if (state.recentIds.has(signature)) continue;
    state.recentIds.add(signature);
    const start = Math.floor(item.time / FIVE_MIN_MS) * FIVE_MIN_MS;
    let bucket = state.openBuckets.get(start);
    if (!bucket) { bucket = emptyBucket(start); state.openBuckets.set(start, bucket); }
    addTradeToBucket(bucket, item);
    state.lastPrice = item.price;
    added += 1;
  }
  if (state.recentIds.size > MAX_RECENT_IDS) state.recentIds = new Set([...state.recentIds].slice(-Math.floor(MAX_RECENT_IDS / 2)));
  if (added > 0) {
    state.lastTradeAt = now;
    finalizeReadyBuckets(state, now);
    for (const waiter of [...state.waiters]) waiter();
  }
}


async function loadOkxContractMultiplier(state) {
  if (state.provider !== 'okx') return { baseMultiplier: 1, quoteMultiplier: null };
  if ((state.okxContractBaseMultiplier ?? 0) > 0 ||
      (state.okxContractQuoteMultiplier ?? 0) > 0) {
    return {
      baseMultiplier: state.okxContractBaseMultiplier,
      quoteMultiplier: state.okxContractQuoteMultiplier,
    };
  }
  const instId = okxInstId(state.symbol);
  const payload = await firstWorkingJson([
    `https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId=${encodeURIComponent(instId)}`,
    `https://aws.okx.com/api/v5/public/instruments?instType=SWAP&instId=${encodeURIComponent(instId)}`,
  ], { timeoutMs: 8000 });
  const item = Array.isArray(payload?.data) ? payload.data[0] : null;
  const [base, quote] = splitSymbol(state.symbol);
  const ctVal = asNumber(item?.ctVal);
  const ctMult = asNumber(item?.ctMult) ?? 1;
  const valueCurrency = String(item?.ctValCcy || '').toUpperCase();
  if (ctVal == null || ctVal <= 0 || ctMult <= 0) {
    throw new Error('okx_contract_multiplier_missing');
  }
  const faceValue = ctVal * ctMult;
  if (!valueCurrency || valueCurrency === base) {
    state.okxContractBaseMultiplier = faceValue;
    state.okxContractQuoteMultiplier = null;
  } else if (valueCurrency === quote) {
    state.okxContractBaseMultiplier = null;
    state.okxContractQuoteMultiplier = faceValue;
  } else {
    throw new Error('okx_contract_multiplier_currency_unsupported');
  }
  return {
    baseMultiplier: state.okxContractBaseMultiplier,
    quoteMultiplier: state.okxContractQuoteMultiplier,
  };
}

function scheduleOkxMultiplierRetry(state, reason) {
  state.status = 'contract_meta_wait';
  state.error = String(reason || 'okx_contract_multiplier_unavailable').slice(0, 180);
  clearTimeout(state.reconnectTimer);
  if (Date.now() - state.lastRequestedAt <= IDLE_CLOSE_MS) {
    state.reconnectTimer = setTimeout(() => startStream(state).catch(() => {}), 30000);
  }
}

async function loadGateContractMultiplier(state) {
  if (state.provider !== 'gate') return 1;
  if (state.gateQuantoMultiplier && state.gateQuantoMultiplier > 0) {
    return state.gateQuantoMultiplier;
  }
  const contract = gateSymbol(state.symbol);
  const settle = gateSettle(state.symbol);
  const payload = await firstWorkingJson([
    `https://api.gateio.ws/api/v4/futures/${settle}/contracts/${encodeURIComponent(contract)}`,
    `https://fx-api.gateio.ws/api/v4/futures/${settle}/contracts/${encodeURIComponent(contract)}`,
  ], { timeoutMs: 8000 });
  const multiplier = asNumber(payload?.quanto_multiplier);
  if (multiplier == null || multiplier <= 0) {
    throw new Error('gate_contract_multiplier_missing');
  }
  state.gateQuantoMultiplier = multiplier;
  return multiplier;
}

function scheduleGateMultiplierRetry(state, reason) {
  state.status = 'contract_meta_wait';
  state.error = String(reason || 'gate_contract_multiplier_unavailable').slice(0, 180);
  clearTimeout(state.reconnectTimer);
  if (Date.now() - state.lastRequestedAt <= IDLE_CLOSE_MS) {
    state.reconnectTimer = setTimeout(() => startStream(state).catch(() => {}), 30000);
  }
}

function pruneBinanceFlowConnectAttempts(now = Date.now()) {
  while (binanceFlowConnectAttempts.length && now - binanceFlowConnectAttempts[0] >= BINANCE_FLOW_CONNECT_WINDOW_MS) {
    binanceFlowConnectAttempts.shift();
  }
}

async function acquireBinanceFlowConnectSlot() {
  let release;
  const previous = binanceFlowConnectChain;
  binanceFlowConnectChain = new Promise((resolve) => { release = resolve; });
  await previous;
  try {
    const now = Date.now();
    pruneBinanceFlowConnectAttempts(now);
    const gapWait = Math.max(0, BINANCE_FLOW_CONNECT_GAP_MS - (now - binanceFlowLastConnectAt));
    const windowWait = binanceFlowConnectAttempts.length >= BINANCE_FLOW_MAX_CONNECT_ATTEMPTS_5M
      ? Math.max(0, binanceFlowConnectAttempts[0] + BINANCE_FLOW_CONNECT_WINDOW_MS - now)
      : 0;
    const waitMs = Math.max(gapWait, windowWait);
    if (waitMs > 0) {
      binanceFlowWsStats.waits += 1;
      if (windowWait > 0) binanceFlowWsStats.window_blocks += 1;
      await new Promise((resolve) => {
        const timer = setTimeout(resolve, waitMs);
        timer.unref?.();
      });
    }
    binanceFlowLastConnectAt = Date.now();
    binanceFlowConnectAttempts.push(binanceFlowLastConnectAt);
    binanceFlowWsStats.attempts += 1;
  } finally {
    release();
  }
}

async function startStream(state) {
  if (state.ws && (state.ws.readyState === WebSocket.OPEN || state.ws.readyState === WebSocket.CONNECTING)) return;
  if (state.connectingPromise) return state.connectingPromise;
  state.connectingPromise = (async () => {
    clearTimeout(state.reconnectTimer);
    if (state.provider === 'okx' &&
        !((state.okxContractBaseMultiplier ?? 0) > 0) &&
        !((state.okxContractQuoteMultiplier ?? 0) > 0)) {
      state.status = 'loading_contract_meta';
      if (!state.okxMultiplierPromise) {
        state.okxMultiplierPromise = loadOkxContractMultiplier(state)
          .then(() => {
            state.okxMultiplierPromise = null;
            if (states.get(state.key) === state && Date.now() - state.lastRequestedAt <= IDLE_CLOSE_MS) startStream(state).catch(() => {});
          })
          .catch((error) => {
            state.okxMultiplierPromise = null;
            scheduleOkxMultiplierRetry(state, error?.message || error);
          });
      }
      return;
    }
    if (state.provider === 'gate' && !(state.gateQuantoMultiplier > 0)) {
      state.status = 'loading_contract_meta';
      if (!state.gateMultiplierPromise) {
        state.gateMultiplierPromise = loadGateContractMultiplier(state)
          .then(() => {
            state.gateMultiplierPromise = null;
            if (states.get(state.key) === state && Date.now() - state.lastRequestedAt <= IDLE_CLOSE_MS) startStream(state).catch(() => {});
          })
          .catch((error) => {
            state.gateMultiplierPromise = null;
            scheduleGateMultiplierRetry(state, error?.message || error);
          });
      }
      return;
    }
    const quantityMeta = state.provider === 'okx'
      ? {
          baseMultiplier: state.okxContractBaseMultiplier,
          quoteMultiplier: state.okxContractQuoteMultiplier,
        }
      : {
          baseMultiplier: state.provider === 'gate'
            ? (state.gateQuantoMultiplier || 1)
            : 1,
          quoteMultiplier: null,
        };
    const cfg = configFor(state.provider, state.symbol, quantityMeta);
    state.status = state.provider === 'binance' ? 'connect_queued' : 'connecting';
    state.error = '';
    if (state.provider === 'binance') {
      await acquireBinanceFlowConnectSlot();
      if (states.get(state.key) !== state || Date.now() - state.lastRequestedAt > IDLE_CLOSE_MS) return;
      if (state.ws && (state.ws.readyState === WebSocket.OPEN || state.ws.readyState === WebSocket.CONNECTING)) return;
    }
    state.status = 'connecting';
    const ws = new WebSocket(cfg.url, { handshakeTimeout: 15000 });
    state.ws = ws;
    ws.on('open', () => {
      state.status = 'open'; state.reconnectAttempt = 0;
      for (const subscription of cfg.subscriptions) ws.send(JSON.stringify(subscription));
      clearInterval(state.heartbeatTimer);
      state.heartbeatTimer = setInterval(() => {
        if (ws.readyState !== WebSocket.OPEN) return;
        try {
          if (typeof cfg.heartbeat === 'string') ws.send(cfg.heartbeat);
          else if (cfg.heartbeat) ws.send(JSON.stringify(cfg.heartbeat));
          else ws.ping();
        } catch (_) {}
      }, 20000);
    });
    ws.on('message', (raw) => {
      const traffic = flowWsTrafficByProvider[state.provider] || (flowWsTrafficByProvider[state.provider] = { messages: 0, bytes: 0 });
      traffic.messages += 1;
      traffic.bytes += flowRawByteLength(raw);
      try { ingest(state, cfg.parse(raw)); } catch (_) {}
    });
    const close = (reason) => {
      if (state.ws !== ws) return;
      clearInterval(state.heartbeatTimer); state.heartbeatTimer = null; state.ws = null;
      state.status = 'closed'; state.error = String(reason || 'upstream_closed').slice(0, 180);
      if (Date.now() - state.lastRequestedAt <= IDLE_CLOSE_MS) {
        state.reconnectAttempt += 1;
        const delay = Math.min(RECONNECT_MAX_MS, 1000 * 2 ** Math.min(5, state.reconnectAttempt));
        state.reconnectTimer = setTimeout(() => startStream(state).catch(() => {}), delay);
      }
    };
    ws.on('close', (code, reason) => close(`${code}:${reason || ''}`));
    ws.on('error', (error) => close(error?.message || 'upstream_error'));
  })().catch((error) => {
    state.status = 'connect_limited';
    state.error = String(error?.message || error).slice(0, 180);
    if (Date.now() - state.lastRequestedAt <= IDLE_CLOSE_MS) {
      clearTimeout(state.reconnectTimer);
      state.reconnectTimer = setTimeout(() => startStream(state).catch(() => {}), 10_000);
      state.reconnectTimer.unref?.();
    }
  }).finally(() => {
    state.connectingPromise = null;
  });
  return state.connectingPromise;
}

function closeAndDeleteState(state, reason = 'evicted') {
  if (!state) return;
  clearInterval(state.heartbeatTimer);
  clearTimeout(state.reconnectTimer);
  clearTimeout(state.metricBackgroundTimer);
  state.lastRequestedAt = 0;
  const ws = state.ws;
  state.ws = null;
  try { ws?.close(1000, reason); } catch (_) {}
  states.delete(state.key);
}

function ensureStateCapacity(provider, symbol) {
  const key = `${provider}:${symbol}`;
  if (states.has(key)) return;
  if (provider === 'binance') {
    const binanceStates = [...states.values()].filter((state) => state.provider === 'binance');
    if (binanceStates.length >= BINANCE_FLOW_MAX_STATES) {
      const candidates = binanceStates
        .filter((state) => !CORE_SYMBOL_SET.has(state.symbol))
        .sort((a, b) => a.lastRequestedAt - b.lastRequestedAt);
      if (candidates.length) {
        closeAndDeleteState(candidates[0], 'binance_capacity');
        binanceFlowWsStats.evictions += 1;
      }
    }
    if ([...states.values()].filter((state) => state.provider === 'binance').length >= BINANCE_FLOW_MAX_STATES) {
      binanceFlowWsStats.capacity_rejections += 1;
      throw new Error('binance_flow_ws_capacity_reached');
    }
  }
  if (states.size < MAX_ACTIVE_STATES) return;
  const candidates = [...states.values()]
    .filter((state) => !CORE_SYMBOL_SET.has(state.symbol))
    .sort((a, b) => a.lastRequestedAt - b.lastRequestedAt);
  if (candidates.length) closeAndDeleteState(candidates[0], 'capacity');
}

function getState(provider, symbol) {
  ensureStateCapacity(provider, symbol);
  const key = `${provider}:${symbol}`;
  let state = states.get(key);
  if (!state) {
    state = {
      key, provider, symbol, openBuckets: new Map(), completedBuckets: new Map(), recentIds: new Set(), waiters: new Set(),
      ws: null, status: 'idle', error: '', lastTradeAt: 0, lastPrice: null, lastRequestedAt: Date.now(), reconnectAttempt: 0,
      reconnectTimer: null, heartbeatTimer: null, connectingPromise: null,
      metricRows: new Map(), metricLoaded: false, metricLoading: false, metricError: '', metricFetchedAt: 0,
      metricFetchPromise: null, metricCooldownUntil: 0, metricPartialRetryCount: 0,
      oiCriticalPromise: null, oiCriticalScheduledAt: 0,
      ratioCriticalPromise: null, ratioCriticalFetchedAt: 0, ratioCriticalError: '',
      metricBackgroundTimer: null,
      okxContractBaseMultiplier: null, okxContractQuoteMultiplier: null, okxMultiplierPromise: null,
      gateQuantoMultiplier: null, gateMultiplierPromise: null,
      historyLoaded: false, historyLoading: false, historyError: '',
    };
    states.set(key, state);
  }
  state.lastRequestedAt = Date.now();
  loadPersistedHistory(state).catch(() => {});
  startStream(state).catch(() => {});
  return state;
}

function provisionalRows(state) {
  const rows = [];
  for (const bucket of state.openBuckets.values()) {
    const row = finalizeBucket(bucket, state.provider, state.symbol);
    const normalized = row ? normalizePersistedRow(row) : null;
    if (normalized) rows.push(normalized);
  }
  return rows;
}

function chooseBucketMs(coverageMs) {
  // Persisted source rows are fixed 5-minute buckets. Every display bucket
  // must therefore be a whole multiple of five minutes; otherwise the API
  // would label 5-minute rows as 2-minute data and mislead the chart.
  if (coverageMs >= 20 * 60 * 60 * 1000) return 2 * 60 * 60 * 1000;
  if (coverageMs >= 6 * 60 * 60 * 1000) return 60 * 60 * 1000;
  if (coverageMs >= 60 * 60 * 1000) return 10 * 60 * 1000;
  return FIVE_MIN_MS;
}
function isoFrom(value) {
  const ms = normalizeTime(value);
  return ms ? new Date(ms).toISOString() : null;
}

async function fetchJson(url, { headers = {}, timeoutMs = 8000 } = {}) {
  const response = await fetch(url, {
    headers: { accept: 'application/json', 'user-agent': 'KakaWeb3/615.5', ...headers },
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await response.text();
  if (!response.ok) {
    const error = new Error(`upstream_${response.status}:${text.slice(0, 220)}`);
    error.status = response.status;
    throw error;
  }
  try { return text.trim() ? JSON.parse(text) : null; }
  catch (_) { throw new Error(`upstream_invalid_json:${text.slice(0, 160)}`); }
}

async function fetchBinanceJson(url, {
  headers = {}, timeoutMs = 8000, source = 'contract_flow', lane = 'auxiliary', priority = 0,
} = {}) {
  // Step650.8.15.14: all Binance public HTTP used by contract flow/meta is relayed
  // through the authenticated Supabase Edge allowlist. Render never contacts
  // fapi.binance.com directly. Critical first-paint OI uses a dedicated low-volume
  // lane; slower ratio history remains background auxiliary work.
  void headers;
  void timeoutMs;
  return await fetchBinancePublicRestRelayJson(url, { source, lane, priority });
}



function firstDataObject(payload) {
  const direct = payload?.data;
  if (Array.isArray(direct)) return direct.find((item) => item && typeof item === 'object') || null;
  if (direct && typeof direct === 'object') {
    if (Array.isArray(direct.list)) return direct.list.find((item) => item && typeof item === 'object') || null;
    return direct;
  }
  const result = payload?.result;
  if (Array.isArray(result?.list)) return result.list.find((item) => item && typeof item === 'object') || null;
  if (Array.isArray(result)) return result.find((item) => item && typeof item === 'object') || null;
  if (result && typeof result === 'object') return result;
  return null;
}

function firstFinite(...values) {
  for (const value of values) {
    const parsed = asNumber(value);
    if (parsed != null && Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function fundingPercent(raw) {
  const value = asNumber(raw);
  if (value == null) return null;
  return Math.abs(value) <= 1 ? value * 100 : value;
}

function normalizeContractMeta(provider, symbol, raw = {}) {
  const markPrice = firstFinite(raw.mark_price, raw.markPrice, raw.markPx);
  const indexPrice = firstFinite(raw.index_price, raw.indexPrice, raw.idxPx);
  const lastPrice = firstFinite(raw.last_price, raw.lastPrice, raw.last, raw.lastPr);
  const fundingRaw = firstFinite(raw.last_funding_rate, raw.funding_rate, raw.fundingRate, raw.lastFundingRate);
  const nextFundingMs = normalizeTime(raw.next_funding_time ?? raw.nextFundingTime ?? raw.nextUpdate ?? raw.nextSettleTime ?? raw.fundingTime);
  const sourceMs = normalizeTime(raw.source_time ?? raw.time ?? raw.ts ?? raw.requestTime ?? Date.now()) || Date.now();
  const basis = markPrice != null && indexPrice != null && indexPrice !== 0
    ? (markPrice - indexPrice) / indexPrice * 100
    : null;
  return {
    provider,
    symbol,
    mark_price: markPrice,
    index_price: indexPrice,
    last_price: lastPrice,
    last_funding_rate: fundingRaw,
    last_funding_rate_percent: fundingPercent(fundingRaw),
    next_funding_time: nextFundingMs ? new Date(nextFundingMs).toISOString() : null,
    mark_index_basis_percent: basis,
    source_time: new Date(sourceMs).toISOString(),
    cached_at: new Date().toISOString(),
    source: String(raw.source || `${provider}_official_contract_meta`),
  };
}

async function fetchBinanceContractMeta(symbol) {
  const realtime = getBinanceContractRealtimeMeta(symbol);
  if (realtime && typeof realtime === 'object') {
    return normalizeContractMeta('binance', symbol, {
      ...realtime,
      mark_price: realtime.mark_price,
      index_price: realtime.index_price,
      last_price: realtime.last_price ?? realtime.price,
      last_funding_rate: realtime.last_funding_rate ?? realtime.funding_rate,
      next_funding_time: realtime.next_funding_time,
      source_time: realtime.source_time ?? realtime.cached_at,
      source: 'binance_official_public_mark_price_websocket',
    });
  }
  // Cold-start fallback only. Normal first paint is WebSocket-only and never waits
  // behind funding history or position-ratio requests.
  const raw = await fetchBinanceJson(
    `https://fapi.binance.com/fapi/v1/premiumIndex?symbol=${encodeURIComponent(symbol)}`,
    { timeoutMs: 5000, source: 'contract_meta:premium_index', lane: 'critical', priority: 70 },
  );
  return normalizeContractMeta('binance', symbol, {
    ...raw,
    mark_price: raw?.markPrice,
    index_price: raw?.indexPrice,
    last_funding_rate: raw?.lastFundingRate,
    next_funding_time: raw?.nextFundingTime,
    source_time: raw?.time,
    source: 'binance_premium_index_edge_relay_cold_start',
  });
}

async function fetchOkxContractMeta(symbol) {
  const instId = okxInstId(symbol);
  const [base, quote] = splitSymbol(symbol);
  const indexId = `${base}-${quote}`;
  const settled = await Promise.allSettled([
    firstWorkingNonEmptyDataJson([
      `https://www.okx.com/api/v5/public/funding-rate?instId=${encodeURIComponent(instId)}`,
      `https://aws.okx.com/api/v5/public/funding-rate?instId=${encodeURIComponent(instId)}`,
    ], { timeoutMs: 5000 }),
    firstWorkingNonEmptyDataJson([
      `https://www.okx.com/api/v5/public/mark-price?instType=SWAP&instId=${encodeURIComponent(instId)}`,
      `https://aws.okx.com/api/v5/public/mark-price?instType=SWAP&instId=${encodeURIComponent(instId)}`,
    ], { timeoutMs: 5000 }),
    firstWorkingNonEmptyDataJson([
      `https://www.okx.com/api/v5/market/index-tickers?instId=${encodeURIComponent(indexId)}`,
      `https://aws.okx.com/api/v5/market/index-tickers?instId=${encodeURIComponent(indexId)}`,
    ], { timeoutMs: 5000 }),
  ]);
  const funding = settled[0].status === 'fulfilled' ? firstDataObject(settled[0].value) : null;
  const mark = settled[1].status === 'fulfilled' ? firstDataObject(settled[1].value) : null;
  const index = settled[2].status === 'fulfilled' ? firstDataObject(settled[2].value) : null;
  if (!funding && !mark && !index) {
    throw new Error(settled.map((item) => item.status === 'rejected' ? String(item.reason?.message || item.reason) : '').join(' | ').slice(0, 700) || 'okx_contract_meta_empty');
  }
  return normalizeContractMeta('okx', symbol, {
    mark_price: mark?.markPx,
    index_price: index?.idxPx,
    last_funding_rate: funding?.fundingRate ?? funding?.settFundingRate,
    next_funding_time: funding?.nextFundingTime ?? funding?.fundingTime,
    source_time: funding?.ts ?? mark?.ts ?? index?.ts,
    source: 'okx_funding_mark_index',
  });
}

async function fetchBybitContractMeta(symbol) {
  const native = nativeContractSymbol('bybit', symbol);
  const raw = await firstWorkingJson([
    `https://api.bybit.com/v5/market/tickers?category=${bybitCategory(state.symbol)}&symbol=${encodeURIComponent(native)}`,
    `https://api.bytick.com/v5/market/tickers?category=${bybitCategory(state.symbol)}&symbol=${encodeURIComponent(native)}`,
  ], { timeoutMs: 5500 });
  const item = firstDataObject(raw);
  if (!item) throw new Error('bybit_contract_meta_empty');
  return normalizeContractMeta('bybit', symbol, {
    ...item,
    mark_price: item.markPrice,
    index_price: item.indexPrice,
    last_price: item.lastPrice,
    last_funding_rate: item.fundingRate,
    next_funding_time: item.nextFundingTime,
    source_time: raw?.time,
    source: `bybit_${bybitCategory(symbol)}_ticker`,
  });
}

async function fetchBitgetContractMeta(symbol) {
  const native = nativeContractSymbol('bitget', symbol);
  const encoded = encodeURIComponent(native);
  const productType = bitgetProductType(symbol);
  const settled = await Promise.allSettled([
    fetchJson(`https://api.bitget.com/api/v2/mix/market/ticker?symbol=${encoded}&productType=${encodeURIComponent(productType)}`, { timeoutMs: 5500 }),
    fetchJson(`https://api.bitget.com/api/v2/mix/market/current-fund-rate?symbol=${encoded}&productType=${encodeURIComponent(productType)}`, { timeoutMs: 5500 }),
    fetchJson(`https://api.bitget.com/api/v2/mix/market/symbol-price?symbol=${encoded}&productType=${encodeURIComponent(productType)}`, { timeoutMs: 5500 }),
  ]);
  const ticker = settled[0].status === 'fulfilled' ? firstDataObject(settled[0].value) : null;
  const funding = settled[1].status === 'fulfilled' ? firstDataObject(settled[1].value) : null;
  const prices = settled[2].status === 'fulfilled' ? firstDataObject(settled[2].value) : null;
  if (!ticker && !funding && !prices) {
    throw new Error(settled.map((item) => item.status === 'rejected' ? String(item.reason?.message || item.reason) : '').join(' | ').slice(0, 700) || 'bitget_contract_meta_empty');
  }
  return normalizeContractMeta('bitget', symbol, {
    mark_price: prices?.markPrice ?? ticker?.markPrice,
    index_price: prices?.indexPrice ?? ticker?.indexPrice,
    last_price: prices?.lastPr ?? prices?.lastPrice ?? ticker?.lastPr ?? ticker?.lastPrice,
    last_funding_rate: funding?.fundingRate ?? ticker?.fundingRate,
    next_funding_time: funding?.nextUpdate ?? funding?.nextFundingTime ?? ticker?.nextSettleTime,
    source_time: funding?.ts ?? prices?.ts ?? ticker?.ts ?? settled[0]?.value?.requestTime,
    source: 'bitget_contract_ticker_funding_price',
  });
}

async function fetchGateContractMeta(symbol) {
  const contract = gateSymbol(symbol);
  const raw = await firstWorkingJson([
    `https://api.gateio.ws/api/v4/futures/${gateSettle(symbol)}/tickers?contract=${encodeURIComponent(contract)}`,
    `https://fx-api.gateio.ws/api/v4/futures/${gateSettle(symbol)}/tickers?contract=${encodeURIComponent(contract)}`,
  ], { timeoutMs: 5500 });
  const item = Array.isArray(raw) ? raw[0] : firstDataObject(raw);
  if (!item) throw new Error('gate_contract_meta_empty');
  return normalizeContractMeta('gate', symbol, {
    mark_price: item.mark_price,
    index_price: item.index_price,
    last_price: item.last,
    last_funding_rate: item.funding_rate ?? item.funding_rate_indicative,
    source_time: Date.now(),
    source: 'gate_futures_ticker',
  });
}

async function fetchProviderContractMeta(provider, symbol) {
  if (provider === 'binance') return fetchBinanceContractMeta(symbol);
  if (provider === 'okx') return fetchOkxContractMeta(symbol);
  if (provider === 'bybit') return fetchBybitContractMeta(symbol);
  if (provider === 'bitget') return fetchBitgetContractMeta(symbol);
  if (provider === 'gate') return fetchGateContractMeta(symbol);
  throw new Error('unsupported_contract_meta_provider');
}

async function getContractMeta(provider, symbol) {
  const key = `${provider}:${symbol}`;
  const now = Date.now();
  let entry = contractMetaCache.get(key);
  if (!entry) {
    entry = { value: null, fetchedAt: 0, cooldownUntil: 0, promise: null, error: '' };
    contractMetaCache.set(key, entry);
  }
  if (entry.value && now - entry.fetchedAt <= CONTRACT_META_TTL_MS) {
    return { ...entry.value, stale: false, meta_error: '' };
  }
  if (now < entry.cooldownUntil) {
    return entry.value && now - entry.fetchedAt <= CONTRACT_META_STALE_MS
      ? { ...entry.value, stale: true, meta_error: entry.error }
      : null;
  }
  if (!entry.promise) {
    entry.promise = (async () => {
      try {
        const value = await fetchProviderContractMeta(provider, symbol);
        entry.value = value;
        entry.fetchedAt = Date.now();
        entry.cooldownUntil = 0;
        entry.error = '';
        return { ...value, stale: false, meta_error: '' };
      } catch (error) {
        const message = String(error?.message || error).slice(0, 700);
        entry.error = message;
        const restricted = /upstream_(403|418|429|451)|banned|too many requests|restricted|waf|cloudfront/i.test(message);
        entry.cooldownUntil = Date.now() + (restricted ? CONTRACT_META_RESTRICTED_RETRY_MS : CONTRACT_META_RETRY_MS);
        if (entry.value && Date.now() - entry.fetchedAt <= CONTRACT_META_STALE_MS) {
          return { ...entry.value, stale: true, meta_error: message };
        }
        return null;
      } finally {
        entry.promise = null;
      }
    })();
  }
  return entry.promise;
}

function latestMetricRow(state) {
  if (!state || !(state.metricRows instanceof Map) || state.metricRows.size === 0) return null;
  const latestKey = [...state.metricRows.keys()].sort((a, b) => b - a)[0];
  return latestKey ? state.metricRows.get(latestKey) || null : null;
}

function latestOpenInterestMetricRow(state) {
  if (!state || !(state.metricRows instanceof Map) || state.metricRows.size === 0) return null;
  const rows = [...state.metricRows.values()].sort((a, b) => metricRowStart(b) - metricRowStart(a));
  for (const row of rows) {
    const amount = asNumber(row?.open_interest);
    const value = asNumber(row?.open_interest_value);
    if (amount > 0 || value > 0) return row;
  }
  return null;
}

function latestOpenInterestPatch(state) {
  const row = latestOpenInterestMetricRow(state);
  if (!row) return null;
  const amount = asNumber(row.open_interest);
  const value = asNumber(row.open_interest_value);
  const units = openInterestUnitSemantics(state.provider, state.symbol);
  return {
    open_interest: amount > 0 ? amount : null,
    contract_oi: amount > 0 ? amount : null,
    open_interest_value: value > 0 ? value : null,
    contract_oi_value: value > 0 ? value : null,
    open_interest_unit: amount > 0 ? units.open_interest_unit : null,
    open_interest_value_unit: value > 0
      ? units.open_interest_value_unit
      : null,
    open_interest_source_time: row.bucket_time,
  };
}

function mergeContractMetaWithOpenInterest(meta, state) {
  const oi = latestOpenInterestPatch(state);
  if (!meta && !oi) return null;
  return { ...(meta || {}), ...(oi || {}) };
}

function getContractMetaFast(provider, symbol) {
  if (provider === 'binance') {
    const realtime = getBinanceContractRealtimeMeta(symbol);
    if (realtime) {
      return normalizeContractMeta('binance', symbol, {
        ...realtime,
        mark_price: realtime.mark_price,
        index_price: realtime.index_price,
        last_price: realtime.last_price ?? realtime.price,
        last_funding_rate: realtime.last_funding_rate ?? realtime.funding_rate,
        next_funding_time: realtime.next_funding_time,
        source_time: realtime.source_time ?? realtime.cached_at,
        source: 'binance_official_public_mark_price_websocket',
      });
    }
  }
  const entry = contractMetaCache.get(`${provider}:${symbol}`);
  if (entry?.value && Date.now() - Number(entry.fetchedAt || 0) <= CONTRACT_META_STALE_MS) {
    return { ...entry.value, stale: Date.now() - Number(entry.fetchedAt || 0) > CONTRACT_META_TTL_MS, meta_error: entry.error || '' };
  }
  return null;
}

function scheduleContractMetaRefresh(provider, symbol) {
  getContractMeta(provider, symbol).catch?.(() => {});
}

async function refreshBinanceOpenInterestCritical(state) {
  if (!state || state.provider !== 'binance') return metricPayloadFromState(state);
  const native = nativeContractSymbol('binance', state.symbol);
  await loadPersistedMetrics(state);
  const latest = latestOpenInterestMetricRow(state);
  const latestTime = metricRowStart(latest);
  if (latestTime && Date.now() - latestTime <= BINANCE_OI_CRITICAL_TTL_MS) {
    return metricPayloadFromState(state);
  }
  if (state.oiCriticalPromise) return state.oiCriticalPromise;
  state.oiCriticalPromise = (async () => {
    if (!state.oiCriticalScheduledAt) {
      state.oiCriticalScheduledAt = Date.now();
      await new Promise((resolve) => setTimeout(resolve, BINANCE_OI_CRITICAL_DELAY_MS));
    }
    try {
      const rows = await fetchBinanceJson(
        `https://fapi.binance.com/futures/data/openInterestHist?symbol=${encodeURIComponent(native)}&period=5m&limit=3`,
        { source: 'position_metrics:open_interest_first_paint', lane: 'critical', priority: 60 },
      );
      for (const item of Array.isArray(rows) ? rows : []) {
        const row = metricPoint(new Map(), state.provider, state.symbol, item.timestamp);
        row.open_interest = asNumber(item.sumOpenInterest);
        row.open_interest_value = asNumber(item.sumOpenInterestValue);
        const merged = mergeMetricRow(state.metricRows, row);
        if (merged) queueMetricPersist(merged);
      }
      state.metricFetchedAt = Date.now();
      state.metricError = '';
      flushMetricPersistQueue().catch(() => {});
    } catch (error) {
      state.metricError = String(error?.message || error).slice(0, 700);
    } finally {
      state.oiCriticalScheduledAt = 0;
    }
    return metricPayloadFromState(state);
  })().finally(() => { state.oiCriticalPromise = null; });
  return state.oiCriticalPromise;
}


function ratioFamilyFieldNames(prefix) {
  if (prefix === 'global') {
    return {
      ratio: 'global_long_short_ratio',
      long: 'global_long_account',
      short: 'global_short_account',
    };
  }
  return {
    ratio: `${prefix}_long_short_ratio`,
    long: `${prefix}_long`,
    short: `${prefix}_short`,
  };
}

function ratioFamilyFields(prefix) {
  const names = ratioFamilyFieldNames(prefix);
  return [names.ratio, names.long, names.short];
}

function latestRatioFamilyTime(state, prefix) {
  const fields = ratioFamilyFields(prefix);
  let latest = 0;
  for (const row of state.metricRows.values()) {
    const valid = fields.some((field) => {
      const value = asNumber(row?.[field]);
      return value != null && value > 0;
    });
    if (!valid) continue;
    latest = Math.max(latest, metricRowStart(row));
  }
  return latest;
}

function hasFreshRatioFamily(state, prefix, now = Date.now()) {
  const latest = latestRatioFamilyTime(state, prefix);
  return latest > 0 && now - latest <= BINANCE_RATIO_CRITICAL_TTL_MS;
}

async function refreshBinanceLongShortCritical(state) {
  if (!state || state.provider !== 'binance') return metricPayloadFromState(state);
  const native = nativeContractSymbol('binance', state.symbol);
  await loadPersistedMetrics(state);
  const now = Date.now();
  const families = [
    {
      prefix: 'global',
      source: 'position_metrics:global_ratio_first_paint',
      priority: 65,
      url: `https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=${encodeURIComponent(native)}&period=5m&limit=${BINANCE_RATIO_CRITICAL_LIMIT}`,
    },
    {
      prefix: 'top_account',
      source: 'position_metrics:top_account_ratio_first_paint',
      priority: 58,
      url: `https://fapi.binance.com/futures/data/topLongShortAccountRatio?symbol=${encodeURIComponent(native)}&period=5m&limit=${BINANCE_RATIO_CRITICAL_LIMIT}`,
    },
    {
      prefix: 'top_position',
      source: 'position_metrics:top_position_ratio_first_paint',
      priority: 56,
      url: `https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol=${encodeURIComponent(native)}&period=5m&limit=${BINANCE_RATIO_CRITICAL_LIMIT}`,
    },
  ];
  if (families.every((item) => hasFreshRatioFamily(state, item.prefix, now))) {
    return metricPayloadFromState(state);
  }
  if (state.ratioCriticalPromise) return state.ratioCriticalPromise;

  state.ratioCriticalPromise = (async () => {
    let successes = 0;
    const errors = [];
    for (const family of families) {
      if (hasFreshRatioFamily(state, family.prefix)) continue;
      try {
        const rows = await fetchBinanceJson(family.url, {
          source: family.source,
          lane: 'critical',
          priority: family.priority,
        });
        for (const item of Array.isArray(rows) ? rows : []) {
          const row = metricPoint(new Map(), state.provider, state.symbol, item.timestamp);
          applyRatio(row, family.prefix, item.longShortRatio, item.longAccount, item.shortAccount);
          const merged = mergeMetricRow(state.metricRows, row);
          if (merged) queueMetricPersist(merged);
        }
        if (hasFreshRatioFamily(state, family.prefix)) successes += 1;
        else errors.push(`${family.prefix}_ratio_empty`);
      } catch (error) {
        errors.push(`${family.prefix}:${String(error?.message || error)}`);
        if (error?.internalBinanceRestGuard || [403, 418, 429, 451].includes(Number(error?.status || 0))) break;
      }
    }

    state.ratioCriticalFetchedAt = Date.now();
    state.ratioCriticalError = errors.join(' | ').slice(0, 700);
    const status = recentMetricFamilyStatus(state);
    if (successes > 0 || status.global_account || status.top_account || status.top_position) {
      // A partial ratio result is useful and must not be hidden by a later family.
      if (status.global_account) state.metricError = '';
      flushMetricPersistQueue().catch(() => {});
      return metricPayloadFromState(state);
    }
    if (state.ratioCriticalError) state.metricError = state.ratioCriticalError;
    throw new Error(state.ratioCriticalError || 'binance_long_short_first_paint_unavailable');
  })().finally(() => { state.ratioCriticalPromise = null; });

  return state.ratioCriticalPromise;
}

function scheduleVenueMetricsRefresh(state) {
  if (!state || state.metricBackgroundTimer) return;
  const delay = state.provider === 'binance' ? 8_000 : 0;
  state.metricBackgroundTimer = setTimeout(() => {
    state.metricBackgroundTimer = null;
    fetchVenueMetrics(state).catch(() => {});
  }, delay);
  state.metricBackgroundTimer.unref?.();
}

function metricBucketStart(value = Date.now()) {
  const ms = normalizeTime(value) || Date.now();
  return Math.floor(ms / FIVE_MIN_MS) * FIVE_MIN_MS;
}

function emptyMetricRow(provider, symbol, time) {
  const start = metricBucketStart(time);
  return {
    provider, symbol, bucket_time: new Date(start).toISOString(),
    open_interest: null, open_interest_value: null,
    global_long_short_ratio: null, global_long_account: null, global_short_account: null,
    top_account_long_short_ratio: null, top_account_long: null, top_account_short: null,
    top_position_long_short_ratio: null, top_position_long: null, top_position_short: null,
    source: 'render_exchange_public_metrics', updated_at: new Date().toISOString(),
  };
}

function metricKey(row) {
  return `${row.provider}:${row.symbol}:${row.bucket_time}`;
}

function metricRowStart(row) {
  return normalizeTime(row?.bucket_time) || 0;
}

function normalizeMetricRow(raw) {
  const provider = providerKey(raw?.provider);
  const symbol = symbolKey(raw?.symbol);
  const start = normalizeTime(raw?.bucket_time);
  if (!provider || !symbol || !start) return null;
  const row = emptyMetricRow(provider, symbol, start);
  const fields = [
    'open_interest','open_interest_value',
    'global_long_short_ratio','global_long_account','global_short_account',
    'top_account_long_short_ratio','top_account_long','top_account_short',
    'top_position_long_short_ratio','top_position_long','top_position_short',
  ];
  for (const field of fields) {
    const value = asNumber(raw[field]);
    // OI、账户占比与多空比在本模块中都应为正值；旧版由null误转出的0视为缺失。
    row[field] = value != null && value > 0 ? value : null;
  }
  // Step650.8.15 wrote Binance global shares to transient legacy keys
  // global_long/global_short, while the persisted schema and App use
  // global_long_account/global_short_account. Accept those legacy in-memory
  // keys once, but always emit/persist the canonical schema fields.
  if (row.global_long_account == null) {
    const legacyLong = asNumber(raw.global_long);
    if (legacyLong != null && legacyLong > 0) row.global_long_account = legacyLong;
  }
  if (row.global_short_account == null) {
    const legacyShort = asNumber(raw.global_short);
    if (legacyShort != null && legacyShort > 0) row.global_short_account = legacyShort;
  }
  row.source = String(raw?.source || row.source);
  row.updated_at = String(raw?.updated_at || row.updated_at);
  return row;
}

function mergeMetricRow(targetMap, incoming) {
  const row = normalizeMetricRow(incoming);
  if (!row) return null;
  const start = metricRowStart(row);
  const current = targetMap.get(start) || emptyMetricRow(row.provider, row.symbol, start);
  const merged = { ...current };
  for (const [key, value] of Object.entries(row)) {
    if (value != null && value !== '') merged[key] = value;
  }
  targetMap.set(start, merged);
  return merged;
}

function queueMetricPersist(row) {
  if (!PERSISTENCE_ENABLED || !row) return;
  metricPersistQueue.set(metricKey(row), row);
}

async function flushMetricPersistQueue() {
  if (!PERSISTENCE_ENABLED || metricPersistFlushPromise || metricPersistQueue.size === 0) return;
  const rows = [...metricPersistQueue.values()].slice(0, 500);
  for (const row of rows) metricPersistQueue.delete(metricKey(row));
  metricPersistFlushPromise = (async () => {
    try {
      const response = await fetch(`${SUPABASE_URL}/rest/v1/rpc/app_upsert_contract_position_metrics`, {
        method: 'POST',
        headers: {
          apikey: SUPABASE_SERVICE_ROLE_KEY,
          authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          'content-type': 'application/json',
          prefer: 'return=minimal',
        },
        body: JSON.stringify({ p_rows: rows }),
        signal: AbortSignal.timeout(15000),
      });
      if (!response.ok) throw new Error(`metric_persist_http_${response.status}:${(await response.text()).slice(0, 180)}`);
    } catch (error) {
      for (const row of rows) metricPersistQueue.set(metricKey(row), row);
      console.error(`[Step615.5] contract metrics persist failed: ${error?.message || error}`);
    }
  })().finally(() => { metricPersistFlushPromise = null; });
  await metricPersistFlushPromise;
}

async function loadPersistedMetrics(state) {
  if (!PERSISTENCE_ENABLED || state.metricLoaded || state.metricLoading) return;
  state.metricLoading = true;
  try {
    const cutoff = new Date(Date.now() - METRIC_HISTORY_MS - FIVE_MIN_MS).toISOString();
    const query = new URLSearchParams({
      select: '*', provider: `eq.${state.provider}`, symbol: `eq.${state.symbol}`,
      bucket_time: `gte.${cutoff}`, order: 'bucket_time.asc', limit: '400',
    });
    const response = await fetch(`${SUPABASE_URL}/rest/v1/${METRIC_TABLE}?${query}`, {
      headers: { apikey: SUPABASE_SERVICE_ROLE_KEY, authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}` },
      signal: AbortSignal.timeout(12000),
    });
    const text = await response.text();
    if (!response.ok) throw new Error(`metric_history_http_${response.status}:${text.slice(0, 180)}`);
    const rows = JSON.parse(text);
    for (const raw of Array.isArray(rows) ? rows : []) mergeMetricRow(state.metricRows, raw);
    state.metricLoaded = true;
    state.metricError = '';
  } catch (error) {
    state.metricError = String(error?.message || error).slice(0, 220);
  } finally {
    state.metricLoading = false;
  }
}

function metricPoint(map, provider, symbol, time) {
  const start = metricBucketStart(time);
  let row = map.get(start);
  if (!row) {
    row = emptyMetricRow(provider, symbol, start);
    map.set(start, row);
  }
  return row;
}

function applyRatio(row, prefix, ratio, longShare, shortShare) {
  const r = asNumber(ratio);
  const l = asNumber(longShare);
  const sh = asNumber(shortShare);
  const fields = ratioFamilyFieldNames(prefix);
  if (r != null) row[fields.ratio] = r;
  if (l != null) row[fields.long] = l;
  if (sh != null) row[fields.short] = sh;
  if (r == null && l != null && sh != null && sh > 0) row[fields.ratio] = l / sh;
}

function applyRatioFromParts(row, prefix, ratio, longPart, shortPart) {
  const r = asNumber(ratio);
  const longValue = asNumber(longPart);
  const shortValue = asNumber(shortPart);
  let longShare = null;
  let shortShare = null;
  if (longValue != null && shortValue != null && longValue >= 0 && shortValue >= 0) {
    const total = longValue + shortValue;
    if (total > 0) {
      longShare = longValue / total;
      shortShare = shortValue / total;
    }
  }
  applyRatio(row, prefix, r, longShare, shortShare);
  if (r == null && longValue != null && shortValue != null && shortValue > 0) {
    row[ratioFamilyFieldNames(prefix).ratio] = longValue / shortValue;
  }
}

async function firstWorkingNonEmptyDataJson(urls, options = {}) {
  const errors = [];
  for (const url of urls) {
    try {
      const payload = await fetchJson(url, options);
      const data = payload?.data;
      if (Array.isArray(data) && data.length > 0) return payload;
      errors.push(`empty_data:${url}`);
    } catch (error) {
      errors.push(String(error?.message || error));
    }
  }
  throw new Error(errors.join(' | ').slice(0, 700) || 'all_upstreams_empty');
}

async function firstWorkingJson(urls, options = {}) {
  const errors = [];
  for (const url of urls) {
    try { return await fetchJson(url, options); }
    catch (error) { errors.push(String(error?.message || error)); }
  }
  throw new Error(errors.join(' | ').slice(0, 700) || 'all_upstreams_failed');
}

async function fetchBinanceMetricRows(state) {
  const native = nativeContractSymbol('binance', state.symbol);
  const host = 'https://fapi.binance.com';
  const headers = BINANCE_API_KEY ? { 'X-MBX-APIKEY': BINANCE_API_KEY } : {};
  // Step650.8.15.14: metrics are requested sequentially under the shared governor.
  // This avoids filling a bounded queue with four calls that must already wait
  // ten seconds between starts. A restricted or unsafe-weight response stops the
  // remaining calls before they touch Binance.
  const metricRequests = [
    [`${host}/futures/data/openInterestHist?symbol=${encodeURIComponent(native)}&period=5m&limit=288`, 'position_metrics:open_interest'],
    [`${host}/futures/data/globalLongShortAccountRatio?symbol=${encodeURIComponent(native)}&period=5m&limit=288`, 'position_metrics:global_ratio'],
    [`${host}/futures/data/topLongShortAccountRatio?symbol=${encodeURIComponent(native)}&period=5m&limit=288`, 'position_metrics:top_account_ratio'],
    [`${host}/futures/data/topLongShortPositionRatio?symbol=${encodeURIComponent(native)}&period=5m&limit=288`, 'position_metrics:top_position_ratio'],
  ];
  const settled = [];
  for (const [url, source] of metricRequests) {
    try {
      settled.push({ status: 'fulfilled', value: await fetchBinanceJson(url, { headers, timeoutMs: 6500, source, lane: 'auxiliary', priority: 10 }) });
    } catch (error) {
      settled.push({ status: 'rejected', reason: error });
      if (error?.internalBinanceRestGuard || [403, 418, 429, 451].includes(Number(error?.status || 0))) break;
    }
  }
  while (settled.length < metricRequests.length) {
    settled.push({ status: 'rejected', reason: new Error('binance_metrics_skipped_by_shared_guard') });
  }
  if (!settled.some((item) => item.status === 'fulfilled')) {
    throw new Error(settled.map((item) => item.status === 'rejected' ? item.reason?.message : '').join(' | ') || 'binance_metrics_unavailable');
  }
  const map = new Map();
  const oi = settled[0].status === 'fulfilled' && Array.isArray(settled[0].value) ? settled[0].value : [];
  for (const item of oi) {
    const row = metricPoint(map, state.provider, state.symbol, item.timestamp);
    row.open_interest = asNumber(item.sumOpenInterest);
    row.open_interest_value = asNumber(item.sumOpenInterestValue);
  }
  const parseRatioList = (index, prefix) => {
    const list = settled[index].status === 'fulfilled' && Array.isArray(settled[index].value) ? settled[index].value : [];
    for (const item of list) {
      const row = metricPoint(map, state.provider, state.symbol, item.timestamp);
      applyRatio(row, prefix, item.longShortRatio, item.longAccount, item.shortAccount);
    }
  };
  parseRatioList(1, 'global');
  parseRatioList(2, 'top_account');
  parseRatioList(3, 'top_position');
  return [...map.values()];
}

async function fetchOkxMetricRows(state) {
  const [base] = splitSymbol(state.symbol);
  const instId = okxInstId(state.symbol);
  const now = Date.now();
  const begin = now - METRIC_HISTORY_MS;
  const settled = await Promise.allSettled([
    firstWorkingNonEmptyDataJson([
      `https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId=${encodeURIComponent(instId)}`,
      `https://aws.okx.com/api/v5/public/open-interest?instType=SWAP&instId=${encodeURIComponent(instId)}`,
    ]),
    firstWorkingNonEmptyDataJson([
      `https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-volume?ccy=${encodeURIComponent(base)}&period=5m&begin=${begin}&end=${now}`,
      `https://aws.okx.com/api/v5/rubik/stat/contracts/open-interest-volume?ccy=${encodeURIComponent(base)}&period=5m&begin=${begin}&end=${now}`,
      `https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-volume?ccy=${encodeURIComponent(base)}&period=5m`,
      `https://aws.okx.com/api/v5/rubik/stat/contracts/open-interest-volume?ccy=${encodeURIComponent(base)}&period=5m`,
    ]),
    firstWorkingNonEmptyDataJson([
      `https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy=${encodeURIComponent(base)}&period=5m&begin=${begin}&end=${now}`,
      `https://aws.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy=${encodeURIComponent(base)}&period=5m&begin=${begin}&end=${now}`,
      `https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy=${encodeURIComponent(base)}&period=5m`,
      `https://aws.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy=${encodeURIComponent(base)}&period=5m`,
    ]),
  ]);
  if (!settled.some((item) => item.status === 'fulfilled')) throw new Error('okx_metrics_unavailable');
  const map = new Map();
  if (settled[1].status === 'fulfilled') {
    for (const item of Array.isArray(settled[1].value?.data) ? settled[1].value.data : []) {
      const ts = Array.isArray(item) ? item[0] : item.ts;
      const row = metricPoint(map, state.provider, state.symbol, ts);
      if (Array.isArray(item)) {
        const value = asNumber(item[1]);
        if (value != null) row.open_interest_value = value;
      } else {
        const amount = asNumber(item.oi ?? item.openInterest);
        const value = asNumber(item.oiUsd ?? item.openInterestUsd);
        if (amount != null) row.open_interest = amount;
        if (value != null) row.open_interest_value = value;
      }
    }
  }
  if (settled[2].status === 'fulfilled') {
    for (const item of Array.isArray(settled[2].value?.data) ? settled[2].value.data : []) {
      const ts = Array.isArray(item) ? item[0] : item.ts;
      const ratio = asNumber(Array.isArray(item) ? item[1] : (item.ratio ?? item.longShortRatio));
      const row = metricPoint(map, state.provider, state.symbol, ts);
      applyRatio(row, 'global', ratio, null, null);
    }
  }
  if (settled[0].status === 'fulfilled') {
    for (const item of Array.isArray(settled[0].value?.data) ? settled[0].value.data : []) {
      const row = metricPoint(map, state.provider, state.symbol, item.ts ?? now);
      row.open_interest = asNumber(item.oiCcy ?? item.openInterestCcy);
      row.open_interest_value = asNumber(item.oiUsd);
    }
  }
  return [...map.values()];
}

async function fetchBybitMetricRows(state) {
  const native = nativeContractSymbol('bybit', state.symbol);
  const [, quote] = splitSymbol(state.symbol);
  const hosts = ['https://api.bybit.com','https://api.bytick.com'];
  const accountRatioOfficiallyAvailable = quote === 'USDT';
  const requests = [
    firstWorkingJson(hosts.map((host) => `${host}/v5/market/open-interest?category=${bybitCategory(state.symbol)}&symbol=${encodeURIComponent(native)}&intervalTime=5min&limit=200`), { timeoutMs: 7000 }),
  ];
  if (accountRatioOfficiallyAvailable) {
    requests.push(
      firstWorkingJson(hosts.map((host) => `${host}/v5/market/account-ratio?category=${bybitCategory(state.symbol)}&symbol=${encodeURIComponent(native)}&period=5min&limit=500`), { timeoutMs: 7000 }),
    );
  }
  const settled = await Promise.allSettled(requests);
  if (settled[0]?.status !== 'fulfilled') throw new Error('bybit_open_interest_unavailable');
  const map = new Map();
  const inverse = quote === 'USD';
  for (const item of Array.isArray(settled[0].value?.result?.list) ? settled[0].value.result.list : []) {
    const row = metricPoint(map, state.provider, state.symbol, item.timestamp);
    row.open_interest = asNumber(item.openInterest);
    if (row.open_interest != null && state.lastPrice && state.lastPrice > 0) {
      // Bybit open-interest endpoint: inverse BTCUSD amount is in USD;
      // linear BTCUSDT amount is in base coin.
      row.open_interest_value = inverse
        ? row.open_interest / state.lastPrice
        : row.open_interest * state.lastPrice;
    }
  }
  if (accountRatioOfficiallyAvailable && settled[1]?.status === 'fulfilled') {
    for (const item of Array.isArray(settled[1].value?.result?.list) ? settled[1].value.result.list : []) {
      const row = metricPoint(map, state.provider, state.symbol, item.timestamp);
      applyRatio(row, 'global', item.longShortRatio, item.buyRatio, item.sellRatio);
    }
  }
  return [...map.values()];
}

async function fetchBitgetMetricRows(state) {
  const symbol = encodeURIComponent(nativeContractSymbol('bitget', state.symbol));
  const productType = bitgetProductType(state.symbol).toLowerCase();
  const settled = await Promise.allSettled([
    fetchJson(`https://api.bitget.com/api/v2/mix/market/open-interest?symbol=${symbol}&productType=${encodeURIComponent(productType)}`, { timeoutMs: 7000 }),
    fetchJson(`https://api.bitget.com/api/v3/market/futures-long-short?symbol=${symbol}&period=5m`, { timeoutMs: 7000 }),
    fetchJson(`https://api.bitget.com/api/v3/market/futures-account-long-short?symbol=${symbol}&period=5m`, { timeoutMs: 7000 }),
    fetchJson(`https://api.bitget.com/api/v3/market/futures-position-long-short?symbol=${symbol}&period=5m`, { timeoutMs: 7000 }),
  ]);
  if (!settled.some((item) => item.status === 'fulfilled')) throw new Error('bitget_metrics_unavailable');
  const map = new Map();
  if (settled[0].status === 'fulfilled') {
    const data = settled[0].value?.data;
    const list = Array.isArray(data?.openInterestList) ? data.openInterestList : (Array.isArray(data) ? data : data ? [data] : []);
    for (const item of list) {
      const row = metricPoint(map, state.provider, state.symbol, item.ts ?? item.timestamp ?? Date.now());
      row.open_interest = asNumber(item.size ?? item.openInterest ?? item.amount);
      row.open_interest_value = asNumber(item.openInterestUsd ?? item.usdValue);
      if (row.open_interest_value == null && row.open_interest != null && state.lastPrice) row.open_interest_value = row.open_interest * state.lastPrice;
    }
  }
  const parseBitgetList = (settledItem, prefix, ratioKeys, longKeys, shortKeys) => {
    if (settledItem.status !== 'fulfilled') return;
    const data = settledItem.value?.data;
    const list = Array.isArray(data) ? data : (Array.isArray(data?.list) ? data.list : data ? [data] : []);
    for (const item of list) {
      const row = metricPoint(map, state.provider, state.symbol, item.ts ?? item.timestamp ?? item.time ?? Date.now());
      const ratio = ratioKeys.map((key) => item[key]).find((value) => value != null);
      const longPart = longKeys.map((key) => item[key]).find((value) => value != null);
      const shortPart = shortKeys.map((key) => item[key]).find((value) => value != null);
      applyRatioFromParts(row, prefix, ratio, longPart, shortPart);
    }
  };
  parseBitgetList(settled[1], 'global', ['longShortRatio','ratio'], ['longRatio','buyRatio','longAccountRatio'], ['shortRatio','sellRatio','shortAccountRatio']);
  parseBitgetList(settled[2], 'top_account', ['longShortAccountRatio','longShortRatio','ratio'], ['longAccountRatio','buyRatio','longRatio'], ['shortAccountRatio','sellRatio','shortRatio']);
  parseBitgetList(settled[3], 'top_position', ['longShortPositionRatio','longShortRatio','ratio'], ['longPositionRatio','longRatio'], ['shortPositionRatio','shortRatio']);
  return [...map.values()];
}

async function fetchGateMetricRows(state) {
  const contract = gateSymbol(state.symbol);
  const multiplier = await loadGateContractMultiplier(state);
  const raw = await firstWorkingJson([
    `https://api.gateio.ws/api/v4/futures/${gateSettle(state.symbol)}/contract_stats?contract=${encodeURIComponent(contract)}&interval=5m&limit=288`,
    `https://fx-api.gateio.ws/api/v4/futures/${gateSettle(state.symbol)}/contract_stats?contract=${encodeURIComponent(contract)}&interval=5m&limit=288`,
  ], { timeoutMs: 8000 });
  const map = new Map();
  for (const item of Array.isArray(raw) ? raw : []) {
    const row = metricPoint(map, state.provider, state.symbol, item.time ?? item.timestamp);
    const contracts = asNumber(item.open_interest);
    row.open_interest = contracts == null ? null : contracts * multiplier;
    row.open_interest_value = asNumber(item.open_interest_usd);
    applyRatioFromParts(row, 'global', item.long_short_ratio, item.long_users ?? item.long_ratio, item.short_users ?? item.short_ratio);
    applyRatioFromParts(row, 'top_account', item.top_long_short_account_ratio, item.top_long_account, item.top_short_account);
    applyRatioFromParts(row, 'top_position', item.top_long_short_position_ratio, item.top_long_size, item.top_short_size);
  }
  return [...map.values()];
}

async function fetchProviderMetricRows(state) {
  if (state.provider === 'binance') return fetchBinanceMetricRows(state);
  if (state.provider === 'okx') return fetchOkxMetricRows(state);
  if (state.provider === 'bybit') return fetchBybitMetricRows(state);
  if (state.provider === 'bitget') return fetchBitgetMetricRows(state);
  if (state.provider === 'gate') return fetchGateMetricRows(state);
  return [];
}


function recentMetricFamilyStatus(state) {
  const cutoff = Date.now() - 30 * 60 * 1000;
  const recent = [...state.metricRows.values()].filter((row) => metricRowStart(row) >= cutoff);
  const has = (fields) => recent.some((row) => fields.some((field) => {
    const value = asNumber(row[field]);
    return value != null && value > 0;
  }));
  const status = {
    oi: has(['open_interest', 'open_interest_value']),
    global_account: has(['global_long_short_ratio', 'global_long_account', 'global_short_account']),
    top_account: has(['top_account_long_short_ratio', 'top_account_long', 'top_account_short']),
    top_position: has(['top_position_long_short_ratio', 'top_position_long', 'top_position_short']),
  };
  const [, quote] = splitSymbol(state.symbol);
  const bybitUsdcAccountRatioUnavailable =
    state.provider === 'bybit' && quote !== 'USDT';
  const required = state.provider === 'binance' || state.provider === 'gate'
    ? ['oi', 'global_account', 'top_account', 'top_position']
    : bybitUsdcAccountRatioUnavailable
      ? ['oi']
      : ['oi', 'global_account'];
  return {
    ...status,
    global_account_official_unavailable: bybitUsdcAccountRatioUnavailable,
    unavailable_reason: bybitUsdcAccountRatioUnavailable
      ? 'bybit_public_account_ratio_is_usdt_linear_contract_only'
      : '',
    required,
    complete: required.every((key) => status[key] === true),
  };
}

function metricPayloadFromState(state) {
  const cutoff = Date.now() - METRIC_HISTORY_MS;
  const rows = [...state.metricRows.values()]
    .filter((row) => metricRowStart(row) >= cutoff)
    .sort((a, b) => metricRowStart(a) - metricRowStart(b))
    .slice(-288);
  const oiRows = [];
  const ratioRows = [];
  const oiUnits = openInterestUnitSemantics(state.provider, state.symbol);
  for (const row of rows) {
    const sourceTime = row.bucket_time;
    const validOi = asNumber(row.open_interest) != null && asNumber(row.open_interest) > 0;
    const validOiValue = asNumber(row.open_interest_value) != null && asNumber(row.open_interest_value) > 0;
    if (validOi || validOiValue) {
      oiRows.push({
        source_time: sourceTime,
        open_interest: validOi ? row.open_interest : null,
        open_interest_value: validOiValue ? row.open_interest_value : null,
        open_interest_unit: validOi ? oiUnits.open_interest_unit : null,
        open_interest_value_unit: validOiValue
          ? oiUnits.open_interest_value_unit
          : null,
      });
    }
    const ratios = [
      ['global_account', row.global_long_short_ratio, row.global_long_account, row.global_short_account],
      ['top_account', row.top_account_long_short_ratio, row.top_account_long, row.top_account_short],
      ['top_position', row.top_position_long_short_ratio, row.top_position_long, row.top_position_short],
    ];
    for (const [type, ratio, longShare, shortShare] of ratios) {
      const validRatio = asNumber(ratio) != null && asNumber(ratio) > 0;
      const validLong = asNumber(longShare) != null && asNumber(longShare) > 0;
      const validShort = asNumber(shortShare) != null && asNumber(shortShare) > 0;
      if (!validRatio && !validLong && !validShort) continue;
      ratioRows.push({
        source_time: sourceTime,
        ratio_type: type,
        long_short_ratio: validRatio ? ratio : null,
        long_account: validLong ? longShare : null,
        short_account: validShort ? shortShare : null,
      });
    }
  }
  return {
    oi_rows: oiRows,
    ratio_rows: ratioRows,
    open_interest_unit: oiUnits.open_interest_unit,
    open_interest_value_unit: oiUnits.open_interest_value_unit,
    metric_error: state.metricError,
    metric_updated_at: rows.at(-1)?.bucket_time || null,
    metric_status: recentMetricFamilyStatus(state),
  };
}

async function fetchVenueMetrics(state) {
  await loadPersistedMetrics(state);
  const now = Date.now();
  const latest = [...state.metricRows.keys()].sort((a, b) => b - a)[0] || 0;
  const statusBefore = recentMetricFamilyStatus(state);
  const fullFresh = statusBefore.complete && latest && now - latest < METRIC_REFRESH_MS + 30000;
  const partialRetryInterval = state.metricPartialRetryCount < 2 ? 60000 : METRIC_REFRESH_MS;
  const partialRetryFresh = !statusBefore.complete && state.metricFetchedAt && now - state.metricFetchedAt < partialRetryInterval;
  if (fullFresh || partialRetryFresh || now < state.metricCooldownUntil) return metricPayloadFromState(state);
  if (!state.metricFetchPromise) {
    state.metricFetchPromise = (async () => {
      try {
        const rows = await fetchProviderMetricRows(state);
        if (!rows.length) throw new Error(`${state.provider}_metrics_empty`);
        for (const raw of rows) {
          const merged = mergeMetricRow(state.metricRows, raw);
          if (merged) queueMetricPersist(merged);
        }
        state.metricFetchedAt = Date.now();
        state.metricError = '';
        state.metricCooldownUntil = 0;
        const statusAfter = recentMetricFamilyStatus(state);
        state.metricPartialRetryCount = statusAfter.complete ? 0 : Math.min(3, state.metricPartialRetryCount + 1);
        await flushMetricPersistQueue();
      } catch (error) {
        const message = String(error?.message || error).slice(0, 700);
        state.metricError = message;
        state.metricFetchedAt = Date.now();
        state.metricPartialRetryCount = Math.min(3, state.metricPartialRetryCount + 1);
        const restricted = /upstream_(403|418|429|451)|banned|restricted|waf|cloudfront/i.test(message);
        state.metricCooldownUntil = Date.now() + (restricted ? 30 * 60 * 1000 : 5 * 60 * 1000);
      }
    })().finally(() => { state.metricFetchPromise = null; });
  }
  await state.metricFetchPromise;
  return metricPayloadFromState(state);
}


function mergeRowsFor24h(state) {
  finalizeReadyBuckets(state);
  const cutoff = Date.now() - HISTORY_MS;
  const byStart = new Map();
  for (const row of state.completedBuckets.values()) if (row.end >= cutoff) byStart.set(row.start, row);
  for (const row of provisionalRows(state)) if (row.end >= cutoff) byStart.set(row.start, row);
  return [...byStart.values()].sort((a, b) => a.start - b.start);
}

function summarize(state, venueMetrics = { oi_rows: [], ratio_rows: [] }) {
  const rows = mergeRowsFor24h(state);
  const firstTime = rows[0]?.start || 0;
  const lastTime = rows.at(-1)?.end || 0;
  const coverageMs = firstTime && lastTime ? Math.max(0, Math.min(HISTORY_MS, lastTime - firstTime)) : 0;
  const completeCount = rows.filter((row) => row.end <= Date.now()).length;
  const historyComplete = coverageMs >= HISTORY_MS - 15 * 60 * 1000 && completeCount >= 276;
  const bucketMs = chooseBucketMs(coverageMs);
  const grouped = new Map();
  for (const row of rows) {
    const start = Math.floor(row.start / bucketMs) * bucketMs;
    let bucket = grouped.get(start);
    if (!bucket) bucket = { start, end: start + bucketMs, buy: 0, sell: 0, samples: 0 };
    bucket.buy += row.buy_quote; bucket.sell += row.sell_quote; bucket.samples += row.trade_count;
    grouped.set(start, bucket);
  }
  let flowBuckets = [...grouped.values()].sort((a,b)=>a.start-b.start);
  if (flowBuckets.length > 24) flowBuckets = flowBuckets.slice(-24);
  flowBuckets = flowBuckets.map((bucket) => ({
    start_time_ms: bucket.start, end_time_ms: bucket.end,
    buy_quote_volume: bucket.buy, sell_quote_volume: bucket.sell,
    net_quote_volume: bucket.buy - bucket.sell, samples: bucket.samples,
  }));

  const distribution = { large_buy_quote: 0, large_sell_quote: 0, regular_buy_quote: 0, regular_sell_quote: 0 };
  const tierMap = new Map([
    ['large', { tier:'large', buy_quote:0, sell_quote:0, buy_count:0, sell_count:0 }],
    ['medium', { tier:'medium', buy_quote:0, sell_quote:0, buy_count:0, sell_count:0 }],
    ['small', { tier:'small', buy_quote:0, sell_quote:0, buy_count:0, sell_count:0 }],
  ]);
  let p70Weighted = 0, p95Weighted = 0, thresholdWeight = 0, tradeCount = 0;
  for (const row of rows) {
    distribution.large_buy_quote += row.large_buy_quote; distribution.large_sell_quote += row.large_sell_quote;
    distribution.regular_buy_quote += row.medium_buy_quote + row.small_buy_quote;
    distribution.regular_sell_quote += row.medium_sell_quote + row.small_sell_quote;
    for (const tierName of ['large','medium','small']) {
      const tier = tierMap.get(tierName);
      tier.buy_quote += row[`${tierName}_buy_quote`]; tier.sell_quote += row[`${tierName}_sell_quote`];
      tier.buy_count += row[`${tierName}_buy_count`]; tier.sell_count += row[`${tierName}_sell_count`];
    }
    const weight = Math.max(1, row.trade_count); p70Weighted += row.p70_quote * weight; p95Weighted += row.p95_quote * weight; thresholdWeight += weight;
    tradeCount += row.trade_count;
  }
  const tiers = [...tierMap.values()].map((tier) => ({ ...tier, net_quote:tier.buy_quote-tier.sell_quote, trade_count:tier.buy_count+tier.sell_count }));
  const totalBuy = rows.reduce((sum,row)=>sum+row.buy_quote,0);
  const totalSell = rows.reduce((sum,row)=>sum+row.sell_quote,0);
  const takerRows = flowBuckets.map((bucket) => ({
    source_time:new Date(bucket.start_time_ms).toISOString(), open_time:new Date(bucket.start_time_ms).toISOString(),
    buy_quote_volume:bucket.buy_quote_volume, sell_quote_volume:bucket.sell_quote_volume,
    buy_sell_ratio:bucket.sell_quote_volume>0?bucket.buy_quote_volume/bucket.sell_quote_volume:null, sample_count:bucket.samples,
  }));
  let cumulative=0;
  const cvdRows = flowBuckets.map((bucket)=>{ cumulative += bucket.net_quote_volume; return {
    source_time:new Date(bucket.start_time_ms).toISOString(), open_time:new Date(bucket.start_time_ms).toISOString(),
    delta_quote:bucket.net_quote_volume, delta_volume:bucket.net_quote_volume, cvd_quote:cumulative, cvd:cumulative,
  };});
  return {
    ok:true, version:VERSION, provider:state.provider, symbol:state.symbol,
    source:'render_exchange_websocket_supabase_5m', scope:historyComplete?'rolling_24h':'building_24h', history_complete:historyComplete,
    persistence_enabled:PERSISTENCE_ENABLED, history_loaded:state.historyLoaded, history_error:state.historyError,
    stream_status:state.status, stream_error:state.error, trade_count:tradeCount,
    sample_started_at_ms:firstTime, sample_ended_at_ms:lastTime, coverage_ms:coverageMs, bucket_ms:bucketMs,
    stored_5m_buckets:rows.length, expected_5m_buckets:288, coverage_ratio:Math.min(1, rows.length/288),
    thresholds:{ p70_quote:thresholdWeight?p70Weighted/thresholdWeight:0, p95_quote:thresholdWeight?p95Weighted/thresholdWeight:0 },
    distribution, tiers, flow_buckets:flowBuckets,
    totals:{ buy_quote:totalBuy, sell_quote:totalSell, net_quote:totalBuy-totalSell },
    metrics:{ oi_rows:Array.isArray(venueMetrics.oi_rows)?venueMetrics.oi_rows:[], ratio_rows:Array.isArray(venueMetrics.ratio_rows)?venueMetrics.ratio_rows:[], taker_rows:takerRows, cvd_rows:cvdRows },
    generated_at:new Date().toISOString(),
  };
}

function sendJson(res,status,body){res.writeHead(status,{'content-type':'application/json; charset=utf-8','cache-control':'no-store','access-control-allow-origin':'*'});res.end(JSON.stringify(body));}
function waitForTrades(state,minTrades,waitMs){
  const count=()=>[...state.openBuckets.values()].reduce((sum,b)=>sum+(b.tradeCount||0),0);
  if(count()>=minTrades||waitMs<=0)return Promise.resolve();
  return new Promise((resolve)=>{let timer;const done=()=>{clearTimeout(timer);state.waiters.delete(check);resolve();};const check=()=>{if(count()>=minTrades)done();};state.waiters.add(check);timer=setTimeout(done,waitMs);});
}

export function getContractFlowHealth() {
  pruneBinanceFlowConnectAttempts();
  return {
    streams: states.size,
    binance_active_streams: [...states.values()].filter((state) => state.provider === 'binance').length,
    binance_max_active_streams: BINANCE_FLOW_MAX_STATES,
    binance_ws_connect_gap_ms: BINANCE_FLOW_CONNECT_GAP_MS,
    binance_ws_max_connect_attempts_5m: BINANCE_FLOW_MAX_CONNECT_ATTEMPTS_5M,
    binance_ws_connect_attempts_in_window: binanceFlowConnectAttempts.length,
    binance_ws_connect_attempts_total: binanceFlowWsStats.attempts,
    binance_ws_connect_waits: binanceFlowWsStats.waits,
    binance_ws_connect_window_blocks: binanceFlowWsStats.window_blocks,
    binance_ws_capacity_rejections: binanceFlowWsStats.capacity_rejections,
    production_ws_only: true,
    websocket_ingress: Object.fromEntries(
      Object.entries(flowWsTrafficByProvider).map(([provider, value]) => [provider, { ...value }]),
    ),
  };
}

export async function handleContractFlow(req,res,url){
  if(url.pathname==='/api/contract-flow/health'){
    sendJson(res,200,{ok:true,version:VERSION,streams:states.size,persistence_enabled:PERSISTENCE_ENABLED,persist_queue:persistQueue.size,metric_persist_queue:metricPersistQueue.size,metric_table:METRIC_TABLE,flow_memory_mode:'fixed_histogram',max_active_streams:MAX_ACTIVE_STATES,binance_active_streams:[...states.values()].filter((state)=>state.provider==='binance').length,binance_max_active_streams:BINANCE_FLOW_MAX_STATES,binance_ws_connect_gap_ms:BINANCE_FLOW_CONNECT_GAP_MS,binance_ws_max_connect_attempts_5m:BINANCE_FLOW_MAX_CONNECT_ATTEMPTS_5M,binance_ws_connect_attempts_in_window:(pruneBinanceFlowConnectAttempts(),binanceFlowConnectAttempts.length),binance_ws_connect_attempts_total:binanceFlowWsStats.attempts,binance_ws_connect_waits:binanceFlowWsStats.waits,binance_ws_connect_window_blocks:binanceFlowWsStats.window_blocks,binance_ws_capacity_rejections:binanceFlowWsStats.capacity_rejections,metric_merge_mode:'coalesce_non_null',contract_meta_cache:contractMetaCache.size,contract_meta_ttl_seconds:30,contract_meta_stale_seconds:1800,binance_meta_first_paint:'mark_price_websocket',binance_oi_first_paint:'critical_background_edge_relay',binance_long_short_first_paint:'critical_edge_relay_global_first',binance_long_short_first_paint_wait_ms:BINANCE_RATIO_FIRST_PAINT_WAIT_MS,binance_long_short_history_limit:BINANCE_RATIO_CRITICAL_LIMIT,binance_global_ratio_schema:'global_long_account_global_short_account',binance_global_ratio_legacy_keys_accepted:true,binance_metric_native_symbol_scope_fix:true,bybit_non_usdt_account_ratio_official_unavailable:true,bybit_non_usdt_account_ratio_substitution:'none',flow_first_paint_waits_for_full_metrics:false,usdc_native_identity:true,usd_inverse_native_identity:true,bybit_usdc_native:'BTCPERP',bitget_usdc_native:'BTCPERP',bitget_usdc_product_type:'USDC-FUTURES',bitget_usd_product_type:'COIN-FUTURES',bybit_usd_category:'inverse',gate_usd_settle:'btc',okx_contract_value:true,okx_unit_source:'v2',gate_contract_multiplier:true,open_interest_unit_metadata:true,bybit_inverse_open_interest_unit:'quote_asset',bybit_inverse_open_interest_value_unit:'base_asset',bybit_inverse_open_interest_value_formula:'open_interest_div_last_price',core_symbols:CORE_SYMBOLS,time:new Date().toISOString()});return true;
  }
  if(url.pathname==='/api/contract-meta'){
    if(req.method!=='GET'&&req.method!=='POST'){sendJson(res,405,{ok:false,error:'method_not_allowed'});return true;}
    let provider=providerKey(url.searchParams.get('provider'));let symbol=symbolKey(url.searchParams.get('symbol'));
    if(req.method==='POST'){const chunks=[];for await(const chunk of req)chunks.push(chunk);try{const body=JSON.parse(Buffer.concat(chunks).toString('utf8')||'{}');provider=providerKey(body.provider)||provider;symbol=symbolKey(body.symbol)||symbol;}catch(_){}}
    if(!provider||!symbol||!supportsNativeContract(provider,symbol)){sendJson(res,400,{ok:false,version:VERSION,error:'invalid_provider_or_symbol'});return true;}
    const state=getState(provider,symbol);
    loadPersistedMetrics(state).catch(()=>{});
    if(provider==='binance'){
      const oiPromise=refreshBinanceOpenInterestCritical(state);
      if(!latestOpenInterestPatch(state)){
        try{await Promise.race([oiPromise,new Promise((resolve)=>setTimeout(resolve,3200))]);}catch(_){}
      }
    }
    let meta=getContractMetaFast(provider,symbol);
    if(!meta){
      try{meta=await Promise.race([getContractMeta(provider,symbol),new Promise((resolve)=>setTimeout(()=>resolve(null),provider==='binance'?1800:5200))]);}catch(_){}
    }
    meta=mergeContractMetaWithOpenInterest(meta,state);
    if(meta){
      sendJson(res,200,{ok:true,version:VERSION,provider,symbol,native_symbol:nativeContractSymbol(provider,symbol),contract_meta:meta,partial:meta.open_interest==null&&meta.open_interest_value==null,background_refresh:true});
    }else{
      scheduleContractMetaRefresh(provider,symbol);
      sendJson(res,200,{ok:true,version:VERSION,provider,symbol,native_symbol:nativeContractSymbol(provider,symbol),contract_meta:null,partial:true,background_refresh:true,warning:'contract_meta_warming'});
    }
    return true;
  }
  if(url.pathname==='/api/contract-flow/warm'){
    let started=0;
    for(const provider of PROVIDERS)for(const symbol of CORE_SYMBOLS){const state=getState(provider,symbol);state.lastRequestedAt=Date.now();started+=1;}
    sendJson(res,200,{ok:true,version:VERSION,started,persistence_enabled:PERSISTENCE_ENABLED,core_symbols:CORE_SYMBOLS,time:new Date().toISOString()});return true;
  }
  if(url.pathname!=='/api/contract-flow')return false;
  if(req.method!=='GET'&&req.method!=='POST'){sendJson(res,405,{ok:false,error:'method_not_allowed'});return true;}
  let provider=providerKey(url.searchParams.get('provider'));let symbol=symbolKey(url.searchParams.get('symbol'));let waitMs=Math.min(5000,Math.max(0,Number(url.searchParams.get('wait_ms')||3200)));
  if(req.method==='POST'){const chunks=[];for await(const chunk of req)chunks.push(chunk);try{const body=JSON.parse(Buffer.concat(chunks).toString('utf8')||'{}');provider=providerKey(body.provider)||provider;symbol=symbolKey(body.symbol)||symbol;if(Number.isFinite(Number(body.wait_ms)))waitMs=Math.min(5000,Math.max(0,Number(body.wait_ms)));}catch(_){}}
  if(!provider||!symbol||!supportsNativeContract(provider,symbol)){sendJson(res,400,{ok:false,version:VERSION,error:'invalid_provider_or_symbol'});return true;}

  const state=getState(provider,symbol);
  loadPersistedHistory(state).catch(()=>{});
  loadPersistedMetrics(state).catch(()=>{});
  let ratioFirstPaintPromise=null;
  if(provider==='binance'){
    // Step650.8.15.14: start the all-account ratio before the OI helper. Both still
    // use the authenticated Edge governor, but global L/S gets the first critical
    // slot so the funds/long-short page does not wait for the 45-second App poll.
    ratioFirstPaintPromise=refreshBinanceLongShortCritical(state).catch(()=>null);
  }
  // Start meta/OI only after the ratio promise has acquired or queued its first
  // critical slot. The persistent markPrice WebSocket still supplies meta first paint.
  scheduleContractMetaRefresh(provider,symbol);
  if(provider==='binance') refreshBinanceOpenInterestCritical(state).catch(()=>{});
  scheduleVenueMetricsRefresh(state);

  if(waitMs>0){
    const waits=[waitForTrades(state,20,waitMs)];
    if(provider==='binance'&&!hasFreshRatioFamily(state,'global')){
      const ratioWaitMs=Math.min(BINANCE_RATIO_FIRST_PAINT_WAIT_MS,Math.max(1800,waitMs));
      waits.push(Promise.race([
        ratioFirstPaintPromise||Promise.resolve(null),
        new Promise((resolve)=>setTimeout(resolve,ratioWaitMs)),
      ]));
    }
    await Promise.all(waits);
  }
  if(provider==='binance'&&waitMs>0&&!latestOpenInterestPatch(state)){
    try{await Promise.race([refreshBinanceOpenInterestCritical(state),new Promise((resolve)=>setTimeout(resolve,Math.min(1200,waitMs)))]);}catch(_){}
  }

  const venueMetrics=metricPayloadFromState(state);
  const payload=summarize(state,venueMetrics);
  payload.version=VERSION;
  payload.native_symbol=nativeContractSymbol(provider,symbol);
  payload.contract_meta=mergeContractMetaWithOpenInterest(getContractMetaFast(provider,symbol),state);
  payload.partial=payload.trade_count<=0||payload.metrics.oi_rows.length===0;
  payload.background_refresh=true;
  payload.metric_refresh_pending=Boolean(state.metricFetchPromise||state.oiCriticalPromise||state.ratioCriticalPromise||state.metricLoading);
  payload.metric_error=state.metricError||'';
  payload.long_short_first_paint={
    transport:'authenticated_edge_relay_critical_global_first',
    global_ready:hasFreshRatioFamily(state,'global'),
    top_account_ready:hasFreshRatioFamily(state,'top_account'),
    top_position_ready:hasFreshRatioFamily(state,'top_position'),
    pending:Boolean(state.ratioCriticalPromise),
    last_attempt_at:state.ratioCriticalFetchedAt?new Date(state.ratioCriticalFetchedAt).toISOString():null,
    error:state.ratioCriticalError||'',
  };
  // A valid symbol always receives a usable partial snapshot. Empty/quiet market
  // windows must not become a 503 that leaves the App spinner running forever.
  sendJson(res,200,payload);return true;
}

setInterval(()=>{const now=Date.now();for(const [key,state] of states.entries()){finalizeReadyBuckets(state,now);if(now-state.lastRequestedAt<=IDLE_CLOSE_MS)continue;closeAndDeleteState(state,'idle');}},60000).unref();
setInterval(()=>{flushPersistQueue().catch(()=>{});flushMetricPersistQueue().catch(()=>{});},20000).unref();
