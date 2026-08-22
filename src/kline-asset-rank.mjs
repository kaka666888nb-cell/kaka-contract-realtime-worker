// Step1041.6.4 / Render 650.8.15.196.11.1
// Shared ranking for exchange assets that have a verified official K-line capability.
// Cash equities are intentionally excluded. User reads are memory-only and never start
// exchange/Binance Wallet/Supabase upstream work. Binance Wallet rankType=40 supplies the
// external mature "Popular" order for tokenized securities; it never substitutes product prices.

const VERSION = '650.8.15.196.11.1';
const DATA_VERSION = 1041064;
const SCHEMA_VERSION = 'step1041_6_4_kline_asset_rank_page_v1';
const ROUTE = '/api/asset-market/ranked-page';
const HEALTH_ROUTE = '/api/asset-market/rank-health';
const PAGE_MAX = 50;
const ORDER_TTL_MS = Math.max(60_000, Number(process.env.KAKA_KLINE_ASSET_RANK_ORDER_TTL_MS || 5 * 60_000));
const ORDER_MAX = Math.max(8, Math.min(32, Number(process.env.KAKA_KLINE_ASSET_RANK_ORDER_MAX || 20)));
const CATALOG_REFRESH_MS = Math.max(10 * 60_000, Number(process.env.KAKA_KLINE_ASSET_RANK_CATALOG_REFRESH_MS || 30 * 60_000));
const MARKET_REFRESH_MS = Math.max(60_000, Number(process.env.KAKA_KLINE_ASSET_RANK_MARKET_REFRESH_MS || 2 * 60_000));
const HOT_REFRESH_MS = Math.max(2 * 60_000, Number(process.env.KAKA_KLINE_ASSET_RANK_HOT_REFRESH_MS || 5 * 60_000));
const START_DELAY_MS = Math.max(8_000, Number(process.env.KAKA_KLINE_ASSET_RANK_START_DELAY_MS || 22_000));
const RETAIN_MS = Math.max(10 * 60_000, Number(process.env.KAKA_KLINE_ASSET_RANK_RETAIN_MS || 45 * 60_000));
const FETCH_TIMEOUT_MS = Math.max(5_000, Number(process.env.KAKA_KLINE_ASSET_RANK_FETCH_TIMEOUT_MS || 15_000));
const SUPABASE_PAGE = 1000;
const BATCH_SYMBOL_MAX = 450;
const SUPPORTED_GROUPS = new Set(['stocks','rwa','commodities','fx','events']);
const KLINE_CAPABILITIES = new Set(['supported','supported_sparse']);
const CASH_CLASS = 'equity_cash';
const BINANCE_STOCK_RANK_URL = 'https://web3.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/market/token/pulse/unified/rank/list/ai';
const BINANCE_STOCK_DETAIL_URL = 'https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/market/token/rwa/stock/detail/list/ai';
const BINANCE_STOCK_RANK_TYPE = 40;
const BINANCE_STOCK_PERIOD = 50; // official 24h code
const BINANCE_STOCK_SORT_BY = 0; // official default board order
const BINANCE_STOCK_SIZE = 200;

const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/$/, '');
const SUPABASE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();

let deps = null;
let started = false;
let catalogTimer = null;
let marketTimer = null;
let hotTimer = null;
let catalogRows = [];
let catalogMap = new Map();
let catalogUpdatedAt = 0;
let catalogVersion = 0;
let marketByKey = new Map();
let marketUpdatedAt = 0;
let marketVersion = 0;
let hotByTicker = new Map();
let hotUpdatedAt = 0;
let hotVersion = 0;
let binanceDetailByExact = new Map();
let binanceDetailUniqueSymbol = new Map();
let lastCatalogError = '';
let lastMarketError = '';
let lastHotError = '';
let catalogInflight = null;
let marketInflight = null;
let hotInflight = null;
let seq = 0;
const orders = new Map();
const currentByScope = new Map();
const stats = {
  user_reads: 0,
  user_read_upstream_requests: 0,
  catalog_refresh_started: 0,
  catalog_refresh_succeeded: 0,
  catalog_refresh_failed: 0,
  supabase_reads: 0,
  reality_catalog_reads: 0,
  market_refresh_started: 0,
  market_refresh_succeeded: 0,
  market_refresh_failed: 0,
  market_group_reads: 0,
  reality_market_shared_reads: 0,
  hot_refresh_started: 0,
  hot_refresh_succeeded: 0,
  hot_refresh_failed: 0,
  binance_stock_rank_requests: 0,
  binance_stock_detail_requests: 0,
  rank_builds: 0,
  rank_hits: 0,
  rank_expired: 0,
};

function text(v) { return String(v ?? '').trim(); }
function lower(v) { return text(v).toLowerCase(); }
function upper(v) { return text(v).toUpperCase(); }
function num(v) { if (v == null || (typeof v === 'string' && !v.trim())) return null; const n = Number(v); return Number.isFinite(n) ? n : null; }
function bool(v) { return v === true || String(v).toLowerCase() === 'true'; }
function exactKey(chainId, address) { const c = text(chainId); const a = lower(address); return c && a ? `${c}|${a}` : ''; }
function rowKey(row) { return `${lower(row?.provider)}|${lower(row?.market_type)}|${upper(row?.exchange_symbol)}`; }
function trustedTicker(row) {
  const explicit = upper(row?.security_ticker);
  if (explicit) return explicit;
  return upper(row?.base_asset);
}
function supportedRow(row) {
  return bool(row?.source_verified) &&
    KLINE_CAPABILITIES.has(lower(row?.official_kline_capability)) &&
    SUPPORTED_GROUPS.has(lower(row?.asset_group)) &&
    lower(row?.asset_class) !== CASH_CLASS &&
    upper(row?.exchange_symbol).length > 0;
}
function assetRankSort(raw) {
  const v = lower(raw);
  if (['hot','popular','trending'].includes(v)) return 'hot';
  if (['change_desc','gainers','gain'].includes(v)) return 'change_desc';
  if (['change_asc','losers','loss'].includes(v)) return 'change_asc';
  if (['volume_desc','turnover_desc','volume','turnover'].includes(v)) return 'volume_desc';
  return 'name_asc';
}
function compareNullable(a,b,descending=false) {
  const av=num(a), bv=num(b);
  if (av == null && bv == null) return 0;
  if (av == null) return 1;
  if (bv == null) return -1;
  return descending ? bv-av : av-bv;
}
function send(res,status,payload) {
  res.writeHead(status, {'content-type':'application/json; charset=utf-8','cache-control':'no-store','access-control-allow-origin':'*'});
  res.end(JSON.stringify(payload));
}
async function fetchJson(url, { headers = {}, label = 'upstream', method = 'GET', body: requestBody = null } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const r = await fetch(url, { method, body: requestBody, signal: controller.signal, headers: { accept:'application/json', 'accept-encoding':'identity', 'user-agent':`binance-web3/2.1 (Skill); KakaWeb3/${VERSION} ${label}`, ...(requestBody ? {'content-type':'application/json'} : {}), ...headers } });
    const responseText = await r.text();
    if (!r.ok) throw new Error(`${label}_http_${r.status}:${responseText.slice(0,160)}`);
    try { return JSON.parse(responseText); } catch { throw new Error(`${label}_invalid_json`); }
  } finally { clearTimeout(timer); }
}

function supabaseHeaders(extra={}) {
  return { apikey: SUPABASE_KEY, authorization:`Bearer ${SUPABASE_KEY}`, ...extra };
}
async function loadSupabaseCatalog() {
  if (!SUPABASE_URL || !SUPABASE_KEY) throw new Error('supabase_not_configured');
  const select = [
    'asset_id','provider','market_type','product_kind','asset_class','asset_group','exchange_symbol','display_symbol','display_name','display_name_zh',
    'base_asset','quote_asset','settle_asset','status','exchange_name','product_venue','is_reality','is_rwa','source_verified','official_kline_capability',
    'official_kline_source','official_kline_identity','secondary_source_status','icon_url','quote_currency_symbol'
  ].join(',');
  const rows=[];
  for (let offset=0; offset<5000; offset+=SUPABASE_PAGE) {
    const q = new URLSearchParams();
    q.set('source_verified','eq.true');
    q.set('official_kline_capability','in.(supported,supported_sparse)');
    q.set('asset_group','in.(stocks,rwa,commodities,fx,events)');
    q.set('asset_class','neq.equity_cash');
    q.set('select',select);
    q.set('order','asset_id.asc');
    stats.supabase_reads += 1;
    const part = await fetchJson(`${SUPABASE_URL}/rest/v1/kaka_exchange_asset_catalog?${q}`, { headers:supabaseHeaders({ range:`${offset}-${offset+SUPABASE_PAGE-1}`, prefer:'count=none' }), label:'asset_rank_supabase_catalog' });
    if (!Array.isArray(part)) throw new Error('asset_rank_supabase_rows_invalid');
    rows.push(...part.filter(supportedRow));
    if (part.length < SUPABASE_PAGE) break;
  }
  return rows;
}

function normalizeRealityRow(raw) {
  const symbol=upper(raw?.exchange_symbol || raw?.symbol);
  if (!symbol) return null;
  return {
    ...raw,
    asset_id: text(raw?.asset_id) || `bitget:spot:reality:${symbol}`,
    provider:'bitget', market_type:'spot', product_kind:'reality_stock_token', asset_class:'equity_token', asset_group:'stocks',
    exchange_symbol:symbol, display_symbol:text(raw?.display_symbol) || symbol,
    base_asset:upper(raw?.base_asset) || upper(raw?.security_ticker), quote_asset:upper(raw?.quote_asset) || 'USDT',
    security_ticker:upper(raw?.security_ticker), is_reality:true, source_verified:true,
    official_kline_capability:'supported', official_kline_source:text(raw?.official_kline_source) || 'bitget_official_public_asset_kline',
  };
}

async function refreshCatalog() {
  if (catalogInflight) return catalogInflight;
  catalogInflight=(async()=>{
    stats.catalog_refresh_started += 1;
    try {
      const supabaseRows=await loadSupabaseCatalog();
      let realityRows=[];
      try {
        stats.reality_catalog_reads += 1;
        const p=await deps.requestIsolatedJson('exchange-assets','/api/asset-klines/reality-map?offset=0&limit=1000',10_000);
        if (p?.ok && Array.isArray(p?.rows)) realityRows=p.rows.map(normalizeRealityRow).filter(Boolean);
      } catch (_) {}
      const map=new Map();
      for (const row of supabaseRows) if (supportedRow(row)) map.set(rowKey(row), row);
      // Dynamic Reality catalog is authoritative for current Bitget isReality inventory.
      if (realityRows.length) {
        for (const [k,row] of [...map.entries()]) if (lower(row?.provider)==='bitget' && lower(row?.asset_class)==='equity_token' && bool(row?.is_reality)) map.delete(k);
        for (const row of realityRows) map.set(rowKey(row),row);
      }
      if (map.size < 500) throw new Error(`kline_asset_catalog_too_small:${map.size}`);
      catalogMap=map; catalogRows=[...map.values()]; catalogUpdatedAt=Date.now(); catalogVersion += 1; lastCatalogError='';
      stats.catalog_refresh_succeeded += 1;
      return catalogRows;
    } catch(e) {
      stats.catalog_refresh_failed += 1; lastCatalogError=String(e?.message||e);
      if (catalogRows.length && Date.now()-catalogUpdatedAt <= RETAIN_MS) return catalogRows;
      throw e;
    } finally { catalogInflight=null; }
  })();
  return catalogInflight;
}

function normalizeMarketTicker(ticker, fallback=null) {
  if (!ticker && !fallback) return null;
  const t=ticker || {};
  const f=fallback || {};
  const last=num(t.last_price ?? t.price ?? f.last_price ?? f.price);
  let ratio=num(t.price_change_24h_ratio);
  let pct=num(t.price_change_percent_24h ?? f.price_change_percent_24h);
  if (ratio == null && pct != null) ratio=pct/100;
  if (pct == null && ratio != null) pct=ratio*100;
  const volume=num(t.volume_24h ?? f.volume_24h);
  const turnover=num(t.turnover_24h ?? t.quote_volume_24h ?? f.quote_volume_24h ?? f.turnover_24h);
  return {
    source:text(t.source || f.source) || null,
    last_price:last,
    price_change_24h_ratio:ratio,
    price_change_percent_24h:pct,
    volume_24h:volume,
    turnover_24h:turnover,
    quote_volume_24h:turnover,
    timestamp_ms:num(t.timestamp_ms ?? f.timestamp_ms ?? f.ts),
  };
}
function marketGroupKey(row) {
  const provider=lower(row?.provider), market=lower(row?.market_type);
  // One provider-market all-ticker call can serve several product sub-classes.
  return `${provider}|${market}`;
}
async function refreshMarket() {
  if (marketInflight) return marketInflight;
  marketInflight=(async()=>{
    stats.market_refresh_started += 1;
    try {
      if (!catalogRows.length || Date.now()-catalogUpdatedAt > CATALOG_REFRESH_MS*2) await refreshCatalog();
      const next=new Map(marketByKey);
      // Reality uses the already-running parent shared Bitget market-light snapshot: no extra exchange request.
      const ml=deps.getMarketLightInternalSnapshot({market:'spot',provider:'bitget'});
      const mlRows=Array.isArray(ml?.rows)?ml.rows:[];
      const mlMap=new Map(mlRows.map(r=>[upper(r?.symbol),r]).filter(x=>x[0]));
      stats.reality_market_shared_reads += 1;
      for (const row of catalogRows) {
        if (lower(row?.provider)==='bitget' && lower(row?.asset_class)==='equity_token' && (bool(row?.is_reality) || lower(row?.product_kind).includes('reality'))) {
          const m=mlMap.get(upper(row?.exchange_symbol));
          if (m) next.set(rowKey(row),normalizeMarketTicker(null,m));
        }
      }
      const groups=new Map();
      for (const row of catalogRows) {
        if (lower(row?.provider)==='bitget' && lower(row?.asset_class)==='equity_token' && (bool(row?.is_reality) || lower(row?.product_kind).includes('reality'))) continue;
        const k=marketGroupKey(row); if (!groups.has(k)) groups.set(k,[]); groups.get(k).push(row);
      }
      for (const rows of groups.values()) {
        const first=rows[0];
        const symbols=[...new Set(rows.map(r=>upper(r?.exchange_symbol)).filter(Boolean))];
        for (let i=0;i<symbols.length;i+=BATCH_SYMBOL_MAX) {
          const chunk=symbols.slice(i,i+BATCH_SYMBOL_MAX);
          const params=new URLSearchParams({
            provider:lower(first?.provider), market_type:lower(first?.market_type), asset_class:lower(first?.asset_class), product_kind:lower(first?.product_kind), symbols:chunk.join(',')
          });
          try {
            stats.market_group_reads += 1;
            const p=await deps.requestIsolatedJson('exchange-assets',`/api/asset-market/tickers?${params}`,18_000);
            if (!p?.ok || !Array.isArray(p?.items)) continue;
            for (const item of p.items) {
              const symbol=upper(item?.native_symbol); if (!symbol) continue;
              const normalized=normalizeMarketTicker(item?.ticker || null);
              if (!normalized) continue;
              for (const row of rows) if (upper(row?.exchange_symbol)===symbol) next.set(rowKey(row),normalized);
            }
          } catch (_) { /* retain previous good group rows */ }
        }
      }
      if (!next.size && marketByKey.size) throw new Error('kline_asset_market_empty');
      marketByKey=next; marketUpdatedAt=Date.now(); marketVersion += 1; lastMarketError=''; stats.market_refresh_succeeded += 1;
      return next;
    } catch(e) {
      stats.market_refresh_failed += 1; lastMarketError=String(e?.message||e);
      if (marketByKey.size && Date.now()-marketUpdatedAt <= RETAIN_MS) return marketByKey;
      throw e;
    } finally { marketInflight=null; }
  })();
  return marketInflight;
}

function dataArray(payload) {
  const candidates=[payload?.data, payload?.data?.list, payload?.data?.rows, payload?.list, payload?.rows];
  for (const c of candidates) if (Array.isArray(c)) return c;
  return [];
}
function rankArray(payload) {
  const candidates=[payload?.data?.list,payload?.data?.rows,payload?.data?.tokens,payload?.data,payload?.list,payload?.rows];
  for (const c of candidates) if (Array.isArray(c)) return c;
  return [];
}
function rebuildDetailMaps(detailRows) {
  const exact=new Map(), symbolTickers=new Map();
  for (const r of detailRows) {
    const ticker=upper(r?.ticker), symbol=upper(r?.symbol), key=exactKey(r?.chainId ?? r?.chain_id, r?.contractAddress ?? r?.contract_address);
    if (!ticker) continue;
    if (key) exact.set(key,ticker);
    if (symbol) { if (!symbolTickers.has(symbol)) symbolTickers.set(symbol,new Set()); symbolTickers.get(symbol).add(ticker); }
  }
  const unique=new Map();
  for (const [s,set] of symbolTickers) if (set.size===1) unique.set(s,[...set][0]);
  binanceDetailByExact=exact; binanceDetailUniqueSymbol=unique;
}
function tickerForRankEntry(r) {
  const key=exactKey(r?.chainId ?? r?.chain_id, r?.contractAddress ?? r?.contract_address ?? r?.address);
  if (key && binanceDetailByExact.has(key)) return {ticker:binanceDetailByExact.get(key),method:'exact_chain_contract'};
  const symbol=upper(r?.symbol ?? r?.tokenSymbol ?? r?.token_symbol);
  if (symbol && binanceDetailUniqueSymbol.has(symbol)) return {ticker:binanceDetailUniqueSymbol.get(symbol),method:'official_unique_symbol'};
  return null;
}
async function refreshHot() {
  if (hotInflight) return hotInflight;
  hotInflight=(async()=>{
    stats.hot_refresh_started += 1;
    try {
      stats.binance_stock_detail_requests += 1;
      const detail=await fetchJson(BINANCE_STOCK_DETAIL_URL,{label:'binance_wallet_stock_detail'});
      const detailRows=dataArray(detail);
      if (!detailRows.length) throw new Error('binance_stock_detail_empty');
      rebuildDetailMaps(detailRows);
      stats.binance_stock_rank_requests += 1;
      const rank=await fetchJson(BINANCE_STOCK_RANK_URL,{
        label:'binance_wallet_stock_rank', method:'POST',
        body:JSON.stringify({ rankType:BINANCE_STOCK_RANK_TYPE, period:BINANCE_STOCK_PERIOD, sortBy:BINANCE_STOCK_SORT_BY, orderAsc:false, page:1, size:BINANCE_STOCK_SIZE }),
      });
      const rankRows=rankArray(rank);
      if (!rankRows.length) throw new Error('binance_stock_rank_empty');
      const next=new Map();
      for (let rawIndex=0; rawIndex<rankRows.length; rawIndex += 1) {
        const r=rankRows[rawIndex];
        const resolved=tickerForRankEntry(r); if (!resolved) continue;
        if (next.has(resolved.ticker)) continue;
        next.set(resolved.ticker,{
          ticker:resolved.ticker, rank:num(r?.rank) ?? rawIndex + 1, method:resolved.method,
          chain_id:text(r?.chainId ?? r?.chain_id) || null,
          contract_address:text(r?.contractAddress ?? r?.contract_address ?? r?.address) || null,
          source_symbol:upper(r?.symbol ?? r?.tokenSymbol ?? r?.token_symbol) || null,
        });
      }
      if (!next.size) throw new Error('binance_stock_rank_no_exact_ticker_matches');
      hotByTicker=next; hotUpdatedAt=Date.now(); hotVersion += 1; lastHotError=''; stats.hot_refresh_succeeded += 1;
      return next;
    } catch(e) {
      stats.hot_refresh_failed += 1; lastHotError=String(e?.message||e);
      if (hotByTicker.size && Date.now()-hotUpdatedAt <= RETAIN_MS) return hotByTicker;
      throw e;
    } finally { hotInflight=null; }
  })();
  return hotInflight;
}

function scopeKey({group,assetClass,provider,sort}) { return `${group}|${assetClass||'all'}|${provider||'all'}|${sort}`; }
function pruneOrders() {
  const now=Date.now();
  for (const [v,s] of orders) if (now-Number(s?.created_at_ms||0)>ORDER_TTL_MS) {
    orders.delete(v); stats.rank_expired += 1;
    for (const [k,x] of currentByScope) if (x===v) currentByScope.delete(k);
  }
  if (orders.size<=ORDER_MAX) return;
  const sorted=[...orders.entries()].sort((a,b)=>a[1].created_at_ms-b[1].created_at_ms);
  while (sorted.length>ORDER_MAX) { const [v]=sorted.shift(); orders.delete(v); for (const [k,x] of currentByScope) if (x===v) currentByScope.delete(k); }
}
function filteredRows({group,assetClass,provider}) {
  return catalogRows.filter(r => supportedRow(r) &&
    (!group || group==='all' || lower(r?.asset_group)===group) &&
    (!assetClass || assetClass==='all' || lower(r?.asset_class)===assetClass) &&
    (!provider || provider==='all' || lower(r?.provider)===provider));
}
function rankedEntries(scope) {
  const rows=filteredRows(scope).map(row=>{
    const market=marketByKey.get(rowKey(row)) || null;
    const ticker=trustedTicker(row);
    const hot=ticker ? hotByTicker.get(ticker) || null : null;
    return { row, key:rowKey(row), market, ticker, hot };
  });
  rows.sort((a,b)=>{
    let cmp=0;
    if (scope.sort==='hot') cmp=compareNullable(a.hot?.rank,b.hot?.rank,false);
    else if (scope.sort==='change_desc') cmp=compareNullable(a.market?.price_change_24h_ratio,b.market?.price_change_24h_ratio,true);
    else if (scope.sort==='change_asc') cmp=compareNullable(a.market?.price_change_24h_ratio,b.market?.price_change_24h_ratio,false);
    else if (scope.sort==='volume_desc') cmp=compareNullable(a.market?.turnover_24h,b.market?.turnover_24h,true);
    if (!cmp && scope.sort!=='name_asc') cmp=compareNullable(a.market?.turnover_24h,b.market?.turnover_24h,true);
    if (!cmp) cmp=upper(a.row?.base_asset).localeCompare(upper(b.row?.base_asset)) || a.key.localeCompare(b.key);
    return cmp;
  });
  return rows;
}
function createOrder(scope) {
  pruneOrders();
  const entries=rankedEntries(scope); seq += 1;
  const version=`kar-${catalogVersion}-${marketVersion}-${hotVersion}-${seq}`;
  const snapshot={
    rank_version:version, scope:scopeKey(scope), created_at_ms:Date.now(), sort:scope.sort,
    catalog_version:catalogVersion, market_version:marketVersion, hot_version:hotVersion,
    order:entries.map(e=>({ key:e.key, catalog_row:e.row, rank_metric_value:scope.sort==='hot'?num(e.hot?.rank):scope.sort==='change_desc'||scope.sort==='change_asc'?num(e.market?.price_change_24h_ratio):scope.sort==='volume_desc'?num(e.market?.turnover_24h):null, hot_rank:num(e.hot?.rank), hot_ticker:e.hot?.ticker||null, hot_match_method:e.hot?.method||null }))
  };
  orders.set(version,snapshot); currentByScope.set(snapshot.scope,version); stats.rank_builds += 1; pruneOrders(); return snapshot;
}
function currentOrder(scope) {
  pruneOrders(); const key=scopeKey(scope); const v=currentByScope.get(key); const s=v?orders.get(v):null;
  if (s && Date.now()-s.created_at_ms<=ORDER_TTL_MS && s.catalog_version===catalogVersion && s.market_version===marketVersion && (scope.sort!=='hot'||s.hot_version===hotVersion)) { stats.rank_hits += 1; return s; }
  return createOrder(scope);
}
function requestedOrder(version,scope) {
  pruneOrders(); const s=orders.get(version); if (!s || s.scope!==scopeKey(scope)) return null; stats.rank_hits += 1; return s;
}
function healthPayload() {
  const age=(ts)=>ts?Math.max(0,Date.now()-ts):null;
  const groups={}; for (const r of catalogRows) { const k=`${lower(r.asset_group)}|${lower(r.asset_class)}`; groups[k]=(groups[k]||0)+1; }
  return {
    ok:true,version:VERSION,data_version:DATA_VERSION,schema_version:SCHEMA_VERSION,route:ROUTE,health_route:HEALTH_ROUTE,
    read_only_shared:true,only_official_kline_assets:true,cash_equities_excluded:true,page_limit_max:PAGE_MAX,
    user_reads_start_upstream:false,user_read_upstream_requests:0,reads_scale_with_users:false,
    supported_sorts:['name_asc','hot','change_desc','change_asc','volume_desc'],hot_supported_asset_groups:['stocks','rwa'],
    catalog:{ready:catalogRows.length>0&&age(catalogUpdatedAt)<=RETAIN_MS,rows:catalogRows.length,version:catalogVersion,updated_at:catalogUpdatedAt?new Date(catalogUpdatedAt).toISOString():null,age_ms:age(catalogUpdatedAt),refresh_ms:CATALOG_REFRESH_MS,groups,last_error:lastCatalogError},
    market:{ready:marketByKey.size>0&&age(marketUpdatedAt)<=RETAIN_MS,rows:marketByKey.size,version:marketVersion,updated_at:marketUpdatedAt?new Date(marketUpdatedAt).toISOString():null,age_ms:age(marketUpdatedAt),refresh_ms:MARKET_REFRESH_MS,last_error:lastMarketError},
    popular:{ready:hotByTicker.size>0&&age(hotUpdatedAt)<=RETAIN_MS,rows:hotByTicker.size,version:hotVersion,updated_at:hotUpdatedAt?new Date(hotUpdatedAt).toISOString():null,age_ms:age(hotUpdatedAt),refresh_ms:HOT_REFRESH_MS,source:'binance_wallet_unified_token_rank_stock',rank_type:BINANCE_STOCK_RANK_TYPE,period:BINANCE_STOCK_PERIOD,sort_by:BINANCE_STOCK_SORT_BY,size:BINANCE_STOCK_SIZE,detail_exact_identity_source:'binance_wallet_tokenized_stock_detail_list',last_error:lastHotError},
    pagination:{ranking_happens_before_pagination:true,pagination_order_frozen_by_rank_version:true,order_ttl_ms:ORDER_TTL_MS,snapshots:orders.size},
    pressure:{catalog_supabase_reads_per_refresh_max:5,market_group_shared_reads_per_refresh_max:12,binance_popular_requests_per_refresh:2,user_scale_upstream_amplification:0},
    stats:{...stats},
  };
}

export function getKlineAssetRankHealth() { return healthPayload(); }
export function startKlineAssetRankCollector(options) {
  if (started) return; started=true; deps=options;
  if (!deps?.requestIsolatedJson || !deps?.getMarketLightInternalSnapshot) throw new Error('kline_asset_rank_dependencies_missing');
  const safe=(fn)=>fn().catch(()=>{});
  setTimeout(()=>safe(async()=>{await refreshCatalog(); await Promise.allSettled([refreshMarket(),refreshHot()]);}),START_DELAY_MS).unref?.();
  catalogTimer=setInterval(()=>safe(refreshCatalog),CATALOG_REFRESH_MS); catalogTimer.unref?.();
  marketTimer=setInterval(()=>safe(refreshMarket),MARKET_REFRESH_MS); marketTimer.unref?.();
  hotTimer=setInterval(()=>safe(refreshHot),HOT_REFRESH_MS); hotTimer.unref?.();
}
export async function handleKlineAssetRank(req,res,url) {
  if (![ROUTE,HEALTH_ROUTE].includes(url?.pathname)) return false;
  if (req.method==='OPTIONS') { res.writeHead(204,{'access-control-allow-origin':'*','access-control-allow-methods':'GET, OPTIONS','cache-control':'no-store'}); res.end(); return true; }
  if (req.method!=='GET') { send(res,405,{ok:false,version:VERSION,error:'method_not_allowed'}); return true; }
  if (url.pathname===HEALTH_ROUTE) { send(res,200,healthPayload()); return true; }
  stats.user_reads += 1;
  const group=lower(url.searchParams.get('asset_group')) || 'all';
  const assetClass=lower(url.searchParams.get('asset_class')) || 'all';
  const provider=lower(url.searchParams.get('provider')) || 'all';
  const sort=assetRankSort(url.searchParams.get('sort'));
  const offset=Math.max(0,Math.trunc(Number(url.searchParams.get('offset')||0)||0));
  const limit=Math.max(1,Math.min(PAGE_MAX,Math.trunc(Number(url.searchParams.get('limit')||50)||50)));
  const requested=text(url.searchParams.get('rank_version'));
  if (group!=='all'&&!SUPPORTED_GROUPS.has(group)) { send(res,400,{ok:false,version:VERSION,error:'unsupported_asset_group'}); return true; }
  if (assetClass===CASH_CLASS) { send(res,400,{ok:false,version:VERSION,error:'cash_equities_not_ranked',cash_equities_excluded:true,user_read_upstream_requests:0}); return true; }
  if (sort==='hot' && !['stocks','rwa','all'].includes(group)) { send(res,400,{ok:false,version:VERSION,error:'popular_sort_only_for_securities',user_read_upstream_requests:0}); return true; }
  if (offset>0&&!requested) { send(res,400,{ok:false,version:VERSION,error:'rank_version_required_after_first_page',restart_from_offset:0,user_read_upstream_requests:0}); return true; }
  if (!catalogRows.length) { send(res,503,{ok:false,version:VERSION,error:'kline_asset_catalog_not_ready',user_read_upstream_requests:0}); return true; }
  const scope={group,assetClass,provider,sort};
  let snapshot=requested?requestedOrder(requested,scope):null;
  if (requested&&!snapshot) { send(res,409,{ok:false,version:VERSION,error:'rank_version_expired_or_scope_mismatch',rank_version:requested,restart_from_offset:0,user_read_upstream_requests:0}); return true; }
  if (!snapshot) snapshot=currentOrder(scope);
  const page=snapshot.order.slice(offset,offset+limit);
  const items=page.map((entry,index)=>{
    const currentMarket=marketByKey.get(entry.key)||null;
    return {
      rank_index:offset+index+1, rank_identity:entry.key, rank_metric_value:entry.rank_metric_value,
      popular_rank:entry.hot_rank, popular_underlying_ticker:entry.hot_ticker, popular_match_method:entry.hot_match_method,
      catalog_row:entry.catalog_row, market_row:currentMarket,
    };
  });
  send(res,200,{
    ok:true,version:VERSION,data_version:DATA_VERSION,schema_version:SCHEMA_VERSION,
    asset_group:group,asset_class:assetClass,provider,sort,rank_version:snapshot.rank_version,rank_snapshot_created_at:new Date(snapshot.created_at_ms).toISOString(),rank_snapshot_ttl_ms:ORDER_TTL_MS,
    total_items:snapshot.order.length,offset,limit,returned_items:items.length,has_more:offset+items.length<snapshot.order.length,next_offset:offset+items.length<snapshot.order.length?offset+items.length:null,
    supported_sorts:['name_asc','hot','change_desc','change_asc','volume_desc'],hot_supported_asset_groups:['stocks','rwa'],
    ranking_happens_before_pagination:true,pagination_order_frozen_by_rank_version:true,app_page_size_remains_50:true,
    only_official_kline_assets:true,cash_equities_excluded:true,cross_provider_substitution:false,cross_product_substitution:false,cross_ticker_substitution:false,
    popular_source:sort==='hot'?'binance_wallet_unified_token_rank_rankType40_default_24h':null,popular_is_underlying_security_order_only:sort==='hot',popular_never_substitutes_product_market_data:true,
    read_only_shared:true,user_read_upstream_requests:0,user_read_upstream_connections:0,reads_scale_with_users:false,
    catalog_version:snapshot.catalog_version,market_version:snapshot.market_version,hot_version:snapshot.hot_version,items,generated_at:new Date().toISOString(),
  });
  return true;
}
