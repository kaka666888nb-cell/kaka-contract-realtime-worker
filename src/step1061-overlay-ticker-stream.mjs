import http from 'node:http';
import { requestIsolatedJson } from './collector-isolation.mjs';

const STEP = '1061.5';
const SCHEMA = 'step1061_5_all_market_overlay_downstream_sse_v1';
const STREAM_ROUTE = '/api/overlay/watchlist-stream';
const HEALTH_ROUTE = '/api/overlay/watchlist-stream-health';

// One client can display at most six exact identities. Collector work is aggregated globally by
// exact identity, never by client count. Spot/contract reuse market-light isolated shared state;
// exchange assets reuse the fixed 5s asset watchlist focus collector; on-chain price reuses the
// fixed 5s exact-pool focus collector. Only on-chain 24h change gets a separate bounded background
// DEX Screener refresh, independent of user reads.
const CLIENT_SPEC_MAX = 6;
const ACTIVE_SPEC_MAX = Math.max(64, Math.min(512, Number(process.env.KAKA_OVERLAY_ACTIVE_SPEC_MAX || 256)));
const CLIENT_MAX = Math.max(1000, Number(process.env.KAKA_OVERLAY_STREAM_CLIENT_MAX || 1500));
const CLIENTS_PER_IP_MAX = Math.max(10, Number(process.env.KAKA_OVERLAY_STREAM_CLIENTS_PER_IP_MAX || 50));
const CONNECTS_PER_IP_PER_MINUTE = Math.max(10, Number(process.env.KAKA_OVERLAY_STREAM_CONNECTS_PER_IP_PER_MINUTE || 60));
const POLL_MS = Math.max(750, Number(process.env.KAKA_OVERLAY_STREAM_POLL_MS || 1000));
const ASSET_REFRESH_MS = Math.max(3000, Number(process.env.KAKA_OVERLAY_ASSET_REFRESH_MS || 5000));
const ONCHAIN_PRICE_REFRESH_MS = Math.max(3000, Number(process.env.KAKA_OVERLAY_ONCHAIN_PRICE_REFRESH_MS || 5000));
const ONCHAIN_CHANGE_REFRESH_MS = Math.max(15_000, Number(process.env.KAKA_OVERLAY_ONCHAIN_CHANGE_REFRESH_MS || 30_000));
const HEARTBEAT_MS = Math.max(10_000, Number(process.env.KAKA_OVERLAY_STREAM_HEARTBEAT_MS || 15_000));
const SLOW_CLIENT_MAX_BUFFERED_BYTES = Math.max(64 * 1024, Number(process.env.KAKA_OVERLAY_STREAM_SLOW_CLIENT_MAX_BUFFERED_BYTES || 512 * 1024));
const LOCAL_PORT = Number(process.env.PORT || 10000);
const LOCAL_ORIGIN = `http://127.0.0.1:${LOCAL_PORT}`;
const DEX_BASE = 'https://api.dexscreener.com';
const DEX_BATCH_MAX = 30;
const DEX_REQUEST_GAP_MS = Math.max(1200, Number(process.env.KAKA_OVERLAY_DEX_REQUEST_GAP_MS || 1500));
const DEX_TIMEOUT_MS = Math.max(5000, Math.min(20_000, Number(process.env.KAKA_OVERLAY_DEX_TIMEOUT_MS || 10_000)));

const SPOT_PROVIDERS = new Set(['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate']);
const CONTRACT_PROVIDERS = new Set(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const ONCHAIN_NETWORKS = new Set(['ethereum', 'bsc', 'base', 'solana', 'arbitrum', 'polygon', 'optimism', 'avalanche', 'linea']);

const clients = new Map();
const activeSpecRefs = new Map();
const latestBySpec = new Map();
const clientsByIp = new Map();
const connectAttemptsByIp = new Map();
let clientSeq = 0;
let pollTimer = null;
let heartbeatTimer = null;
let pollBusy = false;
let lastAssetRefreshAt = 0;
let lastOnchainPriceRefreshAt = 0;
let lastOnchainChangeRefreshAt = 0;
let lastDexStartAt = 0;
let assetRefreshBusy = false;
let onchainPriceRefreshBusy = false;
let onchainChangeRefreshBusy = false;

const stats = {
  accepted_connections: 0,
  closed_connections: 0,
  rejected_capacity: 0,
  rejected_invalid: 0,
  rejected_ip_capacity: 0,
  rejected_ip_rate: 0,
  poll_ticks: 0,
  poll_busy_skips: 0,
  market_batches: 0,
  market_failures: 0,
  asset_batches: 0,
  asset_failures: 0,
  onchain_price_batches: 0,
  onchain_price_failures: 0,
  onchain_change_batches: 0,
  onchain_change_failures: 0,
  onchain_change_upstream_requests: 0,
  downstream_events: 0,
  downstream_bytes: 0,
  unchanged_skips: 0,
  slow_client_disconnects: 0,
  last_poll_started_at: null,
  last_poll_completed_at: null,
  last_error: '',
};

function text(value) { return String(value ?? '').trim(); }
function lower(value) { return text(value).toLowerCase(); }
function normalizeProvider(value) {
  let provider = lower(value);
  if (provider === 'gate.io') provider = 'gate';
  if (provider === 'okex') provider = 'okx';
  return provider;
}
function normalizeSymbol(value) {
  return text(value).toUpperCase().replace(/-SWAP$/i, '').replace(/_UMCBL$/i, '').replace(/[^A-Z0-9]/g, '');
}
function normalizeAddress(network, value) {
  const raw = text(value);
  return network === 'solana' ? raw : raw.toLowerCase();
}
function cleanToken(value) {
  return text(value).replace(/[~|]/g, '');
}

function canonicalSpec(value) {
  const parts = text(value).split('|');
  const kind = lower(parts[0]);
  if (kind === 'spot' || kind === 'contract') {
    if (parts.length !== 3) return '';
    const provider = normalizeProvider(parts[1]);
    const symbol = normalizeSymbol(parts[2]);
    if (!symbol) return '';
    if (kind === 'spot' && !SPOT_PROVIDERS.has(provider)) return '';
    if (kind === 'contract' && !CONTRACT_PROVIDERS.has(provider)) return '';
    return `${kind}|${provider}|${symbol}`;
  }
  if (kind === 'asset') {
    if (![6, 7].includes(parts.length)) return '';
    const provider = normalizeProvider(parts[1]);
    const marketType = lower(parts[2]);
    const assetClass = lower(parts[3]);
    const productKind = lower(parts[4]);
    const symbol = normalizeSymbol(parts[5]);
    if (!provider || !marketType || !assetClass || !productKind || !symbol) return '';
    const securityIdentity = parts.length === 7 ? cleanToken(parts[6]) : '';
    return `asset|${cleanToken(provider)}|${cleanToken(marketType)}|${cleanToken(assetClass)}|${cleanToken(productKind)}|${symbol}${securityIdentity ? `|${securityIdentity}` : ''}`;
  }
  if (kind === 'onchain') {
    if (parts.length !== 4) return '';
    const network = lower(parts[1]);
    if (!ONCHAIN_NETWORKS.has(network)) return '';
    const tokenAddress = normalizeAddress(network, parts[2]);
    const poolAddress = normalizeAddress(network, parts[3]);
    if (!tokenAddress || !poolAddress || tokenAddress.includes('~') || poolAddress.includes('~')) return '';
    return `onchain|${network}|${tokenAddress}|${poolAddress}`;
  }
  return '';
}
function parseSpecs(raw) {
  return [...new Set(text(raw).split('~').map(canonicalSpec).filter(Boolean))].sort();
}
function kindOf(spec) { return spec.split('|')[0] || ''; }
function chunked(values, size) {
  const result = [];
  for (let i = 0; i < values.length; i += size) result.push(values.slice(i, i + size));
  return result;
}
function finite(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
function semanticFingerprint(items) {
  return JSON.stringify(items.map((item) => ({
    spec: item.spec,
    kind: item.kind,
    ready: item.ready,
    row: item.row,
  })));
}

function downstreamIp(req) {
  const forwarded = text(req?.headers?.['x-forwarded-for']).split(',').map((v) => v.trim()).find(Boolean);
  return forwarded || text(req?.socket?.remoteAddress) || 'unknown';
}
function pruneConnectAttempts(ip) {
  const cutoff = Date.now() - 60_000;
  const attempts = connectAttemptsByIp.get(ip) || [];
  while (attempts.length && attempts[0] < cutoff) attempts.shift();
  if (attempts.length) connectAttemptsByIp.set(ip, attempts); else connectAttemptsByIp.delete(ip);
  return attempts;
}
function incrementSpecRefs(specs) {
  for (const spec of specs) activeSpecRefs.set(spec, Number(activeSpecRefs.get(spec) || 0) + 1);
}
function decrementSpecRefs(specs) {
  for (const spec of specs) {
    const next = Math.max(0, Number(activeSpecRefs.get(spec) || 0) - 1);
    if (next) activeSpecRefs.set(spec, next); else activeSpecRefs.delete(spec);
  }
}
function closeClient(entry) {
  if (!entry || entry.closed) return;
  entry.closed = true;
  clients.delete(entry.id);
  decrementSpecRefs(entry.specs);
  const ipCount = Math.max(0, Number(clientsByIp.get(entry.ip) || 0) - 1);
  if (ipCount) clientsByIp.set(entry.ip, ipCount); else clientsByIp.delete(entry.ip);
  stats.closed_connections += 1;
  if (!clients.size) stopTimersIfIdle();
}
function writeSse(entry, event, payload) {
  if (!entry || entry.closed || entry.res.writableEnded || entry.res.destroyed) return false;
  if (Number(entry.res.writableLength || 0) > SLOW_CLIENT_MAX_BUFFERED_BYTES) {
    stats.slow_client_disconnects += 1;
    try { entry.res.destroy(); } catch (_) {}
    closeClient(entry);
    return false;
  }
  const body = `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;
  try {
    entry.res.write(body);
    stats.downstream_events += 1;
    stats.downstream_bytes += Buffer.byteLength(body);
    return true;
  } catch (_) {
    closeClient(entry);
    return false;
  }
}
function sendHeartbeat() {
  const body = `: kaka-${STEP}-keepalive ${Date.now()}\n\n`;
  for (const entry of [...clients.values()]) {
    if (entry.closed || entry.res.writableEnded || entry.res.destroyed) { closeClient(entry); continue; }
    try { entry.res.write(body); stats.downstream_bytes += Buffer.byteLength(body); } catch (_) { closeClient(entry); }
  }
}
function stopTimersIfIdle() {
  clearInterval(pollTimer); clearInterval(heartbeatTimer);
  pollTimer = null; heartbeatTimer = null; latestBySpec.clear();
}

async function localJson(pathname, timeoutMs = 7000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  timer.unref?.();
  try {
    const response = await fetch(`${LOCAL_ORIGIN}${pathname}`, {
      method: 'GET',
      headers: { accept: 'application/json', 'user-agent': 'KakaOverlaySharedCollector/1061.5' },
      signal: controller.signal,
    });
    const payload = await response.json().catch(() => null);
    if (!response.ok || !payload) throw new Error(`local_http_${response.status}:${pathname}`);
    return payload;
  } finally {
    clearTimeout(timer);
  }
}

function marketSpecFromItem(item) {
  const row = item?.row && typeof item.row === 'object' ? item.row : {};
  return canonicalSpec(`${item?.market_type ?? row.market_type}|${item?.provider ?? row.provider}|${item?.symbol ?? row.symbol}`);
}
function normalizeMarketItem(spec, item) {
  const row = item?.row && typeof item.row === 'object' ? item.row : {};
  return {
    spec,
    kind: kindOf(spec),
    ready: item?.ready === true || finite(row?.price) != null || finite(row?.last) != null,
    row: {
      provider: normalizeProvider(item?.provider ?? row.provider ?? spec.split('|')[1]),
      market_type: lower(item?.market_type ?? row.market_type ?? kindOf(spec)),
      symbol: text(item?.symbol ?? row.symbol ?? spec.split('|')[2]).toUpperCase(),
      price: finite(row?.price ?? row?.last ?? row?.last_price),
      change_percent_24h: finite(row?.change_percent_24h ?? row?.change24h ?? row?.change_percent ?? row?.percent_change_24h),
      timestamp: row?.timestamp ?? row?.updated_at ?? row?.ts ?? null,
      source: text(row?.source || item?.source || 'market_light_shared'),
    },
  };
}

async function refreshMarketSpecs(specs) {
  if (!specs.length) return;
  stats.market_batches += 1;
  try {
    const payload = await requestIsolatedJson({
      pathname: '/api/market-light/ticker-exact',
      searchParams: { items: specs.join('~') },
      timeoutMs: Math.max(2500, Math.min(6500, POLL_MS * 4)),
      acceptableSchemas: ['step1060_market_light_ticker_exact_v1', 'step1060_market_light_ticker_exact_v2'],
      label: 'overlay_market_shared',
    });
    if (payload?.reads_scale_with_users === true || Number(payload?.user_read_exchange_requests || 0) > 0 || Number(payload?.user_read_exchange_connections || 0) > 0) {
      throw new Error('overlay_market_shared_boundary_mismatch');
    }
    const rows = Array.isArray(payload?.items) ? payload.items : Array.isArray(payload?.rows) ? payload.rows : [];
    const mapped = new Map();
    for (const item of rows) {
      const spec = marketSpecFromItem(item);
      if (spec) mapped.set(spec, normalizeMarketItem(spec, item));
    }
    for (const spec of specs) {
      const item = mapped.get(spec);
      if (item) latestBySpec.set(spec, item);
      else if (!latestBySpec.has(spec)) latestBySpec.set(spec, { spec, kind:kindOf(spec), ready:false, row:{ provider:spec.split('|')[1], symbol:spec.split('|')[2], price:null, change_percent_24h:null } });
    }
  } catch (error) {
    stats.market_failures += 1;
    stats.last_error = String(error?.message || error).slice(0, 400);
  }
}

function assetFields(spec) {
  const parts = spec.split('|');
  return { provider:parts[1], market_type:parts[2], asset_class:parts[3], product_kind:parts[4], native_symbol:parts[5], security_identity:parts[6] || '' };
}
function assetMatch(spec, row) {
  const f = assetFields(spec);
  const rowProvider = normalizeProvider(row?.provider || row?.exchange || row?.venue);
  const rowMarket = lower(row?.market_type || row?.market || row?.native_market_type);
  const rowClass = lower(row?.asset_class || row?.class || row?.security_type);
  const rowKind = lower(row?.product_kind || row?.kind || row?.product_type);
  const rowSymbol = normalizeSymbol(row?.native_symbol || row?.symbol || row?.product_id || row?.instrument_id);
  const rowIdentity = text(row?.security_identity || row?.security_id || row?.underlying_id || row?.security || '');
  return rowProvider === f.provider && rowMarket === f.market_type && rowClass === f.asset_class && rowKind === f.product_kind && rowSymbol === f.native_symbol && (!f.security_identity || rowIdentity === f.security_identity);
}
async function refreshAssetSpecs(specs) {
  if (!specs.length) return;
  stats.asset_batches += 1;
  try {
    const payload = await localJson(`/api/asset-market/watchlist-tickers?items=${encodeURIComponent(specs.join('~'))}`);
    if (payload?.reads_scale_with_users === true || Number(payload?.user_upstream_requests || 0) > 0 || Number(payload?.user_read_upstream_requests || 0) > 0) {
      throw new Error('overlay_asset_shared_boundary_mismatch');
    }
    const rows = Array.isArray(payload?.items) ? payload.items : Array.isArray(payload?.rows) ? payload.rows : Array.isArray(payload?.data) ? payload.data : [];
    for (const spec of specs) {
      const row = rows.find((x) => assetMatch(spec, x?.row ?? x));
      const raw = row?.row && typeof row.row === 'object' ? row.row : row || {};
      const f = assetFields(spec);
      latestBySpec.set(spec, {
        spec, kind:'asset', ready: !!row,
        row: {
          provider:f.provider, market_type:f.market_type, asset_class:f.asset_class, product_kind:f.product_kind,
          native_symbol:f.native_symbol, security_identity:f.security_identity,
          price:finite(raw?.price ?? raw?.last ?? raw?.last_price ?? raw?.close),
          change_percent_24h:finite(raw?.change_percent_24h ?? raw?.change24h ?? raw?.change_percent ?? raw?.percent_change_24h),
          timestamp:raw?.timestamp ?? raw?.updated_at ?? raw?.ts ?? null,
          source:text(raw?.source || payload?.source || 'asset_shared_watchlist_focus'),
        },
      });
    }
  } catch (error) {
    stats.asset_failures += 1;
    stats.last_error = String(error?.message || error).slice(0, 400);
  }
}

function onchainFields(spec) {
  const p = spec.split('|');
  return { network:p[1], token_address:p[2], pool_address:p[3] };
}
function addressEqual(network, a, b) { return normalizeAddress(network,a) === normalizeAddress(network,b); }
function onchainMatch(spec, row) {
  const f = onchainFields(spec);
  const network = lower(row?.network || row?.chain || row?.chain_id);
  const token = row?.token_address || row?.token_contract || row?.address || row?.base_token_address;
  const pool = row?.pool_address || row?.pool_contract || row?.pair_address || row?.pairAddress;
  return network === f.network && addressEqual(f.network,token,f.token_address) && addressEqual(f.network,pool,f.pool_address);
}
async function refreshOnchainPrices(specs) {
  if (!specs.length) return;
  stats.onchain_price_batches += 1;
  try {
    const payload = await localJson(`/api/onchain/pool-prices?items=${encodeURIComponent(specs.join('~'))}`);
    if (payload?.reads_scale_with_users === true || Number(payload?.user_upstream_requests || 0) > 0 || Number(payload?.user_read_upstream_requests || 0) > 0) {
      throw new Error('overlay_onchain_price_shared_boundary_mismatch');
    }
    const rows = Array.isArray(payload?.items) ? payload.items : Array.isArray(payload?.rows) ? payload.rows : Array.isArray(payload?.data) ? payload.data : [];
    for (const spec of specs) {
      const row = rows.find((x) => onchainMatch(spec, x?.row ?? x));
      const raw = row?.row && typeof row.row === 'object' ? row.row : row || {};
      const f = onchainFields(spec);
      const previousChange = finite(latestBySpec.get(spec)?.row?.change_percent_24h);
      latestBySpec.set(spec, {
        spec, kind:'onchain', ready:!!row,
        row: {
          network:f.network, token_address:f.token_address, pool_address:f.pool_address,
          price:finite(raw?.price ?? raw?.price_usd ?? raw?.usd_price),
          change_percent_24h:finite(raw?.change_percent_24h ?? raw?.change24h ?? raw?.price_change_24h) ?? previousChange,
          timestamp:raw?.timestamp ?? raw?.updated_at ?? raw?.ts ?? null,
          source:text(raw?.source || payload?.source || 'onchain_exact_pool_shared_focus'),
        },
      });
    }
  } catch (error) {
    stats.onchain_price_failures += 1;
    stats.last_error = String(error?.message || error).slice(0, 400);
  }
}

async function sleep(ms) { if (ms > 0) await new Promise((resolve) => setTimeout(resolve, ms)); }
async function dexJson(url) {
  const since = Date.now() - lastDexStartAt;
  if (since < DEX_REQUEST_GAP_MS) await sleep(DEX_REQUEST_GAP_MS - since);
  lastDexStartAt = Date.now(); stats.onchain_change_upstream_requests += 1;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), DEX_TIMEOUT_MS); timer.unref?.();
  try {
    const response = await fetch(url, { headers: { accept:'application/json', 'user-agent':'KakaOverlayOnchainChange/1061.5' }, signal:controller.signal });
    if (!response.ok) throw new Error(`dex_http_${response.status}`);
    return await response.json();
  } finally { clearTimeout(timer); }
}
async function refreshOnchainChanges(specs) {
  if (!specs.length) return;
  stats.onchain_change_batches += 1;
  const byNetwork = new Map();
  for (const spec of specs) {
    const f = onchainFields(spec);
    if (!byNetwork.has(f.network)) byNetwork.set(f.network, []);
    byNetwork.get(f.network).push({ spec, ...f });
  }
  for (const [network, networkSpecs] of byNetwork.entries()) {
    for (const batch of chunked(networkSpecs, DEX_BATCH_MAX)) {
      try {
        const tokenAddresses = [...new Set(batch.map((x) => x.token_address))];
        const url = `${DEX_BASE}/tokens/v1/${encodeURIComponent(network)}/${tokenAddresses.map(encodeURIComponent).join(',')}`;
        const payload = await dexJson(url);
        const pairs = Array.isArray(payload) ? payload : Array.isArray(payload?.pairs) ? payload.pairs : [];
        for (const target of batch) {
          const exactPair = pairs.find((pair) =>
            addressEqual(network, pair?.pairAddress, target.pool_address) &&
            (addressEqual(network, pair?.baseToken?.address, target.token_address) || addressEqual(network, pair?.quoteToken?.address, target.token_address))
          );
          if (!exactPair) continue;
          const change = finite(exactPair?.priceChange?.h24);
          if (change == null) continue;
          const previous = latestBySpec.get(target.spec) || { spec:target.spec, kind:'onchain', ready:false, row:{} };
          latestBySpec.set(target.spec, {
            ...previous,
            row: { ...(previous.row || {}), change_percent_24h: change },
          });
        }
      } catch (error) {
        stats.onchain_change_failures += 1;
        stats.last_error = String(error?.message || error).slice(0, 400);
      }
    }
  }
}

async function runAssetRefresh(specs) {
  if (assetRefreshBusy || !specs.length) return;
  assetRefreshBusy = true;
  try { await refreshAssetSpecs(specs); } finally { assetRefreshBusy = false; }
}
async function runOnchainPriceRefresh(specs) {
  if (onchainPriceRefreshBusy || !specs.length) return;
  onchainPriceRefreshBusy = true;
  try { await refreshOnchainPrices(specs); } finally { onchainPriceRefreshBusy = false; }
}
async function runOnchainChangeRefresh(specs) {
  if (onchainChangeRefreshBusy || !specs.length) return;
  onchainChangeRefreshBusy = true;
  try { await refreshOnchainChanges(specs); } finally { onchainChangeRefreshBusy = false; }
}

async function pollActiveSpecs() {
  if (!clients.size || !activeSpecRefs.size) return;
  if (pollBusy) { stats.poll_busy_skips += 1; return; }
  pollBusy = true; stats.poll_ticks += 1; stats.last_poll_started_at = new Date().toISOString();
  try {
    const specs = [...activeSpecRefs.keys()].sort().slice(0, ACTIVE_SPEC_MAX);
    const marketSpecs = specs.filter((s) => ['spot','contract'].includes(kindOf(s)));
    const assetSpecs = specs.filter((s) => kindOf(s) === 'asset');
    const onchainSpecs = specs.filter((s) => kindOf(s) === 'onchain');
    const now = Date.now();
    // Keep 1s spot/contract latency independent from slower 5s asset/on-chain focus reads and
    // the bounded 30s DEX 24h-change lane. Slow secondary collectors never hold pollBusy.
    if (assetSpecs.length && now - lastAssetRefreshAt >= ASSET_REFRESH_MS) {
      lastAssetRefreshAt = now;
      runAssetRefresh(assetSpecs).catch(() => {});
    }
    if (onchainSpecs.length && now - lastOnchainPriceRefreshAt >= ONCHAIN_PRICE_REFRESH_MS) {
      lastOnchainPriceRefreshAt = now;
      runOnchainPriceRefresh(onchainSpecs).catch(() => {});
    }
    if (onchainSpecs.length && now - lastOnchainChangeRefreshAt >= ONCHAIN_CHANGE_REFRESH_MS) {
      lastOnchainChangeRefreshAt = now;
      runOnchainChangeRefresh(onchainSpecs).catch(() => {});
    }
    await refreshMarketSpecs(marketSpecs);

    for (const entry of [...clients.values()]) {
      if (entry.closed) continue;
      const items = entry.specs.map((spec) => latestBySpec.get(spec)).filter(Boolean);
      if (!items.length) continue;
      const fingerprint = semanticFingerprint(items);
      if (fingerprint === entry.lastFingerprint) { stats.unchanged_skips += 1; continue; }
      entry.lastFingerprint = fingerprint;
      writeSse(entry, 'ticker', {
        ok:true, version:STEP, schema:SCHEMA, route:STREAM_ROUTE,
        items, accepted:entry.specs.length, ready:items.filter((item) => item?.ready === true).length,
        user_read_upstream_requests:0,
        user_read_exchange_connections_started:0,
        reads_scale_with_users:false,
        fixed_background_focus_refresh:true,
        downstream_fanout_shared:true,
        all_market_overlay:true,
        generated_at:new Date().toISOString(),
      });
    }
    stats.last_poll_completed_at = new Date().toISOString();
  } catch (error) {
    stats.last_error = String(error?.message || error).slice(0, 400);
  } finally { pollBusy = false; }
}
function ensureTimers() {
  if (!pollTimer) { pollTimer = setInterval(() => pollActiveSpecs().catch(() => {}), POLL_MS); pollTimer.unref?.(); pollActiveSpecs().catch(() => {}); }
  if (!heartbeatTimer) { heartbeatTimer = setInterval(sendHeartbeat, HEARTBEAT_MS); heartbeatTimer.unref?.(); }
}

function sendJson(res, status, payload, extra = {}) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(status, { 'content-type':'application/json; charset=utf-8', 'cache-control':'no-store', 'access-control-allow-origin':'*', 'content-length':String(body.length), ...extra });
  res.end(body);
}
function healthPayload() {
  const kinds = { spot:0, contract:0, asset:0, onchain:0 };
  for (const spec of activeSpecRefs.keys()) if (kindOf(spec) in kinds) kinds[kindOf(spec)] += 1;
  return {
    ok:true, version:STEP, schema:SCHEMA, stream_route:STREAM_ROUTE,
    client_count:clients.size, client_max:CLIENT_MAX, client_spec_max:CLIENT_SPEC_MAX,
    active_spec_count:activeSpecRefs.size, active_spec_max:ACTIVE_SPEC_MAX, active_by_kind:kinds,
    poll_ms:POLL_MS, asset_refresh_ms:ASSET_REFRESH_MS, onchain_price_refresh_ms:ONCHAIN_PRICE_REFRESH_MS,
    onchain_change_refresh_ms:ONCHAIN_CHANGE_REFRESH_MS, heartbeat_ms:HEARTBEAT_MS,
    user_read_exchange_requests:0, user_read_exchange_connections:0, reads_scale_with_users:false,
    one_downstream_stream_per_overlay_client:true,
    collector_reads_bounded_by_active_exact_identity_not_client_count:true,
    spot_contract_source:'isolated_market_light_shared_snapshot',
    asset_source:'fixed_background_asset_watchlist_focus',
    onchain_price_source:'fixed_background_exact_pool_price_focus',
    onchain_change_source:'bounded_background_dexscreener_exact_pool_refresh',
    onchain_change_user_read_triggered:false,
    binance_futures_rest_requests_added:0,
    semantic_change_only_downstream:true,
    ...stats,
    now:new Date().toISOString(),
  };
}
function handleStream(req, res, url) {
  if (req.method === 'OPTIONS') { res.writeHead(204, {'access-control-allow-origin':'*','access-control-allow-methods':'GET, OPTIONS','access-control-allow-headers':'accept, cache-control','cache-control':'no-store'}); res.end(); return true; }
  if (req.method !== 'GET') { sendJson(res,405,{ok:false,version:STEP,error:'method_not_allowed'}); return true; }
  const specs = parseSpecs(url.searchParams.get('items'));
  if (!specs.length || specs.length > CLIENT_SPEC_MAX) { stats.rejected_invalid += 1; sendJson(res,400,{ok:false,version:STEP,error:'invalid_or_too_many_overlay_specs',client_spec_max:CLIENT_SPEC_MAX}); return true; }
  if (clients.size >= CLIENT_MAX) { stats.rejected_capacity += 1; sendJson(res,503,{ok:false,version:STEP,error:'stream_client_capacity',retry_after_seconds:5},{'retry-after':'5'}); return true; }
  const newUnique = specs.filter((s) => !activeSpecRefs.has(s)).length;
  if (activeSpecRefs.size + newUnique > ACTIVE_SPEC_MAX) { stats.rejected_capacity += 1; sendJson(res,503,{ok:false,version:STEP,error:'stream_active_spec_capacity',retry_after_seconds:5},{'retry-after':'5'}); return true; }
  const ip = downstreamIp(req); const attempts = pruneConnectAttempts(ip);
  if (attempts.length >= CONNECTS_PER_IP_PER_MINUTE) { stats.rejected_ip_rate += 1; sendJson(res,429,{ok:false,version:STEP,error:'stream_ip_connect_rate',retry_after_seconds:5},{'retry-after':'5'}); return true; }
  if (Number(clientsByIp.get(ip) || 0) >= CLIENTS_PER_IP_MAX) { stats.rejected_ip_capacity += 1; sendJson(res,503,{ok:false,version:STEP,error:'stream_ip_capacity',retry_after_seconds:5},{'retry-after':'5'}); return true; }
  attempts.push(Date.now()); connectAttemptsByIp.set(ip, attempts);
  res.writeHead(200, {'content-type':'text/event-stream; charset=utf-8','cache-control':'no-store, no-transform','connection':'keep-alive','access-control-allow-origin':'*','x-kaka-stream-schema':SCHEMA});
  res.write(`event: ready\ndata: ${JSON.stringify({ok:true,version:STEP,schema:SCHEMA,specs,user_read_upstream_requests:0,user_read_exchange_connections_started:0,reads_scale_with_users:false,fixed_background_focus_refresh:true,downstream_fanout_shared:true})}\n\n`);
  const entry = { id:++clientSeq, ip, req, res, specs, lastFingerprint:'', closed:false, connectedAt:Date.now() };
  clients.set(entry.id, entry); clientsByIp.set(ip, Number(clientsByIp.get(ip) || 0) + 1); incrementSpecRefs(specs); stats.accepted_connections += 1;
  const close = () => closeClient(entry); req.once('aborted',close); req.once('close',close); res.once('close',close); res.once('error',close); ensureTimers(); return true;
}

const originalCreateServer = http.createServer.bind(http);
http.createServer = function step1061OverlayStreamCreateServer(listener, ...rest) {
  return originalCreateServer(async (req, res) => {
    let url;
    try { url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`); } catch (_) { return listener(req,res); }
    if (url.pathname === HEALTH_ROUTE) {
      if (req.method === 'OPTIONS') { res.writeHead(204, {'access-control-allow-origin':'*','access-control-allow-methods':'GET, OPTIONS','cache-control':'no-store'}); res.end(); return; }
      if (req.method !== 'GET') { sendJson(res,405,{ok:false,version:STEP,error:'method_not_allowed'}); return; }
      sendJson(res,200,healthPayload()); return;
    }
    if (url.pathname === STREAM_ROUTE) { handleStream(req,res,url); return; }
    return listener(req,res);
  }, ...rest);
};

export function getOverlayDownstreamStreamHealth() { return healthPayload(); }
