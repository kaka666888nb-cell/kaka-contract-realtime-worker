import http from 'node:http';
import { requestIsolatedJson } from './collector-isolation.mjs';

const STEP = '1061.5.1';
const SCHEMA = 'step1061_5_1_all_market_overlay_downstream_sse_v1';
const STREAM_ROUTE = '/api/overlay/watchlist-stream';
const HEALTH_ROUTE = '/api/overlay/watchlist-stream-health';

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
const LOCAL_ORIGIN = `http://127.0.0.1:${Number(process.env.PORT || 10000)}`;
const DEX_BASE = 'https://api.dexscreener.com';
const DEX_BATCH_MAX = 30;
const DEX_REQUEST_GAP_MS = Math.max(1200, Number(process.env.KAKA_OVERLAY_DEX_REQUEST_GAP_MS || 1500));
const DEX_TIMEOUT_MS = Math.max(5000, Math.min(20_000, Number(process.env.KAKA_OVERLAY_DEX_TIMEOUT_MS || 10_000)));

const SPOT_PROVIDERS = new Set(['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate']);
const CONTRACT_PROVIDERS = new Set(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const ONCHAIN_NETWORKS = new Set(['ethereum', 'bsc', 'base', 'solana', 'arbitrum', 'polygon', 'optimism', 'avalanche', 'linea']);

const clients = new Map();
const clientsByIp = new Map();
const connectAttemptsByIp = new Map();
const activeSpecRefs = new Map();
const latestBySpec = new Map();
let clientSeq = 0;
let pollTimer = null;
let heartbeatTimer = null;
let pollBusy = false;
let assetRefreshBusy = false;
let onchainPriceRefreshBusy = false;
let onchainChangeRefreshBusy = false;
let lastAssetRefreshAt = 0;
let lastOnchainPriceRefreshAt = 0;
let lastOnchainChangeRefreshAt = 0;
let lastDexStartAt = 0;

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

const text = (value) => String(value ?? '').trim();
const lower = (value) => text(value).toLowerCase();
const finite = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};
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
function cleanIdentityPart(value) {
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
    if (parts.length !== 6 && parts.length !== 7) return '';
    const provider = normalizeProvider(parts[1]);
    const market = lower(parts[2]);
    const assetClass = lower(parts[3]);
    const productKind = lower(parts[4]);
    const symbol = normalizeSymbol(parts[5]);
    const securityIdentity = parts.length === 7 ? cleanIdentityPart(parts[6]) : '';
    if (!provider || !market || !assetClass || !productKind || !symbol) return '';
    return `asset|${cleanIdentityPart(provider)}|${cleanIdentityPart(market)}|${cleanIdentityPart(assetClass)}|${cleanIdentityPart(productKind)}|${symbol}${securityIdentity ? `|${securityIdentity}` : ''}`;
  }
  if (kind === 'onchain') {
    if (parts.length !== 4) return '';
    const network = lower(parts[1]);
    if (!ONCHAIN_NETWORKS.has(network)) return '';
    const token = normalizeAddress(network, parts[2]);
    const pool = normalizeAddress(network, parts[3]);
    if (!token || !pool || token.includes('~') || pool.includes('~')) return '';
    return `onchain|${network}|${token}|${pool}`;
  }
  return '';
}
function parseSpecs(raw) {
  return [...new Set(text(raw).split('~').map(canonicalSpec).filter(Boolean))].sort();
}
function kindOf(spec) { return spec.split('|')[0] || ''; }
function chunked(values, size) {
  const output = [];
  for (let i = 0; i < values.length; i += size) output.push(values.slice(i, i + size));
  return output;
}
function semanticFingerprint(items) {
  return JSON.stringify(items.map((item) => ({ spec:item.spec, kind:item.kind, ready:item.ready, row:item.row })));
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
function downstreamIp(req) {
  const forwarded = text(req?.headers?.['x-forwarded-for']).split(',').map((x) => x.trim()).find(Boolean);
  return forwarded || text(req?.socket?.remoteAddress) || 'unknown';
}
function pruneConnectAttempts(ip) {
  const cutoff = Date.now() - 60_000;
  const attempts = connectAttemptsByIp.get(ip) || [];
  while (attempts.length && attempts[0] < cutoff) attempts.shift();
  if (attempts.length) connectAttemptsByIp.set(ip, attempts); else connectAttemptsByIp.delete(ip);
  return attempts;
}
function stopTimersIfIdle() {
  if (clients.size) return;
  clearInterval(pollTimer);
  clearInterval(heartbeatTimer);
  pollTimer = null;
  heartbeatTimer = null;
  latestBySpec.clear();
}
function closeClient(entry) {
  if (!entry || entry.closed) return;
  entry.closed = true;
  clients.delete(entry.id);
  decrementSpecRefs(entry.specs);
  const count = Math.max(0, Number(clientsByIp.get(entry.ip) || 0) - 1);
  if (count) clientsByIp.set(entry.ip, count); else clientsByIp.delete(entry.ip);
  stats.closed_connections += 1;
  stopTimersIfIdle();
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

async function localJson(pathname, timeoutMs = 8000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  timer.unref?.();
  try {
    const response = await fetch(`${LOCAL_ORIGIN}${pathname}`, {
      method:'GET',
      headers:{ accept:'application/json', 'user-agent':'KakaOverlaySharedCollector/1061.5.1' },
      signal:controller.signal,
    });
    const payload = await response.json().catch(() => null);
    if (!response.ok || !payload) throw new Error(`local_http_${response.status}:${pathname}`);
    return payload;
  } finally { clearTimeout(timer); }
}

function marketSpecFromItem(item) {
  const row = item?.row && typeof item.row === 'object' ? item.row : {};
  return canonicalSpec(`${item?.market_type ?? row.market_type}|${item?.provider ?? row.provider}|${item?.symbol ?? row.symbol}`);
}
function normalizeMarketItem(spec, item) {
  const row = item?.row && typeof item.row === 'object' ? item.row : {};
  const price = finite(row?.last_price ?? row?.contract_price ?? row?.price ?? row?.last ?? row?.close ?? row?.mark_price ?? row?.markPrice);
  const mark = finite(row?.mark_price ?? row?.markPrice ?? row?.mark_px);
  const change = finite(row?.price_change_percent_24h ?? row?.change_percent_24h ?? row?.change_24h_percent ?? row?.change24h ?? row?.change_percent ?? row?.percent_change_24h);
  return {
    spec,
    kind:kindOf(spec),
    ready:item?.ready === true || (price != null && price > 0),
    row:{
      provider:normalizeProvider(item?.provider ?? row.provider ?? spec.split('|')[1]),
      market_type:lower(item?.market_type ?? row.market_type ?? kindOf(spec)),
      symbol:text(item?.symbol ?? row.symbol ?? spec.split('|')[2]).toUpperCase(),
      price,
      ...(mark != null ? { mark_price:mark } : {}),
      ...(change != null ? { change_percent_24h:change } : {}),
      timestamp:row?.timestamp ?? row?.updated_at ?? row?.ts ?? null,
      source:text(row?.source || item?.source || 'market_light_shared_watchlist'),
    },
  };
}
async function refreshMarketSpecs(specs) {
  if (!specs.length) return;
  for (const batch of chunked(specs, 64)) {
    stats.market_batches += 1;
    try {
      const path = `/api/market-light/watchlist-tickers?items=${encodeURIComponent(batch.join('~'))}`;
      const payload = await requestIsolatedJson('market-light', path, 4500);
      if (payload?.ok !== true || Number(payload?.user_read_upstream_requests ?? -1) !== 0 || payload?.reads_scale_with_users !== false || !Array.isArray(payload?.items)) {
        throw new Error('overlay_market_shared_boundary_mismatch');
      }
      for (const item of payload.items) {
        const spec = marketSpecFromItem(item);
        if (!spec || !activeSpecRefs.has(spec)) continue;
        latestBySpec.set(spec, normalizeMarketItem(spec, item));
      }
    } catch (error) {
      stats.market_failures += 1;
      stats.last_error = String(error?.message || error).slice(0, 400);
    }
  }
}

function assetParts(spec) {
  const p = spec.split('|');
  return { provider:p[1], market_type:p[2], asset_class:p[3], product_kind:p[4], symbol:p[5] };
}
function normalizeAssetItem(spec, item) {
  const current = item?.market_item && typeof item.market_item === 'object' ? item.market_item : {};
  const ticker = current?.ticker && typeof current.ticker === 'object' ? current.ticker : current;
  const price = finite(ticker?.last_price ?? ticker?.price ?? current?.last_price ?? current?.price);
  const ratio = finite(ticker?.price_change_24h_ratio ?? current?.price_change_24h_ratio);
  const percent = finite(ticker?.price_change_percent_24h ?? current?.price_change_percent_24h);
  const change = ratio != null ? ratio * 100 : percent;
  return { spec, kind:'asset', ready:price != null && price > 0, row:{ price, ...(change != null ? { change_percent_24h:change } : {}) } };
}
async function refreshAssetSpecs(specs) {
  if (!specs.length) return;
  for (const batch of chunked(specs, 32)) {
    stats.asset_batches += 1;
    try {
      const itemsParam = batch.map((spec) => {
        const p = assetParts(spec);
        return [p.provider,p.market_type,p.asset_class,p.product_kind,p.symbol].join('|');
      }).join('~');
      const payload = await localJson(`/api/asset-market/watchlist-tickers?items=${encodeURIComponent(itemsParam)}`);
      if (payload?.ok !== true || Number(payload?.user_read_upstream_requests ?? -1) !== 0 || payload?.reads_scale_with_users !== false || payload?.fixed_background_refresh_independent_of_user_count !== true || !Array.isArray(payload?.items)) {
        throw new Error('overlay_asset_shared_boundary_mismatch');
      }
      const requestedByBase = new Map();
      for (const requested of batch) {
        const p = assetParts(requested);
        const base = canonicalSpec(`asset|${p.provider}|${p.market_type}|${p.asset_class}|${p.product_kind}|${p.symbol}`);
        if (!requestedByBase.has(base)) requestedByBase.set(base, []);
        requestedByBase.get(base).push(requested);
      }
      for (const item of payload.items) {
        const base = canonicalSpec(`asset|${item?.provider}|${item?.market_type}|${item?.asset_class}|${item?.product_kind}|${item?.native_symbol}`);
        if (!base) continue;
        for (const spec of requestedByBase.get(base) || []) {
          if (activeSpecRefs.has(spec)) latestBySpec.set(spec, normalizeAssetItem(spec, item));
        }
      }
    } catch (error) {
      stats.asset_failures += 1;
      stats.last_error = String(error?.message || error).slice(0, 400);
    }
  }
}

function onchainParts(spec) {
  const p = spec.split('|');
  return { network:p[1], token_address:p[2], pool_address:p[3] };
}
function onchainSpecFromRow(row) {
  return canonicalSpec(`onchain|${row?.network}|${row?.token_address}|${row?.pool_address}`);
}
async function refreshOnchainPrices(specs) {
  if (!specs.length) return;
  for (const batch of chunked(specs, 32)) {
    stats.onchain_price_batches += 1;
    try {
      const itemsParam = batch.map((spec) => {
        const p = onchainParts(spec);
        return `${p.network}|${p.token_address}|${p.pool_address}`;
      }).join('~');
      const payload = await localJson(`/api/onchain/pool-prices?items=${encodeURIComponent(itemsParam)}`);
      if (Number(payload?.user_read_upstream_requests ?? -1) !== 0 || Number(payload?.direct_upstream_requests ?? -1) !== 0 || payload?.fixed_background_rate_independent_of_user_count !== true || !Array.isArray(payload?.rows)) {
        throw new Error('overlay_onchain_price_shared_boundary_mismatch');
      }
      for (const row of payload.rows) {
        const spec = onchainSpecFromRow(row);
        if (!spec || !activeSpecRefs.has(spec)) continue;
        const price = finite(row?.price_usd);
        const previous = latestBySpec.get(spec);
        latestBySpec.set(spec, {
          spec, kind:'onchain', ready:price != null && price > 0,
          row:{ price, ...(previous?.row?.change_percent_24h != null ? { change_percent_24h:previous.row.change_percent_24h } : {}) },
        });
      }
    } catch (error) {
      stats.onchain_price_failures += 1;
      stats.last_error = String(error?.message || error).slice(0, 400);
    }
  }
}

function addressEqual(network, a, b) {
  return normalizeAddress(network, a) === normalizeAddress(network, b);
}
async function waitDexLane() {
  const wait = Math.max(0, DEX_REQUEST_GAP_MS - (Date.now() - lastDexStartAt));
  if (wait > 0) await new Promise((resolve) => setTimeout(resolve, wait));
  lastDexStartAt = Date.now();
}
async function dexJson(url) {
  await waitDexLane();
  stats.onchain_change_upstream_requests += 1;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), DEX_TIMEOUT_MS);
  timer.unref?.();
  try {
    const response = await fetch(url, { headers:{ accept:'application/json', 'user-agent':'KakaOverlayOnchainChange/1061.5.1' }, signal:controller.signal });
    if (!response.ok) throw new Error(`dex_http_${response.status}`);
    return await response.json();
  } finally { clearTimeout(timer); }
}
async function refreshOnchainChanges(specs) {
  if (!specs.length) return;
  const byNetwork = new Map();
  for (const spec of specs) {
    const p = onchainParts(spec);
    if (!byNetwork.has(p.network)) byNetwork.set(p.network, []);
    byNetwork.get(p.network).push({ spec, ...p });
  }
  for (const [network, rows] of byNetwork) {
    for (const batch of chunked(rows, DEX_BATCH_MAX)) {
      stats.onchain_change_batches += 1;
      try {
        const tokens = [...new Set(batch.map((x) => x.token_address))];
        const payload = await dexJson(`${DEX_BASE}/tokens/v1/${encodeURIComponent(network)}/${tokens.map(encodeURIComponent).join(',')}`);
        const pairs = Array.isArray(payload) ? payload : Array.isArray(payload?.pairs) ? payload.pairs : [];
        for (const target of batch) {
          const pair = pairs.find((row) => addressEqual(network, row?.pairAddress, target.pool_address) && (addressEqual(network, row?.baseToken?.address, target.token_address) || addressEqual(network, row?.quoteToken?.address, target.token_address)));
          const change = finite(pair?.priceChange?.h24);
          if (change == null) continue;
          const previous = latestBySpec.get(target.spec) || { spec:target.spec, kind:'onchain', ready:false, row:{} };
          latestBySpec.set(target.spec, { ...previous, row:{ ...(previous.row || {}), change_percent_24h:change } });
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
  pollBusy = true;
  stats.poll_ticks += 1;
  stats.last_poll_started_at = new Date().toISOString();
  try {
    const specs = [...activeSpecRefs.keys()].sort().slice(0, ACTIVE_SPEC_MAX);
    const marketSpecs = specs.filter((spec) => ['spot','contract'].includes(kindOf(spec)));
    const assetSpecs = specs.filter((spec) => kindOf(spec) === 'asset');
    const onchainSpecs = specs.filter((spec) => kindOf(spec) === 'onchain');
    const now = Date.now();
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
  if (!pollTimer) {
    pollTimer = setInterval(() => pollActiveSpecs().catch(() => {}), POLL_MS);
    pollTimer.unref?.();
    pollActiveSpecs().catch(() => {});
  }
  if (!heartbeatTimer) {
    heartbeatTimer = setInterval(sendHeartbeat, HEARTBEAT_MS);
    heartbeatTimer.unref?.();
  }
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
    spot_contract_source:'isolated_market_light_watchlist_tickers',
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
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {'access-control-allow-origin':'*','access-control-allow-methods':'GET, OPTIONS','access-control-allow-headers':'accept, cache-control','cache-control':'no-store'});
    res.end();
    return true;
  }
  if (req.method !== 'GET') { sendJson(res,405,{ok:false,version:STEP,error:'method_not_allowed'}); return true; }
  const specs = parseSpecs(url.searchParams.get('items'));
  if (!specs.length || specs.length > CLIENT_SPEC_MAX) { stats.rejected_invalid += 1; sendJson(res,400,{ok:false,version:STEP,error:'invalid_or_too_many_overlay_specs',client_spec_max:CLIENT_SPEC_MAX}); return true; }
  if (clients.size >= CLIENT_MAX) { stats.rejected_capacity += 1; sendJson(res,503,{ok:false,version:STEP,error:'stream_client_capacity',retry_after_seconds:5},{'retry-after':'5'}); return true; }
  const newUnique = specs.filter((spec) => !activeSpecRefs.has(spec)).length;
  if (activeSpecRefs.size + newUnique > ACTIVE_SPEC_MAX) { stats.rejected_capacity += 1; sendJson(res,503,{ok:false,version:STEP,error:'stream_active_spec_capacity',retry_after_seconds:5},{'retry-after':'5'}); return true; }
  const ip = downstreamIp(req);
  const attempts = pruneConnectAttempts(ip);
  if (attempts.length >= CONNECTS_PER_IP_PER_MINUTE) { stats.rejected_ip_rate += 1; sendJson(res,429,{ok:false,version:STEP,error:'stream_ip_connect_rate',retry_after_seconds:5},{'retry-after':'5'}); return true; }
  if (Number(clientsByIp.get(ip) || 0) >= CLIENTS_PER_IP_MAX) { stats.rejected_ip_capacity += 1; sendJson(res,503,{ok:false,version:STEP,error:'stream_ip_capacity',retry_after_seconds:5},{'retry-after':'5'}); return true; }
  attempts.push(Date.now());
  connectAttemptsByIp.set(ip, attempts);
  res.writeHead(200, {'content-type':'text/event-stream; charset=utf-8','cache-control':'no-store, no-transform','connection':'keep-alive','access-control-allow-origin':'*','x-kaka-stream-schema':SCHEMA});
  res.write(`event: ready\ndata: ${JSON.stringify({ok:true,version:STEP,schema:SCHEMA,specs,user_read_upstream_requests:0,user_read_exchange_connections_started:0,reads_scale_with_users:false,fixed_background_focus_refresh:true,downstream_fanout_shared:true})}\n\n`);
  const entry = { id:++clientSeq, ip, req, res, specs, lastFingerprint:'', closed:false, connectedAt:Date.now() };
  clients.set(entry.id, entry);
  clientsByIp.set(ip, Number(clientsByIp.get(ip) || 0) + 1);
  incrementSpecRefs(specs);
  stats.accepted_connections += 1;
  const close = () => closeClient(entry);
  req.once('aborted', close);
  req.once('close', close);
  res.once('close', close);
  res.once('error', close);
  ensureTimers();
  return true;
}

const originalCreateServer = http.createServer.bind(http);
http.createServer = function step1061OverlayStreamCreateServer(listener, ...rest) {
  return originalCreateServer(async (req, res) => {
    let url;
    try { url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`); }
    catch (_) { return listener(req, res); }
    if (url.pathname === HEALTH_ROUTE) {
      if (req.method === 'OPTIONS') { res.writeHead(204, {'access-control-allow-origin':'*','access-control-allow-methods':'GET, OPTIONS','cache-control':'no-store'}); res.end(); return; }
      if (req.method !== 'GET') { sendJson(res,405,{ok:false,version:STEP,error:'method_not_allowed'}); return; }
      sendJson(res,200,healthPayload());
      return;
    }
    if (url.pathname === STREAM_ROUTE) { handleStream(req,res,url); return; }
    return listener(req,res);
  }, ...rest);
};

export function getOverlayDownstreamStreamHealth() { return healthPayload(); }
