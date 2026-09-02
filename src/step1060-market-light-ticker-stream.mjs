import http from 'node:http';
import { requestIsolatedJson } from './collector-isolation.mjs';

const STEP = '1060.31.7';
const SCHEMA = 'step1060_31_7_market_light_downstream_sse_v1';
const STREAM_ROUTE = '/api/market-light/watchlist-stream';
const HEALTH_ROUTE = '/api/market-light/watchlist-stream-health';

const CLIENT_SPEC_MAX = 16;
const ACTIVE_SPEC_MAX = 256;
const COLLECTOR_BATCH_MAX = 64;
const CLIENT_MAX = Math.max(1000, Number(process.env.KAKA_MARKET_STREAM_CLIENT_MAX || 1500));
const CLIENTS_PER_IP_MAX = Math.max(10, Number(process.env.KAKA_MARKET_STREAM_CLIENTS_PER_IP_MAX || 50));
const CONNECTS_PER_IP_PER_MINUTE = Math.max(10, Number(process.env.KAKA_MARKET_STREAM_CONNECTS_PER_IP_PER_MINUTE || 60));
const POLL_MS = Math.max(750, Number(process.env.KAKA_MARKET_STREAM_POLL_MS || 1000));
const HEARTBEAT_MS = Math.max(10_000, Number(process.env.KAKA_MARKET_STREAM_HEARTBEAT_MS || 15_000));
const SLOW_CLIENT_MAX_BUFFERED_BYTES = Math.max(64 * 1024, Number(process.env.KAKA_MARKET_STREAM_SLOW_CLIENT_MAX_BUFFERED_BYTES || 512 * 1024));

const SPOT_PROVIDERS = new Set(['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate']);
const CONTRACT_PROVIDERS = new Set(['binance', 'okx', 'bybit', 'bitget', 'gate']);

const clients = new Map();
const activeSpecRefs = new Map();
const latestBySpec = new Map();
const clientsByIp = new Map();
const connectAttemptsByIp = new Map();
let clientSeq = 0;
let pollTimer = null;
let heartbeatTimer = null;
let pollBusy = false;

const stats = {
  accepted_connections: 0,
  closed_connections: 0,
  rejected_capacity: 0,
  rejected_invalid: 0,
  rejected_ip_capacity: 0,
  rejected_ip_rate: 0,
  collector_batches: 0,
  collector_batch_failures: 0,
  collector_items: 0,
  poll_ticks: 0,
  poll_busy_skips: 0,
  downstream_events: 0,
  downstream_bytes: 0,
  unchanged_skips: 0,
  slow_client_disconnects: 0,
  last_poll_started_at: null,
  last_poll_completed_at: null,
  last_error: '',
};

function normalizeSymbol(value) {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/-SWAP$/i, '')
    .replace(/_UMCBL$/i, '')
    .replace(/[^A-Z0-9]/g, '');
}

function normalizeProvider(value) {
  let provider = String(value || '').trim().toLowerCase();
  if (provider === 'gate.io') provider = 'gate';
  if (provider === 'okex') provider = 'okx';
  return provider;
}

function canonicalSpec(value) {
  const parts = String(value || '').trim().split('|');
  if (parts.length !== 3) return '';
  const market = String(parts[0] || '').trim().toLowerCase();
  const provider = normalizeProvider(parts[1]);
  const symbol = normalizeSymbol(parts[2]);
  if (!['spot', 'contract'].includes(market) || !symbol) return '';
  if (market === 'spot' && !SPOT_PROVIDERS.has(provider)) return '';
  if (market === 'contract' && !CONTRACT_PROVIDERS.has(provider)) return '';
  return `${market}|${provider}|${symbol}`;
}

function parseSpecs(raw) {
  const values = String(raw || '')
    .split('~')
    .map(canonicalSpec)
    .filter(Boolean);
  return [...new Set(values)].sort();
}

function itemSpec(item) {
  const row = item?.row && typeof item.row === 'object' ? item.row : {};
  return canonicalSpec([
    item?.market_type ?? row.market_type,
    item?.provider ?? row.provider,
    item?.symbol ?? row.symbol,
  ].join('|'));
}

const VOLATILE_KEYS = new Set([
  'generated_at',
  'cached_at',
  'cache_age_seconds',
  'response_generated_at',
  'last_started_at',
  'last_completed_at',
  'last_attempt_at',
  'last_success_at',
]);

function semanticValue(value) {
  if (Array.isArray(value)) return value.map(semanticValue);
  if (!value || typeof value !== 'object') return value;
  const output = {};
  for (const key of Object.keys(value).sort()) {
    if (VOLATILE_KEYS.has(key) || key.endsWith('_age_ms')) continue;
    output[key] = semanticValue(value[key]);
  }
  return output;
}

function semanticFingerprint(items) {
  return JSON.stringify(items.map((item) => semanticValue({
    market_type: item?.market_type,
    provider: item?.provider,
    symbol: item?.symbol,
    ready: item?.ready,
    row: item?.row,
  })));
}

function downstreamIp(req) {
  const forwarded = String(req?.headers?.['x-forwarded-for'] || '')
    .split(',')
    .map((part) => part.trim())
    .find(Boolean);
  return forwarded || String(req?.socket?.remoteAddress || 'unknown');
}

function pruneConnectAttempts(ip) {
  const cutoff = Date.now() - 60_000;
  const attempts = connectAttemptsByIp.get(ip) || [];
  while (attempts.length && attempts[0] < cutoff) attempts.shift();
  if (attempts.length) connectAttemptsByIp.set(ip, attempts);
  else connectAttemptsByIp.delete(ip);
  return attempts;
}

function incrementSpecRefs(specs) {
  for (const spec of specs) activeSpecRefs.set(spec, Number(activeSpecRefs.get(spec) || 0) + 1);
}

function decrementSpecRefs(specs) {
  for (const spec of specs) {
    const next = Math.max(0, Number(activeSpecRefs.get(spec) || 0) - 1);
    if (next) activeSpecRefs.set(spec, next);
    else activeSpecRefs.delete(spec);
  }
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
  const ipCount = Math.max(0, Number(clientsByIp.get(entry.ip) || 0) - 1);
  if (ipCount) clientsByIp.set(entry.ip, ipCount);
  else clientsByIp.delete(entry.ip);
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
  const text = `: kaka-${STEP}-keepalive ${Date.now()}\n\n`;
  for (const entry of [...clients.values()]) {
    if (entry.closed || entry.res.writableEnded || entry.res.destroyed) {
      closeClient(entry);
      continue;
    }
    if (Number(entry.res.writableLength || 0) > SLOW_CLIENT_MAX_BUFFERED_BYTES) {
      stats.slow_client_disconnects += 1;
      try { entry.res.destroy(); } catch (_) {}
      closeClient(entry);
      continue;
    }
    try {
      entry.res.write(text);
      stats.downstream_bytes += Buffer.byteLength(text);
    } catch (_) {
      closeClient(entry);
    }
  }
}

function chunked(values, size) {
  const chunks = [];
  for (let index = 0; index < values.length; index += size) chunks.push(values.slice(index, index + size));
  return chunks;
}

async function pollActiveSpecs() {
  if (!clients.size || !activeSpecRefs.size) return;
  if (pollBusy) {
    stats.poll_busy_skips += 1;
    return;
  }
  pollBusy = true;
  stats.poll_ticks += 1;
  stats.last_poll_started_at = new Date().toISOString();
  try {
    const specs = [...activeSpecRefs.keys()].sort().slice(0, ACTIVE_SPEC_MAX);
    const batches = chunked(specs, COLLECTOR_BATCH_MAX);
    const results = await Promise.allSettled(batches.map(async (batch) => {
      stats.collector_batches += 1;
      const path = `/api/market-light/watchlist-tickers?items=${encodeURIComponent(batch.join('~'))}`;
      const payload = await requestIsolatedJson('market-light', path, 4_000);
      if (payload?.ok !== true ||
          Number(payload?.user_read_upstream_requests ?? -1) !== 0 ||
          payload?.reads_scale_with_users !== false ||
          !Array.isArray(payload?.items)) {
        throw new Error('market_stream_shared_boundary_mismatch');
      }
      return payload;
    }));

    for (const result of results) {
      if (result.status !== 'fulfilled') {
        stats.collector_batch_failures += 1;
        stats.last_error = String(result.reason?.message || result.reason || 'collector_batch_failed').slice(0, 400);
        continue;
      }
      for (const item of result.value.items) {
        const key = itemSpec(item);
        if (!key || !activeSpecRefs.has(key)) continue;
        latestBySpec.set(key, item);
        stats.collector_items += 1;
      }
    }

    for (const entry of [...clients.values()]) {
      if (entry.closed) continue;
      const items = entry.specs.map((spec) => latestBySpec.get(spec)).filter(Boolean);
      if (!items.length) continue;
      const fingerprint = semanticFingerprint(items);
      if (fingerprint === entry.lastFingerprint) {
        stats.unchanged_skips += 1;
        continue;
      }
      entry.lastFingerprint = fingerprint;
      writeSse(entry, 'ticker', {
        ok: true,
        version: STEP,
        schema: SCHEMA,
        route: STREAM_ROUTE,
        items,
        accepted: entry.specs.length,
        ready: items.filter((item) => item?.ready === true).length,
        user_read_upstream_requests: 0,
        user_read_exchange_connections_started: 0,
        reads_scale_with_users: false,
        fixed_background_focus_refresh: true,
        downstream_fanout_shared: true,
        generated_at: new Date().toISOString(),
      });
    }
    stats.last_poll_completed_at = new Date().toISOString();
    if (!results.some((result) => result.status === 'rejected')) stats.last_error = '';
  } catch (error) {
    stats.last_error = String(error?.message || error).slice(0, 400);
  } finally {
    pollBusy = false;
  }
}

function ensureTimers() {
  let startedPollTimer = false;
  if (!pollTimer) {
    startedPollTimer = true;
    pollTimer = setInterval(() => pollActiveSpecs().catch(() => {}), POLL_MS);
    pollTimer.unref?.();
  }
  if (!heartbeatTimer) {
    heartbeatTimer = setInterval(sendHeartbeat, HEARTBEAT_MS);
    heartbeatTimer.unref?.();
  }
  if (startedPollTimer) pollActiveSpecs().catch(() => {});
}

function sendJson(res, status, payload, extraHeaders = {}) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'access-control-allow-origin': '*',
    'content-length': String(body.length),
    ...extraHeaders,
  });
  res.end(body);
}

function healthPayload() {
  return {
    ok: true,
    version: STEP,
    schema: SCHEMA,
    stream_route: STREAM_ROUTE,
    client_count: clients.size,
    client_max: CLIENT_MAX,
    active_spec_count: activeSpecRefs.size,
    active_spec_max: ACTIVE_SPEC_MAX,
    client_spec_max: CLIENT_SPEC_MAX,
    collector_batch_max: COLLECTOR_BATCH_MAX,
    fixed_max_collector_batches_per_poll: Math.ceil(ACTIVE_SPEC_MAX / COLLECTOR_BATCH_MAX),
    poll_ms: POLL_MS,
    heartbeat_ms: HEARTBEAT_MS,
    clients_per_ip_max: CLIENTS_PER_IP_MAX,
    connects_per_ip_per_minute: CONNECTS_PER_IP_PER_MINUTE,
    slow_client_max_buffered_bytes: SLOW_CLIENT_MAX_BUFFERED_BYTES,
    user_read_exchange_requests: 0,
    user_read_exchange_connections: 0,
    reads_scale_with_users: false,
    collector_reads_bounded_by_active_exact_identity_not_client_count: true,
    semantic_change_only_downstream: true,
    ...stats,
    now: new Date().toISOString(),
  };
}

function handleStream(req, res, url) {
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET, OPTIONS',
      'access-control-allow-headers': 'accept, cache-control',
      'cache-control': 'no-store',
    });
    res.end();
    return true;
  }
  if (req.method !== 'GET') {
    sendJson(res, 405, { ok: false, version: STEP, error: 'method_not_allowed' });
    return true;
  }

  const specs = parseSpecs(url.searchParams.get('items'));
  if (!specs.length || specs.length > CLIENT_SPEC_MAX) {
    stats.rejected_invalid += 1;
    sendJson(res, 400, {
      ok: false,
      version: STEP,
      error: 'invalid_or_too_many_exact_ticker_specs',
      client_spec_max: CLIENT_SPEC_MAX,
    });
    return true;
  }

  if (clients.size >= CLIENT_MAX) {
    stats.rejected_capacity += 1;
    sendJson(res, 503, { ok: false, version: STEP, error: 'stream_client_capacity', retry_after_seconds: 5 }, { 'retry-after': '5' });
    return true;
  }

  const newUniqueCount = specs.filter((spec) => !activeSpecRefs.has(spec)).length;
  if (activeSpecRefs.size + newUniqueCount > ACTIVE_SPEC_MAX) {
    stats.rejected_capacity += 1;
    sendJson(res, 503, { ok: false, version: STEP, error: 'stream_active_spec_capacity', retry_after_seconds: 5 }, { 'retry-after': '5' });
    return true;
  }

  const ip = downstreamIp(req);
  const attempts = pruneConnectAttempts(ip);
  if (attempts.length >= CONNECTS_PER_IP_PER_MINUTE) {
    stats.rejected_ip_rate += 1;
    sendJson(res, 429, { ok: false, version: STEP, error: 'stream_ip_connect_rate', retry_after_seconds: 5 }, { 'retry-after': '5' });
    return true;
  }
  if (Number(clientsByIp.get(ip) || 0) >= CLIENTS_PER_IP_MAX) {
    stats.rejected_ip_capacity += 1;
    sendJson(res, 503, { ok: false, version: STEP, error: 'stream_ip_capacity', retry_after_seconds: 5 }, { 'retry-after': '5' });
    return true;
  }

  attempts.push(Date.now());
  connectAttemptsByIp.set(ip, attempts);

  res.writeHead(200, {
    'content-type': 'text/event-stream; charset=utf-8',
    'cache-control': 'no-store, no-transform',
    'connection': 'keep-alive',
    'access-control-allow-origin': '*',
    'x-kaka-stream-schema': SCHEMA,
  });
  res.write(`event: ready\ndata: ${JSON.stringify({
    ok: true,
    version: STEP,
    schema: SCHEMA,
    specs,
    user_read_upstream_requests: 0,
    user_read_exchange_connections_started: 0,
    reads_scale_with_users: false,
    fixed_background_focus_refresh: true,
  })}\n\n`);

  const entry = {
    id: ++clientSeq,
    ip,
    req,
    res,
    specs,
    lastFingerprint: '',
    closed: false,
    connectedAt: Date.now(),
  };
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
http.createServer = function step1060MarketStreamCreateServer(listener, ...rest) {
  return originalCreateServer(async (req, res) => {
    let url;
    try {
      url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
    } catch (_) {
      return listener(req, res);
    }

    if (url.pathname === HEALTH_ROUTE) {
      if (req.method === 'OPTIONS') {
        res.writeHead(204, {
          'access-control-allow-origin': '*',
          'access-control-allow-methods': 'GET, OPTIONS',
          'cache-control': 'no-store',
        });
        res.end();
        return;
      }
      if (req.method !== 'GET') {
        sendJson(res, 405, { ok: false, version: STEP, error: 'method_not_allowed' });
        return;
      }
      sendJson(res, 200, healthPayload());
      return;
    }

    if (url.pathname === STREAM_ROUTE) {
      handleStream(req, res, url);
      return;
    }

    return listener(req, res);
  }, ...rest);
};

export function getMarketLightDownstreamStreamHealth() {
  return healthPayload();
}
