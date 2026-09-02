import http from 'node:http';
import { gzipSync } from 'node:zlib';

const VERSION = 'step1060_3_render_supabase_egress_proxy_v1';
const MIN_PROXY_BYTES = Math.max(8 * 1024, Number(process.env.KAKA_RENDER_SUPABASE_PROXY_MIN_BYTES || 32 * 1024));
const MAX_COMPRESSED_BYTES = 4 * 1024 * 1024;
const ALLOWED_TABLES = new Set([
  'kaka_exchange_stock_catalog_v2_stage',
  'kaka_exchange_stock_catalog_v2_state',
  'kaka_project_fundamentals',
  'app_airdrop_events',
]);

const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
const CONFIGURED = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);

let installed = false;
let proxyTransportFetch = null;
const stats = {
  considered: 0,
  proxied: 0,
  proxy_failures: 0,
  raw_bytes_avoided: 0,
  compressed_bytes_sent: 0,
  last_target: '',
  last_error: '',
  last_success_at: '',
};

function bytesOf(value) {
  if (value == null) return null;
  if (typeof value === 'string') return Buffer.from(value);
  if (Buffer.isBuffer(value)) return value;
  if (value instanceof Uint8Array) return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
  if (value instanceof ArrayBuffer) return Buffer.from(value);
  if (value instanceof URLSearchParams) return Buffer.from(value.toString());
  return null;
}

function combinedHeaders(input, init) {
  const headers = new Headers(input instanceof Request ? input.headers : undefined);
  if (init?.headers) {
    const extra = new Headers(init.headers);
    extra.forEach((value, key) => headers.set(key, value));
  }
  return headers;
}

function requestMeta(input, init) {
  let url = '';
  let method = 'GET';
  let body = init?.body;
  if (input instanceof Request) {
    url = input.url;
    method = input.method || method;
    if (body == null) return null;
  } else {
    url = String(input || '');
  }
  if (init?.method) method = String(init.method);
  const raw = bytesOf(body);
  if (!raw) return null;
  return { url, method: method.toUpperCase(), raw, headers: combinedHeaders(input, init), signal: init?.signal };
}

function eligible(meta) {
  if (!CONFIGURED || !meta || meta.method !== 'POST' || meta.raw.length < MIN_PROXY_BYTES) return null;
  let url;
  let base;
  try {
    url = new URL(meta.url);
    base = new URL(SUPABASE_URL);
  } catch (_) {
    return null;
  }
  if (url.origin !== base.origin || !url.pathname.startsWith('/rest/v1/')) return null;
  const table = url.pathname.slice('/rest/v1/'.length);
  if (!ALLOWED_TABLES.has(table) || table.includes('/')) return null;
  return { url, table };
}

function syntheticFailure(status, error) {
  return new Response(JSON.stringify({
    ok: false,
    error,
    version: VERSION,
    preserve_last_verified_data: true,
    direct_uncompressed_fallback: false,
  }), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
      'x-kaka-render-egress-proxy': 'failed_closed',
      'x-kaka-render-egress-proxy-version': VERSION,
    },
  });
}

function proxyHealth() {
  const saved = Math.max(0, stats.raw_bytes_avoided - stats.compressed_bytes_sent);
  return {
    ok: true,
    version: VERSION,
    configured: CONFIGURED,
    min_proxy_bytes: MIN_PROXY_BYTES,
    fail_closed_for_large_background_writes: true,
    direct_uncompressed_fallback: false,
    allowed_tables: [...ALLOWED_TABLES],
    considered: stats.considered,
    proxied: stats.proxied,
    proxy_failures: stats.proxy_failures,
    raw_bytes_avoided: stats.raw_bytes_avoided,
    compressed_bytes_sent: stats.compressed_bytes_sent,
    estimated_saved_bytes: saved,
    compression_ratio: stats.raw_bytes_avoided > 0
      ? Number((stats.compressed_bytes_sent / stats.raw_bytes_avoided).toFixed(4))
      : null,
    last_target: stats.last_target || null,
    last_error: stats.last_error || null,
    last_success_at: stats.last_success_at || null,
  };
}

async function runEdgeProbe() {
  if (!CONFIGURED || typeof proxyTransportFetch !== 'function') {
    return { ok: false, error: 'proxy_transport_not_ready' };
  }
  const raw = Buffer.from(JSON.stringify({
    probe: 'kaka_render_egress_proxy',
    padding: 'A'.repeat(64 * 1024),
  }));
  const compressed = gzipSync(raw, { level: 6 });
  try {
    const response = await proxyTransportFetch(`${SUPABASE_URL}/functions/v1/kaka-render-egress-ingest`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        apikey: SUPABASE_SERVICE_ROLE_KEY,
        'content-type': 'application/octet-stream',
        'x-kaka-compression': 'gzip',
        'x-kaka-probe': '1',
        'x-kaka-raw-bytes': String(raw.length),
      },
      body: compressed,
    });
    let body = null;
    try { body = await response.json(); } catch (_) {}
    return {
      ok: response.ok && response.headers.get('x-kaka-render-egress-proxy') === 'ok' && body?.probe === true,
      status: response.status,
      raw_bytes: raw.length,
      compressed_bytes: compressed.length,
      edge_version: body?.version || null,
      error: body?.error || null,
    };
  } catch (error) {
    return { ok: false, error: String(error?.message || error).slice(0, 200) };
  }
}

function installProxyHealthRoute() {
  if (http.createServer.__kakaSupabaseEgressProxyWrapped) return;
  const previousCreateServer = http.createServer.bind(http);
  function patchedCreateServer(listener, ...rest) {
    const wrapped = async (req, res) => {
      let pathname = '/';
      try { pathname = new URL(req.url || '/', 'http://127.0.0.1').pathname; } catch (_) {}
      if (pathname === '/health/egress/proxy') {
        let probe = null;
        try {
          const url = new URL(req.url || '/', 'http://127.0.0.1');
          if (url.searchParams.get('probe') === '1') probe = await runEdgeProbe();
        } catch (_) {}
        const body = Buffer.from(JSON.stringify({ ...proxyHealth(), probe }));
        res.statusCode = probe && probe.ok !== true ? 503 : 200;
        res.setHeader('content-type', 'application/json; charset=utf-8');
        res.setHeader('cache-control', 'no-store');
        res.setHeader('content-length', String(body.length));
        res.end(body);
        return;
      }
      return await listener(req, res);
    };
    return previousCreateServer(wrapped, ...rest);
  }
  patchedCreateServer.__kakaSupabaseEgressProxyWrapped = true;
  http.createServer = patchedCreateServer;
}

export function installRenderSupabaseEgressProxy() {
  if (installed) return;
  installed = true;
  installProxyHealthRoute();

  if (typeof globalThis.fetch !== 'function' || globalThis.fetch.__kakaSupabaseEgressProxyWrapped) return;
  const originalFetch = globalThis.fetch.bind(globalThis);
  proxyTransportFetch = originalFetch;

  const wrapped = async function kakaSupabaseEgressProxyFetch(input, init = undefined) {
    const meta = requestMeta(input, init);
    const target = eligible(meta);
    if (!target) return originalFetch(input, init);

    stats.considered += 1;
    stats.last_target = `${target.table}${target.url.search}`;
    stats.last_error = '';

    let compressed;
    try {
      compressed = gzipSync(meta.raw, { level: 6 });
    } catch (error) {
      stats.proxy_failures += 1;
      stats.last_error = `gzip_failed:${String(error?.message || error).slice(0, 180)}`;
      return syntheticFailure(503, 'render_egress_gzip_failed');
    }

    if (!compressed.length || compressed.length > MAX_COMPRESSED_BYTES) {
      stats.proxy_failures += 1;
      stats.last_error = `compressed_size_invalid:${compressed.length}`;
      return syntheticFailure(503, 'render_egress_compressed_payload_too_large');
    }

    const edgeUrl = `${SUPABASE_URL}/functions/v1/kaka-render-egress-ingest`;
    const proxyHeaders = new Headers({
      authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      'content-type': 'application/octet-stream',
      'x-kaka-compression': 'gzip',
      'x-kaka-target': encodeURIComponent(`${target.url.pathname}${target.url.search}`),
      'x-kaka-original-method': meta.method,
      'x-kaka-original-content-type': meta.headers.get('content-type') || 'application/json',
      'x-kaka-raw-bytes': String(meta.raw.length),
    });
    for (const [source, dest] of [
      ['prefer', 'x-kaka-original-prefer'],
      ['content-profile', 'x-kaka-original-content-profile'],
      ['accept-profile', 'x-kaka-original-accept-profile'],
    ]) {
      const value = meta.headers.get(source);
      if (value) proxyHeaders.set(dest, value);
    }

    let response;
    try {
      response = await originalFetch(edgeUrl, {
        method: 'POST',
        headers: proxyHeaders,
        body: compressed,
        signal: meta.signal,
      });
    } catch (error) {
      stats.proxy_failures += 1;
      stats.last_error = `proxy_network_failed:${String(error?.message || error).slice(0, 180)}`;
      return syntheticFailure(503, 'render_egress_proxy_network_failed');
    }

    const marker = response.headers.get('x-kaka-render-egress-proxy');
    if (marker !== 'ok') {
      stats.proxy_failures += 1;
      stats.last_error = `proxy_marker_missing:http_${response.status}`;
      return syntheticFailure(503, 'render_egress_proxy_unverified_response');
    }

    stats.proxied += 1;
    stats.raw_bytes_avoided += meta.raw.length;
    stats.compressed_bytes_sent += compressed.length;
    stats.last_success_at = new Date().toISOString();
    if (!response.ok) stats.last_error = `upstream_http_${response.status}`;
    return response;
  };

  wrapped.__kakaSupabaseEgressProxyWrapped = true;
  globalThis.fetch = wrapped;
  globalThis.__kakaRenderSupabaseEgressProxyHealth = proxyHealth;
}

export function getRenderSupabaseEgressProxyHealth() {
  return proxyHealth();
}
