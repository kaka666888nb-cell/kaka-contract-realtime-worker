import http from 'node:http';
import { gzipSync } from 'node:zlib';
import { WebSocket } from 'ws';

const VERSION = '650.8.15.197.3.3.25.1';
const SCHEMA = 'step1060_render_egress_cost_guard_v1';
const STARTED_AT = Date.now();
const MAX_ROUTES = 192;
const MAX_HOSTS = 96;
const GZIP_MIN_BYTES = 512;

const httpRoutes = new Map();
const outboundHosts = new Map();
const outboundPaths = new Map();
const healthClients = new Map();
let httpRequests = 0;
let httpRawBytes = 0;
let httpSentBytes = 0;
let gzipResponses = 0;
let wsMessages = 0;
let wsPayloadBytes = 0;
let outboundRequests = 0;
let outboundKnownBodyBytes = 0;
let installed = false;

function safePath(raw) {
  try { return new URL(String(raw || '/'), 'http://127.0.0.1').pathname || '/'; }
  catch (_) { return '/'; }
}

function safeHost(raw) {
  try { return new URL(String(raw || '')).hostname.toLowerCase() || 'unknown'; }
  catch (_) { return 'unknown'; }
}

function safeOutboundPath(raw) {
  try {
    const u = new URL(String(raw || ''));
    return `${u.hostname.toLowerCase()}${u.pathname || '/'}`.slice(0, 240);
  } catch (_) {
    return 'unknown/';
  }
}

function bytesOf(value) {
  if (value == null) return 0;
  if (typeof value === 'string') return Buffer.byteLength(value);
  if (Buffer.isBuffer(value)) return value.length;
  if (value instanceof Uint8Array) return value.byteLength;
  if (value instanceof ArrayBuffer) return value.byteLength;
  if (value instanceof URLSearchParams) return Buffer.byteLength(value.toString());
  return 0;
}

function boundedRow(map, key, factory, max) {
  let row = map.get(key);
  if (!row) {
    row = factory();
    map.set(key, row);
  }
  while (map.size > max) {
    const first = map.keys().next().value;
    if (first == null || first === key) break;
    map.delete(first);
  }
  return row;
}

function recordHttp(path, status, rawBytes, sentBytes, gzip, clientKey = '') {
  const route = safePath(path);
  const raw = Math.max(0, Number(rawBytes) || 0);
  const sent = Math.max(0, Number(sentBytes) || 0);
  httpRequests += 1;
  httpRawBytes += raw;
  httpSentBytes += sent;
  if (gzip) gzipResponses += 1;
  const row = boundedRow(httpRoutes, route, () => ({
    path: route,
    requests: 0,
    raw_bytes: 0,
    sent_bytes: 0,
    gzip_responses: 0,
    status_2xx: 0,
    status_4xx: 0,
    status_5xx: 0,
  }), MAX_ROUTES);
  row.requests += 1;
  row.raw_bytes += raw;
  row.sent_bytes += sent;
  if (gzip) row.gzip_responses += 1;
  if (status >= 500) row.status_5xx += 1;
  else if (status >= 400) row.status_4xx += 1;
  else if (status >= 200) row.status_2xx += 1;

  if (route === '/health' && clientKey) {
    const client = boundedRow(healthClients, clientKey, () => ({
      client: clientKey,
      requests: 0,
      raw_bytes: 0,
      sent_bytes: 0,
      gzip_responses: 0,
    }), 32);
    client.requests += 1;
    client.raw_bytes += raw;
    client.sent_bytes += sent;
    if (gzip) client.gzip_responses += 1;
  }
}

function topBy(map, field, limit) {
  return [...map.values()]
    .sort((a, b) => Number(b[field] || 0) - Number(a[field] || 0))
    .slice(0, limit)
    .map((row) => ({ ...row }));
}

export function egressHealth() {
  return {
    ok: true,
    version: VERSION,
    schema: SCHEMA,
    started_at: new Date(STARTED_AT).toISOString(),
    uptime_seconds: Math.floor((Date.now() - STARTED_AT) / 1000),
    accounting_scope: 'process_lifetime_payload_estimate_excludes_tls_http2_and_ws_frame_overhead',
    http: {
      requests: httpRequests,
      raw_response_bytes: httpRawBytes,
      sent_response_bytes: httpSentBytes,
      gzip_responses: gzipResponses,
      gzip_saved_bytes: Math.max(0, httpRawBytes - httpSentBytes),
      compression_ratio: httpRawBytes > 0 ? Number((httpSentBytes / httpRawBytes).toFixed(4)) : null,
      top_routes_by_sent_bytes: topBy(httpRoutes, 'sent_bytes', 32),
      health_probe_clients: topBy(healthClients, 'sent_bytes', 32),
    },
    websocket: {
      downstream_messages: wsMessages,
      downstream_payload_bytes: wsPayloadBytes,
    },
    outbound_requests: {
      requests: outboundRequests,
      known_request_body_bytes: outboundKnownBodyBytes,
      note: 'known fetch request bodies sent from Render; upstream response bodies are inbound and excluded',
      top_hosts_by_known_request_body_bytes: topBy(outboundHosts, 'known_request_body_bytes', 24),
      top_paths_by_known_request_body_bytes: topBy(outboundPaths, 'known_request_body_bytes', 32),
    },
  };
}

function sendCompactJson(res, status, body) {
  const text = JSON.stringify(body);
  res.statusCode = status;
  res.setHeader('content-type', 'application/json; charset=utf-8');
  res.setHeader('cache-control', 'no-store');
  res.end(text);
}

function installFetchMeter() {
  if (typeof globalThis.fetch !== 'function' || globalThis.fetch.__kakaEgressWrapped) return;
  const original = globalThis.fetch.bind(globalThis);
  const wrapped = async function kakaEgressFetch(input, init = undefined) {
    let url = '';
    let method = 'GET';
    try {
      if (input instanceof Request) {
        url = input.url;
        method = input.method || method;
      } else {
        url = String(input || '');
      }
      if (init?.method) method = String(init.method).toUpperCase();
    } catch (_) {}
    const bodyBytes = bytesOf(init?.body);
    const host = safeHost(url);
    outboundRequests += 1;
    outboundKnownBodyBytes += bodyBytes;
    const row = boundedRow(outboundHosts, host, () => ({
      host,
      requests: 0,
      known_request_body_bytes: 0,
      methods: {},
    }), MAX_HOSTS);
    row.requests += 1;
    row.known_request_body_bytes += bodyBytes;
    row.methods[method] = Number(row.methods[method] || 0) + 1;

    const pathKey = safeOutboundPath(url);
    const pathRow = boundedRow(outboundPaths, pathKey, () => ({
      path: pathKey,
      requests: 0,
      known_request_body_bytes: 0,
      max_request_body_bytes: 0,
      methods: {},
    }), MAX_ROUTES);
    pathRow.requests += 1;
    pathRow.known_request_body_bytes += bodyBytes;
    pathRow.max_request_body_bytes = Math.max(pathRow.max_request_body_bytes, bodyBytes);
    pathRow.methods[method] = Number(pathRow.methods[method] || 0) + 1;
    return await original(input, init);
  };
  wrapped.__kakaEgressWrapped = true;
  globalThis.fetch = wrapped;
}

function installWebSocketMeter() {
  if (!WebSocket?.prototype?.send || WebSocket.prototype.send.__kakaEgressWrapped) return;
  const original = WebSocket.prototype.send;
  function meteredSend(data, ...rest) {
    try {
      wsMessages += 1;
      wsPayloadBytes += bytesOf(data);
    } catch (_) {}
    return original.call(this, data, ...rest);
  }
  meteredSend.__kakaEgressWrapped = true;
  WebSocket.prototype.send = meteredSend;
}

function mergeVary(current, token) {
  const values = String(current || '').split(',').map((v) => v.trim()).filter(Boolean);
  if (!values.some((v) => v.toLowerCase() === token.toLowerCase())) values.push(token);
  return values.join(', ');
}

function acceptsGzip(req) {
  return /(?:^|,)\s*gzip\s*(?:,|$)/i.test(String(req.headers?.['accept-encoding'] || ''));
}

function installResponseMeter(req, res) {
  const routePath = safePath(req.url);
  const gzipAccepted = acceptsGzip(req) && String(req.method || 'GET').toUpperCase() !== 'HEAD';
  const userAgent = String(req.headers?.['user-agent'] || 'unknown').replace(/\s+/g, ' ').slice(0, 120);
  const clientKey = `${userAgent}|ae=${gzipAccepted ? 'gzip' : 'none'}`;
  const originalWriteHead = res.writeHead.bind(res);
  const originalEnd = res.end.bind(res);
  const originalWrite = res.write.bind(res);
  let gzipEnabled = false;
  let statusCode = Number(res.statusCode || 200);
  let streamedBytes = 0;

  res.write = function kakaEgressWrite(chunk, encoding, callback) {
    streamedBytes += bytesOf(chunk);
    return originalWrite(chunk, encoding, callback);
  };

  res.writeHead = function kakaEgressWriteHead(status, statusMessageOrHeaders, maybeHeaders) {
    statusCode = Number(status || res.statusCode || 200);
    const headers = (statusMessageOrHeaders && typeof statusMessageOrHeaders === 'object' && !Array.isArray(statusMessageOrHeaders))
      ? { ...statusMessageOrHeaders }
      : (maybeHeaders && typeof maybeHeaders === 'object' ? { ...maybeHeaders } : {});
    const contentType = String(headers['content-type'] ?? headers['Content-Type'] ?? res.getHeader('content-type') ?? '').toLowerCase();
    const noBody = statusCode === 204 || statusCode === 304;
    const existingEncoding = String(headers['content-encoding'] ?? headers['Content-Encoding'] ?? res.getHeader('content-encoding') ?? '').trim();
    const endDecision = String(res.__kakaEndGzipDecision || '');
    gzipEnabled = endDecision !== 'identity' && streamedBytes === 0 && gzipAccepted && !noBody && contentType.includes('application/json') && !existingEncoding;
    if (gzipEnabled) {
      delete headers['content-length']; delete headers['Content-Length'];
      headers['content-encoding'] = 'gzip';
      headers['vary'] = mergeVary(headers['vary'] ?? headers['Vary'] ?? res.getHeader('vary'), 'Accept-Encoding');
      try { res.removeHeader('content-length'); } catch (_) {}
    }
    if (statusMessageOrHeaders && typeof statusMessageOrHeaders === 'string') {
      return originalWriteHead(status, statusMessageOrHeaders, headers);
    }
    return originalWriteHead(status, headers);
  };

  res.end = function kakaEgressEnd(chunk, encoding, callback) {
    let raw = Buffer.alloc(0);
    if (chunk != null) raw = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk), typeof encoding === 'string' ? encoding : 'utf8');
    let sent = raw;
    let usedGzip = gzipEnabled;

    if (!res.headersSent) {
      statusCode = Number(res.statusCode || 200);
      const contentType = String(res.getHeader('content-type') || '').toLowerCase();
      const existingEncoding = String(res.getHeader('content-encoding') || '').trim();
      usedGzip = streamedBytes === 0 && gzipAccepted && statusCode !== 204 && statusCode !== 304 && contentType.includes('application/json') && !existingEncoding && raw.length >= GZIP_MIN_BYTES;
      if (usedGzip) {
        res.removeHeader('content-length');
        res.setHeader('content-encoding', 'gzip');
        res.setHeader('vary', mergeVary(res.getHeader('vary'), 'Accept-Encoding'));
      }
    }

    if (usedGzip && raw.length > 0) {
      try {
        sent = gzipSync(raw, { level: 4 });
      } catch (_) {
        sent = raw;
        usedGzip = false;
        if (!res.headersSent) {
          try { res.removeHeader('content-encoding'); } catch (_) {}
        }
      }
    }
    if (!res.headersSent && streamedBytes === 0 && sent.length > 0) res.setHeader('content-length', String(sent.length));
    recordHttp(routePath, statusCode, streamedBytes + raw.length, streamedBytes + sent.length, usedGzip, clientKey);
    res.__kakaEndGzipDecision = usedGzip ? 'gzip' : 'identity';
    const finalCallback = typeof encoding === 'function' ? encoding : callback;
    return originalEnd(sent, undefined, finalCallback);
  };
}

function installHttpMeterAndCompression() {
  if (http.createServer.__kakaEgressWrapped) return;
  const originalCreateServer = http.createServer.bind(http);

  function patchedCreateServer(listener, ...rest) {
    const wrappedListener = async (req, res) => {
      installResponseMeter(req, res);
      const route = safePath(req.url);
      if (route === '/health/ready') {
        sendCompactJson(res, 200, {
          ok: true,
          ready: true,
          service: 'kaka-contract-realtime-worker',
          version: VERSION,
          schema: SCHEMA,
          uptime_seconds: Math.floor(process.uptime()),
        });
        return;
      }
      if (route === '/health/egress') {
        sendCompactJson(res, 200, egressHealth());
        return;
      }
      if (route === '/health' && new URL(req.url || '/', 'http://127.0.0.1').searchParams.get('compact') === '1') {
        sendCompactJson(res, 200, {
          ok: true,
          ready: true,
          service: 'kaka-contract-realtime-worker',
          version: VERSION,
          schema: SCHEMA,
          uptime_seconds: Math.floor(process.uptime()),
        });
        return;
      }
      return await listener(req, res);
    };
    return originalCreateServer(wrappedListener, ...rest);
  }
  patchedCreateServer.__kakaEgressWrapped = true;
  http.createServer = patchedCreateServer;
}

export function installRenderEgressCostGuard() {
  if (installed) return;
  installed = true;
  installFetchMeter();
  installWebSocketMeter();
  installHttpMeterAndCompression();
}

export const renderEgressVersion = VERSION;
export const renderEgressSchema = SCHEMA;
