import http from 'node:http';

// Step1061.9.2: 1000-user system-overlay connection guard.
//
// Goals:
// 1) Carrier-grade NAT safety: many legitimate phones may share one public IP.
//    The legacy stream still has a strict 50-active / 60-connects-per-minute IP
//    guard, so this layer fans one real carrier IP into 32 deterministic internal
//    buckets while enforcing a much larger real-IP ceiling here.
// 2) Render restart smoothing: admit reconnects at a bounded process-wide rate so
//    hundreds/thousands of clients do not all enter the overlay collector in one tick.
// 3) Idle egress reduction: the legacy stream emits a heartbeat every 15s. Forward
//    at most one every ~40s; the Android read timeout is 70s, so this remains safe.
//
// This module does not start exchange requests and does not change active market specs.

const VERSION = '1061.9.2';
const SCHEMA = 'step1061_9_2_overlay_capacity_nat_guard_v1';
const STREAM_ROUTE = '/api/overlay/watchlist-stream';
const HEALTH_ROUTE = '/api/overlay/capacity-guard-health';

const NAT_BUCKETS = Math.max(8, Math.min(64, Number(process.env.KAKA_OVERLAY_NAT_BUCKETS || 32)));
const REAL_IP_ACTIVE_MAX = Math.max(1000, Math.min(1500, Number(process.env.KAKA_OVERLAY_REAL_IP_ACTIVE_MAX || 1200)));
const REAL_IP_CONNECTS_PER_MINUTE = Math.max(
  1200,
  Number(process.env.KAKA_OVERLAY_REAL_IP_CONNECTS_PER_MINUTE || 3000),
);
const ADMISSION_RATE_PER_SECOND = Math.max(
  50,
  Math.min(500, Number(process.env.KAKA_OVERLAY_ADMISSION_RATE_PER_SECOND || 250)),
);
const ADMISSION_MAX_DELAY_MS = Math.max(
  1000,
  Math.min(8000, Number(process.env.KAKA_OVERLAY_ADMISSION_MAX_DELAY_MS || 6000)),
);
const HEARTBEAT_MIN_INTERVAL_MS = Math.max(
  25_000,
  Math.min(55_000, Number(process.env.KAKA_OVERLAY_HEARTBEAT_MIN_INTERVAL_MS || 40_000)),
);

const realIpActive = new Map();
const realIpAttempts = new Map();
const responseHeartbeatAt = new WeakMap();

let installed = false;
let connectionSeq = 0;
let nextAdmissionAt = 0;

const stats = {
  admitted: 0,
  admission_delayed: 0,
  admission_queue_rejected: 0,
  real_ip_capacity_rejected: 0,
  real_ip_rate_rejected: 0,
  heartbeat_forwarded: 0,
  heartbeat_suppressed: 0,
  max_admission_delay_ms: 0,
  last_error: '',
};

function text(value) {
  return String(value ?? '').trim();
}

function safePath(raw) {
  try { return new URL(String(raw || '/'), 'http://127.0.0.1').pathname || '/'; }
  catch (_) { return '/'; }
}

function realIp(req) {
  const forwarded = text(req?.headers?.['x-forwarded-for'])
    .split(',')
    .map((x) => x.trim())
    .find(Boolean);
  return forwarded || text(req?.socket?.remoteAddress) || 'unknown';
}

function pruneAttempts(ip, now = Date.now()) {
  const cutoff = now - 60_000;
  const attempts = realIpAttempts.get(ip) || [];
  while (attempts.length && attempts[0] < cutoff) attempts.shift();
  if (attempts.length) realIpAttempts.set(ip, attempts);
  else realIpAttempts.delete(ip);
  return attempts;
}

function pruneAllAttempts() {
  const now = Date.now();
  for (const ip of [...realIpAttempts.keys()]) pruneAttempts(ip, now);
}

function retryAfterSeconds() {
  return 3 + Math.floor(Math.random() * 10);
}

function sendRetry(res, status, error, extra = {}) {
  if (res.headersSent || res.writableEnded || res.destroyed) return;
  const retry = retryAfterSeconds();
  const payload = JSON.stringify({
    ok: false,
    version: VERSION,
    schema: SCHEMA,
    error,
    retry_after_seconds: retry,
    reads_scale_with_users: false,
    user_read_exchange_requests: 0,
    ...extra,
  });
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'retry-after': String(retry),
    'content-length': String(Buffer.byteLength(payload)),
  });
  res.end(payload);
}

async function admissionSlot() {
  const now = Date.now();
  const spacingMs = Math.max(1, Math.ceil(1000 / ADMISSION_RATE_PER_SECOND));
  const scheduledAt = Math.max(now, nextAdmissionAt);
  nextAdmissionAt = scheduledAt + spacingMs;
  const delayMs = Math.max(0, scheduledAt - now);
  stats.max_admission_delay_ms = Math.max(stats.max_admission_delay_ms, delayMs);
  if (delayMs > ADMISSION_MAX_DELAY_MS) return { ok: false, delayMs };
  if (delayMs > 0) {
    stats.admission_delayed += 1;
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }
  return { ok: true, delayMs };
}

function syntheticBucketIp(ip, seq) {
  const bucket = Math.abs(Number(seq) || 0) % NAT_BUCKETS;
  return `${ip}#kaka_nat_${bucket}`;
}

function isOverlayHeartbeat(chunk) {
  const value = Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk ?? '');
  return value.startsWith(': kaka-') && value.includes('-keepalive ');
}

function installHeartbeatThrottle(res) {
  if (!res || res.__kakaOverlayHeartbeatThrottleInstalled) return;
  res.__kakaOverlayHeartbeatThrottleInstalled = true;
  const originalWrite = res.write.bind(res);

  res.write = function kakaOverlayHeartbeatThrottleWrite(chunk, encoding, callback) {
    try {
      if (isOverlayHeartbeat(chunk)) {
        const now = Date.now();
        const last = Number(responseHeartbeatAt.get(res) || 0);
        if (last > 0 && now - last < HEARTBEAT_MIN_INTERVAL_MS) {
          stats.heartbeat_suppressed += 1;
          if (typeof callback === 'function') queueMicrotask(callback);
          return true;
        }
        responseHeartbeatAt.set(res, now);
        stats.heartbeat_forwarded += 1;
      }
    } catch (error) {
      stats.last_error = String(error?.message || error).slice(0, 300);
    }
    return originalWrite(chunk, encoding, callback);
  };
}

function releaseRealIp(ip, state) {
  if (!state || state.released) return;
  state.released = true;
  const next = Math.max(0, Number(realIpActive.get(ip) || 0) - 1);
  if (next) realIpActive.set(ip, next);
  else realIpActive.delete(ip);
}

function healthPayload() {
  pruneAllAttempts();
  return {
    ok: true,
    version: VERSION,
    schema: SCHEMA,
    scope: 'system_market_overlay_sse_only',
    client_target: 1000,
    nat_buckets: NAT_BUCKETS,
    real_ip_active_max: REAL_IP_ACTIVE_MAX,
    real_ip_connects_per_minute: REAL_IP_CONNECTS_PER_MINUTE,
    carrier_nat_safe: true,
    legacy_inner_ip_guard_sharded: true,
    global_stream_client_limit_remains_authoritative: true,
    admission_rate_per_second: ADMISSION_RATE_PER_SECOND,
    admission_max_delay_ms: ADMISSION_MAX_DELAY_MS,
    reconnect_storm_server_smoothing: true,
    heartbeat_min_interval_ms: HEARTBEAT_MIN_INTERVAL_MS,
    android_read_timeout_margin_ms: 70_000 - HEARTBEAT_MIN_INTERVAL_MS,
    heartbeat_idle_egress_reduction_vs_15s:
      Number((1 - 15_000 / HEARTBEAT_MIN_INTERVAL_MS).toFixed(4)),
    active_real_ips: realIpActive.size,
    active_connections_tracked: [...realIpActive.values()].reduce((a, b) => a + Number(b || 0), 0),
    attempts_real_ips: realIpAttempts.size,
    reads_scale_with_users: false,
    user_read_exchange_requests: 0,
    user_read_exchange_connections: 0,
    ...stats,
    now: new Date().toISOString(),
  };
}

function sendHealth(res) {
  const body = JSON.stringify(healthPayload());
  res.writeHead(200, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': String(Buffer.byteLength(body)),
  });
  res.end(body);
}

export function installOverlayCapacityNatGuard() {
  if (installed) return;
  installed = true;

  const originalCreateServer = http.createServer.bind(http);

  function patchedCreateServer(listener, ...rest) {
    const wrappedListener = async (req, res) => {
      const route = safePath(req.url);

      if (route === HEALTH_ROUTE) {
        if (String(req.method || 'GET').toUpperCase() !== 'GET') {
          res.writeHead(405, { 'cache-control': 'no-store' });
          res.end();
          return;
        }
        sendHealth(res);
        return;
      }

      if (route !== STREAM_ROUTE) {
        return await listener(req, res);
      }

      if (String(req.method || 'GET').toUpperCase() === 'OPTIONS') {
        return await listener(req, res);
      }

      const ip = realIp(req);
      const attempts = pruneAttempts(ip);
      if (attempts.length >= REAL_IP_CONNECTS_PER_MINUTE) {
        stats.real_ip_rate_rejected += 1;
        sendRetry(res, 429, 'overlay_real_ip_connect_rate', {
          carrier_nat_safe: true,
          real_ip_connects_per_minute: REAL_IP_CONNECTS_PER_MINUTE,
        });
        return;
      }
      attempts.push(Date.now());
      realIpAttempts.set(ip, attempts);

      const admission = await admissionSlot();
      if (req.destroyed || res.destroyed || res.writableEnded) return;
      if (!admission.ok) {
        stats.admission_queue_rejected += 1;
        sendRetry(res, 503, 'overlay_reconnect_admission_queue', {
          admission_max_delay_ms: ADMISSION_MAX_DELAY_MS,
        });
        return;
      }

      const active = Number(realIpActive.get(ip) || 0);
      if (active >= REAL_IP_ACTIVE_MAX) {
        stats.real_ip_capacity_rejected += 1;
        sendRetry(res, 503, 'overlay_real_ip_capacity', {
          carrier_nat_safe: true,
          real_ip_active_max: REAL_IP_ACTIVE_MAX,
        });
        return;
      }

      const seq = ++connectionSeq;
      const releaseState = { released: false };
      realIpActive.set(ip, active + 1);
      const release = () => releaseRealIp(ip, releaseState);
      req.once('aborted', release);
      res.once('close', release);
      res.once('error', release);

      req.headers['x-kaka-real-forwarded-for'] = text(req.headers['x-forwarded-for']);
      req.headers['x-forwarded-for'] = syntheticBucketIp(ip, seq);
      req.headers['x-kaka-overlay-admission-delay-ms'] = String(admission.delayMs);

      installHeartbeatThrottle(res);
      stats.admitted += 1;

      try {
        return await listener(req, res);
      } catch (error) {
        release();
        stats.last_error = String(error?.message || error).slice(0, 300);
        throw error;
      }
    };

    return originalCreateServer(wrappedListener, ...rest);
  }

  patchedCreateServer.__kakaOverlayCapacityNatWrapped = true;
  http.createServer = patchedCreateServer;

  console.log(
    `[Step1061.9.2] overlay 1000-user capacity/NAT/reconnect guard installed ` +
    `nat_buckets=${NAT_BUCKETS} real_ip_active_max=${REAL_IP_ACTIVE_MAX} ` +
    `admission_per_sec=${ADMISSION_RATE_PER_SECOND} heartbeat_min_ms=${HEARTBEAT_MIN_INTERVAL_MS}`,
  );
}

export function getOverlayCapacityNatGuardHealth() {
  return healthPayload();
}
