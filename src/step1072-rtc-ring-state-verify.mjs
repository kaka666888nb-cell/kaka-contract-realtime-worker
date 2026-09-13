import http from 'node:http';

const STEP = '1072.7.6.1';
const VERIFY_CONTRACT = 'step1072_7_6_1_rtc_ring_state_v1';
const ROUTE_RE = /^\/api\/rtc\/calls\/([0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\/ring-state$/i;

const text = (value) => String(value ?? '').trim();
const intEnv = (name, fallback, min, max) => {
  const n = Number(process.env[name]);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.trunc(n)));
};

const RING_TIMEOUT_SECONDS = intEnv('KAKA_RTC_RING_TIMEOUT_SECONDS', 45, 15, 120);
const SUPABASE_URL = text(process.env.SUPABASE_URL).replace(/\/$/, '');
const SUPABASE_SERVICE_ROLE_KEY = text(process.env.SUPABASE_SERVICE_ROLE_KEY);
const NEGATIVE_CACHE_MS = 15_000;
const CLOCK_SLOP_MS = 5_000;
const negativeCache = new Map();

function json(res, status, payload) {
  if (res.headersSent || res.writableEnded) return;
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store, max-age=0',
    'content-length': Buffer.byteLength(body),
  });
  res.end(body);
}

async function supabaseGet(path) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error('supabase_server_env_missing');
  }
  const response = await fetch(`${SUPABASE_URL}${path}`, {
    method: 'GET',
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      accept: 'application/json',
    },
  });
  const raw = await response.text();
  let payload = null;
  try { payload = raw ? JSON.parse(raw) : null; } catch (_) { payload = null; }
  if (!response.ok) throw new Error(`supabase_http_${response.status}`);
  return payload;
}

function timestampMs(value) {
  const ms = Date.parse(text(value));
  return Number.isFinite(ms) ? ms : 0;
}

function rowIsFreshRinging(row) {
  if (!row || typeof row !== 'object') return false;
  if (text(row.status).toLowerCase() !== 'ringing') return false;
  if (text(row.ended_at)) return false;

  const startedAt = timestampMs(row.ringing_at || row.created_at);
  if (!startedAt) return false;

  const ageMs = Date.now() - startedAt;
  return ageMs >= -CLOCK_SLOP_MS &&
    ageMs <= (RING_TIMEOUT_SECONDS * 1000) + CLOCK_SLOP_MS;
}

async function readRingState(callId) {
  const now = Date.now();
  const cachedUntil = Number(negativeCache.get(callId) || 0);
  if (cachedUntil > now) return false;
  if (cachedUntil) negativeCache.delete(callId);

  const rows = await supabaseGet(
    `/rest/v1/app_rtc_calls?id=eq.${encodeURIComponent(callId)}` +
      '&select=status,created_at,ringing_at,ended_at&limit=1',
  );
  const row = Array.isArray(rows) ? (rows[0] ?? null) : null;
  const ringing = rowIsFreshRinging(row);

  // Never cache true: a caller may hang up immediately after this read.
  if (!ringing) {
    negativeCache.set(callId, now + NEGATIVE_CACHE_MS);
    if (negativeCache.size > 512) {
      for (const [key, until] of negativeCache) {
        if (until <= now) negativeCache.delete(key);
        if (negativeCache.size <= 384) break;
      }
    }
  }
  return ringing;
}

async function handleRingState(_req, res, callId) {
  try {
    const ringing = await readRingState(callId);
    return json(res, 200, {
      ok: true,
      verify_contract: VERIFY_CONTRACT,
      ringing,
    });
  } catch (error) {
    console.warn(`[Step${STEP}] ring-state verify failed error=${text(error?.message) || 'unknown'}`);
    return json(res, 503, {
      ok: false,
      verify_contract: VERIFY_CONTRACT,
      ringing: false,
      error: 'rtc_ring_state_unavailable',
    });
  }
}

export function installRtcRingStateVerify() {
  if (http.__kakaStep1072RtcRingStateVerifyInstalled) return;

  const originalCreateServer = http.createServer.bind(http);
  http.createServer = function kakaRtcRingStateCreateServer(...args) {
    const listenerIndex = args.findIndex((value) => typeof value === 'function');
    if (listenerIndex < 0) return originalCreateServer(...args);

    const originalListener = args[listenerIndex];
    args[listenerIndex] = function kakaRtcRingStateListener(req, res) {
      let url;
      try {
        url = new URL(req.url || '/', 'http://127.0.0.1');
      } catch (_) {
        return originalListener(req, res);
      }

      const match = url.pathname.match(ROUTE_RE);
      if (text(req.method).toUpperCase() === 'GET' && match) {
        void handleRingState(req, res, match[1]);
        return;
      }
      return originalListener(req, res);
    };

    return originalCreateServer(...args);
  };

  http.__kakaStep1072RtcRingStateVerifyInstalled = true;
  console.log(
    `[Step${STEP}] RTC stale-ringing server verify installed ` +
      `contract=${VERIFY_CONTRACT} pii=false idle_polling=false`,
  );
}
