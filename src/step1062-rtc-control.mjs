import http from 'node:http';
import crypto from 'node:crypto';
import zlib from 'node:zlib';

const STEP = '1062.1';
const SCHEMA = 'step1062_1_rtc_secure_control_plane_v1';
const ROUTE_PREFIX = '/api/rtc';

const text = (value) => String(value ?? '').trim();
const intEnv = (name, fallback, min, max) => {
  const n = Number(process.env[name]);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.trunc(n)));
};
const boolEnv = (name, fallback = false) => {
  const raw = text(process.env[name]).toLowerCase();
  if (!raw) return fallback;
  return ['1', 'true', 'yes', 'on'].includes(raw);
};

const RTC_ENABLED = boolEnv('KAKA_RTC_ENABLED', false);
const SDK_APP_ID = intEnv('KAKA_TRTC_SDK_APP_ID', 0, 0, 2_147_483_647);
const SECRET_KEY = text(process.env.KAKA_TRTC_SECRET_KEY);
const USER_SIG_EXPIRE_SECONDS = intEnv('KAKA_RTC_USERSIG_EXPIRE_SECONDS', 7200, 600, 86400);
const RING_TIMEOUT_SECONDS = intEnv('KAKA_RTC_RING_TIMEOUT_SECONDS', 45, 15, 120);
const VOICE_SINGLE_MAX_SECONDS = intEnv('KAKA_RTC_VOICE_SINGLE_MAX_SECONDS', 3600, 60, 7200);
const VIDEO_SINGLE_MAX_SECONDS = intEnv('KAKA_RTC_VIDEO_SINGLE_MAX_SECONDS', 1800, 60, 3600);
const VOICE_DAILY_MAX_SECONDS = intEnv('KAKA_RTC_VOICE_DAILY_MAX_SECONDS', 7200, 60, 86400);
const VIDEO_DAILY_MAX_SECONDS = intEnv('KAKA_RTC_VIDEO_DAILY_MAX_SECONDS', 3600, 60, 43200);
const MONTHLY_WARNING_WEIGHTED_MINUTES = intEnv('KAKA_RTC_MONTHLY_WARNING_WEIGHTED_MINUTES', 7000, 0, 100000000);
const MONTHLY_VIDEO_DISABLE_WEIGHTED_MINUTES = intEnv('KAKA_RTC_MONTHLY_VIDEO_DISABLE_WEIGHTED_MINUTES', 8500, 0, 100000000);
const MONTHLY_STOP_WEIGHTED_MINUTES = intEnv('KAKA_RTC_MONTHLY_STOP_WEIGHTED_MINUTES', 9500, 1, 100000000);
const SUPABASE_URL = text(process.env.SUPABASE_URL).replace(/\/$/, '');
const SUPABASE_SERVICE_ROLE_KEY = text(process.env.SUPABASE_SERVICE_ROLE_KEY);

const stats = {
  auth_ok: 0,
  auth_failed: 0,
  create_ok: 0,
  create_rejected: 0,
  accept_ok: 0,
  reject_ok: 0,
  end_ok: 0,
  status_reads: 0,
  db_errors: 0,
  last_error: '',
};

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

function fail(res, status, code, message = '') {
  return json(res, status, {
    ok: false,
    step: STEP,
    schema: SCHEMA,
    error: code,
    ...(message ? { message } : {}),
  });
}

async function readJsonBody(req, maxBytes = 16 * 1024) {
  const chunks = [];
  let bytes = 0;
  for await (const chunk of req) {
    bytes += chunk.length;
    if (bytes > maxBytes) throw new Error('request_body_too_large');
    chunks.push(chunk);
  }
  if (!chunks.length) return {};
  const raw = Buffer.concat(chunks).toString('utf8');
  if (!raw.trim()) return {};
  const parsed = JSON.parse(raw);
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error('invalid_json_body');
  return parsed;
}

function configReady() {
  return RTC_ENABLED && SDK_APP_ID > 0 && SECRET_KEY.length >= 16 && SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY;
}

function makeRtcUserId(userId) {
  // TRTC userId <= 32 bytes. Supabase UUID is 36 chars, so never pass it directly.
  const hex = crypto.createHash('sha256').update(text(userId)).digest('hex');
  return `k_${hex.slice(0, 30)}`; // exactly 32 ASCII bytes
}

function makeUserSig(userId, expireSeconds = USER_SIG_EXPIRE_SECONDS) {
  if (!SDK_APP_ID || !SECRET_KEY) throw new Error('trtc_secret_not_configured');
  const currTime = Math.floor(Date.now() / 1000);
  const identifier = text(userId);
  const content =
    `TLS.identifier:${identifier}\n` +
    `TLS.sdkappid:${SDK_APP_ID}\n` +
    `TLS.time:${currTime}\n` +
    `TLS.expire:${expireSeconds}\n`;
  const sig = crypto.createHmac('sha256', SECRET_KEY).update(content).digest('base64');
  const doc = {
    'TLS.ver': '2.0',
    'TLS.identifier': identifier,
    'TLS.sdkappid': SDK_APP_ID,
    'TLS.expire': expireSeconds,
    'TLS.time': currTime,
    'TLS.sig': sig,
  };
  return zlib.deflateSync(Buffer.from(JSON.stringify(doc), 'utf8'))
    .toString('base64')
    .replace(/\+/g, '*')
    .replace(/\//g, '-')
    .replace(/=/g, '_');
}

async function supabaseFetch(path, { method = 'GET', body = null, userAuthorization = '' } = {}) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) throw new Error('supabase_server_env_missing');
  const headers = {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    authorization: userAuthorization || `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    accept: 'application/json',
  };
  if (body != null) headers['content-type'] = 'application/json';
  const response = await fetch(`${SUPABASE_URL}${path}`, {
    method,
    headers,
    body: body == null ? undefined : JSON.stringify(body),
  });
  const raw = await response.text();
  let payload = null;
  try { payload = raw ? JSON.parse(raw) : null; } catch (_) { payload = raw; }
  if (!response.ok) {
    const err = new Error(`supabase_http_${response.status}`);
    err.status = response.status;
    err.payload = payload;
    throw err;
  }
  return payload;
}

async function authenticatedUser(req) {
  const auth = text(req.headers.authorization);
  if (!/^Bearer\s+\S+/i.test(auth)) {
    stats.auth_failed += 1;
    throw Object.assign(new Error('auth_required'), { httpStatus: 401 });
  }
  try {
    const payload = await supabaseFetch('/auth/v1/user', { userAuthorization: auth });
    const id = text(payload?.id);
    if (!id) throw new Error('auth_user_missing');
    stats.auth_ok += 1;
    return { id, email: text(payload?.email) };
  } catch (error) {
    stats.auth_failed += 1;
    throw Object.assign(new Error('auth_invalid'), { httpStatus: 401, cause: error });
  }
}

function rpcErrorCode(error) {
  const msg = text(error?.payload?.message || error?.payload?.error_description || error?.payload?.hint || error?.message);
  const known = [
    'rtc_disabled', 'rtc_invalid_peer', 'rtc_user_deleted', 'rtc_user_banned',
    'rtc_users_blocked', 'rtc_peer_no_private_messages', 'rtc_user_busy',
    'rtc_daily_voice_limit', 'rtc_daily_video_limit', 'rtc_monthly_stop',
    'rtc_video_monthly_stop', 'rtc_call_not_found', 'rtc_call_not_ringing',
    'rtc_not_callee', 'rtc_not_participant',
  ];
  return known.find((code) => msg.includes(code)) || 'rtc_database_rejected';
}

async function dbRpc(name, params) {
  try {
    const payload = await supabaseFetch(`/rest/v1/rpc/${encodeURIComponent(name)}`, {
      method: 'POST',
      body: params,
    });
    if (Array.isArray(payload)) return payload[0] ?? null;
    return payload;
  } catch (error) {
    stats.db_errors += 1;
    stats.last_error = `${name}:${rpcErrorCode(error)}`;
    throw error;
  }
}

function publicCall(row) {
  if (!row || typeof row !== 'object') return null;
  return {
    id: text(row.id),
    room_id: text(row.room_id),
    caller_user_id: text(row.caller_user_id),
    callee_user_id: text(row.callee_user_id),
    caller_rtc_user_id: text(row.caller_rtc_user_id),
    callee_rtc_user_id: text(row.callee_rtc_user_id),
    call_type: text(row.call_type),
    status: text(row.status),
    created_at: row.created_at ?? null,
    ringing_at: row.ringing_at ?? null,
    accepted_at: row.accepted_at ?? null,
    answered_at: row.answered_at ?? null,
    ended_at: row.ended_at ?? null,
    duration_seconds: Number(row.duration_seconds || 0),
    weighted_participant_minutes: Number(row.weighted_participant_minutes || 0),
  };
}

function credentialFor(row, authUserId) {
  const caller = text(row.caller_user_id) === authUserId;
  const rtcUserId = caller ? text(row.caller_rtc_user_id) : text(row.callee_rtc_user_id);
  const peerRtcUserId = caller ? text(row.callee_rtc_user_id) : text(row.caller_rtc_user_id);
  if (!rtcUserId || !peerRtcUserId) throw new Error('rtc_identity_missing');
  return {
    sdk_app_id: SDK_APP_ID,
    rtc_user_id: rtcUserId,
    peer_rtc_user_id: peerRtcUserId,
    user_sig: makeUserSig(rtcUserId),
    expires_in_seconds: USER_SIG_EXPIRE_SECONDS,
  };
}

async function dbSchemaReady() {
  try {
    await supabaseFetch('/rest/v1/app_rtc_calls?select=id&limit=1');
    return true;
  } catch (_) {
    return false;
  }
}

async function handleHealth(_req, res) {
  const dbReady = await dbSchemaReady();
  return json(res, 200, {
    ok: true,
    step: STEP,
    schema: SCHEMA,
    enabled: RTC_ENABLED,
    configured: configReady(),
    sdk_app_id_configured: SDK_APP_ID > 0,
    secret_key_configured: SECRET_KEY.length >= 16,
    supabase_server_configured: Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY),
    db_schema_ready: dbReady,
    media_proxy_on_render: false,
    server_generated_usersig: true,
    raw_supabase_uuid_used_as_rtc_user_id: false,
    limits: {
      ring_timeout_seconds: RING_TIMEOUT_SECONDS,
      voice_single_max_seconds: VOICE_SINGLE_MAX_SECONDS,
      video_single_max_seconds: VIDEO_SINGLE_MAX_SECONDS,
      voice_daily_max_seconds: VOICE_DAILY_MAX_SECONDS,
      video_daily_max_seconds: VIDEO_DAILY_MAX_SECONDS,
      monthly_warning_weighted_minutes: MONTHLY_WARNING_WEIGHTED_MINUTES,
      monthly_video_disable_weighted_minutes: MONTHLY_VIDEO_DISABLE_WEIGHTED_MINUTES,
      monthly_stop_weighted_minutes: MONTHLY_STOP_WEIGHTED_MINUTES,
      video_weight: 4,
      voice_weight: 1,
    },
    stats: { ...stats },
  });
}

async function handleCreate(req, res) {
  if (!configReady()) return fail(res, 503, 'rtc_not_configured');
  const user = await authenticatedUser(req);
  const body = await readJsonBody(req);
  const peerUserId = text(body.peer_user_id);
  const callType = text(body.call_type).toLowerCase();
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(peerUserId)) {
    stats.create_rejected += 1;
    return fail(res, 400, 'rtc_invalid_peer');
  }
  if (!['voice', 'video'].includes(callType)) {
    stats.create_rejected += 1;
    return fail(res, 400, 'rtc_invalid_call_type');
  }
  try {
    const row = await dbRpc('app_rtc_server_create_call', {
      p_caller_user_id: user.id,
      p_callee_user_id: peerUserId,
      p_call_type: callType,
      p_rtc_enabled: RTC_ENABLED,
      p_ring_timeout_seconds: RING_TIMEOUT_SECONDS,
      p_voice_daily_limit_seconds: VOICE_DAILY_MAX_SECONDS,
      p_video_daily_limit_seconds: VIDEO_DAILY_MAX_SECONDS,
      p_monthly_video_disable_weighted_minutes: MONTHLY_VIDEO_DISABLE_WEIGHTED_MINUTES,
      p_monthly_stop_weighted_minutes: MONTHLY_STOP_WEIGHTED_MINUTES,
    });
    if (!row) throw new Error('rtc_create_empty');
    stats.create_ok += 1;
    return json(res, 201, {
      ok: true,
      step: STEP,
      schema: SCHEMA,
      call: publicCall(row),
      credential: credentialFor(row, user.id),
      single_call_max_seconds: callType === 'video' ? VIDEO_SINGLE_MAX_SECONDS : VOICE_SINGLE_MAX_SECONDS,
    });
  } catch (error) {
    stats.create_rejected += 1;
    return fail(res, 409, rpcErrorCode(error));
  }
}

async function handleAccept(req, res, callId) {
  if (!configReady()) return fail(res, 503, 'rtc_not_configured');
  const user = await authenticatedUser(req);
  try {
    const row = await dbRpc('app_rtc_server_accept_call', {
      p_call_id: callId,
      p_callee_user_id: user.id,
      p_ring_timeout_seconds: RING_TIMEOUT_SECONDS,
    });
    if (!row) throw new Error('rtc_accept_empty');
    stats.accept_ok += 1;
    return json(res, 200, {
      ok: true,
      step: STEP,
      schema: SCHEMA,
      call: publicCall(row),
      credential: credentialFor(row, user.id),
      single_call_max_seconds: text(row.call_type) === 'video' ? VIDEO_SINGLE_MAX_SECONDS : VOICE_SINGLE_MAX_SECONDS,
    });
  } catch (error) {
    return fail(res, 409, rpcErrorCode(error));
  }
}

async function handleReject(req, res, callId) {
  const user = await authenticatedUser(req);
  try {
    const row = await dbRpc('app_rtc_server_reject_call', { p_call_id: callId, p_callee_user_id: user.id });
    stats.reject_ok += 1;
    return json(res, 200, { ok: true, step: STEP, schema: SCHEMA, call: publicCall(row) });
  } catch (error) {
    return fail(res, 409, rpcErrorCode(error));
  }
}

async function handleEnd(req, res, callId) {
  const user = await authenticatedUser(req);
  try {
    const row = await dbRpc('app_rtc_server_end_call', {
      p_call_id: callId,
      p_actor_user_id: user.id,
      p_voice_single_max_seconds: VOICE_SINGLE_MAX_SECONDS,
      p_video_single_max_seconds: VIDEO_SINGLE_MAX_SECONDS,
    });
    stats.end_ok += 1;
    return json(res, 200, { ok: true, step: STEP, schema: SCHEMA, call: publicCall(row) });
  } catch (error) {
    return fail(res, 409, rpcErrorCode(error));
  }
}

async function handleStatus(req, res, callId) {
  const user = await authenticatedUser(req);
  try {
    const row = await dbRpc('app_rtc_server_get_call', { p_call_id: callId, p_actor_user_id: user.id });
    stats.status_reads += 1;
    return json(res, 200, { ok: true, step: STEP, schema: SCHEMA, call: publicCall(row) });
  } catch (error) {
    return fail(res, 404, rpcErrorCode(error));
  }
}

async function handleIncoming(req, res) {
  const user = await authenticatedUser(req);
  try {
    const row = await dbRpc('app_rtc_server_get_incoming_call', {
      p_callee_user_id: user.id,
      p_ring_timeout_seconds: RING_TIMEOUT_SECONDS,
    });
    stats.status_reads += 1;
    return json(res, 200, { ok: true, step: STEP, schema: SCHEMA, call: publicCall(row) });
  } catch (error) {
    return fail(res, 500, rpcErrorCode(error));
  }
}

async function routeRtc(req, res, url) {
  const method = text(req.method).toUpperCase();
  const pathname = url.pathname;
  if (method === 'GET' && pathname === `${ROUTE_PREFIX}/health`) return handleHealth(req, res);
  if (method === 'POST' && pathname === `${ROUTE_PREFIX}/calls`) return handleCreate(req, res);
  if (method === 'GET' && pathname === `${ROUTE_PREFIX}/incoming`) return handleIncoming(req, res);
  const match = pathname.match(/^\/api\/rtc\/calls\/([0-9a-f-]{36})(?:\/(accept|reject|end))?$/i);
  if (match) {
    const callId = match[1];
    const action = match[2] || '';
    if (method === 'GET' && !action) return handleStatus(req, res, callId);
    if (method === 'POST' && action === 'accept') return handleAccept(req, res, callId);
    if (method === 'POST' && action === 'reject') return handleReject(req, res, callId);
    if (method === 'POST' && action === 'end') return handleEnd(req, res, callId);
  }
  return fail(res, 404, 'rtc_route_not_found');
}

export function installRtcControlPlane() {
  if (http.__kakaStep1062RtcControlInstalled) return;
  const originalCreateServer = http.createServer.bind(http);
  http.createServer = function patchedCreateServer(...args) {
    const listenerIndex = args.findIndex((value) => typeof value === 'function');
    if (listenerIndex < 0) return originalCreateServer(...args);
    const originalListener = args[listenerIndex];
    args[listenerIndex] = function rtcWrappedListener(req, res) {
      let url;
      try { url = new URL(req.url || '/', 'http://127.0.0.1'); } catch (_) { return originalListener(req, res); }
      if (!url.pathname.startsWith(`${ROUTE_PREFIX}/`) && url.pathname !== ROUTE_PREFIX) {
        return originalListener(req, res);
      }
      Promise.resolve(routeRtc(req, res, url)).catch((error) => {
        stats.last_error = text(error?.message || error);
        const status = Number(error?.httpStatus || 500);
        fail(res, status, status === 401 ? text(error.message) : 'rtc_internal_error');
      });
    };
    return originalCreateServer(...args);
  };
  http.__kakaStep1062RtcControlInstalled = true;
  console.log(`[Step${STEP}] RTC secure control plane installed enabled=${RTC_ENABLED} sdk_app_id_configured=${SDK_APP_ID > 0} secret_configured=${SECRET_KEY.length >= 16} media_proxy_on_render=false`);
}
