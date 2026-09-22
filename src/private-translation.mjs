import crypto from 'node:crypto';
import {
  getSharedTranslationHealth,
  translatePrivateText,
} from './content-translation.mjs';

const VERSION = '1073.r48.private-translation.1';
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(
  process.env.SUPABASE_SERVICE_ROLE_KEY || '',
).trim();

const MAX_BODY_BYTES = 6 * 1024;
const MAX_TEXT_BYTES = 3000;
const PER_IP_WINDOW_MS = 60_000;
const PER_IP_MAX_REQUESTS = 12;
const AUTH_TIMEOUT_MS = 8_000;

const ipWindows = new Map();
const inFlight = new Map();

const state = {
  version: VERSION,
  enabled: true,
  auth_required: true,
  post_body_only: true,
  private_text_persisted: false,
  private_text_logged: false,
  private_text_in_url: false,
  source_text_max_utf8_bytes: MAX_TEXT_BYTES,
  per_ip_per_minute: PER_IP_MAX_REQUESTS,
  per_user_daily_characters: 3000,
  per_user_daily_requests: 20,
  atomic_budget_claim: true,
  requests: 0,
  auth_checks: 0,
  auth_failures: 0,
  validation_rejects: 0,
  rate_limited: 0,
  budget_rejects: 0,
  provider_failures: 0,
  successes: 0,
  inflight_hits: 0,
  last_success_at: null,
  last_error: null,
};

function text(value) {
  return String(value ?? '').trim();
}

function json(res, status, body) {
  const raw = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': String(Buffer.byteLength(raw)),
    'cache-control': 'no-store, max-age=0',
    pragma: 'no-cache',
  });
  res.end(raw);
}

function ipKey(req) {
  const forwarded = text(req.headers?.['x-forwarded-for'])
    .split(',')[0]
    .trim();
  return forwarded || text(req.socket?.remoteAddress) || 'unknown';
}

function allowIp(req) {
  const key = ipKey(req);
  const now = Date.now();
  const row = ipWindows.get(key);
  if (!row || now - row.startedAt >= PER_IP_WINDOW_MS) {
    ipWindows.set(key, { startedAt: now, count: 1 });
  } else {
    if (row.count >= PER_IP_MAX_REQUESTS) return false;
    row.count += 1;
  }

  if (ipWindows.size > 2000) {
    for (const [candidate, value] of ipWindows.entries()) {
      if (now - value.startedAt > PER_IP_WINDOW_MS * 2) {
        ipWindows.delete(candidate);
      }
    }
  }
  return true;
}

async function readJsonBody(req) {
  return await new Promise((resolve, reject) => {
    let raw = '';
    let bytes = 0;
    req.setEncoding('utf8');
    req.on('data', (chunk) => {
      bytes += Buffer.byteLength(chunk, 'utf8');
      if (bytes > MAX_BODY_BYTES) {
        reject(new Error('request_too_large'));
        req.destroy();
        return;
      }
      raw += chunk;
    });
    req.on('end', () => {
      try {
        resolve(raw ? JSON.parse(raw) : {});
      } catch (_) {
        reject(new Error('invalid_json'));
      }
    });
    req.on('error', reject);
  });
}

async function verifyUser(req) {
  state.auth_checks += 1;
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    state.auth_failures += 1;
    return null;
  }

  const authorization = text(req.headers?.authorization);
  if (!/^Bearer\s+\S+$/i.test(authorization)) {
    state.auth_failures += 1;
    return null;
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), AUTH_TIMEOUT_MS);
  try {
    const response = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
      method: 'GET',
      headers: {
        apikey: SUPABASE_SERVICE_ROLE_KEY,
        authorization,
        accept: 'application/json',
      },
      signal: controller.signal,
    });
    if (!response.ok) {
      state.auth_failures += 1;
      return null;
    }
    const user = await response.json();
    const id = text(user?.id);
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(id)) {
      state.auth_failures += 1;
      return null;
    }
    return { id };
  } catch (_) {
    state.auth_failures += 1;
    return null;
  } finally {
    clearTimeout(timer);
  }
}

function statusForError(error) {
  const code = text(error?.code);
  const message = text(error?.message);
  if (code === 'PRIVATE_TRANSLATION_TOO_LARGE') return 413;
  if (code === 'PRIVATE_TRANSLATION_INVALID') return 400;
  if (code === 'PRIVATE_TRANSLATION_AUTH') return 401;
  if (
    code.includes('BUDGET') ||
    message.includes('budget_exhausted') ||
    message.includes('private_user_daily_')
  ) return 429;
  return 503;
}

export function getPrivateTranslationHealth() {
  return {
    ...state,
    configured: Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY),
    inflight: inFlight.size,
    ip_windows: ipWindows.size,
    translation_service: getSharedTranslationHealth(),
  };
}

export async function handlePrivateTranslation(req, res, url) {
  if (url.pathname === '/api/private-translation/health') {
    if (req.method !== 'GET') {
      json(res, 405, { ok: false, error: 'method_not_allowed' });
      return true;
    }
    json(res, 200, { ok: true, ...getPrivateTranslationHealth() });
    return true;
  }

  if (url.pathname !== '/api/private-translation') return false;

  if (req.method !== 'POST') {
    json(res, 405, { ok: false, error: 'post_required' });
    return true;
  }

  state.requests += 1;
  if (!allowIp(req)) {
    state.rate_limited += 1;
    json(res, 429, { ok: false, error: 'rate_limited' });
    return true;
  }

  const user = await verifyUser(req);
  if (!user) {
    json(res, 401, { ok: false, error: 'auth_required' });
    return true;
  }

  let body;
  try {
    body = await readJsonBody(req);
  } catch (error) {
    state.validation_rejects += 1;
    json(
      res,
      text(error?.message) === 'request_too_large' ? 413 : 400,
      { ok: false, error: text(error?.message) || 'invalid_request' },
    );
    return true;
  }

  const rawText = text(body?.text);
  const sourceLanguage = text(body?.source_language).toLowerCase();
  const targetLanguage = text(body?.target_language).toLowerCase();
  const sourceBytes = Buffer.byteLength(rawText, 'utf8');

  if (
    !rawText ||
    sourceBytes > MAX_TEXT_BYTES ||
    !['zh', 'en'].includes(sourceLanguage) ||
    !['zh', 'en'].includes(targetLanguage) ||
    sourceLanguage === targetLanguage
  ) {
    state.validation_rejects += 1;
    json(res, sourceBytes > MAX_TEXT_BYTES ? 413 : 400, {
      ok: false,
      error:
        sourceBytes > MAX_TEXT_BYTES
          ? 'private_translation_text_too_large'
          : 'invalid_translation_request',
    });
    return true;
  }

  const digest = crypto
    .createHash('sha256')
    .update(user.id)
    .update('\0')
    .update(sourceLanguage)
    .update('\0')
    .update(targetLanguage)
    .update('\0')
    .update(rawText, 'utf8')
    .digest('hex');
  const key = `${user.id}:${digest}`;

  let promise = inFlight.get(key);
  if (promise) {
    state.inflight_hits += 1;
  } else {
    promise = translatePrivateText({
      rawText,
      sourceLanguage,
      targetLanguage,
      userId: user.id,
    }).finally(() => inFlight.delete(key));
    inFlight.set(key, promise);
  }

  try {
    const result = await promise;
    state.successes += 1;
    state.last_success_at = new Date().toISOString();
    state.last_error = null;
    json(res, 200, {
      ok: true,
      translated_text: result.translated_text,
      source_language: result.source_language,
      target_language: result.target_language,
      source_bytes: result.source_bytes,
      source_characters: result.source_characters,
      private_text_persisted: false,
    });
  } catch (error) {
    const status = statusForError(error);
    if (status === 429) state.budget_rejects += 1;
    else state.provider_failures += 1;
    state.last_error = text(error?.message || error).slice(0, 240);
    json(res, status, {
      ok: false,
      error:
        status === 429
          ? 'translation_budget_unavailable'
          : status === 413
            ? 'private_translation_text_too_large'
            : status === 400
              ? 'invalid_translation_request'
              : 'translation_temporarily_unavailable',
    });
  }

  return true;
}
