import {
  contentTranslationHash,
  detectContentLanguage,
  getSharedTranslationHealth,
  noteUserSharedTranslationRequest,
  translateContentFields,
  translationNeedsWork,
} from './content-translation.mjs';

const SCHEMA = 'step1045_9_shared_on_demand_translation_v1';
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
const MAX_BODY_BYTES = 4096;
const SINGLE_ITEM_MAX_SOURCE_CHARS = Math.max(1000, Math.min(13000, Number(process.env.KAKA_TRANSLATION_ON_DEMAND_SINGLE_ITEM_MAX_CHARS || 12000) || 12000));
const PER_IP_WINDOW_MS = 60_000;
const PER_IP_MAX_REQUESTS = Math.max(2, Math.min(30, Number(process.env.KAKA_TRANSLATION_ON_DEMAND_PER_IP_PER_MINUTE || 8) || 8));
const inFlight = new Map();
const ipWindows = new Map();

const state = {
  schema: SCHEMA,
  enabled: true,
  supabase_configured: Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY),
  arbitrary_text_translation_allowed: false,
  public_content_id_only: true,
  shared_singleflight: true,
  silent_unavailable_response: true,
  supported_kinds: ['news', 'article', 'social', 'airdrop'],
  single_item_max_source_chars: SINGLE_ITEM_MAX_SOURCE_CHARS,
  per_ip_per_minute: PER_IP_MAX_REQUESTS,
  requests: 0,
  cached_hits: 0,
  same_language_skips: 0,
  budget_silent_skips: 0,
  provider_silent_skips: 0,
  validation_rejects: 0,
  translated: 0,
  db_writes: 0,
  supabase_reads: 0,
  last_success_at: null,
  last_error: null,
};

function text(v) { return String(v ?? '').trim(); }
function nowIso() { return new Date().toISOString(); }
function headers(extra = {}) {
  return {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    'content-type': 'application/json',
    ...extra,
  };
}
function json(res, status, body) {
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
  res.end(JSON.stringify(body));
}
function silent(res, status = 'unavailable') {
  json(res, 200, { ok: true, translated: false, silent: true, status });
}
function isUuid(value) { return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value); }
function ipKey(req) {
  const forwarded = text(req.headers?.['x-forwarded-for']).split(',')[0].trim();
  return forwarded || text(req.socket?.remoteAddress) || 'unknown';
}
function allowIp(req) {
  const key = ipKey(req);
  const now = Date.now();
  const existing = ipWindows.get(key);
  if (!existing || now - existing.started_at >= PER_IP_WINDOW_MS) {
    ipWindows.set(key, { started_at: now, count: 1 });
    if (ipWindows.size > 2000) {
      for (const [candidate, row] of ipWindows) if (now - row.started_at > PER_IP_WINDOW_MS * 2) ipWindows.delete(candidate);
    }
    return true;
  }
  if (existing.count >= PER_IP_MAX_REQUESTS) return false;
  existing.count++;
  return true;
}
async function readJsonBody(req) {
  return new Promise((resolve, reject) => {
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
      try { resolve(raw ? JSON.parse(raw) : {}); }
      catch (_) { reject(new Error('invalid_json')); }
    });
    req.on('error', reject);
  });
}
async function supabaseFetch(path, init = {}) {
  if (!state.supabase_configured) throw new Error('translation_supabase_not_configured');
  state.supabase_reads += init.method && init.method !== 'GET' ? 0 : 1;
  const response = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
    ...init,
    headers: { ...headers(), ...(init.headers || {}) },
  });
  const raw = await response.text();
  let payload = null;
  try { payload = raw ? JSON.parse(raw) : null; } catch (_) { payload = raw; }
  if (!response.ok) throw new Error(`translation_supabase_http_${response.status}:${text(raw).slice(0, 200)}`);
  return payload;
}
function firstRow(payload) { return Array.isArray(payload) && payload[0] && typeof payload[0] === 'object' ? payload[0] : null; }

async function loadPublicRow(kind, id) {
  if (kind === 'news') {
    if (!isUuid(id)) return null;
    return firstRow(await supabaseFetch(`app_newsflashes?id=eq.${encodeURIComponent(id)}&is_active=eq.true&select=id,title,content,translations,translation_source_hash,translation_updated_at&limit=1`));
  }
  if (kind === 'article') {
    if (!isUuid(id)) return null;
    const row = firstRow(await supabaseFetch(`app_articles?id=eq.${encodeURIComponent(id)}&status=eq.published&select=id,title,summary,content,status,visibility_type,translations,translation_source_hash,translation_updated_at&limit=1`));
    if (!row) return null;
    const visibility = text(row.visibility_type).toLowerCase();
    return !visibility || visibility === 'public' ? row : null;
  }
  if (kind === 'social') {
    if (!/^\d{1,20}$/.test(id)) return null;
    const row = firstRow(await supabaseFetch(`app_social_watch_events?id=eq.${encodeURIComponent(id)}&select=id,watch_account_id,content,translations,translation_source_hash,translation_updated_at&limit=1`));
    if (!row || !text(row.watch_account_id)) return null;
    const account = firstRow(await supabaseFetch(`app_social_watch_accounts?id=eq.${encodeURIComponent(text(row.watch_account_id))}&is_active=eq.true&public_visible=eq.true&select=id&limit=1`));
    return account ? row : null;
  }
  if (kind === 'airdrop') {
    if (!isUuid(id)) return null;
    return firstRow(await supabaseFetch(`app_airdrop_events?id=eq.${encodeURIComponent(id)}&select=id,title,reward_text,eligibility_text,raw_summary,content_text,translations,translation_source_hash,translation_updated_at&limit=1`));
  }
  return null;
}
function fieldsFor(kind, row) {
  if (kind === 'news') return { title: text(row.title), content: text(row.content) };
  if (kind === 'article') return { title: text(row.title), summary: text(row.summary), content: text(row.content) };
  if (kind === 'social') return { content: text(row.content) };
  if (kind === 'airdrop') return {
    title: text(row.title),
    reward_text: text(row.reward_text),
    eligibility_text: text(row.eligibility_text),
    raw_summary: text(row.raw_summary),
    content: text(row.content_text),
  };
  return {};
}
function normalizeTranslations(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {};
  const out = {};
  for (const locale of ['zh', 'en']) {
    const row = raw[locale];
    if (row && typeof row === 'object' && !Array.isArray(row)) out[locale] = { ...row };
  }
  return out;
}
function sourceMatchesTarget(source, target) {
  return (source === 'zh' && target === 'zh') || (source === 'en' && target === 'en');
}
function targetNeedsSource(source, target) {
  return (source === 'en' && target === 'zh') || (source === 'zh' && target === 'en');
}
function estimateMissingChars(fields, translations, sourceHash, target) {
  const normalized = {};
  for (const [key, value] of Object.entries(fields)) if (text(value)) normalized[key] = text(value);
  const fresh = text(sourceHash) === contentTranslationHash(normalized);
  const existing = fresh ? normalizeTranslations(translations) : {};
  let missing = 0;
  let crossLanguageFields = 0;
  let sameLanguageFields = 0;
  for (const [field, value] of Object.entries(normalized)) {
    const source = detectContentLanguage(value);
    if (sourceMatchesTarget(source, target)) { sameLanguageFields++; continue; }
    if (!targetNeedsSource(source, target)) continue;
    crossLanguageFields++;
    if (!text(existing?.[target]?.[field])) missing += Array.from(value).length;
  }
  return { missing, crossLanguageFields, sameLanguageFields, existing, source_hash: contentTranslationHash(normalized) };
}
async function persist(kind, id, result) {
  const table = kind === 'news' ? 'app_newsflashes'
    : kind === 'article' ? 'app_articles'
      : kind === 'social' ? 'app_social_watch_events'
        : 'app_airdrop_events';
  const response = await fetch(`${SUPABASE_URL}/rest/v1/${table}?id=eq.${encodeURIComponent(id)}`, {
    method: 'PATCH',
    headers: headers({ Prefer: 'return=minimal' }),
    body: JSON.stringify({
      translations: result.translations,
      translation_source_hash: result.source_hash,
      translation_updated_at: nowIso(),
    }),
  });
  if (!response.ok) {
    const raw = await response.text();
    throw new Error(`translation_persist_http_${response.status}:${raw.slice(0, 180)}`);
  }
  state.db_writes++;
}
async function translateKnownPublicContent(kind, id, target) {
  const row = await loadPublicRow(kind, id);
  if (!row) return { status: 'not_found', silent: true };
  const fields = fieldsFor(kind, row);
  const estimate = estimateMissingChars(fields, row.translations, row.translation_source_hash, target);
  if (estimate.crossLanguageFields < 1) {
    state.same_language_skips++;
    return { status: 'same_language', translations: normalizeTranslations(row.translations), source_hash: estimate.source_hash };
  }
  if (estimate.missing < 1 && !translationNeedsWork({
    fields,
    existingTranslations: row.translations,
    existingSourceHash: text(row.translation_source_hash),
    targetLocale: target,
  })) {
    state.cached_hits++;
    return { status: 'cached', translations: normalizeTranslations(row.translations), source_hash: estimate.source_hash };
  }
  const health = getSharedTranslationHealth();
  const dailyRemaining = Math.max(0, Number(health.daily_budget_remaining || 0) || 0);
  const monthlyRemaining = Math.max(0, Number(health.monthly_bank_remaining ?? (Number(health.monthly_char_limit || 0) - Number(health.monthly_characters_used || 0))) || 0);
  if (!health.configured || health.provider_ready === false || estimate.missing > SINGLE_ITEM_MAX_SOURCE_CHARS || estimate.missing > dailyRemaining || estimate.missing > monthlyRemaining) {
    state.budget_silent_skips++;
    return { status: 'unavailable', silent: true };
  }
  noteUserSharedTranslationRequest();
  const result = await translateContentFields({
    fields,
    existingTranslations: row.translations,
    existingSourceHash: text(row.translation_source_hash),
    maxSourceChars: SINGLE_ITEM_MAX_SOURCE_CHARS,
    budgetBucket: 'user_on_demand',
    targetLocale: target,
  });
  if (!result.changed || result.translated_fields < 1) return { status: 'unavailable', silent: true };
  await persist(kind, id, result);
  state.translated++;
  state.last_success_at = nowIso();
  state.last_error = null;
  return { status: 'translated', translations: result.translations, source_hash: result.source_hash };
}

export function getContentOnDemandTranslationHealth() {
  return {
    ...state,
    inflight: inFlight.size,
    translation_service: getSharedTranslationHealth(),
  };
}

export async function handleContentOnDemandTranslation(req, res, url) {
  if (url.pathname === '/api/content-translation/on-demand/health') {
    if (req.method !== 'GET') { json(res, 405, { ok: false, error: 'method_not_allowed' }); return true; }
    json(res, 200, { ok: true, ...getContentOnDemandTranslationHealth() });
    return true;
  }
  if (url.pathname !== '/api/content-translation/request') return false;
  if (req.method !== 'POST') { json(res, 405, { ok: false, error: 'method_not_allowed' }); return true; }
  state.requests++;
  if (!state.supabase_configured || !allowIp(req)) { silent(res); return true; }
  let body;
  try { body = await readJsonBody(req); }
  catch (_) { state.validation_rejects++; silent(res); return true; }
  const kind = text(body?.kind).toLowerCase();
  const id = text(body?.id);
  const target = text(body?.target_locale).toLowerCase();
  if (!state.supported_kinds.includes(kind) || !id || !['zh', 'en'].includes(target)) {
    state.validation_rejects++;
    silent(res);
    return true;
  }
  const key = `${kind}:${id}:${target}`;
  let promise = inFlight.get(key);
  if (!promise) {
    promise = translateKnownPublicContent(kind, id, target)
      .catch((error) => {
        state.provider_silent_skips++;
        state.last_error = String(error?.message || error);
        return { status: 'unavailable', silent: true };
      })
      .finally(() => inFlight.delete(key));
    inFlight.set(key, promise);
  }
  const result = await promise;
  if (result?.silent) { silent(res, result.status || 'unavailable'); return true; }
  json(res, 200, {
    ok: true,
    translated: result.status === 'translated',
    cached: result.status === 'cached',
    same_language: result.status === 'same_language',
    status: result.status,
    translations: result.translations || {},
    translation_source_hash: result.source_hash || null,
  });
  return true;
}
