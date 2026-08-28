import crypto from 'node:crypto';

const ENABLED = String(process.env.KAKA_CONTENT_AUTO_TRANSLATION_ENABLED || '1') !== '0';
const GOOGLE_API_KEY = String(process.env.KAKA_GOOGLE_TRANSLATION_API_KEY || process.env.GOOGLE_TRANSLATION_API_KEY || '').trim();
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
const USAGE_TABLE = 'app_content_translation_usage';
const DAILY_USAGE_TABLE = 'app_content_translation_daily_usage';
const MONTHLY_CHAR_LIMIT = Math.max(50_000, Math.min(5_000_000, Number(process.env.KAKA_TRANSLATION_MONTHLY_CHAR_LIMIT || 450000) || 450000));
const DAILY_SOFT_CHAR_LIMIT = Math.max(5_000, Math.min(100_000, Number(process.env.KAKA_TRANSLATION_DAILY_SOFT_CHAR_LIMIT || 14000) || 14000));
// Step1045.9: Google itself does not roll unused daily quota into tomorrow. Kaka therefore
// keeps the real monthly 450k guard as the carry-forward bank, while reserving part of each
// Pacific day for explicit user translation taps. Background auto-translation is Chinese-first
// and may consume at most 9k/day by default; user-on-demand may use any remaining room under the 14k soft cap.
const AUTO_DAILY_CHAR_LIMIT = Math.max(1_000, Math.min(DAILY_SOFT_CHAR_LIMIT, Number(process.env.KAKA_TRANSLATION_AUTO_DAILY_CHAR_LIMIT || 9000) || 9000));
const DAILY_BUCKET_LIMITS = Object.freeze({
  auto_zh: AUTO_DAILY_CHAR_LIMIT,
  user_on_demand: DAILY_SOFT_CHAR_LIMIT,
  // Legacy bucket names remain readable so an in-progress Pacific day created by older
  // versions is accounted for exactly instead of being forgotten after deploy/restart.
  news_title: DAILY_SOFT_CHAR_LIMIT,
  airdrop: DAILY_SOFT_CHAR_LIMIT,
  social: DAILY_SOFT_CHAR_LIMIT,
  article: DAILY_SOFT_CHAR_LIMIT,
  news_body: DAILY_SOFT_CHAR_LIMIT,
  legacy_bootstrap: DAILY_SOFT_CHAR_LIMIT,
});
const REQUEST_TIMEOUT_MS = Math.max(3000, Math.min(20000, Number(process.env.KAKA_TRANSLATION_TIMEOUT_MS || 10000) || 10000));
const MAX_FIELD_CHARS = Math.max(1200, Math.min(60000, Number(process.env.KAKA_TRANSLATION_MAX_FIELD_CHARS || 30000) || 30000));
const SEGMENT_CHARS = Math.max(1200, Math.min(5000, Number(process.env.KAKA_TRANSLATION_SEGMENT_CHARS || 3800) || 3800));
const PROVIDER_MIN_GAP_MS = Math.max(80, Math.min(1500, Number(process.env.KAKA_TRANSLATION_REQUEST_GAP_MS || 180) || 180));
const RETRY_COOLDOWN_MS = Math.max(60_000, Math.min(6 * 60 * 60_000, Number(process.env.KAKA_TRANSLATION_RETRY_COOLDOWN_MS || 15 * 60_000) || 15 * 60_000));

const state = {
  enabled: ENABLED,
  configured: Boolean(GOOGLE_API_KEY),
  provider_mode: GOOGLE_API_KEY ? 'google_cloud_translation_v2' : 'google_cloud_translation_key_required',
  google_configured: Boolean(GOOGLE_API_KEY),
  mymemory_disabled: true,
  shared_background_only: false,
  shared_background_and_on_demand: true,
  user_on_demand_enabled: true,
  arbitrary_text_translation_allowed: false,
  user_translation_requests: 0,
  requests: 0,
  successes: 0,
  failures: 0,
  translated_fields: 0,
  translated_characters: 0,
  skipped_unclassified: 0,
  last_request_at: null,
  last_success_at: null,
  last_error: GOOGLE_API_KEY ? null : 'translation_provider_not_configured:KAKA_GOOGLE_TRANSLATION_API_KEY',
  cooldown_until: null,
  cooldown_reason: null,
  cooldown_opened_at: null,
  provider_auth_mode: 'x_goog_api_key_header',
  provider_last_http_status: null,
  provider_last_http_error: null,
  provider_last_error_at: null,
  provider_last_success_at: null,
  provider_last_response_ms: null,
  provider_failures: 0,
  provider_consecutive_failures: 0,
  monthly_char_limit: MONTHLY_CHAR_LIMIT,
  monthly_characters_used: 0,
  monthly_requests: 0,
  daily_soft_char_limit: DAILY_SOFT_CHAR_LIMIT,
  auto_daily_char_limit: AUTO_DAILY_CHAR_LIMIT,
  daily_characters_used: 0,
  daily_requests: 0,
  daily_by_bucket: {},
  daily_budget_day_key: null,
  daily_budget_persistence_healthy: true,
  daily_bucket_limits: { ...DAILY_BUCKET_LIMITS },
  usage_persistence_configured: Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY),
  usage_persistence_healthy: true,
};

let lane = Promise.resolve();
let lastProviderRequestAtMs = 0;
let cooldownUntilMs = 0;
let usageLoadedMonth = '';
let usageLoadPromise = null;
let dailyUsageLoadedKey = '';
let dailyUsageLoadPromise = null;

function text(value) { return String(value ?? '').trim(); }
function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }
function normalizeWhitespace(raw) {
  return text(raw)
    .replace(/\r/g, '')
    .replace(/[ \t]+/g, ' ')
    .replace(/ *\n */g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}
function decodeHtmlEntities(raw) {
  return text(raw)
    .replace(/&quot;/gi, '"')
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&amp;/gi, '&')
    .replace(/&#x([0-9a-f]+);/gi, (_, hex) => String.fromCodePoint(parseInt(hex, 16)))
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(parseInt(n, 10)));
}

export function detectContentLanguage(raw) {
  const value = text(raw);
  if (!value) return 'unknown';
  const cjk = (value.match(/[\u3400-\u4dbf\u4e00-\u9fff]/g) || []).length;
  const latin = (value.match(/[A-Za-z]/g) || []).length;
  if (cjk >= 4 && cjk * 1.15 >= latin) return 'zh';
  if (latin >= 8 && latin >= cjk * 1.8) return 'en';
  if (cjk >= 2 && cjk > latin) return 'zh';
  if (latin >= 5 && latin > cjk) return 'en';
  return 'unknown';
}

export function contentTranslationHash(fields) {
  const ordered = Object.keys(fields || {})
    .sort()
    .map((key) => `${key}\u0000${normalizeWhitespace(fields?.[key])}`)
    .join('\u0001');
  return crypto.createHash('sha256').update(ordered, 'utf8').digest('hex');
}

function targetForSource(sourceLanguage) {
  if (sourceLanguage === 'en') return 'zh';
  if (sourceLanguage === 'zh') return 'en';
  return '';
}
function googleLanguage(locale) { return locale === 'zh' ? 'zh-CN' : 'en'; }

function monthKey() { return new Date().toISOString().slice(0, 7); }
function pacificDayKey() {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Los_Angeles', year: 'numeric', month: '2-digit', day: '2-digit',
  }).format(new Date());
}
function normalizeBudgetBucket(value) {
  const key = text(value).toLowerCase();
  return Object.prototype.hasOwnProperty.call(DAILY_BUCKET_LIMITS, key) ? key : 'legacy_bootstrap';
}
function usageHeaders(extra = {}) {
  return {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    'content-type': 'application/json',
    ...extra,
  };
}
async function ensureUsageLoaded() {
  const month = monthKey();
  if (usageLoadedMonth === month) return;
  if (usageLoadPromise) return usageLoadPromise;
  usageLoadPromise = (async () => {
    state.monthly_characters_used = 0;
    state.monthly_requests = 0;
    if (SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY) {
      try {
        const response = await fetch(`${SUPABASE_URL}/rest/v1/${USAGE_TABLE}?month_key=eq.${encodeURIComponent(month)}&select=characters_used,requests&limit=1`, { headers: usageHeaders({ accept: 'application/json' }) });
        const raw = await response.text();
        if (!response.ok) throw new Error(`translation_usage_http_${response.status}:${raw.slice(0, 180)}`);
        let rows = [];
        try { rows = raw ? JSON.parse(raw) : []; } catch (_) {}
        const row = Array.isArray(rows) ? rows[0] : null;
        state.monthly_characters_used = Math.max(0, Number(row?.characters_used || 0) || 0);
        state.monthly_requests = Math.max(0, Number(row?.requests || 0) || 0);
        state.usage_persistence_healthy = true;
      } catch (error) {
        state.usage_persistence_healthy = false;
        state.last_error = String(error?.message || error);
        throw error;
      }
    }
    usageLoadedMonth = month;
  })().finally(() => { usageLoadPromise = null; });
  return usageLoadPromise;
}
async function ensureDailyUsageLoaded() {
  const day = pacificDayKey();
  if (dailyUsageLoadedKey === day) return;
  if (dailyUsageLoadPromise) return dailyUsageLoadPromise;
  dailyUsageLoadPromise = (async () => {
    state.daily_characters_used = 0;
    state.daily_requests = 0;
    state.daily_by_bucket = {};
    state.daily_budget_day_key = day;
    if (SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY) {
      try {
        const response = await fetch(`${SUPABASE_URL}/rest/v1/${DAILY_USAGE_TABLE}?day_key=eq.${encodeURIComponent(day)}&select=bucket,characters_used,requests`, { headers: usageHeaders({ accept: 'application/json' }) });
        const raw = await response.text();
        if (!response.ok) throw new Error(`translation_daily_usage_http_${response.status}:${raw.slice(0, 180)}`);
        let rows = [];
        try { rows = raw ? JSON.parse(raw) : []; } catch (_) {}
        for (const row of Array.isArray(rows) ? rows : []) {
          const bucket = normalizeBudgetBucket(row?.bucket);
          const chars = Math.max(0, Number(row?.characters_used || 0) || 0);
          const requests = Math.max(0, Number(row?.requests || 0) || 0);
          state.daily_by_bucket[bucket] = (state.daily_by_bucket[bucket] || 0) + chars;
          state.daily_characters_used += chars;
          state.daily_requests += requests;
        }
        state.daily_budget_persistence_healthy = true;
      } catch (error) {
        state.daily_budget_persistence_healthy = false;
        state.last_error = String(error?.message || error);
        throw error;
      }
    }
    dailyUsageLoadedKey = day;
  })().finally(() => { dailyUsageLoadPromise = null; });
  return dailyUsageLoadPromise;
}

function assertDailyBudget(bucket, chargeChars) {
  const normalized = normalizeBudgetBucket(bucket);
  const bucketUsed = Math.max(0, Number(state.daily_by_bucket?.[normalized] || 0) || 0);
  if (state.daily_characters_used + chargeChars > DAILY_SOFT_CHAR_LIMIT) {
    const error = new Error(`translation_daily_soft_budget_exhausted:${state.daily_characters_used}/${DAILY_SOFT_CHAR_LIMIT}`);
    error.code = 'TRANSLATION_DAILY_BUDGET';
    throw error;
  }
  if (normalized === 'auto_zh' && bucketUsed + chargeChars > AUTO_DAILY_CHAR_LIMIT) {
    const error = new Error(`translation_auto_daily_budget_exhausted:${bucketUsed}/${AUTO_DAILY_CHAR_LIMIT}`);
    error.code = 'TRANSLATION_AUTO_DAILY_BUDGET';
    throw error;
  }
  return normalized;
}

async function persistDailyUsage(bucket, deltaChars) {
  const normalized = normalizeBudgetBucket(bucket);
  const day = pacificDayKey();
  const nextChars = Math.max(0, Number(state.daily_by_bucket?.[normalized] || 0) || 0) + deltaChars;
  state.daily_by_bucket[normalized] = nextChars;
  state.daily_characters_used += deltaChars;
  state.daily_requests += 1;
  state.daily_budget_day_key = day;
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) return;
  const currentRows = await fetch(`${SUPABASE_URL}/rest/v1/${DAILY_USAGE_TABLE}?day_key=eq.${encodeURIComponent(day)}&bucket=eq.${encodeURIComponent(normalized)}&select=characters_used,requests&limit=1`, { headers: usageHeaders({ accept: 'application/json' }) });
  const currentRaw = await currentRows.text();
  if (!currentRows.ok) throw new Error(`translation_daily_usage_read_http_${currentRows.status}:${currentRaw.slice(0, 180)}`);
  let current = null;
  try { current = (JSON.parse(currentRaw) || [])[0] || null; } catch (_) {}
  const dbChars = Math.max(0, Number(current?.characters_used || 0) || 0) + deltaChars;
  const dbRequests = Math.max(0, Number(current?.requests || 0) || 0) + 1;
  const response = await fetch(`${SUPABASE_URL}/rest/v1/${DAILY_USAGE_TABLE}?on_conflict=day_key,bucket`, {
    method: 'POST',
    headers: usageHeaders({ Prefer: 'resolution=merge-duplicates,return=minimal' }),
    body: JSON.stringify({
      day_key: day,
      bucket: normalized,
      characters_used: dbChars,
      requests: dbRequests,
      updated_at: new Date().toISOString(),
    }),
  });
  if (!response.ok) {
    const raw = await response.text();
    state.daily_budget_persistence_healthy = false;
    throw new Error(`translation_daily_usage_persist_http_${response.status}:${raw.slice(0, 180)}`);
  }
  state.daily_budget_persistence_healthy = true;
}

async function persistUsage(deltaChars) {
  state.monthly_characters_used += deltaChars;
  state.monthly_requests += 1;
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) return;
  const month = monthKey();
  const response = await fetch(`${SUPABASE_URL}/rest/v1/${USAGE_TABLE}?on_conflict=month_key`, {
    method: 'POST',
    headers: usageHeaders({ Prefer: 'resolution=merge-duplicates,return=minimal' }),
    body: JSON.stringify({
      month_key: month,
      provider: 'google_cloud_translation_v2',
      characters_used: state.monthly_characters_used,
      requests: state.monthly_requests,
      updated_at: new Date().toISOString(),
    }),
  });
  if (!response.ok) {
    const raw = await response.text();
    state.usage_persistence_healthy = false;
    throw new Error(`translation_usage_persist_http_${response.status}:${raw.slice(0, 180)}`);
  }
  state.usage_persistence_healthy = true;
}

function splitText(raw) {
  const value = normalizeWhitespace(raw).slice(0, MAX_FIELD_CHARS);
  if (!value) return [];
  if (value.length <= SEGMENT_CHARS) return [value];
  const out = [];
  let remaining = value;
  while (remaining.length > SEGMENT_CHARS) {
    let cut = SEGMENT_CHARS;
    const window = remaining.slice(0, SEGMENT_CHARS + 1);
    const candidates = [window.lastIndexOf('\n\n'), window.lastIndexOf('\n'), window.lastIndexOf('。'), window.lastIndexOf('. '), window.lastIndexOf('！'), window.lastIndexOf('？')];
    const best = Math.max(...candidates);
    if (best >= Math.floor(SEGMENT_CHARS * 0.55)) cut = best + 1;
    out.push(remaining.slice(0, cut).trim());
    remaining = remaining.slice(cut).trim();
  }
  if (remaining) out.push(remaining);
  return out.filter(Boolean);
}

async function serializedProviderRequest(fn) {
  const run = lane.then(async () => {
    const now = Date.now();
    if (cooldownUntilMs > now) {
      const error = new Error(`translation_provider_cooldown_until:${new Date(cooldownUntilMs).toISOString()}`);
      error.code = 'TRANSLATION_COOLDOWN';
      throw error;
    }
    const wait = Math.max(0, PROVIDER_MIN_GAP_MS - (Date.now() - lastProviderRequestAtMs));
    if (wait > 0) await sleep(wait);
    lastProviderRequestAtMs = Date.now();
    state.last_request_at = new Date().toISOString();
    return fn();
  });
  lane = run.catch(() => undefined);
  return run;
}

async function fetchWithTimeout(url, init = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try { return await fetch(url, { ...init, signal: controller.signal }); }
  finally { clearTimeout(timer); }
}

function nextPacificMidnightMs() {
  // Google daily Translation quotas reset at midnight Pacific Time.  Use Intl so
  // DST is handled by the runtime rather than hard-coding PST/PDT offsets.
  const now = new Date();
  const parts = Object.fromEntries(new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Los_Angeles', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23',
  }).formatToParts(now).filter((part) => part.type !== 'literal').map((part) => [part.type, part.value]));
  const pacificTodayUtcGuess = Date.UTC(Number(parts.year), Number(parts.month) - 1, Number(parts.day), 12, 0, 0);
  let probe = new Date(pacificTodayUtcGuess + 24 * 60 * 60_000);
  for (let i = 0; i < 4; i++) {
    const pp = Object.fromEntries(new Intl.DateTimeFormat('en-US', {
      timeZone: 'America/Los_Angeles', year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23',
    }).formatToParts(probe).filter((part) => part.type !== 'literal').map((part) => [part.type, part.value]));
    const deltaMinutes = Number(pp.hour) * 60 + Number(pp.minute);
    probe = new Date(probe.getTime() - deltaMinutes * 60_000 - Number(pp.second) * 1000);
    if (Number(pp.hour) === 0 && Number(pp.minute) === 0) break;
  }
  return Math.max(Date.now() + RETRY_COOLDOWN_MS, probe.getTime() + 2 * 60_000);
}
function openCooldown(message, response = null) {
  let untilMs = Date.now() + RETRY_COOLDOWN_MS;
  const header = Number(response?.headers?.get?.('retry-after') || 0);
  if (Number.isFinite(header) && header > 0) untilMs = Math.max(untilMs, Date.now() + header * 1000);
  if (/daily limit exceeded|user.?rate.?limit.?exceeded|userRateLimitExceeded/i.test(message)) untilMs = nextPacificMidnightMs();
  cooldownUntilMs = untilMs;
  state.cooldown_until = new Date(cooldownUntilMs).toISOString();
  state.cooldown_reason = message;
  state.cooldown_opened_at = new Date().toISOString();
  state.last_error = message;
}

async function translateGoogleSegment(value, source, target, budgetBucket = 'airdrop') {
  if (!GOOGLE_API_KEY) throw new Error('translation_provider_not_configured:KAKA_GOOGLE_TRANSLATION_API_KEY');
  await ensureUsageLoaded();
  await ensureDailyUsageLoaded();
  const chargeChars = Array.from(value).length;
  const normalizedBucket = assertDailyBudget(budgetBucket, chargeChars);
  if (state.monthly_characters_used + chargeChars > MONTHLY_CHAR_LIMIT) {
    const error = new Error(`translation_monthly_budget_exhausted:${state.monthly_characters_used}/${MONTHLY_CHAR_LIMIT}`);
    state.last_error = error.message;
    throw error;
  }
  state.requests++;
  return serializedProviderRequest(async () => {
    const startedAt = Date.now();
    let response = null;
    try {
      response = await fetchWithTimeout('https://translation.googleapis.com/language/translate/v2', {
        method: 'POST',
        headers: {
          'content-type': 'application/json; charset=utf-8',
          accept: 'application/json',
          'x-goog-api-key': GOOGLE_API_KEY,
        },
        body: JSON.stringify({
          q: value,
          source: googleLanguage(source),
          target: googleLanguage(target),
          format: 'text',
        }),
      });
      state.provider_last_http_status = response.status;
      state.provider_last_response_ms = Date.now() - startedAt;
      const raw = await response.text();
      if (!response.ok) {
        const message = `google_translation_http_${response.status}:${raw.slice(0, 420)}`;
        state.provider_last_http_error = message;
        state.provider_last_error_at = new Date().toISOString();
        state.provider_failures++;
        state.provider_consecutive_failures++;
        if (response.status === 429 || response.status === 403 || response.status >= 500) openCooldown(message, response);
        throw new Error(message);
      }
      let payload = null;
      try { payload = JSON.parse(raw); } catch (_) {}
      const translated = decodeHtmlEntities(payload?.data?.translations?.[0]?.translatedText || '');
      if (!translated) throw new Error('google_translation_empty');
      await persistUsage(chargeChars);
      await persistDailyUsage(normalizedBucket, chargeChars);
      state.provider_last_success_at = new Date().toISOString();
      state.provider_consecutive_failures = 0;
      state.cooldown_reason = null;
      state.cooldown_until = null;
      return translated;
    } catch (error) {
      state.provider_last_response_ms = Date.now() - startedAt;
      if (error?.name === 'AbortError') {
        const message = 'google_translation_timeout';
        state.provider_last_http_error = message;
        state.provider_last_error_at = new Date().toISOString();
        state.provider_failures++;
        state.provider_consecutive_failures++;
        openCooldown(message, response);
      }
      throw error;
    }
  });
}

async function translateField(raw, source, target, budgetBucket = 'airdrop') {
  const segments = splitText(raw);
  if (!segments.length) return '';
  const translated = [];
  for (const segment of segments) translated.push(await translateGoogleSegment(segment, source, target, budgetBucket));
  return normalizeWhitespace(translated.join('\n\n'));
}

function normalizeTranslations(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {};
  const out = {};
  for (const locale of ['zh', 'en']) {
    const value = raw[locale];
    if (!value || typeof value !== 'object' || Array.isArray(value)) continue;
    out[locale] = { ...value };
  }
  return out;
}

export function translationNeedsWork({ fields, existingTranslations = {}, existingSourceHash = '', requiredFields = null, targetLocale = '' }) {
  const normalizedFields = {};
  for (const [key, value] of Object.entries(fields || {})) {
    const clean = normalizeWhitespace(value);
    if (clean) normalizedFields[key] = clean;
  }
  const sourceHash = contentTranslationHash(normalizedFields);
  const fresh = text(existingSourceHash) === sourceHash;
  const existing = fresh ? normalizeTranslations(existingTranslations) : {};
  const allow = Array.isArray(requiredFields) && requiredFields.length ? new Set(requiredFields) : null;
  const requestedTarget = text(targetLocale).toLowerCase();
  for (const [field, value] of Object.entries(normalizedFields)) {
    if (allow && !allow.has(field)) continue;
    const source = detectContentLanguage(value);
    const target = targetForSource(source);
    if (!target) continue;
    if (requestedTarget && target !== requestedTarget) continue;
    if (!fresh || !text(existing?.[target]?.[field])) return true;
  }
  return false;
}

export async function translateContentFields({ fields, existingTranslations = {}, existingSourceHash = '', onlyFields = null, maxSourceChars = Number.POSITIVE_INFINITY, budgetBucket = 'auto_zh', targetLocale = '' }) {
  const normalizedFields = {};
  for (const [key, value] of Object.entries(fields || {})) {
    const clean = normalizeWhitespace(value);
    if (clean) normalizedFields[key] = clean;
  }
  const sourceHash = contentTranslationHash(normalizedFields);
  const existing = normalizeTranslations(existingTranslations);
  if (!ENABLED || !Object.keys(normalizedFields).length) {
    return { changed: false, source_hash: sourceHash, translations: existing, translated_fields: 0 };
  }
  if (!GOOGLE_API_KEY) throw new Error('translation_provider_not_configured:KAKA_GOOGLE_TRANSLATION_API_KEY');

  const needed = [];
  for (const [field, value] of Object.entries(normalizedFields)) {
    const source = detectContentLanguage(value);
    const target = targetForSource(source);
    if (!target) { state.skipped_unclassified++; continue; }
    const requestedTarget = text(targetLocale).toLowerCase();
    if (requestedTarget && target !== requestedTarget) continue;
    needed.push({ field, value, source, target });
  }
  if (!needed.length) {
    return { changed: existingSourceHash !== sourceHash, source_hash: sourceHash, translations: {}, translated_fields: 0 };
  }

  // Source edits atomically invalidate stale translations.
  const next = existingSourceHash === sourceHash ? existing : {};
  const allowed = Array.isArray(onlyFields) && onlyFields.length ? new Set(onlyFields) : null;
  let translatedFields = 0;
  let translatedSourceChars = 0;
  let changed = existingSourceHash !== sourceHash;
  for (const job of needed) {
    if (allowed && !allowed.has(job.field)) continue;
    const current = text(next?.[job.target]?.[job.field]);
    if (current && existingSourceHash === sourceHash) continue;
    const jobChars = Array.from(job.value).length;
    if (translatedSourceChars + jobChars > maxSourceChars) continue;
    try {
      const translated = await translateField(job.value, job.source, job.target, budgetBucket);
      if (!translated) continue;
      next[job.target] ||= {};
      next[job.target][job.field] = translated;
      translatedFields++;
      translatedSourceChars += jobChars;
      changed = true;
      state.successes++;
      state.translated_fields++;
      state.translated_characters += jobChars;
      state.last_success_at = new Date().toISOString();
      state.last_error = null;
    } catch (error) {
      state.failures++;
      if (error?.code !== 'TRANSLATION_COOLDOWN') {
        state.last_error = String(error?.name === 'AbortError' ? 'translation_timeout' : error?.message || error);
      }
      throw error;
    }
  }
  return { changed, source_hash: sourceHash, translations: next, translated_fields: translatedFields, translated_source_chars: translatedSourceChars };
}

export function noteUserSharedTranslationRequest() {
  state.user_translation_requests += 1;
  return state.user_translation_requests;
}

export function getSharedTranslationHealth() {
  return {
    ...state,
    configured: Boolean(GOOGLE_API_KEY),
    provider_mode: GOOGLE_API_KEY ? 'google_cloud_translation_v2' : 'google_cloud_translation_key_required',
    provider_auth_mode: 'x_goog_api_key_header',
    cooldown_until: cooldownUntilMs > Date.now() ? new Date(cooldownUntilMs).toISOString() : null,
    provider_ready: Boolean(GOOGLE_API_KEY) && state.provider_consecutive_failures === 0 && cooldownUntilMs <= Date.now(),
    daily_budget_ready: state.daily_characters_used < DAILY_SOFT_CHAR_LIMIT,
    daily_budget_remaining: Math.max(0, DAILY_SOFT_CHAR_LIMIT - state.daily_characters_used),
    auto_daily_budget_ready: Math.max(0, Number(state.daily_by_bucket?.auto_zh || 0) || 0) < AUTO_DAILY_CHAR_LIMIT && state.daily_characters_used < DAILY_SOFT_CHAR_LIMIT,
    auto_daily_budget_remaining: Math.max(0, Math.min(AUTO_DAILY_CHAR_LIMIT - Math.max(0, Number(state.daily_by_bucket?.auto_zh || 0) || 0), DAILY_SOFT_CHAR_LIMIT - state.daily_characters_used)),
    on_demand_daily_budget_remaining: Math.max(0, DAILY_SOFT_CHAR_LIMIT - state.daily_characters_used),
    monthly_bank_remaining: Math.max(0, MONTHLY_CHAR_LIMIT - state.monthly_characters_used),
    google_daily_hard_quota_rollover_supported: false,
    unused_daily_budget_preserved_as_monthly_bank: true,
    daily_bucket_remaining: Object.fromEntries(Object.entries(DAILY_BUCKET_LIMITS).map(([key, limit]) => [key, key === 'user_on_demand' ? Math.max(0, DAILY_SOFT_CHAR_LIMIT - state.daily_characters_used) : Math.max(0, limit - Math.max(0, Number(state.daily_by_bucket?.[key] || 0) || 0))])),
  };
}
