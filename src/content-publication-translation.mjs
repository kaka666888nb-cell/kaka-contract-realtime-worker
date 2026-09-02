import {
  getSharedTranslationHealth,
  translateContentFields,
  translationNeedsWork,
} from './content-translation.mjs';

const SCHEMA = 'step1045_9_chinese_first_publication_translation_v1';
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
const NEWS_TABLE = 'app_newsflashes';
const ARTICLES_TABLE = 'app_articles';

// Chinese-first product policy:
// - Never auto-generate Chinese -> English (including titles).
// - English titles may be warmed to Chinese for recent public content.
// - Short English bodies may be warmed to Chinese; long bodies are on-demand only.
// - The 16k+ historical news archive is never bulk translated.
const NEWS_SCAN_LIMIT = Math.max(80, Math.min(300, Number(process.env.KAKA_TRANSLATION_NEWS_SCAN_LIMIT || 180) || 180));
const NEWS_TITLE_PER_TICK = Math.max(1, Math.min(8, Number(process.env.KAKA_TRANSLATION_NEWS_TITLE_PER_TICK || 4) || 4));
const NEWS_SHORT_BODY_CHARS = Math.max(200, Math.min(2000, Number(process.env.KAKA_TRANSLATION_NEWS_AUTO_SHORT_BODY_CHARS || 900) || 900));
const NEWS_BODY_PER_TICK = Math.max(0, Math.min(2, Number(process.env.KAKA_TRANSLATION_NEWS_BODY_PER_TICK || 1) || 1));
const ARTICLE_SCAN_LIMIT = Math.max(9, Math.min(50, Number(process.env.KAKA_TRANSLATION_ARTICLE_SCAN_LIMIT || 30) || 30));
const ARTICLE_TITLE_PER_TICK = Math.max(1, Math.min(3, Number(process.env.KAKA_TRANSLATION_ARTICLE_TITLE_PER_TICK || 1) || 1));
const ARTICLE_SHORT_BODY_CHARS = Math.max(300, Math.min(4000, Number(process.env.KAKA_TRANSLATION_ARTICLE_AUTO_SHORT_BODY_CHARS || 1200) || 1200));
const ARTICLE_BODY_PER_TICK = Math.max(0, Math.min(2, Number(process.env.KAKA_TRANSLATION_ARTICLE_BODY_PER_TICK || 1) || 1));
const TICK_MS = Math.max(15_000, Math.min(10 * 60_000, Number(process.env.KAKA_TRANSLATION_PUBLICATION_TICK_MS || 60_000) || 60_000));
const IDLE_TICK_MS = Math.max(TICK_MS, Math.min(15 * 60_000, Number(process.env.KAKA_TRANSLATION_PUBLICATION_IDLE_TICK_MS || 180_000) || 180_000));
const OFFICIAL_ENGLISH_NEWS_SOURCE_KEYS = Object.freeze([
  'sec_official_press',
  'cftc_official_enforcement',
  'cftc_official_general',
  'ethereum_foundation_blog',
]);
const OFFICIAL_ENGLISH_NEWS_SCAN_LIMIT = Math.max(20, Math.min(100, Number(process.env.KAKA_TRANSLATION_OFFICIAL_EN_NEWS_SCAN_LIMIT || 80) || 80));
const OFFICIAL_ENGLISH_NEWS_TITLE_PER_TICK = Math.max(1, Math.min(8, Number(process.env.KAKA_TRANSLATION_OFFICIAL_EN_NEWS_TITLE_PER_TICK || 6) || 6));
const OFFICIAL_ENGLISH_NEWS_BODY_MAX_CHARS = Math.max(900, Math.min(2400, Number(process.env.KAKA_TRANSLATION_OFFICIAL_EN_NEWS_BODY_MAX_CHARS || 1600) || 1600));
const OFFICIAL_ENGLISH_NEWS_BODY_PER_TICK = Math.max(0, Math.min(3, Number(process.env.KAKA_TRANSLATION_OFFICIAL_EN_NEWS_BODY_PER_TICK || 2) || 2));

const state = {
  schema: SCHEMA,
  started: false,
  enabled: true,
  supabase_configured: Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY),
  policy: 'english_to_chinese_title_plus_short_body_auto_long_body_on_demand',
  chinese_to_english_auto: false,
  auto_target_locale: 'zh',
  recent_news_scan_limit: NEWS_SCAN_LIMIT,
  news_short_body_max_chars: NEWS_SHORT_BODY_CHARS,
  public_article_scan_limit: ARTICLE_SCAN_LIMIT,
  article_short_body_max_chars: ARTICLE_SHORT_BODY_CHARS,
  public_published_articles_only: true,
  active_tick_ms: TICK_MS,
  idle_tick_ms: IDLE_TICK_MS,
  runs: 0,
  scan_reads: 0,
  db_writes: 0,
  news_title_rows_translated: 0,
  news_short_body_rows_translated: 0,
  article_title_rows_translated: 0,
  article_short_body_rows_translated: 0,
  failures: 0,
  last_run_at: null,
  last_success_at: null,
  last_error: null,
  pending_news_titles: 0,
  pending_news_short_bodies: 0,
  official_english_source_keys: OFFICIAL_ENGLISH_NEWS_SOURCE_KEYS,
  official_english_body_max_chars: OFFICIAL_ENGLISH_NEWS_BODY_MAX_CHARS,
  pending_official_english_titles: 0,
  pending_official_english_short_bodies: 0,
  pending_article_titles: 0,
  pending_article_short_bodies: 0,
};

let timer = null;
let inFlight = false;
let stopping = false;

function text(v) { return String(v ?? '').trim(); }
function chars(v) { return Array.from(text(v)).length; }
function nowIso() { return new Date().toISOString(); }
function authHeaders(extra = {}) {
  return {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    'content-type': 'application/json',
    ...extra,
  };
}
async function supabaseFetch(path, init = {}) {
  if (!state.supabase_configured) throw new Error('publication_translation_supabase_not_configured');
  const response = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
    ...init,
    headers: { ...authHeaders(), ...(init.headers || {}) },
  });
  const raw = await response.text();
  let payload = null;
  try { payload = raw ? JSON.parse(raw) : null; } catch (_) { payload = raw; }
  if (!response.ok) throw new Error(`publication_translation_supabase_http_${response.status}:${text(raw).slice(0, 240)}`);
  return payload;
}
function newsFields(row) { return { title: text(row?.title), content: text(row?.content) }; }
function articleFields(row) { return { title: text(row?.title), summary: text(row?.summary), content: text(row?.content) }; }
function isPublicPublishedArticle(row) {
  if (text(row?.status).toLowerCase() !== 'published') return false;
  const visibility = text(row?.visibility_type).toLowerCase();
  return !visibility || visibility === 'public';
}
function needs(row, fields, requiredFields) {
  return translationNeedsWork({
    fields: fields(row),
    existingTranslations: row?.translations,
    existingSourceHash: text(row?.translation_source_hash),
    requiredFields,
    targetLocale: 'zh',
  });
}
async function loadNews() {
  state.scan_reads++;
  const rows = await supabaseFetch(`${NEWS_TABLE}?is_active=eq.true&lifecycle_status=eq.active&select=id,title,content,translations,translation_source_hash,translation_updated_at,published_at,updated_at&order=sort_order.asc,published_at.desc&limit=${NEWS_SCAN_LIMIT}`);
  return Array.isArray(rows) ? rows : [];
}
async function loadOfficialEnglishNews() {
  state.scan_reads++;
  const sourceFilter = OFFICIAL_ENGLISH_NEWS_SOURCE_KEYS.join(',');
  const rows = await supabaseFetch(`${NEWS_TABLE}?is_active=eq.true&lifecycle_status=eq.active&primary_source_key=in.(${sourceFilter})&select=id,title,content,translations,translation_source_hash,translation_updated_at,published_at,updated_at,primary_source_key&order=published_at.desc&limit=${OFFICIAL_ENGLISH_NEWS_SCAN_LIMIT}`);
  return Array.isArray(rows) ? rows : [];
}
async function loadArticles() {
  state.scan_reads++;
  const rows = await supabaseFetch(`${ARTICLES_TABLE}?status=eq.published&select=id,title,summary,content,status,visibility_type,translations,translation_source_hash,translation_updated_at,created_at,edited_by_user_at&order=is_top.desc,created_at.desc&limit=${ARTICLE_SCAN_LIMIT}`);
  return (Array.isArray(rows) ? rows : []).filter(isPublicPublishedArticle);
}
async function persist(table, id, result) {
  const updatedAt = nowIso();
  await supabaseFetch(`${table}?id=eq.${encodeURIComponent(id)}`, {
    method: 'PATCH',
    headers: { Prefer: 'return=minimal' },
    body: JSON.stringify({
      translations: result.translations,
      translation_source_hash: result.source_hash,
      translation_updated_at: updatedAt,
    }),
  });
  state.db_writes++;
  state.last_success_at = updatedAt;
  state.last_error = null;
}
async function translateRow({ table, row, fields, onlyFields, maxSourceChars, counter }) {
  const id = text(row?.id);
  if (!id) return false;
  const result = await translateContentFields({
    fields: fields(row),
    existingTranslations: row?.translations,
    existingSourceHash: text(row?.translation_source_hash),
    onlyFields,
    maxSourceChars,
    budgetBucket: 'auto_zh',
    targetLocale: 'zh',
  });
  if (!result.changed || result.translated_fields < 1) return false;
  await persist(table, id, result);
  state[counter]++;
  return true;
}
function isBudgetStop(error) {
  return ['TRANSLATION_COOLDOWN', 'TRANSLATION_DAILY_BUDGET', 'TRANSLATION_AUTO_DAILY_BUDGET', 'TRANSLATION_BUCKET_BUDGET'].includes(String(error?.code || ''));
}
async function runOnce() {
  if (inFlight || stopping || !state.enabled || !state.supabase_configured) return false;
  const service = getSharedTranslationHealth();
  if (!service.configured) { state.last_error = 'translation_provider_not_configured:KAKA_GOOGLE_TRANSLATION_API_KEY'; return false; }
  if (service.daily_budget_ready === false || service.auto_daily_budget_ready === false) { state.last_error = 'translation_auto_zh_budget_locked'; return false; }
  inFlight = true;
  state.runs++;
  state.last_run_at = nowIso();
  try {
    const [newsRows, articleRows, officialEnglishRows] = await Promise.all([loadNews(), loadArticles(), loadOfficialEnglishNews()]);
    const officialIds = new Set(officialEnglishRows.map((row) => text(row?.id)).filter(Boolean));
    const pendingOfficialEnglishTitles = officialEnglishRows.filter((row) => needs(row, newsFields, ['title']));
    const pendingNewsTitles = newsRows.filter((row) => !officialIds.has(text(row?.id)) && needs(row, newsFields, ['title']));
    const pendingArticleTitles = articleRows.filter((row) => needs(row, articleFields, ['title']));
    const shortOfficialEnglishRows = officialEnglishRows.filter((row) => chars(row?.content) > 0 && chars(row?.content) <= OFFICIAL_ENGLISH_NEWS_BODY_MAX_CHARS);
    const shortNewsRows = newsRows.filter((row) => !officialIds.has(text(row?.id)) && chars(row?.content) > 0 && chars(row?.content) <= NEWS_SHORT_BODY_CHARS);
    const shortArticleRows = articleRows.filter((row) => {
      const bodyChars = chars(row?.summary) + chars(row?.content);
      return bodyChars > 0 && bodyChars <= ARTICLE_SHORT_BODY_CHARS;
    });
    const pendingOfficialEnglishBodies = shortOfficialEnglishRows.filter((row) => needs(row, newsFields, ['content']));
    const pendingNewsBodies = shortNewsRows.filter((row) => needs(row, newsFields, ['content']));
    const pendingArticleBodies = shortArticleRows.filter((row) => needs(row, articleFields, ['summary', 'content']));
    state.pending_news_titles = pendingNewsTitles.length;
    state.pending_news_short_bodies = pendingNewsBodies.length;
    state.pending_official_english_titles = pendingOfficialEnglishTitles.length;
    state.pending_official_english_short_bodies = pendingOfficialEnglishBodies.length;
    state.pending_article_titles = pendingArticleTitles.length;
    state.pending_article_short_bodies = pendingArticleBodies.length;

    // Tiny first-party English feeds get their own bounded priority lane so SEC/CFTC/EF
    // never sit behind hundreds of already-Chinese high-frequency newsflashes.
    for (const row of pendingOfficialEnglishTitles.slice(0, OFFICIAL_ENGLISH_NEWS_TITLE_PER_TICK)) {
      try {
        await translateRow({ table: NEWS_TABLE, row, fields: newsFields, onlyFields: ['title'], maxSourceChars: 700, counter: 'news_title_rows_translated' });
      } catch (error) {
        state.failures++; state.last_error = String(error?.message || error); if (isBudgetStop(error)) break;
      }
    }

    // Titles first. This makes list browsing useful in Chinese while keeping bodies cheap.
    for (const row of pendingNewsTitles.slice(0, NEWS_TITLE_PER_TICK)) {
      try {
        await translateRow({ table: NEWS_TABLE, row, fields: newsFields, onlyFields: ['title'], maxSourceChars: 700, counter: 'news_title_rows_translated' });
      } catch (error) {
        state.failures++; state.last_error = String(error?.message || error); if (isBudgetStop(error)) break;
      }
    }
    for (const row of pendingArticleTitles.slice(0, ARTICLE_TITLE_PER_TICK)) {
      try {
        await translateRow({ table: ARTICLES_TABLE, row, fields: articleFields, onlyFields: ['title'], maxSourceChars: 700, counter: 'article_title_rows_translated' });
      } catch (error) {
        state.failures++; state.last_error = String(error?.message || error); if (isBudgetStop(error)) break;
      }
    }

    // First-party captured RSS bodies are bounded (currently <=1200 chars), so this dedicated lane can auto-translate them too.
    if (pendingOfficialEnglishTitles.length === 0 && OFFICIAL_ENGLISH_NEWS_BODY_PER_TICK > 0) {
      for (const row of pendingOfficialEnglishBodies.slice(0, OFFICIAL_ENGLISH_NEWS_BODY_PER_TICK)) {
        try {
          await translateRow({ table: NEWS_TABLE, row, fields: newsFields, onlyFields: ['content'], maxSourceChars: OFFICIAL_ENGLISH_NEWS_BODY_MAX_CHARS, counter: 'news_short_body_rows_translated' });
        } catch (error) {
          state.failures++; state.last_error = String(error?.message || error); if (isBudgetStop(error)) break;
        }
      }
    }
    if (pendingNewsTitles.length === 0 && NEWS_BODY_PER_TICK > 0) {
      for (const row of pendingNewsBodies.slice(0, NEWS_BODY_PER_TICK)) {
        try {
          await translateRow({ table: NEWS_TABLE, row, fields: newsFields, onlyFields: ['content'], maxSourceChars: NEWS_SHORT_BODY_CHARS, counter: 'news_short_body_rows_translated' });
        } catch (error) {
          state.failures++; state.last_error = String(error?.message || error); if (isBudgetStop(error)) break;
        }
      }
    }
    if (pendingArticleTitles.length === 0 && ARTICLE_BODY_PER_TICK > 0) {
      for (const row of pendingArticleBodies.slice(0, ARTICLE_BODY_PER_TICK)) {
        try {
          await translateRow({ table: ARTICLES_TABLE, row, fields: articleFields, onlyFields: ['summary', 'content'], maxSourceChars: ARTICLE_SHORT_BODY_CHARS, counter: 'article_short_body_rows_translated' });
        } catch (error) {
          state.failures++; state.last_error = String(error?.message || error); if (isBudgetStop(error)) break;
        }
      }
    }
    return true;
  } finally {
    inFlight = false;
  }
}
function schedule(delayMs = TICK_MS) {
  if (stopping || !state.enabled) return;
  if (timer) clearTimeout(timer);
  timer = setTimeout(async () => {
    try { await runOnce(); }
    finally {
      if (!stopping) {
        const backlog = state.pending_official_english_titles > 0 || state.pending_official_english_short_bodies > 0 || state.pending_news_titles > 0 || state.pending_news_short_bodies > 0 || state.pending_article_titles > 0 || state.pending_article_short_bodies > 0;
        schedule(backlog ? TICK_MS : IDLE_TICK_MS);
      }
    }
  }, Math.max(5_000, delayMs));
  timer.unref?.();
}
export function getContentPublicationTranslationHealth() {
  return { ...state, in_flight: inFlight, translation_service: getSharedTranslationHealth() };
}
export function startContentPublicationTranslation() {
  if (state.started) return;
  state.started = true;
  stopping = false;
  if (!state.supabase_configured) { state.last_error = 'publication_translation_supabase_not_configured'; return; }
  schedule(7_000);
}
export function stopContentPublicationTranslation() {
  stopping = true;
  if (timer) clearTimeout(timer);
  timer = null;
}
export async function handleContentPublicationTranslation(req, res, url) {
  if (url.pathname !== '/api/content-translation/health') return false;
  if (req.method !== 'GET') {
    res.writeHead(405, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ ok: false, error: 'method_not_allowed' }));
    return true;
  }
  res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
  res.end(JSON.stringify({ ok: true, ...getContentPublicationTranslationHealth() }));
  return true;
}
