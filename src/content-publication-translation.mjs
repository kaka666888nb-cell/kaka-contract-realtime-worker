import {
  contentTranslationHash,
  getSharedTranslationHealth,
  translateContentFields,
  translationNeedsWork,
} from './content-translation.mjs';

const SCHEMA = 'step1045_6_1_publication_shared_translation_v1';
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
const NEWS_TABLE = 'app_newsflashes';
const ARTICLES_TABLE = 'app_articles';

// Product rule: do not translate the 16k+ historical news archive. Keep only a bounded
// recent title window warm. Full news bodies are deliberately much smaller so the
// external 15k/day hard quota can remain a real cost guard.
const NEWS_SCAN_LIMIT = Math.max(80, Math.min(300, Number(process.env.KAKA_TRANSLATION_NEWS_SCAN_LIMIT || 180) || 180));
const NEWS_TITLE_PER_TICK = Math.max(1, Math.min(10, Number(process.env.KAKA_TRANSLATION_NEWS_TITLE_PER_TICK || 5) || 5));
const NEWS_BODY_BACKGROUND_LIMIT = Math.max(0, Math.min(24, Number(process.env.KAKA_TRANSLATION_NEWS_BODY_BACKGROUND_LIMIT || 8) || 8));
const NEWS_BODY_PER_TICK = Math.max(0, Math.min(2, Number(process.env.KAKA_TRANSLATION_NEWS_BODY_PER_TICK || 1) || 1));
const ARTICLE_SCAN_LIMIT = Math.max(9, Math.min(50, Number(process.env.KAKA_TRANSLATION_ARTICLE_SCAN_LIMIT || 30) || 30));
const ARTICLES_PER_TICK = Math.max(1, Math.min(3, Number(process.env.KAKA_TRANSLATION_ARTICLES_PER_TICK || 1) || 1));
const TICK_MS = Math.max(15_000, Math.min(10 * 60_000, Number(process.env.KAKA_TRANSLATION_PUBLICATION_TICK_MS || 60_000) || 60_000));
const IDLE_TICK_MS = Math.max(TICK_MS, Math.min(15 * 60_000, Number(process.env.KAKA_TRANSLATION_PUBLICATION_IDLE_TICK_MS || 180_000) || 180_000));

const state = {
  schema: SCHEMA,
  started: false,
  enabled: true,
  supabase_configured: Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY),
  shared_background_only: true,
  user_translation_requests: 0,
  user_supabase_requests: 0,
  recent_news_scan_limit: NEWS_SCAN_LIMIT,
  recent_news_title_only_default: true,
  recent_news_body_background_limit: NEWS_BODY_BACKGROUND_LIMIT,
  public_article_scan_limit: ARTICLE_SCAN_LIMIT,
  public_published_articles_only: true,
  active_tick_ms: TICK_MS,
  idle_tick_ms: IDLE_TICK_MS,
  runs: 0,
  scan_reads: 0,
  db_writes: 0,
  news_rows_translated: 0,
  news_title_rows_translated: 0,
  news_body_rows_translated: 0,
  article_rows_translated: 0,
  failures: 0,
  last_run_at: null,
  last_success_at: null,
  last_error: null,
  last_news_id: null,
  last_article_id: null,
  pending_news_titles: 0,
  pending_news_bodies: 0,
  pending_articles: 0,
};

let timer = null;
let inFlight = false;
let stopping = false;

function text(v) { return String(v ?? '').trim(); }
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
function newsFields(row) {
  return { title: text(row?.title), content: text(row?.content) };
}
function articleFields(row) {
  return { title: text(row?.title), summary: text(row?.summary), content: text(row?.content) };
}
function isPublicPublishedArticle(row) {
  if (text(row?.status).toLowerCase() !== 'published') return false;
  const visibility = text(row?.visibility_type).toLowerCase();
  return !visibility || visibility === 'public';
}
function pending(row, fields, requiredFields = null) {
  return translationNeedsWork({
    fields: fields(row),
    existingTranslations: row?.translations,
    existingSourceHash: text(row?.translation_source_hash),
    requiredFields,
  });
}

async function loadNews() {
  state.scan_reads++;
  const rows = await supabaseFetch(
    `${NEWS_TABLE}?is_active=eq.true&select=id,title,content,translations,translation_source_hash,translation_updated_at,published_at,updated_at&order=sort_order.asc,published_at.desc&limit=${NEWS_SCAN_LIMIT}`,
  );
  return Array.isArray(rows) ? rows : [];
}
async function loadArticles() {
  state.scan_reads++;
  const rows = await supabaseFetch(
    `${ARTICLES_TABLE}?status=eq.published&select=id,title,summary,content,status,visibility_type,translations,translation_source_hash,translation_updated_at,created_at,edited_by_user_at&order=is_top.desc,created_at.desc&limit=${ARTICLE_SCAN_LIMIT}`,
  );
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
  return updatedAt;
}

async function translateRow({ table, row, fields, kind, onlyFields = null, maxSourceChars = Number.POSITIVE_INFINITY }) {
  const id = text(row?.id);
  if (!id) return false;
  const result = await translateContentFields({
    fields: fields(row),
    existingTranslations: row?.translations,
    existingSourceHash: text(row?.translation_source_hash),
    onlyFields,
    maxSourceChars,
  });
  if (!result.changed || result.translated_fields < 1) return false;
  await persist(table, id, result);
  if (kind === 'news_title') {
    state.news_rows_translated++;
    state.news_title_rows_translated++;
    state.last_news_id = id;
  } else if (kind === 'news_body') {
    state.news_rows_translated++;
    state.news_body_rows_translated++;
    state.last_news_id = id;
  } else {
    state.article_rows_translated++;
    state.last_article_id = id;
  }
  return true;
}

async function runOnce() {
  if (inFlight || stopping || !state.enabled || !state.supabase_configured) return false;
  const service = getSharedTranslationHealth();
  if (!service.configured) {
    state.last_error = 'translation_provider_not_configured:KAKA_GOOGLE_TRANSLATION_API_KEY';
    return false;
  }
  inFlight = true;
  state.runs++;
  state.last_run_at = nowIso();
  try {
    const [newsRows, articleRows] = await Promise.all([loadNews(), loadArticles()]);
    const pendingArticles = articleRows.filter((row) => pending(row, articleFields));
    const pendingNewsTitles = newsRows.filter((row) => pending(row, newsFields, ['title']));
    const bodyWindow = NEWS_BODY_BACKGROUND_LIMIT > 0 ? newsRows.slice(0, NEWS_BODY_BACKGROUND_LIMIT) : [];
    const pendingNewsBodies = bodyWindow.filter((row) => pending(row, newsFields, ['content']));
    state.pending_articles = pendingArticles.length;
    state.pending_news_titles = pendingNewsTitles.length;
    state.pending_news_bodies = pendingNewsBodies.length;

    // Public articles are few and bounded; reserve one slot so article bilingual mode
    // never waits behind a continuously-arriving news stream.
    for (const row of pendingArticles.slice(0, ARTICLES_PER_TICK)) {
      try {
        await translateRow({ table: ARTICLES_TABLE, row, fields: articleFields, kind: 'article', maxSourceChars: 12_000 });
      } catch (error) {
        state.failures++;
        state.last_error = String(error?.name === 'AbortError' ? 'translation_timeout' : error?.message || error);
        break;
      }
    }

    // News: title-first. Current production volume is ~12k title characters/day,
    // which fits the user's 15k/day Google hard quota far better than translating
    // every historical/full body (~43k source chars/day).
    for (const row of pendingNewsTitles.slice(0, NEWS_TITLE_PER_TICK)) {
      try {
        await translateRow({ table: NEWS_TABLE, row, fields: newsFields, kind: 'news_title', onlyFields: ['title'], maxSourceChars: 600 });
      } catch (error) {
        state.failures++;
        state.last_error = String(error?.name === 'AbortError' ? 'translation_timeout' : error?.message || error);
        break;
      }
    }

    // Only when the recent title queue is caught up do we spend spare quota on a
    // tiny latest-body window. This prevents 16k historical rows from draining quota.
    if (pendingNewsTitles.length === 0 && NEWS_BODY_PER_TICK > 0) {
      for (const row of pendingNewsBodies.slice(0, NEWS_BODY_PER_TICK)) {
        try {
          await translateRow({ table: NEWS_TABLE, row, fields: newsFields, kind: 'news_body', onlyFields: ['content'], maxSourceChars: 1200 });
        } catch (error) {
          state.failures++;
          state.last_error = String(error?.name === 'AbortError' ? 'translation_timeout' : error?.message || error);
          break;
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
        const hasBacklog = state.pending_news_titles > 0 || state.pending_news_bodies > 0 || state.pending_articles > 0;
        schedule(hasBacklog ? TICK_MS : IDLE_TICK_MS);
      }
    }
  }, Math.max(5_000, delayMs));
  timer.unref?.();
}

export function getContentPublicationTranslationHealth() {
  return {
    ...state,
    in_flight: inFlight,
    translation_service: getSharedTranslationHealth(),
  };
}
export function startContentPublicationTranslation() {
  if (state.started) return;
  state.started = true;
  stopping = false;
  if (!state.supabase_configured) {
    state.last_error = 'publication_translation_supabase_not_configured';
    return;
  }
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
