import { contentTranslationHash, getSharedTranslationHealth, translateContentFields, translationNeedsWork } from './content-translation.mjs';
const STEP_SCHEMA = 'step1047_content_lifecycle_airdrop_v1';
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
const EVENTS_TABLE = 'app_airdrop_events';
const SOCIAL_EVENTS_TABLE = 'app_social_watch_events';
const DEFAULT_REFRESH_MS = 10 * 60_000;
const MIN_REFRESH_MS = 3 * 60_000;
const MAX_REFRESH_MS = 60 * 60_000;
const SOURCE_TIMEOUT_MS = 12_000;
const DETAIL_TIMEOUT_MS = 10_000;
const MAX_DETAIL_FETCHES_PER_SOURCE = 0;
const CONTENT_ENRICH_FETCHES_PER_SOURCE = 3;
const CONTENT_TEXT_MAX_CHARS = 24_000;
const CONTENT_MIN_CHARS = 80;
const BYBIT_STRUCTURED_CONTENT_MIN_CHARS = 20;
const CONTENT_RETRY_MS = 6 * 60 * 60_000;
const PUBLIC_CACHE_LIMIT = 600;
const PUBLIC_ENDPOINT_MAX_LIMIT = 200;
const DB_RELOAD_MS = 5 * 60_000;
const PERSIST_HEARTBEAT_MS = Math.max(60 * 60_000, Number(process.env.KAKA_AIRDROP_PERSIST_HEARTBEAT_MS || 6 * 60 * 60_000) || 6 * 60 * 60_000);
const HTML_MAX_BYTES = 3 * 1024 * 1024;
const TRANSLATION_TICK_MS = 60_000;
const TRANSLATION_ROWS_PER_TICK = 5;
const TRANSLATION_DETAIL_ROWS_PER_TICK = 2;
const AUTO_SHORT_DETAIL_CHARS = Math.max(200, Math.min(2000, Number(process.env.KAKA_AIRDROP_AUTO_SHORT_DETAIL_CHARS || 900) || 900));
const TRANSLATION_WINDOW_ROWS = Math.max(25, Math.min(100, Number(process.env.KAKA_AIRDROP_TRANSLATION_WINDOW_ROWS || 60) || 60));

const OFFICIAL_SOURCES = Object.freeze([
  {
    id: 'binance',
    provider: 'binance',
    display_name: 'Binance',
    // Binance announcement HTML is Cloudflare-challenged from Render (HTTP 202).
    // Reuse the existing official X filtered-stream collector instead of scraping
    // or relying on undocumented Binance BAPI endpoints.
    social_x_handles: ['binance', 'BinanceWallet'],
    allowed_hosts: ['x.com', 'www.x.com'],
    include: [
      /alpha.{0,100}(airdrop|box|points|tge|token|reward|competition|campaign)/i,
      /hodler\s*airdrops?/i,
      /launchpool/i,
      /token\s+generation\s+event/i,
      /\btge\b/i,
      /\bairdrop\b/i,
    ],
    exclude: [/delist/i, /maintenance/i],
  },
  {
    id: 'okx',
    provider: 'okx',
    display_name: 'OKX',
    roots: [
      'https://www.okx.com/zh-hans/help/section/latest-events',
      'https://www.okx.com/zh-hans/help/section/announcements-jumpstart',
    ],
    allowed_hosts: ['okx.com', 'www.okx.com'],
    include: [
      /jumpstart/i,
      /空投/i,
      /奖励/i,
      /瓜分/i,
      /活动/i,
      /airdrop/i,
      /reward/i,
    ],
    exclude: [/vip\s*直升/i, /费率/i, /maintenance/i, /维护/i],
  },
  {
    id: 'bybit',
    provider: 'bybit',
    display_name: 'Bybit',
    json_roots: [
      'https://api.bybit.com/v5/announcements/index?locale=en-US&type=latest_activities&limit=50',
    ],
    roots: ['https://announcements.bybit.com/en/'],
    allowed_hosts: ['api.bybit.com', 'announcements.bybit.com', 'bybit.com', 'www.bybit.com'],
    include: [
      /launchpool/i,
      /alpha.*(airdrop|reward|points?|quest|competition)/i,
      /(airdrop|reward|prize pool|token splash)/i,
    ],
    exclude: [/removal/i, /delist/i, /maintenance/i],
  },
  {
    id: 'bitget',
    provider: 'bitget',
    display_name: 'Bitget',
    roots: [
      'https://www.bitget.com/support/sections/4413154768537',
      'https://www.bitget.com/support/',
    ],
    allowed_hosts: ['bitget.com', 'www.bitget.com'],
    include: [
      /poolx/i,
      /candybomb/i,
      /launchpool/i,
      /\bairdrop\b/i,
      /空投/i,
    ],
    exclude: [/maintenance/i, /delist/i],
  },
  {
    id: 'gate',
    provider: 'gate',
    display_name: 'Gate',
    roots: [
      'https://www.gate.com/zh/announcements/latest',
      'https://www.gate.com/zh/announcements',
      'https://www.gate.com/announcements',
    ],
    allowed_hosts: ['gate.com', 'www.gate.com'],
    include: [
      /hodler\s*airdrop/i,
      /candydrop/i,
      /launchpool/i,
      /vip\s*airdrop/i,
      /空投/i,
    ],
    exclude: [/双周报/i, /biweekly/i, /maintenance/i],
  },
]);

const state = {
  started: false,
  enabled: String(process.env.KAKA_AIRDROP_WATCH_ENABLED || '1') !== '0',
  supabaseConfigured: Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY),
  refreshMs: clampInt(process.env.KAKA_AIRDROP_REFRESH_SECONDS, MIN_REFRESH_MS / 1000, MAX_REFRESH_MS / 1000, DEFAULT_REFRESH_MS / 1000) * 1000,
  refreshes: 0,
  refreshFailures: 0,
  refreshInFlight: false,
  lastRefreshStartedAt: null,
  lastRefreshCompletedAt: null,
  lastPersistAt: null,
  lastDbLoadAt: null,
  lastError: null,
  publicReads: 0,
  publicSnapshotLoaded: false,
  publicSnapshotRows: 0,
  publicSnapshotDbReads: 0,
  sharedBridgeReads: 0,
  upstreamRequests: 0,
  upstreamNotModified: 0,
  upstreamFailures: 0,
  detailRequests: 0,
  contentEnrichRequests: 0,
  contentEnrichFailures: 0,
  contentRows: 0,
  detailReads: 0,
  persistedRows: 0,
  persistSuccesses: 0,
  persistFailures: 0,
  persistRowsConsidered: 0,
  persistRowsWritten: 0,
  persistRowsNoopSkipped: 0,
  persistHeartbeatRows: 0,
  persistNoopBatches: 0,
  translationRuns: 0,
  translationRowsTranslated: 0,
  translationTitleRowsTranslated: 0,
  translationDetailRowsTranslated: 0,
  translationFailures: 0,
  translationDbWrites: 0,
  translationLastRunAt: null,
  translationLastSuccessAt: null,
  translationLastError: null,
  sourceStates: {},
};

let timer = null;
let translationTimer = null;
let translationBackfillInFlight = false;
let stopping = false;
let publicSnapshot = [];
let dbLoadedAtMs = 0;
let refreshPromise = null;
const conditionalHeaders = new Map();
const contentRetryAfter = new Map();

function nowIso() { return new Date().toISOString(); }
const POSTGRES_NUL = String.fromCharCode(0);
function postgresSafeString(v) { return String(v ?? '').split(POSTGRES_NUL).join(''); }
function text(v) { return postgresSafeString(v).trim(); }
function sanitizePostgresJsonValue(value) {
  if (typeof value === 'string') return postgresSafeString(value);
  if (Array.isArray(value)) return value.map(sanitizePostgresJsonValue);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [
      postgresSafeString(key),
      sanitizePostgresJsonValue(item),
    ]));
  }
  return value;
}
function finiteNumber(v, fallback = 0) { const n = Number(v); return Number.isFinite(n) ? n : fallback; }
function clampInt(v, min, max, fallback) {
  const n = Math.trunc(finiteNumber(v, fallback));
  return Math.max(min, Math.min(max, n));
}
function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }
function json(res, status, body, { cacheSeconds = 0 } = {}) {
  const headers = { 'content-type': 'application/json; charset=utf-8' };
  if (cacheSeconds > 0) {
    headers['cache-control'] = `public, max-age=${cacheSeconds}, stale-while-revalidate=${Math.max(cacheSeconds * 4, 60)}`;
    headers['cdn-cache-control'] = `public, max-age=${cacheSeconds}`;
  } else {
    headers['cache-control'] = 'no-store';
  }
  res.writeHead(status, headers);
  res.end(JSON.stringify(body));
}
function authHeaders(extra = {}) {
  return {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    'content-type': 'application/json',
    ...extra,
  };
}
function normalizeWhitespace(v) { return text(v).replace(/\s+/g, ' ').trim(); }
function decodeHtmlEntities(raw) {
  return text(raw)
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&#x([0-9a-f]+);/gi, (_, hex) => String.fromCodePoint(parseInt(hex, 16)))
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(parseInt(n, 10)));
}
function stripHtml(raw) {
  return normalizeWhitespace(decodeHtmlEntities(text(raw)
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, ' ')
    .replace(/<noscript\b[^>]*>[\s\S]*?<\/noscript>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')));
}
function htmlArticleText(raw) {
  const html = text(raw);
  if (!html) return '';
  // Prefer structured articleBody when the official publisher exposes it.
  for (const match of html.matchAll(/"articleBody"\s*:\s*"((?:\\.|[^"\\])*)"/gi)) {
    try {
      const decoded = JSON.parse(`"${match[1]}"`);
      const value = normalizeContentText(decoded);
      if (value.length >= CONTENT_MIN_CHARS) return value.slice(0, CONTENT_TEXT_MAX_CHARS);
    } catch (_) {}
  }
  let body = html;
  const article = html.match(/<article\b[^>]*>([\s\S]*?)<\/article>/i);
  const main = html.match(/<main\b[^>]*>([\s\S]*?)<\/main>/i);
  if (article?.[1]) body = article[1];
  else if (main?.[1]) body = main[1];
  body = body
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, '\n')
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, '\n')
    .replace(/<noscript\b[^>]*>[\s\S]*?<\/noscript>/gi, '\n')
    .replace(/<nav\b[^>]*>[\s\S]*?<\/nav>/gi, '\n')
    .replace(/<footer\b[^>]*>[\s\S]*?<\/footer>/gi, '\n')
    .replace(/<header\b[^>]*>[\s\S]*?<\/header>/gi, '\n')
    .replace(/<(?:br|hr)\b[^>]*>/gi, '\n')
    .replace(/<\/(?:p|div|li|h[1-6]|tr|section)>/gi, '\n')
    .replace(/<li\b[^>]*>/gi, '• ')
    .replace(/<[^>]+>/g, ' ');
  return normalizeContentText(decodeHtmlEntities(body)).slice(0, CONTENT_TEXT_MAX_CHARS);
}
function normalizeContentText(raw) {
  return text(raw)
    .replace(/\r/g, '')
    .replace(/[ \t]+/g, ' ')
    .replace(/ *\n */g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}
function contentUsable(value) {
  const v = normalizeContentText(value);
  if (v.length < CONTENT_MIN_CHARS) return false;
  if (/captcha|access denied|cloudflare|enable javascript|verify you are human/i.test(v.slice(0, 800))) return false;
  return true;
}
function contentUsableForProvider(provider, value) {
  const v = normalizeContentText(value);
  const minChars = text(provider).toLowerCase() === 'bybit'
    ? BYBIT_STRUCTURED_CONTENT_MIN_CHARS
    : CONTENT_MIN_CHARS;
  if (v.length < minChars) return false;
  if (/captcha|access denied|cloudflare|enable javascript|verify you are human/i.test(v.slice(0, 800))) return false;
  return true;
}
function safeUrl(raw, baseUrl) {
  const value = decodeHtmlEntities(text(raw)).replace(/\\u002F/gi, '/').replace(/\\\//g, '/');
  if (!value || value.startsWith('javascript:') || value.startsWith('#')) return '';
  try {
    const u = new URL(value, baseUrl);
    if (!['http:', 'https:'].includes(u.protocol)) return '';
    u.hash = '';
    return u.toString();
  } catch (_) { return ''; }
}
function normalizeContentMediaItems(raw) {
  if (!Array.isArray(raw)) return [];
  const seen = new Set();
  const rows = [];
  for (const value of raw) {
    if (!value || typeof value !== 'object') continue;
    const kind = text(value.kind || value.type).toLowerCase() || 'image';
    const url = text(value.url || value.media_url);
    const previewUrl = text(value.preview_url || value.preview_image_url || value.thumbnail_url) || url;
    if (!url && !previewUrl) continue;
    const key = `${kind}|${url}|${previewUrl}`;
    if (seen.has(key)) continue;
    seen.add(key);
    rows.push({ ...value, kind, url, preview_url: previewUrl });
    if (rows.length >= 8) break;
  }
  return rows;
}
function officialPageMediaItems(html, baseUrl) {
  const urls = [];
  const add = (raw) => {
    const urlValue = safeUrl(raw, baseUrl);
    if (!urlValue || urls.includes(urlValue)) return;
    urls.push(urlValue);
  };
  for (const match of text(html).matchAll(/<meta\b([^>]*?)>/gi)) {
    const attrs = match[1] || '';
    const key = text((attrs.match(/\b(?:property|name)=["']([^"']+)["']/i) || [])[1]).toLowerCase();
    const content = (attrs.match(/\bcontent=["']([^"']+)["']/i) || [])[1] || '';
    if (['og:image','og:image:url','twitter:image','twitter:image:src'].includes(key)) add(content);
  }
  for (const match of text(html).matchAll(/"image"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"/gi)) {
    add(match[1].replace(/\\\//g, '/'));
  }
  return urls.slice(0, 4).map((url) => ({
    kind: 'image', url, preview_url: url, source: 'official_page_metadata',
  }));
}

function hostAllowed(urlValue, source) {
  try {
    const host = new URL(urlValue).hostname.toLowerCase();
    return source.allowed_hosts.some((allowed) => host === allowed || host.endsWith(`.${allowed}`));
  } catch (_) { return false; }
}

function officialXHandleFromUrl(urlValue) {
  try {
    const u = new URL(urlValue);
    if (!['x.com', 'www.x.com'].includes(u.hostname.toLowerCase())) return '';
    const handle = (u.pathname.split('/').filter(Boolean)[0] || '').replace(/^@/, '');
    return /^[A-Za-z0-9_]{1,15}$/.test(handle) ? handle : '';
  } catch (_) { return ''; }
}
function officialXUrlAllowed(urlValue, source) {
  if (!hostAllowed(urlValue, source)) return false;
  const allowed = new Set((source.social_x_handles || []).map((handle) => text(handle).toLowerCase()));
  return allowed.has(officialXHandleFromUrl(urlValue).toLowerCase());
}

function canonicalUrl(raw) {
  try {
    const u = new URL(raw);
    for (const key of [...u.searchParams.keys()]) {
      if (/^(utm_|ref$|ref_|source$|campaign$|from$)/i.test(key)) u.searchParams.delete(key);
    }
    u.hash = '';
    return u.toString().replace(/\/$/, '');
  } catch (_) { return text(raw); }
}
function normalizeCandidateUrl(source, rawUrl) {
  const value = canonicalUrl(rawUrl);
  if (!value) return '';
  try {
    const u = new URL(value);
    // Some official announcement indexes expose article slugs as relative values.
    // Resolving those against a /section/... listing can create a syntactically
    // valid but non-existent URL. Repair only the provider-specific shapes that
    // are known from their public announcement routing; never leave the official host.
    if (source?.provider === 'okx') {
      const m = u.pathname.match(/^(\/[^/]+\/help)\/section\/([^/]+)$/i);
      if (m && !/^(latest-events|announcements-jumpstart)$/i.test(m[2])) {
        u.pathname = `${m[1]}/${m[2]}`;
      }
    }
    if (source?.provider === 'bitget') {
      let m = u.pathname.match(/^(\/support)\/sections\/articles\/(\d{8,})$/i);
      if (m) u.pathname = `${m[1]}/articles/${m[2]}`;
      m = u.pathname.match(/^(\/support)\/sections\/(\d{10,})$/i);
      if (m && m[2] !== '4413154768537') u.pathname = `${m[1]}/articles/${m[2]}`;
    }
    if (source?.provider === 'bybit' && u.hostname.toLowerCase() === 'announcements.bybit.com') {
      // The v5 announcement API can publish locale-qualified /en-US/article URLs,
      // while the current public announcement frontend canonically serves /en/article.
      // Normalize only the locale prefix; keep the official host and article slug intact.
      u.pathname = u.pathname.replace(/^\/en(?:-|_)US\/article\//i, '/en/article/');
    }
    return canonicalUrl(u.toString());
  } catch (_) { return value; }
}
function hashString(value) {
  // Stable non-cryptographic fingerprint; enough for source event identity fallback.
  let h1 = 0x811c9dc5;
  for (const ch of text(value)) {
    h1 ^= ch.codePointAt(0);
    h1 = Math.imul(h1, 0x01000193) >>> 0;
  }
  return h1.toString(16).padStart(8, '0');
}
function sourceEventId(provider, urlValue, title) {
  const u = canonicalUrl(urlValue);
  const path = (() => { try { return new URL(u).pathname; } catch (_) { return ''; } })();
  const idMatch = path.match(/(?:detail|article|articles)[\/-]([A-Za-z0-9_-]{8,})/i) || path.match(/([A-Fa-f0-9]{24,})/);
  if (idMatch?.[1]) return `${provider}:${idMatch[1]}`;
  return `${provider}:${hashString(`${u}|${normalizeWhitespace(title).toLowerCase()}`)}`;
}
function titleMatches(source, title) {
  const value = normalizeWhitespace(title);
  if (!value || value.length < 8) return false;
  if (source.exclude.some((rx) => rx.test(value))) return false;
  return source.include.some((rx) => rx.test(value));
}
function categoryFor(provider, title) {
  const value = normalizeWhitespace(title);
  if (provider === 'binance') {
    if (/alpha\s*box/i.test(value)) return 'alpha_box';
    if (/alpha/i.test(value)) return 'alpha';
    if (/hodler\s*airdrops?/i.test(value)) return 'hodler_airdrop';
    if (/launchpool/i.test(value)) return 'launchpool';
    if (/token\s+generation\s+event|\btge\b/i.test(value)) return 'tge';
    if (/trading\s+(?:competition|campaign)|trade.{0,40}(?:reward|airdrop|share)|total\s+airdrop|交易.{0,30}(?:空投|奖励)|总空投/i.test(value)) return 'trading_campaign';
    if (/airdrop/i.test(value)) return 'airdrop';
    return 'other';
  }
  if (provider === 'okx') {
    if (/jumpstart/i.test(value)) return 'jumpstart';
    if (/空投|airdrop/i.test(value)) return 'airdrop';
    if (/奖励|瓜分|reward|campaign|活动/i.test(value)) return 'reward_campaign';
    return 'other';
  }
  if (provider === 'bybit') {
    if (/launchpool/i.test(value)) return 'launchpool';
    if (/alpha/i.test(value)) return 'alpha';
    if (/airdrop/i.test(value)) return 'airdrop';
    if (/reward|prize|campaign|competition|token splash/i.test(value)) return 'reward_campaign';
    return 'other';
  }
  if (provider === 'bitget') {
    if (/poolx/i.test(value)) return 'poolx';
    if (/candybomb/i.test(value)) return 'candybomb';
    if (/launchpool/i.test(value)) return 'launchpool';
    return 'airdrop';
  }
  if (provider === 'gate') {
    if (/hodler\s*airdrop/i.test(value)) return 'hodler_airdrop';
    if (/candydrop/i.test(value)) return 'candydrop';
    if (/launchpool/i.test(value)) return 'launchpool';
    if (/vip\s*airdrop/i.test(value)) return 'vip_airdrop';
    return 'airdrop';
  }
  return 'airdrop';
}
function projectSymbolFromTitle(title) {
  const value = normalizeWhitespace(title);
  const pairs = [...value.matchAll(/\(([A-Z0-9][A-Z0-9._-]{1,14})\)/g)].map((m) => m[1]);
  if (pairs.length) return pairs[pairs.length - 1];
  const x = value.match(/\bx\s+([A-Z][A-Z0-9._-]{1,14})\b/i);
  if (x?.[1]) return x[1].toUpperCase();
  return null;
}
function extractRewardText(title, detailText) {
  const value = `${normalizeWhitespace(title)} ${normalizeWhitespace(detailText)}`;
  const patterns = [
    /(?:claim|receive|get|领取|可领取)[^\d]{0,30}([\d,.]+\s*(?:[KMBT]\s*)?[A-Z][A-Z0-9._-]{1,14})/i,
    /(?:share|瓜分|奖池|奖励|空投总量|total\s+airdrop|reward\s+pool|campaign\s+pool)[^\n。；;]{0,60}?([\d,.]+\s*(?:[KMBT]\s*)?[A-Z][A-Z0-9._-]{1,14})/i,
    /([\d,.]+\s*(?:[KMBT]\s*)?(?:USDT|USDC|BTC|ETH|BNB|BGB|GT|OKB|MNT|[A-Z]{2,12}))\s*(?:reward|prize|airdrop|奖池|奖励|空投)/i,
  ];
  for (const rx of patterns) {
    const m = value.match(rx);
    if (m?.[1]) return normalizeWhitespace(m[1]).replace(/[.,;:!?。；，]+$/, '').slice(0, 80);
  }
  return null;
}
function extractEligibilityText(detailText) {
  // Conservative only: never turn generic footer/navigation wording into a user eligibility claim.
  const value = normalizeWhitespace(detailText).slice(0, 8_000);
  if (!value) return null;
  const sentences = value.split(/(?<=[。.!?；;])\s+/).filter((s) => s.length >= 12 && s.length <= 420);
  const chosen = sentences.filter((s) => /(eligib(?:le|ility)|to\s+participate|participation\s+requirement|complete\s+kyc|identity\s+verification|hold\s+at\s+least|stake\s+at\s+least|lock\s+at\s+least|参与资格|参与要求|需完成.{0,16}(?:认证|交易|申购|质押)|持有至少|质押至少|锁定至少)/i.test(s));
  return chosen.slice(0, 2).join(' ').slice(0, 320) || null;
}
function parseDateCandidate(raw) {
  const value = normalizeWhitespace(raw)
    .replace(/年/g, '-')
    .replace(/月/g, '-')
    .replace(/日/g, ' ')
    .replace(/[年月]/g, '-')
    .replace(/\s+/g, ' ');
  const ms = Date.parse(value);
  return Number.isFinite(ms) ? new Date(ms).toISOString() : null;
}
function extractPublishedAt(html, plain) {
  const candidates = [];
  for (const rx of [
    /(?:article:published_time|datePublished)["'\s:=]+(?:content=["'])?([^"'<]{8,40})/i,
    /"datePublished"\s*:\s*"([^"]+)"/i,
    /<time\b[^>]*datetime=["']([^"']+)["']/i,
  ]) {
    const m = html.match(rx);
    if (m?.[1]) candidates.push(m[1]);
  }
  const dateMatches = plain.match(/(?:20\d{2})[-/.年]\s?\d{1,2}[-/.月]\s?\d{1,2}(?:日)?(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?(?:\s*\(?(?:UTC(?:[+-]\d{1,2})?|GMT(?:[+-]\d{1,2})?)\)?)?/gi) || [];
  candidates.push(...dateMatches.slice(0, 3));
  for (const candidate of candidates) {
    const parsed = parseDateCandidate(candidate);
    if (parsed) return parsed;
  }
  return null;
}
function extractPeriod(detailText) {
  const value = normalizeWhitespace(detailText);
  if (!value) return { start_at: null, end_at: null };
  const datePattern = '(20\\d{2}[-/.年]\\s?\\d{1,2}[-/.月]\\s?\\d{1,2}(?:日)?(?:[ T]\\d{1,2}:\\d{2}(?::\\d{2})?)?(?:\\s*\\(?(?:UTC(?:[+-]\\d{1,2})?|GMT(?:[+-]\\d{1,2})?)\\)?)?)';
  const range = new RegExp(`(?:promotion|event|locking|campaign|活动|锁定|申购|质押)[^。;]{0,40}?${datePattern}\\s*(?:–|—|-|to|至|~|～)\\s*${datePattern}`, 'i');
  const m = value.match(range);
  if (m) {
    return { start_at: parseDateCandidate(m[1]), end_at: parseDateCandidate(m[2]) };
  }
  const endRx = new RegExp(`(?:end(?:s|ing)?|结束时间|截止时间)[^。;]{0,20}?${datePattern}`, 'i');
  const endMatch = value.match(endRx);
  return { start_at: null, end_at: endMatch?.[1] ? parseDateCandidate(endMatch[1]) : null };
}
function statusFor(startAt, endAt) {
  const now = Date.now();
  const start = Date.parse(startAt || '');
  const end = Date.parse(endAt || '');
  if (Number.isFinite(end) && end < now) return 'ended';
  if (Number.isFinite(start) && start > now) return 'upcoming';
  if ((Number.isFinite(start) && start <= now) || (Number.isFinite(end) && end >= now)) return 'active';
  return 'announced';
}
function isoFromEpoch(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  const ms = n < 10_000_000_000 ? n * 1000 : n;
  try { return new Date(ms).toISOString(); } catch (_) { return null; }
}
function extractBybitApiCandidates(raw, source) {
  let payload = null;
  try { payload = JSON.parse(raw); } catch (_) { return []; }
  const list = payload?.result?.list || payload?.retResult?.list || payload?.data?.list || [];
  if (!Array.isArray(list)) return [];
  const rows = [];
  for (const item of list) {
    const title = normalizeWhitespace(item?.title);
    const sourceUrl = normalizeCandidateUrl(source, item?.url);
    if (!titleMatches(source, title) || !sourceUrl || !hostAllowed(sourceUrl, source)) continue;
    rows.push({
      title,
      source_url: sourceUrl,
      description: normalizeWhitespace(item?.description).slice(0, 1200) || null,
      published_at: isoFromEpoch(item?.publishTime ?? item?.dateTimestamp),
      start_at: isoFromEpoch(item?.startDataTimestamp),
      end_at: isoFromEpoch(item?.endDataTimestamp),
    });
  }
  return rows.slice(0, 80);
}
function extractCandidates(html, source, baseUrl) {
  const found = new Map();
  const add = (href, rawTitle) => {
    const title = stripHtml(rawTitle);
    if (!titleMatches(source, title)) return;
    const urlValue = normalizeCandidateUrl(source, safeUrl(href, baseUrl));
    if (!urlValue || !hostAllowed(urlValue, source)) return;
    if (urlValue === canonicalUrl(baseUrl)) return;
    const key = `${urlValue}|${title.toLowerCase()}`;
    if (!found.has(key)) found.set(key, { title, source_url: urlValue });
  };
  for (const match of html.matchAll(/<a\b([^>]*?)href=["']([^"']+)["']([^>]*)>([\s\S]*?)<\/a>/gi)) {
    add(match[2], match[4]);
  }
  // Common SSR/Next payload shapes. These are deliberately conservative: a title
  // still has to pass provider-specific official-event keywords and the URL must stay
  // on the provider's official domain.
  for (const match of html.matchAll(/"(?:title|name|headline)"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"[\s\S]{0,800}?"(?:url|href|link|slug)"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"/gi)) {
    add(match[2], match[1].replace(/\\n/g, ' ').replace(/\\"/g, '"'));
  }
  for (const match of html.matchAll(/"(?:url|href|link|slug)"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"[\s\S]{0,800}?"(?:title|name|headline)"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"/gi)) {
    add(match[1], match[2].replace(/\\n/g, ' ').replace(/\\"/g, '"'));
  }
  return [...found.values()].slice(0, 80);
}
function publicEventView(row) {
  const provider = text(row?.provider).toLowerCase();
  const title = text(row?.title);
  const translationHash = contentTranslationHash(translationFieldsForAirdrop(row));
  const translationFresh = text(row?.translation_source_hash) === translationHash;
  return {
    id: row?.id ?? null,
    is_active: row?.is_active !== false && (text(row?.lifecycle_status).toLowerCase() || 'active') === 'active',
    lifecycle_status: text(row?.lifecycle_status).toLowerCase() || 'active',
    last_seen_at: text(row?.last_seen_at) || null,
    removed_at: text(row?.removed_at) || null,
    provider,
    provider_name: text(row?.provider_name),
    // Recompute provider taxonomy for every public row, including historical DB seed rows.
    // Step1045.2 stored generic values such as `campaign`; those must never leak back into
    // the Step1045.3 provider-specific contract after a restart or stale-snapshot merge.
    category: categoryFor(provider, title),
    source_event_id: text(row?.source_event_id),
    title: text(row?.title),
    project_symbol: text(row?.project_symbol) || null,
    reward_text: text(row?.reward_text) || null,
    eligibility_text: text(row?.eligibility_text) || null,
    start_at: text(row?.start_at) || null,
    end_at: text(row?.end_at) || null,
    published_at: text(row?.published_at) || null,
    source_url: text(row?.source_url),
    status: statusFor(row?.start_at, row?.end_at),
    raw_summary: text(row?.raw_summary) || null,
    content_text: normalizeContentText(row?.content_text) || null,
    content_fetched_at: text(row?.content_fetched_at) || null,
    content_available: contentUsableForProvider(provider, row?.content_text),
    media_items: normalizeContentMediaItems(row?.media_items),
    translations: translationFresh && row?.translations && typeof row.translations === 'object' && !Array.isArray(row.translations) ? row.translations : {},
    translation_source_hash: text(row?.translation_source_hash) || null,
    translation_updated_at: text(row?.translation_updated_at) || null,
    translation_pending: !translationFresh,
    fetched_at: text(row?.fetched_at) || null,
  };
}
function sortSnapshot(rows) {
  const score = (row) => {
    const status = text(row?.status);
    const statusScore = status === 'active' ? 4 : status === 'upcoming' ? 3 : status === 'announced' ? 2 : 1;
    const when = Date.parse(row?.published_at || row?.start_at || '') || 0;
    return [statusScore, when];
  };
  return rows.sort((a, b) => {
    const sa = score(a); const sb = score(b);
    if (sb[0] !== sa[0]) return sb[0] - sa[0];
    return sb[1] - sa[1];
  });
}
function mergePublicSnapshot(rows) {
  const byKey = new Map();
  for (const oldRow of publicSnapshot) {
    const row = publicEventView(oldRow);
    if (!row.is_active || row.lifecycle_status !== 'active') continue;
    const key = row.source_event_id || `${row.provider}|${row.source_url}`;
    if (key) byKey.set(key, row);
  }
  for (const incoming of rows) {
    const row = publicEventView(incoming);
    if (!row.is_active || row.lifecycle_status !== 'active') continue;
    const key = row.source_event_id || `${row.provider}|${row.source_url}`;
    if (!key) continue;
    const previous = byKey.get(key) || {};
    const incomingHasContent = contentUsableForProvider(row.provider, row.content_text);
    byKey.set(key, {
      ...previous,
      ...row,
      last_seen_at: row.last_seen_at || previous.last_seen_at || null,
      content_text: incomingHasContent ? row.content_text : (previous.content_text || null),
      content_fetched_at: incomingHasContent ? row.content_fetched_at : (previous.content_fetched_at || null),
      content_available: incomingHasContent || contentUsableForProvider(row.provider, previous.content_text),
      media_items: normalizeContentMediaItems(row.media_items).length
        ? normalizeContentMediaItems(row.media_items)
        : normalizeContentMediaItems(previous.media_items),
      translations: row.translation_source_hash ? row.translations : (previous.translations || {}),
      translation_source_hash: row.translation_source_hash || previous.translation_source_hash || null,
      translation_updated_at: row.translation_updated_at || previous.translation_updated_at || null,
    });
  }
  publicSnapshot = sortSnapshot([...byKey.values()]).slice(0, PUBLIC_CACHE_LIMIT);
  state.publicSnapshotRows = publicSnapshot.length;
  state.publicSnapshotLoaded = true;
}
function replacePublicSnapshot(rows) {
  publicSnapshot = sortSnapshot((Array.isArray(rows) ? rows : [])
    .map(publicEventView)
    .filter((row) => row.is_active && row.lifecycle_status === 'active'))
    .slice(0, PUBLIC_CACHE_LIMIT);
  state.publicSnapshotRows = publicSnapshot.length;
  state.publicSnapshotLoaded = true;
}
async function supabaseFetch(path, init = {}) {
  if (!state.supabaseConfigured) throw new Error('airdrop_watch_supabase_not_configured');
  const response = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
    ...init,
    headers: { ...authHeaders(), ...(init.headers || {}) },
  });
  const raw = await response.text();
  let payload = null;
  try { payload = raw ? JSON.parse(raw) : null; } catch (_) { payload = raw; }
  if (!response.ok) throw new Error(`airdrop_watch_supabase_http_${response.status}:${text(raw).slice(0, 260)}`);
  return payload;
}
async function loadDbSnapshot({ force = false } = {}) {
  if (!state.supabaseConfigured) return false;
  if (!force && dbLoadedAtMs && Date.now() - dbLoadedAtMs < DB_RELOAD_MS) return false;
  const rows = await supabaseFetch(`${EVENTS_TABLE}?is_active=eq.true&lifecycle_status=eq.active&select=id,provider,provider_name,category,source_event_id,title,project_symbol,reward_text,eligibility_text,start_at,end_at,published_at,source_url,status,raw_summary,content_text,content_fetched_at,media_items,translations,translation_source_hash,translation_updated_at,fetched_at,is_active,lifecycle_status,last_seen_at,removed_at&order=published_at.desc.nullslast,fetched_at.desc&limit=${PUBLIC_CACHE_LIMIT}`);
  state.publicSnapshotDbReads++;
  replacePublicSnapshot(Array.isArray(rows) ? rows : []);
  dbLoadedAtMs = Date.now();
  state.lastDbLoadAt = nowIso();
  return true;
}
async function fetchHtml(urlValue, sourceId, { detail = false, content = false } = {}) {
  const timeoutMs = detail ? DETAIL_TIMEOUT_MS : SOURCE_TIMEOUT_MS;
  const controller = new AbortController();
  const timerId = setTimeout(() => controller.abort(), timeoutMs);
  const conditional = content ? {} : (conditionalHeaders.get(urlValue) || {});
  const headers = {
    accept: 'text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8',
    'accept-language': 'en-US,en;q=0.8,zh-CN;q=0.6',
    'user-agent': 'Mozilla/5.0 (compatible; KakaWeb3-OfficialAirdropWatch/1.0; +https://kakaweb3.app)',
    ...conditional,
  };
  state.upstreamRequests++;
  if (detail) state.detailRequests++;
  try {
    const response = await fetch(urlValue, { headers, signal: controller.signal, redirect: 'follow' });
    const sourceState = state.sourceStates[sourceId] ||= {};
    if (!content) {
      sourceState.last_http_status = response.status;
      sourceState.last_request_at = nowIso();
    }
    if (response.status === 304) {
      state.upstreamNotModified++;
      return { notModified: true, html: '', finalUrl: urlValue };
    }
    if (!response.ok) throw new Error(`official_http_${response.status}`);
    const etag = text(response.headers.get('etag'));
    const lastModified = text(response.headers.get('last-modified'));
    const nextConditional = {};
    if (etag) nextConditional['if-none-match'] = etag;
    if (lastModified) nextConditional['if-modified-since'] = lastModified;
    if (!content && Object.keys(nextConditional).length) conditionalHeaders.set(urlValue, nextConditional);
    const reader = response.body?.getReader?.();
    if (!reader) {
      const raw = await response.text();
      return { notModified: false, html: raw.slice(0, HTML_MAX_BYTES), finalUrl: response.url || urlValue };
    }
    const decoder = new TextDecoder();
    let total = 0;
    let body = '';
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      total += value?.byteLength || 0;
      if (total > HTML_MAX_BYTES) break;
      body += decoder.decode(value, { stream: true });
    }
    try { reader.cancel(); } catch (_) {}
    return { notModified: false, html: body, finalUrl: response.url || urlValue };
  } catch (error) {
    state.upstreamFailures++;
    const sourceState = state.sourceStates[sourceId] ||= {};
    if (!content) {
      sourceState.failures = finiteNumber(sourceState.failures, 0) + 1;
      sourceState.last_error = String(error?.name === 'AbortError' ? 'official_timeout' : error?.message || error);
    }
    throw error;
  } finally {
    clearTimeout(timerId);
  }
}
async function hydrateCandidate(source, candidate, known) {
  const base = {
    provider: source.provider,
    provider_name: source.display_name,
    category: categoryFor(source.provider, candidate.title),
    source_event_id: sourceEventId(source.provider, candidate.source_url, candidate.title),
    title: normalizeWhitespace(candidate.title).slice(0, 600),
    project_symbol: projectSymbolFromTitle(candidate.title),
    source_url: canonicalUrl(candidate.source_url),
    source_domain: (() => { try { return new URL(candidate.source_url).hostname; } catch (_) { return ''; } })(),
    fetched_at: nowIso(),
  };
  if (known && text(known.published_at)) {
    // Timestamp-enriched rows do not refetch their detail page on every refresh.
    return { ...known, ...base, fetched_at: nowIso() };
  }
  let detailHtml = '';
  try {
    const detail = await fetchHtml(candidate.source_url, source.id, { detail: true });
    detailHtml = detail.html || '';
  } catch (_) {
    // Keep the official listing row even if detail fetch is temporarily unavailable.
  }
  const detailText = stripHtml(detailHtml);
  const structuredSummary = normalizeWhitespace(candidate.description);
  const combinedText = [structuredSummary, detailText].filter(Boolean).join(' ');
  const period = extractPeriod(combinedText);
  const startAt = candidate.start_at || period.start_at;
  const endAt = candidate.end_at || period.end_at;
  const publishedAt = candidate.published_at || extractPublishedAt(detailHtml, detailText) || null;
  const reward = extractRewardText(candidate.title, combinedText);
  return {
    ...base,
    reward_text: reward,
    eligibility_text: extractEligibilityText(combinedText),
    start_at: startAt,
    end_at: endAt,
    published_at: publishedAt,
    status: statusFor(startAt, endAt),
    raw_summary: combinedText ? combinedText.slice(0, 600) : (known?.raw_summary || null),
    media_items: officialPageMediaItems(detailHtml, candidate.source_url).length
      ? officialPageMediaItems(detailHtml, candidate.source_url)
      : normalizeContentMediaItems(known?.media_items),
    content_text: contentUsable(known?.content_text) ? known.content_text : null,
    content_fetched_at: contentUsable(known?.content_text) ? (known.content_fetched_at || null) : null,
  };
}

function rowsFromSocialXEvents(events, source) {
  const handles = (source.social_x_handles || []).map((handle) => text(handle)).filter(Boolean);
  const allowedHandles = new Set(handles.map((handle) => handle.toLowerCase()));
  const rows = [];
  const seen = new Set();
  for (const event of Array.isArray(events) ? events : []) {
    const postId = text(event?.source_post_id);
    const authorHandle = text(event?.author_handle);
    const content = normalizeWhitespace(event?.content);
    const postUrl = canonicalUrl(event?.post_url);
    if (!postId || !content || !allowedHandles.has(authorHandle.toLowerCase())) continue;
    if (!officialXUrlAllowed(postUrl, source) || !titleMatches(source, content)) continue;
    const id = `${source.provider}:x:${postId}`;
    if (seen.has(id)) continue;
    seen.add(id);
    const period = extractPeriod(content);
    const publishedAt = text(event?.published_at) || null;
    rows.push({
      provider: source.provider,
      provider_name: source.display_name,
      category: categoryFor(source.provider, content),
      source_event_id: id,
      title: content.slice(0, 600),
      project_symbol: projectSymbolFromTitle(content),
      reward_text: extractRewardText(content, content),
      eligibility_text: extractEligibilityText(content),
      start_at: period.start_at,
      end_at: period.end_at,
      published_at: publishedAt,
      source_url: postUrl,
      source_domain: (() => { try { return new URL(postUrl).hostname; } catch (_) { return ''; } })(),
      status: statusFor(period.start_at, period.end_at),
      raw_summary: content.slice(0, 600),
      content_text: content.slice(0, CONTENT_TEXT_MAX_CHARS),
      content_fetched_at: nowIso(),
      media_items: normalizeContentMediaItems(event?.media_items),
      fetched_at: nowIso(),
    });
  }
  return rows.slice(0, 80);
}

async function collectSocialXSource(source, sourceState) {
  sourceState.last_collection_complete = false;
  const handles = (source.social_x_handles || []).map((handle) => text(handle)).filter(Boolean);
  if (!handles.length || !state.supabaseConfigured) return [];
  const handleFilter = handles.join(',');
  const firstCycleAttempts = finiteNumber(sourceState.refreshes, 0) === 0 ? 6 : 1;
  let rows = [];
  for (let attempt = 0; attempt < firstCycleAttempts; attempt++) {
    state.sharedBridgeReads++;
    sourceState.bridge_reads = finiteNumber(sourceState.bridge_reads, 0) + 1;
    const events = await supabaseFetch(
      `${SOCIAL_EVENTS_TABLE}?source=eq.x&is_active=eq.true&lifecycle_status=eq.active&author_handle=in.(${handleFilter})&select=source_post_id,author_handle,author_name,content,post_url,published_at,media_items&order=published_at.desc&limit=100`,
    );
    rows = rowsFromSocialXEvents(events, source);
    if (rows.length || attempt === firstCycleAttempts - 1) break;
    // The social watcher has to sync two new X rules and seed recent posts first.
    // Only the first collector cycle waits, and only on shared Supabase reads.
    await sleep(3_000);
  }
  sourceState.last_candidate_count = rows.length;
  sourceState.last_new_detail_fetches = 0;
  sourceState.last_http_status = 'shared_x';
  sourceState.refreshes = finiteNumber(sourceState.refreshes, 0) + 1;
  sourceState.last_success_at = nowIso();
  sourceState.last_error = null;
  sourceState.last_collection_complete = true;
  return rows;
}

async function collectSource(source) {
  const sourceState = state.sourceStates[source.id] ||= {
    provider: source.provider,
    roots: (source.roots?.length || 0) + (source.json_roots?.length || 0) + (source.social_x_handles?.length || 0),
    refreshes: 0,
    failures: 0,
    last_error: null,
    last_success_at: null,
    last_candidate_count: 0,
    last_new_detail_fetches: 0,
  };
  if (source.social_x_handles?.length) return collectSocialXSource(source, sourceState);
  const knownById = new Map(publicSnapshot.filter((row) => row.provider === source.provider).map((row) => [row.source_event_id, row]));
  const candidatesByKey = new Map();
  let structuredSourceHealthy = false;
  let sourceAttempts = 0;
  let sourceSuccesses = 0;
  let sourceHadNotModified = false;
  for (const root of source.json_roots || []) {
    sourceAttempts++;
    try {
      const result = await fetchHtml(root, source.id);
      sourceSuccesses++;
      structuredSourceHealthy = true;
      if (result.notModified) { sourceHadNotModified = true; continue; }
      const extracted = source.provider === 'bybit'
        ? extractBybitApiCandidates(result.html, source)
        : [];
      for (const candidate of extracted) {
        const id = sourceEventId(source.provider, candidate.source_url, candidate.title);
        if (!candidatesByKey.has(id)) candidatesByKey.set(id, candidate);
      }
    } catch (error) {
      sourceState.last_error = String(error?.message || error);
    }
  }
  // Bybit exposes a documented public announcement API. The HTML page is only
  // a fallback when that official structured source is unavailable, avoiding
  // a redundant webpage request on every normal refresh.
  if (!structuredSourceHealthy) {
    for (const root of source.roots || []) {
      sourceAttempts++;
      try {
        const result = await fetchHtml(root, source.id);
        sourceSuccesses++;
        if (result.notModified) { sourceHadNotModified = true; continue; }
        for (const candidate of extractCandidates(result.html, source, result.finalUrl || root)) {
          const id = sourceEventId(source.provider, candidate.source_url, candidate.title);
          if (!candidatesByKey.has(id)) candidatesByKey.set(id, candidate);
        }
      } catch (error) {
        sourceState.last_error = String(error?.message || error);
      }
    }
  }
  const candidates = [...candidatesByKey.entries()].slice(0, 80);
  sourceState.last_candidate_count = candidates.length;
  const rows = [];
  for (const [id, candidate] of candidates) {
    const known = knownById.get(id) || null;
    const structuredSummary = normalizeWhitespace(candidate.description);
    const startAt = candidate.start_at || known?.start_at || null;
    const endAt = candidate.end_at || known?.end_at || null;
    const publishedAt = candidate.published_at || known?.published_at || null;
    rows.push({
      ...(known || {}),
      provider: source.provider,
      provider_name: source.display_name,
      category: categoryFor(source.provider, candidate.title),
      source_event_id: id,
      title: normalizeWhitespace(candidate.title).slice(0, 600),
      project_symbol: projectSymbolFromTitle(candidate.title) || known?.project_symbol || null,
      reward_text: extractRewardText(candidate.title, structuredSummary) || known?.reward_text || null,
      eligibility_text: extractEligibilityText(structuredSummary) || known?.eligibility_text || null,
      start_at: startAt,
      end_at: endAt,
      published_at: publishedAt,
      source_url: canonicalUrl(candidate.source_url),
      source_domain: (() => { try { return new URL(candidate.source_url).hostname; } catch (_) { return ''; } })(),
      status: statusFor(startAt, endAt),
      raw_summary: structuredSummary ? structuredSummary.slice(0, 600) : (known?.raw_summary || null),
      media_items: normalizeContentMediaItems(candidate.media_items).length
        ? normalizeContentMediaItems(candidate.media_items)
        : normalizeContentMediaItems(known?.media_items),
      content_text: contentUsableForProvider(source.provider, known?.content_text)
        ? known.content_text
        : (source.provider === 'bybit' && contentUsableForProvider('bybit', structuredSummary) ? structuredSummary : null),
      content_fetched_at: contentUsableForProvider(source.provider, known?.content_text)
        ? known.content_fetched_at
        : (source.provider === 'bybit' && contentUsableForProvider('bybit', structuredSummary) ? nowIso() : null),
      fetched_at: nowIso(),
    });
  }
  // Step1045.3: the old optional metadata-detail fan-out is fully disabled.
  // Native detail body synchronization is handled only by enrichSourceContent().
  sourceState.last_new_detail_fetches = 0;
  sourceState.last_collection_complete = sourceAttempts > 0 && sourceSuccesses === sourceAttempts && !sourceHadNotModified;
  sourceState.refreshes = finiteNumber(sourceState.refreshes, 0) + 1;
  if (rows.length || sourceState.last_error == null) {
    sourceState.last_success_at = nowIso();
    if (rows.length) sourceState.last_error = null;
  }
  return rows;
}
async function enrichSourceContent(source, rows) {
  if (source.social_x_handles?.length) return rows;
  let budget = CONTENT_ENRICH_FETCHES_PER_SOURCE;
  const out = [];
  const priorByKey = new Map(publicSnapshot
    .filter((item) => item.provider === source.provider)
    .map((item) => [item.source_event_id, item]));
  for (const rawRow of rows) {
    const previous = priorByKey.get(rawRow?.source_event_id) || {};
    const row = contentUsableForProvider(source.provider, rawRow?.content_text)
      ? rawRow
      : {
          ...rawRow,
          content_text: contentUsableForProvider(source.provider, previous?.content_text) ? previous.content_text : null,
          content_fetched_at: contentUsableForProvider(source.provider, previous?.content_text) ? previous.content_fetched_at : null,
        };
    const rowHasContent = contentUsableForProvider(source.provider, row?.content_text);
    const rowHasMedia = normalizeContentMediaItems(row?.media_items).length > 0;
    if ((rowHasContent && rowHasMedia) || budget <= 0) {
      out.push(row);
      continue;
    }
    if (source.provider === 'bybit') {
      // Bybit's documented Announcement API already provides official description/time fields.
      // The public article frontend is JS-rendered and can hang in Render; never fan out to it.
      out.push(row);
      continue;
    }
    const retryKey = `${row.provider}|${row.source_event_id}`;
    const retryAt = finiteNumber(contentRetryAfter.get(retryKey), 0);
    if (retryAt > Date.now()) {
      out.push(row);
      continue;
    }
    budget--;
    state.contentEnrichRequests++;
    const sourceState = state.sourceStates[source.id] ||= {};
    sourceState.content_enrich_fetches = finiteNumber(sourceState.content_enrich_fetches, 0) + 1;
    try {
      const detail = await fetchHtml(row.source_url, source.id, { detail: true, content: true });
      const detailHtml = detail.html || '';
      const contentText = htmlArticleText(detailHtml);
      const fetchedMediaItems = officialPageMediaItems(detailHtml, row.source_url);
      const fetchedContentUsable = contentUsable(contentText);
      if (!rowHasContent && !fetchedContentUsable && fetchedMediaItems.length === 0) throw new Error('official_content_and_media_too_small');
      const detailText = stripHtml(detailHtml);
      const period = extractPeriod(contentText || detailText);
      const publishedAt = row.published_at || extractPublishedAt(detailHtml, detailText) || null;
      const startAt = row.start_at || period.start_at;
      const endAt = row.end_at || period.end_at;
      out.push({
        ...row,
        reward_text: row.reward_text || extractRewardText(row.title, contentText) || null,
        eligibility_text: row.eligibility_text || extractEligibilityText(contentText) || null,
        start_at: startAt,
        end_at: endAt,
        published_at: publishedAt,
        status: statusFor(startAt, endAt),
        raw_summary: row.raw_summary || contentText.slice(0, 600),
        media_items: fetchedMediaItems.length
          ? fetchedMediaItems
          : normalizeContentMediaItems(row.media_items),
        content_text: fetchedContentUsable ? contentText : row.content_text,
        content_fetched_at: fetchedContentUsable ? nowIso() : row.content_fetched_at,
      });
      contentRetryAfter.delete(retryKey);
      sourceState.content_last_error = null;
      sourceState.content_last_success_at = nowIso();
    } catch (error) {
      state.contentEnrichFailures++;
      sourceState.content_enrich_failures = finiteNumber(sourceState.content_enrich_failures, 0) + 1;
      sourceState.content_last_error = String(error?.name === 'AbortError' ? 'official_timeout' : error?.message || error);
      // Preserve the list row and avoid hammering a 404/challenge URL every cycle.
      contentRetryAfter.set(retryKey, Date.now() + CONTENT_RETRY_MS);
      out.push(row);
    }
  }
  return out;
}
function listEventView(row) {
  const view = publicEventView(row);
  const { content_text, ...rest } = view;
  return rest;
}
function detailRow(url) {
  const provider = text(url.searchParams.get('provider')).toLowerCase();
  const sourceEventIdValue = text(url.searchParams.get('source_event_id'));
  if (!provider || !sourceEventIdValue) return null;
  const row = publicSnapshot.find((item) => item.provider === provider && item.source_event_id === sourceEventIdValue);
  return row ? publicEventView(row) : null;
}

function stableComparableValue(value) {
  if (typeof value === 'string') return postgresSafeString(value);
  if (Array.isArray(value)) return value.map(stableComparableValue);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map((key) => [key, stableComparableValue(value[key])]));
  }
  return value;
}
function comparableTime(value) {
  const ms = Date.parse(text(value));
  return Number.isFinite(ms) ? new Date(ms).toISOString() : null;
}
function airdropPersistSignature(row) {
  const provider = text(row?.provider).toLowerCase();
  const title = normalizeWhitespace(row?.title);
  return JSON.stringify(stableComparableValue({
    provider,
    provider_name: text(row?.provider_name),
    category: categoryFor(provider, title),
    source_event_id: text(row?.source_event_id),
    title,
    project_symbol: text(row?.project_symbol) || null,
    reward_text: text(row?.reward_text) || null,
    eligibility_text: text(row?.eligibility_text) || null,
    start_at: comparableTime(row?.start_at),
    end_at: comparableTime(row?.end_at),
    published_at: comparableTime(row?.published_at),
    source_url: canonicalUrl(row?.source_url),
    status: statusFor(row?.start_at, row?.end_at),
    raw_summary: normalizeWhitespace(row?.raw_summary) || null,
    content_text: normalizeContentText(row?.content_text) || null,
    media_items: normalizeContentMediaItems(row?.media_items),
  }));
}

async function persistRows(rows, previousSnapshot = []) {
  if (!rows.length || !state.supabaseConfigured) return [];
  const priorByKey = new Map((Array.isArray(previousSnapshot) ? previousSnapshot : []).map((item) => [`${text(item?.provider).toLowerCase()}|${text(item?.source_event_id)}`, item]));
  const nowMs = Date.now();
  const payload = [];
  let noopSkipped = 0;
  let heartbeatRows = 0;
  state.persistRowsConsidered += rows.length;
  for (const rawRow of rows) {
    const key = `${text(rawRow?.provider).toLowerCase()}|${text(rawRow?.source_event_id)}`;
    const previous = priorByKey.get(key) || {};
    const row = contentUsableForProvider(rawRow?.provider, rawRow?.content_text)
      ? rawRow
      : {
          ...rawRow,
          content_text: contentUsableForProvider(rawRow?.provider, previous?.content_text) ? previous.content_text : null,
          content_fetched_at: contentUsableForProvider(rawRow?.provider, previous?.content_text) ? previous.content_fetched_at : null,
        };
    const previousActive = previous?.is_active !== false && (text(previous?.lifecycle_status).toLowerCase() || 'active') === 'active';
    const unchanged = previousActive && airdropPersistSignature(previous) === airdropPersistSignature(row);
    const lastSeenMs = Date.parse(text(previous?.last_seen_at));
    const heartbeatDue = !Number.isFinite(lastSeenMs) || nowMs - lastSeenMs >= PERSIST_HEARTBEAT_MS;
    if (unchanged && !heartbeatDue) {
      noopSkipped += 1;
      continue;
    }
    if (unchanged && heartbeatDue) heartbeatRows += 1;
    payload.push({
      provider: row.provider,
      provider_name: row.provider_name,
      category: row.category,
      source_event_id: row.source_event_id,
      title: row.title,
      project_symbol: row.project_symbol,
      reward_text: row.reward_text,
      eligibility_text: row.eligibility_text,
      start_at: row.start_at,
      end_at: row.end_at,
      published_at: row.published_at,
      source_url: row.source_url,
      source_domain: row.source_domain,
      status: statusFor(row.start_at, row.end_at),
      raw_summary: row.raw_summary,
      content_text: contentUsableForProvider(row.provider, row.content_text) ? normalizeContentText(row.content_text).slice(0, CONTENT_TEXT_MAX_CHARS) : null,
      content_fetched_at: contentUsableForProvider(row.provider, row.content_text) ? (row.content_fetched_at || nowIso()) : null,
      media_items: normalizeContentMediaItems(row.media_items),
      fetched_at: nowIso(),
      is_active: true,
      lifecycle_status: 'active',
      last_seen_at: nowIso(),
      removed_at: null,
      expired_at: null,
      lifecycle_reason: '',
      updated_at: nowIso(),
    });
  }
  state.persistRowsNoopSkipped += noopSkipped;
  state.persistHeartbeatRows += heartbeatRows;
  if (!payload.length) {
    state.persistNoopBatches += 1;
    return [];
  }
  const result = await supabaseFetch(`${EVENTS_TABLE}?on_conflict=provider,source_event_id&select=id,provider,provider_name,category,source_event_id,title,project_symbol,reward_text,eligibility_text,start_at,end_at,published_at,source_url,status,raw_summary,content_text,content_fetched_at,media_items,translations,translation_source_hash,translation_updated_at,fetched_at,is_active,lifecycle_status,last_seen_at,removed_at`, {
    method: 'POST',
    headers: { prefer: 'resolution=merge-duplicates,return=representation' },
    body: JSON.stringify(payload.map(sanitizePostgresJsonValue)),
  });
  state.persistedRows += payload.length;
  state.persistRowsWritten += payload.length;
  state.persistSuccesses++;
  state.lastPersistAt = nowIso();
  return Array.isArray(result) ? result : payload;
}
function translationFieldsForAirdrop(row) {
  const content = normalizeContentText(row?.content_text);
  const summary = text(row?.raw_summary);
  return {
    title: text(row?.title),
    reward_text: text(row?.reward_text),
    eligibility_text: text(row?.eligibility_text),
    ...(content ? { content } : (summary ? { raw_summary: summary } : {})),
  };
}
function translationPriority(row) {
  let score = 0;
  const provider = text(row?.provider).toLowerCase();
  const category = text(row?.category).toLowerCase();
  const status = text(row?.status).toLowerCase();
  if (provider === 'binance' && ['alpha', 'alpha_box'].includes(category)) score += 100000;
  if (status === 'active') score += 30000;
  else if (status === 'upcoming') score += 20000;
  else if (status === 'announced') score += 10000;
  const when = Date.parse(row?.published_at || row?.start_at || '') || 0;
  score += Math.floor(when / 1_000_000_000);
  return score;
}
async function translateAirdropSnapshot() {
  if (translationBackfillInFlight || !state.supabaseConfigured || !publicSnapshot.length) return false;
  const translationHealth = getSharedTranslationHealth();
  if (!translationHealth.configured) { state.translationLastError = 'translation_provider_not_configured:KAKA_GOOGLE_TRANSLATION_API_KEY'; return false; }
  if (translationHealth.daily_budget_ready === false || translationHealth.auto_daily_budget_ready === false) { state.translationLastError = 'translation_auto_zh_budget_locked'; return false; }
  translationBackfillInFlight = true;
  state.translationRuns++;
  state.translationLastRunAt = nowIso();
  try {
    // Keep a bounded hot/relevant window only.  Phase 1 is title-first and
    // provider-balanced so Binance/OKX/Bybit/Bitget/Gate all become readable
    // before long article bodies can consume the daily translation quota.
    const translationWindow = [...publicSnapshot]
      .filter((row) => text(row?.source_event_id))
      .sort((a, b) => translationPriority(b) - translationPriority(a))
      .slice(0, TRANSLATION_WINDOW_ROWS);
    const pendingTitles = translationWindow.filter((row) => translationNeedsWork({
      fields: translationFieldsForAirdrop(row),
      existingTranslations: row?.translations,
      existingSourceHash: text(row?.translation_source_hash),
      requiredFields: ['title'],
      targetLocale: 'zh',
    }));
    const titleCandidates = [];
    for (const source of OFFICIAL_SOURCES) {
      const candidate = pendingTitles.find((row) => row.provider === source.provider && !titleCandidates.includes(row));
      if (candidate) titleCandidates.push(candidate);
      if (titleCandidates.length >= TRANSLATION_ROWS_PER_TICK) break;
    }
    for (const candidate of pendingTitles) {
      if (titleCandidates.length >= TRANSLATION_ROWS_PER_TICK) break;
      if (!titleCandidates.includes(candidate)) titleCandidates.push(candidate);
    }

    const persistTranslation = async (candidate, result, kind) => {
      const provider = text(candidate.provider).toLowerCase();
      const sourceEventId = text(candidate.source_event_id);
      if (!provider || !sourceEventId || !result.changed || result.translated_fields < 1) return false;
      const updatedAt = nowIso();
      await supabaseFetch(`${EVENTS_TABLE}?provider=eq.${encodeURIComponent(provider)}&source_event_id=eq.${encodeURIComponent(sourceEventId)}`, {
        method: 'PATCH',
        headers: { prefer: 'return=minimal' },
        body: JSON.stringify({
          translations: result.translations,
          translation_source_hash: result.source_hash,
          translation_updated_at: updatedAt,
        }),
      });
      state.translationDbWrites++;
      state.translationRowsTranslated++;
      if (kind === 'title') state.translationTitleRowsTranslated++;
      else state.translationDetailRowsTranslated++;
      state.translationLastSuccessAt = updatedAt;
      state.translationLastError = null;
      publicSnapshot = publicSnapshot.map((row) =>
        row.provider === provider && row.source_event_id === sourceEventId
          ? publicEventView({
              ...row,
              translations: result.translations,
              translation_source_hash: result.source_hash,
              translation_updated_at: updatedAt,
            })
          : row,
      );
      return true;
    };

    for (const candidate of titleCandidates) {
      try {
        const result = await translateContentFields({
          fields: translationFieldsForAirdrop(candidate),
          existingTranslations: candidate.translations,
          existingSourceHash: text(candidate.translation_source_hash),
          onlyFields: ['title'],
          maxSourceChars: 700,
          budgetBucket: 'auto_zh',
          targetLocale: 'zh',
        });
        await persistTranslation(candidate, result, 'title');
      } catch (error) {
        state.translationFailures++;
        state.translationLastError = String(error?.name === 'AbortError' ? 'translation_timeout' : error?.message || error);
        if (['TRANSLATION_COOLDOWN','TRANSLATION_DAILY_BUDGET','TRANSLATION_AUTO_DAILY_BUDGET','TRANSLATION_BUCKET_BUDGET'].includes(String(error?.code || ''))) break;
      }
    }

    // Chinese-first short-detail rule: after titles are caught up, auto-translate only
    // genuinely short English activity details. Long official bodies keep their Chinese
    // title warm, while the body is translated once on explicit user tap and then shared.
    if (pendingTitles.length === 0 && TRANSLATION_DETAIL_ROWS_PER_TICK > 0) {
      const detailFields = ['reward_text', 'eligibility_text', 'content', 'raw_summary'];
      const pendingDetails = translationWindow.filter((row) => {
        const fields = translationFieldsForAirdrop(row);
        const detailChars = detailFields.reduce((sum, key) => sum + Array.from(text(fields[key])).length, 0);
        if (detailChars <= 0 || detailChars > AUTO_SHORT_DETAIL_CHARS) return false;
        return translationNeedsWork({
          fields,
          existingTranslations: row?.translations,
          existingSourceHash: text(row?.translation_source_hash),
          requiredFields: detailFields,
          targetLocale: 'zh',
        });
      });
      for (const candidate of pendingDetails.slice(0, TRANSLATION_DETAIL_ROWS_PER_TICK)) {
        try {
          const result = await translateContentFields({
            fields: translationFieldsForAirdrop(candidate),
            existingTranslations: candidate.translations,
            existingSourceHash: text(candidate.translation_source_hash),
            onlyFields: detailFields,
            maxSourceChars: AUTO_SHORT_DETAIL_CHARS,
            budgetBucket: 'auto_zh',
            targetLocale: 'zh',
          });
          await persistTranslation(candidate, result, 'detail');
        } catch (error) {
          state.translationFailures++;
          state.translationLastError = String(error?.name === 'AbortError' ? 'translation_timeout' : error?.message || error);
          if (['TRANSLATION_COOLDOWN','TRANSLATION_DAILY_BUDGET','TRANSLATION_AUTO_DAILY_BUDGET'].includes(String(error?.code || ''))) break;
        }
      }
    }
    state.publicSnapshotRows = publicSnapshot.length;
    return true;
  } finally {
    translationBackfillInFlight = false;
  }
}
function scheduleTranslationTick(delayMs = TRANSLATION_TICK_MS) {
  if (stopping || !state.enabled) return;
  if (translationTimer) clearTimeout(translationTimer);
  translationTimer = setTimeout(async () => {
    try { await translateAirdropSnapshot(); }
    finally { if (!stopping) scheduleTranslationTick(); }
  }, Math.max(5_000, delayMs));
  translationTimer.unref?.();
}

async function reconcileAirdropProviderWindow(source, rows) {
  if (!state.supabaseConfigured || source?.last_collection_complete !== true) return null;
  const cutoff = Date.now() - 7 * 24 * 60 * 60_000;
  const recent = (Array.isArray(rows) ? rows : [])
    .map((row) => ({ row, ms: Date.parse(text(row?.published_at)) }))
    .filter((item) => Number.isFinite(item.ms) && item.ms >= cutoff)
    .sort((a, b) => a.ms - b.ms);
  if (recent.length < 2) return null;
  const windowStart = new Date(recent[0].ms).toISOString();
  const windowEnd = new Date(recent[recent.length - 1].ms).toISOString();
  const ids = [...new Set(recent.map((item) => text(item.row?.source_event_id)).filter(Boolean))];
  if (ids.length < 2) return null;
  return supabaseFetch('rpc/app_edge_reconcile_airdrop_provider_window', {
    method: 'POST',
    body: JSON.stringify({
      p_provider: source.provider,
      p_source_event_ids: ids,
      p_window_start: windowStart,
      p_window_end: windowEnd,
    }),
  });
}
async function refreshOnce({ force = false } = {}) {
  if (refreshPromise && !force) return refreshPromise;
  refreshPromise = (async () => {
    if (!state.enabled || stopping) return false;
    state.refreshInFlight = true;
    state.lastRefreshStartedAt = nowIso();
    try {
      if (!state.publicSnapshotLoaded) {
        try { await loadDbSnapshot({ force: true }); } catch (error) { state.lastError = `db_load:${String(error?.message || error)}`; }
      }
      const rows = [];
      const reconcileGroups = [];
      // Run sources sequentially to keep outbound pressure flat and bounded.
      for (const source of OFFICIAL_SOURCES) {
        if (stopping) break;
        try {
          const sourceRows = await collectSource(source);
          const enrichedRows = await enrichSourceContent(source, sourceRows);
          rows.push(...enrichedRows);
          const sourceState = state.sourceStates[source.id] || {};
          if (sourceState.last_collection_complete === true) {
            reconcileGroups.push({ source: { ...source, last_collection_complete: true }, rows: enrichedRows });
          }
        } catch (error) {
          const sourceState = state.sourceStates[source.id] ||= {};
          sourceState.last_error = String(error?.message || error);
          sourceState.failures = finiteNumber(sourceState.failures, 0) + 1;
        }
        await sleep(250);
      }
      if (rows.length) {
        const previousSnapshot = publicSnapshot.slice();
        mergePublicSnapshot(rows);
        if (state.supabaseConfigured) {
          try {
            const persisted = await persistRows(rows, previousSnapshot);
            if (persisted.length) mergePublicSnapshot(persisted);
            let reconciled = false;
            for (const group of reconcileGroups) {
              try {
                const result = await reconcileAirdropProviderWindow(group.source, group.rows);
                if (result) reconciled = true;
              } catch (error) {
                const sourceState = state.sourceStates[group.source.id] ||= {};
                sourceState.lifecycle_reconcile_error = String(error?.message || error);
              }
            }
            if (reconciled) await loadDbSnapshot({ force: true });
          } catch (error) {
            state.persistFailures++;
            state.lastError = `persist:${String(error?.message || error)}`;
          }
        }
      }
      state.refreshes++;
      state.lastRefreshCompletedAt = nowIso();
      if (!state.lastError?.startsWith('persist:')) state.lastError = null;
      scheduleTranslationTick(5_000);
      return true;
    } catch (error) {
      state.refreshFailures++;
      state.lastError = String(error?.message || error);
      return false;
    } finally {
      state.refreshInFlight = false;
    }
  })();
  try { return await refreshPromise; }
  finally { refreshPromise = null; }
}
function scheduleNext(delayMs = state.refreshMs) {
  if (timer) clearTimeout(timer);
  if (stopping || !state.enabled) return;
  const jitter = Math.round(Math.min(45_000, delayMs * 0.08) * (Math.random() - 0.5));
  timer = setTimeout(async () => {
    await refreshOnce();
    scheduleNext(state.refreshMs);
  }, Math.max(5_000, delayMs + jitter));
  timer.unref?.();
}
function publicHealth() {
  const sources = {};
  for (const source of OFFICIAL_SOURCES) {
    sources[source.id] = {
      provider: source.provider,
      official_only: true,
      structured_official_api: Boolean(source.json_roots?.length),
      structured_official_x: Boolean(source.social_x_handles?.length),
      official_x_handles: source.social_x_handles || [],
      native_content_mode: source.provider === 'bybit' ? 'documented_announcement_api_description' : (source.social_x_handles?.length ? 'official_x_post' : 'official_article_html'),
      root_count: (source.roots?.length || 0) + (source.json_roots?.length || 0) + (source.social_x_handles?.length || 0),
      ...(state.sourceStates[source.id] || {}),
    };
  }
  return {
    schema: STEP_SCHEMA,
    started: state.started,
    enabled: state.enabled,
    supabase_configured: state.supabaseConfigured,
    refresh_seconds: Math.round(state.refreshMs / 1000),
    refresh_in_flight: state.refreshInFlight,
    refreshes: state.refreshes,
    refresh_failures: state.refreshFailures,
    last_refresh_started_at: state.lastRefreshStartedAt,
    last_refresh_completed_at: state.lastRefreshCompletedAt,
    last_persist_at: state.lastPersistAt,
    last_db_load_at: state.lastDbLoadAt,
    last_error: state.lastError,
    official_source_count: OFFICIAL_SOURCES.length,
    official_sources: OFFICIAL_SOURCES.map((s) => s.provider),
    sources,
    upstream_requests: state.upstreamRequests,
    upstream_not_modified: state.upstreamNotModified,
    upstream_failures: state.upstreamFailures,
    detail_requests: state.detailRequests,
    content_enrich_requests: state.contentEnrichRequests,
    content_enrich_failures: state.contentEnrichFailures,
    content_rows: publicSnapshot.filter((row) => contentUsableForProvider(row?.provider, row?.content_text)).length,
    media_items_supported: true,
    media_source_policy: 'official_page_metadata_or_shared_x_only',
    media_rows: publicSnapshot.filter((row) => normalizeContentMediaItems(row?.media_items).length > 0).length,
    detail_reads: state.detailReads,
    persisted_rows: state.persistedRows,
    persist_successes: state.persistSuccesses,
    persist_failures: state.persistFailures,
    persistence_cost_guard: {
      version: 'step1060_11_1_airdrop_persist_noop_guard_v2',
      postgres_nul_sanitized_before_persist: true,
      unchanged_active_rows_write_skipped: true,
      removed_rows_can_reactivate: true,
      status_or_content_changes_write_immediately: true,
      heartbeat_hours: PERSIST_HEARTBEAT_MS / 3_600_000,
      rows_considered: state.persistRowsConsidered,
      rows_written: state.persistRowsWritten,
      rows_noop_skipped: state.persistRowsNoopSkipped,
      heartbeat_rows_written: state.persistHeartbeatRows,
      noop_batches: state.persistNoopBatches,
    },
    public_reads: state.publicReads,
    public_snapshot_loaded: state.publicSnapshotLoaded,
    public_snapshot_rows: state.publicSnapshotRows,
    public_snapshot_db_reads: state.publicSnapshotDbReads,
    shared_bridge_reads: state.sharedBridgeReads,
    user_read_upstream_requests: 0,
    user_read_supabase_requests: 0,
    upstream_requests_scale_with_users: false,
    auto_translation: {
      shared_background_only: false,
      shared_background_and_on_demand: true,
      policy: 'english_to_chinese_title_plus_short_detail_auto',
      chinese_to_english_auto: false,
      short_detail_max_chars: AUTO_SHORT_DETAIL_CHARS,
      user_translation_requests: getSharedTranslationHealth().user_translation_requests,
      runs: state.translationRuns,
      rows_translated: state.translationRowsTranslated,
      title_rows_translated: state.translationTitleRowsTranslated,
      detail_rows_translated: state.translationDetailRowsTranslated,
      failures: state.translationFailures,
      db_writes: state.translationDbWrites,
      last_run_at: state.translationLastRunAt,
      last_success_at: state.translationLastSuccessAt,
      last_error: state.translationLastError,
      hot_window_rows: TRANSLATION_WINDOW_ROWS,
      default_landing_priority: 'binance_alpha_first',
      provider_balanced_titles: true,
      rows_per_tick: TRANSLATION_ROWS_PER_TICK,
      detail_rows_per_tick: TRANSLATION_DETAIL_ROWS_PER_TICK,
      shared_service: getSharedTranslationHealth(),
    },
    official_domain_whitelist_only: true,
    stale_snapshot_preserved_on_failure: true,
  };
}
function filteredRows(url) {
  const limit = clampInt(url.searchParams.get('limit'), 1, PUBLIC_ENDPOINT_MAX_LIMIT, 80);
  const provider = text(url.searchParams.get('provider')).toLowerCase();
  const status = text(url.searchParams.get('status')).toLowerCase();
  const category = text(url.searchParams.get('category')).toLowerCase();
  let rows = publicSnapshot;
  if (provider && provider !== 'all') rows = rows.filter((row) => row.provider === provider);
  if (status && status !== 'all') rows = rows.filter((row) => row.status === status);
  if (category && category !== 'all') rows = rows.filter((row) => row.category === category);
  return rows.slice(0, limit).map((row) => listEventView(row));
}

export function startAirdropWatch() {
  if (state.started) return;
  state.started = true;
  stopping = false;
  if (!state.enabled) return;
  // DB is only a startup/history seed. Public reads never query Supabase directly.
  loadDbSnapshot({ force: true })
    .then(() => scheduleTranslationTick(5_000))
    .catch((error) => {
      state.lastError = `db_load:${String(error?.message || error)}`;
      scheduleTranslationTick(15_000);
    });
  scheduleNext(3_000);
}

export function stopAirdropWatch() {
  stopping = true;
  if (timer) clearTimeout(timer);
  timer = null;
  if (translationTimer) clearTimeout(translationTimer);
  translationTimer = null;
}

export function getAirdropWatchHealth() {
  return publicHealth();
}

export async function handleAirdropWatch(req, res, url) {
  if (url.pathname === '/api/airdrop-watch/health') {
    json(res, 200, { ok: true, airdrop_watch: publicHealth(), time: nowIso() });
    return true;
  }
  if (url.pathname === '/api/airdrop-watch/public/detail') {
    state.detailReads++;
    // Public detail reads are memory-only. Startup DB seeding is owned by
    // startAirdropWatch(); a user request must never initiate Supabase/upstream work.
    const row = detailRow(url);
    if (!row) {
      json(res, 404, { ok: false, schema: STEP_SCHEMA, error: 'airdrop_not_found' });
      return true;
    }
    json(res, 200, {
      ok: true,
      schema: STEP_SCHEMA,
      row,
      shared_public_snapshot: true,
      official_only: true,
      user_read_upstream_requests: 0,
      user_read_supabase_requests: 0,
      time: nowIso(),
    }, { cacheSeconds: 30 });
    return true;
  }
  if (url.pathname === '/api/airdrop-watch/public') {
    state.publicReads++;
    // Public list reads are memory-only for the same reason as detail reads.
    const rows = filteredRows(url);
    json(res, 200, {
      ok: true,
      schema: STEP_SCHEMA,
      rows,
      row_count: rows.length,
      providers: OFFICIAL_SOURCES.map((source) => ({ key: source.provider, name: source.display_name })),
      shared_public_snapshot: true,
      official_only: true,
      user_read_upstream_requests: 0,
      user_read_supabase_requests: 0,
      upstream_requests_scale_with_users: false,
      last_refresh_at: state.lastRefreshCompletedAt,
      time: nowIso(),
    }, { cacheSeconds: 15 });
    return true;
  }
  return false;
}

// Test-only pure helpers. Importing this module never starts collectors; proxy.mjs owns startup.
export const __airdropWatchTest = Object.freeze({
  extractCandidates,
  extractBybitApiCandidates,
  rowsFromSocialXEvents,
  officialXUrlAllowed,
  normalizeCandidateUrl,
  categoryFor,
  projectSymbolFromTitle,
  extractRewardText,
  htmlArticleText,
  contentUsable,
  contentUsableForProvider,
  publicEventView,
  statusFor,
  sources: OFFICIAL_SOURCES,
});
