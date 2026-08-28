import crypto from 'node:crypto';

const ENABLED = String(process.env.KAKA_CONTENT_AUTO_TRANSLATION_ENABLED || '1') !== '0';
const GOOGLE_API_KEY = String(process.env.KAKA_GOOGLE_TRANSLATION_API_KEY || process.env.GOOGLE_TRANSLATION_API_KEY || '').trim();
const MYMEMORY_EMAIL = String(process.env.KAKA_TRANSLATION_CONTACT_EMAIL || '').trim();
const REQUEST_TIMEOUT_MS = Math.max(3000, Math.min(15000, Number(process.env.KAKA_TRANSLATION_TIMEOUT_MS || 8000) || 8000));
const MAX_FIELD_CHARS = Math.max(800, Math.min(12000, Number(process.env.KAKA_TRANSLATION_MAX_FIELD_CHARS || 6000) || 6000));
const MYMEMORY_MAX_BYTES = 450;
const MYMEMORY_MAX_SEGMENTS = 32;
const PROVIDER_MIN_GAP_MS = Math.max(80, Math.min(1200, Number(process.env.KAKA_TRANSLATION_REQUEST_GAP_MS || 220) || 220));

const state = {
  enabled: ENABLED,
  provider_mode: GOOGLE_API_KEY ? 'google_cloud_translation_v2' : 'mymemory_shared_fallback',
  google_configured: Boolean(GOOGLE_API_KEY),
  requests: 0,
  google_requests: 0,
  mymemory_requests: 0,
  successes: 0,
  failures: 0,
  translated_fields: 0,
  translated_characters: 0,
  skipped_same_language: 0,
  skipped_unclassified: 0,
  last_request_at: null,
  last_success_at: null,
  last_error: null,
};

let lane = Promise.resolve();
let lastProviderRequestAtMs = 0;

function text(value) { return String(value ?? '').trim(); }
function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }
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
function normalizeWhitespace(raw) {
  return text(raw)
    .replace(/\r/g, '')
    .replace(/[ \t]+/g, ' ')
    .replace(/ *\n */g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
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
function googleTarget(target) { return target === 'zh' ? 'zh-CN' : 'en'; }
function myMemoryPair(source, target) {
  const from = source === 'zh' ? 'zh-CN' : 'en';
  const to = target === 'zh' ? 'zh-CN' : 'en';
  return `${from}|${to}`;
}

function splitUtf8Segments(raw) {
  const value = normalizeWhitespace(raw).slice(0, MAX_FIELD_CHARS);
  if (!value) return [];
  const segments = [];
  let current = '';
  const flush = () => {
    const clean = current.trim();
    if (clean) segments.push(clean);
    current = '';
  };
  const units = value.split(/(?<=[.!?。！？；;:\n])\s*/u).filter(Boolean);
  for (const unitRaw of units) {
    let unit = unitRaw;
    while (unit) {
      const candidate = current ? `${current} ${unit}` : unit;
      if (Buffer.byteLength(candidate, 'utf8') <= MYMEMORY_MAX_BYTES) {
        current = candidate;
        unit = '';
        continue;
      }
      if (current) {
        flush();
        continue;
      }
      let cut = Math.min(unit.length, 300);
      while (cut > 1 && Buffer.byteLength(unit.slice(0, cut), 'utf8') > MYMEMORY_MAX_BYTES) cut--;
      if (cut <= 1) break;
      segments.push(unit.slice(0, cut).trim());
      unit = unit.slice(cut).trim();
      if (segments.length >= MYMEMORY_MAX_SEGMENTS) break;
    }
    if (segments.length >= MYMEMORY_MAX_SEGMENTS) break;
  }
  if (segments.length < MYMEMORY_MAX_SEGMENTS) flush();
  return segments.slice(0, MYMEMORY_MAX_SEGMENTS);
}

async function serializedProviderRequest(fn) {
  const run = lane.then(async () => {
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
  try {
    return await fetch(url, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function translateGoogle(value, source, target) {
  state.requests++;
  state.google_requests++;
  return serializedProviderRequest(async () => {
    const response = await fetchWithTimeout(`https://translation.googleapis.com/language/translate/v2?key=${encodeURIComponent(GOOGLE_API_KEY)}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json; charset=utf-8' },
      body: JSON.stringify({
        q: value.slice(0, MAX_FIELD_CHARS),
        source: source === 'zh' ? 'zh-CN' : 'en',
        target: googleTarget(target),
        format: 'text',
      }),
    });
    const raw = await response.text();
    if (!response.ok) throw new Error(`google_translation_http_${response.status}:${raw.slice(0, 180)}`);
    let payload = null;
    try { payload = JSON.parse(raw); } catch (_) {}
    const translated = decodeHtmlEntities(payload?.data?.translations?.[0]?.translatedText || '');
    if (!translated) throw new Error('google_translation_empty');
    return translated;
  });
}

async function translateMyMemory(value, source, target) {
  const segments = splitUtf8Segments(value);
  if (!segments.length) return '';
  const out = [];
  for (const segment of segments) {
    state.requests++;
    state.mymemory_requests++;
    const translated = await serializedProviderRequest(async () => {
      const url = new URL('https://api.mymemory.translated.net/get');
      url.searchParams.set('q', segment);
      url.searchParams.set('langpair', myMemoryPair(source, target));
      url.searchParams.set('mt', '1');
      if (MYMEMORY_EMAIL) url.searchParams.set('de', MYMEMORY_EMAIL);
      const response = await fetchWithTimeout(url, {
        headers: {
          accept: 'application/json',
          'user-agent': 'KakaWeb3-SharedContentTranslation/1.0',
        },
      });
      const raw = await response.text();
      if (!response.ok) throw new Error(`mymemory_http_${response.status}:${raw.slice(0, 180)}`);
      let payload = null;
      try { payload = JSON.parse(raw); } catch (_) {}
      const result = decodeHtmlEntities(payload?.responseData?.translatedText || '');
      if (!result) throw new Error('mymemory_translation_empty');
      return result;
    });
    out.push(translated);
  }
  return normalizeWhitespace(out.join('\n'));
}

async function translateField(raw, source, target) {
  const value = normalizeWhitespace(raw);
  if (!value) return '';
  if (GOOGLE_API_KEY) return translateGoogle(value, source, target);
  return translateMyMemory(value, source, target);
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

export async function translateContentFields({ fields, existingTranslations = {}, existingSourceHash = '' }) {
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

  const needed = [];
  for (const [field, value] of Object.entries(normalizedFields)) {
    const source = detectContentLanguage(value);
    const target = targetForSource(source);
    if (!target) {
      state.skipped_unclassified++;
      continue;
    }
    if (source === target) {
      state.skipped_same_language++;
      continue;
    }
    needed.push({ field, value, source, target });
  }
  if (!needed.length) {
    return { changed: existingSourceHash !== sourceHash, source_hash: sourceHash, translations: {}, translated_fields: 0 };
  }

  // A content edit invalidates the old translation atomically; never expose stale translated text.
  const next = existingSourceHash === sourceHash ? existing : {};
  let translatedFields = 0;
  let changed = existingSourceHash !== sourceHash;

  for (const job of needed) {
    const current = text(next?.[job.target]?.[job.field]);
    if (current && existingSourceHash === sourceHash) continue;
    try {
      const translated = await translateField(job.value, job.source, job.target);
      if (!translated) continue;
      next[job.target] ||= {};
      next[job.target][job.field] = translated;
      translatedFields++;
      changed = true;
      state.successes++;
      state.translated_fields++;
      state.translated_characters += job.value.length;
      state.last_success_at = new Date().toISOString();
      state.last_error = null;
    } catch (error) {
      state.failures++;
      state.last_error = String(error?.name === 'AbortError' ? 'translation_timeout' : error?.message || error);
      throw error;
    }
  }

  return { changed, source_hash: sourceHash, translations: next, translated_fields: translatedFields };
}

export function getSharedTranslationHealth() {
  return { ...state };
}
