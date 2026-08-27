const STEP_SCHEMA = 'step1044_x_configurable_celebrity_watch_v1';
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
const X_API_BEARER_TOKEN = String(process.env.X_API_BEARER_TOKEN || '').trim();
const X_API_BASE = String(process.env.X_API_BASE || 'https://api.x.com').replace(/\/+$/, '');
const POST_READ_REFERENCE_USD = Number(process.env.X_POST_READ_REFERENCE_USD || '0.005');
const RULE_TAG_PREFIX = 'kaka-social:';
const SETTINGS_TABLE = 'app_social_watch_settings';
const ACCOUNTS_TABLE = 'app_social_watch_accounts';
const EVENTS_TABLE = 'app_social_watch_events';
const NOTIFICATIONS_TABLE = 'app_notifications';
const DEFAULT_CONFIG_REFRESH_MS = 30_000;
const MIN_CONFIG_REFRESH_MS = 15_000;
const MAX_CONFIG_REFRESH_MS = 3_600_000;
const STREAM_RECONNECT_MAX_MS = 60_000;
const STREAM_STALL_MS = 35_000;
const RULE_BATCH_SIZE = 25;

const state = {
  started: false,
  enabled: false,
  supabaseConfigured: Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY),
  xTokenConfigured: Boolean(X_API_BEARER_TOKEN),
  configRefreshMs: DEFAULT_CONFIG_REFRESH_MS,
  maxActiveAccounts: 100,
  maxDailyPosts: 500,
  autoAppNotification: true,
  activeAccounts: 0,
  totalAccounts: 0,
  managedRules: 0,
  streamConnected: false,
  streamConnecting: false,
  streamReconnects: 0,
  streamLastConnectedAt: null,
  streamLastLineAt: null,
  lastConfigSyncAt: null,
  lastRuleSyncAt: null,
  lastEventAt: null,
  lastEventId: null,
  lastEventHandle: null,
  lastPersistAt: null,
  lastNotificationAt: null,
  lastError: null,
  lastXHttpStatus: null,
  dailyDateUtc: new Date().toISOString().slice(0, 10),
  dailyPostsReceived: 0,
  dailyPostsPersisted: 0,
  dailyNotificationsCreated: 0,
  dailyCapReached: false,
  configReads: 0,
  ruleReads: 0,
  ruleWrites: 0,
  ruleSyncFailures: 0,
  ruleSyncNextAllowedAt: null,
  xPostReads: 0,
  userReadXRequests: 0,
};

let settingsTimer = null;
let streamAbort = null;
let streamLoopPromise = null;
let stopping = false;
let desiredAccountsByTag = new Map();
let lastConfigSignature = '';

function nowIso() { return new Date().toISOString(); }
function text(v) { return String(v ?? '').trim(); }
function finiteNumber(v, fallback = 0) { const n = Number(v); return Number.isFinite(n) ? n : fallback; }
function clampInt(v, min, max, fallback) {
  const n = Math.trunc(finiteNumber(v, fallback));
  return Math.max(min, Math.min(max, n));
}
function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }
function authHeaders(extra = {}) {
  return {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    'content-type': 'application/json',
    ...extra,
  };
}
function xHeaders(extra = {}) {
  return {
    authorization: `Bearer ${X_API_BEARER_TOKEN}`,
    accept: 'application/json',
    'user-agent': 'KakaWeb3-Step1044-SocialWatch/1.0',
    ...extra,
  };
}
function json(res, status, body) {
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
  });
  res.end(JSON.stringify(body));
}
function normalizeHandle(raw) {
  const v = text(raw).replace(/^@/, '');
  return /^[A-Za-z0-9_]{1,15}$/.test(v) ? v : '';
}
function ruleTag(id) { return `${RULE_TAG_PREFIX}${text(id)}`; }
function accountRuleValue(account) {
  const handle = normalizeHandle(account.handle);
  if (!handle) return '';
  const parts = [`from:${handle}`];
  if (account.include_reposts !== true) parts.push('-is:retweet');
  if (account.include_replies !== true) parts.push('-is:reply');
  return parts.join(' ');
}
function resetDailyIfNeeded() {
  const today = new Date().toISOString().slice(0, 10);
  if (today === state.dailyDateUtc) return;
  state.dailyDateUtc = today;
  state.dailyPostsReceived = 0;
  state.dailyPostsPersisted = 0;
  state.dailyNotificationsCreated = 0;
  state.dailyCapReached = false;
}
function publicHealth() {
  resetDailyIfNeeded();
  return {
    schema: STEP_SCHEMA,
    started: state.started,
    enabled: state.enabled,
    supabase_configured: state.supabaseConfigured,
    x_api_bearer_configured: state.xTokenConfigured,
    active_accounts: state.activeAccounts,
    total_accounts: state.totalAccounts,
    managed_rules: state.managedRules,
    config_refresh_seconds: Math.round(state.configRefreshMs / 1000),
    max_active_accounts: state.maxActiveAccounts,
    max_daily_posts: state.maxDailyPosts,
    auto_app_notification: state.autoAppNotification,
    stream_connected: state.streamConnected,
    stream_connecting: state.streamConnecting,
    stream_reconnects: state.streamReconnects,
    stream_last_connected_at: state.streamLastConnectedAt,
    stream_last_line_at: state.streamLastLineAt,
    last_config_sync_at: state.lastConfigSyncAt,
    last_rule_sync_at: state.lastRuleSyncAt,
    last_event_at: state.lastEventAt,
    last_event_id: state.lastEventId,
    last_event_handle: state.lastEventHandle,
    last_persist_at: state.lastPersistAt,
    last_notification_at: state.lastNotificationAt,
    last_error: state.lastError,
    last_x_http_status: state.lastXHttpStatus,
    daily_date_utc: state.dailyDateUtc,
    daily_posts_received: state.dailyPostsReceived,
    daily_posts_persisted: state.dailyPostsPersisted,
    daily_notifications_created: state.dailyNotificationsCreated,
    daily_cap_reached: state.dailyCapReached,
    current_reference_post_read_usd: Number.isFinite(POST_READ_REFERENCE_USD) ? POST_READ_REFERENCE_USD : null,
    estimated_daily_post_read_cost_usd: Number.isFinite(POST_READ_REFERENCE_USD)
      ? Number((state.dailyPostsReceived * POST_READ_REFERENCE_USD).toFixed(4))
      : null,
    user_read_x_requests: 0,
    x_requests_scale_with_users: false,
    config_reads: state.configReads,
    rule_reads: state.ruleReads,
    rule_writes: state.ruleWrites,
    rule_sync_failures: state.ruleSyncFailures,
    rule_sync_next_allowed_at: state.ruleSyncNextAllowedAt,
    x_post_reads: state.xPostReads,
  };
}

async function supabaseFetch(path, init = {}) {
  if (!state.supabaseConfigured) throw new Error('social_watch_supabase_not_configured');
  const response = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
    ...init,
    headers: { ...authHeaders(), ...(init.headers || {}) },
  });
  const raw = await response.text();
  let payload = null;
  try { payload = raw ? JSON.parse(raw) : null; } catch (_) { payload = raw; }
  if (!response.ok) throw new Error(`social_watch_supabase_http_${response.status}:${text(raw).slice(0, 280)}`);
  return payload;
}

async function xFetch(path, init = {}) {
  if (!state.xTokenConfigured) throw new Error('social_watch_x_token_not_configured');
  const response = await fetch(`${X_API_BASE}${path}`, {
    ...init,
    headers: { ...xHeaders(), ...(init.headers || {}) },
  });
  state.lastXHttpStatus = response.status;
  return response;
}

async function readConfig() {
  const [settingsRows, accountRows] = await Promise.all([
    supabaseFetch(`${SETTINGS_TABLE}?id=eq.default&select=*`),
    supabaseFetch(`${ACCOUNTS_TABLE}?source=eq.x&select=*&order=sort_order.asc,created_at.asc&limit=1000`),
  ]);
  state.configReads += 2;
  const settings = Array.isArray(settingsRows) && settingsRows[0] ? settingsRows[0] : {};
  const accounts = Array.isArray(accountRows) ? accountRows : [];
  state.enabled = settings.enabled !== false;
  state.autoAppNotification = settings.auto_app_notification !== false;
  state.maxActiveAccounts = clampInt(settings.max_active_accounts, 1, 1000, 100);
  state.maxDailyPosts = clampInt(settings.max_daily_posts, 1, 100000, 500);
  state.configRefreshMs = clampInt(settings.config_refresh_seconds, 15, 3600, 30) * 1000;
  state.totalAccounts = accounts.length;
  const valid = accounts
    .filter((a) => a?.is_active === true && normalizeHandle(a?.handle))
    .slice(0, state.maxActiveAccounts);
  state.activeAccounts = valid.length;
  desiredAccountsByTag = new Map(valid.map((a) => [ruleTag(a.id), a]));
  state.lastConfigSyncAt = nowIso();

  const sig = JSON.stringify({
    enabled: state.enabled,
    auto: state.autoAppNotification,
    max: state.maxActiveAccounts,
    cap: state.maxDailyPosts,
    accounts: valid.map((a) => ({
      id: a.id,
      handle: normalizeHandle(a.handle),
      replies: a.include_replies === true,
      reposts: a.include_reposts === true,
      notify: a.notify_app !== false,
    })),
  });
  const changed = sig !== lastConfigSignature;
  lastConfigSignature = sig;

  // Keep paused statuses understandable in the admin page.
  const activeIds = new Set(valid.map((a) => text(a.id)));
  const overflowActive = accounts.filter((a) =>
    a?.is_active === true &&
    !activeIds.has(text(a?.id)) &&
    (text(a?.rule_status) !== 'paused_limit' || text(a?.rule_id))
  );
  const inactive = accounts.filter((a) =>
    a?.is_active !== true &&
    (text(a?.rule_status) !== 'paused' || text(a?.rule_id))
  );
  await Promise.all([
    ...overflowActive.map((a) => updateAccountStatus(a.id, { rule_status: 'paused_limit', rule_id: null })),
    ...inactive.map((a) => updateAccountStatus(a.id, { rule_status: 'paused', rule_id: null })),
  ]).catch(() => undefined);
  return changed;
}

async function updateAccountStatus(id, fields) {
  if (!id) return;
  await supabaseFetch(`${ACCOUNTS_TABLE}?id=eq.${encodeURIComponent(id)}`, {
    method: 'PATCH',
    headers: { Prefer: 'return=minimal' },
    body: JSON.stringify(fields),
  });
}

async function listXRules() {
  const response = await xFetch('/2/tweets/search/stream/rules');
  state.ruleReads++;
  const raw = await response.text();
  let payload = {};
  try { payload = raw ? JSON.parse(raw) : {}; } catch (_) { payload = {}; }
  if (!response.ok) throw new Error(`social_watch_x_rules_http_${response.status}:${text(raw).slice(0, 280)}`);
  return Array.isArray(payload?.data) ? payload.data : [];
}

async function writeXRules(body) {
  const response = await xFetch('/2/tweets/search/stream/rules', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  state.ruleWrites++;
  const raw = await response.text();
  let payload = {};
  try { payload = raw ? JSON.parse(raw) : {}; } catch (_) { payload = {}; }
  if (!response.ok) throw new Error(`social_watch_x_rules_write_http_${response.status}:${text(raw).slice(0, 280)}`);
  return payload;
}

function chunks(values, size) {
  const out = [];
  for (let i = 0; i < values.length; i += size) out.push(values.slice(i, i + size));
  return out;
}

async function syncRules() {
  resetDailyIfNeeded();
  if (!state.enabled) {
    if (streamAbort) streamAbort.abort('social_watch_disabled');
    return;
  }
  if (desiredAccountsByTag.size === 0 && streamAbort) {
    streamAbort.abort('social_watch_empty');
  }
  if (!state.xTokenConfigured) {
    for (const account of desiredAccountsByTag.values()) {
      if (text(account?.rule_status) === 'waiting_x_api' && !text(account?.rule_id) && !text(account?.last_error)) continue;
      updateAccountStatus(account.id, {
        rule_status: 'waiting_x_api',
        rule_id: null,
        last_error: null,
      }).catch(() => undefined);
    }
    state.managedRules = 0;
    state.lastRuleSyncAt = nowIso();
    state.ruleSyncFailures = 0;
    state.ruleSyncNextAllowedAt = null;
    return;
  }

  const currentRules = await listXRules();
  const managed = currentRules.filter((r) => text(r?.tag).startsWith(RULE_TAG_PREFIX));
  const byTag = new Map(managed.map((r) => [text(r.tag), r]));
  const deleteIds = [];
  const additions = [];

  for (const rule of managed) {
    const tag = text(rule.tag);
    const desired = desiredAccountsByTag.get(tag);
    const desiredValue = desired ? accountRuleValue(desired) : '';
    if (!desired || !desiredValue || text(rule.value) !== desiredValue) deleteIds.push(text(rule.id));
  }
  for (const [tag, account] of desiredAccountsByTag) {
    const value = accountRuleValue(account);
    if (!value) continue;
    const existing = byTag.get(tag);
    if (!existing || text(existing.value) !== value) additions.push({ value, tag });
  }

  for (const batch of chunks(deleteIds.filter(Boolean), RULE_BATCH_SIZE)) {
    if (batch.length) await writeXRules({ delete: { ids: batch } });
  }
  for (const batch of chunks(additions, RULE_BATCH_SIZE)) {
    if (batch.length) await writeXRules({ add: batch });
  }

  const finalRules = await listXRules();
  const finalManaged = finalRules.filter((r) => text(r?.tag).startsWith(RULE_TAG_PREFIX));
  state.managedRules = finalManaged.length;
  state.lastRuleSyncAt = nowIso();
  state.ruleSyncFailures = 0;
  state.ruleSyncNextAllowedAt = null;
  const finalByTag = new Map(finalManaged.map((r) => [text(r.tag), r]));
  await Promise.all([...desiredAccountsByTag.entries()].map(async ([tag, account]) => {
    const rule = finalByTag.get(tag);
    await updateAccountStatus(account.id, {
      rule_id: rule ? text(rule.id) : null,
      rule_status: rule ? 'active' : 'pending',
      last_rule_sync_at: state.lastRuleSyncAt,
      last_error: rule ? null : 'X rule pending',
    });
  }));
}

function notificationText(content) {
  const clean = text(content).replace(/\s+/g, ' ');
  if (clean.length <= 420) return clean;
  return `${clean.slice(0, 417)}…`;
}

async function insertEvent(account, post, tag) {
  const postId = text(post?.id);
  const handle = normalizeHandle(account?.handle);
  if (!postId || !handle) return false;
  const postUrl = `https://x.com/${handle}/status/${postId}`;
  const publishedAt = text(post?.created_at) || nowIso();
  const body = {
    watch_account_id: account.id,
    source: 'x',
    source_post_id: postId,
    author_handle: handle,
    author_name: text(account.display_name),
    content: text(post?.text),
    post_url: postUrl,
    published_at: publishedAt,
    language: text(post?.lang) || null,
    matched_rule_tag: tag,
  };
  const inserted = await supabaseFetch(
    `${EVENTS_TABLE}?on_conflict=source,source_post_id&select=id`,
    {
      method: 'POST',
      headers: { Prefer: 'resolution=ignore-duplicates,return=representation' },
      body: JSON.stringify(body),
    },
  );
  const row = Array.isArray(inserted) ? inserted[0] : null;
  if (!row) return false;
  state.dailyPostsPersisted++;
  state.lastPersistAt = nowIso();

  let notificationId = null;
  const shouldNotify = state.autoAppNotification && account.notify_app !== false;
  if (shouldNotify) {
    const displayName = text(account.display_name) || `@${handle}`;
    const title = `${displayName} · X 新动态`;
    const content = notificationText(post?.text);
    try {
      const notificationRows = await supabaseFetch(`${NOTIFICATIONS_TABLE}?select=id`, {
        method: 'POST',
        headers: { Prefer: 'return=representation' },
        body: JSON.stringify({
          title,
          content,
          translations: {
            en: {
              title: `${displayName} · New post on X`,
              content,
            },
          },
          type: 'activity',
          link_url: postUrl,
          is_global: true,
          user_id: null,
          is_active: true,
          target_type: 'external_url',
          target_id: '',
        }),
      });
      notificationId = Array.isArray(notificationRows) && notificationRows[0]
        ? text(notificationRows[0].id)
        : null;
      state.dailyNotificationsCreated++;
      state.lastNotificationAt = nowIso();
    } catch (error) {
      state.lastError = `notification:${String(error?.message || error)}`;
    }
  }

  await Promise.all([
    updateAccountStatus(account.id, {
      last_post_id: postId,
      last_post_at: publishedAt,
      last_error: null,
    }),
    supabaseFetch(`${EVENTS_TABLE}?id=eq.${encodeURIComponent(row.id)}`, {
      method: 'PATCH',
      headers: { Prefer: 'return=minimal' },
      body: JSON.stringify({
        notification_created: Boolean(notificationId),
        notification_id: notificationId,
      }),
    }),
  ]).catch(() => undefined);
  return true;
}

async function handleStreamPayload(payload) {
  const post = payload?.data;
  if (!post?.id) return;
  resetDailyIfNeeded();
  state.dailyPostsReceived++;
  state.xPostReads++;
  state.lastEventAt = nowIso();
  state.lastEventId = text(post.id);
  const matches = Array.isArray(payload?.matching_rules) ? payload.matching_rules : [];
  const managedMatch = matches.find((r) => text(r?.tag).startsWith(RULE_TAG_PREFIX));
  const tag = text(managedMatch?.tag);
  const account = desiredAccountsByTag.get(tag);
  if (!account) return;
  state.lastEventHandle = normalizeHandle(account.handle);
  try {
    await insertEvent(account, post, tag);
  } catch (error) {
    state.lastError = `event:${String(error?.message || error)}`;
    updateAccountStatus(account.id, { last_error: state.lastError }).catch(() => undefined);
  }
  if (state.dailyPostsReceived >= state.maxDailyPosts) {
    state.dailyCapReached = true;
    if (streamAbort) streamAbort.abort('social_watch_daily_cap');
  }
}

async function consumeStream(signal) {
  const params = new URLSearchParams({
    'tweet.fields': 'id,text,created_at,author_id,lang,conversation_id,edit_history_tweet_ids,referenced_tweets',
  });
  const response = await xFetch(`/2/tweets/search/stream?${params.toString()}`, { signal });
  state.lastXHttpStatus = response.status;
  if (!response.ok || !response.body) {
    const raw = await response.text().catch(() => '');
    throw new Error(`social_watch_x_stream_http_${response.status}:${text(raw).slice(0, 300)}`);
  }
  state.streamConnected = true;
  state.streamConnecting = false;
  state.streamLastConnectedAt = nowIso();
  state.lastError = null;

  const decoder = new TextDecoder();
  let buffer = '';
  let lastLineMs = Date.now();
  const watchdog = setInterval(() => {
    if (Date.now() - lastLineMs > STREAM_STALL_MS && streamAbort && !streamAbort.signal.aborted) {
      streamAbort.abort('social_watch_stream_stall');
    }
  }, 5_000);
  watchdog.unref?.();
  try {
    for await (const chunk of response.body) {
      if (signal.aborted) break;
      buffer += decoder.decode(chunk, { stream: true });
      while (true) {
        const nl = buffer.indexOf('\n');
        if (nl < 0) break;
        const rawLine = buffer.slice(0, nl).replace(/\r$/, '');
        buffer = buffer.slice(nl + 1);
        lastLineMs = Date.now();
        state.streamLastLineAt = nowIso();
        if (!rawLine.trim()) continue;
        let payload;
        try { payload = JSON.parse(rawLine); } catch (_) { continue; }
        await handleStreamPayload(payload);
      }
    }
  } finally {
    clearInterval(watchdog);
    state.streamConnected = false;
    state.streamConnecting = false;
  }
}

async function streamLoop() {
  let backoff = 1_000;
  while (!stopping) {
    resetDailyIfNeeded();
    if (!state.enabled || !state.xTokenConfigured || desiredAccountsByTag.size === 0 || state.dailyCapReached) {
      state.streamConnected = false;
      state.streamConnecting = false;
      await sleep(2_000);
      continue;
    }
    streamAbort = new AbortController();
    state.streamConnecting = true;
    try {
      await consumeStream(streamAbort.signal);
      backoff = 1_000;
    } catch (error) {
      if (stopping) break;
      const message = String(error?.message || error);
      if (!message.includes('social_watch_disabled_or_empty') && !message.includes('social_watch_daily_cap')) {
        state.lastError = message;
        state.streamReconnects++;
      }
      state.streamConnected = false;
      state.streamConnecting = false;
      await sleep(backoff);
      backoff = Math.min(STREAM_RECONNECT_MAX_MS, Math.round(backoff * 1.8));
    } finally {
      streamAbort = null;
    }
  }
}

async function configTick() {
  if (stopping || !state.supabaseConfigured) return;
  try {
    const changed = await readConfig();
    const nextAllowedMs = Date.parse(state.ruleSyncNextAllowedAt || '');
    const canSyncRules = !Number.isFinite(nextAllowedMs) || Date.now() >= nextAllowedMs;
    const periodicDue = !state.lastRuleSyncAt || Date.now() - Date.parse(state.lastRuleSyncAt || 0) > 10 * 60_000;
    if (canSyncRules && (changed || periodicDue)) {
      try {
        await syncRules();
      } catch (error) {
        state.ruleSyncFailures++;
        const waitMs = Math.min(10 * 60_000, 30_000 * (2 ** Math.min(5, state.ruleSyncFailures - 1)));
        state.ruleSyncNextAllowedAt = new Date(Date.now() + waitMs).toISOString();
        throw error;
      }
    }
    if (!state.lastError?.startsWith('stream')) state.lastError = null;
  } catch (error) {
    state.lastError = `config:${String(error?.message || error)}`;
  } finally {
    if (!stopping) scheduleConfigTick();
  }
}

function scheduleConfigTick() {
  if (settingsTimer) clearTimeout(settingsTimer);
  settingsTimer = setTimeout(configTick, Math.max(MIN_CONFIG_REFRESH_MS, Math.min(MAX_CONFIG_REFRESH_MS, state.configRefreshMs)));
  settingsTimer.unref?.();
}

export function startSocialWatch() {
  if (state.started) return;
  state.started = true;
  stopping = false;
  if (!state.supabaseConfigured) {
    state.lastError = 'social_watch_supabase_not_configured';
    return;
  }
  configTick();
  streamLoopPromise = streamLoop().catch((error) => {
    state.lastError = `stream_loop:${String(error?.message || error)}`;
  });
}

export function stopSocialWatch() {
  stopping = true;
  if (settingsTimer) clearTimeout(settingsTimer);
  settingsTimer = null;
  if (streamAbort && !streamAbort.signal.aborted) streamAbort.abort('social_watch_shutdown');
  streamAbort = null;
  state.streamConnected = false;
}

export function getSocialWatchHealth() {
  return publicHealth();
}

async function readLatestEvents(limit) {
  const safeLimit = clampInt(limit, 1, 100, 30);
  const rows = await supabaseFetch(
    `${EVENTS_TABLE}?select=id,source_post_id,author_handle,author_name,content,post_url,published_at,language,notification_created&order=published_at.desc&limit=${safeLimit}`,
  );
  return Array.isArray(rows) ? rows : [];
}

export async function handleSocialWatch(req, res, url) {
  if (url.pathname === '/api/social-watch/health') {
    json(res, 200, { ok: true, social_watch: publicHealth(), time: nowIso() });
    return true;
  }
  if (url.pathname === '/api/social-watch/latest') {
    try {
      const rows = await readLatestEvents(url.searchParams.get('limit'));
      json(res, 200, {
        ok: true,
        schema: STEP_SCHEMA,
        rows,
        row_count: rows.length,
        user_read_x_requests: 0,
        x_requests_scale_with_users: false,
        time: nowIso(),
      });
    } catch (error) {
      json(res, 503, {
        ok: false,
        schema: STEP_SCHEMA,
        error: String(error?.message || error),
        user_read_x_requests: 0,
        time: nowIso(),
      });
    }
    return true;
  }
  return false;
}
