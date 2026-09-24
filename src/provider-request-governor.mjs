const GOVERNOR_VERSION = '652.1C.2';
const NATIVE_FETCH = globalThis.fetch.bind(globalThis);
const STEP1073_EGRESS_SCHEMA = 'step1073_r66_fetch_attribution_v1';
const STEP1073_EGRESS_LOG_MS = Math.max(60_000, Number(process.env.KAKA_STEP1073_EGRESS_LOG_MS || 300_000));
const STEP1073_EGRESS_FIRST_LOG_MS = Math.max(30_000, Number(process.env.KAKA_STEP1073_EGRESS_FIRST_LOG_MS || 60_000));
const step1073EgressHosts = new Map();
let step1073EgressRequests = 0;
let step1073EgressKnownBodyBytes = 0;
let step1073EgressEstimatedHeaderBytes = 0;
let step1073EgressTimer = null;
let step1073EgressFirstTimer = null;

function step1073BytesOf(value) {
  if (value == null) return 0;
  if (typeof value === 'string') return Buffer.byteLength(value);
  if (Buffer.isBuffer(value)) return value.length;
  if (value instanceof Uint8Array) return value.byteLength;
  if (value instanceof ArrayBuffer) return value.byteLength;
  if (value instanceof URLSearchParams) return Buffer.byteLength(value.toString());
  return 0;
}
function step1073IsLoopback(host) {
  const value = String(host || '').toLowerCase();
  return value === 'localhost' || value === '::1' || value === '[::1]' ||
    value === '127.0.0.1' || value.startsWith('127.');
}
function step1073HeaderBytes(headersLike) {
  let total = 0;
  try {
    const headers = new Headers(headersLike || undefined);
    headers.forEach((value, key) => {
      total += Buffer.byteLength(String(key)) + Buffer.byteLength(String(value)) + 4;
    });
  } catch (_) {}
  return total;
}
function recordStep1073TransportEgress(input, init = undefined) {
  let url;
  let method = 'GET';
  let headers = init?.headers || null;
  let bodyBytes = step1073BytesOf(init?.body);
  try {
    if (input instanceof Request) {
      url = new URL(input.url);
      method = String(init?.method || input.method || 'GET').toUpperCase();
      headers = new Headers(input.headers);
      if (init?.headers) {
        const extra = new Headers(init.headers);
        extra.forEach((value, key) => headers.set(key, value));
      }
      if (bodyBytes <= 0) {
        const declared = Number(headers.get('content-length') || 0);
        if (Number.isFinite(declared) && declared > 0) bodyBytes = declared;
      }
    } else {
      url = new URL(String(input || ''));
      method = String(init?.method || 'GET').toUpperCase();
    }
  } catch (_) { return; }
  const host = String(url.hostname || '').toLowerCase();
  if (!host || step1073IsLoopback(host)) return;
  const target = `${url.pathname || '/'}${url.search || ''}`;
  const headerBytes = step1073HeaderBytes(headers) + Buffer.byteLength(method) + Buffer.byteLength(target) + 12;
  step1073EgressRequests += 1;
  step1073EgressKnownBodyBytes += bodyBytes;
  step1073EgressEstimatedHeaderBytes += headerBytes;
  let row = step1073EgressHosts.get(host);
  if (!row) {
    row = { host, requests: 0, known_body_bytes: 0, estimated_header_bytes: 0, methods: {} };
    step1073EgressHosts.set(host, row);
  }
  row.requests += 1;
  row.known_body_bytes += bodyBytes;
  row.estimated_header_bytes += headerBytes;
  row.methods[method] = Number(row.methods[method] || 0) + 1;
  while (step1073EgressHosts.size > 48) {
    step1073EgressHosts.delete(step1073EgressHosts.keys().next().value);
  }
}
function step1073EgressSnapshot() {
  const rows = [...step1073EgressHosts.values()]
    .map((row) => ({ ...row, estimated_send_bytes: row.known_body_bytes + row.estimated_header_bytes }))
    .sort((a, b) => b.estimated_send_bytes - a.estimated_send_bytes)
    .slice(0, 16);
  return {
    schema: STEP1073_EGRESS_SCHEMA,
    role: processRole,
    loopback_excluded: true,
    content_values_logged: false,
    requests: step1073EgressRequests,
    known_body_bytes: step1073EgressKnownBodyBytes,
    estimated_header_bytes: step1073EgressEstimatedHeaderBytes,
    estimated_send_bytes: step1073EgressKnownBodyBytes + step1073EgressEstimatedHeaderBytes,
    top_hosts: rows,
  };
}
function emitStep1073EgressLog(reason) {
  try {
    console.log(`[Step1073 R66 fetch-egress] ${JSON.stringify({ reason, at: new Date().toISOString(), ...step1073EgressSnapshot() })}`);
  } catch (_) {}
}
function startStep1073EgressLogs() {
  if (step1073EgressTimer) return;
  step1073EgressFirstTimer = setTimeout(() => emitStep1073EgressLog('first_window'), STEP1073_EGRESS_FIRST_LOG_MS);
  step1073EgressFirstTimer.unref?.();
  step1073EgressTimer = setInterval(() => emitStep1073EgressLog('interval'), STEP1073_EGRESS_LOG_MS);
  step1073EgressTimer.unref?.();
}
function step1073NativeFetch(input, init = undefined) {
  recordStep1073TransportEgress(input, init);
  return NATIVE_FETCH(input, init);
}

const GLOBAL_MAX_ACTIVE = 6;
const UNSUPPORTED_TTL_MS = 15 * 60_000;
const MAX_UNSUPPORTED_ENTRIES = 500;
const MAX_CAPTURE_BYTES = 32 * 1024 * 1024;
const UPSTREAM_TIMEOUT_MS = 20_000;

const POLICY = Object.freeze({
  okx: Object.freeze({
    min_start_gap_ms: 220,
    max_concurrent: 2,
    max_queue: 96,
    rate_cooldown_ms: 60_000,
    forbidden_cooldown_ms: 15 * 60_000,
    official_reference: 'endpoint-specific; public unauthenticated REST limits are IP-based; code 50011/HTTP 429 means throttle',
  }),
  bybit: Object.freeze({
    min_start_gap_ms: 220,
    max_concurrent: 2,
    max_queue: 96,
    rate_cooldown_ms: 90_000,
    forbidden_cooldown_ms: 10 * 60_000,
    official_reference: '600 requests per 5 seconds per IP; HTTP 403 access-too-frequent requires at least 10 minutes stop',
  }),
  bitget: Object.freeze({
    min_start_gap_ms: 220,
    max_concurrent: 2,
    max_queue: 96,
    rate_cooldown_ms: 60_000,
    forbidden_cooldown_ms: 10 * 60_000,
    official_reference: 'public market interfaces maximum 20 requests per second per IP; HTTP 429 on excess',
  }),
  gate: Object.freeze({
    min_start_gap_ms: 220,
    max_concurrent: 2,
    max_queue: 96,
    rate_cooldown_ms: 60_000,
    forbidden_cooldown_ms: 10 * 60_000,
    official_reference: 'public REST is IP limited; rate-limit response headers and reset timestamp are honored',
  }),
  coinbase: Object.freeze({
    min_start_gap_ms: 220,
    max_concurrent: 2,
    max_queue: 96,
    rate_cooldown_ms: 60_000,
    forbidden_cooldown_ms: 10 * 60_000,
    official_reference: 'public Exchange REST 10 requests per second per IP, burst up to 15; HTTP 429 on excess',
  }),
});

const states = new Map(Object.entries(POLICY).map(([provider, policy]) => [provider, {
  provider,
  policy,
  queue: [],
  active: 0,
  next_start_at: 0,
  cooldown_until: 0,
  cooldown_reason: '',
  timer: null,
  counters: {
    seen: 0,
    queued: 0,
    started: 0,
    completed: 0,
    succeeded: 0,
    failed_http: 0,
    failed_network: 0,
    merged: 0,
    queue_rejected: 0,
    queue_cancelled: 0,
    caller_aborts: 0,
    cooldown_rejected: 0,
    cooldowns_opened: 0,
    retry_after_honored: 0,
    unsupported_cached: 0,
    unsupported_cache_hits: 0,
    max_queue_seen: 0,
  },
  last_status: 0,
  last_error: '',
  last_path: '',
  last_started_at: 0,
  last_completed_at: 0,
}]));

const inflight = new Map();
const unsupportedCache = new Map();
let installed = false;
let processRole = 'uninstalled';
let globalActive = 0;
const providerOrder = [...states.keys()];
let pumpCursor = 0;

function hostProvider(url) {
  let host = '';
  try { host = new URL(String(url)).hostname.toLowerCase(); }
  catch (_) { return null; }
  if (host === 'www.okx.com' || host === 'aws.okx.com' || host === 'okx.com' || host.endsWith('.okx.com')) return 'okx';
  if (host === 'api.bybit.com' || host === 'api.bytick.com' || host === 'api.bybick.com' || host.includes('bybit.') || host.includes('bytick.')) return 'bybit';
  if (host === 'api.bitget.com' || host.endsWith('.bitget.com')) return 'bitget';
  if (host === 'api.gateio.ws' || host === 'fx-api.gateio.ws' || host.endsWith('.gateio.ws')) return 'gate';
  if (host === 'api.exchange.coinbase.com' || host === 'api.coinbase.com' || host.endsWith('.coinbase.com')) return 'coinbase';
  return null;
}

function safePath(url) {
  try { return new URL(String(url)).pathname.slice(0, 240); }
  catch (_) { return ''; }
}

function headersObject(headers) {
  const result = {};
  try {
    for (const [name, value] of headers.entries()) {
      const lower = String(name).toLowerCase();
      if (['content-length','content-encoding','transfer-encoding','connection','set-cookie'].includes(lower)) continue;
      result[lower] = String(value);
    }
  } catch (_) {}
  return result;
}

function snapshotResponse(snapshot) {
  return new Response(snapshot.body.slice(0), {
    status: snapshot.status,
    statusText: snapshot.statusText || '',
    headers: snapshot.headers,
  });
}

function jsonSnapshot(status, payload, extraHeaders = {}) {
  const body = new TextEncoder().encode(JSON.stringify(payload));
  return {
    status,
    statusText: status === 429 ? 'Too Many Requests' : status === 503 ? 'Service Unavailable' : 'Error',
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
      ...extraHeaders,
    },
    body,
  };
}

function textOf(snapshot, maxBytes = 16_384) {
  try { return new TextDecoder().decode(snapshot.body.slice(0, maxBytes)); }
  catch (_) { return ''; }
}

function finiteMs(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : 0;
}

function retryAfterMs(headers, now = Date.now()) {
  const raw = String(headers?.['retry-after'] || '').trim();
  if (raw) {
    const seconds = Number(raw);
    if (Number.isFinite(seconds) && seconds >= 0) return Math.ceil(seconds * 1000);
    const date = Date.parse(raw);
    if (Number.isFinite(date) && date > now) return date - now;
  }
  for (const name of ['x-gate-ratelimit-reset-timestamp','x-ratelimit-reset','ratelimit-reset']) {
    const value = Number(headers?.[name]);
    if (!Number.isFinite(value) || value <= 0) continue;
    const resetMs = value > 10_000_000_000 ? value : value * 1000;
    if (resetMs > now) return resetMs - now;
  }
  return 0;
}

function restrictionInfo(provider, snapshot) {
  const lower = textOf(snapshot).toLowerCase();
  const status = Number(snapshot.status || 0);
  const okxCode = /[\"']?code[\"']?\s*:\s*[\"']?(50011|50013|58102)[\"']?/i.test(lower);
  const bybitCode = /[\"']?retcode[\"']?\s*:\s*[\"']?10006[\"']?/i.test(lower);
  const generic = lower.includes('too many requests') || lower.includes('too many visits') || lower.includes('rate limit') || lower.includes('access too frequent');
  if (!(status === 403 || status === 418 || status === 429 || status === 451 || okxCode || bybitCode || generic)) return null;

  const policy = POLICY[provider];
  let duration = policy.rate_cooldown_ms;
  let reason = 'rate_limit';
  if (status === 403 || status === 418 || status === 451 || lower.includes('access too frequent')) {
    duration = policy.forbidden_cooldown_ms;
    reason = provider === 'bybit' ? 'bybit_access_too_frequent_10m' : 'forbidden_or_region_block';
  }
  const headerMs = retryAfterMs(snapshot.headers);
  if (headerMs > duration) duration = headerMs + 2_000;
  return { duration_ms: duration, reason, retry_after_honored: headerMs > 0 };
}

function unsupportedInfo(snapshot) {
  const status = Number(snapshot.status || 0);
  const lower = textOf(snapshot).toLowerCase();
  if (status === 404) return { cache: true, reason: 'http_404' };
  if (status !== 400) return { cache: false, reason: '' };
  const patterns = [
    'instrument id does not exist', 'instrument does not exist', 'symbol not found',
    'product not found', 'currency_pair not found', 'currency pair not found',
    'invalid symbol', 'symbol does not exist', 'contract not found', 'market not found',
  ];
  const matched = patterns.find((item) => lower.includes(item));
  return { cache: Boolean(matched), reason: matched || '' };
}

function pruneUnsupported(now = Date.now()) {
  for (const [key, value] of unsupportedCache) {
    if (value.until <= now) unsupportedCache.delete(key);
  }
  while (unsupportedCache.size > MAX_UNSUPPORTED_ENTRIES) {
    const first = unsupportedCache.keys().next().value;
    if (first == null) break;
    unsupportedCache.delete(first);
  }
}

function cooldownSnapshot(state) {
  const seconds = Math.max(1, Math.ceil((state.cooldown_until - Date.now()) / 1000));
  return jsonSnapshot(429, {
    ok: false,
    error: 'provider_governor_cooldown',
    provider: state.provider,
    reason: state.cooldown_reason || 'rate_limit',
    retry_after_seconds: seconds,
  }, { 'retry-after': String(seconds), 'x-kaka-provider-governor': 'cooldown' });
}

function flushQueuedForCooldown(state) {
  if (!state.queue.length) return;
  const snapshot = cooldownSnapshot(state);
  const queued = state.queue.splice(0, state.queue.length);
  for (const task of queued) {
    state.counters.cooldown_rejected += 1;
    task.resolve(snapshot);
  }
}

function openCooldown(state, info) {
  const until = Date.now() + Math.max(1_000, finiteMs(info?.duration_ms));
  if (until > state.cooldown_until) {
    state.cooldown_until = until;
    state.cooldown_reason = String(info?.reason || 'rate_limit');
    state.counters.cooldowns_opened += 1;
  }
  if (info?.retry_after_honored) state.counters.retry_after_honored += 1;
  flushQueuedForCooldown(state);
}

function mergeAllowed(request) {
  if (!['GET','HEAD'].includes(request.method)) return false;
  return !request.headers.has('authorization') && !request.headers.has('cookie') && !request.headers.has('x-api-key');
}

function requestKey(provider, request) {
  return `${provider}|${request.method}|${request.url}`;
}

function removeQueuedTask(task) {
  const state = task?.state;
  if (!state || task.phase !== 'queued') return false;
  const index = state.queue.indexOf(task);
  if (index < 0) return false;
  state.queue.splice(index, 1);
  task.phase = 'cancelled';
  state.counters.queue_cancelled += 1;
  task.shared.settled = true;
  task.reject(new DOMException('The operation was aborted while queued.', 'AbortError'));
  return true;
}

function attachConsumer(shared, signal) {
  shared.consumers += 1;
  if (signal?.aborted) {
    shared.consumers = Math.max(0, shared.consumers - 1);
    shared.task.state.counters.caller_aborts += 1;
    if (shared.consumers === 0) removeQueuedTask(shared.task);
    return Promise.reject(new DOMException('The operation was aborted.', 'AbortError'));
  }
  return new Promise((resolve, reject) => {
    let finished = false;
    const complete = (callback, value) => {
      if (finished) return;
      finished = true;
      signal?.removeEventListener?.('abort', onAbort);
      shared.consumers = Math.max(0, shared.consumers - 1);
      callback(value);
    };
    const onAbort = () => {
      shared.task.state.counters.caller_aborts += 1;
      complete(reject, new DOMException('The operation was aborted.', 'AbortError'));
      if (shared.consumers === 0) removeQueuedTask(shared.task);
    };
    signal?.addEventListener?.('abort', onAbort, { once: true });
    shared.promise.then(
      (value) => complete(resolve, value),
      (error) => complete(reject, error),
    );
  });
}

function schedulePump(state, delayMs) {
  if (state.timer) return;
  state.timer = setTimeout(() => {
    state.timer = null;
    pumpAll();
  }, Math.max(1, delayMs));
  state.timer.unref?.();
}

function pumpState(state) {
  const now = Date.now();
  if (state.cooldown_until > now) {
    flushQueuedForCooldown(state);
    return;
  }
  if (state.cooldown_until) {
    state.cooldown_until = 0;
    state.cooldown_reason = '';
  }
  if (!state.queue.length || state.active >= state.policy.max_concurrent || globalActive >= GLOBAL_MAX_ACTIVE) return;
  if (now < state.next_start_at) {
    schedulePump(state, state.next_start_at - now);
    return;
  }

  const task = state.queue.shift();
  task.phase = 'active';
  state.active += 1;
  globalActive += 1;
  state.next_start_at = now + state.policy.min_start_gap_ms;
  state.counters.started += 1;
  state.last_started_at = now;
  state.last_path = safePath(task.url);

  executeTask(state, task).finally(() => {
    state.active = Math.max(0, state.active - 1);
    globalActive = Math.max(0, globalActive - 1);
    pumpAll();
  });
  if (state.queue.length) schedulePump(state, state.policy.min_start_gap_ms);
}

function pumpAll() {
  if (!providerOrder.length) return;
  for (let index = 0; index < providerOrder.length; index += 1) {
    const provider = providerOrder[(pumpCursor + index) % providerOrder.length];
    pumpState(states.get(provider));
  }
  pumpCursor = (pumpCursor + 1) % providerOrder.length;
}

async function executeTask(state, task) {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS);
    timeout.unref?.();
    let response;
    try {
      response = await step1073NativeFetch(task.url, {
        method: task.method,
        headers: task.headers,
        redirect: task.redirect,
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timeout);
    }
    const buffer = new Uint8Array(await response.arrayBuffer());
    if (buffer.byteLength > MAX_CAPTURE_BYTES) throw new Error('provider_governor_response_too_large');
    const snapshot = {
      status: response.status,
      statusText: response.statusText,
      headers: headersObject(response.headers),
      body: buffer,
    };
    state.last_status = snapshot.status;
    state.last_completed_at = Date.now();
    state.counters.completed += 1;
    if (response.ok) state.counters.succeeded += 1;
    else state.counters.failed_http += 1;

    const restricted = restrictionInfo(state.provider, snapshot);
    let deliveredSnapshot = snapshot;
    if (restricted) {
      openCooldown(state, restricted);
      if (snapshot.status < 400) deliveredSnapshot = cooldownSnapshot(state);
    }

    const unsupported = unsupportedInfo(snapshot);
    if (!restricted && unsupported.cache) {
      unsupportedCache.set(task.key, {
        until: Date.now() + UNSUPPORTED_TTL_MS,
        snapshot,
        reason: unsupported.reason,
      });
      state.counters.unsupported_cached += 1;
      pruneUnsupported();
    }
    task.phase = 'settled';
    task.shared.settled = true;
    task.resolve(deliveredSnapshot);
  } catch (error) {
    state.last_error = String(error?.message || error).slice(0, 240);
    state.last_completed_at = Date.now();
    state.counters.completed += 1;
    state.counters.failed_network += 1;
    task.phase = 'settled';
    task.shared.settled = true;
    task.reject(error);
  }
}

async function governedFetch(input, init = undefined) {
  let request;
  try { request = new Request(input, init); }
  catch (_) { return step1073NativeFetch(input, init); }
  const provider = hostProvider(request.url);
  if (!provider || !['GET','HEAD'].includes(request.method)) return step1073NativeFetch(input, init);

  const state = states.get(provider);
  state.counters.seen += 1;
  const key = requestKey(provider, request);
  pruneUnsupported();
  const negative = unsupportedCache.get(key);
  if (negative && negative.until > Date.now()) {
    state.counters.unsupported_cache_hits += 1;
    return snapshotResponse(negative.snapshot);
  }

  if (state.cooldown_until > Date.now()) {
    state.counters.cooldown_rejected += 1;
    return snapshotResponse(cooldownSnapshot(state));
  }

  const canMerge = mergeAllowed(request);
  let shared = canMerge ? inflight.get(key) : null;
  if (shared) {
    state.counters.merged += 1;
    return snapshotResponse(await attachConsumer(shared, init?.signal || request.signal));
  }

  if (state.queue.length >= state.policy.max_queue) {
    state.counters.queue_rejected += 1;
    return snapshotResponse(jsonSnapshot(503, {
      ok: false,
      error: 'provider_governor_queue_full',
      provider,
      retry_after_seconds: 2,
    }, { 'retry-after': '2', 'x-kaka-provider-governor': 'queue-full' }));
  }

  let resolveBase;
  let rejectBase;
  const promise = new Promise((resolve, reject) => {
    resolveBase = resolve;
    rejectBase = reject;
  });
  shared = {
    key,
    promise,
    task: null,
    consumers: 0,
    settled: false,
  };
  const task = {
    provider,
    state,
    shared,
    url: request.url,
    method: request.method,
    headers: Object.fromEntries(request.headers.entries()),
    redirect: request.redirect,
    key,
    phase: 'queued',
    resolve: resolveBase,
    reject: rejectBase,
    queued_at: Date.now(),
  };
  shared.task = task;
  state.queue.push(task);
  state.counters.queued += 1;
  state.counters.max_queue_seen = Math.max(state.counters.max_queue_seen, state.queue.length);
  if (canMerge) {
    inflight.set(key, shared);
    promise.then(() => inflight.delete(key), () => inflight.delete(key));
  }
  pumpAll();
  return snapshotResponse(await attachConsumer(shared, init?.signal || request.signal));
}

export function installProviderGovernorFetch({ role = 'worker' } = {}) {
  if (!installed) {
    globalThis.fetch = governedFetch;
    installed = true;
  }
  processRole = String(role || processRole || 'worker');
  startStep1073EgressLogs();
  return getProviderGovernorHealth();
}

function stateHealth(state) {
  const now = Date.now();
  return {
    provider: state.provider,
    policy: { ...state.policy },
    active: state.active,
    queue_length: state.queue.length,
    next_start_in_ms: Math.max(0, state.next_start_at - now),
    cooldown_active: state.cooldown_until > now,
    cooldown_until: state.cooldown_until > now ? new Date(state.cooldown_until).toISOString() : null,
    cooldown_remaining_ms: Math.max(0, state.cooldown_until - now),
    cooldown_reason: state.cooldown_until > now ? state.cooldown_reason : '',
    inflight_keys: [...inflight.keys()].filter((key) => key.startsWith(`${state.provider}|`)).length,
    last_status: state.last_status,
    last_error: state.last_error,
    last_path: state.last_path,
    last_started_at: state.last_started_at ? new Date(state.last_started_at).toISOString() : null,
    last_completed_at: state.last_completed_at ? new Date(state.last_completed_at).toISOString() : null,
    counters: { ...state.counters },
  };
}

export function getProviderGovernorHealth() {
  pruneUnsupported();
  return {
    enabled: installed,
    governor_version: GOVERNOR_VERSION,
    process_role: processRole,
    mode: 'shared_provider_queue_exact_get_inflight_merge_retry_after_hard_cooldown_negative_cache',
    binance_ownership: 'unchanged_separate_persistent_guard_and_authenticated_edge_relay',
    governed_providers: [...states.keys()],
    global_max_active: GLOBAL_MAX_ACTIVE,
    global_active: globalActive,
    upstream_timeout_ms: UPSTREAM_TIMEOUT_MS,
    provider_round_robin_fairness: true,
    queued_work_cancelled_when_all_callers_abort: true,
    exact_get_inflight_entries: inflight.size,
    unsupported_cache_entries: unsupportedCache.size,
    unsupported_cache_ttl_seconds: Math.round(UNSUPPORTED_TTL_MS / 1000),
    step1073_egress_attribution: step1073EgressSnapshot(),
    providers: Object.fromEntries([...states.entries()].map(([key, state]) => [key, stateHealth(state)])),
    time: new Date().toISOString(),
  };
}

function test(condition, name, details = '') {
  return { name, ok: Boolean(condition), details: String(details || '') };
}

export function runProviderGovernorSelfTest() {
  const retry = retryAfterMs({ 'retry-after': '7' }, 1_000);
  const gateRetry = retryAfterMs({ 'x-gate-ratelimit-reset-timestamp': '20' }, 10_000);
  const bybit403 = restrictionInfo('bybit', jsonSnapshot(403, { retMsg: 'access too frequent' }));
  const okx50011 = restrictionInfo('okx', jsonSnapshot(200, { code: '50011', msg: 'Rate limit reached' }));
  const bitget429 = restrictionInfo('bitget', jsonSnapshot(429, { msg: 'Too Many Requests' }));
  const bybit10006 = restrictionInfo('bybit', jsonSnapshot(200, { retCode: 10006, retMsg: 'Too many visits' }));
  const unsupported404 = unsupportedInfo(jsonSnapshot(404, { message: 'Not Found' }));
  const unsupported400 = unsupportedInfo(jsonSnapshot(400, { message: 'symbol not found' }));
  const tests = [
    test(hostProvider('https://www.okx.com/api/v5/market/ticker') === 'okx', 'host_okx'),
    test(hostProvider('https://api.bybit.com/v5/market/tickers') === 'bybit', 'host_bybit'),
    test(hostProvider('https://api.bitget.com/api/v2/spot/market/tickers') === 'bitget', 'host_bitget'),
    test(hostProvider('https://api.gateio.ws/api/v4/spot/tickers') === 'gate', 'host_gate'),
    test(hostProvider('https://api.exchange.coinbase.com/products') === 'coinbase', 'host_coinbase'),
    test(hostProvider('https://fapi.binance.com/fapi/v1/openInterest') == null, 'binance_not_owned'),
    test(retry === 7_000, 'retry_after_seconds', retry),
    test(gateRetry === 10_000, 'gate_reset_timestamp', gateRetry),
    test(bybit403?.duration_ms >= 10 * 60_000, 'bybit_403_minimum_10_minutes', bybit403?.duration_ms),
    test(okx50011?.duration_ms >= 60_000, 'okx_50011_detected', okx50011?.duration_ms),
    test(bitget429?.duration_ms >= 60_000, 'bitget_429_detected', bitget429?.duration_ms),
    test(bybit10006?.duration_ms >= 90_000, 'bybit_10006_detected', bybit10006?.duration_ms),
    test(unsupported404.cache === true, 'unsupported_http_404_cached'),
    test(unsupported400.cache === true, 'unsupported_symbol_400_cached'),
    test(POLICY.coinbase.min_start_gap_ms >= 200, 'coinbase_below_official_10rps'),
    test(POLICY.bitget.min_start_gap_ms >= 100, 'bitget_below_official_20rps'),
    test(POLICY.bybit.forbidden_cooldown_ms >= 10 * 60_000, 'bybit_official_ban_wait_preserved'),
    test(Object.values(POLICY).every((item) => item.max_concurrent <= 2), 'per_provider_concurrency_bounded'),
    test(Object.values(POLICY).every((item) => item.max_queue <= 96), 'per_provider_queue_bounded'),
    test(GLOBAL_MAX_ACTIVE <= 6, 'global_concurrency_bounded'),
    test(UPSTREAM_TIMEOUT_MS <= 20_000, 'upstream_timeout_bounded'),
  ];
  return {
    ok: tests.every((item) => item.ok),
    governor_version: GOVERNOR_VERSION,
    tests,
  };
}
