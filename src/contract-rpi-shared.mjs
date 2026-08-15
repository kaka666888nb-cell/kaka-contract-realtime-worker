import { getContractFocusPoolInternalSnapshot } from './contract-focus-pool.mjs';

const VERSION = '650.8.15.1';
const SNAPSHOT_ROUTE = '/api/contract-rpi-shared/current-snapshot';
const HEALTH_ROUTE = '/api/contract-rpi-shared/health';
const PROVIDERS = Object.freeze(['okx', 'bybit', 'bitget']);
const TARGET_PER_PROVIDER = 15;
const TARGET_ROWS = PROVIDERS.length * TARGET_PER_PROVIDER;
const LEVELS = 20;
const SAMPLE_PER_PROVIDER_PER_CYCLE = 4;
const SCAN_INTERVAL_MS = Math.max(12_000, Number(process.env.KAKA_RPI_SCAN_INTERVAL_MS || 15_000));
const STARTUP_DELAY_MS = Math.max(8_000, Number(process.env.KAKA_RPI_STARTUP_DELAY_MS || 18_000));
const STALE_MS = Math.max(90_000, Number(process.env.KAKA_RPI_STALE_MS || 150_000));
const REQUEST_TIMEOUT_MS = Math.max(2_500, Number(process.env.KAKA_RPI_REQUEST_TIMEOUT_MS || 8_000));
const RETRY_COOLDOWN_MS = Math.max(15_000, Number(process.env.KAKA_RPI_RETRY_COOLDOWN_MS || 45_000));
const RESPONSE_CACHE_TTL_MS = 5_000;

const state = new Map();
const cursors = Object.fromEntries(PROVIDERS.map((provider) => [provider, 0]));

let started = false;
let running = null;
let startupTimer = null;
let interval = null;
let round = 0;
let totalScans = 0;
let totalScanFailures = 0;
let totalReads = 0;
let responseCacheHits = 0;
let responseCacheMisses = 0;
let requestAttempts = 0;
let requestSuccesses = 0;
let requestFailures = 0;
let cooldownSkips = 0;
let lastStartedAt = null;
let lastCompletedAt = null;
let lastError = '';
let responseCache = null;

function finite(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function nonNegative(value) {
  const number = finite(value);
  return number != null && number >= 0 ? number : null;
}

function normalizeProvider(raw) {
  const provider = String(raw || '').trim().toLowerCase().replace('okex', 'okx');
  return PROVIDERS.includes(provider) ? provider : '';
}

function normalizeSymbol(raw) {
  return String(raw || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function clone(value) {
  return value == null ? value : JSON.parse(JSON.stringify(value));
}

function focusRows() {
  const snapshot = getContractFocusPoolInternalSnapshot();
  const byProvider = Object.fromEntries(PROVIDERS.map((provider) => [provider, []]));
  if (snapshot?.ready !== true) {
    return { ready: false, round: Number(snapshot?.round || 0), byProvider, rows: [] };
  }
  for (const provider of PROVIDERS) {
    const sourceRows = Array.isArray(snapshot?.providers?.[provider]?.rows)
      ? snapshot.providers[provider].rows
      : [];
    byProvider[provider] = sourceRows
      .map((row) => ({
        provider,
        symbol: normalizeSymbol(row?.symbol),
        base_asset: String(row?.base_asset || '').trim().toUpperCase(),
        role: String(row?.role || ''),
        slot: Number(row?.slot || 0),
      }))
      .filter((row) => row.symbol && row.symbol.endsWith('USDT'))
      .slice(0, TARGET_PER_PROVIDER);
  }
  const rows = PROVIDERS.flatMap((provider) => byProvider[provider]);
  return {
    ready: rows.length === TARGET_ROWS && PROVIDERS.every((provider) => byProvider[provider].length === TARGET_PER_PROVIDER),
    round: Number(snapshot?.round || 0),
    byProvider,
    rows,
  };
}

function focusSignature(focus) {
  return PROVIDERS.map((provider) =>
    `${provider}:${(focus.byProvider[provider] || []).map((row) => row.symbol).join(',')}`
  ).join('|');
}

function providerSymbol(provider, symbol) {
  const normalized = normalizeSymbol(symbol);
  const base = normalized.endsWith('USDT') ? normalized.slice(0, -4) : '';
  if (!base) return '';
  if (provider === 'okx') return `${base}-USDT-SWAP`;
  return `${base}USDT`;
}

function parseOfficialLevel(provider, raw, side) {
  if (!Array.isArray(raw) || raw.length < 3) return null;
  const price = finite(raw[0]);
  if (price == null || price <= 0) return null;

  let totalQuantity = null;
  let nonRpiQuantity = null;
  let rpiQuantity = null;
  let orderCount = null;

  if (provider === 'okx') {
    totalQuantity = nonNegative(raw[1]);
    nonRpiQuantity = nonNegative(raw[2]);
    orderCount = nonNegative(raw[3]);
    if (totalQuantity == null || nonRpiQuantity == null) return null;
    // OKX officially defines RPI at a price level as totalQty - nonRpiQty.
    // Clamp only tiny floating-point underflow; materially inconsistent
    // upstream values remain unavailable instead of being guessed.
    const difference = totalQuantity - nonRpiQuantity;
    if (difference < -Math.max(1e-12, totalQuantity * 1e-9)) return null;
    rpiQuantity = Math.max(0, difference);
  } else {
    nonRpiQuantity = nonNegative(raw[1]);
    rpiQuantity = nonNegative(raw[2]);
    if (nonRpiQuantity == null || rpiQuantity == null) return null;
    totalQuantity = nonRpiQuantity + rpiQuantity;
  }

  return {
    side,
    price,
    total_quantity: totalQuantity,
    non_rpi_quantity: nonRpiQuantity,
    rpi_quantity: rpiQuantity,
    total_quote_amount: price * totalQuantity,
    non_rpi_quote_amount: price * nonRpiQuantity,
    rpi_quote_amount: price * rpiQuantity,
    order_count: orderCount,
  };
}

function parseLevels(provider, rows, side) {
  return (Array.isArray(rows) ? rows : [])
    .map((row) => parseOfficialLevel(provider, row, side))
    .filter(Boolean)
    .sort((a, b) => side === 'bid' ? b.price - a.price : a.price - b.price)
    .slice(0, LEVELS);
}

function sum(levels, field, count = LEVELS) {
  const values = (Array.isArray(levels) ? levels : []).slice(0, count);
  if (!values.length) return null;
  return values.reduce((total, row) => total + Number(row?.[field] || 0), 0);
}

function buildRow(focusRow, provider, nativeSymbol, bids, asks, timestampMs, source) {
  const rpiBidLevels = bids.filter((row) => Number(row.rpi_quantity || 0) > 0).length;
  const rpiAskLevels = asks.filter((row) => Number(row.rpi_quantity || 0) > 0).length;
  const now = Date.now();
  return {
    provider,
    market_type: 'contract',
    symbol: focusRow.symbol,
    native_symbol: nativeSymbol,
    base_asset: focusRow.base_asset,
    quote_asset: 'USDT',
    role: focusRow.role,
    slot: focusRow.slot,
    source,
    official_rpi: true,
    official_rpi_semantics:
      provider === 'okx'
        ? 'official_total_quantity_minus_official_non_rpi_quantity_same_price_level'
        : 'official_non_rpi_and_rpi_quantities_separate_same_price_level',
    standard_orderbook_kept_separate: true,
    local_rpi_guessing: false,
    best_bid: bids[0]?.price ?? null,
    best_ask: asks[0]?.price ?? null,
    bid_levels: bids.length,
    ask_levels: asks.length,
    rpi_bid_levels: rpiBidLevels,
    rpi_ask_levels: rpiAskLevels,
    rpi_present: rpiBidLevels > 0 || rpiAskLevels > 0,
    bid_total_quote_20: sum(bids, 'total_quote_amount'),
    ask_total_quote_20: sum(asks, 'total_quote_amount'),
    bid_non_rpi_quote_20: sum(bids, 'non_rpi_quote_amount'),
    ask_non_rpi_quote_20: sum(asks, 'non_rpi_quote_amount'),
    bid_rpi_quote_20: sum(bids, 'rpi_quote_amount'),
    ask_rpi_quote_20: sum(asks, 'rpi_quote_amount'),
    bids,
    asks,
    timestamp_ms: Number(timestampMs || 0) || now,
    sampled_at_ms: now,
    sampled_at: new Date(now).toISOString(),
    refresh_error: '',
    retry_not_before_ms: 0,
  };
}

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: {
      accept: 'application/json',
      'user-agent': 'KakaWeb3-contract-rpi-shared/650.8.15.1',
    },
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`rpi_http_${response.status}:${text.slice(0, 160)}`);
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    throw new Error(`rpi_non_json_response:${text.slice(0, 100)}`);
  }
  return data;
}

async function loadOkx(focusRow) {
  const nativeSymbol = providerSymbol('okx', focusRow.symbol);
  const data = await fetchJson(
    `https://www.okx.com/api/v5/market/books-rpi?instId=${encodeURIComponent(nativeSymbol)}&sz=${LEVELS}`,
  );
  if (String(data?.code ?? '') !== '0' || !Array.isArray(data?.data) || !data.data[0]) {
    throw new Error(`okx_rpi_${data?.code ?? 'invalid'}`);
  }
  const raw = data.data[0];
  const bids = parseLevels('okx', raw?.bids, 'bid');
  const asks = parseLevels('okx', raw?.asks, 'ask');
  if (!bids.length || !asks.length) throw new Error('okx_rpi_empty');
  return buildRow(
    focusRow,
    'okx',
    nativeSymbol,
    bids,
    asks,
    finite(raw?.ts) || Date.now(),
    'okx_official_books_rpi_rest',
  );
}

async function loadBybit(focusRow) {
  const nativeSymbol = providerSymbol('bybit', focusRow.symbol);
  const data = await fetchJson(
    `https://api.bybit.com/v5/market/rpi_orderbook?category=linear&symbol=${encodeURIComponent(nativeSymbol)}&limit=${LEVELS}`,
  );
  if (Number(data?.retCode) !== 0 || !data?.result) {
    throw new Error(`bybit_rpi_${data?.retCode ?? 'invalid'}`);
  }
  const raw = data.result;
  const bids = parseLevels('bybit', raw?.b, 'bid');
  const asks = parseLevels('bybit', raw?.a, 'ask');
  if (!bids.length || !asks.length) throw new Error('bybit_rpi_empty');
  return buildRow(
    focusRow,
    'bybit',
    nativeSymbol,
    bids,
    asks,
    finite(raw?.cts) || finite(raw?.ts) || finite(data?.time) || Date.now(),
    'bybit_official_rpi_orderbook_rest',
  );
}

async function loadBitget(focusRow) {
  const nativeSymbol = providerSymbol('bitget', focusRow.symbol);
  const data = await fetchJson(
    `https://api.bitget.com/api/v3/market/rpi-orderbook?category=USDT-FUTURES&symbol=${encodeURIComponent(nativeSymbol)}&limit=${LEVELS}`,
  );
  if (String(data?.code || '') !== '00000' || !data?.data) {
    throw new Error(`bitget_rpi_${data?.code ?? 'invalid'}`);
  }
  const raw = data.data;
  const bids = parseLevels('bitget', raw?.b, 'bid');
  const asks = parseLevels('bitget', raw?.a, 'ask');
  if (!bids.length || !asks.length) throw new Error('bitget_rpi_empty');
  return buildRow(
    focusRow,
    'bitget',
    nativeSymbol,
    bids,
    asks,
    finite(raw?.ts) || finite(data?.requestTime) || Date.now(),
    'bitget_official_uta_rpi_orderbook_rest',
  );
}

async function loadRow(row) {
  if (row.provider === 'okx') return loadOkx(row);
  if (row.provider === 'bybit') return loadBybit(row);
  if (row.provider === 'bitget') return loadBitget(row);
  throw new Error(`unsupported_rpi_provider:${row.provider}`);
}

function nextTargets(focus) {
  const now = Date.now();
  const targets = [];
  for (const provider of PROVIDERS) {
    const rows = focus.byProvider[provider] || [];
    if (!rows.length) continue;
    const start = Number(cursors[provider] || 0) % rows.length;
    let inspected = 0;
    let selected = 0;
    while (inspected < rows.length && selected < SAMPLE_PER_PROVIDER_PER_CYCLE) {
      const row = rows[(start + inspected) % rows.length];
      const current = state.get(`${provider}:${row.symbol}`);
      inspected += 1;
      if (Number(current?.retry_not_before_ms || 0) > now) {
        cooldownSkips += 1;
        continue;
      }
      targets.push(row);
      selected += 1;
    }
    cursors[provider] = (start + Math.max(1, inspected)) % rows.length;
  }
  return targets;
}

async function refreshRow(row) {
  const key = `${row.provider}:${row.symbol}`;
  const previous = state.get(key) || null;
  requestAttempts += 1;
  try {
    const next = await loadRow(row);
    state.set(key, next);
    requestSuccesses += 1;
  } catch (error) {
    requestFailures += 1;
    const message = String(error?.message || error).slice(0, 320);
    const now = Date.now();
    state.set(key, {
      ...(previous || {
        provider: row.provider,
        market_type: 'contract',
        symbol: row.symbol,
        native_symbol: providerSymbol(row.provider, row.symbol),
        base_asset: row.base_asset,
        quote_asset: 'USDT',
        role: row.role,
        slot: row.slot,
        official_rpi: true,
        bids: [],
        asks: [],
        sampled_at_ms: 0,
      }),
      refresh_error: message,
      last_refresh_failed_at_ms: now,
      retry_not_before_ms: now + RETRY_COOLDOWN_MS,
    });
  }
}

async function scan() {
  if (running) return running;
  running = (async () => {
    const focus = focusRows();
    if (!focus.ready) {
      lastError = 'rpi_focus_pool_not_ready';
      return;
    }
    round += 1;
    totalScans += 1;
    lastStartedAt = new Date().toISOString();
    const targets = nextTargets(focus);
    try {
      // Requests stay sequential inside each provider, while the three
      // independent providers run in parallel. A blocked OKX/Bybit route must
      // not starve Bitget, and no provider exceeds four requests per cycle.
      await Promise.all(PROVIDERS.map(async (provider) => {
        for (const target of targets.filter((row) => row.provider === provider)) {
          await refreshRow(target);
        }
      }));
      lastCompletedAt = new Date().toISOString();
      lastError = '';
      responseCache = null;
    } catch (error) {
      totalScanFailures += 1;
      lastError = String(error?.message || error).slice(0, 320);
    }
  })().finally(() => { running = null; });
  return running;
}

export function startContractRpiSharedScanner() {
  if (started || process.env.KAKA_DISABLE_CONTRACT_RPI_SHARED === '1') return;
  started = true;
  startupTimer = setTimeout(() => {
    scan().catch(() => {});
    interval = setInterval(() => scan().catch(() => {}), SCAN_INTERVAL_MS);
    interval.unref?.();
  }, STARTUP_DELAY_MS);
  startupTimer.unref?.();
}

function currentPayload() {
  totalReads += 1;
  const now = Date.now();
  if (responseCache && now - responseCache.at <= RESPONSE_CACHE_TTL_MS) {
    responseCacheHits += 1;
    return { ...clone(responseCache.payload), cache_hit: true, cache_age_ms: now - responseCache.at };
  }
  responseCacheMisses += 1;
  const focus = focusRows();
  const signature = focusSignature(focus);
  const rows = focus.rows.map((focusRow) => {
    const current = state.get(`${focusRow.provider}:${focusRow.symbol}`);
    const ageMs = current?.sampled_at_ms ? Math.max(0, now - Number(current.sampled_at_ms)) : null;
    return {
      ...(current || {
        provider: focusRow.provider,
        market_type: 'contract',
        symbol: focusRow.symbol,
        native_symbol: providerSymbol(focusRow.provider, focusRow.symbol),
        base_asset: focusRow.base_asset,
        quote_asset: 'USDT',
        role: focusRow.role,
        slot: focusRow.slot,
        official_rpi: true,
        bids: [],
        asks: [],
        sampled_at_ms: 0,
        refresh_error: 'rpi_snapshot_not_sampled_yet',
      }),
      role: focusRow.role,
      slot: focusRow.slot,
      age_ms: ageMs,
      fresh: ageMs != null && ageMs <= STALE_MS,
    };
  });
  const freshRows = rows.filter((row) => row.fresh === true).length;
  const providerCoverage = {};
  for (const provider of PROVIDERS) {
    const providerRows = rows.filter((row) => row.provider === provider);
    providerCoverage[provider] = {
      target_rows: TARGET_PER_PROVIDER,
      row_count: providerRows.length,
      sampled_rows: providerRows.filter((row) => Number(row.sampled_at_ms || 0) > 0).length,
      fresh_rows: providerRows.filter((row) => row.fresh === true).length,
      rpi_present_rows: providerRows.filter((row) => row.fresh === true && row.rpi_present === true).length,
      refresh_error_rows: providerRows.filter((row) => String(row.refresh_error || '').trim()).length,
    };
  }
  const payload = {
    ok: true,
    version: VERSION,
    source: 'official_rpi_orderbook_shared_focus15_overlay',
    ready: focus.ready && rows.length === TARGET_ROWS && freshRows === TARGET_ROWS,
    focus_pool_ready: focus.ready,
    focus_pool_round: focus.round,
    focus_signature: signature,
    providers: PROVIDERS,
    provider_count: PROVIDERS.length,
    target_per_provider: TARGET_PER_PROVIDER,
    target_rows: TARGET_ROWS,
    row_count: rows.length,
    sampled_rows: rows.filter((row) => Number(row.sampled_at_ms || 0) > 0).length,
    fresh_rows: freshRows,
    stale_rows: rows.filter((row) => Number(row.sampled_at_ms || 0) > 0 && row.fresh !== true).length,
    missing_rows: rows.filter((row) => Number(row.sampled_at_ms || 0) === 0).length,
    rpi_present_rows: rows.filter((row) => row.fresh === true && row.rpi_present === true).length,
    provider_coverage: providerCoverage,
    depth_levels: LEVELS,
    scan_interval_seconds: SCAN_INTERVAL_MS / 1000,
    sample_per_provider_per_cycle: SAMPLE_PER_PROVIDER_PER_CYCLE,
    worst_case_refresh_seconds: Math.ceil(TARGET_PER_PROVIDER / SAMPLE_PER_PROVIDER_PER_CYCLE) * SCAN_INTERVAL_MS / 1000,
    stale_threshold_seconds: STALE_MS / 1000,
    official_first: true,
    official_empty_stays_zero_or_null: true,
    standard_orderbook_kept_separate: true,
    local_rpi_guessing: false,
    cross_provider_substitution: false,
    cross_quote_substitution: false,
    exchange_requests_started_by_user_read: 0,
    exchange_connections_started_by_user_read: 0,
    user_reads_trigger_scan: false,
    user_reads_scale_with_users: false,
    rows,
    generated_at: lastCompletedAt,
  };
  responseCache = { at: now, payload };
  return { ...clone(payload), cache_hit: false, cache_age_ms: 0 };
}

export function runContractRpiSharedSelfTest() {
  const okx = parseOfficialLevel('okx', ['100', '7', '5', '3'], 'bid');
  const bybit = parseOfficialLevel('bybit', ['100', '5', '2'], 'bid');
  const bitget = parseOfficialLevel('bitget', [100, 5, 2], 'ask');
  const malformedOkx = parseOfficialLevel('okx', ['100', '4', '5', '2'], 'bid');
  return {
    ok: Boolean(
      okx?.total_quantity === 7 && okx?.non_rpi_quantity === 5 && okx?.rpi_quantity === 2 &&
      bybit?.total_quantity === 7 && bybit?.non_rpi_quantity === 5 && bybit?.rpi_quantity === 2 &&
      bitget?.total_quantity === 7 && bitget?.non_rpi_quantity === 5 && bitget?.rpi_quantity === 2 &&
      malformedOkx == null
    ),
    okx_total_minus_non_rpi: okx,
    bybit_official_separate_components: bybit,
    bitget_official_separate_components: bitget,
    materially_inconsistent_okx_rejected: malformedOkx == null,
  };
}

export function getContractRpiSharedHealth() {
  const payload = currentPayload();
  return {
    ok: true,
    version: VERSION,
    enabled: started || process.env.KAKA_DISABLE_CONTRACT_RPI_SHARED !== '1',
    snapshot_endpoint: SNAPSHOT_ROUTE,
    health_endpoint: HEALTH_ROUTE,
    ready: payload.ready,
    focus_pool_ready: payload.focus_pool_ready,
    target_rows: TARGET_ROWS,
    row_count: payload.row_count,
    sampled_rows: payload.sampled_rows,
    fresh_rows: payload.fresh_rows,
    stale_rows: payload.stale_rows,
    missing_rows: payload.missing_rows,
    rpi_present_rows: payload.rpi_present_rows,
    provider_coverage: payload.provider_coverage,
    depth_levels: LEVELS,
    sample_per_provider_per_cycle: SAMPLE_PER_PROVIDER_PER_CYCLE,
    scan_interval_seconds: SCAN_INTERVAL_MS / 1000,
    worst_case_refresh_seconds: payload.worst_case_refresh_seconds,
    stale_threshold_seconds: STALE_MS / 1000,
    request_timeout_ms: REQUEST_TIMEOUT_MS,
    retry_cooldown_seconds: RETRY_COOLDOWN_MS / 1000,
    running: Boolean(running),
    round,
    total_scans: totalScans,
    total_scan_failures: totalScanFailures,
    request_attempts: requestAttempts,
    request_successes: requestSuccesses,
    request_failures: requestFailures,
    cooldown_skips: cooldownSkips,
    last_started_at: lastStartedAt,
    last_completed_at: lastCompletedAt,
    last_error: lastError,
    total_reads: totalReads,
    response_cache_ttl_seconds: RESPONSE_CACHE_TTL_MS / 1000,
    response_cache_hits: responseCacheHits,
    response_cache_misses: responseCacheMisses,
    background_collection_is_bounded: true,
    failures_preserve_last_verified_snapshot: true,
    official_first: true,
    official_empty_stays_zero_or_null: true,
    standard_orderbook_kept_separate: true,
    local_rpi_guessing: false,
    exchange_requests_started_by_user_read: 0,
    exchange_connections_started_by_user_read: 0,
    user_reads_trigger_scan: false,
    user_reads_scale_with_users: false,
    self_test: runContractRpiSharedSelfTest(),
  };
}

function sendJson(res, status, payload) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': String(body.length),
  });
  res.end(body);
}

export async function handleContractRpiShared(req, res, url) {
  if (url.pathname === HEALTH_ROUTE) {
    sendJson(res, 200, getContractRpiSharedHealth());
    return true;
  }
  if (url.pathname !== SNAPSHOT_ROUTE) return false;
  const provider = normalizeProvider(url.searchParams.get('provider'));
  const symbol = normalizeSymbol(url.searchParams.get('symbol'));
  const payload = currentPayload();
  if (provider || symbol) {
    const rows = payload.rows.filter((row) =>
      (!provider || row.provider === provider) && (!symbol || row.symbol === symbol)
    );
    sendJson(res, 200, {
      ...payload,
      filtered: true,
      filter_provider: provider || null,
      filter_symbol: symbol || null,
      row_count: rows.length,
      rows,
    });
    return true;
  }
  sendJson(res, 200, payload);
  return true;
}
