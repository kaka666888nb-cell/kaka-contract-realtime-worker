// Kaka Web3 Step308「合约 Edge Function 接口规划 + 草案」
// Function name: kaka-contract-binance-sync
// 当前是草案：先 dry_run / 手动调用审核，不要直接上 Cron 高频跑。
// 只使用 Binance USDS-M public market endpoints，不接交易账户，不下单，不需要 Binance API Key。

import { serve } from 'https://deno.land/std@0.224.0/http/server.ts';
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.45.4';

type ContractSymbolConfig = {
  provider: string;
  market_type: string;
  symbol: string;
  base_symbol: string;
  quote_symbol: string;
  enabled: boolean;
};

type SyncMode = 'dry_run' | 'snapshots' | 'klines' | 'ratios' | 'taker' | 'cvd' | 'all';

const FUNCTION_VERSION = '1073.r35b.contract-kline-finality.1';
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type, x-kaka-sync-secret',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};

const BINANCE_FAPI_BASE = 'https://fapi.binance.com';
const BINANCE_FUTURES_DATA_BASE = 'https://fapi.binance.com/futures/data';
const DEFAULT_SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT', 'AVAXUSDT', 'LINKUSDT', 'TRXUSDT', 'DOTUSDT', 'LTCUSDT'];
const DEFAULT_INTERVALS = ['5m'];
const DEFAULT_PERIOD = '5m';
const DEFAULT_LIMIT = 30;
const MAX_SYMBOLS = 12;
const MAX_INTERVALS = 15;
const MAX_UPSTREAM_REQUEST_UNITS = 60;
const FETCH_TIMEOUT_MS = 12_000;
const VALID_INTERVALS = new Set(['1m','3m','5m','15m','30m','1h','2h','4h','6h','8h','12h','1d','3d','1w','1M']);

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers: { ...corsHeaders, 'Content-Type': 'application/json; charset=utf-8' },
  });
}

function textOrDefault(value: unknown, fallback: string): string {
  const text = typeof value === 'string' ? value.trim() : '';
  return text.length === 0 ? fallback : text;
}

function bearerToken(req: Request): string {
  const auth = req.headers.get('authorization')?.trim() ?? '';
  return auth.toLowerCase().startsWith('bearer ') ? auth.substring(7).trim() : '';
}

function decodeJwtClaims(token: string): Record<string, unknown> | null {
  const parts = token.split('.');
  if (parts.length !== 3) return null;
  try {
    const base64 = parts[1].replaceAll('-', '+').replaceAll('_', '/');
    const padded = base64.padEnd(Math.ceil(base64.length / 4) * 4, '=');
    return JSON.parse(atob(padded)) as Record<string, unknown>;
  } catch (_) {
    return null;
  }
}

function projectRefFromUrl(): string {
  try {
    return new URL(Deno.env.get('SUPABASE_URL') ?? '').hostname.split('.')[0] ?? '';
  } catch (_) {
    return '';
  }
}

function authorizeInternal(req: Request, serviceRoleKey: string): { ok: boolean; error?: string } {
  const configuredSecret = Deno.env.get('KAKA_SYNC_SECRET')?.trim() ?? '';
  if (!configuredSecret) return { ok: false, error: 'sync_secret_not_configured' };
  const providedSecret = req.headers.get('x-kaka-sync-secret')?.trim() ?? '';
  if (!providedSecret || providedSecret !== configuredSecret) {
    return { ok: false, error: 'invalid_sync_secret' };
  }

  const apikey = req.headers.get('apikey')?.trim() ?? '';
  const bearer = bearerToken(req);
  if (!apikey || !bearer || apikey !== bearer) {
    return { ok: false, error: 'service_role_required' };
  }

  if (serviceRoleKey && bearer === serviceRoleKey) return { ok: true };

  const claims = decodeJwtClaims(bearer);
  const expectedRef = projectRefFromUrl();
  if (
    claims?.role !== 'service_role' ||
    (expectedRef && claims?.ref !== expectedRef)
  ) {
    return { ok: false, error: 'service_role_required' };
  }
  return { ok: true };
}

function upstreamRequestUnits(mode: SyncMode, symbolCount: number, intervalCount: number): number {
  if (mode === 'dry_run' || mode === 'cvd') return 0;
  if (mode === 'snapshots') return symbolCount * 3;
  if (mode === 'klines') return symbolCount * intervalCount;
  if (mode === 'ratios') return symbolCount * 4;
  if (mode === 'taker') return symbolCount;
  if (mode === 'all') return symbolCount * (8 + intervalCount);
  return MAX_UPSTREAM_REQUEST_UNITS + 1;
}

function numOrNull(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  const n = Number(String(value).replaceAll(',', '').trim());
  return Number.isFinite(n) ? n : null;
}

function msToIso(value: unknown): string | null {
  const n = numOrNull(value);
  if (n === null || n <= 0) return null;
  try {
    return new Date(n).toISOString();
  } catch (_) {
    return null;
  }
}

function baseFromSymbol(symbol: string): string {
  const clean = symbol.trim().toUpperCase();
  return clean.endsWith('USDT') && clean.length > 4 ? clean.substring(0, clean.length - 4) : clean;
}

function safeSymbolConfig(symbol: string): ContractSymbolConfig {
  const clean = symbol.trim().toUpperCase().replaceAll('-', '');
  return {
    provider: 'binance',
    market_type: 'contract',
    symbol: clean,
    base_symbol: baseFromSymbol(clean),
    quote_symbol: 'USDT',
    enabled: true,
  };
}

async function fetchJson(url: string, label: string): Promise<any> {
  const resp = await fetch(url, {
    headers: {
      'User-Agent': 'KakaWeb3-ContractDataSync/step1073-r11',
      'Accept': 'application/json',
    },
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`${label} failed: HTTP ${resp.status} ${text.substring(0, 240)}`);
  }
  try {
    return JSON.parse(text);
  } catch (_) {
    throw new Error(`${label} returned non-json: ${text.substring(0, 240)}`);
  }
}

async function loadEnabledSymbols(supabase: ReturnType<typeof createClient>, forcedSymbols: string[]): Promise<ContractSymbolConfig[]> {
  if (forcedSymbols.length > 0) {
    return forcedSymbols.map(safeSymbolConfig);
  }

  const { data, error } = await supabase
    .from('app_contract_symbol_configs')
    .select('provider,market_type,symbol,base_symbol,quote_symbol,enabled')
    .eq('provider', 'binance')
    .eq('market_type', 'contract')
    .eq('enabled', true)
    .order('sort_order', { ascending: true });

  if (error) throw new Error(`load enabled symbols failed: ${error.message}`);

  const rows = (data ?? []) as ContractSymbolConfig[];
  if (rows.length > 0) return rows;
  return DEFAULT_SYMBOLS.map(safeSymbolConfig);
}

async function upsertSnapshots(supabase: ReturnType<typeof createClient>, symbols: ContractSymbolConfig[]) {
  const rows: any[] = [];
  const errors: string[] = [];

  for (const item of symbols) {
    const symbol = item.symbol;
    try {
      const ticker = await fetchJson(`${BINANCE_FAPI_BASE}/fapi/v1/ticker/24hr?symbol=${encodeURIComponent(symbol)}`, `${symbol} ticker24h`);
      const premium = await fetchJson(`${BINANCE_FAPI_BASE}/fapi/v1/premiumIndex?symbol=${encodeURIComponent(symbol)}`, `${symbol} premiumIndex`);
      const oi = await fetchJson(`${BINANCE_FAPI_BASE}/fapi/v1/openInterest?symbol=${encodeURIComponent(symbol)}`, `${symbol} openInterest`);

      const openInterest = numOrNull(oi.openInterest);
      const markPrice = numOrNull(premium.markPrice);
      rows.push({
        provider: 'binance',
        market_type: 'contract',
        symbol,
        base_symbol: item.base_symbol || baseFromSymbol(symbol),
        quote_symbol: item.quote_symbol || 'USDT',
        last_price: numOrNull(ticker.lastPrice),
        price_change_percent_24h: numOrNull(ticker.priceChangePercent),
        high_price_24h: numOrNull(ticker.highPrice),
        low_price_24h: numOrNull(ticker.lowPrice),
        volume_24h: numOrNull(ticker.volume),
        quote_volume_24h: numOrNull(ticker.quoteVolume),
        open_interest: openInterest,
        open_interest_value: openInterest !== null && markPrice !== null ? openInterest * markPrice : null,
        funding_rate: numOrNull(premium.lastFundingRate),
        next_funding_time: msToIso(premium.nextFundingTime),
        source_time: msToIso(premium.time ?? oi.time ?? ticker.closeTime) ?? new Date().toISOString(),
        raw: { ticker, premiumIndex: premium, openInterest: oi },
        updated_at: new Date().toISOString(),
      });
    } catch (e) {
      errors.push(`${symbol}: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  if (rows.length > 0) {
    const { error } = await supabase
      .from('app_contract_market_snapshots')
      .upsert(rows, { onConflict: 'provider,market_type,symbol' });
    if (error) throw new Error(`upsert snapshots failed: ${error.message}`);
  }

  return { inserted_or_updated: rows.length, errors };
}

async function upsertKlines(supabase: ReturnType<typeof createClient>, symbols: ContractSymbolConfig[], intervals: string[], limit: number) {
  let total = 0;
  let actualWritten = 0;
  let unchanged = 0;
  const errors: string[] = [];

  for (const item of symbols) {
    const symbol = item.symbol;
    for (const interval of intervals) {
      try {
        const raw = await fetchJson(`${BINANCE_FAPI_BASE}/fapi/v1/klines?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}&limit=${limit}`, `${symbol} ${interval} klines`);
        if (!Array.isArray(raw)) throw new Error('klines response is not an array');
        const rows = raw.map((k: any[]) => ({
          provider: 'binance',
          market_type: 'contract',
          symbol,
          base_symbol: item.base_symbol || baseFromSymbol(symbol),
          quote_symbol: item.quote_symbol || 'USDT',
          interval,
          open_time: msToIso(k[0]),
          open_price: numOrNull(k[1]),
          high_price: numOrNull(k[2]),
          low_price: numOrNull(k[3]),
          close_price: numOrNull(k[4]),
          volume: numOrNull(k[5]),
          close_time: msToIso(k[6]),
          quote_volume: numOrNull(k[7]),
          trade_count: numOrNull(k[8]),
          taker_buy_volume: numOrNull(k[9]),
          taker_buy_quote_volume: numOrNull(k[10]),
          source_time: msToIso(k[6]),
          raw: { kline: k },
          updated_at: new Date().toISOString(),
        })).filter((row) => row.open_time !== null);

        const finalityNowMs = Date.now();
        const finalizedRows = rows.filter((row) => {
          const closeMs = Date.parse(String(row.close_time || ''));
          return Number.isFinite(closeMs) && closeMs <= finalityNowMs - 2_000;
        });

        if (finalizedRows.length > 0) {
          const { data, error } = await supabase.rpc('app_upsert_contract_klines_batch_diff', { p_rows: finalizedRows });
          if (error) throw new Error(error.message);
          total += finalizedRows.length;
          actualWritten += Number(data?.written || 0);
          unchanged += Number(data?.unchanged || 0);
        }
      } catch (e) {
        errors.push(`${symbol}/${interval}: ${e instanceof Error ? e.message : String(e)}`);
      }
    }
  }

  return {
    inserted_or_updated: total,
    actual_written: actualWritten,
    unchanged,
    persistence: 'app_upsert_contract_klines_batch_diff',
    open_candle_persisted: false,
    finality_grace_ms: 2000,
    historical_noop_updates_skipped: true,
    errors,
  };
}

async function upsertOiHistory(supabase: ReturnType<typeof createClient>, symbols: ContractSymbolConfig[], period: string, limit: number) {
  let total = 0;
  const errors: string[] = [];
  for (const item of symbols) {
    const symbol = item.symbol;
    try {
      const raw = await fetchJson(`${BINANCE_FUTURES_DATA_BASE}/openInterestHist?symbol=${encodeURIComponent(symbol)}&period=${encodeURIComponent(period)}&limit=${limit}`, `${symbol} oi hist`);
      if (!Array.isArray(raw)) throw new Error('openInterestHist response is not an array');
      const rows = raw.map((r) => ({
        provider: 'binance',
        market_type: 'contract',
        symbol,
        base_symbol: item.base_symbol || baseFromSymbol(symbol),
        quote_symbol: item.quote_symbol || 'USDT',
        period,
        open_interest: numOrNull(r.sumOpenInterest),
        open_interest_value: numOrNull(r.sumOpenInterestValue),
        source_time: msToIso(r.timestamp),
        raw: r,
      })).filter((row) => row.source_time !== null);
      if (rows.length > 0) {
        const { error } = await supabase
          .from('app_contract_open_interest_history_cache')
          .upsert(rows, { onConflict: 'provider,market_type,symbol,period,source_time' });
        if (error) throw new Error(error.message);
        total += rows.length;
      }
    } catch (e) {
      errors.push(`${symbol}: ${e instanceof Error ? e.message : String(e)}`);
    }
  }
  return { inserted_or_updated: total, errors };
}

async function upsertLongShortRatios(supabase: ReturnType<typeof createClient>, symbols: ContractSymbolConfig[], period: string, limit: number) {
  const endpoints = [
    { ratio_type: 'global_account', path: 'globalLongShortAccountRatio' },
    { ratio_type: 'top_account', path: 'topLongShortAccountRatio' },
    { ratio_type: 'top_position', path: 'topLongShortPositionRatio' },
  ];
  let total = 0;
  const errors: string[] = [];

  for (const item of symbols) {
    const symbol = item.symbol;
    for (const ep of endpoints) {
      try {
        const raw = await fetchJson(`${BINANCE_FUTURES_DATA_BASE}/${ep.path}?symbol=${encodeURIComponent(symbol)}&period=${encodeURIComponent(period)}&limit=${limit}`, `${symbol} ${ep.ratio_type}`);
        if (!Array.isArray(raw)) throw new Error(`${ep.ratio_type} response is not an array`);
        const rows = raw.map((r) => ({
          provider: 'binance',
          market_type: 'contract',
          symbol,
          base_symbol: item.base_symbol || baseFromSymbol(symbol),
          quote_symbol: item.quote_symbol || 'USDT',
          ratio_type: ep.ratio_type,
          period,
          long_account: numOrNull(r.longAccount),
          short_account: numOrNull(r.shortAccount),
          long_short_ratio: numOrNull(r.longShortRatio),
          source_time: msToIso(r.timestamp),
          raw: r,
        })).filter((row) => row.source_time !== null);
        if (rows.length > 0) {
          const { error } = await supabase
            .from('app_contract_long_short_ratio_cache')
            .upsert(rows, { onConflict: 'provider,market_type,symbol,ratio_type,period,source_time' });
          if (error) throw new Error(error.message);
          total += rows.length;
        }
      } catch (e) {
        errors.push(`${symbol}/${ep.ratio_type}: ${e instanceof Error ? e.message : String(e)}`);
      }
    }
  }
  return { inserted_or_updated: total, errors };
}

async function upsertTakerBuySell(supabase: ReturnType<typeof createClient>, symbols: ContractSymbolConfig[], period: string, limit: number) {
  let total = 0;
  const errors: string[] = [];

  for (const item of symbols) {
    const symbol = item.symbol;
    try {
      const raw = await fetchJson(`${BINANCE_FUTURES_DATA_BASE}/takerlongshortRatio?symbol=${encodeURIComponent(symbol)}&period=${encodeURIComponent(period)}&limit=${limit}`, `${symbol} taker buy/sell`);
      if (!Array.isArray(raw)) throw new Error('taker response is not an array');
      const rows = raw.map((r) => ({
        provider: 'binance',
        market_type: 'contract',
        symbol,
        base_symbol: item.base_symbol || baseFromSymbol(symbol),
        quote_symbol: item.quote_symbol || 'USDT',
        period,
        buy_volume: numOrNull(r.buyVol),
        sell_volume: numOrNull(r.sellVol),
        buy_sell_ratio: numOrNull(r.buySellRatio),
        buy_quote_volume: null,
        sell_quote_volume: null,
        source_time: msToIso(r.timestamp),
        raw: r,
      })).filter((row) => row.source_time !== null);
      if (rows.length > 0) {
        const { error } = await supabase
          .from('app_contract_taker_buy_sell_cache')
          .upsert(rows, { onConflict: 'provider,market_type,symbol,period,source_time' });
        if (error) throw new Error(error.message);
        total += rows.length;
      }
    } catch (e) {
      errors.push(`${symbol}: ${e instanceof Error ? e.message : String(e)}`);
    }
  }
  return { inserted_or_updated: total, errors };
}

async function rebuildCvdFromTaker(supabase: ReturnType<typeof createClient>, symbols: ContractSymbolConfig[], period: string) {
  let total = 0;
  const errors: string[] = [];

  for (const item of symbols) {
    const symbol = item.symbol;
    try {
      const { data, error } = await supabase
        .from('app_contract_taker_buy_sell_cache')
        .select('provider,market_type,symbol,base_symbol,quote_symbol,period,buy_volume,sell_volume,source_time,raw')
        .eq('provider', 'binance')
        .eq('market_type', 'contract')
        .eq('symbol', symbol)
        .eq('period', period)
        .order('source_time', { ascending: true })
        .limit(500);
      if (error) throw new Error(error.message);
      const rows = data ?? [];
      let cvd = 0;
      const out = rows.map((r: any) => {
        const buy = numOrNull(r.buy_volume) ?? 0;
        const sell = numOrNull(r.sell_volume) ?? 0;
        const delta = buy - sell;
        cvd += delta;
        return {
          provider: 'binance',
          market_type: 'contract',
          symbol,
          base_symbol: item.base_symbol || baseFromSymbol(symbol),
          quote_symbol: item.quote_symbol || 'USDT',
          interval: period,
          cvd_value: cvd,
          delta_volume: delta,
          buy_volume: buy,
          sell_volume: sell,
          source_time: r.source_time,
          raw: { source: 'taker_buy_sell_cache', taker_raw: r.raw },
        };
      });
      if (out.length > 0) {
        const { error: upsertError } = await supabase
          .from('app_contract_cvd_cache')
          .upsert(out, { onConflict: 'provider,market_type,symbol,interval,source_time' });
        if (upsertError) throw new Error(upsertError.message);
        total += out.length;
      }
    } catch (e) {
      errors.push(`${symbol}: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  return { inserted_or_updated: total, errors };
}

serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: corsHeaders });
  if (req.method !== 'POST') {
    return jsonResponse({ ok: false, function_version: FUNCTION_VERSION, error: 'POST required' }, 405);
  }

  const startedAt = new Date();
  try {
    const supabaseUrl = Deno.env.get('SUPABASE_URL') ?? '';
    const serviceRoleKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? '';
    if (!supabaseUrl || !serviceRoleKey) {
      return jsonResponse({ ok: false, function_version: FUNCTION_VERSION, error: 'Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY' }, 500);
    }

    const auth = authorizeInternal(req, serviceRoleKey);
    if (!auth.ok) {
      return jsonResponse({ ok: false, function_version: FUNCTION_VERSION, error: auth.error }, 401);
    }

    let body: Record<string, unknown> = {};
    try { body = await req.json(); } catch (_) { body = {}; }

    const modeRaw = textOrDefault(body.mode, 'dry_run');
    if (!['dry_run', 'snapshots', 'klines', 'ratios', 'taker', 'cvd', 'all'].includes(modeRaw)) {
      return jsonResponse({ ok: false, function_version: FUNCTION_VERSION, error: 'unsupported_mode', mode: modeRaw }, 400);
    }
    const normalizedMode = modeRaw as SyncMode;
    const period = textOrDefault(body.period, DEFAULT_PERIOD);
    if (!VALID_INTERVALS.has(period)) {
      return jsonResponse({ ok: false, function_version: FUNCTION_VERSION, error: 'unsupported_period', period }, 400);
    }

    const limitRaw = numOrNull(body.limit);
    const limit = Math.max(1, Math.min(limitRaw ?? DEFAULT_LIMIT, 500));
    const intervalsRaw = textOrDefault(body.intervals, DEFAULT_INTERVALS.join(','));
    const intervals = [...new Set(intervalsRaw.split(',').map((x) => x.trim()).filter(Boolean))];
    if (intervals.length < 1 || intervals.length > MAX_INTERVALS) {
      return jsonResponse({ ok: false, function_version: FUNCTION_VERSION, error: 'interval_count_out_of_range', max_intervals: MAX_INTERVALS }, 400);
    }
    const badIntervals = intervals.filter((x) => !VALID_INTERVALS.has(x));
    if (badIntervals.length > 0) {
      return jsonResponse({ ok: false, function_version: FUNCTION_VERSION, error: 'unsupported_interval', intervals: badIntervals }, 400);
    }

    const symbolsRaw = textOrDefault(body.symbols, '');
    const forcedSymbols = [...new Set(symbolsRaw.split(',').map((x) => x.trim().toUpperCase()).filter(Boolean))];
    if (forcedSymbols.length > MAX_SYMBOLS) {
      return jsonResponse({ ok: false, function_version: FUNCTION_VERSION, error: 'too_many_symbols', max_symbols: MAX_SYMBOLS }, 400);
    }
    const badSymbols = forcedSymbols.filter((x) => !/^[A-Z0-9]{2,30}$/.test(x));
    if (badSymbols.length > 0) {
      return jsonResponse({ ok: false, function_version: FUNCTION_VERSION, error: 'invalid_symbol', symbols: badSymbols }, 400);
    }

    const supabase = createClient(supabaseUrl, serviceRoleKey, {
      auth: { persistSession: false, autoRefreshToken: false },
    });

    const symbols = await loadEnabledSymbols(supabase, forcedSymbols);
    if (symbols.length < 1 || symbols.length > MAX_SYMBOLS) {
      return jsonResponse({ ok: false, function_version: FUNCTION_VERSION, error: 'resolved_symbol_count_out_of_range', max_symbols: MAX_SYMBOLS, resolved_symbols: symbols.length }, 400);
    }

    const requestUnits = upstreamRequestUnits(normalizedMode, symbols.length, intervals.length);
    if (requestUnits > MAX_UPSTREAM_REQUEST_UNITS) {
      return jsonResponse({
        ok: false,
        function_version: FUNCTION_VERSION,
        error: 'upstream_request_budget_exceeded',
        request_units: requestUnits,
        max_request_units: MAX_UPSTREAM_REQUEST_UNITS,
      }, 400);
    }

    const plan = {
      function_version: FUNCTION_VERSION,
      mode: normalizedMode,
      provider: 'binance',
      market_type: 'contract',
      symbols: symbols.map((x) => x.symbol),
      period,
      intervals,
      limit,
      request_units: requestUnits,
      max_request_units: MAX_UPSTREAM_REQUEST_UNITS,
      credential_scope: 'service_role_plus_sync_secret',
    };

    if (normalizedMode === 'dry_run') {
      return jsonResponse({ ok: true, dry_run: true, plan, elapsed_ms: Date.now() - startedAt.getTime() });
    }

    const result: Record<string, unknown> = { plan };
    if (normalizedMode === 'snapshots' || normalizedMode === 'all') {
      result.snapshots = await upsertSnapshots(supabase, symbols);
    }
    if (normalizedMode === 'klines' || normalizedMode === 'all') {
      result.klines = await upsertKlines(supabase, symbols, intervals, limit);
    }
    if (normalizedMode === 'ratios' || normalizedMode === 'all') {
      result.oi_history = await upsertOiHistory(supabase, symbols, period, limit);
      result.long_short_ratios = await upsertLongShortRatios(supabase, symbols, period, limit);
    }
    if (normalizedMode === 'taker' || normalizedMode === 'all') {
      result.taker_buy_sell = await upsertTakerBuySell(supabase, symbols, period, limit);
    }
    if (normalizedMode === 'cvd' || normalizedMode === 'all') {
      result.cvd = await rebuildCvdFromTaker(supabase, symbols, period);
    }

    return jsonResponse({ ok: true, function_version: FUNCTION_VERSION, ...result, elapsed_ms: Date.now() - startedAt.getTime() });
  } catch (e) {
    return jsonResponse({ ok: false, function_version: FUNCTION_VERSION, error: e instanceof Error ? e.message : String(e), elapsed_ms: Date.now() - startedAt.getTime() }, 500);
  }
});
