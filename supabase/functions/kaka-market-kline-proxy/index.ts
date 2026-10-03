// =========================================================
// Kaka Web3 App
// Step276.90 Binance K线写入 app_market_klines_cache + 日志 + 数据源状态
// Function: kaka-market-kline-proxy
// Purpose:
// - Fetch Binance public spot Kline/Candlestick data
// - Normalize OHLCV fields
// - dry_run=true: preview only, no DB writes/log/status updates
// - dry_run=false: write normalized klines through service-role RPCs
// Safety:
// - No exchange API key
// - No trading/account/deposit/withdraw/order APIs
// - Uses existing KAKA_SYNC_SECRET guard if configured
// =========================================================

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.4";

const FUNCTION_VERSION = "1073.r12.spot-finality.1";
const DEFAULT_PROVIDER = "binance";
const DEFAULT_MARKET_TYPE = "spot";
const DEFAULT_SYMBOL = "BTCUSDT";
const DEFAULT_INTERVAL = "1m";
const DEFAULT_LIMIT = 60;
const MAX_LIMIT = 300;
const FETCH_TIMEOUT_MS = 10_000;
const ALLOWED_SYMBOLS = new Set([
  "BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT",
  "XRPUSDT","DOGEUSDT","ADAUSDT","AVAXUSDT",
  "LINKUSDT","TRXUSDT","DOTUSDT","LTCUSDT",
]);

const ALLOWED_INTERVALS = new Set([
  "1s", "1m", "3m", "5m", "15m", "30m",
  "1h", "2h", "4h", "6h", "8h", "12h",
  "1d", "3d", "1w", "1M",
]);

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-kaka-sync-secret",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

function jsonResponse(payload: Record<string, unknown>, status = 200): Response {
  return new Response(JSON.stringify(payload, null, 2), {
    status,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json; charset=utf-8",
    },
  });
}

function textValue(value: unknown, fallback = ""): string {
  const text = value == null ? "" : String(value).trim();
  return text === "" ? fallback : text;
}

function boolValue(value: unknown, fallback = false): boolean {
  if (typeof value === "boolean") return value;
  const text = textValue(value).toLowerCase();
  if (["true", "1", "yes", "y"].includes(text)) return true;
  if (["false", "0", "no", "n"].includes(text)) return false;
  return fallback;
}

function numberValue(value: unknown, fallback: number): number {
  if (value == null) return fallback;
  const text = String(value).trim();
  if (text === "") return fallback;
  const n = Number(text);
  return Number.isFinite(n) ? n : fallback;
}

function clampInt(value: unknown, fallback: number, min: number, max: number): number {
  const n = Math.trunc(numberValue(value, fallback));
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, n));
}

function upperSymbol(value: unknown, fallback = DEFAULT_SYMBOL): string {
  const text = textValue(value, fallback).toUpperCase().replace(/[^A-Z0-9]/g, "");
  return text === "" ? fallback : text;
}

function normalizeInterval(value: unknown): string {
  const interval = textValue(value, DEFAULT_INTERVAL);
  return ALLOWED_INTERVALS.has(interval) ? interval : DEFAULT_INTERVAL;
}

function msToIso(value: unknown): string | null {
  const n = numberValue(value, NaN);
  if (!Number.isFinite(n)) return null;
  const date = new Date(n);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString();
}

function splitSymbol(symbol: string): { base_asset: string; quote_asset: string } {
  const quotes = ["USDT", "USDC", "FDUSD", "BUSD", "BTC", "ETH", "BNB", "TRY", "EUR", "BRL"];
  for (const quote of quotes) {
    if (symbol.endsWith(quote) && symbol.length > quote.length) {
      return {
        base_asset: symbol.slice(0, symbol.length - quote.length),
        quote_asset: quote,
      };
    }
  }
  return { base_asset: symbol, quote_asset: "" };
}

function decimal(value: unknown): number | null {
  if (value == null) return null;
  const n = Number(String(value).trim());
  return Number.isFinite(n) ? n : null;
}

type NormalizedKline = {
  provider: string;
  market_type: string;
  symbol: string;
  base_asset: string;
  quote_asset: string;
  kline_interval: string;
  open_time: string | null;
  open_price: number | null;
  high_price: number | null;
  low_price: number | null;
  close_price: number | null;
  volume: number | null;
  close_time: string | null;
  quote_volume: number | null;
  trade_count: number | null;
  taker_buy_base_volume: number | null;
  taker_buy_quote_volume: number | null;
  raw_row?: unknown;
};

function normalizeKline(row: unknown, symbol: string, interval: string): NormalizedKline | null {
  if (!Array.isArray(row) || row.length < 11) return null;
  const assets = splitSymbol(symbol);
  const openTime = msToIso(row[0]);
  if (!openTime) return null;
  return {
    provider: DEFAULT_PROVIDER,
    market_type: DEFAULT_MARKET_TYPE,
    symbol,
    base_asset: assets.base_asset,
    quote_asset: assets.quote_asset,
    kline_interval: interval,
    open_time: openTime,
    open_price: decimal(row[1]),
    high_price: decimal(row[2]),
    low_price: decimal(row[3]),
    close_price: decimal(row[4]),
    volume: decimal(row[5]),
    close_time: msToIso(row[6]),
    quote_volume: decimal(row[7]),
    trade_count: Math.trunc(numberValue(row[8], 0)),
    taker_buy_base_volume: decimal(row[9]),
    taker_buy_quote_volume: decimal(row[10]),
    raw_row: row,
  };
}

function finalizedForPersistence(row: NormalizedKline, nowMs = Date.now()): boolean {
  const closeMs = Date.parse(row.close_time ?? "");
  return Number.isFinite(closeMs) && closeMs <= nowMs - 2_000;
}

async function readInput(req: Request): Promise<Record<string, unknown>> {
  const url = new URL(req.url);
  let body: Record<string, unknown> = {};
  if (req.method !== "GET") {
    const raw = await req.text();
    if (raw.trim() !== "") {
      try {
        const parsed = JSON.parse(raw);
        if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
          body = parsed as Record<string, unknown>;
        }
      } catch (_error) {
        body = {};
      }
    }
  }

  const input: Record<string, unknown> = { ...body };
  for (const [key, value] of url.searchParams.entries()) {
    input[key] = value;
  }
  return input;
}

function bearerToken(req: Request): string {
  const auth = (req.headers.get("authorization") ?? "").trim();
  return auth.toLowerCase().startsWith("bearer ") ? auth.substring(7).trim() : "";
}

function decodeJwtClaims(token: string): Record<string, unknown> | null {
  const parts = token.split(".");
  if (parts.length !== 3) return null;
  try {
    const base64 = parts[1].replaceAll("-", "+").replaceAll("_", "/");
    const padded = base64.padEnd(Math.ceil(base64.length / 4) * 4, "=");
    return JSON.parse(atob(padded)) as Record<string, unknown>;
  } catch (_error) {
    return null;
  }
}

function projectRefFromUrl(): string {
  try {
    return new URL(Deno.env.get("SUPABASE_URL") ?? "").hostname.split(".")[0] ?? "";
  } catch (_error) {
    return "";
  }
}

function authorizeInternal(req: Request): { ok: boolean; reason: string } {
  const expectedSecret = (Deno.env.get("KAKA_SYNC_SECRET") ?? "").trim();
  if (expectedSecret === "") return { ok: false, reason: "sync_secret_not_configured" };

  const actualSecret = (req.headers.get("x-kaka-sync-secret") ?? "").trim();
  if (actualSecret === "" || actualSecret !== expectedSecret) {
    return { ok: false, reason: "invalid_secret" };
  }

  const serviceRoleKey = (Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "").trim();
  const apikey = (req.headers.get("apikey") ?? "").trim();
  const bearer = bearerToken(req);
  if (!apikey || !bearer || apikey !== bearer) {
    return { ok: false, reason: "service_role_required" };
  }

  if (serviceRoleKey && bearer === serviceRoleKey) {
    return { ok: true, reason: "ok" };
  }

  const claims = decodeJwtClaims(bearer);
  const expectedRef = projectRefFromUrl();
  if (
    claims?.role !== "service_role" ||
    (expectedRef && claims?.ref !== expectedRef)
  ) {
    return { ok: false, reason: "service_role_required" };
  }

  return { ok: true, reason: "ok" };
}

function getServiceClient() {
  const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
  if (!supabaseUrl || !serviceRoleKey) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }
  return createClient(supabaseUrl, serviceRoleKey, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
}

function compactError(error: unknown): Record<string, unknown> {
  if (!error) return {};
  if (error instanceof Error) {
    return { message: error.message, name: error.name, stack: error.stack?.slice(0, 1000) };
  }
  if (typeof error === "object") return error as Record<string, unknown>;
  return { message: String(error) };
}

async function rpcLogProvider(
  supabase: ReturnType<typeof createClient>,
  payload: {
    provider: string;
    task_name: string;
    status: string;
    latency_ms: number;
    is_cache_hit: boolean;
    http_status: number | null;
    error_message: string | null;
    raw_payload: Record<string, unknown>;
  },
): Promise<string | null> {
  const { data, error } = await supabase.rpc("app_edge_log_market_provider", {
    p_provider: payload.provider,
    p_task_name: payload.task_name,
    p_status: payload.status,
    p_latency_ms: payload.latency_ms,
    p_is_cache_hit: payload.is_cache_hit,
    p_http_status: payload.http_status,
    p_error_message: payload.error_message,
    p_raw_payload: payload.raw_payload,
  });
  if (error) throw error;
  return typeof data === "string" ? data : null;
}

async function rpcUpdateSourceStatus(
  supabase: ReturnType<typeof createClient>,
  payload: {
    source_key: string;
    last_status: string;
    last_error_message: string | null;
    mark_success: boolean;
  },
): Promise<void> {
  const { error } = await supabase.rpc("app_edge_update_data_source_status", {
    p_source_key: payload.source_key,
    p_last_status: payload.last_status,
    p_last_error_message: payload.last_error_message,
    p_mark_success: payload.mark_success,
  });
  if (error) throw error;
}

type BatchKlineWriteResult = {
  written: number;
  failed: number;
  items: Array<Record<string, unknown>>;
  errors: Array<Record<string, unknown>>;
};

function batchInt(value: unknown): number {
  const n = Number(value);
  return Number.isFinite(n) ? Math.max(0, Math.trunc(n)) : 0;
}

async function rpcUpsertKlinesBatch(
  supabase: ReturnType<typeof createClient>,
  klines: NormalizedKline[],
): Promise<BatchKlineWriteResult> {
  if (klines.length === 0) {
    return { written: 0, failed: 0, items: [], errors: [] };
  }

  const rows = klines.map((kline) => ({
      provider: kline.provider,
      market_type: kline.market_type,
      symbol: kline.symbol,
      kline_interval: kline.kline_interval,
      open_time: kline.open_time,
      close_time: kline.close_time,
      open_price: kline.open_price,
      high_price: kline.high_price,
      low_price: kline.low_price,
      close_price: kline.close_price,
      volume: kline.volume,
      quote_volume: kline.quote_volume,
      trade_count: kline.trade_count,
      raw_payload: {
        function_version: FUNCTION_VERSION,
        source: "binance_spot_klines",
        taker_buy_base_volume: kline.taker_buy_base_volume,
        taker_buy_quote_volume: kline.taker_buy_quote_volume,
        raw_row: kline.raw_row,
      },
    }));
  const { data, error } = await supabase.rpc("app_edge_upsert_market_klines_batch", {
    p_rows: rows,
  });
  if (error) throw error;
  if (!data || typeof data !== "object" || Array.isArray(data)) {
    throw new Error("invalid_batch_kline_rpc_response");
  }

  const result = data as Record<string, unknown>;
  return {
    written: batchInt(result.written),
    failed: batchInt(result.failed),
    items: Array.isArray(result.items)
      ? result.items.filter((item): item is Record<string, unknown> =>
        item != null && typeof item === "object" && !Array.isArray(item))
      : [],
    errors: Array.isArray(result.errors)
      ? result.errors.filter((item): item is Record<string, unknown> =>
        item != null && typeof item === "object" && !Array.isArray(item))
      : [],
  };
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }
  if (req.method !== "POST") {
    return jsonResponse({ ok: false, function_version: FUNCTION_VERSION, error: "POST required" }, 405);
  }

  const startedAt = Date.now();
  const sourceKey = "binance_spot_kline";
  const taskName = "market_kline_sync";
  const auth = authorizeInternal(req);
  if (!auth.ok) {
    return jsonResponse({
      ok: false,
      function_version: FUNCTION_VERSION,
      error: "Unauthorized",
      reason: auth.reason,
    }, 401);
  }

  try {
    const input = await readInput(req);
    const provider = textValue(input.provider, DEFAULT_PROVIDER).toLowerCase();
    if (provider !== DEFAULT_PROVIDER) {
      return jsonResponse({
        ok: false,
        function_version: FUNCTION_VERSION,
        error: "unsupported_provider",
        provider,
        supported_providers: [DEFAULT_PROVIDER],
      }, 400);
    }

    const rawSymbol = textValue(input.symbol, DEFAULT_SYMBOL).trim().toUpperCase();
    if (!/^[A-Z0-9]{2,24}$/.test(rawSymbol) || !ALLOWED_SYMBOLS.has(rawSymbol)) {
      return jsonResponse({
        ok: false,
        function_version: FUNCTION_VERSION,
        error: "unsupported_symbol",
        symbol: rawSymbol,
      }, 400);
    }
    const symbol = rawSymbol;

    const rawInterval = textValue(input.interval ?? input.kline_interval, DEFAULT_INTERVAL);
    if (!ALLOWED_INTERVALS.has(rawInterval)) {
      return jsonResponse({
        ok: false,
        function_version: FUNCTION_VERSION,
        error: "unsupported_interval",
        kline_interval: rawInterval,
      }, 400);
    }
    const interval = rawInterval;
    const limit = clampInt(input.limit, DEFAULT_LIMIT, 1, MAX_LIMIT);
    const dryRun = boolValue(input.dry_run, true);

    const baseUrl = textValue(Deno.env.get("BINANCE_SPOT_API_BASE"), "https://api.binance.com").replace(/\/$/, "");
    const requestUrl = new URL(`${baseUrl}/api/v3/klines`);
    requestUrl.searchParams.set("symbol", symbol);
    requestUrl.searchParams.set("interval", interval);
    requestUrl.searchParams.set("limit", String(limit));

    const upstream = await fetch(requestUrl.toString(), {
      method: "GET",
      headers: { "Accept": "application/json" },
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
    });

    const rawText = await upstream.text();
    let rawJson: unknown = null;
    try {
      rawJson = rawText ? JSON.parse(rawText) : null;
    } catch (_error) {
      rawJson = rawText;
    }

    const rows = Array.isArray(rawJson) ? rawJson : [];
    const normalized = rows
      .map((row) => normalizeKline(row, symbol, interval))
      .filter((row): row is NormalizedKline => row !== null);

    const finalized = normalized.filter((row) => finalizedForPersistence(row));
    const skippedUnfinalized = Math.max(0, normalized.length - finalized.length);
    const providerError = upstream.ok ? "" : (typeof rawJson === "object" ? JSON.stringify(rawJson) : rawText).slice(0, 1000);
    let written = 0;
    let failedWrites = 0;
    const itemErrors: unknown[] = [];
    const sideEffectErrors: unknown[] = [];
    const written_ids: Array<Record<string, unknown>> = [];

    if (!dryRun) {
      const supabase = getServiceClient();

      if (upstream.ok && finalized.length > 0) {
        try {
          const batch = await rpcUpsertKlinesBatch(supabase, finalized);
          written = batch.written;
          failedWrites = batch.failed;
          written_ids.push(...batch.items);
          itemErrors.push(...batch.errors);
        } catch (error) {
          failedWrites = finalized.length;
          itemErrors.push({
            batch: true,
            normalized: normalized.length,
            error: compactError(error),
          });
        }
      }

      const okForSideEffects = upstream.ok && normalized.length > 0 && failedWrites === 0;
      const status = okForSideEffects ? "success" : "error";
      const errorMessage = okForSideEffects ? null : providerError || (itemErrors.length > 0 ? "kline_write_error" : "kline_fetch_or_normalize_error");
      const rawPayload = {
        function_version: FUNCTION_VERSION,
        source_key: sourceKey,
        symbol,
        kline_interval: interval,
        limit,
        request_url: requestUrl.toString(),
        fetched: rows.length,
        normalized: normalized.length,
        finalized_for_persistence: finalized.length,
        skipped_unfinalized: skippedUnfinalized,
        write_mode: "batch_rpc",
        written,
        failed_writes: failedWrites,
        skipped: Math.max(0, rows.length - normalized.length),
        latest_open_time: normalized.length > 0 ? normalized[normalized.length - 1].open_time : null,
        latest_close_time: normalized.length > 0 ? normalized[normalized.length - 1].close_time : null,
      };

      try {
        await rpcLogProvider(supabase, {
          provider: DEFAULT_PROVIDER,
          task_name: taskName,
          status,
          latency_ms: Date.now() - startedAt,
          is_cache_hit: false,
          http_status: upstream.status,
          error_message: errorMessage,
          raw_payload: rawPayload,
        });
      } catch (error) {
        sideEffectErrors.push({ action: "log_provider", error: compactError(error) });
      }

      try {
        await rpcUpdateSourceStatus(supabase, {
          source_key: sourceKey,
          last_status: status,
          last_error_message: errorMessage,
          mark_success: status === "success",
        });
      } catch (error) {
        sideEffectErrors.push({ action: "update_source_status", error: compactError(error) });
      }
    }

    const ok = upstream.ok && (dryRun || (written > 0 && failedWrites === 0 && sideEffectErrors.length === 0));

    return jsonResponse({
      ok,
      mode: dryRun ? "dry_run" : "sync",
      write_enabled: !dryRun,
      side_effects: dryRun ? "disabled" : "enabled",
      provider: DEFAULT_PROVIDER,
      function_version: FUNCTION_VERSION,
      source_key: sourceKey,
      market_type: DEFAULT_MARKET_TYPE,
      symbol,
      kline_interval: interval,
      limit,
      request_url: requestUrl.toString(),
      http_status: upstream.status,
      duration_ms: Date.now() - startedAt,
      fetched: rows.length,
      normalized: normalized.length,
      write_mode: dryRun ? "disabled" : "batch_rpc",
      skipped: Math.max(0, rows.length - normalized.length),
      written,
      failed_writes: failedWrites,
      provider_error: providerError,
      side_effect_errors: sideEffectErrors,
      item_errors: itemErrors,
      written_ids: written_ids.slice(-10),
      preview: normalized.slice(-10).map(({ raw_row: _rawRow, ...rest }) => rest),
      note: dryRun
        ? "Step276.90 dry_run=true 只验证 Binance K线请求和 OHLCV 字段归一化，不写数据库。"
        : "Step276.90 dry_run=false 写入 app_market_klines_cache，并记录 provider logs/source status。",
      next_step_hint: "通过后可做 Step276.91：K线 1m 定时同步 / Cron 设置，之后再做 App K线图基础展示。",
    }, ok ? 200 : 502);
  } catch (error) {
    return jsonResponse({
      ok: false,
      function_version: FUNCTION_VERSION,
      error: error instanceof Error ? error.message : String(error),
      duration_ms: Date.now() - startedAt,
    }, 500);
  }
});
