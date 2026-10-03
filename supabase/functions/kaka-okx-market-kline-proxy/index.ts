// @ts-nocheck
// Kaka Web3 · Step276.121
// OKX 公开现货 K线写入缓存 + 日志状态基础版
//
// Function name: kaka-okx-market-kline-proxy
//
// 作用：
// 1. 请求 OKX 公开现货 K线：/api/v5/market/candles
// 2. 归一化 OHLCV 字段
// 3. dry_run=true：只预览，不写数据库、不写日志、不更新状态
// 4. dry_run=false：通过现有 service_role RPC 写入：
//    - app_market_klines_cache
//    - app_market_provider_logs
//    - app_data_source_status
//
// 安全规则：
// - 不需要 OKX API key
// - 不接账户/订单/充值/提现/下单接口
// - 不把任何 secret 放进 Flutter
// - 写库只在 Supabase Edge Function 后端通过 service_role RPC 执行

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.4";

type JsonMap = Record<string, unknown>;

type NormalizedKline = {
  provider: string;
  market_type: string;
  symbol: string;
  inst_id: string;
  base_asset: string;
  quote_asset: string;
  kline_interval: string;
  okx_bar: string;
  open_time: string | null;
  close_time: string | null;
  open_price: number | null;
  high_price: number | null;
  low_price: number | null;
  close_price: number | null;
  volume: number | null;
  quote_volume: number | null;
  trade_count: number | null;
  confirm: string;
  raw_row: unknown[];
};

const FUNCTION_VERSION = "651.1J-finality";

const DEFAULT_PROVIDER = "okx";
const DEFAULT_MARKET_TYPE = "spot";
const DEFAULT_SYMBOL = "BTC-USDT";
const DEFAULT_INTERVAL = "1m";
const DEFAULT_LIMIT = 80;
const MAX_LIMIT = 300;

const allowedIntervals = new Set([
  "1m", "3m", "5m", "15m", "30m",
  "1H", "2H", "4H", "6H", "12H",
  "1D", "3D", "1W", "1M",
]);

const intervalAliases: Record<string, string> = {
  "1MIN": "1m",
  "3MIN": "3m",
  "5MIN": "5m",
  "15MIN": "15m",
  "30MIN": "30m",
  "1H": "1H",
  "2H": "2H",
  "4H": "4H",
  "6H": "6H",
  "12H": "12H",
  "24H": "1D",
  "1D": "1D",
  "3D": "3D",
  "1W": "1W",
  "WEEK": "1W",
  "1M": "1M",
  "MONTH": "1M",
};

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-kaka-sync-secret",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
};

function jsonResponse(body: Record<string, unknown>, status = 200) {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json; charset=utf-8",
    },
  });
}

function textValue(value: unknown, fallback = ""): string {
  if (value === null || value === undefined) return fallback;
  if (typeof value === "string") {
    const text = value.trim();
    return text === "" ? fallback : text;
  }
  if (typeof value === "number" || typeof value === "boolean") return String(value).trim();
  return fallback;
}

function formatError(error: unknown): JsonMap {
  if (error instanceof Error) {
    return {
      message: error.message,
      name: error.name,
      stack: error.stack?.slice(0, 1200) ?? "",
    };
  }

  if (error && typeof error === "object") {
    const map = error as JsonMap;
    return {
      message:
        textValue(map["message"]) ||
        textValue(map["error_description"]) ||
        textValue(map["error"]) ||
        JSON.stringify(map).slice(0, 1200),
      code: textValue(map["code"]),
      details: textValue(map["details"]),
      hint: textValue(map["hint"]),
      raw: map,
    };
  }

  return { message: String(error) };
}

function numberValue(value: unknown): number | null {
  const raw = textValue(value);
  if (!raw) return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

function boolValue(value: unknown, fallback = true): boolean {
  if (typeof value === "boolean") return value;
  const raw = textValue(value).toLowerCase();
  if (!raw) return fallback;
  if (["true", "1", "yes", "y"].includes(raw)) return true;
  if (["false", "0", "no", "n"].includes(raw)) return false;
  return fallback;
}

function intValue(value: unknown, fallback: number, min: number, max: number): number {
  const n = Math.trunc(Number(value ?? fallback));
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, n));
}

function normalizeOkxInstId(raw: unknown): string {
  let value = textValue(raw, DEFAULT_SYMBOL).toUpperCase();
  value = value.replace(/\s+/g, "");
  value = value.replace(/\//g, "-");
  value = value.replace(/_/g, "-");

  if (value.includes("-")) return value;

  const quotes = ["USDT", "USDC", "USD", "BTC", "ETH"];
  for (const quote of quotes) {
    if (value.endsWith(quote) && value.length > quote.length) {
      return `${value.slice(0, value.length - quote.length)}-${quote}`;
    }
  }

  return value || DEFAULT_SYMBOL;
}

function compactOkxInstId(instId: string): string {
  return instId.replace(/-/g, "");
}

function splitOkxInstId(instId: string): { base_asset: string; quote_asset: string } {
  const [base_asset = "", quote_asset = ""] = instId.split("-");
  return { base_asset, quote_asset };
}

function normalizeInterval(raw: unknown): string {
  const value = textValue(raw, DEFAULT_INTERVAL);
  if (allowedIntervals.has(value)) return value;

  const upper = value.toUpperCase();
  if (intervalAliases[upper]) return intervalAliases[upper];

  const lower = value.toLowerCase();
  if (lower === "1d") return "1D";
  if (lower === "3d") return "3D";
  if (lower === "1w") return "1W";
  if (lower === "1m") return "1m";
  if (lower === "3m") return "3m";
  if (lower === "5m") return "5m";
  if (lower === "15m") return "15m";
  if (lower === "30m") return "30m";
  if (lower === "1h") return "1H";
  if (lower === "2h") return "2H";
  if (lower === "4h") return "4H";
  if (lower === "6h") return "6H";
  if (lower === "12h") return "12H";

  return value;
}

function appIntervalFromOkxBar(bar: string): string {
  switch (bar) {
    case "1H": return "1h";
    case "2H": return "2h";
    case "4H": return "4h";
    case "6H": return "6h";
    case "12H": return "12h";
    case "1D": return "1d";
    case "3D": return "3d";
    case "1W": return "1w";
    case "1M": return "1M";
    default: return bar;
  }
}

function barToMilliseconds(bar: string): number {
  switch (bar) {
    case "1m": return 60_000;
    case "3m": return 3 * 60_000;
    case "5m": return 5 * 60_000;
    case "15m": return 15 * 60_000;
    case "30m": return 30 * 60_000;
    case "1H": return 60 * 60_000;
    case "2H": return 2 * 60 * 60_000;
    case "4H": return 4 * 60 * 60_000;
    case "6H": return 6 * 60 * 60_000;
    case "12H": return 12 * 60 * 60_000;
    case "1D": return 24 * 60 * 60_000;
    case "3D": return 3 * 24 * 60 * 60_000;
    case "1W": return 7 * 24 * 60 * 60_000;
    case "1M": return 30 * 24 * 60 * 60_000;
    default: return 60_000;
  }
}

function msToIso(value: unknown): string | null {
  const n = numberValue(value);
  if (n === null) return null;
  const date = new Date(n);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString();
}

async function readRequestPayload(req: Request): Promise<Record<string, unknown>> {
  const url = new URL(req.url);
  const queryPayload: Record<string, unknown> = {};

  for (const [key, value] of url.searchParams.entries()) {
    queryPayload[key] = value;
  }

  if (req.method === "GET") return queryPayload;

  const raw = await req.text();
  if (!raw.trim()) return queryPayload;

  try {
    const body = JSON.parse(raw);
    if (body && typeof body === "object" && !Array.isArray(body)) {
      return { ...queryPayload, ...body };
    }
  } catch (_e) {
    // ignore bad json and use query only
  }

  return queryPayload;
}

function checkSecret(req: Request): { ok: boolean; configured: boolean } {
  const expected = (Deno.env.get("KAKA_SYNC_SECRET") ?? "").trim();
  if (!expected) return { ok: true, configured: false };

  const headerSecret = req.headers.get("x-kaka-sync-secret")?.trim() ?? "";
  const auth = req.headers.get("authorization")?.trim() ?? "";
  const bearer = auth.toLowerCase().startsWith("bearer ") ? auth.slice(7).trim() : "";

  return { ok: headerSecret === expected || bearer === expected, configured: true };
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

function normalizeCandle(row: unknown[], instId: string, bar: string): NormalizedKline | null {
  if (!Array.isArray(row) || row.length < 8) return null;

  const ts = numberValue(row[0]);
  if (ts === null) return null;

  const openTime = msToIso(ts);
  const closeTime = msToIso(ts + barToMilliseconds(bar) - 1);
  const { base_asset, quote_asset } = splitOkxInstId(instId);
  const symbol = compactOkxInstId(instId);

  return {
    provider: DEFAULT_PROVIDER,
    market_type: DEFAULT_MARKET_TYPE,
    symbol,
    inst_id: instId,
    base_asset,
    quote_asset,
    kline_interval: appIntervalFromOkxBar(bar),
    okx_bar: bar,
    open_time: openTime,
    close_time: closeTime,
    open_price: numberValue(row[1]),
    high_price: numberValue(row[2]),
    low_price: numberValue(row[3]),
    close_price: numberValue(row[4]),
    volume: numberValue(row[5]),
    quote_volume: numberValue(row[7]) ?? numberValue(row[6]),
    trade_count: null,
    confirm: textValue(row[8]),
    raw_row: row,
  };
}

function previewKline(item: NormalizedKline): JsonMap {
  return {
    provider: item.provider,
    market_type: item.market_type,
    symbol: item.symbol,
    inst_id: item.inst_id,
    kline_interval: item.kline_interval,
    okx_bar: item.okx_bar,
    open_time: item.open_time,
    close_time: item.close_time,
    open_price: item.open_price,
    high_price: item.high_price,
    low_price: item.low_price,
    close_price: item.close_price,
    volume: item.volume,
    quote_volume: item.quote_volume,
    trade_count: item.trade_count,
    confirm: item.confirm,
  };
}

type BatchKlineWriteResult = {
  written: number;
  failed: number;
  items: JsonMap[];
  errors: JsonMap[];
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
        source: "okx_spot_candles",
        inst_id: kline.inst_id,
        okx_bar: kline.okx_bar,
        confirm: kline.confirm,
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

  const result = data as JsonMap;
  return {
    written: batchInt(result["written"]),
    failed: batchInt(result["failed"]),
    items: Array.isArray(result["items"])
      ? result["items"].filter((item): item is JsonMap =>
        item != null && typeof item === "object" && !Array.isArray(item))
      : [],
    errors: Array.isArray(result["errors"])
      ? result["errors"].filter((item): item is JsonMap =>
        item != null && typeof item === "object" && !Array.isArray(item))
      : [],
  };
}

async function rpcLogProvider(
  supabase: ReturnType<typeof createClient>,
  args: {
    status: string;
    latencyMs: number;
    httpStatus: number | null;
    errorMessage: string | null;
    rawPayload: JsonMap;
  },
): Promise<JsonMap | null> {
  try {
    const { error } = await supabase.rpc("app_edge_log_market_provider", {
      p_provider: DEFAULT_PROVIDER,
      p_task_name: "market_kline_sync",
      p_status: args.status,
      p_latency_ms: args.latencyMs,
      p_is_cache_hit: false,
      p_http_status: args.httpStatus,
      p_error_message: args.errorMessage,
      p_raw_payload: args.rawPayload,
    });

    return error ? formatError(error) : null;
  } catch (error) {
    return formatError(error);
  }
}

async function rpcUpdateSourceStatus(
  supabase: ReturnType<typeof createClient>,
  args: { status: string; errorMessage: string | null },
): Promise<JsonMap | null> {
  try {
    const { error } = await supabase.rpc("app_edge_update_data_source_status", {
      p_source_key: "okx_spot_kline",
      p_last_status: args.status,
      p_last_error_message: args.errorMessage,
      p_mark_success: args.status === "success",
    });

    return error ? formatError(error) : null;
  } catch (error) {
    return formatError(error);
  }
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  if (req.method !== "POST" && req.method !== "GET") {
    return jsonResponse({ ok: false, error: "Only POST or GET is allowed" }, 405);
  }

  const startedAt = Date.now();
  const sourceKey = "okx_spot_kline";
  const secret = checkSecret(req);

  if (!secret.ok) {
    return jsonResponse({
      ok: false,
      provider: DEFAULT_PROVIDER,
      function_version: FUNCTION_VERSION,
      error: "unauthorized",
    }, 401);
  }

  const payload = await readRequestPayload(req);
  const provider = textValue(payload.provider || DEFAULT_PROVIDER).toLowerCase();
  const dryRun = boolValue(payload.dry_run, true);
  const instId = normalizeOkxInstId(payload.symbol || payload.instId || DEFAULT_SYMBOL);
  const bar = normalizeInterval(payload.interval || payload.bar || DEFAULT_INTERVAL);
  const interval = appIntervalFromOkxBar(bar);
  const symbol = compactOkxInstId(instId);
  const limit = intValue(payload.limit, DEFAULT_LIMIT, 1, MAX_LIMIT);

  if (provider !== DEFAULT_PROVIDER) {
    return jsonResponse({
      ok: false,
      provider,
      function_version: FUNCTION_VERSION,
      error: "unsupported_provider",
      supported_provider: DEFAULT_PROVIDER,
    }, 400);
  }

  if (!allowedIntervals.has(bar)) {
    return jsonResponse({
      ok: false,
      provider: DEFAULT_PROVIDER,
      function_version: FUNCTION_VERSION,
      error: "unsupported_interval",
      requested_interval: payload.interval || payload.bar || "",
      normalized_bar: bar,
      supported_intervals: Array.from(allowedIntervals),
    }, 400);
  }

  const baseUrl = (Deno.env.get("OKX_PUBLIC_API_BASE") ?? "https://www.okx.com").trim().replace(/\/+$/, "");
  const requestUrl = `${baseUrl}/api/v5/market/candles?instId=${encodeURIComponent(instId)}&bar=${encodeURIComponent(bar)}&limit=${limit}`;

  let httpStatus: number | null = null;
  let providerOk = false;
  let providerError = "";
  let rawRows: unknown[] = [];
  const normalized: NormalizedKline[] = [];
  const itemErrors: JsonMap[] = [];

  try {
    const resp = await fetch(requestUrl, {
      method: "GET",
      headers: {
        "Accept": "application/json",
        "User-Agent": `KakaWeb3/Step${FUNCTION_VERSION}`,
      },
    });

    httpStatus = resp.status;
    const rawText = await resp.text();

    let rawJson: any = null;
    try {
      rawJson = JSON.parse(rawText);
    } catch (_e) {
      rawJson = null;
    }

    if (!resp.ok || !rawJson || rawJson.code !== "0") {
      providerError = `OKX kline request failed: HTTP ${resp.status}; code=${rawJson?.code ?? ""}; msg=${rawJson?.msg ?? rawText.slice(0, 300)}`;
    } else {
      providerOk = true;
      rawRows = Array.isArray(rawJson.data) ? rawJson.data : [];

      for (const row of rawRows) {
        try {
          if (!Array.isArray(row)) {
            itemErrors.push({ error: "row_is_not_array", raw: row });
            continue;
          }
          const kline = normalizeCandle(row, instId, bar);
          if (kline) {
            normalized.push(kline);
          } else {
            itemErrors.push({ error: "normalize_returned_null", raw: row });
          }
        } catch (error) {
          itemErrors.push({
            error: formatError(error),
            raw: Array.isArray(row) ? row.slice(0, 9) : row,
          });
        }
      }

      normalized.sort((a, b) => {
        const at = Date.parse(a.open_time || "");
        const bt = Date.parse(b.open_time || "");
        return at - bt;
      });
    }
  } catch (error) {
    providerError = error instanceof Error ? error.message : String(error);
  }

  const finalized = normalized.filter((row) => row.confirm === "1");
  const skippedUnfinalized = Math.max(0, normalized.length - finalized.length);
  const durationMs = Date.now() - startedAt;

  if (dryRun) {
    return jsonResponse({
      ok: providerOk,
      mode: "dry_run",
      write_enabled: false,
      side_effects: "disabled",
      provider: DEFAULT_PROVIDER,
      function_version: FUNCTION_VERSION,
      source_key: sourceKey,
      market_type: DEFAULT_MARKET_TYPE,
      request_url: requestUrl,
      http_status: httpStatus,
      duration_ms: durationMs,
      symbol,
      inst_id: instId,
      requested_interval: payload.interval || payload.bar || DEFAULT_INTERVAL,
      okx_bar: bar,
      kline_interval: interval,
      limit,
      fetched: rawRows.length,
      normalized: normalized.length,
      finalized_for_persistence: finalized.length,
      skipped_unfinalized: skippedUnfinalized,
      skipped: itemErrors.length,
      written: 0,
      failed_writes: 0,
      provider_error: providerError,
      side_effect_errors: [],
      item_errors: itemErrors.slice(0, 10),
      preview: normalized.slice(-10).map(previewKline),
      note: "Step276.121 dry_run=true 只验证 OKX 公开现货K线请求和字段归一化，不写数据库。",
      next_step_hint: "dry-run 正常后，把 dry_run 改成 false，验证写入 app_market_klines_cache + provider logs + source status。",
    }, providerOk ? 200 : 502);
  }

  let written = 0;
  let failedWrites = 0;
  const writeErrors: JsonMap[] = [];
  const sideEffectErrors: JsonMap[] = [];
  const writtenIds: JsonMap[] = [];

  let supabase: ReturnType<typeof createClient>;
  try {
    supabase = getServiceClient();
  } catch (error) {
    return jsonResponse({
      ok: false,
      mode: "sync",
      write_enabled: true,
      provider: DEFAULT_PROVIDER,
      function_version: FUNCTION_VERSION,
      source_key: sourceKey,
      error: error instanceof Error ? error.message : String(error),
    }, 500);
  }

  if (providerOk && finalized.length > 0) {
    try {
      const batch = await rpcUpsertKlinesBatch(supabase, finalized);
      written = batch.written;
      failedWrites = batch.failed;
      writtenIds.push(...batch.items);
      writeErrors.push(...batch.errors);
    } catch (error) {
      failedWrites = finalized.length;
      writeErrors.push({
        batch: true,
        normalized: normalized.length,
        error: formatError(error),
      });
    }
  }

  const finalStatus = !providerOk
    ? "error"
    : normalized.length === 0
      ? "error"
      : failedWrites > 0
        ? "partial"
        : "success";

  const finalErrorMessage = finalStatus === "success"
    ? null
    : providerError || writeErrors.slice(0, 3)
      .map((item) => {
        const error = item["error"] as JsonMap | undefined;
        return textValue(error?.["message"]) || JSON.stringify(item).slice(0, 300);
      })
      .filter(Boolean)
      .join(" | ") ||
      "okx_kline_sync_error";

  const logError = await rpcLogProvider(supabase, {
    status: finalStatus,
    latencyMs: durationMs,
    httpStatus,
    errorMessage: finalErrorMessage,
    rawPayload: {
      source_key: sourceKey,
      function_version: FUNCTION_VERSION,
      symbol,
      inst_id: instId,
      okx_bar: bar,
      kline_interval: interval,
      limit,
      request_url: requestUrl,
      fetched: rawRows.length,
      normalized: normalized.length,
      finalized_for_persistence: finalized.length,
      skipped_unfinalized: skippedUnfinalized,
      write_mode: "batch_rpc",
      written,
      failed_writes: failedWrites,
      skipped: itemErrors.length,
      dry_run: false,
      latest_open_time: normalized.length > 0 ? normalized[normalized.length - 1].open_time : null,
      latest_close_time: normalized.length > 0 ? normalized[normalized.length - 1].close_time : null,
    },
  });
  if (logError) sideEffectErrors.push({ action: "log_provider", error: logError });

  const statusError = await rpcUpdateSourceStatus(supabase, {
    status: finalStatus,
    errorMessage: finalErrorMessage,
  });
  if (statusError) sideEffectErrors.push({ action: "update_source_status", error: statusError });

  const ok = providerOk &&
    normalized.length > 0 &&
    written > 0 &&
    failedWrites === 0 &&
    sideEffectErrors.length === 0;

  return jsonResponse({
    ok,
    mode: "sync",
    write_enabled: true,
    side_effects: "enabled",
    provider: DEFAULT_PROVIDER,
    function_version: FUNCTION_VERSION,
    source_key: sourceKey,
    market_type: DEFAULT_MARKET_TYPE,
    request_url: requestUrl,
    http_status: httpStatus,
    duration_ms: durationMs,
    symbol,
    inst_id: instId,
    requested_interval: payload.interval || payload.bar || DEFAULT_INTERVAL,
    okx_bar: bar,
    kline_interval: interval,
    limit,
    fetched: rawRows.length,
    normalized: normalized.length,
    write_mode: "batch_rpc",
    skipped: itemErrors.length,
    written,
    failed_writes: failedWrites,
    provider_error: providerError,
    side_effect_errors: sideEffectErrors,
    item_errors: [...itemErrors, ...writeErrors].slice(0, 10),
    written_ids: writtenIds.slice(-10),
    preview: normalized.slice(-10).map(previewKline),
    write_debug: writeErrors.length > 0 ? {
      note: "写入 app_market_klines_cache 失败。请把 item_errors.error.message/code/details/hint 发给我，不要发 KAKA_SYNC_SECRET。",
      rpc: "app_edge_upsert_market_klines_batch",
      source_key: sourceKey,
      provider: DEFAULT_PROVIDER,
    } : null,
    next_step_hint: "通过后执行 SQL 检查 app_market_klines_cache、app_market_provider_logs、app_data_source_status。",
  }, ok ? 200 : 502);
});
