import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2.57.4";

const SCHEMA = "step1073_v101_ops_alerts_compact_health_v2";
const SUPABASE_URL = (Deno.env.get("SUPABASE_URL") ?? "").trim();
const SERVICE_ROLE_KEY = (Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "").trim();
const SYNC_SECRET = (Deno.env.get("KAKA_SYNC_SECRET") ?? "").trim();
const TELEGRAM_BOT_TOKEN = (Deno.env.get("KAKA_TELEGRAM_BOT_TOKEN") ?? "").trim();
const TELEGRAM_CHAT_ID = (Deno.env.get("KAKA_TELEGRAM_CHAT_ID") ?? "").trim();
const WECOM_BOT_WEBHOOK = (Deno.env.get("KAKA_WECOM_BOT_WEBHOOK") ?? "").trim();
const WORKER_HEALTH_OVERRIDE = (Deno.env.get("KAKA_RENDER_WORKER_HEALTH_URL") ?? "").trim();
const ALERT_PREFIX = (Deno.env.get("KAKA_OPS_ALERT_PREFIX") ?? "Kaka Web3").trim();
const DEFAULT_WORKER_HEALTH = "https://kaka-contract-realtime-worker.onrender.com/health/ready";
const NETWORK_TIMEOUT_MS = 8000;
const CLAIM_LIMIT = 10;
const PROCESS_CONCURRENCY = 4;

const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

function jsonResponse(status: number, body: unknown) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function sameSecret(left: string, right: string) {
  const a = new TextEncoder().encode(left);
  const b = new TextEncoder().encode(right);
  let diff = a.length ^ b.length;
  const length = Math.max(a.length, b.length);
  for (let i = 0; i < length; i += 1) {
    diff |= (a[i] ?? 0) ^ (b[i] ?? 0);
  }
  return diff === 0;
}

function authorized(req: Request) {
  const provided = (req.headers.get("x-kaka-sync-secret") ?? "").trim();
  return Boolean(SYNC_SECRET && provided && sameSecret(SYNC_SECRET, provided));
}

async function boundedFetch(
  input: RequestInfo | URL,
  init: RequestInit = {},
  timeoutMs = NETWORK_TIMEOUT_MS,
) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.max(1000, Math.min(timeoutMs, 8000)));
  try {
    return await fetch(input, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

function errorText(error: unknown) {
  if (error instanceof DOMException && error.name === "AbortError") return "timeout";
  if (error instanceof Error) return error.message.slice(0, 500);
  return String(error).slice(0, 500);
}

function asMap(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function compactWorkerHealthUrl(value: string) {
  const raw = String(value || "").trim();
  try {
    const url = new URL(raw);
    if (url.pathname.replace(/\/+$/, "") === "/health") {
      url.pathname = "/health/ready";
      url.search = "";
      url.hash = "";
    }
    return url.toString();
  } catch {
    return raw;
  }
}

async function loadConfig() {
  const { data, error } = await supabase.rpc("app_edge_get_ops_alert_config");
  if (error) throw new Error(`config_rpc:${error.message}`);
  const config = asMap(data);
  return {
    enabled: config.enabled !== false,
    workerHealthUrl: compactWorkerHealthUrl(WORKER_HEALTH_OVERRIDE ||
      String(config.worker_health_url ?? DEFAULT_WORKER_HEALTH).trim() || DEFAULT_WORKER_HEALTH),
    workerTimeoutMs: Math.max(1000, Math.min(Number(config.worker_timeout_ms ?? NETWORK_TIMEOUT_MS), 8000)),
  };
}

async function checkWorker(url: string, timeoutMs: number) {
  const started = Date.now();
  try {
    const response = await boundedFetch(url, {
      method: "GET",
      headers: { accept: "application/json", "user-agent": "kaka-ops-monitor/1073.101" },
    }, timeoutMs);
    const body = (await response.text()).slice(0, 600);
    let applicationOk = true;
    try {
      const parsed = body ? JSON.parse(body) : {};
      if (parsed && typeof parsed === "object" && parsed.ok === false) applicationOk = false;
    } catch {
      // A 2xx non-JSON health response is still considered reachable.
    }
    return {
      ok: response.ok && applicationOk,
      httpStatus: response.status,
      latencyMs: Date.now() - started,
      error: response.ok && applicationOk ? "" : `health_response:${body}`.slice(0, 500),
    };
  } catch (error) {
    return {
      ok: false,
      httpStatus: 0,
      latencyMs: Date.now() - started,
      error: errorText(error),
    };
  }
}

function severityLabel(value: string) {
  if (value === "critical") return "紧急";
  if (value === "warning") return "警告";
  return "提醒";
}

function alertText(item: Record<string, unknown>) {
  const severity = severityLabel(String(item.severity ?? "warning"));
  const title = String(item.title ?? "运维提醒").trim();
  const body = String(item.body ?? "").trim();
  const category = String(item.category ?? "operations").trim();
  const createdAt = new Date().toISOString();
  return [
    `【${ALERT_PREFIX} · ${severity}】`,
    title,
    body,
    `分类：${category}`,
    `时间：${createdAt}`,
  ].filter(Boolean).join("\n").slice(0, 3500);
}

async function sendTelegram(text: string) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    return { configured: false, ok: false, status: 0, error: "not_configured" };
  }
  try {
    const response = await boundedFetch(
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
      {
        method: "POST",
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({
          chat_id: TELEGRAM_CHAT_ID,
          text,
          disable_web_page_preview: true,
        }),
      },
    );
    // Step1065.6.2: parse the COMPLETE Telegram JSON response first.
    // The old code sliced the body to 500 chars before JSON.parse; successful
    // sendMessage responses can exceed 500 chars, so valid HTTP 200 / ok=true
    // responses were truncated into invalid JSON and incorrectly marked retry.
    const rawBody = await response.text();
    let providerOk = response.ok;
    try {
      const parsed = rawBody ? JSON.parse(rawBody) : {};
      providerOk = response.ok && parsed.ok === true;
    } catch {
      providerOk = false;
    }
    return {
      configured: true,
      ok: providerOk,
      status: response.status,
      error: providerOk
        ? ""
        : `telegram_${response.status}:${rawBody.slice(0, 500)}`.slice(0, 500),
    };
  } catch (error) {
    return { configured: true, ok: false, status: 0, error: `telegram:${errorText(error)}` };
  }
}

async function sendWeCom(text: string) {
  if (!WECOM_BOT_WEBHOOK) {
    return { configured: false, ok: false, status: 0, error: "not_configured" };
  }
  try {
    const response = await boundedFetch(WECOM_BOT_WEBHOOK, {
      method: "POST",
      headers: { "content-type": "application/json; charset=utf-8" },
      body: JSON.stringify({ msgtype: "text", text: { content: text } }),
    });
    // Keep the same full-body-before-parse rule for WeCom.
    const rawBody = await response.text();
    let providerOk = response.ok;
    try {
      const parsed = rawBody ? JSON.parse(rawBody) : {};
      providerOk = response.ok && Number(parsed.errcode ?? -1) === 0;
    } catch {
      providerOk = false;
    }
    return {
      configured: true,
      ok: providerOk,
      status: response.status,
      error: providerOk
        ? ""
        : `wecom_${response.status}:${rawBody.slice(0, 500)}`.slice(0, 500),
    };
  } catch (error) {
    return { configured: true, ok: false, status: 0, error: `wecom:${errorText(error)}` };
  }
}

async function processItem(item: Record<string, unknown>) {
  const id = String(item.id ?? "").trim();
  if (!id) return { id, status: "invalid" };
  const text = alertText(item);
  const [telegram, wecom] = await Promise.all([sendTelegram(text), sendWeCom(text)]);
  const results = { telegram, wecom };
  const delivered = telegram.ok || wecom.ok;

  if (delivered) {
    const { error } = await supabase.rpc("app_edge_mark_ops_alert_sent", {
      p_queue_id: id,
      p_channel_results: results,
    });
    if (error) throw new Error(`mark_sent:${error.message}`);
    return { id, status: "sent", telegram: telegram.ok, wecom: wecom.ok };
  }

  const errors = [telegram.error, wecom.error].filter(Boolean).join(" | ").slice(0, 900);
  const { error } = await supabase.rpc("app_edge_mark_ops_alert_failed", {
    p_queue_id: id,
    p_error: errors || "all_channels_failed",
    p_retry_after_seconds: 300,
  });
  if (error) throw new Error(`mark_failed:${error.message}`);
  return { id, status: "retry", telegram: false, wecom: false, error: errors };
}

async function processPool(rows: Record<string, unknown>[]) {
  const results: Record<string, unknown>[] = new Array(rows.length);
  let next = 0;
  const workers = Array.from(
    { length: Math.min(PROCESS_CONCURRENCY, rows.length) },
    async () => {
      while (true) {
        const index = next++;
        if (index >= rows.length) return;
        try {
          results[index] = await processItem(rows[index]);
        } catch (error) {
          const id = String(rows[index]?.id ?? "");
          if (id) {
            await supabase.rpc("app_edge_mark_ops_alert_failed", {
              p_queue_id: id,
              p_error: `process_exception:${errorText(error)}`,
              p_retry_after_seconds: 300,
            });
          }
          results[index] = { id, status: "retry", error: errorText(error) };
        }
      }
    },
  );
  await Promise.all(workers);
  return results;
}

Deno.serve(async (req: Request) => {
  try {
    if (req.method !== "GET" && req.method !== "POST") {
      return jsonResponse(405, { ok: false, schema: SCHEMA, error: "method_not_allowed" });
    }
    if (!authorized(req)) {
      return jsonResponse(401, { ok: false, schema: SCHEMA, error: "unauthorized" });
    }
    if (!SUPABASE_URL || !SERVICE_ROLE_KEY) {
      return jsonResponse(500, { ok: false, schema: SCHEMA, error: "supabase_runtime_secret_missing" });
    }

    const config = await loadConfig();
    const telegramConfigured = Boolean(TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID);
    const wecomConfigured = Boolean(WECOM_BOT_WEBHOOK);
    const worker = await checkWorker(config.workerHealthUrl, config.workerTimeoutMs);

    const { data: collection, error: collectionError } = await supabase.rpc(
      "app_edge_collect_ops_alerts",
      {
        p_worker_ok: worker.ok,
        p_worker_http_status: worker.httpStatus,
        p_worker_latency_ms: worker.latencyMs,
        p_worker_error: worker.error,
        p_telegram_configured: telegramConfigured,
        p_wecom_configured: wecomConfigured,
      },
    );
    if (collectionError) {
      return jsonResponse(500, {
        ok: false,
        schema: SCHEMA,
        error: `collect_failed:${collectionError.message}`,
        worker,
      });
    }

    const requestUrl = new URL(req.url);
    if (requestUrl.searchParams.get("health") === "1") {
      return jsonResponse(200, {
        ok: true,
        schema: SCHEMA,
        config_enabled: config.enabled,
        telegram_configured: telegramConfigured,
        wecom_configured: wecomConfigured,
        worker,
        collection,
        claim_limit: CLAIM_LIMIT,
        concurrency: PROCESS_CONCURRENCY,
        network_timeout_ms: NETWORK_TIMEOUT_MS,
        user_reads_start_requests: false,
      });
    }

    const { data: claimed, error: claimError } = await supabase.rpc(
      "app_edge_claim_ops_alerts",
      { p_limit: CLAIM_LIMIT, p_lease_seconds: 90 },
    );
    if (claimError) {
      return jsonResponse(500, {
        ok: false,
        schema: SCHEMA,
        error: `claim_failed:${claimError.message}`,
        worker,
        collection,
      });
    }
    const rows = Array.isArray(claimed) ? claimed.map(asMap) : [];
    const results = await processPool(rows);

    return jsonResponse(200, {
      ok: true,
      schema: SCHEMA,
      worker,
      collection,
      telegram_configured: telegramConfigured,
      wecom_configured: wecomConfigured,
      claimed: rows.length,
      sent: results.filter((item) => item.status === "sent").length,
      retry: results.filter((item) => item.status === "retry").length,
      results,
      user_reads_start_requests: false,
    });
  } catch (error) {
    return jsonResponse(500, { ok: false, schema: SCHEMA, error: errorText(error) });
  }
});
