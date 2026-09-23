import "jsr:@supabase/functions-js/edge-runtime.d.ts";

const VERSION = "step1073_v101_render_egress_ingest_v7";
const ALLOWED_TABLES = new Set([
  "kaka_exchange_stock_catalog_v2_stage",
  "kaka_exchange_stock_catalog_v2_state",
  "kaka_project_fundamentals",
  "app_airdrop_events",
]);
const ALLOWED_RPCS = new Set([
  "app_upsert_market_backend_snapshots_diff",
  "app_upsert_bybit_second_history_chunks",
]);
const MAX_COMPRESSED_BYTES = 4 * 1024 * 1024;
const MAX_DECOMPRESSED_BYTES = 16 * 1024 * 1024;
const VERIFY_RPC = "kaka_verify_render_egress_service_role";

let verifiedKeyHash = "";
let verifiedKeyUntil = 0;

function json(status: number, body: Record<string, unknown>, verified = false) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      "x-kaka-render-egress-proxy": verified ? "ok" : "error",
      "x-kaka-render-egress-proxy-version": VERSION,
    },
  });
}

async function sha256Hex(value: string): Promise<string> {
  const bytes = new TextEncoder().encode(value);
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  return [...hash].map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function verifyCallerServiceRole(req: Request, supabaseUrl: string): Promise<boolean> {
  const callerKey = String(req.headers.get("x-kaka-caller-key") || "").trim();
  if (!callerKey || callerKey.length > 4096) return false;

  const keyHash = await sha256Hex(callerKey);
  if (keyHash === verifiedKeyHash && Date.now() < verifiedKeyUntil) return true;

  try {
    const response = await fetch(`${supabaseUrl}/rest/v1/rpc/${VERIFY_RPC}`, {
      method: "POST",
      headers: {
        apikey: callerKey,
        authorization: `Bearer ${callerKey}`,
        "content-type": "application/json",
      },
      body: "{}",
    });
    if (!response.ok) return false;
    const body = await response.json().catch(() => null);
    const ok = body === true || (Array.isArray(body) && body[0] === true);
    if (ok) {
      verifiedKeyHash = keyHash;
      verifiedKeyUntil = Date.now() + 10 * 60_000;
    }
    return ok;
  } catch {
    return false;
  }
}

async function gunzip(bytes: Uint8Array): Promise<Uint8Array> {
  const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"));
  const out = new Uint8Array(await new Response(stream).arrayBuffer());
  if (out.byteLength > MAX_DECOMPRESSED_BYTES) throw new Error("decompressed_payload_too_large");
  return out;
}

Deno.serve(async (req: Request) => {
  if (req.method !== "POST") return json(405, { ok: false, error: "method_not_allowed", version: VERSION });

  const supabaseUrl = String(Deno.env.get("SUPABASE_URL") || "").replace(/\/+$/, "");
  const serviceRole = String(Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "").trim();
  if (!supabaseUrl || !serviceRole) return json(503, { ok: false, error: "supabase_server_config_missing", version: VERSION });

  // The gateway still verifies Authorization (verify_jwt=true). Separately,
  // validate the caller's server-only key against an RPC executable only by
  // Postgres service_role. This works with either legacy JWT keys or newer
  // secret API keys and does not grant ordinary authenticated users access.
  if (!(await verifyCallerServiceRole(req, supabaseUrl))) {
    return json(403, { ok: false, error: "service_role_required", version: VERSION });
  }

  if (String(req.headers.get("x-kaka-compression") || "").toLowerCase() !== "gzip") {
    return json(400, { ok: false, error: "gzip_required", version: VERSION });
  }

  const compressed = new Uint8Array(await req.arrayBuffer());
  if (!compressed.byteLength || compressed.byteLength > MAX_COMPRESSED_BYTES) {
    return json(413, { ok: false, error: "compressed_payload_size_invalid", compressed_bytes: compressed.byteLength, version: VERSION });
  }

  let plain: Uint8Array;
  try { plain = await gunzip(compressed); }
  catch (error) { return json(400, { ok: false, error: String(error?.message || error), version: VERSION }); }

  const claimedRaw = Number(req.headers.get("x-kaka-raw-bytes") || 0);
  if (Number.isFinite(claimedRaw) && claimedRaw > 0 && claimedRaw !== plain.byteLength) {
    return json(400, { ok: false, error: "raw_size_mismatch", claimed_raw_bytes: claimedRaw, actual_raw_bytes: plain.byteLength, version: VERSION });
  }

  if (String(req.headers.get("x-kaka-probe") || "") === "1") {
    return json(200, {
      ok: true,
      probe: true,
      compressed_bytes: compressed.byteLength,
      raw_bytes: plain.byteLength,
      service_role_verified: true,
      version: VERSION,
    }, true);
  }

  const encodedTarget = String(req.headers.get("x-kaka-target") || "").trim();
  let target = "";
  try { target = decodeURIComponent(encodedTarget); } catch { return json(400, { ok: false, error: "invalid_target_encoding", version: VERSION }); }
  if (!target.startsWith("/rest/v1/")) return json(400, { ok: false, error: "invalid_target_path", version: VERSION });

  const parsed = new URL(target, "http://kaka.local");
  const targetName = parsed.pathname.slice("/rest/v1/".length);
  const rpc = targetName.startsWith("rpc/") ? targetName.slice("rpc/".length) : "";
  const table = rpc ? "" : targetName;
  const allowedTable = Boolean(table && ALLOWED_TABLES.has(table) && !table.includes("/"));
  const allowedRpc = Boolean(rpc && ALLOWED_RPCS.has(rpc) && !rpc.includes("/"));
  if (!allowedTable && !allowedRpc) {
    return json(403, { ok: false, error: "target_not_allowed", target: targetName, version: VERSION });
  }

  const originalMethod = String(req.headers.get("x-kaka-original-method") || "POST").toUpperCase();
  if (originalMethod !== "POST") return json(405, { ok: false, error: "original_method_not_allowed", version: VERSION });

  const forwardHeaders: Record<string, string> = {
    apikey: serviceRole,
    authorization: `Bearer ${serviceRole}`,
    "content-type": String(req.headers.get("x-kaka-original-content-type") || "application/json"),
  };
  const prefer = String(req.headers.get("x-kaka-original-prefer") || "").trim();
  const contentProfile = String(req.headers.get("x-kaka-original-content-profile") || "").trim();
  const acceptProfile = String(req.headers.get("x-kaka-original-accept-profile") || "").trim();
  if (prefer) forwardHeaders.prefer = prefer;
  if (contentProfile) forwardHeaders["content-profile"] = contentProfile;
  if (acceptProfile) forwardHeaders["accept-profile"] = acceptProfile;

  try {
    const upstream = await fetch(`${supabaseUrl}${parsed.pathname}${parsed.search}`, {
      method: originalMethod,
      headers: forwardHeaders,
      body: plain,
    });
    const body = await upstream.arrayBuffer();
    const headers = new Headers();
    headers.set("content-type", upstream.headers.get("content-type") || "text/plain; charset=utf-8");
    headers.set("cache-control", "no-store");
    headers.set("x-kaka-render-egress-proxy", "ok");
    headers.set("x-kaka-render-egress-proxy-version", VERSION);
    headers.set("x-kaka-proxy-compressed-bytes", String(compressed.byteLength));
    headers.set("x-kaka-proxy-raw-bytes", String(plain.byteLength));
    return new Response(body, { status: upstream.status, headers });
  } catch (error) {
    return json(502, { ok: false, error: "upstream_fetch_failed", detail: String(error?.message || error).slice(0, 240), version: VERSION });
  }
});
