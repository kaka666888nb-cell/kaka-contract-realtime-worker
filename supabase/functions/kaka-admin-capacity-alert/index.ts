import { serve } from 'https://deno.land/std@0.224.0/http/server.ts';
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.45.4';

const SCHEMA = 'step1073_v101_admin_capacity_dimensions_v3';
const SUPABASE_URL = (Deno.env.get('SUPABASE_URL') || '').trim();
const SERVICE_ROLE_KEY = (Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') || '').trim();
const SYNC_SECRET = (
  Deno.env.get('KAKA_SYNC_SECRET') ||
  Deno.env.get('KAKA_SYNC_SECRET_VALUE') ||
  ''
).trim();
const RESEND_API_KEY = (Deno.env.get('RESEND_API_KEY') || '').trim();
const EMAIL_FROM = (
  Deno.env.get('PRICE_ALERT_EMAIL_FROM') ||
  Deno.env.get('KAKA_PRICE_ALERT_EMAIL_FROM') ||
  ''
).trim();

function capacityHealthUrl(value: string) {
  const raw = String(value || '').trim();
  try {
    const url = new URL(raw);
    const path = url.pathname.replace(/\/+$/, '');
    if (path === '/health' || path === '/api/realtime-ws-health') {
      url.pathname = '/api/capacity-health';
      url.search = '';
      url.hash = '';
    }
    return url.toString();
  } catch {
    return raw;
  }
}

const RENDER_HEALTH = capacityHealthUrl(
  Deno.env.get('KAKA_RENDER_WORKER_HEALTH_URL') ||
  'https://kaka-contract-realtime-worker.onrender.com/api/capacity-health',
);
const sb = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

function out(status: number, body: unknown) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
    },
  });
}

function txt(value: unknown) {
  return String(value ?? '').trim();
}

function esc(value: unknown) {
  return txt(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;');
}

function wholeNonnegative(value: unknown) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.max(0, Math.round(parsed)) : 0;
}

function normalizeDimensions(payload: any) {
  const runtimeId = txt(payload?.runtime_id).slice(0, 160);
  const source = Array.isArray(payload?.dimensions)
    ? payload.dimensions.slice(0, 20)
    : [];
  const dimensions = source.flatMap((raw: any) => {
    const id = txt(raw?.id).toLowerCase();
    if (!/^[a-z0-9_:-]{1,80}$/.test(id)) return [];
    const used = wholeNonnegative(raw?.used);
    const limit = Math.max(1, wholeNonnegative(raw?.limit));
    return [{
      id,
      label: txt(raw?.label).slice(0, 80) || id,
      used,
      limit,
      percent: Math.round((used * 10000) / limit) / 100,
      rejected: wholeNonnegative(raw?.rejected),
      runtime_id: runtimeId,
    }];
  });
  if (!runtimeId || !dimensions.length) {
    throw new Error('render_capacity_schema_invalid');
  }
  return dimensions;
}

async function fetchHealth() {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 8000);
  try {
    const response = await fetch(RENDER_HEALTH, {
      headers: {
        accept: 'application/json',
        'user-agent': 'kaka-capacity-monitor/1073.101.3',
      },
      signal: controller.signal,
    });
    const payload = await response.json();
    if (!response.ok || payload?.ok !== true) {
      throw new Error(`render_http_${response.status}`);
    }
    const dimensions = normalizeDimensions(payload);
    const highest = dimensions.reduce(
      (best: any, item: any) => !best || item.percent > best.percent ? item : best,
      null,
    );
    return {
      dimensions,
      highest,
      rejected: dimensions.reduce(
        (total: number, item: any) => total + item.rejected,
        0,
      ),
    };
  } finally {
    clearTimeout(timer);
  }
}

async function sendEmail(
  to: string[],
  subject: string,
  body: string,
  idempotencyKey: string,
) {
  if (!to.length) return { ok: false, error: 'no_admin_email', response_received: true };
  if (!RESEND_API_KEY || !EMAIL_FROM) {
    return { ok: false, error: 'resend_not_configured', response_received: true };
  }
  const html = `<div style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Arial,sans-serif;line-height:1.7;color:#111827"><h3>${esc(subject)}</h3><p>${esc(body)}</p><p style="color:#6b7280">仅发送给 Kaka Web3 管理员，不发送给普通用户。</p></div>`;
  try {
    const response = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: {
        authorization: `Bearer ${RESEND_API_KEY}`,
        'content-type': 'application/json',
        'Idempotency-Key': idempotencyKey,
      },
      body: JSON.stringify({ from: EMAIL_FROM, to, subject, text: body, html }),
    });
    const raw = await response.text();
    let providerId = '';
    if (response.ok) {
      try { providerId = String(JSON.parse(raw)?.id || ''); } catch (_) {}
    }
    return {
      ok: response.ok,
      status: response.status,
      error: response.ok ? '' : raw.slice(0, 300),
      provider_id: providerId,
      response_received: true,
    };
  } catch (error) {
    return {
      ok: false,
      status: null,
      error: error instanceof Error ? error.message : String(error),
      provider_id: '',
      response_received: false,
    };
  }
}

serve(async (req) => {
  try {
    if (!['GET', 'POST'].includes(req.method)) {
      return out(405, { ok: false, schema: SCHEMA, error: 'method_not_allowed' });
    }
    const provided = (req.headers.get('x-kaka-sync-secret') || '').trim();
    if (!SYNC_SECRET || provided !== SYNC_SECRET) {
      return out(401, { ok: false, schema: SCHEMA, error: 'unauthorized' });
    }
    if (!SUPABASE_URL || !SERVICE_ROLE_KEY) {
      return out(500, { ok: false, schema: SCHEMA, error: 'missing_supabase_env' });
    }

    let dry = false;
    try {
      if (req.method === 'POST') dry = (await req.json())?.dry_run === true;
    } catch (_) {}

    const health = await fetchHealth();
    const { data, error } = await sb.rpc(
      'kaka_edge_capacity_dimensions_alert_decide',
      { p_dimensions: health.dimensions, p_dry_run: dry },
    );
    if (error) {
      return out(500, { ok: false, schema: SCHEMA, error: `decision:${error.message}` });
    }

    let email: any = { ok: true, skipped: true };
    let budget: any = null;
    if (!dry && data?.notify === true) {
      const emails = Array.isArray(data?.admin_emails)
        ? data.admin_emails.map(txt).filter(Boolean)
        : [];
      const eventId = txt(data?.email_event_id);
      const budgetKey = eventId ? `admin-capacity/${eventId}` : '';
      if (!budgetKey) {
        email = { ok: false, skipped: true, error: 'missing_email_event_id' };
      } else {
        const { data: budgetRows, error: budgetError } = await sb.rpc(
          'app_claim_resend_platform_budget',
          {
            p_idempotency_key: budgetKey,
            p_category: 'admin_capacity',
            p_units: Math.max(1, emails.length),
            p_metadata: {
              level: txt(data?.level),
              dimension: txt(data?.dimension),
              used: Number(data?.used || 0),
              limit: Number(data?.limit || 0),
            },
          },
        );
        budget = Array.isArray(budgetRows) && budgetRows.length
          ? budgetRows[0]
          : null;
        if (budgetError || budget?.allowed !== true) {
          email = {
            ok: false,
            skipped: true,
            error: budgetError?.message || budget?.reason || 'resend_platform_budget_denied',
          };
        } else {
          email = await sendEmail(
            emails,
            txt(data?.title),
            txt(data?.body),
            `kaka-admin-capacity/${eventId}`,
          );
          if (email?.ok === true) {
            await sb.rpc('app_commit_resend_platform_budget', {
              p_idempotency_key: budgetKey,
              p_provider: 'resend',
              p_provider_message_id: txt(email?.provider_id),
            });
          } else if (email?.response_received === true) {
            await sb.rpc('app_release_resend_platform_budget', {
              p_idempotency_key: budgetKey,
              p_error: txt(email?.error),
            });
          }
        }
      }
    }

    const { data: platformBudget } = await sb.rpc(
      'app_get_resend_platform_budget_status',
    );
    return out(200, {
      ok: true,
      schema: SCHEMA,
      dry_run: dry,
      dimensions: health.dimensions,
      highest_utilization: health.highest,
      rejected_capacity: health.rejected,
      decision: data,
      budget_claim: budget,
      platform_budget: platformBudget ?? null,
      email: {
        ok: Boolean(email?.ok),
        skipped: Boolean(email?.skipped),
        error: txt(email?.error),
      },
    });
  } catch (error) {
    return out(500, {
      ok: false,
      schema: SCHEMA,
      error: error instanceof Error ? error.message : String(error),
    });
  }
});
