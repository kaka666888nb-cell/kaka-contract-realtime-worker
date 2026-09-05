import http from 'node:http';

const STEP = '1062.1.3';

function toUtf8Text(chunk, encoding) {
  if (chunk == null) return null;
  if (Buffer.isBuffer(chunk)) return chunk.toString('utf8');
  if (typeof chunk === 'string') return chunk;
  if (chunk instanceof Uint8Array) return Buffer.from(chunk).toString('utf8');
  try { return String(chunk); } catch (_) { return null; }
}

function redactRtcHealthBody(body) {
  if (!body || !body.includes('"configured"')) return body;
  let parsed;
  try { parsed = JSON.parse(body); } catch (_) { return body; }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return body;

  // Step1062.1.3: health must expose booleans only. Never allow any secret/string
  // returned by a truthy && chain (for example a Supabase secret key) to leave Render.
  parsed.configured = Boolean(
    parsed.enabled === true &&
    parsed.sdk_app_id_configured === true &&
    parsed.secret_key_configured === true &&
    parsed.supabase_server_configured === true &&
    parsed.db_schema_ready === true
  );

  // Defense in depth for this public health route: remove any accidental secret-like
  // diagnostic fields without changing the intended booleans/limits/stats contract.
  for (const key of Object.keys(parsed)) {
    const lower = key.toLowerCase();
    if ((lower.includes('secret') || lower.includes('service_role') || lower.includes('api_key')) &&
        !lower.endsWith('_configured')) {
      delete parsed[key];
    }
  }

  return JSON.stringify(parsed);
}

export function installRtcHealthRedactionGuard() {
  if (http.__kakaStep1062RtcHealthRedactionInstalled) return;

  const originalEnd = http.ServerResponse.prototype.end;
  http.ServerResponse.prototype.end = function kakaRtcHealthRedactedEnd(chunk, encoding, callback) {
    try {
      const reqUrl = String(this?.req?.url || '');
      const pathname = reqUrl.split('?')[0];
      if (pathname === '/api/rtc/health' && chunk != null) {
        const originalBody = toUtf8Text(chunk, encoding);
        const redacted = redactRtcHealthBody(originalBody);
        if (typeof originalBody === 'string' && typeof redacted === 'string' && redacted !== originalBody) {
          const originalBytes = Buffer.byteLength(originalBody, 'utf8');
          const redactedBytes = Buffer.byteLength(redacted, 'utf8');
          // The RTC json() helper sets Content-Length before end(). If headers are already
          // sent, preserve the exact byte count by padding legal JSON trailing whitespace.
          if (redactedBytes <= originalBytes) {
            chunk = redacted + ' '.repeat(originalBytes - redactedBytes);
          }
        }
      }
    } catch (_) {
      // Fail open for availability, but the normal path above is deterministic JSON.
    }
    return originalEnd.call(this, chunk, encoding, callback);
  };

  http.__kakaStep1062RtcHealthRedactionInstalled = true;
  console.log(`[Step${STEP}] RTC public health secret redaction guard installed`);
}
