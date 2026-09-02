import { installRenderEgressCostGuard } from './render-egress-cost-guard.mjs';
import { installRenderSupabaseEgressProxy } from './render-supabase-egress-proxy.mjs';

// Step1060.3: install metering/compression first, then route only the known
// large background Supabase JSON writes through the authenticated gzip ingest.
// If that ingest is unavailable, large writes fail closed and preserve the
// last verified DB data instead of silently falling back to expensive raw egress.
installRenderEgressCostGuard();
installRenderSupabaseEgressProxy();
await import('./step1042-bitget-long-kline-continuity-proxy.mjs');
