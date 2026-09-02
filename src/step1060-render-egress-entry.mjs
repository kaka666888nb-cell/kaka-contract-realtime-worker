import { installRenderEgressCostGuard } from './render-egress-cost-guard.mjs';
import { installRenderSupabaseEgressProxy } from './render-supabase-egress-proxy.mjs';
import { installLiquidation1hNoopEgressGuard } from './liquidation-noop-egress-guard.mjs';

// Step1060.3: install metering/compression first, then route only the known
// large background Supabase JSON writes through the authenticated gzip ingest.
// If that ingest is unavailable, large writes fail closed and preserve the
// last verified DB data instead of silently falling back to expensive raw egress.
installRenderEgressCostGuard();
installRenderSupabaseEgressProxy();
// Step1060.26: filter identical liquidation 1H persistence rows before they
// reach the existing Supabase egress proxy. Real semantic changes still pass.
installLiquidation1hNoopEgressGuard();
// Step1060.31.7: public-device ticker fan-out reads only the already isolated
// market-light shared snapshot. Active exact identities, not client count, bound
// the localhost collector reads. The existing HTTP exact route remains the App
// fallback and no client stream opens an exchange request or exchange connection.
await import('./step1060-market-light-ticker-stream.mjs');
await import('./step1042-bitget-long-kline-continuity-proxy.mjs');
