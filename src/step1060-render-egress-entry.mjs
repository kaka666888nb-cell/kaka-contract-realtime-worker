import { installRenderEgressCostGuard } from './render-egress-cost-guard.mjs';

// Install cost protection before loading the existing production entrypoint.
// This preserves all existing market/contract/on-chain behavior and only adds
// compact readiness, response compression and process-lifetime egress metering.
installRenderEgressCostGuard();
await import('./step1042-bitget-long-kline-continuity-proxy.mjs');
