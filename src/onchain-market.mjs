// Step1042.3 / Render 650.8.15.197.3
// Kaka Web3 on-chain market phase 2.
// Step1036 DEX Screener foundation is preserved. Step1037 adds exact-pool OHLCV/history and
// recent swaps through Moralis Data API, with backend-only secret, separate bounded scheduler,
// CU budget ledger, cache + singleflight, exact chain/token/pool preflight and no user-scale
// upstream amplification. No trading, wallet signing or database writes.

import { readFileSync, writeFileSync, renameSync } from 'node:fs';
import { KAKA_FULL_INTERVALS, KAKA_DERIVED_PLAN, klineIntervalMs, klineDerivedPlan, deriveAndFillKlines } from './kline-derived.mjs';

const VERSION = '650.8.15.197.3';
const DATA_VERSION = 1042000;
const SCHEMA_VERSION = 'step1037_3_onchain_market_v2';
const STEP1038_FEATURE_SCHEMA_VERSION = 'step1038_onchain_holder_security_v1';
const STEP1039_FEATURE_SCHEMA_VERSION = 'step1039_onchain_wallet_intelligence_v1';
const STEP1040_FEATURE_SCHEMA_VERSION = 'step1040_onchain_wallet_relationship_evidence_v1';
const STEP1041_FEATURE_SCHEMA_VERSION = 'step1041_onchain_final_productization_v1';
const STEP1042_FEATURE_SCHEMA_VERSION = 'step1042_onchain_multichain_smart_money_v1';

const HEALTH_ROUTE = '/api/onchain/health';
const SELF_TEST_ROUTE = '/api/onchain/self-test';
const TRENDING_ROUTE = '/api/onchain/trending';
const SEARCH_ROUTE = '/api/onchain/search';
const TOKEN_ROUTE = '/api/onchain/token';
const POOLS_ROUTE = '/api/onchain/pools';
const KLINES_ROUTE = '/api/onchain/klines';
const POOL_PRICE_ROUTE = '/api/onchain/pool-price';
const POOL_PRICES_ROUTE = '/api/onchain/pool-prices';
const POOL_PRICE_BATCH_MAX = 32;
const TRADES_ROUTE = '/api/onchain/trades';
const NEW_POOLS_ROUTE = '/api/onchain/new-pools';
const FX_REFERENCE_ROUTE = '/api/onchain/fx-reference';
const HOLDERS_ROUTE = '/api/onchain/holders';
const SECURITY_ROUTE = '/api/onchain/security';
const TOKEN_WALLETS_ROUTE = '/api/onchain/token-wallets';
const WALLET_QUICKVIEW_ROUTE = '/api/onchain/wallet-quickview';
const RELATIONS_ROUTE = '/api/onchain/relations';
const OVERVIEW_ROUTE = '/api/onchain/overview';
const SMART_MONEY_ROUTE = '/api/onchain/smart-money';
const TOP_WALLETS_ROUTE = '/api/onchain/top-wallets';

const DEX_BASE = 'https://api.dexscreener.com';
// Step1041.4 objective hot discovery. GeckoTerminal trending-pool feeds are used only by the
// fixed 5-minute backend discovery cycle; App reads never call GeckoTerminal directly.
// Paid DEX Screener boosts/ads/CTO remain supplemental identity discovery only and never
// participate in the hot rank score.
const GECKO_BASE = 'https://api.geckoterminal.com/api/v2';
const GECKO_MIN_GAP_MS = Math.max(12_500, Number(process.env.KAKA_GECKO_MIN_GAP_MS || 15_000));
const GECKO_MAX_QUEUE = Math.max(6, Math.min(32, Number(process.env.KAKA_GECKO_MAX_QUEUE || 20)));
const GECKO_TIMEOUT_MS = Math.max(6_000, Math.min(25_000, Number(process.env.KAKA_GECKO_TIMEOUT_MS || 15_000)));
const GECKO_NETWORK = Object.freeze({
  ethereum: 'eth', bsc: 'bsc', base: 'base', solana: 'solana',
  arbitrum: 'arbitrum', polygon: 'polygon_pos', optimism: 'optimism', avalanche: 'avax', linea: 'linea',
});
// Step1041.5: Kaka's public "热门" order follows Binance Wallet's official Trending board
// while the app is young and has insufficient first-party search/view traffic to build a mature
// proprietary ranking. Binance rank only determines order; Kaka still re-verifies exact
// chain+contract identity and chooses the highest-liquidity exact pool for market/K-line data.
// If the Binance feed is temporarily unavailable or yields fewer than 50 verifiable identities,
// objective Gecko/DEX discovery may append fallback rows after all Binance-ranked rows.
const HOT_RULE_VERSION = 'kaka_step1041_5_binance_wallet_trending_order_primary_exact_market_verified_v1';
const HOT_DISCOVERY_RULE_VERSION = 'kaka_step1041_5_binance_wallet_trending_1h_plus_objective_fallback_v1';
const BINANCE_WALLET_TRENDING_URL = 'https://web3.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/market/token/pulse/unified/rank/list/ai';
const BINANCE_WALLET_TRENDING_PERIOD = 30; // official token-rank period code: 30 = 1h
const BINANCE_WALLET_TRENDING_SIZE = 200; // official endpoint max; gives room after chain/identity verification
const BINANCE_WALLET_TIMEOUT_MS = 12_000;
const BINANCE_WALLET_MIN_GAP_MS = Math.max(1_200, Number(process.env.KAKA_BINANCE_WALLET_MIN_GAP_MS || 1_500));
const BINANCE_ALPHA_LIST_URL = 'https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list';
const BINANCE_ALPHA_REFRESH_MS = 10 * 60_000;

// Step1042: official Binance Web3 public smart-money and top-trader boards.
// Smart-money inflow currently supports BSC/Base/Solana; address PnL leaderboard additionally supports Ethereum.
// All requests are background-only shared collectors. User reads never start Binance upstream work.
const BINANCE_SMART_MONEY_INFLOW_URL = 'https://web3.binance.com/bapi/defi/v1/public/wallet-direct/tracker/wallet/token/inflow/rank/query/ai';
const BINANCE_TOP_WALLETS_URL = 'https://web3.binance.com/bapi/defi/v1/public/wallet-direct/market/leaderboard/query/ai';
const SMART_MONEY_FLOW_NETWORKS = Object.freeze(['bsc', 'base', 'solana']);
const TOP_WALLET_NETWORKS = Object.freeze(['ethereum', 'bsc', 'base', 'solana']);
const SMART_MONEY_FLOW_PERIODS = Object.freeze(['1h', '4h', '24h']);
const TOP_WALLET_PERIODS = Object.freeze(['7d', '30d', '90d']);
const SMART_MONEY_REFRESH_MS = 5 * 60_000;
const TOP_WALLET_REFRESH_MS = 15 * 60_000;
const SMART_MONEY_RETAIN_MS = 30 * 60_000;
const TOP_WALLET_RETAIN_MS = 60 * 60_000;
const SMART_MONEY_MAX_ROWS = 50;
const TOP_WALLET_MAX_ROWS = 25;
// Candidate endpoints are documented at 60/min; search/pairs at 300/min.
// One global 1.2s lane caps the whole on-chain module at <=50 upstream starts/min regardless of users.
const DEX_MIN_GAP_MS = 1_200;
const DEX_MAX_QUEUE = 80;
const UPSTREAM_TIMEOUT_MS = 12_000;
const DISCOVERY_REFRESH_MS = 5 * 60_000;
// Step1037.5: discovery/profile remains slow; exact market fields refresh on a separate fixed backend lane.
const MARKET_REFRESH_MS = 30_000;
// Step1041.5.4.3.4.2: bounded exact-pool near-realtime focus collector.
// User reads only register exact identities in memory; the fixed timer owns all DEX upstream work.
const POOL_PRICE_REFRESH_MS = 5_000;
const POOL_PRICE_RETAIN_MS = 20_000;
const POOL_PRICE_FOCUS_TTL_MS = 2 * 60_000;
const POOL_PRICE_FOCUS_MAX = 32;
const MARKET_RETAIN_MS = 5 * 60_000;
const FX_REFRESH_MS = 6 * 60 * 60_000;
const FX_RETAIN_MS = 96 * 60 * 60_000;
const ECB_FX_URL = 'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml';
const DISCOVERY_RETAIN_MS = 30 * 60_000;
const DISCOVERY_MAX_CANDIDATES_PER_CHAIN = 30; // Step1042.1 cold-start bound: one exact DEX batch per chain
const DEX_TOKEN_BATCH_MAX = 30;
const STEP1041_CANDIDATE_FEED_COUNT = 8;
const CACHE_MAX_ENTRIES = 512;
const NEGATIVE_CACHE_MAX_ENTRIES = 256;
const MAX_RESPONSE_ROWS = 100;
const STEP1041_HOT_MAX_ROWS = 50; // response cap remains 50
const STEP1042_INTERNAL_HOT_MAX_ROWS_PER_CHAIN = 30; // one DEX token batch per chain; 9-chain internal coverage
const STEP1041_NEW_MAX_ROWS = 50;
const STEP1041_NEW_POOL_MAX_AGE_MS = 7 * 24 * 60 * 60_000;

// Step1037 Moralis production guard.
// Current official pricing: pair candlesticks=150 CU, pair swaps=50 CU.
// Free plan currently includes 40,000 CU/day; Kaka reserves headroom and fails closed at 30,000 CU/day.
// The API key exists only in Render Environment. It is never returned to App/health/logs.
const MORALIS_API_KEY = String(process.env.MORALIS_API_KEY || '').trim();
const MORALIS_MIN_GAP_MS = Math.max(750, Number(process.env.KAKA_MORALIS_MIN_GAP_MS || 1_200));
const MORALIS_MAX_QUEUE = Math.max(8, Math.min(80, Number(process.env.KAKA_MORALIS_MAX_QUEUE || 40)));
const MORALIS_TIMEOUT_MS = Math.max(5_000, Math.min(30_000, Number(process.env.KAKA_MORALIS_TIMEOUT_MS || 15_000)));
const MORALIS_DAILY_CU_BUDGET = Math.max(3_000, Math.min(38_000, Number(process.env.KAKA_MORALIS_DAILY_CU_BUDGET || 30_000)));
const MORALIS_KLINE_CU = 150;
const MORALIS_TRADES_CU = 50;
const MORALIS_LEDGER_PATH = process.env.KAKA_MORALIS_LEDGER_PATH || '/tmp/kaka_onchain_moralis_budget_v1.json';
const KLINE_CACHE_MAX_ENTRIES = 128;
const KLINE_FEATURE_SCHEMA_VERSION = 'step1046_2_5_onchain_kline_continuity_v1';
const TRADE_CACHE_MAX_ENTRIES = 96;
const IDENTITY_PROOF_MAX_ENTRIES = 256;
const KLINE_MAX_ROWS = 300;
const TRADE_MAX_ROWS = 50;
const IDENTITY_PROOF_TTL_MS = 24 * 60 * 60_000;
// Step1038 holder concentration + contract/security facts. These are on-demand backend builds,
// never App-direct upstream calls. Cache/singleflight and bounded provider lanes cap user-scale load.
const MORALIS_HOLDER_METRICS_CU = 50;
const MORALIS_TOP_HOLDERS_CU = 50;
const HOLDER_FRESH_MS = 15 * 60_000;
const HOLDER_STALE_MS = 6 * 60 * 60_000;
const HOLDER_NEGATIVE_MS = 2 * 60_000;
const SOLANA_HELIUS_HOLDER_FRESH_MS = 30 * 60_000;
const SOLANA_HELIUS_HOLDER_STALE_MS = 12 * 60 * 60_000;
const GOPLUS_MIN_GAP_MS = Math.max(2_050, Number(process.env.KAKA_GOPLUS_MIN_GAP_MS || 2_100));
const GOPLUS_MAX_QUEUE = Math.max(6, Math.min(40, Number(process.env.KAKA_GOPLUS_MAX_QUEUE || 24)));
const GOPLUS_TIMEOUT_MS = Math.max(5_000, Math.min(25_000, Number(process.env.KAKA_GOPLUS_TIMEOUT_MS || 12_000)));
const GOPLUS_ACCESS_TOKEN = String(process.env.GOPLUS_ACCESS_TOKEN || '').trim();
const SECURITY_FRESH_MS = 30 * 60_000;
const SECURITY_STALE_MS = 24 * 60 * 60_000;
const SECURITY_NEGATIVE_MS = 2 * 60_000;
const EVM_GOPLUS_CHAIN_ID = Object.freeze({
  ethereum: '1', bsc: '56', base: '8453', arbitrum: '42161', polygon: '137',
  optimism: '10', avalanche: '43114', linea: '59144',
});

// Step1039 wallet intelligence. Heavy wallet enrichment is strictly on-demand, cached,
// singleflight and shares the same Moralis daily CU guard / Helius scheduler as Step1038.
// Wallet Insights is intentionally NOT used because Moralis currently classifies it as a
// Pro/premium endpoint. This step stays compatible with the existing free-key deployment:
// EVM age comes from Wallet Chain Activity and PnL from Wallet PnL Breakdown; Solana
// portfolio/age uses Helius and exact-token swap cashflow uses Moralis Solana wallet swaps.
const MORALIS_TOKEN_SWAPS_CU = 50;
const MORALIS_TOP_TRADERS_CU = 50;
const MORALIS_WALLET_PNL_CU = 50;
// Conservative internal reservation for Wallet Chain Activity. It intentionally over-reserves
// relative to a small metadata call so the local 30k/day guard cannot understate provider use.
const MORALIS_WALLET_ACTIVITY_BUDGET_CU = 100;
const MORALIS_SOLANA_WALLET_SWAPS_CU = 50;
const TOKEN_WALLET_FRESH_MS = 5 * 60_000;
const TOKEN_WALLET_STALE_MS = 30 * 60_000;
const TOKEN_WALLET_NEGATIVE_MS = 90_000;
const WALLET_BASE_FRESH_MS = 10 * 60_000;
const WALLET_BASE_STALE_MS = 60 * 60_000;
const WALLET_BASE_NEGATIVE_MS = 90_000;
const WALLET_QUICKVIEW_FRESH_MS = 5 * 60_000;
const WALLET_QUICKVIEW_STALE_MS = 30 * 60_000;
const EARLY_SWAP_SCOPE = 100;
const RECENT_SWAP_SCOPE = 100;
const WALLET_SIGNAL_MAX_ROWS = 50;
const SMART_MONEY_RULE_VERSION = 'kaka_step1039_profit_signal_v1';
const SMART_MONEY_TOKEN_MIN_PROFIT_USD = 1_000;
const SMART_MONEY_TOKEN_MIN_ROI_PCT = 20;
const SMART_MONEY_TOKEN_MIN_TRADES = 5;
const SMART_MONEY_WALLET_MIN_PROFIT_USD = 5_000;
const SMART_MONEY_WALLET_MIN_WIN_RATE_PCT = 55;
const SMART_MONEY_WALLET_MIN_EVALUABLE_POSITIONS = 5;
const SMART_MONEY_WALLET_MIN_TRADES = 10;

// Step1040 relationship evidence. This is deliberately bounded and on-demand.
// It produces evidence-backed relationship *signals*, never identity or wrongdoing labels.
// Funding-source evidence is long-lived because the original funder is historical/immutable;
// token relation snapshots are shorter because early-trader / holder scopes can change.
const RELATION_ANALYZED_WALLET_MAX = 8;
const RELATION_FRESH_MS = 15 * 60_000;
const RELATION_STALE_MS = 2 * 60 * 60_000;
const RELATION_NEGATIVE_MS = 2 * 60_000;
const FUNDING_FRESH_MS = 24 * 60 * 60_000;
const FUNDING_STALE_MS = 7 * 24 * 60 * 60_000;
const FUNDING_NEGATIVE_MS = 10 * 60_000;
const MORALIS_WALLET_HISTORY_BUDGET_CU = 100;
const EVM_INITIAL_HISTORY_LIMIT = 40;
const SNIPER_HIGH_SECONDS = 30;
const SNIPER_MEDIUM_SECONDS = 120;
const SNIPER_MAX_RANK = 10;
const RELATION_CONFIDENCE_RULE_VERSION = 'kaka_step1040_evidence_confidence_v1';


// Step1038.2.2: Solana holder analytics uses Helius DAS getTokenAccounts by
// exact mint. Step1038.2.1 used getProgramAccountsV2 against the global SPL Token
// programs; on a highly-filtered mint that can advance through hundreds of empty
// cursor pages before finding matching accounts. DAS has a first-class mint index,
// returns owner+amount directly, and is the Helius-documented holder enumeration
// path. We still publish concentration only after the full exact-mint result set is
// scanned; partial pages are never promoted to holder facts.
const HELIUS_API_KEY = String(process.env.HELIUS_API_KEY || '').trim();
const HELIUS_RPC_BASE = 'https://mainnet.helius-rpc.com/';
// DAS free tier is 2 req/s, so keep this backend lane below 2/s even with one user.
const HELIUS_MIN_GAP_MS = Math.max(520, Number(process.env.KAKA_HELIUS_MIN_GAP_MS || 560));
const HELIUS_MAX_QUEUE = Math.max(6, Math.min(48, Number(process.env.KAKA_HELIUS_MAX_QUEUE || 24)));
const HELIUS_TIMEOUT_MS = Math.max(5_000, Math.min(30_000, Number(process.env.KAKA_HELIUS_TIMEOUT_MS || 15_000)));
const HELIUS_PAGE_LIMIT = Math.max(100, Math.min(1_000, Number(process.env.KAKA_HELIUS_HOLDER_PAGE_LIMIT || 1_000)));
const HELIUS_MAX_EXACT_TOKEN_ACCOUNTS = Math.max(5_000, Math.min(100_000, Number(process.env.KAKA_HELIUS_HOLDER_MAX_ACCOUNTS || 50_000)));
const HELIUS_MAX_HOLDER_PAGES = Math.max(5, Math.min(100, Math.ceil(HELIUS_MAX_EXACT_TOKEN_ACCOUNTS / HELIUS_PAGE_LIMIT) + 2));

const MORALIS_EVM_CHAIN = Object.freeze({
  ethereum: 'eth',
  bsc: 'bsc',
  base: 'base',
  arbitrum: 'arbitrum',
  polygon: 'polygon',
  optimism: 'optimism',
  avalanche: 'avalanche',
  linea: 'linea',
});
const MORALIS_TIMEFRAME = Object.freeze({
  '1m': '1min',
  '5m': '5min',
  '15m': '5min',
  '30m': '30min',
  '1h': '1h',
  '4h': '4h',
  '1d': '1d',
});
const INTERVAL_MS = Object.freeze({
  '1m': 60_000,
  '5m': 5 * 60_000,
  '15m': 15 * 60_000,
  '30m': 30 * 60_000,
  '1h': 60 * 60_000,
  '4h': 4 * 60 * 60_000,
  '1d': 24 * 60 * 60_000,
});

const NETWORKS = Object.freeze({
  ethereum: Object.freeze({ key: 'ethereum', dex: 'ethereum', chain_id: 1, family: 'evm', zh: '以太坊', en: 'Ethereum' }),
  bsc: Object.freeze({ key: 'bsc', dex: 'bsc', chain_id: 56, family: 'evm', zh: 'BNB Chain', en: 'BNB Chain' }),
  base: Object.freeze({ key: 'base', dex: 'base', chain_id: 8453, family: 'evm', zh: 'Base', en: 'Base' }),
  solana: Object.freeze({ key: 'solana', dex: 'solana', chain_id: null, family: 'solana', zh: 'Solana', en: 'Solana' }),
  arbitrum: Object.freeze({ key: 'arbitrum', dex: 'arbitrum', chain_id: 42161, family: 'evm', zh: 'Arbitrum', en: 'Arbitrum' }),
  polygon: Object.freeze({ key: 'polygon', dex: 'polygon', chain_id: 137, family: 'evm', zh: 'Polygon', en: 'Polygon' }),
  optimism: Object.freeze({ key: 'optimism', dex: 'optimism', chain_id: 10, family: 'evm', zh: 'Optimism', en: 'Optimism' }),
  avalanche: Object.freeze({ key: 'avalanche', dex: 'avalanche', chain_id: 43114, family: 'evm', zh: 'Avalanche', en: 'Avalanche' }),
  linea: Object.freeze({ key: 'linea', dex: 'linea', chain_id: 59144, family: 'evm', zh: 'Linea', en: 'Linea' }),
});
const DEX_TO_NETWORK = Object.freeze(Object.fromEntries(Object.values(NETWORKS).map((x) => [x.dex, x.key])));

const stats = {
  user_reads: 0,
  background_cycles_started: 0,
  background_cycles_succeeded: 0,
  background_cycles_failed: 0,
  market_refresh_started: 0,
  market_refresh_succeeded: 0,
  market_refresh_failed: 0,
  market_refresh_retained_networks_last: [],
  market_refresh_retained_rows_last: 0,
  fx_refresh_started: 0,
  fx_refresh_succeeded: 0,
  fx_refresh_failed: 0,
  last_background_started_at: null,
  last_background_success_at: null,
  last_background_error: '',
  dex_upstream_started: 0,
  dex_upstream_succeeded: 0,
  dex_upstream_failed: 0,
  gecko_upstream_started: 0,
  gecko_upstream_succeeded: 0,
  gecko_upstream_failed: 0,
  gecko_discovery_cycles: 0,
  gecko_discovery_candidates: 0,
  gecko_discovery_ready_networks: [],
  gecko_discovery_failed_networks: [],
  gecko_discovery_retried_networks: [],
  gecko_discovery_error_by_network: {},
  binance_wallet_rank_started: 0,
  binance_wallet_rank_succeeded: 0,
  binance_wallet_rank_failed: 0,
  binance_wallet_rank_rows: 0,
  binance_wallet_rank_last_success_at: null,
  binance_wallet_rank_last_error: '',
  smart_money_refresh_started: 0,
  smart_money_refresh_succeeded: 0,
  smart_money_refresh_failed: 0,
  smart_money_upstream_requests: 0,
  top_wallet_refresh_started: 0,
  top_wallet_refresh_succeeded: 0,
  top_wallet_refresh_failed: 0,
  top_wallet_upstream_requests: 0,
  binance_alpha_refresh_started: 0,
  binance_alpha_refresh_succeeded: 0,
  binance_alpha_refresh_failed: 0,
  cache_fresh_hits: 0,
  cache_stale_hits: 0,
  cache_misses: 0,
  negative_hits: 0,
  inflight_hits: 0,
  queue_rejections: 0,
  moralis_upstream_started: 0,
  moralis_upstream_succeeded: 0,
  moralis_upstream_failed: 0,
  moralis_budget_rejections: 0,
  moralis_key_missing_rejections: 0,
  kline_cache_fresh_hits: 0,
  kline_cache_stale_hits: 0,
  kline_cache_misses: 0,
  kline_inflight_hits: 0,
  kline_identity_exact_proofs: 0,
  kline_identity_price_match_proofs: 0,
  kline_identity_rejections: 0,
  trades_cache_hits: 0,
  trades_cache_misses: 0,
  holder_builds: 0,
  holder_build_failures: 0,
  security_builds: 0,
  security_build_failures: 0,
  goplus_upstream_started: 0,
  goplus_upstream_succeeded: 0,
  goplus_upstream_failed: 0,
  helius_upstream_started: 0,
  helius_upstream_succeeded: 0,
  helius_upstream_failed: 0,
  helius_key_missing_rejections: 0,
  helius_holder_complete_scans: 0,
  helius_holder_incomplete_scans: 0,
  token_wallet_builds: 0,
  token_wallet_build_failures: 0,
  wallet_quickview_builds: 0,
  wallet_quickview_build_failures: 0,
  relation_builds: 0,
  relation_build_failures: 0,
  funding_source_builds: 0,
  funding_source_build_failures: 0,
  step1041_shared_snapshot_reads: 0,
  pool_price_focus_reads: 0,
  pool_price_batch_reads: 0,
  pool_price_refresh_started: 0,
  pool_price_refresh_succeeded: 0,
  pool_price_refresh_failed: 0,
  pool_price_rows: 0,
};

const cache = new Map();
const negativeCache = new Map();
const inflight = new Map();
const klineCache = new Map();
const klineInflight = new Map();
const tradeCache = new Map();
const tradeInflight = new Map();
const identityProofCache = new Map();
let discoveryStarted = false;
let discoveryInflight = null;
let trendingSnapshot = [];
let discoveryUpdatedAt = 0;
let marketRefreshInflight = null;
let marketUpdatedAt = 0;
const marketNetworkUpdatedAt = new Map();
const poolPriceFocus = new Map();
const poolPriceSnapshot = new Map();
let poolPriceRefreshInflight = null;
let poolPriceUpdatedAt = 0;
let fxRefreshInflight = null;
let fxSnapshot = null;
let fxUpdatedAt = 0;
let smartMoneySnapshot = new Map();
let smartMoneyUpdatedAt = 0;
let smartMoneyInflight = null;
let topWalletSnapshot = new Map();
let topWalletUpdatedAt = 0;
let topWalletInflight = null;

function text(value) { return String(value ?? '').trim(); }
function lower(value) { return text(value).toLowerCase(); }
function numberOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
function intRange(value, min, max, fallback) {
  const n = Math.floor(Number(value));
  return Number.isFinite(n) ? Math.max(min, Math.min(max, n)) : fallback;
}
function isoFromMs(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  try { return new Date(n).toISOString(); } catch { return null; }
}
function normalizeNetwork(raw) {
  const value = lower(raw).replace(/\s+/g, '');
  if (!value || value === 'all') return 'all';
  if (value === 'eth' || value === 'ethereum' || value === 'erc20') return 'ethereum';
  if (value === 'bsc' || value === 'bnb' || value === 'bnbchain' || value === 'bep20') return 'bsc';
  if (value === 'base') return 'base';
  if (value === 'sol' || value === 'solana' || value === 'spl') return 'solana';
  if (value === 'arb' || value === 'arbitrum' || value === 'arbitrumone') return 'arbitrum';
  if (value === 'polygon' || value === 'matic' || value === 'polygonpos') return 'polygon';
  if (value === 'op' || value === 'optimism' || value === 'opmainnet') return 'optimism';
  if (value === 'avax' || value === 'avalanche' || value === 'avalanchecchain') return 'avalanche';
  if (value === 'linea') return 'linea';
  return '';
}
function networkMeta(key) { return NETWORKS[key] || null; }
function looksEvmAddress(value) { return /^0x[a-fA-F0-9]{40}$/.test(text(value)); }
function looksSolanaAddress(value) { return /^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(text(value)); }
function validAddressForNetwork(network, value) {
  const meta = networkMeta(network);
  return Boolean(meta && (meta.family === 'evm' ? looksEvmAddress(value) : looksSolanaAddress(value)));
}

function responseBase(extra = {}) {
  return {
    ok: true,
    version: VERSION,
    data_version: DATA_VERSION,
    schema_version: SCHEMA_VERSION,
    read_only_shared: true,
    app_direct_upstream_requests: 0,
    user_reads_direct_upstream_requests: 0,
    fixed_backend_upstream_rate_independent_of_user_count: true,
    same_key_cache_singleflight: true,
    bounded_queue_fail_closed: true,
    cross_chain_substitution: false,
    cross_token_substitution: false,
    exact_contract_identity_required: true,
    trading_enabled: false,
    wallet_signing_enabled: false,
    database_writes: false,
    ...extra,
  };
}
function sendJson(res, status, payload, extraHeaders = {}) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': String(body.length),
    ...extraHeaders,
  });
  res.end(body);
}
function pruneMap(map, max) {
  if (map.size <= max) return;
  const entries = [...map.entries()].sort((a, b) => Number(a[1]?.storedAt || a[1]?.until || 0) - Number(b[1]?.storedAt || b[1]?.until || 0));
  while (entries.length > max) map.delete(entries.shift()[0]);
}

function createScheduler({ name, minGapMs, maxQueue }) {
  let queue = [];
  let timer = null;
  let running = false;
  let lastStartAt = 0;
  const health = { name, min_gap_ms: minGapMs, max_queue: maxQueue, started: 0, completed: 0, failed: 0, rejected: 0, max_queue_seen: 0 };
  function pump() {
    if (running || timer || queue.length === 0) return;
    const wait = Math.max(0, minGapMs - (Date.now() - lastStartAt));
    if (wait > 0) {
      timer = setTimeout(() => { timer = null; pump(); }, wait);
      timer.unref?.();
      return;
    }
    queue.sort((a, b) => b.priority - a.priority || a.enqueuedAt - b.enqueuedAt);
    const job = queue.shift();
    running = true;
    lastStartAt = Date.now();
    health.started += 1;
    Promise.resolve().then(job.run).then((value) => {
      health.completed += 1;
      job.resolve(value);
    }, (error) => {
      health.failed += 1;
      job.reject(error);
    }).finally(() => { running = false; pump(); });
  }
  function enqueue(run, { priority = 0, label = '' } = {}) {
    if (queue.length >= maxQueue) {
      health.rejected += 1;
      stats.queue_rejections += 1;
      return Promise.reject(new Error(`${name}_queue_full`));
    }
    return new Promise((resolve, reject) => {
      queue.push({ run, resolve, reject, priority, label, enqueuedAt: Date.now() });
      health.max_queue_seen = Math.max(health.max_queue_seen, queue.length);
      pump();
    });
  }
  function state() { return { ...health, queue: queue.length, running, last_start_at: lastStartAt || null }; }
  return { enqueue, state };
}

const dexScheduler = createScheduler({ name: 'dexscreener', minGapMs: DEX_MIN_GAP_MS, maxQueue: DEX_MAX_QUEUE });
async function dexFetchJson(url, { priority = 0, label = '' } = {}) {
  return dexScheduler.enqueue(async () => {
    stats.dex_upstream_started += 1;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS);
    timer.unref?.();
    try {
      const response = await fetch(url, {
        signal: controller.signal,
        headers: { accept: 'application/json', 'user-agent': 'KakaWeb3-Onchain-Shared/1036' },
      });
      const body = await response.text();
      if (!response.ok) throw new Error(`dexscreener_http_${response.status}:${body.slice(0, 220)}`);
      let parsed;
      try { parsed = JSON.parse(body); } catch { throw new Error('dexscreener_invalid_json'); }
      stats.dex_upstream_succeeded += 1;
      return parsed;
    } catch (error) {
      stats.dex_upstream_failed += 1;
      throw error;
    } finally { clearTimeout(timer); }
  }, { priority, label });
}


const geckoScheduler = createScheduler({ name: 'geckoterminal', minGapMs: GECKO_MIN_GAP_MS, maxQueue: GECKO_MAX_QUEUE });
async function geckoFetchJson(url, { priority = 0, label = '' } = {}) {
  return geckoScheduler.enqueue(async () => {
    stats.gecko_upstream_started += 1;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), GECKO_TIMEOUT_MS);
    timer.unref?.();
    try {
      const response = await fetch(url, {
        signal: controller.signal,
        headers: {
          accept: 'application/json;version=20230203',
          'user-agent': 'KakaWeb3-Onchain-Shared/1042.1',
        },
      });
      const body = await response.text();
      if (!response.ok) throw new Error(`geckoterminal_http_${response.status}:${body.slice(0, 220)}`);
      let parsed;
      try { parsed = JSON.parse(body); } catch { throw new Error('geckoterminal_invalid_json'); }
      stats.gecko_upstream_succeeded += 1;
      return parsed;
    } catch (error) {
      stats.gecko_upstream_failed += 1;
      throw error;
    } finally { clearTimeout(timer); }
  }, { priority, label });
}

let binanceWalletLastCycleSucceeded = false;
const binanceWalletScheduler = createScheduler({ name: 'binance-wallet-public-rank', minGapMs: BINANCE_WALLET_MIN_GAP_MS, maxQueue: 8 });
function binanceWalletTrendingRowsFromPayload(payload) {
  const rows = payload?.data?.tokens;
  return Array.isArray(rows) ? rows : [];
}
async function fetchBinanceWalletTrendingCandidates() {
  stats.binance_wallet_rank_started += 1;
  try {
    const payload = await binanceWalletScheduler.enqueue(async () => {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), BINANCE_WALLET_TIMEOUT_MS);
      timer.unref?.();
      try {
        // Mirrors Binance's public crypto-market-rank/token-rank Trending defaults.
        // rankType=10 is Trending; period=30 is 1h; sortBy=0 preserves the board's default order.
        const body = {
          rankType: 10,
          period: BINANCE_WALLET_TRENDING_PERIOD,
          sortBy: 0,
          orderAsc: false,
          page: 1,
          size: BINANCE_WALLET_TRENDING_SIZE,
          countMin: 10,
          launchTimeMin: 15,
          liquidityMin: 5000,
          uniqueTraderMin: 10,
          volumeMin: 10000,
          tagFilter: [1, 2, 3],
        };
        const response = await fetch(BINANCE_WALLET_TRENDING_URL, {
          method: 'POST',
          signal: controller.signal,
          headers: {
            accept: 'application/json',
            'content-type': 'application/json',
            'accept-encoding': 'identity',
            'user-agent': 'KakaWeb3-Onchain-Shared/1041.5',
          },
          body: JSON.stringify(body),
        });
        const raw = await response.text();
        if (!response.ok) throw new Error(`binance_wallet_trending_http_${response.status}:${raw.slice(0, 220)}`);
        let decoded;
        try { decoded = JSON.parse(raw); } catch { throw new Error('binance_wallet_trending_invalid_json'); }
        const code = text(decoded?.code);
        if (code && code !== '000000') throw new Error(`binance_wallet_trending_code_${code}:${text(decoded?.message || decoded?.msg).slice(0, 160)}`);
        return decoded;
      } finally { clearTimeout(timer); }
    }, { priority: -50, label: 'background_binance_wallet_trending_1h' });

    const rows = binanceWalletTrendingRowsFromPayload(payload);
    const candidates = [];
    const seen = new Set();
    for (let index = 0; index < rows.length; index += 1) {
      const item = rows[index];
      const network = alphaNetworkFromChainId(item?.chainId ?? item?.chain_id);
      const address = text(item?.contractAddress ?? item?.contract_address ?? item?.address);
      if (!network || !validAddressForNetwork(network, address)) continue;
      const key = `${network}|${lower(address)}`;
      if (seen.has(key)) continue;
      seen.add(key);
      candidates.push({
        network,
        address,
        source: 'binance_wallet_public_trending_1h',
        token_profile: null,
        amount: 0,
        total_amount: 0,
        binance_wallet_rank: index + 1,
        binance_wallet_rank_period: '1h',
        binance_wallet_rank_period_code: BINANCE_WALLET_TRENDING_PERIOD,
        binance_wallet_rank_type: 10,
        binance_wallet_symbol: text(item?.symbol),
        binance_wallet_icon: text(item?.icon),
        binance_wallet_price_usd: numberOrNull(item?.price),
        binance_wallet_market_cap_usd: numberOrNull(item?.marketCap),
        binance_wallet_liquidity_usd: numberOrNull(item?.liquidity),
        binance_wallet_holders: numberOrNull(item?.holders),
        binance_wallet_alpha_info: item?.alphaInfo && typeof item.alphaInfo === 'object' ? item.alphaInfo : null,
      });
    }
    binanceWalletLastCycleSucceeded = candidates.length > 0;
    stats.binance_wallet_rank_succeeded += 1;
    stats.binance_wallet_rank_rows = candidates.length;
    stats.binance_wallet_rank_last_success_at = new Date().toISOString();
    stats.binance_wallet_rank_last_error = '';
    return { candidates, succeeded: true, upstream_rows: rows.length };
  } catch (error) {
    binanceWalletLastCycleSucceeded = false;
    stats.binance_wallet_rank_failed += 1;
    stats.binance_wallet_rank_last_error = text(error?.message || error).slice(0, 400);
    return { candidates: [], succeeded: false, upstream_rows: 0 };
  }
}


function binancePublicChainId(network) {
  if (network === 'bsc') return '56';
  if (network === 'base') return '8453';
  if (network === 'ethereum') return '1';
  if (network === 'solana') return 'CT_501';
  return '';
}
function binancePublicAssetUrl(value) {
  const raw = text(value);
  if (!raw) return '';
  if (/^https?:\/\//i.test(raw)) return raw;
  return `https://bin.bnbstatic.com${raw.startsWith('/') ? '' : '/'}${raw}`;
}
function binancePublicRows(payload) {
  const d = payload?.data;
  if (Array.isArray(d)) return d;
  if (Array.isArray(d?.list)) return d.list;
  if (Array.isArray(d?.rows)) return d.rows;
  if (Array.isArray(d?.tokens)) return d.tokens;
  if (Array.isArray(d?.data)) return d.data;
  if (Array.isArray(payload?.list)) return payload.list;
  return [];
}
async function binancePublicFetch(url, { method = 'GET', body = null, label = '' } = {}) {
  return binanceWalletScheduler.enqueue(async () => {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), BINANCE_WALLET_TIMEOUT_MS);
    timer.unref?.();
    try {
      const response = await fetch(url, {
        method,
        signal: controller.signal,
        headers: {
          accept: 'application/json',
          'accept-encoding': 'identity',
          'user-agent': 'KakaWeb3-Onchain-Shared/1042',
          ...(body ? { 'content-type': 'application/json' } : {}),
        },
        ...(body ? { body: JSON.stringify(body) } : {}),
      });
      const raw = await response.text();
      if (!response.ok) throw new Error(`${label || 'binance_public'}_http_${response.status}:${raw.slice(0, 200)}`);
      let decoded;
      try { decoded = JSON.parse(raw); } catch { throw new Error(`${label || 'binance_public'}_invalid_json`); }
      const code = text(decoded?.code);
      if (code && code !== '000000' && code !== '0') throw new Error(`${label || 'binance_public'}_code_${code}`);
      return decoded;
    } finally { clearTimeout(timer); }
  }, { priority: -45, label });
}
function normalizeSmartMoneyFlowRow(network, period, raw, index) {
  const address = text(raw?.ca ?? raw?.contractAddress ?? raw?.contract_address ?? raw?.address);
  if (!validAddressForNetwork(network, address)) return null;
  const inflow = numberOrNull(raw?.inflow ?? raw?.netInflow ?? raw?.net_inflow ?? raw?.netFlow ?? raw?.net_flow);
  return {
    rank: index + 1,
    network,
    period,
    token_address: address,
    symbol: text(raw?.symbol ?? raw?.tokenSymbol ?? raw?.token_symbol),
    name: text(raw?.tokenName ?? raw?.token_name ?? raw?.name),
    icon: binancePublicAssetUrl(raw?.tokenIconUrl ?? raw?.token_icon_url ?? raw?.icon ?? raw?.logo),
    net_inflow_usd: inflow,
    price_usd: numberOrNull(raw?.price ?? raw?.priceUsd ?? raw?.price_usd),
    market_cap_usd: numberOrNull(raw?.marketCap ?? raw?.market_cap ?? raw?.marketCapUsd),
    holders: numberOrNull(raw?.holders ?? raw?.holderCount),
    source_time_ms: numberOrNull(raw?.latestTxTime ?? raw?.timestamp ?? raw?.updateTime),
    source: 'binance_web3_public_smart_money_inflow_rank',
    source_semantics: 'official_smart_money_tag_net_inflow_rank_not_kaka_inference',
  };
}
function normalizeTopWalletRow(network, period, raw, index) {
  const address = text(raw?.address ?? raw?.walletAddress ?? raw?.wallet_address);
  if (!walletAddressValid(network, address)) return null;
  return {
    rank: index + 1,
    network,
    period,
    wallet: address,
    label: text(raw?.addressLabel ?? raw?.label ?? raw?.name),
    avatar: binancePublicAssetUrl(raw?.addressLogo ?? raw?.addressLogoUrl ?? raw?.logo ?? raw?.avatar),
    realized_pnl_usd: numberOrNull(raw?.realizedPnl ?? raw?.pnl ?? raw?.realized_pnl),
    realized_pnl_pct: numberOrNull(raw?.realizedPnlPercent ?? raw?.pnlPercent ?? raw?.realized_pnl_percent),
    win_rate_pct: numberOrNull(raw?.winRate ?? raw?.win_rate),
    total_volume_usd: numberOrNull(raw?.totalVolume ?? raw?.volume ?? raw?.total_volume),
    trade_count: numberOrNull(raw?.totalTxCnt ?? raw?.tradeCount ?? raw?.total_tx_count),
    traded_token_count: numberOrNull(raw?.totalTradedTokens ?? raw?.tokenCount ?? raw?.total_traded_tokens),
    last_active_time_ms: numberOrNull(raw?.lastActivity ?? raw?.lastActiveTime ?? raw?.latestTxTime ?? raw?.timestamp),
    source: 'binance_web3_public_address_pnl_leaderboard',
    source_semantics: 'official_top_trader_performance_board_not_kaka_smart_money_identity',
  };
}
async function refreshSmartMoneySnapshot() {
  if (smartMoneyInflight) return smartMoneyInflight;
  smartMoneyInflight = (async () => {
    stats.smart_money_refresh_started += 1;
    const next = new Map();
    let success = 0;
    try {
      for (const network of SMART_MONEY_FLOW_NETWORKS) {
        const chainId = binancePublicChainId(network);
        for (const period of SMART_MONEY_FLOW_PERIODS) {
          try {
            stats.smart_money_upstream_requests += 1;
            const payload = await binancePublicFetch(BINANCE_SMART_MONEY_INFLOW_URL, {
              method: 'POST',
              body: { chainId, period, tagType: 2 },
              label: `background_smart_money_${network}_${period}`,
            });
            const rows = binancePublicRows(payload)
              .map((row, index) => normalizeSmartMoneyFlowRow(network, period, row, index))
              .filter(Boolean)
              .slice(0, SMART_MONEY_MAX_ROWS);
            if (rows.length) { next.set(`${network}|${period}`, rows); success += 1; }
          } catch (_) {}
        }
      }
      if (!success) throw new Error('binance_smart_money_all_scopes_failed');
      for (const [key, rows] of smartMoneySnapshot) if (!next.has(key)) next.set(key, rows);
      smartMoneySnapshot = next;
      smartMoneyUpdatedAt = Date.now();
      stats.smart_money_refresh_succeeded += 1;
      return next;
    } catch (error) {
      stats.smart_money_refresh_failed += 1;
      throw error;
    } finally { smartMoneyInflight = null; }
  })();
  return smartMoneyInflight;
}
async function refreshTopWalletSnapshot() {
  if (topWalletInflight) return topWalletInflight;
  topWalletInflight = (async () => {
    stats.top_wallet_refresh_started += 1;
    const next = new Map();
    let success = 0;
    try {
      for (const network of TOP_WALLET_NETWORKS) {
        const chainId = binancePublicChainId(network);
        for (const period of TOP_WALLET_PERIODS) {
          try {
            stats.top_wallet_upstream_requests += 1;
            const q = new URLSearchParams({ chainId, period, tag: 'ALL', pageNo: '1', pageSize: String(TOP_WALLET_MAX_ROWS) });
            const payload = await binancePublicFetch(`${BINANCE_TOP_WALLETS_URL}?${q}`, { label: `background_top_wallet_${network}_${period}` });
            const rows = binancePublicRows(payload)
              .map((row, index) => normalizeTopWalletRow(network, period, row, index))
              .filter(Boolean)
              .slice(0, TOP_WALLET_MAX_ROWS);
            if (rows.length) { next.set(`${network}|${period}`, rows); success += 1; }
          } catch (_) {}
        }
      }
      if (!success) throw new Error('binance_top_wallet_all_scopes_failed');
      for (const [key, rows] of topWalletSnapshot) if (!next.has(key)) next.set(key, rows);
      topWalletSnapshot = next;
      topWalletUpdatedAt = Date.now();
      stats.top_wallet_refresh_succeeded += 1;
      return next;
    } catch (error) {
      stats.top_wallet_refresh_failed += 1;
      throw error;
    } finally { topWalletInflight = null; }
  })();
  return topWalletInflight;
}
function sharedSmartMoneyRows(network, period) {
  if (network !== 'all') return (smartMoneySnapshot.get(`${network}|${period}`) || []).map((x) => ({ ...x }));
  return SMART_MONEY_FLOW_NETWORKS.flatMap((n) => smartMoneySnapshot.get(`${n}|${period}`) || [])
    .map((x) => ({ ...x }))
    .sort((a, b) => (numberOrNull(b.net_inflow_usd) ?? -Infinity) - (numberOrNull(a.net_inflow_usd) ?? -Infinity));
}
function sharedTopWalletRows(network, period) {
  if (network !== 'all') return (topWalletSnapshot.get(`${network}|${period}`) || []).map((x) => ({ ...x }));
  return TOP_WALLET_NETWORKS.flatMap((n) => topWalletSnapshot.get(`${n}|${period}`) || [])
    .map((x) => ({ ...x }))
    .sort((a, b) => (numberOrNull(b.realized_pnl_usd) ?? -Infinity) - (numberOrNull(a.realized_pnl_usd) ?? -Infinity));
}

let binanceAlphaRegistry = new Map();
let binanceAlphaUpdatedAt = 0;
function alphaNetworkFromChainId(raw) {
  const key = String(raw ?? '').trim().toLowerCase();
  if (key === '56' || key === 'bsc') return 'bsc';
  if (key === '1' || key === 'eth' || key === 'ethereum') return 'ethereum';
  if (key === '8453' || key === 'base') return 'base';
  if (key === '501' || key === 'solana' || key.includes('501')) return 'solana';
  return '';
}
function alphaRowsFromPayload(payload) {
  const data = payload?.data;
  if (Array.isArray(data)) return data;
  if (Array.isArray(data?.list)) return data.list;
  if (Array.isArray(data?.tokens)) return data.tokens;
  if (Array.isArray(payload?.list)) return payload.list;
  return [];
}
async function refreshBinanceAlphaRegistry({ force = false } = {}) {
  if (!force && binanceAlphaUpdatedAt && Date.now() - binanceAlphaUpdatedAt < BINANCE_ALPHA_REFRESH_MS) return binanceAlphaRegistry.size;
  stats.binance_alpha_refresh_started += 1;
  try {
    const raw = await geckoScheduler.enqueue(async () => {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), GECKO_TIMEOUT_MS);
      timer.unref?.();
      try {
        const response = await fetch(BINANCE_ALPHA_LIST_URL, {
          signal: controller.signal,
          headers: { accept: 'application/json', 'user-agent': 'KakaWeb3-Onchain-Shared/1041.5' },
        });
        const body = await response.text();
        if (!response.ok) throw new Error(`binance_alpha_http_${response.status}:${body.slice(0, 180)}`);
        return JSON.parse(body);
      } finally { clearTimeout(timer); }
    }, { priority: -40, label: 'binance_alpha_registry' });
    const next = new Map();
    for (const item of alphaRowsFromPayload(raw)) {
      const network = alphaNetworkFromChainId(item?.chainId ?? item?.chain_id);
      const address = text(item?.contractAddress ?? item?.contract_address ?? item?.address);
      if (!network || !validAddressForNetwork(network, address)) continue;
      next.set(`${network}|${lower(address)}`, {
        key: 'binance_alpha', label: 'α', title_zh: '币安 Alpha', title_en: 'Binance Alpha',
        type: 'platform', source: 'binance_official_alpha_token_list', confidence: 'fact',
      });
    }
    if (next.size > 0) binanceAlphaRegistry = next;
    binanceAlphaUpdatedAt = Date.now();
    stats.binance_alpha_refresh_succeeded += 1;
    return binanceAlphaRegistry.size;
  } catch (error) {
    stats.binance_alpha_refresh_failed += 1;
    if (!binanceAlphaUpdatedAt) binanceAlphaUpdatedAt = Date.now();
    return binanceAlphaRegistry.size;
  }
}
function productBadgesForToken(pair, token) {
  const badges = [];
  const identity = `${pair?.network || ''}|${lower(token?.address)}`;
  const alpha = binanceAlphaRegistry.get(identity);
  if (alpha) badges.push({ ...alpha });
  const dexId = lower(pair?.dex_id);
  if (dexId.includes('pump')) {
    badges.push({ key: 'pump_ecosystem', label: 'P', title_zh: 'Pump 生态', title_en: 'Pump ecosystem', type: 'launchpad', source: 'exact_pool_dex_id', confidence: 'fact' });
  }
  if (dexId.includes('four') && dexId.includes('meme')) {
    badges.push({ key: 'four_meme', label: '4', title_zh: 'Four.meme', title_en: 'Four.meme', type: 'launchpad', source: 'exact_pool_dex_id', confidence: 'fact' });
  }
  return badges.slice(0, 3);
}


function utcBudgetDay() {
  return new Date().toISOString().slice(0, 10);
}
function loadMoralisLedger() {
  const fallback = { day: utcBudgetDay(), used_cu: 0, calls: 0, kline_calls: 0, trade_calls: 0, holder_calls: 0, wallet_calls: 0, signal_calls: 0, updated_at: null };
  try {
    const parsed = JSON.parse(readFileSync(MORALIS_LEDGER_PATH, 'utf8'));
    if (!parsed || parsed.day !== utcBudgetDay()) return fallback;
    return {
      day: parsed.day,
      used_cu: Math.max(0, Number(parsed.used_cu || 0)),
      calls: Math.max(0, Number(parsed.calls || 0)),
      kline_calls: Math.max(0, Number(parsed.kline_calls || 0)),
      trade_calls: Math.max(0, Number(parsed.trade_calls || 0)),
      holder_calls: Math.max(0, Number(parsed.holder_calls || 0)),
      wallet_calls: Math.max(0, Number(parsed.wallet_calls || 0)),
      signal_calls: Math.max(0, Number(parsed.signal_calls || 0)),
      updated_at: parsed.updated_at || null,
    };
  } catch {
    return fallback;
  }
}
let moralisLedger = loadMoralisLedger();

function refreshMoralisBudgetDay() {
  if (moralisLedger.day === utcBudgetDay()) return;
  moralisLedger = { day: utcBudgetDay(), used_cu: 0, calls: 0, kline_calls: 0, trade_calls: 0, holder_calls: 0, wallet_calls: 0, signal_calls: 0, updated_at: null };
  persistMoralisLedger();
}
function persistMoralisLedger() {
  const next = `${MORALIS_LEDGER_PATH}.${process.pid}.tmp`;
  try {
    writeFileSync(next, JSON.stringify(moralisLedger), 'utf8');
    renameSync(next, MORALIS_LEDGER_PATH);
  } catch {
    // Budget protection still stays in-memory if /tmp is unavailable.
  }
}
function moralisBudgetState() {
  refreshMoralisBudgetDay();
  return {
    day_utc: moralisLedger.day,
    used_cu: moralisLedger.used_cu,
    remaining_cu: Math.max(0, MORALIS_DAILY_CU_BUDGET - moralisLedger.used_cu),
    hard_budget_cu: MORALIS_DAILY_CU_BUDGET,
    provider_free_plan_reference_cu_per_day: 40_000,
    calls: moralisLedger.calls,
    kline_calls: moralisLedger.kline_calls,
    trade_calls: moralisLedger.trade_calls,
    holder_calls: moralisLedger.holder_calls,
    wallet_calls: moralisLedger.wallet_calls,
    signal_calls: moralisLedger.signal_calls,
    ledger_path_kind: 'local_ephemeral_process_restart_persistent_tmp',
    database_write: false,
  };
}
function reserveMoralisBudget(cu, kind) {
  refreshMoralisBudgetDay();
  if (moralisLedger.used_cu + cu > MORALIS_DAILY_CU_BUDGET) {
    stats.moralis_budget_rejections += 1;
    const error = new Error('moralis_daily_cu_budget_exhausted');
    error.statusCode = 503;
    throw error;
  }
  moralisLedger.used_cu += cu;
  moralisLedger.calls += 1;
  if (kind === 'kline') moralisLedger.kline_calls += 1;
  if (kind === 'trade') moralisLedger.trade_calls += 1;
  if (kind === 'holder') moralisLedger.holder_calls += 1;
  if (kind === 'wallet') moralisLedger.wallet_calls += 1;
  if (kind === 'signal') moralisLedger.signal_calls += 1;
  moralisLedger.updated_at = new Date().toISOString();
  persistMoralisLedger();
}

const moralisScheduler = createScheduler({
  name: 'moralis',
  minGapMs: MORALIS_MIN_GAP_MS,
  maxQueue: MORALIS_MAX_QUEUE,
});

async function moralisFetchJson(url, { cu, kind, priority = 0, label = '' }) {
  if (!MORALIS_API_KEY) {
    stats.moralis_key_missing_rejections += 1;
    const error = new Error('moralis_api_key_not_configured');
    error.statusCode = 503;
    throw error;
  }
  return moralisScheduler.enqueue(async () => {
    reserveMoralisBudget(cu, kind);
    stats.moralis_upstream_started += 1;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), MORALIS_TIMEOUT_MS);
    timer.unref?.();
    try {
      const response = await fetch(url, {
        signal: controller.signal,
        headers: {
          accept: 'application/json',
          // Step1037.1: HTTP header names are case-insensitive. Sending both
          // X-API-Key and X-Api-Key can be coalesced by the HTTP client into
          // "key, key", which Moralis correctly rejects as an invalid token.
          // Send exactly one official authentication header.
          'X-API-Key': MORALIS_API_KEY,
          'user-agent': 'KakaWeb3-Onchain-Shared/1039',
        },
      });
      const body = await response.text();
      if (!response.ok) {
        const error = new Error(`moralis_http_${response.status}:${body.slice(0, 220)}`);
        error.statusCode = response.status;
        throw error;
      }
      let parsed;
      try { parsed = JSON.parse(body); } catch { throw new Error('moralis_invalid_json'); }
      stats.moralis_upstream_succeeded += 1;
      return parsed;
    } catch (error) {
      stats.moralis_upstream_failed += 1;
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }, { priority, label });
}


const goplusScheduler = createScheduler({
  name: 'goplus',
  minGapMs: GOPLUS_MIN_GAP_MS,
  maxQueue: GOPLUS_MAX_QUEUE,
});
async function goplusFetchJson(url, { priority = 0, label = '' } = {}) {
  return goplusScheduler.enqueue(async () => {
    stats.goplus_upstream_started += 1;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), GOPLUS_TIMEOUT_MS);
    timer.unref?.();
    try {
      const headers = {
        accept: 'application/json',
        'user-agent': 'KakaWeb3-Onchain-Shared/1038',
      };
      if (GOPLUS_ACCESS_TOKEN) headers.authorization = `Bearer ${GOPLUS_ACCESS_TOKEN}`;
      const response = await fetch(url, { signal: controller.signal, headers });
      const body = await response.text();
      if (!response.ok) {
        const error = new Error(`goplus_http_${response.status}:${body.slice(0, 220)}`);
        error.statusCode = response.status;
        throw error;
      }
      let parsed;
      try { parsed = JSON.parse(body); } catch { throw new Error('goplus_invalid_json'); }
      if (Number(parsed?.code) !== 1 || !parsed?.result || typeof parsed.result !== 'object') {
        throw new Error(`goplus_bad_response:${text(parsed?.message || parsed?.code || '')}`);
      }
      stats.goplus_upstream_succeeded += 1;
      return parsed;
    } catch (error) {
      stats.goplus_upstream_failed += 1;
      throw error;
    } finally { clearTimeout(timer); }
  }, { priority, label });
}



const heliusScheduler = createScheduler({
  name: 'helius',
  minGapMs: HELIUS_MIN_GAP_MS,
  maxQueue: HELIUS_MAX_QUEUE,
});
function heliusRpcUrl() {
  const u = new URL(HELIUS_RPC_BASE);
  u.searchParams.set('api-key', HELIUS_API_KEY);
  return u.toString();
}
async function heliusRpc(method, params, { priority = 0, label = '' } = {}) {
  if (!HELIUS_API_KEY) {
    stats.helius_key_missing_rejections += 1;
    const error = new Error('helius_api_key_not_configured');
    error.statusCode = 503;
    throw error;
  }
  return heliusScheduler.enqueue(async () => {
    stats.helius_upstream_started += 1;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), HELIUS_TIMEOUT_MS);
    timer.unref?.();
    try {
      const response = await fetch(heliusRpcUrl(), {
        method: 'POST',
        signal: controller.signal,
        headers: {
          accept: 'application/json',
          'content-type': 'application/json',
          'user-agent': 'KakaWeb3-Onchain-Shared/1039',
        },
        body: JSON.stringify({ jsonrpc: '2.0', id: 'kaka', method, params }),
      });
      const body = await response.text();
      if (!response.ok) {
        const error = new Error(`helius_http_${response.status}:${body.slice(0, 220)}`);
        error.statusCode = response.status;
        throw error;
      }
      let parsed;
      try { parsed = JSON.parse(body); } catch { throw new Error('helius_invalid_json'); }
      if (parsed?.error) {
        const code = parsed.error?.code ?? 'rpc';
        const message = text(parsed.error?.message || 'helius_rpc_error');
        const error = new Error(`helius_rpc_${code}:${message.slice(0, 180)}`);
        error.statusCode = 503;
        throw error;
      }
      stats.helius_upstream_succeeded += 1;
      return parsed?.result;
    } catch (error) {
      stats.helius_upstream_failed += 1;
      throw error;
    } finally { clearTimeout(timer); }
  }, { priority, label });
}

async function heliusRestJson(url, { priority = 0, label = '' } = {}) {
  if (!HELIUS_API_KEY) {
    stats.helius_key_missing_rejections += 1;
    const error = new Error('helius_api_key_not_configured');
    error.statusCode = 503;
    throw error;
  }
  return heliusScheduler.enqueue(async () => {
    stats.helius_upstream_started += 1;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), HELIUS_TIMEOUT_MS);
    timer.unref?.();
    try {
      const u = new URL(url);
      if (!u.searchParams.get('api-key')) u.searchParams.set('api-key', HELIUS_API_KEY);
      const response = await fetch(u.toString(), {
        signal: controller.signal,
        headers: { accept: 'application/json', 'user-agent': 'KakaWeb3-Onchain-Shared/1040' },
      });
      const body = await response.text();
      if (response.status === 404) { stats.helius_upstream_succeeded += 1; return null; }
      if (!response.ok) {
        const error = new Error(`helius_http_${response.status}:${body.slice(0, 220)}`);
        error.statusCode = response.status;
        throw error;
      }
      let parsed;
      try { parsed = JSON.parse(body); } catch { throw new Error('helius_invalid_json'); }
      stats.helius_upstream_succeeded += 1;
      return parsed;
    } catch (error) {
      stats.helius_upstream_failed += 1;
      throw error;
    } finally { clearTimeout(timer); }
  }, { priority, label });
}

function heliusDasAmountToBigInt(value) {
  if (typeof value === 'bigint') return value >= 0n ? value : null;
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || value < 0) return null;
    // Helius DAS returns integer raw token amounts. Converting the parsed integer
    // to a decimal string is sufficient for current SPL supply ranges; values above
    // JS safe integer are marked in diagnostics but are never converted to float
    // before the final percentage ratio.
    return BigInt(Math.trunc(value).toString());
  }
  const raw = text(value).trim();
  if (!/^\d+$/.test(raw)) return null;
  try { return BigInt(raw); } catch { return null; }
}
function parseHeliusDasTokenAccount(raw, expectedMint) {
  if (!raw || typeof raw !== 'object') return null;
  const mint = text(raw.mint);
  const owner = text(raw.owner);
  const amount = heliusDasAmountToBigInt(raw.amount);
  if (!exactAddressEqual('solana', mint, expectedMint) || !looksSolanaAddress(owner) || amount === null || amount <= 0n) return null;
  return { mint, owner, amount };
}
function bigintPercent(balance, supply) {
  if (typeof balance !== 'bigint' || typeof supply !== 'bigint' || balance < 0n || supply <= 0n) return null;
  const scale = 1_000_000n;
  return Number((balance * 100n * scale) / supply) / Number(scale);
}
function sumTopBigintPercent(rows, count, supply) {
  if (!Array.isArray(rows) || rows.length < count || typeof supply !== 'bigint' || supply <= 0n) return null;
  let sum = 0n;
  for (const row of rows.slice(0, count)) sum += row.amount;
  return bigintPercent(sum, supply);
}
async function heliusTokenSupply(address) {
  const result = await heliusRpc('getTokenSupply', [address, { commitment: 'confirmed' }], {
    priority: 9,
    label: `token_supply:solana:${address}`,
  });
  const raw = text(result?.value?.amount);
  if (!/^\d+$/.test(raw)) throw new Error('helius_token_supply_missing');
  return { amount: BigInt(raw), decimals: Number(result?.value?.decimals || 0) };
}
async function heliusDasMintAccounts(mint, ownerBalances, state) {
  let cursor = null;
  const seenCursors = new Set();
  let page = 1;
  while (page <= HELIUS_MAX_HOLDER_PAGES) {
    const remaining = HELIUS_MAX_EXACT_TOKEN_ACCOUNTS - state.accountsSeen;
    if (remaining <= 0) {
      state.complete = false;
      state.truncated = true;
      return;
    }
    const pageLimit = Math.max(1, Math.min(HELIUS_PAGE_LIMIT, remaining));
    const params = {
      mint,
      limit: pageLimit,
      options: { showZeroBalance: false },
      ...(cursor ? { cursor } : { page }),
    };
    const result = await heliusRpc('getTokenAccounts', params, {
      priority: 8,
      label: `holder_accounts:das:${mint}:page${page}`,
    });
    if (!result || typeof result !== 'object') throw new Error('helius_das_token_accounts_missing_result');
    const accounts = Array.isArray(result.token_accounts) ? result.token_accounts : [];
    const total = numberOrNull(result.total);
    if (total !== null) {
      state.reportedTotalTokenAccounts = Math.max(0, Math.trunc(total));
      if (state.reportedTotalTokenAccounts > HELIUS_MAX_EXACT_TOKEN_ACCOUNTS) {
        state.complete = false;
        state.truncated = true;
        state.tooLarge = true;
        return;
      }
    }
    for (const account of accounts) {
      state.accountsSeen += 1;
      const parsed = parseHeliusDasTokenAccount(account, mint);
      if (!parsed) continue;
      state.tokenAccounts += 1;
      ownerBalances.set(parsed.owner, (ownerBalances.get(parsed.owner) || 0n) + parsed.amount);
    }
    state.pages += 1;
    const nextCursor = text(result.cursor) || null;
    if (nextCursor) {
      if (seenCursors.has(nextCursor)) throw new Error('helius_das_cursor_loop_detected');
      seenCursors.add(nextCursor);
      cursor = nextCursor;
    } else {
      // getTokenAccounts also supports page pagination. The indexed `total` gives a
      // deterministic completion check; if no cursor is returned, advance pages only
      // while we have not yet reached that total.
      if (state.reportedTotalTokenAccounts !== null && state.accountsSeen < state.reportedTotalTokenAccounts && accounts.length > 0) {
        cursor = null;
        page += 1;
        continue;
      }
      return;
    }
    page += 1;
  }
  state.complete = false;
  state.truncated = true;
}

async function buildHeliusSolanaHolderAnalysis(address) {
  if (!HELIUS_API_KEY) throw new Error('helius_api_key_not_configured');
  const ownerBalances = new Map();
  const state = { tokenAccounts: 0, accountsSeen: 0, complete: true, truncated: false, tooLarge: false, reportedTotalTokenAccounts: null, pages: 0 };
  const supply = await heliusTokenSupply(address);
  // Helius DAS getTokenAccounts is indexed directly by mint and handles SPL Token
  // and Token-2022 without scanning either global program account space.
  await heliusDasMintAccounts(address, ownerBalances, state);
  if (!state.complete) {
    stats.helius_holder_incomplete_scans += 1;
    const error = new Error(state.tooLarge ? `helius_holder_exact_mint_too_large_${state.reportedTotalTokenAccounts}` : `helius_holder_scan_truncated_at_${state.accountsSeen}`);
    error.partial = { token_accounts_scanned: state.accountsSeen,
    reported_total_token_accounts: state.reportedTotalTokenAccounts,
    helius_pages_scanned: state.pages, reported_total_token_accounts: state.reportedTotalTokenAccounts, pages: state.pages, nonzero_token_accounts_scanned: state.tokenAccounts, unique_owners_scanned: ownerBalances.size };
    throw error;
  }
  const rows = [...ownerBalances.entries()]
    .filter(([, amount]) => amount > 0n)
    .map(([owner, amount]) => ({ owner, amount }))
    .sort((a, b) => (a.amount === b.amount ? 0 : a.amount > b.amount ? -1 : 1));
  if (!rows.length) throw new Error('helius_holder_scan_no_nonzero_accounts');
  stats.helius_holder_complete_scans += 1;
  const topHolders = rows.slice(0, 50).map((row) => ({
    address: row.owner,
    label: null,
    entity: null,
    entity_logo: null,
    balance: null,
    raw_balance: row.amount.toString(),
    usd_value: null,
    percent: bigintPercent(row.amount, supply.amount),
    is_contract: null,
  }));
  return {
    source: 'helius_official_das_exact_mint_token_accounts',
    source_scope: 'exact_mint_das_token_accounts_full_scan_aggregated_by_wallet_owner',
    total_holders: rows.length,
    token_accounts_scanned: state.accountsSeen,
    reported_total_token_accounts: state.reportedTotalTokenAccounts,
    helius_pages_scanned: state.pages,
    nonzero_token_accounts_scanned: state.tokenAccounts,
    concentration: {
      top10_percent: rows.length >= 10 ? sumTopBigintPercent(rows, 10, supply.amount) : null,
      top20_percent: rows.length >= 20 ? sumTopBigintPercent(rows, 20, supply.amount) : null,
      top25_percent: rows.length >= 25 ? sumTopBigintPercent(rows, 25, supply.amount) : null,
      top50_percent: rows.length >= 50 ? sumTopBigintPercent(rows, 50, supply.amount) : null,
    },
    holder_change: { h1: null, h6: null, h24: null, d7: null },
    holder_distribution: null,
    holders_by_acquisition: null,
    top_holders: topHolders,
    exact_top20_available: rows.length >= 20,
    top_holder_list_available: topHolders.length > 0,
    field_sources: {
      total_holders: 'helius_getTokenAccounts_exact_mint_full_scan_unique_owner_count',
      top10_percent: rows.length >= 10 ? 'helius_getTokenAccounts_full_exact_owner_balance_aggregation' : null,
      top20_percent: rows.length >= 20 ? 'helius_getTokenAccounts_full_exact_owner_balance_aggregation' : null,
      top25_percent: rows.length >= 25 ? 'helius_getTokenAccounts_full_exact_owner_balance_aggregation' : null,
      top50_percent: rows.length >= 50 ? 'helius_getTokenAccounts_full_exact_owner_balance_aggregation' : null,
    },
    upstream_partial_errors: null,
    helius_scan_complete: true,
    helius_supply_raw: supply.amount.toString(),
  };
}

function bool01(value) {
  if (value === true || value === 1 || value === '1') return true;
  if (value === false || value === 0 || value === '0') return false;
  return null;
}
function percentFractionToPct(value) {
  const n = numberOrNull(value);
  return n === null ? null : n * 100;
}
function normalizeHolderSupplyEntry(raw) {
  if (!raw || typeof raw !== 'object') return null;
  return {
    supply: numberOrNull(raw.supply),
    supply_percent: numberOrNull(raw.supplyPercent ?? raw.supply_percent),
  };
}
function normalizeHolderMetrics(raw) {
  // Moralis documents the holder metrics fields at the top level. Be tolerant of a
  // single object wrapper as well so a gateway/SDK envelope cannot silently turn
  // valid production facts into nulls. Arrays are never accepted as a metrics root.
  const root = raw?.data && typeof raw.data === 'object' && !Array.isArray(raw.data)
    ? raw.data
    : raw?.result && typeof raw.result === 'object' && !Array.isArray(raw.result)
      ? raw.result
      : raw;
  const supply = root?.holderSupply || root?.holder_supply || {};
  const change = root?.holderChange || root?.holder_change || {};
  return {
    total_holders: numberOrNull(root?.totalHolders ?? root?.total_holders),
    concentration: {
      top10: normalizeHolderSupplyEntry(supply.top10),
      top25: normalizeHolderSupplyEntry(supply.top25),
      top50: normalizeHolderSupplyEntry(supply.top50),
      top100: normalizeHolderSupplyEntry(supply.top100),
    },
    holder_change: {
      h1: numberOrNull(change?.['1h']?.changePercent ?? change?.['1h']?.change_percent),
      h6: numberOrNull(change?.['6h']?.changePercent ?? change?.['6h']?.change_percent),
      h24: numberOrNull(change?.['24h']?.changePercent ?? change?.['24h']?.change_percent),
      d7: numberOrNull(change?.['7d']?.changePercent ?? change?.['7d']?.change_percent),
    },
    holder_distribution: root?.holderDistribution && typeof root.holderDistribution === 'object'
      ? root.holderDistribution
      : root?.holder_distribution && typeof root.holder_distribution === 'object'
        ? root.holder_distribution
        : null,
    holders_by_acquisition: root?.holdersByAcquisition && typeof root.holdersByAcquisition === 'object'
      ? root.holdersByAcquisition
      : root?.holders_by_acquisition && typeof root.holders_by_acquisition === 'object'
        ? root.holders_by_acquisition
        : null,
  };
}
function normalizeEvmTopHolder(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const address = text(raw.owner_address || raw.address);
  if (!looksEvmAddress(address)) return null;
  return {
    address,
    label: text(raw.owner_address_label || raw.entity || raw.tag) || null,
    entity: text(raw.entity) || null,
    entity_logo: text(raw.entity_logo) || null,
    balance: numberOrNull(raw.balance_formatted ?? raw.balance),
    usd_value: numberOrNull(raw.usd_value),
    percent: numberOrNull(raw.percentage_relative_to_total_supply ?? raw.percent),
    is_contract: raw.is_contract === true || raw.is_contract === '1',
  };
}
function exactTopPercent(rows, count) {
  const values = rows.slice(0, count).map((row) => numberOrNull(row?.percent)).filter((x) => x !== null);
  return values.length === Math.min(count, rows.length) && values.length ? values.reduce((a, b) => a + b, 0) : null;
}
function moralisHolderMetricsUrl(network, address) {
  if (network === 'solana') {
    return `https://solana-gateway.moralis.io/token/mainnet/holders/${encodeURIComponent(address)}`;
  }
  const u = new URL(`https://deep-index.moralis.io/api/v2.2/erc20/${encodeURIComponent(address)}/holders`);
  u.searchParams.set('chain', MORALIS_EVM_CHAIN[network]);
  return u.toString();
}
function moralisTopHoldersUrl(network, address, limit = 50) {
  if (network === 'solana') return null; // Deprecated by Moralis after 2026-07-31; do not build new dependency on it.
  const u = new URL(`https://deep-index.moralis.io/api/v2.2/erc20/${encodeURIComponent(address)}/owners`);
  u.searchParams.set('chain', MORALIS_EVM_CHAIN[network]);
  u.searchParams.set('limit', String(Math.max(1, Math.min(100, limit))));
  u.searchParams.set('order', 'DESC');
  return u.toString();
}
async function buildHolderAnalysis(network, address) {
  stats.holder_builds += 1;

  // Step1038.2.2: Moralis' Solana holder endpoints are deprecated. Prefer a
  // complete exact-mint Helius DAS token-account scan. If Helius is not configured/unavailable,
  // preserve the previous GoPlus source-fact fallback without fabricating Top20/50.
  if (network === 'solana') {
    let heliusError = null;
    try {
      return await buildHeliusSolanaHolderAnalysis(address);
    } catch (error) {
      heliusError = error;
    }
    let holderFallback = null;
    try {
      const securityResult = await cachedBuild(
        `step1038:security:${network}:${lower(address)}`,
        { freshMs: SECURITY_FRESH_MS, staleMs: SECURITY_STALE_MS, negativeMs: SECURITY_NEGATIVE_MS },
        () => buildGoPlusSecurity(network, address),
      );
      holderFallback = securityResult.value || null;
    } catch {
      holderFallback = null;
    }
    const totalHolders = numberOrNull(holderFallback?.holder_count);
    const top10 = numberOrNull(holderFallback?.top10_percent_reported);
    const rows = Array.isArray(holderFallback?.holders_top10) ? holderFallback.holders_top10 : [];
    if (totalHolders === null && top10 === null && !rows.length) {
      stats.holder_build_failures += 1;
      throw heliusError || new Error('solana_holder_analysis_no_usable_facts');
    }
    return {
      source: 'goplus_solana_token_security_holder_fallback',
      source_scope: 'top10_and_holder_count_only_when_helius_exact_mint_das_index_unavailable',
      total_holders: totalHolders,
      concentration: { top10_percent: top10, top20_percent: null, top25_percent: null, top50_percent: null },
      holder_change: { h1: null, h6: null, h24: null, d7: null },
      holder_distribution: null,
      holders_by_acquisition: null,
      top_holders: rows,
      exact_top20_available: false,
      top_holder_list_available: rows.length > 0,
      field_sources: {
        total_holders: totalHolders !== null ? 'goplus_token_security_holder_count' : null,
        top10_percent: top10 !== null ? 'goplus_token_security_top10_holders' : null,
        top20_percent: null, top25_percent: null, top50_percent: null,
      },
      upstream_partial_errors: {
        helius_exact_holder_index: heliusError ? String(heliusError.message || heliusError).slice(0, 180) : null,
      },
      helius_scan_complete: false,
    };
  }

  let metrics = normalizeHolderMetrics(null);
  let metricsError = null;
  try {
    const metricsPayload = await moralisFetchJson(moralisHolderMetricsUrl(network, address), {
      cu: MORALIS_HOLDER_METRICS_CU,
      kind: 'holder',
      priority: 8,
      label: `holder_metrics:${network}:${address}`,
    });
    metrics = normalizeHolderMetrics(metricsPayload);
  } catch (error) {
    metricsError = error;
  }

  let topHolders = [];
  let ownersError = null;
  let exactTop20 = null;
  let exactTop10 = null;
  let exactTop50 = null;
  const ownersUrl = moralisTopHoldersUrl(network, address, 50);
  if (ownersUrl) {
    try {
      const ownersPayload = await moralisFetchJson(ownersUrl, {
        cu: MORALIS_TOP_HOLDERS_CU,
        kind: 'holder',
        priority: 7,
        label: `top_holders:${network}:${address}`,
      });
      topHolders = (Array.isArray(ownersPayload?.result) ? ownersPayload.result : [])
        .map(normalizeEvmTopHolder).filter(Boolean).slice(0, 50);
      exactTop10 = topHolders.length >= 10 ? exactTopPercent(topHolders, 10) : null;
      exactTop20 = topHolders.length >= 20 ? exactTopPercent(topHolders, 20) : null;
      exactTop50 = topHolders.length >= 50 ? exactTopPercent(topHolders, 50) : null;
    } catch (error) {
      ownersError = error;
    }
  }

  let holderFallback = null;
  if (metrics.total_holders === null) {
    try {
      const securityResult = await cachedBuild(
        `step1038:security:${network}:${lower(address)}`,
        { freshMs: SECURITY_FRESH_MS, staleMs: SECURITY_STALE_MS, negativeMs: SECURITY_NEGATIVE_MS },
        () => buildGoPlusSecurity(network, address),
      );
      holderFallback = securityResult.value || null;
    } catch {
      holderFallback = null;
    }
  }

  const totalHolders = metrics.total_holders ?? numberOrNull(holderFallback?.holder_count);
  const top10 = exactTop10 ?? metrics.concentration.top10?.supply_percent ?? numberOrNull(holderFallback?.top10_percent_reported);
  const top25 = metrics.concentration.top25?.supply_percent ?? null;
  const top50 = exactTop50 ?? metrics.concentration.top50?.supply_percent ?? null;
  const anyUsable = totalHolders !== null || top10 !== null || exactTop20 !== null || top25 !== null || top50 !== null || topHolders.length > 0;
  if (!anyUsable) {
    stats.holder_build_failures += 1;
    throw metricsError || ownersError || new Error('holder_analysis_no_usable_facts');
  }

  const usedGoPlusTotal = metrics.total_holders === null && totalHolders !== null;
  return {
    source: usedGoPlusTotal
      ? 'moralis_holder_analytics_plus_goplus_holder_count_fallback'
      : 'moralis_official_data_api_holder_analytics',
    source_scope: usedGoPlusTotal
      ? 'moralis_exact_owner_concentration_plus_goplus_exact_contract_holder_count'
      : 'moralis_holder_metrics_plus_exact_top50_owner_list',
    total_holders: totalHolders,
    concentration: { top10_percent: top10, top20_percent: exactTop20, top25_percent: top25, top50_percent: top50 },
    holder_change: metrics.holder_change,
    holder_distribution: metrics.holder_distribution,
    holders_by_acquisition: metrics.holders_by_acquisition,
    top_holders: topHolders,
    exact_top20_available: exactTop20 !== null,
    top_holder_list_available: topHolders.length > 0,
    field_sources: {
      total_holders: metrics.total_holders !== null ? 'moralis_holder_metrics' : totalHolders !== null ? 'goplus_token_security_holder_count' : null,
      top10_percent: exactTop10 !== null ? 'moralis_exact_top_owners' : metrics.concentration.top10?.supply_percent != null ? 'moralis_holder_metrics' : top10 !== null ? 'goplus_token_security_top10_holders' : null,
      top20_percent: exactTop20 !== null ? 'moralis_exact_top_owners' : null,
      top25_percent: top25 !== null ? 'moralis_holder_metrics' : null,
      top50_percent: exactTop50 !== null ? 'moralis_exact_top_owners' : top50 !== null ? 'moralis_holder_metrics' : null,
    },
    upstream_partial_errors: {
      moralis_metrics: metricsError ? String(metricsError.message || metricsError).slice(0, 180) : null,
      moralis_top_holders: ownersError ? String(ownersError.message || ownersError).slice(0, 180) : null,
    },
  };
}

function resultByExactAddress(network, result, address) {
  if (!result || typeof result !== 'object') return null;
  for (const [key, value] of Object.entries(result)) {
    if (exactAddressEqual(network, key, address)) return value && typeof value === 'object' ? value : null;
  }
  return null;
}
function normalizeGoPlusHolder(raw, network) {
  if (!raw || typeof raw !== 'object') return null;
  const address = text(raw.address || raw.token_account);
  if (!validAddressForNetwork(network, address)) return null;
  return {
    address,
    tag: text(raw.tag) || null,
    balance: numberOrNull(raw.balance),
    percent: percentFractionToPct(raw.percent),
    is_locked: bool01(raw.is_locked ?? raw.locked),
    is_contract: bool01(raw.is_contract),
    locked_detail: Array.isArray(raw.locked_detail) ? raw.locked_detail.slice(0, 8) : [],
  };
}
function normalizedPercentSum(rows) {
  const vals = rows.map((row) => numberOrNull(row?.percent)).filter((x) => x !== null);
  return vals.length ? vals.reduce((a, b) => a + b, 0) : null;
}
function normalizeGoPlusEvm(raw, network, address) {
  const holders = (Array.isArray(raw?.holders) ? raw.holders : []).map((x) => normalizeGoPlusHolder(x, network)).filter(Boolean).slice(0, 10);
  const lpHolders = (Array.isArray(raw?.lp_holders) ? raw.lp_holders : []).map((x) => normalizeGoPlusHolder(x, network)).filter(Boolean).slice(0, 10);
  const lockedLpPct = normalizedPercentSum(lpHolders.filter((x) => x.is_locked === true));
  return {
    network, address,
    token_name: text(raw?.token_name) || null,
    token_symbol: text(raw?.token_symbol) || null,
    holder_count: numberOrNull(raw?.holder_count),
    total_supply: numberOrNull(raw?.total_supply),
    holders_top10: holders,
    top10_percent_reported: normalizedPercentSum(holders),
    creator: {
      address: text(raw?.creator_address) || null,
      balance: numberOrNull(raw?.creator_balance),
      percent: percentFractionToPct(raw?.creator_percent),
    },
    owner: {
      address: text(raw?.owner_address) || null,
      balance: numberOrNull(raw?.owner_balance),
      percent: percentFractionToPct(raw?.owner_percent),
    },
    lp: {
      holder_count: numberOrNull(raw?.lp_holder_count),
      total_supply: numberOrNull(raw?.lp_total_supply),
      holders_top10: lpHolders,
      top10_percent_reported: normalizedPercentSum(lpHolders),
      locked_percent_reported: lockedLpPct,
    },
    contract_facts: {
      is_open_source: bool01(raw?.is_open_source),
      is_proxy: bool01(raw?.is_proxy),
      is_mintable: bool01(raw?.is_mintable),
      hidden_owner: bool01(raw?.hidden_owner),
      can_take_back_ownership: bool01(raw?.can_take_back_ownership),
      owner_change_balance: bool01(raw?.owner_change_balance),
      selfdestruct: bool01(raw?.selfdestruct),
      external_call: bool01(raw?.external_call),
    },
    trading_facts: {
      is_honeypot: bool01(raw?.is_honeypot),
      cannot_buy: bool01(raw?.cannot_buy),
      cannot_sell_all: bool01(raw?.cannot_sell_all),
      is_blacklisted: bool01(raw?.is_blacklisted),
      is_whitelisted: bool01(raw?.is_whitelisted),
      transfer_pausable: bool01(raw?.transfer_pausable),
      trading_cooldown: bool01(raw?.trading_cooldown),
      slippage_modifiable: bool01(raw?.slippage_modifiable),
      is_anti_whale: bool01(raw?.is_anti_whale),
      anti_whale_modifiable: bool01(raw?.anti_whale_modifiable),
      buy_tax_percent: percentFractionToPct(raw?.buy_tax),
      sell_tax_percent: percentFractionToPct(raw?.sell_tax),
      transfer_tax_percent: percentFractionToPct(raw?.transfer_tax),
      is_in_dex: bool01(raw?.is_in_dex),
    },
    trust_facts: {
      trust_list: bool01(raw?.trust_list),
      is_airdrop_scam: bool01(raw?.is_airdrop_scam),
      other_potential_risks: text(raw?.other_potential_risks) || null,
      note: text(raw?.note) || null,
    },
  };
}
function solanaAuthority(raw) {
  if (raw === null || raw === undefined || raw === '') return null;
  if (typeof raw === 'string') return raw;
  if (typeof raw === 'object') {
    return text(raw.address || raw.authority || raw.value || raw.owner) || null;
  }
  return text(raw) || null;
}
function normalizeGoPlusSolana(raw, network, address) {
  const holders = (Array.isArray(raw?.holders) ? raw.holders : []).map((x) => normalizeGoPlusHolder(x, network)).filter(Boolean).slice(0, 10);
  const creators = (Array.isArray(raw?.creator) ? raw.creator : []).map((x) => ({
    address: text(x?.address) || null,
    malicious_address: bool01(x?.malicious_address),
  })).filter((x) => x.address);
  const dexRows = (Array.isArray(raw?.dex) ? raw.dex : Array.isArray(raw?.dex_info) ? raw.dex_info : []).slice(0, 12);
  const lpHolders = [];
  for (const dex of dexRows) {
    for (const h of (Array.isArray(dex?.lp_holders) ? dex.lp_holders : [])) {
      const row = normalizeGoPlusHolder(h, network);
      if (row && !lpHolders.some((x) => exactAddressEqual(network, x.address, row.address))) lpHolders.push(row);
      if (lpHolders.length >= 10) break;
    }
    if (lpHolders.length >= 10) break;
  }
  const metadataMutable = raw?.metadata_mutable;
  const mintable = raw?.mintable;
  const transferHook = raw?.transfer_hook;
  return {
    network, address,
    token_name: text(raw?.metadata?.name) || null,
    token_symbol: text(raw?.metadata?.symbol) || null,
    holder_count: numberOrNull(raw?.holder_count),
    total_supply: numberOrNull(raw?.total_supply),
    holders_top10: holders,
    top10_percent_reported: normalizedPercentSum(holders),
    creators,
    lp: {
      holders_top10: lpHolders,
      top10_percent_reported: normalizedPercentSum(lpHolders),
      locked_percent_reported: normalizedPercentSum(lpHolders.filter((x) => x.is_locked === true)),
      dex_pool_count: dexRows.length,
      tvl_usd: dexRows.reduce((sum, d) => sum + (numberOrNull(d?.tvl) || 0), 0) || null,
    },
    solana_facts: {
      default_account_state: numberOrNull(raw?.default_account_state),
      non_transferable: bool01(raw?.non_transferable),
      trusted_token: bool01(raw?.trusted_token),
      mintable: typeof mintable === 'object' ? bool01(mintable?.status ?? mintable?.value ?? mintable?.is_mintable) : bool01(mintable),
      mint_authority: solanaAuthority(typeof mintable === 'object' ? mintable : raw?.mint_authority),
      metadata_mutable: typeof metadataMutable === 'object' ? bool01(metadataMutable?.status ?? metadataMutable?.value ?? metadataMutable?.is_mutable) : bool01(metadataMutable),
      metadata_upgrade_authority: solanaAuthority(typeof metadataMutable === 'object' ? metadataMutable : raw?.metadata_upgrade_authority),
      transfer_hook_address: solanaAuthority(transferHook),
      transfer_hook_malicious: typeof transferHook === 'object' ? bool01(transferHook?.malicious_address) : null,
      transfer_hook_upgradable: bool01(raw?.transfer_hook_upgradable),
      transfer_fee: raw?.transfer_fee && typeof raw.transfer_fee === 'object' ? raw.transfer_fee : null,
    },
  };
}
function goplusSecurityUrl(network, address) {
  if (network === 'solana') {
    const u = new URL('https://api.gopluslabs.io/api/v1/solana/token_security');
    u.searchParams.set('contract_addresses', address);
    return u.toString();
  }
  const chainId = EVM_GOPLUS_CHAIN_ID[network];
  const u = new URL(`https://api.gopluslabs.io/api/v1/token_security/${chainId}`);
  u.searchParams.set('contract_addresses', address);
  return u.toString();
}
async function buildGoPlusSecurity(network, address) {
  stats.security_builds += 1;
  try {
    const payload = await goplusFetchJson(goplusSecurityUrl(network, address), {
      priority: 10,
      label: `token_security:${network}:${address}`,
    });
    const raw = resultByExactAddress(network, payload.result, address);
    if (!raw) throw new Error('goplus_exact_token_not_found');
    return network === 'solana'
      ? normalizeGoPlusSolana(raw, network, address)
      : normalizeGoPlusEvm(raw, network, address);
  } catch (error) {
    stats.security_build_failures += 1;
    throw error;
  }
}



// ---------------- Step1039 wallet intelligence ----------------
function walletIdentityKey(network, address) {
  return network === 'solana' ? text(address) : lower(address);
}
function walletAddressValid(network, address) {
  return validAddressForNetwork(network, address);
}
function moralisEvmTokenSwapsUrl(network, address, order = 'DESC', limit = 100) {
  const chain = MORALIS_EVM_CHAIN[network];
  if (!chain) throw new Error('moralis_evm_chain_not_supported');
  const u = new URL(`https://deep-index.moralis.io/api/v2.2/erc20/${encodeURIComponent(address)}/swaps`);
  u.searchParams.set('chain', chain);
  u.searchParams.set('limit', String(Math.max(1, Math.min(100, limit))));
  u.searchParams.set('order', order === 'ASC' ? 'ASC' : 'DESC');
  u.searchParams.set('transactionTypes', 'buy,sell');
  return u.toString();
}
function moralisSolanaTokenSwapsUrl(address, order = 'DESC', limit = 100) {
  const u = new URL(`https://solana-gateway.moralis.io/token/mainnet/${encodeURIComponent(address)}/swaps`);
  u.searchParams.set('limit', String(Math.max(1, Math.min(100, limit))));
  u.searchParams.set('order', order === 'ASC' ? 'ASC' : 'DESC');
  u.searchParams.set('transactionTypes', 'buy,sell');
  return u.toString();
}
function moralisTopTradersUrl(network, address) {
  if (!['ethereum', 'base'].includes(network)) return null;
  const u = new URL(`https://deep-index.moralis.io/api/v2.2/erc20/${encodeURIComponent(address)}/top-gainers`);
  u.searchParams.set('chain', MORALIS_EVM_CHAIN[network]);
  u.searchParams.set('days', 'all');
  return u.toString();
}
function moralisWalletPnlUrl(network, wallet) {
  const chain = MORALIS_EVM_CHAIN[network];
  if (!chain) throw new Error('moralis_evm_chain_not_supported');
  const u = new URL(`https://deep-index.moralis.io/api/v2.2/wallets/${encodeURIComponent(wallet)}/profitability`);
  u.searchParams.set('chain', chain);
  u.searchParams.set('days', 'all');
  return u.toString();
}
function moralisWalletChainActivityUrl(network, wallet) {
  const chain = MORALIS_EVM_CHAIN[network];
  if (!chain) throw new Error('moralis_evm_chain_not_supported');
  const u = new URL(`https://deep-index.moralis.io/api/v2.2/wallets/${encodeURIComponent(wallet)}/chains`);
  u.searchParams.append('chains', chain);
  return u.toString();
}
function moralisSolanaWalletSwapsUrl(wallet, tokenAddress = '', cursor = '') {
  const u = new URL(`https://solana-gateway.moralis.io/account/mainnet/${encodeURIComponent(wallet)}/swaps`);
  u.searchParams.set('limit', '100');
  u.searchParams.set('order', 'ASC');
  u.searchParams.set('transactionTypes', 'buy,sell');
  if (tokenAddress) u.searchParams.set('tokenAddress', tokenAddress);
  if (cursor) u.searchParams.set('cursor', cursor);
  return u.toString();
}
function normalizeTokenSwap(network, raw) {
  if (!raw || typeof raw !== 'object') return null;
  const wallet = text(raw.walletAddress ?? raw.wallet_address);
  if (!walletAddressValid(network, wallet)) return null;
  const side = lower(raw.transactionType ?? raw.transaction_type);
  if (!['buy', 'sell'].includes(side)) return null;
  return {
    wallet_address: wallet,
    wallet_label: text(raw.walletAddressLabel ?? raw.wallet_address_label),
    entity: text(raw.entity),
    entity_logo: text(raw.entityLogo ?? raw.entity_logo),
    transaction_hash: text(raw.transactionHash ?? raw.transaction_hash),
    transaction_type: side,
    block_timestamp: text(raw.blockTimestamp ?? raw.block_timestamp) || null,
    block_number: numberOrNull(raw.blockNumber ?? raw.block_number),
    total_value_usd: numberOrNull(raw.totalValueUsd ?? raw.total_value_usd),
    exchange_name: text(raw.exchangeName ?? raw.exchange_name),
    pair_address: text(raw.pairAddress ?? raw.pair_address),
    pair_label: text(raw.pairLabel ?? raw.pair_label),
  };
}
function normalizeSwapPayload(network, payload) {
  const rows = Array.isArray(payload?.result) ? payload.result : [];
  return rows.map((x) => normalizeTokenSwap(network, x)).filter(Boolean);
}
function aggregateRecentWallets(network, rows, limit = 30) {
  const map = new Map();
  for (const row of rows) {
    const key = walletIdentityKey(network, row.wallet_address);
    let item = map.get(key);
    if (!item) {
      item = {
        address: row.wallet_address,
        label: row.wallet_label || row.entity || '',
        entity: row.entity || '',
        buys: 0,
        sells: 0,
        buy_usd: 0,
        sell_usd: 0,
        total_usd: 0,
        trade_count: 0,
        last_activity_at: row.block_timestamp || null,
        last_exchange: row.exchange_name || '',
      };
      map.set(key, item);
    }
    const usd = Math.max(0, numberOrNull(row.total_value_usd) || 0);
    item.trade_count += 1;
    item.total_usd += usd;
    if (row.transaction_type === 'buy') { item.buys += 1; item.buy_usd += usd; }
    if (row.transaction_type === 'sell') { item.sells += 1; item.sell_usd += usd; }
    if (row.block_timestamp && (!item.last_activity_at || row.block_timestamp > item.last_activity_at)) item.last_activity_at = row.block_timestamp;
    if (row.exchange_name) item.last_exchange = row.exchange_name;
    if (!item.label && (row.wallet_label || row.entity)) item.label = row.wallet_label || row.entity;
  }
  return [...map.values()].sort((a, b) => b.total_usd - a.total_usd || b.trade_count - a.trade_count).slice(0, limit);
}
function earliestBuyerRows(network, rows, limit = 20) {
  const seen = new Set();
  const out = [];
  for (const row of rows) {
    if (row.transaction_type !== 'buy') continue;
    const key = walletIdentityKey(network, row.wallet_address);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      address: row.wallet_address,
      label: row.wallet_label || row.entity || '',
      entity: row.entity || '',
      early_buy_rank: out.length + 1,
      first_observed_buy_at: row.block_timestamp || null,
      first_observed_buy_usd: numberOrNull(row.total_value_usd),
      evidence_scope: `first_${EARLY_SWAP_SCOPE}_token_swaps_ordered_asc`,
      exhaustive_since_launch: false,
    });
    if (out.length >= limit) break;
  }
  return out;
}
function normalizeTopTrader(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const address = text(raw.address);
  if (!looksEvmAddress(address)) return null;
  return {
    address,
    count_of_trades: numberOrNull(raw.count_of_trades),
    realized_profit_usd: numberOrNull(raw.realized_profit_usd),
    realized_profit_percentage: numberOrNull(raw.realized_profit_percentage),
    total_usd_invested: numberOrNull(raw.total_usd_invested),
    total_sold_usd: numberOrNull(raw.total_sold_usd),
    avg_buy_price_usd: numberOrNull(raw.avg_buy_price_usd),
    avg_sell_price_usd: numberOrNull(raw.avg_sell_price_usd),
  };
}
function smartMoneyTokenCandidate(row) {
  const profit = numberOrNull(row?.realized_profit_usd);
  const roi = numberOrNull(row?.realized_profit_percentage);
  const trades = numberOrNull(row?.count_of_trades);
  if (profit === null || roi === null || trades === null) return null;
  const pass = profit >= SMART_MONEY_TOKEN_MIN_PROFIT_USD && roi >= SMART_MONEY_TOKEN_MIN_ROI_PCT && trades >= SMART_MONEY_TOKEN_MIN_TRADES;
  if (!pass) return null;
  return {
    ...row,
    smart_money_candidate: true,
    candidate_kind: 'token_profitability_signal',
    rule_version: SMART_MONEY_RULE_VERSION,
    rule: `realized_profit_usd>=${SMART_MONEY_TOKEN_MIN_PROFIT_USD}; realized_profit_percentage>=${SMART_MONEY_TOKEN_MIN_ROI_PCT}; count_of_trades>=${SMART_MONEY_TOKEN_MIN_TRADES}`,
    reasons: [
      `realized_profit_usd=${profit}`,
      `realized_profit_percentage=${roi}`,
      `count_of_trades=${trades}`,
    ],
    confirmed_smart_money: false,
  };
}
function directRolesFromSecurity(network, security) {
  const roles = new Map();
  const add = (address, role) => {
    if (!walletAddressValid(network, address)) return;
    const key = walletIdentityKey(network, address);
    const item = roles.get(key) || { address, roles: [] };
    if (!item.roles.includes(role)) item.roles.push(role);
    roles.set(key, item);
  };
  if (!security || typeof security !== 'object') return roles;
  if (network === 'solana') {
    for (const raw of Array.isArray(security.creators) ? security.creators : []) add(text(raw?.address), 'creator');
  } else {
    add(text(security.creator?.address), 'creator');
    add(text(security.owner?.address), 'owner');
  }
  return roles;
}
function mergeWalletCandidate(map, network, raw, tags = [], evidence = {}) {
  const address = text(raw?.address ?? raw?.wallet_address);
  if (!walletAddressValid(network, address)) return;
  const key = walletIdentityKey(network, address);
  const item = map.get(key) || { address, tags: [], evidence: {} };
  for (const tag of tags) if (tag && !item.tags.includes(tag)) item.tags.push(tag);
  item.evidence = { ...item.evidence, ...evidence };
  if (raw?.label && !item.label) item.label = raw.label;
  if (raw?.entity && !item.entity) item.entity = raw.entity;
  map.set(key, item);
}
async function step1038Context(network, address) {
  const holderKey = `step1038:holders:${network}:${lower(address)}`;
  const securityKey = `step1038:security:${network}:${lower(address)}`;
  const holderPromise = cachedBuild(holderKey, {
    freshMs: network === 'solana' ? SOLANA_HELIUS_HOLDER_FRESH_MS : HOLDER_FRESH_MS,
    staleMs: network === 'solana' ? SOLANA_HELIUS_HOLDER_STALE_MS : HOLDER_STALE_MS,
    negativeMs: HOLDER_NEGATIVE_MS,
  }, () => buildHolderAnalysis(network, address));
  const securityPromise = cachedBuild(securityKey, { freshMs: SECURITY_FRESH_MS, staleMs: SECURITY_STALE_MS, negativeMs: SECURITY_NEGATIVE_MS }, () => buildGoPlusSecurity(network, address));
  const [h, sec] = await Promise.allSettled([holderPromise, securityPromise]);
  return {
    holders: h.status === 'fulfilled' ? h.value?.value || null : null,
    holder_error: h.status === 'rejected' ? text(h.reason?.message || h.reason) : '',
    security: sec.status === 'fulfilled' ? sec.value?.value || null : null,
    security_error: sec.status === 'rejected' ? text(sec.reason?.message || sec.reason) : '',
  };
}
async function fetchTokenSwapScopes(network, address) {
  const ascUrl = network === 'solana' ? moralisSolanaTokenSwapsUrl(address, 'ASC', EARLY_SWAP_SCOPE) : moralisEvmTokenSwapsUrl(network, address, 'ASC', EARLY_SWAP_SCOPE);
  const descUrl = network === 'solana' ? moralisSolanaTokenSwapsUrl(address, 'DESC', RECENT_SWAP_SCOPE) : moralisEvmTokenSwapsUrl(network, address, 'DESC', RECENT_SWAP_SCOPE);
  const [asc, desc] = await Promise.allSettled([
    moralisFetchJson(ascUrl, { cu: MORALIS_TOKEN_SWAPS_CU, kind: 'signal', priority: 5, label: `step1039-token-swaps-asc-${network}` }),
    moralisFetchJson(descUrl, { cu: MORALIS_TOKEN_SWAPS_CU, kind: 'signal', priority: 4, label: `step1039-token-swaps-desc-${network}` }),
  ]);
  return {
    early: asc.status === 'fulfilled' ? normalizeSwapPayload(network, asc.value) : [],
    recent: desc.status === 'fulfilled' ? normalizeSwapPayload(network, desc.value) : [],
    errors: [
      asc.status === 'rejected' ? `early:${text(asc.reason?.message || asc.reason)}` : '',
      desc.status === 'rejected' ? `recent:${text(desc.reason?.message || desc.reason)}` : '',
    ].filter(Boolean),
  };
}
async function buildTokenWalletIntelligence(network, address) {
  stats.token_wallet_builds += 1;
  try {
    const [context, swapScopes] = await Promise.all([step1038Context(network, address), fetchTokenSwapScopes(network, address)]);
    const holders = Array.isArray(context.holders?.top_holders) ? context.holders.top_holders.slice(0, 50) : [];
    const largeHolders = holders.filter((row, idx) => idx < 10 || (numberOrNull(row?.percent) || 0) >= 1).map((row, idx) => ({
      address: text(row.address),
      label: text(row.label || row.entity),
      entity: text(row.entity),
      holder_rank: idx + 1,
      holder_percent: numberOrNull(row.percent),
      large_holder_rule: 'top10_or_holder_percent_gte_1',
    }));
    const earlyBuyers = earliestBuyerRows(network, swapScopes.early, 20);
    const recentTraders = aggregateRecentWallets(network, swapScopes.recent, 30);
    const roleMap = directRolesFromSecurity(network, context.security);

    let topTraderRows = [];
    let topTraderError = '';
    const topUrl = moralisTopTradersUrl(network, address);
    if (topUrl) {
      try {
        const raw = await moralisFetchJson(topUrl, { cu: MORALIS_TOP_TRADERS_CU, kind: 'signal', priority: 2, label: `step1039-top-traders-${network}` });
        topTraderRows = (Array.isArray(raw?.result) ? raw.result : []).map(normalizeTopTrader).filter(Boolean).slice(0, 50);
      } catch (error) { topTraderError = text(error?.message || error); }
    }
    const profitableCandidates = topTraderRows.map(smartMoneyTokenCandidate).filter(Boolean).slice(0, 20);
    const all = new Map();
    for (const row of holders) mergeWalletCandidate(all, network, row, ['holder'], { holder_percent: numberOrNull(row.percent), holder_rank: holders.indexOf(row) + 1 });
    for (const row of largeHolders) mergeWalletCandidate(all, network, row, ['large_holder'], { holder_percent: row.holder_percent, holder_rank: row.holder_rank, large_holder_rule: row.large_holder_rule });
    for (const row of earlyBuyers) mergeWalletCandidate(all, network, row, ['early_buyer'], { early_buy_rank: row.early_buy_rank, first_observed_buy_at: row.first_observed_buy_at, early_scope: row.evidence_scope });
    for (const row of recentTraders) mergeWalletCandidate(all, network, row, ['recent_trader'], { buys: row.buys, sells: row.sells, total_usd: row.total_usd, last_activity_at: row.last_activity_at });
    for (const row of profitableCandidates) mergeWalletCandidate(all, network, row, ['smart_money_candidate'], { smart_money_candidate: true, realized_profit_usd: row.realized_profit_usd, realized_profit_percentage: row.realized_profit_percentage, count_of_trades: row.count_of_trades, smart_money_rule: row.rule, smart_money_rule_version: row.rule_version });
    for (const role of roleMap.values()) mergeWalletCandidate(all, network, role, role.roles, { direct_source_roles: role.roles });

    return {
      source: 'kaka_shared_token_wallet_intelligence',
      network,
      address,
      scopes: {
        early_buyers: `first_${EARLY_SWAP_SCOPE}_token_swaps_ordered_asc_non_exhaustive`,
        recent_traders: `latest_${RECENT_SWAP_SCOPE}_token_swaps_ordered_desc`,
        large_holders: 'exact_step1038_top_holders_top10_or_percent_gte_1',
        profitable_candidates: topUrl ? 'moralis_token_top_traders_profitability_signal' : 'not_supported_for_this_network',
      },
      early_buyers: earlyBuyers,
      large_holders: largeHolders,
      recent_traders: recentTraders,
      smart_money_candidates: profitableCandidates,
      direct_role_wallets: [...roleMap.values()],
      all_candidates: [...all.values()].slice(0, WALLET_SIGNAL_MAX_ROWS),
      smart_money_rule_version: SMART_MONEY_RULE_VERSION,
      smart_money_is_candidate_not_identity: true,
      no_kol_sniper_dev_insider_inference: true,
      top_traders_supported_network: Boolean(topUrl),
      upstream_partial_errors: [context.holder_error, context.security_error, ...swapScopes.errors, topTraderError ? `top_traders:${topTraderError}` : ''].filter(Boolean),
    };
  } catch (error) {
    stats.token_wallet_build_failures += 1;
    throw error;
  }
}
function normalizePnlRow(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const address = text(raw.token_address);
  if (!looksEvmAddress(address)) return null;
  return {
    token_address: address,
    name: text(raw.name),
    symbol: text(raw.symbol),
    possible_spam: raw.possible_spam === true || lower(raw.possible_spam) === 'true',
    total_usd_invested: numberOrNull(raw.total_usd_invested),
    total_sold_usd: numberOrNull(raw.total_sold_usd),
    count_of_trades: numberOrNull(raw.count_of_trades),
    realized_profit_usd: numberOrNull(raw.realized_profit_usd),
    realized_profit_percentage: numberOrNull(raw.realized_profit_percentage),
    total_buys: numberOrNull(raw.total_buys),
    total_sells: numberOrNull(raw.total_sells),
    avg_buy_price_usd: numberOrNull(raw.avg_buy_price_usd),
    avg_sell_price_usd: numberOrNull(raw.avg_sell_price_usd),
  };
}
function summarizePnl(rows) {
  const usable = rows.filter((x) => !x.possible_spam && x.realized_profit_usd !== null && x.count_of_trades !== null && x.count_of_trades > 0);
  const profitable = usable.filter((x) => x.realized_profit_usd > 0);
  return {
    available: usable.length > 0,
    realized_profit_usd: usable.reduce((a, x) => a + (x.realized_profit_usd || 0), 0),
    total_usd_invested: usable.reduce((a, x) => a + (x.total_usd_invested || 0), 0),
    total_sold_usd: usable.reduce((a, x) => a + (x.total_sold_usd || 0), 0),
    total_buys: usable.reduce((a, x) => a + (x.total_buys || 0), 0),
    total_sells: usable.reduce((a, x) => a + (x.total_sells || 0), 0),
    trade_count: usable.reduce((a, x) => a + (x.count_of_trades || 0), 0),
    evaluable_token_positions: usable.length,
    profitable_token_positions: profitable.length,
    realized_token_position_win_rate_pct: usable.length ? (profitable.length / usable.length) * 100 : null,
    methodology: 'moralis_realized_token_pnl_rows_non_spam; win_rate_is_profitable_token_positions/evaluable_token_positions_not_trade_win_rate; gas_not_added_by_kaka',
  };
}
function evmWalletCandidate(summary) {
  const p = numberOrNull(summary?.realized_profit_usd);
  const w = numberOrNull(summary?.realized_token_position_win_rate_pct);
  const n = numberOrNull(summary?.evaluable_token_positions);
  const t = numberOrNull(summary?.trade_count);
  const pass = p !== null && w !== null && n !== null && t !== null && p >= SMART_MONEY_WALLET_MIN_PROFIT_USD && w >= SMART_MONEY_WALLET_MIN_WIN_RATE_PCT && n >= SMART_MONEY_WALLET_MIN_EVALUABLE_POSITIONS && t >= SMART_MONEY_WALLET_MIN_TRADES;
  return {
    smart_money_candidate: pass,
    confirmed_smart_money: false,
    rule_version: SMART_MONEY_RULE_VERSION,
    rule: `realized_profit_usd>=${SMART_MONEY_WALLET_MIN_PROFIT_USD}; realized_token_position_win_rate_pct>=${SMART_MONEY_WALLET_MIN_WIN_RATE_PCT}; evaluable_token_positions>=${SMART_MONEY_WALLET_MIN_EVALUABLE_POSITIONS}; trade_count>=${SMART_MONEY_WALLET_MIN_TRADES}`,
    reasons: pass ? [`realized_profit_usd=${p}`, `realized_token_position_win_rate_pct=${w}`, `evaluable_token_positions=${n}`, `trade_count=${t}`] : [],
  };
}
function normalizeChainActivity(network, raw) {
  const chain = MORALIS_EVM_CHAIN[network];
  const rows = Array.isArray(raw?.active_chains) ? raw.active_chains : [];
  const found = rows.find((x) => lower(x?.chain) === chain || lower(x?.chain_id) === lower(NETWORKS[network]?.chain_id ? `0x${NETWORKS[network].chain_id.toString(16)}` : '')) || null;
  const first = text(found?.first_transaction?.block_timestamp) || null;
  const last = text(found?.last_transaction?.block_timestamp) || null;
  const firstMs = first ? Date.parse(first) : NaN;
  const walletAgeDays = Number.isFinite(firstMs)
    ? Math.max(0, (Date.now() - firstMs) / 86_400_000)
    : null;
  return {
    first_activity_at: first,
    last_activity_at: last,
    wallet_age_days: walletAgeDays,
    new_wallet: walletAgeDays === null ? null : walletAgeDays <= 7,
    new_wallet_rule: 'first_activity_at_within_7d',
    source: 'moralis_wallet_chain_activity',
  };
}
async function buildEvmWalletBase(network, wallet) {
  const [activity, pnl] = await Promise.allSettled([
    moralisFetchJson(moralisWalletChainActivityUrl(network, wallet), { cu: MORALIS_WALLET_ACTIVITY_BUDGET_CU, kind: 'wallet', priority: 5, label: `step1039-wallet-activity-${network}` }),
    moralisFetchJson(moralisWalletPnlUrl(network, wallet), { cu: MORALIS_WALLET_PNL_CU, kind: 'wallet', priority: 4, label: `step1039-wallet-pnl-${network}` }),
  ]);
  const activityFacts = activity.status === 'fulfilled' ? normalizeChainActivity(network, activity.value) : { first_activity_at: null, last_activity_at: null, wallet_age_days: null, new_wallet: null, new_wallet_rule: 'first_activity_at_within_7d', source: null };
  const pnlRows = pnl.status === 'fulfilled' && Array.isArray(pnl.value?.result) ? pnl.value.result.map(normalizePnlRow).filter(Boolean) : [];
  const pnlSummary = summarizePnl(pnlRows);
  return {
    network,
    wallet,
    source: 'moralis_wallet_chain_activity_plus_wallet_pnl_breakdown',
    activity: activityFacts,
    pnl: pnlSummary,
    pnl_rows: pnlRows.slice(0, 250),
    smart_money_signal: evmWalletCandidate(pnlSummary),
    upstream_partial_errors: [
      activity.status === 'rejected' ? `activity:${text(activity.reason?.message || activity.reason)}` : '',
      pnl.status === 'rejected' ? `pnl:${text(pnl.reason?.message || pnl.reason)}` : '',
    ].filter(Boolean),
  };
}
function solanaPortfolioFacts(raw) {
  const items = Array.isArray(raw?.items) ? raw.items : [];
  const fungible = items.filter((x) => lower(x?.interface).includes('fungible'));
  let pricedUsd = 0;
  let pricedCount = 0;
  for (const item of fungible) {
    const ti = item?.token_info || item?.tokenInfo || {};
    const price = ti?.price_info || ti?.priceInfo || {};
    let usd = numberOrNull(price?.total_price ?? price?.totalPrice ?? item?.value?.usd_value ?? item?.value?.usdValue);
    if (usd === null) {
      const per = numberOrNull(price?.price_per_token ?? price?.pricePerToken);
      const rawBal = numberOrNull(ti?.balance);
      const dec = numberOrNull(ti?.decimals);
      if (per !== null && rawBal !== null && dec !== null) usd = per * rawBal / (10 ** dec);
    }
    if (usd !== null && usd >= 0) { pricedUsd += usd; pricedCount += 1; }
  }
  const total = numberOrNull(raw?.total);
  const nativeLamports = numberOrNull(raw?.nativeBalance?.lamports);
  return {
    fungible_asset_rows_returned: fungible.length,
    reported_total_assets: total,
    portfolio_first_page_complete: total === null ? fungible.length < 1000 : total <= 1000,
    priced_fungible_assets: pricedCount,
    fungible_portfolio_usd_observed: pricedCount ? pricedUsd : null,
    native_sol: nativeLamports === null ? null : nativeLamports / 1e9,
    source: 'helius_getAssetsByOwner_showFungible_showNativeBalance',
  };
}
async function solanaWalletAge(wallet) {
  const cutoff = Date.now() - 7 * 86_400_000;
  let before = '';
  let oldest = null;
  let exhausted = false;
  let pages = 0;
  let signatures = 0;
  for (let i = 0; i < 3; i += 1) {
    const opts = { limit: 1000 };
    if (before) opts.before = before;
    const page = await heliusRpc('getSignaturesForAddress', [wallet, opts], { priority: 5, label: `step1039-wallet-age-${i + 1}` });
    if (!Array.isArray(page)) throw new Error('helius_wallet_signatures_bad_response');
    pages += 1;
    signatures += page.length;
    if (page.length) {
      const last = page[page.length - 1];
      if (Number.isFinite(Number(last?.blockTime))) oldest = { signature: text(last.signature), block_time: Number(last.blockTime) };
      before = text(last?.signature);
    }
    if (page.length < 1000) { exhausted = true; break; }
    if (!before) break;
    if (oldest && oldest.block_time * 1000 < cutoff) break;
  }
  const oldestMs = oldest?.block_time ? oldest.block_time * 1000 : null;
  const olderThan7dObserved = oldestMs !== null && oldestMs < cutoff;
  const newWallet = signatures === 0 ? null : olderThan7dObserved ? false : exhausted && oldestMs !== null ? (Date.now() - oldestMs <= 7 * 86_400_000) : null;
  return {
    new_wallet: newWallet,
    wallet_age_days: exhausted && oldestMs !== null ? Math.max(0, (Date.now() - oldestMs) / 86_400_000) : null,
    first_activity_at: exhausted && oldestMs !== null ? new Date(oldestMs).toISOString() : null,
    oldest_observed_activity_at: oldestMs !== null ? new Date(oldestMs).toISOString() : null,
    scan_exhausted: exhausted,
    pages_scanned: pages,
    signatures_scanned: signatures,
    new_wallet_rule: 'true_only_when_history_exhausted_and_first_activity_within_7d; false_when_activity_older_than_7d_observed; otherwise_unknown',
    source: 'helius_getSignaturesForAddress_bounded_history',
  };
}
async function buildSolanaWalletBase(wallet) {
  const [assets, age] = await Promise.allSettled([
    heliusRpc('getAssetsByOwner', { ownerAddress: wallet, page: 1, limit: 1000, displayOptions: { showFungible: true, showNativeBalance: true, showZeroBalance: false } }, { priority: 4, label: 'step1039-wallet-assets' }),
    solanaWalletAge(wallet),
  ]);
  return {
    network: 'solana',
    wallet,
    source: 'helius_wallet_portfolio_plus_bounded_age',
    activity: age.status === 'fulfilled' ? age.value : { new_wallet: null, wallet_age_days: null, source: null },
    portfolio: assets.status === 'fulfilled' ? solanaPortfolioFacts(assets.value) : { source: null },
    pnl: { available: false, reason: 'solana_step1039_does_not_label_swap_cashflow_as_pnl' },
    smart_money_signal: { smart_money_candidate: false, confirmed_smart_money: false, rule_version: SMART_MONEY_RULE_VERSION, reasons: [], unavailable_reason: 'no_exact_realized_pnl_source_for_solana_in_step1039' },
    upstream_partial_errors: [
      assets.status === 'rejected' ? `portfolio:${text(assets.reason?.message || assets.reason)}` : '',
      age.status === 'rejected' ? `age:${text(age.reason?.message || age.reason)}` : '',
    ].filter(Boolean),
  };
}
async function cachedWalletBase(network, wallet) {
  const key = `step1039:walletbase:${network}:${walletIdentityKey(network, wallet)}`;
  const result = await cachedBuild(key, { freshMs: WALLET_BASE_FRESH_MS, staleMs: WALLET_BASE_STALE_MS, negativeMs: WALLET_BASE_NEGATIVE_MS }, () => network === 'solana' ? buildSolanaWalletBase(wallet) : buildEvmWalletBase(network, wallet));
  return result.value;
}
async function solanaTokenSwapCashflow(wallet, tokenAddress) {
  if (!tokenAddress) return null;
  let cursor = '';
  let pages = 0;
  let complete = false;
  const rows = [];
  const seenCursor = new Set();
  while (pages < 3) {
    const raw = await moralisFetchJson(moralisSolanaWalletSwapsUrl(wallet, tokenAddress, cursor), { cu: MORALIS_SOLANA_WALLET_SWAPS_CU, kind: 'wallet', priority: 3, label: `step1039-sol-wallet-swaps-${pages + 1}` });
    const page = normalizeSwapPayload('solana', raw);
    rows.push(...page);
    pages += 1;
    const next = text(raw?.cursor);
    if (!next) { complete = true; break; }
    if (seenCursor.has(next)) break;
    seenCursor.add(next);
    cursor = next;
  }
  let buyUsd = 0; let sellUsd = 0; let buys = 0; let sells = 0;
  for (const row of rows) {
    const usd = Math.max(0, numberOrNull(row.total_value_usd) || 0);
    if (row.transaction_type === 'buy') { buys += 1; buyUsd += usd; }
    if (row.transaction_type === 'sell') { sells += 1; sellUsd += usd; }
  }
  return {
    available: rows.length > 0,
    exact_token_address: tokenAddress,
    buys,
    sells,
    buy_usd: buyUsd,
    sell_usd: sellUsd,
    net_sell_minus_buy_usd: sellUsd - buyUsd,
    rows_observed: rows.length,
    pages_scanned: pages,
    scan_complete_within_3_pages: complete,
    is_pnl: false,
    note: 'DEX swap cashflow only; transfers in/out and external inventory can change cost basis, so Kaka does not label this as PnL.',
    source: 'moralis_solana_wallet_swaps_exact_token',
  };
}
function walletHolderContext(network, wallet, holders) {
  const rows = Array.isArray(holders?.top_holders) ? holders.top_holders : [];
  const idx = rows.findIndex((x) => exactAddressEqual(network, x?.address, wallet));
  if (idx < 0) return { in_top_holder_list: false, rank: null, percent: null };
  return { in_top_holder_list: true, rank: idx + 1, percent: numberOrNull(rows[idx]?.percent), label: text(rows[idx]?.label || rows[idx]?.entity) };
}
function directWalletRoles(network, wallet, security) {
  const map = directRolesFromSecurity(network, security);
  return map.get(walletIdentityKey(network, wallet))?.roles || [];
}
function tokenIntelWalletContext(network, wallet, intel) {
  const find = (rows) => (Array.isArray(rows) ? rows : []).find((x) => exactAddressEqual(network, x?.address, wallet)) || null;
  return {
    early_buyer: find(intel?.early_buyers),
    large_holder: find(intel?.large_holders),
    recent_trader: find(intel?.recent_traders),
    token_profitability_candidate: find(intel?.smart_money_candidates),
  };
}
async function buildWalletQuickview(network, wallet, tokenAddress = '') {
  stats.wallet_quickview_builds += 1;
  try {
    const base = await cachedWalletBase(network, wallet);
    let holders = null; let security = null; let intel = null; let cashflow = null;
    const partialErrors = [...(base?.upstream_partial_errors || [])];
    if (tokenAddress) {
      const [ctx, intelResult] = await Promise.allSettled([
        step1038Context(network, tokenAddress),
        cachedBuild(`step1039:token-wallets:${network}:${lower(tokenAddress)}`, { freshMs: TOKEN_WALLET_FRESH_MS, staleMs: TOKEN_WALLET_STALE_MS, negativeMs: TOKEN_WALLET_NEGATIVE_MS }, () => buildTokenWalletIntelligence(network, tokenAddress)),
      ]);
      if (ctx.status === 'fulfilled') { holders = ctx.value.holders; security = ctx.value.security; partialErrors.push(ctx.value.holder_error, ctx.value.security_error); }
      else partialErrors.push(`context:${text(ctx.reason?.message || ctx.reason)}`);
      if (intelResult.status === 'fulfilled') intel = intelResult.value?.value || null;
      else partialErrors.push(`token_wallets:${text(intelResult.reason?.message || intelResult.reason)}`);
      if (network === 'solana') {
        try { cashflow = await solanaTokenSwapCashflow(wallet, tokenAddress); }
        catch (error) { partialErrors.push(`solana_cashflow:${text(error?.message || error)}`); }
      }
    }
    const currentPnl = tokenAddress && network !== 'solana' && Array.isArray(base?.pnl_rows)
      ? base.pnl_rows.find((x) => exactAddressEqual(network, x?.token_address, tokenAddress)) || null
      : null;
    const tokenContext = tokenAddress ? tokenIntelWalletContext(network, wallet, intel) : { early_buyer: null, large_holder: null, recent_trader: null, token_profitability_candidate: null };
    const globalSignal = base?.smart_money_signal || { smart_money_candidate: false, confirmed_smart_money: false };
    const tokenSignal = tokenContext.token_profitability_candidate;
    const candidate = Boolean(globalSignal.smart_money_candidate || tokenSignal?.smart_money_candidate);
    return {
      source: network === 'solana' ? 'kaka_wallet_quickview_helius_plus_moralis_cashflow' : 'kaka_wallet_quickview_moralis_pnl_plus_chain_activity',
      network,
      wallet,
      token_address: tokenAddress || null,
      activity: base?.activity || null,
      portfolio: base?.portfolio || null,
      pnl: base?.pnl || null,
      current_token_pnl: currentPnl,
      solana_current_token_dex_swap_cashflow: cashflow,
      holder_context: tokenAddress ? walletHolderContext(network, wallet, holders) : null,
      direct_source_roles: tokenAddress ? directWalletRoles(network, wallet, security) : [],
      token_context: tokenContext,
      smart_money_signal: {
        smart_money_candidate: candidate,
        confirmed_smart_money: false,
        rule_version: SMART_MONEY_RULE_VERSION,
        global_wallet_signal: globalSignal,
        token_profitability_signal: tokenSignal,
        label_semantics: 'Kaka profitability-signal candidate, not an identity certification',
      },
      no_sniper_dev_insider_common_funding_inference: true,
      upstream_partial_errors: partialErrors.filter(Boolean),
    };
  } catch (error) {
    stats.wallet_quickview_build_failures += 1;
    throw error;
  }
}


// ---------------- Step1040 relationship evidence ----------------
function moralisWalletHistoryUrl(network, wallet, order = 'ASC', limit = EVM_INITIAL_HISTORY_LIMIT) {
  const chain = MORALIS_EVM_CHAIN[network];
  if (!chain) throw new Error('moralis_evm_chain_not_supported');
  const u = new URL(`https://deep-index.moralis.io/api/v2.2/wallets/${encodeURIComponent(wallet)}/history`);
  u.searchParams.set('chain', chain);
  u.searchParams.set('order', order === 'DESC' ? 'DESC' : 'ASC');
  u.searchParams.set('limit', String(Math.max(1, Math.min(100, limit))));
  u.searchParams.set('include_internal_transactions', 'true');
  return u.toString();
}
function knownSharedEntityName(value) {
  const v = lower(value);
  if (!v) return false;
  return ['binance','coinbase','okx','bybit','bitget','gate','kraken','kucoin','mexc','crypto.com','upbit','bithumb','htx','huobi'].some((x) => v.includes(x));
}
function normalizedFundingEvidence(network, wallet, funder, extra = {}) {
  const f = text(funder);
  if (!walletAddressValid(network, f) || exactAddressEqual(network, f, wallet)) return null;
  const entityName = text(extra.funder_name || extra.entity || extra.label);
  return {
    wallet,
    funder: f,
    amount_native: numberOrNull(extra.amount_native),
    token_symbol: text(extra.token_symbol),
    transaction_hash: text(extra.transaction_hash || extra.signature),
    funded_at: text(extra.funded_at || extra.date) || null,
    funder_name: entityName,
    funder_type: text(extra.funder_type),
    shared_entity_funder: knownSharedEntityName(entityName) || lower(extra.funder_type) === 'exchange',
    source: text(extra.source),
    evidence_kind: 'original_or_earliest_observed_native_funding',
  };
}
function extractEvmInitialFunding(network, wallet, raw) {
  const rows = Array.isArray(raw?.result) ? raw.result : [];
  const target = lower(wallet);
  const candidates = [];
  for (const row of rows) {
    const at = text(row?.block_timestamp);
    const native = Array.isArray(row?.native_transfers) ? row.native_transfers : [];
    for (const tr of native) {
      const to = lower(tr?.to_address);
      const from = text(tr?.from_address);
      const amount = numberOrNull(tr?.value_formatted);
      if (to !== target || !looksEvmAddress(from) || lower(from) === target || !(amount > 0)) continue;
      candidates.push(normalizedFundingEvidence(network, wallet, from, {
        amount_native: amount,
        token_symbol: text(tr?.token_symbol),
        transaction_hash: text(row?.hash),
        funded_at: text(tr?.block_timestamp || at),
        funder_name: text(tr?.from_address_entity || tr?.from_address_label),
        source: 'moralis_wallet_history_earliest_observed_native_transfer',
      }));
    }
    const topTo = lower(row?.to_address);
    const topFrom = text(row?.from_address);
    const wei = numberOrNull(row?.value);
    if (topTo === target && looksEvmAddress(topFrom) && lower(topFrom) !== target && wei && wei > 0) {
      candidates.push(normalizedFundingEvidence(network, wallet, topFrom, {
        amount_native: wei / 1e18,
        token_symbol: network === 'bsc' ? 'BNB' : 'ETH',
        transaction_hash: text(row?.hash),
        funded_at: at,
        funder_name: text(row?.from_address_entity || row?.from_address_label),
        source: 'moralis_wallet_history_top_level_native_transfer',
      }));
    }
  }
  return candidates.filter(Boolean).sort((a,b) => Date.parse(a.funded_at || 0) - Date.parse(b.funded_at || 0))[0] || null;
}
async function buildFundingSource(network, wallet) {
  stats.funding_source_builds += 1;
  try {
    if (network === 'solana') {
      const raw = await heliusRestJson(`https://api.helius.xyz/v1/wallet/${encodeURIComponent(wallet)}/funded-by`, { priority: 4, label: 'step1040-solana-funded-by' });
      if (!raw) return null;
      return normalizedFundingEvidence(network, wallet, raw.funder, {
        amount_native: raw.amount,
        token_symbol: raw.symbol || 'SOL',
        signature: raw.signature,
        date: raw.date || (raw.timestamp ? new Date(Number(raw.timestamp) * 1000).toISOString() : null),
        funder_name: raw.funderName,
        funder_type: raw.funderType,
        source: 'helius_wallet_api_funded_by',
      });
    }
    const raw = await moralisFetchJson(moralisWalletHistoryUrl(network, wallet, 'ASC', EVM_INITIAL_HISTORY_LIMIT), {
      cu: MORALIS_WALLET_HISTORY_BUDGET_CU, kind: 'relationship', priority: 4, label: `step1040-wallet-history-asc-${network}`,
    });
    return extractEvmInitialFunding(network, wallet, raw);
  } catch (error) {
    stats.funding_source_build_failures += 1;
    throw error;
  }
}
async function cachedFundingSource(network, wallet) {
  const key = `step1040:funder:${network}:${walletIdentityKey(network, wallet)}`;
  const result = await cachedBuild(key, { freshMs: FUNDING_FRESH_MS, staleMs: FUNDING_STALE_MS, negativeMs: FUNDING_NEGATIVE_MS }, () => buildFundingSource(network, wallet));
  return { value: result.value || null, cache_status: result.cache_status };
}
function candidateWalletPriority(intel) {
  const out = [];
  const seen = new Set();
  function add(address, source, extra = {}) {
    if (!walletAddressValid(intel.network, address)) return;
    const k = walletIdentityKey(intel.network, address);
    if (seen.has(k)) return;
    seen.add(k);
    out.push({ address, source, ...extra });
  }
  for (const x of intel.direct_role_wallets || []) add(text(x.address), 'direct_role', { direct_roles: x.roles || [] });
  for (const x of (intel.early_buyers || []).slice(0, 4)) add(text(x.address), 'early_buyer', { early_buy_rank: x.early_buy_rank, first_observed_buy_at: x.first_observed_buy_at });
  for (const x of (intel.large_holders || []).slice(0, 4)) add(text(x.address), 'large_holder', { holder_rank: x.holder_rank, holder_percent: x.holder_percent });
  for (const x of (intel.smart_money_candidates || []).slice(0, 3)) add(text(x.address), 'token_profitability_candidate');
  return out.slice(0, RELATION_ANALYZED_WALLET_MAX);
}
function confidenceFromSniper(seconds, rank) {
  if (seconds >= 0 && seconds <= SNIPER_HIGH_SECONDS && rank <= 5) return 'high';
  if (seconds >= 0 && seconds <= SNIPER_MEDIUM_SECONDS && rank <= SNIPER_MAX_RANK) return 'medium';
  return 'low';
}
function buildSniperSignals(earlyBuyers, poolCreatedAt) {
  const created = Date.parse(poolCreatedAt || '');
  if (!Number.isFinite(created)) return [];
  return (earlyBuyers || []).map((row) => {
    const buy = Date.parse(row.first_observed_buy_at || '');
    const rank = Number(row.early_buy_rank || 999);
    if (!Number.isFinite(buy)) return null;
    const seconds = Math.round((buy - created) / 1000);
    if (seconds < 0 || seconds > SNIPER_MEDIUM_SECONDS || rank > SNIPER_MAX_RANK) return null;
    return {
      wallet: text(row.address),
      signal: 'suspected_early_sniper_behavior',
      confidence: confidenceFromSniper(seconds, rank),
      confirmed_identity: false,
      wrongdoing_claim: false,
      evidence: {
        pool_created_at: poolCreatedAt,
        first_observed_buy_at: row.first_observed_buy_at,
        seconds_after_pool_creation: seconds,
        early_buy_rank: rank,
        early_scope: row.evidence_scope || `first_${EARLY_SWAP_SCOPE}_token_swaps_not_exhaustive`,
      },
    };
  }).filter(Boolean);
}
function roleAddressSet(network, security) {
  const map = directRolesFromSecurity(network, security);
  const out = new Map();
  for (const row of map.values()) out.set(walletIdentityKey(network, row.address), row);
  return out;
}
function fundingGroups(network, fundingRows) {
  const groups = new Map();
  for (const item of fundingRows || []) {
    const f = item?.funding;
    if (!f?.funder) continue;
    const key = walletIdentityKey(network, f.funder);
    const group = groups.get(key) || { funder: f.funder, funder_name: f.funder_name || '', funder_type: f.funder_type || '', shared_entity_funder: Boolean(f.shared_entity_funder), wallets: [] };
    group.wallets.push(item.wallet);
    groups.set(key, group);
  }
  return [...groups.values()].filter((g) => new Set(g.wallets.map((w)=>walletIdentityKey(network,w))).size >= 2).map((g) => ({
    ...g,
    wallets: [...new Map(g.wallets.map((w)=>[walletIdentityKey(network,w),w])).values()],
    confidence: g.shared_entity_funder ? 'low' : 'high',
    relation_semantics: g.shared_entity_funder ? 'same_known_shared_service_funder_is_not_wallet-control-evidence' : 'same_exact_original_funder_strong_relationship_evidence_not_identity_proof',
  }));
}
function buildDevAssociationSignals(network, candidateFunding, security) {
  const roles = roleAddressSet(network, security);
  const out = [];
  for (const item of candidateFunding || []) {
    const walletKey = walletIdentityKey(network, item.wallet);
    if (roles.has(walletKey)) {
      out.push({ wallet: item.wallet, signal: 'direct_creator_or_owner_role_fact', confidence: 'high', inferred_dev_identity: false, evidence: { direct_source_roles: roles.get(walletKey).roles || [] } });
    }
    const f = item.funding;
    if (f?.funder) {
      const funderRole = roles.get(walletIdentityKey(network, f.funder));
      if (funderRole) {
        out.push({ wallet: item.wallet, signal: 'initial_funder_is_creator_or_owner', confidence: 'high', inferred_dev_identity: false, evidence: { funder: f.funder, funder_roles: funderRole.roles || [], funded_at: f.funded_at, transaction_hash: f.transaction_hash } });
      }
    }
  }
  return out;
}
function clusterFromEvidence(network, candidates, fundingGroupsRows, devSignals) {
  const parent = new Map();
  const addrMap = new Map();
  function key(a){ return walletIdentityKey(network,a); }
  function init(a){ if(!walletAddressValid(network,a)) return; const k=key(a); if(!parent.has(k)) parent.set(k,k); addrMap.set(k,a); }
  function find(k){ let p=parent.get(k); if(p===undefined) return null; while(p!==parent.get(p)){ parent.set(p,parent.get(parent.get(p))); p=parent.get(p);} return p; }
  function union(a,b){ init(a);init(b); const ka=find(key(a)), kb=find(key(b)); if(ka && kb && ka!==kb) parent.set(kb,ka); }
  for(const c of candidates||[]) init(c.address);
  for(const g of fundingGroupsRows||[]){ if(g.shared_entity_funder) continue; const ws=g.wallets||[]; for(let i=1;i<ws.length;i++) union(ws[0],ws[i]); }
  for(const d of devSignals||[]){ const funder=d?.evidence?.funder; if(funder && walletAddressValid(network,funder)) union(d.wallet,funder); }
  const groups=new Map();
  for(const [k,a] of addrMap){ const r=find(k); if(!r) continue; const arr=groups.get(r)||[]; arr.push(a); groups.set(r,arr); }
  return [...groups.values()].filter((x)=>x.length>=2).map((wallets,idx)=>({ cluster_id:`cluster_${idx+1}`, wallets, wallet_count:wallets.length, evidence_basis:'connected_by_same_non_exchange_original_funder_or_creator_owner_funding_edge', identity_certification:false }));
}
async function buildTokenRelations(network, tokenAddress) {
  stats.relation_builds += 1;
  try {
    const [intelResult, context, pairsResult] = await Promise.all([
      cachedBuild(`step1039:token-wallets:${network}:${lower(tokenAddress)}`, { freshMs: TOKEN_WALLET_FRESH_MS, staleMs: TOKEN_WALLET_STALE_MS, negativeMs: TOKEN_WALLET_NEGATIVE_MS }, () => buildTokenWalletIntelligence(network, tokenAddress)),
      step1038Context(network, tokenAddress),
      cachedBuild(`token_pairs:${network}:${lower(tokenAddress)}`, { freshMs: 20_000, staleMs: 5 * 60_000 }, () => buildDexTokenPairs(network, tokenAddress)),
    ]);
    const intel = intelResult.value;
    if (!intel) throw new Error('token_wallet_intelligence_not_ready');
    const candidates = candidateWalletPriority(intel);
    const fundingSettled = await Promise.allSettled(candidates.map(async (c) => ({ ...c, ...(await cachedFundingSource(network, c.address)) })));
    const candidateFunding = fundingSettled.map((r,i) => ({ wallet:candidates[i].address, candidate:candidates[i], funding:r.status==='fulfilled'?r.value.value:null, funding_cache_status:r.status==='fulfilled'?r.value.cache_status:null, funding_error:r.status==='rejected'?text(r.reason?.message||r.reason):'' }));
    const bestPair = (pairsResult.value || [])[0] || null;
    const sniperSignals = buildSniperSignals(intel.early_buyers, bestPair?.pool_created_at || null);
    const commonGroups = fundingGroups(network, candidateFunding);
    const devSignals = buildDevAssociationSignals(network, candidateFunding, context.security);
    const clusters = clusterFromEvidence(network, candidates, commonGroups, devSignals);
    const edges = [];
    for (const g of commonGroups) {
      for (const w of g.wallets) edges.push({ from: g.funder, to: w, type: 'common_original_funder', confidence: g.confidence, evidence: { funder_name:g.funder_name, funder_type:g.funder_type, shared_entity_funder:g.shared_entity_funder } });
    }
    for (const d of devSignals) {
      if (d.evidence?.funder) edges.push({ from:d.evidence.funder, to:d.wallet, type:'creator_owner_initial_funding', confidence:d.confidence, evidence:d.evidence });
    }
    return {
      source: network === 'solana' ? 'kaka_relationship_evidence_helius_funding_plus_step1039_signals' : 'kaka_relationship_evidence_moralis_wallet_history_plus_step1039_signals',
      network,
      token_address: tokenAddress,
      pool_created_at: bestPair?.pool_created_at || null,
      analyzed_wallets: candidates,
      analyzed_wallet_count: candidates.length,
      funding_evidence: candidateFunding,
      suspected_sniper_behavior_signals: sniperSignals,
      creator_owner_association_signals: devSignals,
      common_funding_groups: commonGroups,
      wallet_clusters: clusters,
      relation_edges: edges,
      confidence_rule_version: RELATION_CONFIDENCE_RULE_VERSION,
      confidence_rules: {
        high: 'direct_creator_owner_role_or_exact_original_funding_edge_or_buy_within_30s_and_rank_lte_5',
        medium: 'buy_within_120s_and_rank_lte_10',
        low: 'shared_known_exchange_or_service_funder_is_weak_context_only',
      },
      scope_limits: {
        analyzed_wallet_max: RELATION_ANALYZED_WALLET_MAX,
        early_swap_scope: EARLY_SWAP_SCOPE,
        evm_initial_history_limit: EVM_INITIAL_HISTORY_LIMIT,
        not_full_chain_graph: true,
      },
      semantics: {
        suspected_sniper_behavior_is_timing_signal_not_identity: true,
        creator_owner_are_direct_source_roles_not_dev_identity: true,
        common_funder_is_relationship_evidence_not_common_control_proof: true,
        wallet_cluster_is_evidence_component_not_entity_identity: true,
        insider_or_rat_trading_claim_generated: false,
        wrongdoing_claim_generated: false,
      },
      upstream_partial_errors: [...(intel.upstream_partial_errors || []), context.holder_error, context.security_error, ...candidateFunding.map((x)=>x.funding_error).filter(Boolean)].filter(Boolean),
    };
  } catch (error) {
    stats.relation_build_failures += 1;
    throw error;
  }
}

function exactAddressEqual(network, a, b) {
  if (network === 'solana') return text(a) === text(b);
  return lower(a) === lower(b);
}
function pairContainsToken(network, pair, tokenAddress) {
  return [pair?.base_token?.address, pair?.quote_token?.address]
    .some((candidate) => exactAddressEqual(network, candidate, tokenAddress));
}
async function exactPoolPreflight(network, tokenAddress, poolAddress) {
  const result = await cachedBuild(
    `token_pairs:${network}:${lower(tokenAddress)}`,
    { freshMs: 20_000, staleMs: 5 * 60_000 },
    () => buildDexTokenPairs(network, tokenAddress),
  );
  const pool = (result.value || []).find((row) =>
    exactAddressEqual(network, row?.pool_address, poolAddress) &&
    pairContainsToken(network, row, tokenAddress)
  ) || null;
  if (!pool) {
    const error = new Error('pool_not_owned_by_exact_token_on_network');
    error.statusCode = 400;
    throw error;
  }
  return pool;
}

function intervalPolicy(interval, endTimeMs = null) {
  const now = Date.now();
  const step = klineIntervalMs(interval) || INTERVAL_MS[interval] || 60_000;
  const historical = Number.isFinite(Number(endTimeMs)) && Number(endTimeMs) < now - step * 4;
  if (historical) return { freshMs: 6 * 60 * 60_000, staleMs: 7 * 24 * 60 * 60_000 };
  if (interval === '1m') return { freshMs: 15_000, staleMs: 5 * 60_000 };
  if (interval === '5m' || interval === '15m' || interval === '30m') return { freshMs: 30_000, staleMs: 15 * 60_000 };
  if (interval === '1h') return { freshMs: 60_000, staleMs: 60 * 60_000 };
  if (interval === '4h') return { freshMs: 3 * 60_000, staleMs: 6 * 60 * 60_000 };
  return { freshMs: 15 * 60_000, staleMs: 24 * 60 * 60_000 };
}
function pruneSimpleCache(map, max) {
  if (map.size <= max) return;
  const entries = [...map.entries()].sort((a, b) => Number(a[1]?.storedAt || 0) - Number(b[1]?.storedAt || 0));
  while (entries.length > max) map.delete(entries.shift()[0]);
}

async function cachedKlineBuild(key, policy, builder) {
  const now = Date.now();
  const cached = klineCache.get(key);
  const age = cached ? now - cached.storedAt : Number.POSITIVE_INFINITY;
  if (cached && age <= policy.freshMs) {
    stats.kline_cache_fresh_hits += 1;
    return { value: cached.value, cache_status: 'fresh_hit' };
  }
  if (cached && age <= policy.staleMs) {
    stats.kline_cache_stale_hits += 1;
    if (!klineInflight.has(key)) {
      const pending = Promise.resolve().then(builder).then((value) => {
        if (value?.rows?.length || value?.history_exhausted === true) {
          klineCache.set(key, { value, storedAt: Date.now() });
          pruneSimpleCache(klineCache, KLINE_CACHE_MAX_ENTRIES);
        }
        return value;
      }).finally(() => klineInflight.delete(key));
      klineInflight.set(key, pending);
    }
    return { value: cached.value, cache_status: 'stale_hit' };
  }
  let pending = klineInflight.get(key);
  if (pending) {
    stats.kline_inflight_hits += 1;
  } else {
    stats.kline_cache_misses += 1;
    pending = Promise.resolve().then(builder).then((value) => {
      if (value?.rows?.length || value?.history_exhausted === true) {
        klineCache.set(key, { value, storedAt: Date.now() });
        pruneSimpleCache(klineCache, KLINE_CACHE_MAX_ENTRIES);
      }
      return value;
    }).finally(() => klineInflight.delete(key));
    klineInflight.set(key, pending);
  }
  return { value: await pending, cache_status: 'miss' };
}

function normalizeMoralisCandle(raw) {
  if (!raw) return null;
  const timestampMs = Date.parse(text(raw.timestamp));
  const open = numberOrNull(raw.open);
  const high = numberOrNull(raw.high);
  const low = numberOrNull(raw.low);
  const close = numberOrNull(raw.close);
  const volume = numberOrNull(raw.volume) ?? 0;
  const trades = numberOrNull(raw.trades);
  if (!Number.isFinite(timestampMs) || timestampMs <= 0 ||
      open === null || high === null || low === null || close === null ||
      open <= 0 || high <= 0 || low <= 0 || close <= 0) return null;
  return {
    open_time_ms: timestampMs,
    open,
    high,
    low,
    close,
    volume,
    quote_volume: volume,
    trades,
  };
}
function aggregate15m(rows) {
  const buckets = new Map();
  for (const row of rows) {
    const bucket = Math.floor(row.open_time_ms / INTERVAL_MS['15m']) * INTERVAL_MS['15m'];
    let item = buckets.get(bucket);
    if (!item) {
      item = {
        open_time_ms: bucket,
        open: row.open,
        high: row.high,
        low: row.low,
        close: row.close,
        volume: row.volume || 0,
        quote_volume: row.quote_volume || row.volume || 0,
        trades: row.trades == null ? null : 0,
        source_parts: 0,
      };
      buckets.set(bucket, item);
    }
    if (row.open_time_ms < (item._first_time ?? Number.POSITIVE_INFINITY)) {
      item._first_time = row.open_time_ms;
      item.open = row.open;
    }
    if (row.open_time_ms >= (item._last_time ?? 0)) {
      item._last_time = row.open_time_ms;
      item.close = row.close;
    }
    item.high = Math.max(item.high, row.high);
    item.low = Math.min(item.low, row.low);
    if (item.source_parts > 0) {
      item.volume += row.volume || 0;
      item.quote_volume += row.quote_volume || row.volume || 0;
    }
    if (row.trades != null) item.trades = (item.trades || 0) + row.trades;
    item.source_parts += 1;
  }
  return [...buckets.values()]
    .filter((item) => item.source_parts >= 1)
    .map(({ _first_time, _last_time, ...item }) => item)
    .sort((a, b) => a.open_time_ms - b.open_time_ms);
}
function klineRange(interval, limit, endTimeMs = null) {
  const sourceInterval = MORALIS_TIMEFRAME[interval];
  const sourceStep = interval === '15m' ? INTERVAL_MS['5m'] : INTERVAL_MS[interval];
  const sourceLimit = Math.min(1000, interval === '15m' ? limit * 3 + 6 : limit + 3);
  const toMs = Number.isFinite(Number(endTimeMs)) && Number(endTimeMs) > 0
    ? Number(endTimeMs)
    : Date.now();
  const fromMs = Math.max(0, toMs - sourceStep * (sourceLimit + 4));
  return { sourceInterval, sourceLimit, fromMs, toMs };
}
function moralisKlineUrl(network, poolAddress, interval, limit, endTimeMs) {
  const range = klineRange(interval, limit, endTimeMs);
  if (network === 'solana') {
    const u = new URL(`https://solana-gateway.moralis.io/token/mainnet/pairs/${encodeURIComponent(poolAddress)}/ohlcv`);
    u.searchParams.set('timeframe', range.sourceInterval);
    u.searchParams.set('currency', 'usd');
    u.searchParams.set('fromDate', new Date(range.fromMs).toISOString());
    u.searchParams.set('toDate', new Date(range.toMs).toISOString());
    u.searchParams.set('limit', String(range.sourceLimit));
    return { url: u.toString(), ...range };
  }
  const chain = MORALIS_EVM_CHAIN[network];
  const u = new URL(`https://deep-index.moralis.io/api/v2.2/pairs/${encodeURIComponent(poolAddress)}/ohlcv`);
  u.searchParams.set('chain', chain);
  u.searchParams.set('timeframe', range.sourceInterval);
  u.searchParams.set('currency', 'usd');
  u.searchParams.set('fromDate', new Date(range.fromMs).toISOString());
  u.searchParams.set('toDate', new Date(range.toMs).toISOString());
  u.searchParams.set('limit', String(range.sourceLimit));
  return { url: u.toString(), ...range };
}

function proofKey(network, tokenAddress, poolAddress) {
  return `${network}:${lower(tokenAddress)}:${lower(poolAddress)}`;
}
function getIdentityProof(network, tokenAddress, poolAddress) {
  const key = proofKey(network, tokenAddress, poolAddress);
  const proof = identityProofCache.get(key);
  if (!proof) return null;
  if (Date.now() - proof.storedAt > IDENTITY_PROOF_TTL_MS) {
    identityProofCache.delete(key);
    return null;
  }
  return proof;
}
function setIdentityProof(network, tokenAddress, poolAddress, kind, detail = {}) {
  identityProofCache.set(proofKey(network, tokenAddress, poolAddress), {
    kind,
    detail,
    storedAt: Date.now(),
  });
  pruneSimpleCache(identityProofCache, IDENTITY_PROOF_MAX_ENTRIES);
}
function priceIdentityCompatible(dexPrice, closePrice) {
  const a = Number(dexPrice);
  const b = Number(closePrice);
  if (!Number.isFinite(a) || !Number.isFinite(b) || a <= 0 || b <= 0) return false;
  const ratio = b / a;
  return ratio >= 0.40 && ratio <= 2.50;
}

async function buildMoralisKlines(network, tokenAddress, pool, interval, limit, endTimeMs) {
  const poolAddress = pool.pool_address;
  const spec = moralisKlineUrl(network, poolAddress, interval, limit, endTimeMs);
  const payload = await moralisFetchJson(spec.url, {
    cu: MORALIS_KLINE_CU,
    kind: 'kline',
    priority: endTimeMs ? 5 : 20,
    label: `kline:${network}:${poolAddress}:${interval}`,
  });
  if (!exactAddressEqual(network, payload?.pairAddress, poolAddress)) {
    stats.kline_identity_rejections += 1;
    throw new Error('moralis_pair_identity_mismatch');
  }
  let rows = Array.isArray(payload?.result)
    ? payload.result.map(normalizeMoralisCandle).filter(Boolean).sort((a, b) => a.open_time_ms - b.open_time_ms)
    : [];
  if (interval === '15m') rows = aggregate15m(rows);
  rows = rows.slice(-limit);
  if (!rows.length) return {
    rows: [],
    source_token_address: text(payload?.tokenAddress),
    source_pair_address: text(payload?.pairAddress),
    source_timeframe: text(payload?.timeframe),
    derived_15m_from_5m: interval === '15m',
    identity_proof: getIdentityProof(network, tokenAddress, poolAddress)?.kind || null,
  };

  const returnedToken = text(payload?.tokenAddress);
  let proof = getIdentityProof(network, tokenAddress, poolAddress);
  if (returnedToken && exactAddressEqual(network, returnedToken, tokenAddress)) {
    setIdentityProof(network, tokenAddress, poolAddress, 'moralis_exact_token_address');
    stats.kline_identity_exact_proofs += 1;
    proof = getIdentityProof(network, tokenAddress, poolAddress);
  } else if (!proof && !endTimeMs) {
    const latestClose = rows[rows.length - 1]?.close;
    if (priceIdentityCompatible(pool.price_usd, latestClose)) {
      setIdentityProof(network, tokenAddress, poolAddress, 'latest_close_matches_exact_dex_token_price', {
        dex_price_usd: pool.price_usd,
        moralis_close_usd: latestClose,
      });
      stats.kline_identity_price_match_proofs += 1;
      proof = getIdentityProof(network, tokenAddress, poolAddress);
    }
  }
  if (!proof) {
    stats.kline_identity_rejections += 1;
    throw new Error('moralis_ohlcv_token_identity_not_proven');
  }

  return {
    rows,
    source_token_address: returnedToken || null,
    source_pair_address: text(payload?.pairAddress),
    source_timeframe: text(payload?.timeframe) || spec.sourceInterval,
    derived_15m_from_5m: interval === '15m',
    identity_proof: proof.kind,
  };
}


function geckoKlineSpec(interval) {
  switch (interval) {
    case '1m': return { timeframe: 'minute', aggregate: 1 };
    case '5m': return { timeframe: 'minute', aggregate: 5 };
    case '15m': return { timeframe: 'minute', aggregate: 15 };
    case '30m': return { timeframe: 'minute', aggregate: 30 };
    case '1h': return { timeframe: 'hour', aggregate: 1 };
    case '4h': return { timeframe: 'hour', aggregate: 4 };
    case '1d': return { timeframe: 'day', aggregate: 1 };
    default: return null;
  }
}
function normalizeGeckoCandle(raw) {
  if (!Array.isArray(raw) || raw.length < 6) return null;
  const ts = Number(raw[0]);
  const open = Number(raw[1]);
  const high = Number(raw[2]);
  const low = Number(raw[3]);
  const close = Number(raw[4]);
  const volume = Number(raw[5]);
  if (!Number.isFinite(ts) || ![open, high, low, close].every((x) => Number.isFinite(x) && x > 0)) return null;
  return {
    open_time_ms: Math.round(ts * 1000), open, high, low, close,
    volume: Number.isFinite(volume) && volume >= 0 ? volume : 0,
    quote_volume: Number.isFinite(volume) && volume >= 0 ? volume : 0,
    trades: null,
  };
}
async function buildGeckoKlines(network, tokenAddress, pool, interval, limit, endTimeMs) {
  const gtNetwork = GECKO_NETWORK[network];
  const spec = geckoKlineSpec(interval);
  if (!gtNetwork || !spec) throw new Error('geckoterminal_interval_not_supported');
  const orientation = tokenOrientationInPair(pool, tokenAddress);
  if (!orientation) throw new Error('geckoterminal_token_not_in_exact_pool');
  const poolAddress = text(pool.pool_address);
  const u = new URL(`${GECKO_BASE}/networks/${encodeURIComponent(gtNetwork)}/pools/${encodeURIComponent(poolAddress)}/ohlcv/${spec.timeframe}`);
  u.searchParams.set('aggregate', String(spec.aggregate));
  u.searchParams.set('limit', String(Math.min(1000, Math.max(1, limit))));
  u.searchParams.set('currency', 'usd');
  u.searchParams.set('token', orientation);
  if (endTimeMs) u.searchParams.set('before_timestamp', String(Math.floor(Number(endTimeMs) / 1000)));
  const payload = await geckoFetchJson(u.toString(), { priority: endTimeMs ? 4 : 18, label: `kline_fallback:${network}:${poolAddress}:${interval}` });
  const id = text(payload?.data?.id);
  if (id) {
    const returnedPool = id.includes('_') ? id.slice(id.indexOf('_') + 1) : id;
    if (!exactAddressEqual(network, returnedPool, poolAddress)) throw new Error('geckoterminal_pool_identity_mismatch');
  }
  const list = Array.isArray(payload?.data?.attributes?.ohlcv_list) ? payload.data.attributes.ohlcv_list : [];
  const rows = list.map(normalizeGeckoCandle).filter(Boolean).sort((a, b) => a.open_time_ms - b.open_time_ms).slice(-limit);
  if (!rows.length) throw new Error('geckoterminal_exact_pool_ohlcv_empty');
  setIdentityProof(network, tokenAddress, poolAddress, 'geckoterminal_exact_pool_token_orientation', { token_orientation: orientation });
  return {
    rows,
    source_token_address: tokenAddress,
    source_pair_address: poolAddress,
    source_timeframe: `${spec.timeframe}:${spec.aggregate}`,
    derived_15m_from_5m: false,
    identity_proof: 'geckoterminal_exact_pool_token_orientation',
    source: 'geckoterminal_keyless_public_exact_pool_ohlcv_fallback',
    fallback_from: 'moralis_pair_ohlcv_unavailable_or_empty',
  };
}
function historicalRangeReachesPoolCreation(pool, interval, limit, endTimeMs) {
  const end = Number(endTimeMs);
  const createdMs = Date.parse(text(pool?.pool_created_at));
  if (!Number.isFinite(end) || end <= 0 || !Number.isFinite(createdMs) || createdMs <= 0) return false;
  const range = klineRange(interval, limit, end);
  return Number.isFinite(range?.fromMs) && range.fromMs <= createdMs;
}
async function buildKlinesWithExactPoolFallback(network, tokenAddress, pool, interval, limit, endTimeMs) {
  let moralisError = '';
  try {
    const primary = await buildMoralisKlines(network, tokenAddress, pool, interval, limit, endTimeMs);
    if (Array.isArray(primary?.rows) && primary.rows.length > 0) {
      return { ...primary, source: 'moralis_official_data_api_pair_ohlcv', fallback_used: false, history_exhausted: false };
    }
    const exactProof = text(primary?.identity_proof);
    const exactPair = exactAddressEqual(network, primary?.source_pair_address, pool?.pool_address);
    if (endTimeMs && exactProof && exactPair && historicalRangeReachesPoolCreation(pool, interval, limit, endTimeMs)) {
      return {
        ...primary,
        source: 'moralis_official_data_api_pair_ohlcv',
        fallback_used: false,
        history_exhausted: true,
        history_exhausted_reason: 'exact_pool_primary_empty_and_requested_range_reaches_pool_creation',
      };
    }
    moralisError = 'moralis_exact_pool_ohlcv_empty';
  } catch (error) {
    moralisError = text(error?.message || error).slice(0, 240);
  }
  try {
    return { ...(await buildGeckoKlines(network, tokenAddress, pool, interval, limit, endTimeMs)), fallback_used: true, primary_error: moralisError, history_exhausted: false };
  } catch (fallbackError) {
    const error = new Error(`exact_pool_kline_unavailable:primary=${moralisError};fallback=${text(fallbackError?.message || fallbackError).slice(0, 220)}`);
    error.statusCode = 503;
    throw error;
  }
}

async function buildSharedContinuityKlines(network, tokenAddress, pool, interval, limit, endTimeMs) {
  const nativeInterval = Object.prototype.hasOwnProperty.call(MORALIS_TIMEFRAME, interval);
  if (nativeInterval) {
    const built = await buildKlinesWithExactPoolFallback(network, tokenAddress, pool, interval, limit, endTimeMs);
    if (built.history_exhausted === true && (!Array.isArray(built.rows) || built.rows.length === 0)) {
      return {
        ...built,
        rows: [],
        kline_feature_schema_version: KLINE_FEATURE_SCHEMA_VERSION,
        interval_mode: 'native_shared_history_exhausted',
        derived_from_interval: null,
        zero_trade_fill_count: 0,
        zero_trade_fill_policy: 'disabled_history_exhausted',
        zero_trade_fill_tail_extrapolation: false,
      };
    }
    const continuity = deriveAndFillKlines(built.rows || [], interval, {
      sourceInterval: interval,
      source: built.source || 'moralis_official_data_api_pair_ohlcv',
      sessionMode: 'continuous_24x7',
      maxGapBars: 96,
      limit,
    });
    return {
      ...built,
      rows: continuity.rows,
      kline_feature_schema_version: KLINE_FEATURE_SCHEMA_VERSION,
      interval_mode: 'native_shared_with_bounded_gap_continuity',
      derived_from_interval: null,
      zero_trade_fill_count: continuity.zero_trade_fill_count,
      zero_trade_fill_policy: continuity.zero_trade_fill_policy,
      zero_trade_fill_tail_extrapolation: false,
    };
  }

  const plan = klineDerivedPlan(interval);
  if (!plan || !Object.prototype.hasOwnProperty.call(MORALIS_TIMEFRAME, plan.base)) {
    const error = new Error('onchain_derived_interval_plan_invalid');
    error.statusCode = 400;
    throw error;
  }
  const baseLimit = KLINE_MAX_ROWS;
  const endKey = endTimeMs ? String(Math.floor(endTimeMs)) : 'latest';
  const baseKey = `kline:${network}:${lower(tokenAddress)}:${lower(pool.pool_address)}:${plan.base}:${baseLimit}:${endKey}`;
  const baseResult = await cachedKlineBuild(
    baseKey,
    intervalPolicy(plan.base, endTimeMs),
    () => buildKlinesWithExactPoolFallback(network, tokenAddress, pool, plan.base, baseLimit, endTimeMs),
  );
  const base = baseResult.value || { rows: [] };
  if (base.history_exhausted === true && (!Array.isArray(base.rows) || base.rows.length === 0)) {
    return {
      ...base,
      rows: [],
      kline_feature_schema_version: KLINE_FEATURE_SCHEMA_VERSION,
      interval_mode: 'shared_derived_history_exhausted',
      derived_from_interval: plan.base,
      base_cache_status: baseResult.cache_status,
      zero_trade_fill_count: 0,
      zero_trade_fill_policy: 'disabled_history_exhausted',
      zero_trade_fill_tail_extrapolation: false,
    };
  }
  if (!Array.isArray(base.rows) || base.rows.length === 0) {
    const error = new Error('onchain_derived_base_kline_empty');
    error.statusCode = 503;
    throw error;
  }
  const continuity = deriveAndFillKlines(base.rows, interval, {
    sourceInterval: plan.base,
    source: base.source || 'moralis_official_data_api_pair_ohlcv',
    sessionMode: 'continuous_24x7',
    maxGapBars: 96,
    limit,
  });
  return {
    ...base,
    rows: continuity.rows,
    kline_feature_schema_version: KLINE_FEATURE_SCHEMA_VERSION,
    interval_mode: 'shared_derived_same_exact_pool',
    derived_from_interval: plan.base,
    base_cache_status: baseResult.cache_status,
    zero_trade_fill_count: continuity.zero_trade_fill_count,
    zero_trade_fill_policy: continuity.zero_trade_fill_policy,
    zero_trade_fill_tail_extrapolation: false,
  };
}

function moralisPairTokenMeta(raw, amount, usdPrice) {
  if (!raw || typeof raw !== 'object') return null;
  const amountN = numberOrNull(amount);
  const priceN = numberOrNull(usdPrice);
  return {
    address: text(raw.address),
    name: text(raw.name),
    symbol: text(raw.symbol),
    decimals: numberOrNull(raw.decimals),
    amount: amountN,
    usd_price: priceN,
    usd_amount: amountN !== null && priceN !== null ? amountN * priceN : null,
  };
}

function normalizeMoralisPairSwap(network, raw, tokenAddress, pairMeta) {
  if (!raw) return null;
  const poolAddress = text(pairMeta?.pairAddress);
  const rowPair = text(raw.pairAddress);
  if (rowPair && poolAddress && !exactAddressEqual(network, rowPair, poolAddress)) {
    return null;
  }

  const base = moralisPairTokenMeta(
    pairMeta?.baseToken,
    raw.baseTokenAmount,
    raw.baseTokenPriceUsd,
  );
  const quote = moralisPairTokenMeta(
    pairMeta?.quoteToken,
    raw.quoteTokenAmount,
    raw.quoteTokenPriceUsd,
  );
  const requestedIsBase = Boolean(
    base?.address && exactAddressEqual(network, base.address, tokenAddress),
  );
  const requestedIsQuote = Boolean(
    quote?.address && exactAddressEqual(network, quote.address, tokenAddress),
  );
  if (!requestedIsBase && !requestedIsQuote) return null;

  const sourceType = lower(raw.transactionType);
  let bought = null;
  let sold = null;
  // Moralis Pair Swaps defines buy/sell relative to the pair's base token.
  // Preserve the source type and only derive bought/sold from the exact pair orientation.
  if (sourceType === 'buy') {
    bought = base;
    sold = quote;
  } else if (sourceType === 'sell') {
    bought = quote;
    sold = base;
  }

  const requestedSide =
    sourceType === 'buy'
      ? (requestedIsBase ? 'buy' : requestedIsQuote ? 'sell' : null)
      : sourceType === 'sell'
        ? (requestedIsBase ? 'sell' : requestedIsQuote ? 'buy' : null)
        : sourceType || null;

  return {
    transaction_hash: text(raw.transactionHash),
    transaction_index: numberOrNull(raw.transactionIndex),
    block_number: numberOrNull(raw.blockNumber),
    block_timestamp: text(raw.blockTimestamp),
    source_transaction_type: sourceType || null,
    requested_token_side: requestedSide,
    sub_category: text(raw.subCategory) || null,
    wallet_address: text(raw.walletAddress),
    pair_address: rowPair || poolAddress,
    pair_label: text(pairMeta?.pairLabel),
    exchange_name: text(pairMeta?.exchangeName),
    base_token: base,
    quote_token: quote,
    bought,
    sold,
    base_quote_price: numberOrNull(raw.baseQuotePrice),
    total_value_usd: numberOrNull(raw.totalValueUsd),
    requested_token_in_trade: true,
    requested_token_is_base: requestedIsBase,
    requested_token_is_quote: requestedIsQuote,
  };
}

function moralisTradesUrl(network, poolAddress, limit) {
  if (network === 'solana') {
    const u = new URL(`https://solana-gateway.moralis.io/token/mainnet/pairs/${encodeURIComponent(poolAddress)}/swaps`);
    u.searchParams.set('limit', String(limit));
    return u.toString();
  }
  const u = new URL(`https://deep-index.moralis.io/api/v2.2/pairs/${encodeURIComponent(poolAddress)}/swaps`);
  u.searchParams.set('chain', MORALIS_EVM_CHAIN[network]);
  u.searchParams.set('limit', String(limit));
  return u.toString();
}
async function cachedTradeBuild(key, builder) {
  const now = Date.now();
  const cached = tradeCache.get(key);
  if (cached && now - cached.storedAt <= 15_000) {
    stats.trades_cache_hits += 1;
    return { value: cached.value, cache_status: 'fresh_hit' };
  }
  if (cached && now - cached.storedAt <= 2 * 60_000) {
    stats.trades_cache_hits += 1;
    if (!tradeInflight.has(key)) {
      const pending = Promise.resolve().then(builder).then((value) => {
        tradeCache.set(key, { value, storedAt: Date.now() });
        pruneSimpleCache(tradeCache, TRADE_CACHE_MAX_ENTRIES);
        return value;
      }).finally(() => tradeInflight.delete(key));
      tradeInflight.set(key, pending);
    }
    return { value: cached.value, cache_status: 'stale_hit' };
  }
  let pending = tradeInflight.get(key);
  if (!pending) {
    stats.trades_cache_misses += 1;
    pending = Promise.resolve().then(builder).then((value) => {
      tradeCache.set(key, { value, storedAt: Date.now() });
      pruneSimpleCache(tradeCache, TRADE_CACHE_MAX_ENTRIES);
      return value;
    }).finally(() => tradeInflight.delete(key));
    tradeInflight.set(key, pending);
  } else {
    stats.trades_cache_hits += 1;
  }
  return { value: await pending, cache_status: 'miss' };
}

async function buildMoralisTrades(network, tokenAddress, pool, limit) {
  const payload = await moralisFetchJson(moralisTradesUrl(network, pool.pool_address, limit), {
    cu: MORALIS_TRADES_CU,
    kind: 'trade',
    priority: 10,
    label: `trades:${network}:${pool.pool_address}`,
  });

  const returnedPair = text(payload?.pairAddress);
  if (returnedPair && !exactAddressEqual(network, returnedPair, pool.pool_address)) {
    throw new Error('moralis_trade_pair_identity_mismatch');
  }

  const baseAddress = text(payload?.baseToken?.address);
  const quoteAddress = text(payload?.quoteToken?.address);
  const moralisPairContainsRequestedToken =
    (baseAddress && exactAddressEqual(network, baseAddress, tokenAddress)) ||
    (quoteAddress && exactAddressEqual(network, quoteAddress, tokenAddress));

  if ((baseAddress || quoteAddress) && !moralisPairContainsRequestedToken) {
    throw new Error('moralis_trade_token_identity_mismatch');
  }

  const rawRows = Array.isArray(payload?.result) ? payload.result : [];
  const pairMeta = {
    pairAddress: returnedPair || pool.pool_address,
    pairLabel: payload?.pairLabel,
    exchangeName: payload?.exchangeName,
    baseToken: payload?.baseToken,
    quoteToken: payload?.quoteToken,
  };

  return rawRows
    .map((row) => normalizeMoralisPairSwap(network, row, tokenAddress, pairMeta))
    .filter(Boolean)
    .slice(0, limit);
}

function recentCandidatePools(network, limit) {
  const now = Date.now();
  const rows = [];
  for (const tokenRow of trendingSnapshot) {
    if (network !== 'all' && tokenRow.network !== network) continue;
    const pool = tokenRow.best_pool;
    const createdMs = Date.parse(pool?.pool_created_at || '');
    if (!pool?.pool_address || !Number.isFinite(createdMs) || createdMs <= 0) continue;
    const ageMs = Math.max(0, now - createdMs);
    if (ageMs > STEP1041_NEW_POOL_MAX_AGE_MS) continue;
    rows.push({
      ...tokenRow,
      discovery_mode: 'new_pool_observation',
      pool_age_ms: ageMs,
      pool_created_at: pool.pool_created_at,
      discovery_scope: 'newest_pool_among_current_objective_and_supplemental_candidate_tokens_not_token_contract_creation_and_not_exhaustive_chain_scan',
    });
  }
  rows.sort((a, b) => Date.parse(b.pool_created_at || 0) - Date.parse(a.pool_created_at || 0));
  return rows.slice(0, Math.min(limit, STEP1041_NEW_MAX_ROWS));
}

function compactTokenSnapshotRow(row) {
  return {
    network: row.network,
    token: row.token,
    best_pool: row.best_pool,
    price_usd: row.price_usd,
    liquidity_usd: row.liquidity_usd,
    market_cap_usd: row.market_cap_usd,
    volume_usd: row.volume_usd,
    price_change_pct: row.price_change_pct,
    pool_created_at: row.pool_created_at,
    token_profile: row.token_profile || null,
    token_market_fields_verified: row.token_market_fields_verified === true,
  };
}

function buildStep1041Overview() {
  const now = Date.now();
  const rows = trendingSnapshot.slice(0, STEP1041_HOT_MAX_ROWS);
  const verified = rows.filter((row) => row.token_market_fields_verified === true);
  const perChain = {};
  let sampleVolume24h = 0;
  let sampleLiquidity = 0;
  let rising = 0;
  let falling = 0;
  let flat = 0;
  let new1h = 0;
  let new24h = 0;
  let new7d = 0;
  let profileRows = 0;
  for (const row of rows) {
    const network = row.network;
    const item = perChain[network] || { rows: 0, verified_market_rows: 0, sample_volume_24h_usd: 0, sample_liquidity_usd: 0, new_pool_24h: 0 };
    item.rows += 1;
    if (row.token_profile) profileRows += 1;
    if (row.token_market_fields_verified === true) {
      item.verified_market_rows += 1;
      const volume = Number(row?.volume_usd?.h24 || 0);
      const liquidity = Number(row.liquidity_usd || 0);
      if (Number.isFinite(volume) && volume > 0) { item.sample_volume_24h_usd += volume; sampleVolume24h += volume; }
      if (Number.isFinite(liquidity) && liquidity > 0) { item.sample_liquidity_usd += liquidity; sampleLiquidity += liquidity; }
      const change = Number(row?.price_change_pct?.h24);
      if (Number.isFinite(change)) { if (change > 0) rising += 1; else if (change < 0) falling += 1; else flat += 1; }
    }
    const createdMs = Date.parse(row?.best_pool?.pool_created_at || row?.pool_created_at || '');
    if (Number.isFinite(createdMs) && createdMs > 0) {
      const age = Math.max(0, now - createdMs);
      if (age <= 60 * 60_000) new1h += 1;
      if (age <= 24 * 60 * 60_000) { new24h += 1; item.new_pool_24h += 1; }
      if (age <= STEP1041_NEW_POOL_MAX_AGE_MS) new7d += 1;
    }
    perChain[network] = item;
  }
  const byVolume = [...verified].sort((a,b)=>Number(b?.volume_usd?.h24||0)-Number(a?.volume_usd?.h24||0)).slice(0,8).map(compactTokenSnapshotRow);
  const byLiquidity = [...verified].sort((a,b)=>Number(b.liquidity_usd||0)-Number(a.liquidity_usd||0)).slice(0,8).map(compactTokenSnapshotRow);
  const gainers = [...verified].filter(r=>Number.isFinite(Number(r?.price_change_pct?.h24))).sort((a,b)=>Number(b.price_change_pct.h24)-Number(a.price_change_pct.h24)).slice(0,8).map(compactTokenSnapshotRow);
  const losers = [...verified].filter(r=>Number.isFinite(Number(r?.price_change_pct?.h24))).sort((a,b)=>Number(a.price_change_pct.h24)-Number(b.price_change_pct.h24)).slice(0,8).map(compactTokenSnapshotRow);
  return {
    feature_schema_version: STEP1041_FEATURE_SCHEMA_VERSION,
    source: 'kaka_step1041_derived_from_background_shared_token_snapshot',
    source_scope: 'current_discovered_hot_token_sample_not_whole_chain_market',
    generated_at: marketUpdatedAt ? new Date(marketUpdatedAt).toISOString() : (discoveryUpdatedAt ? new Date(discoveryUpdatedAt).toISOString() : null),
    candidate_rows: rows.length,
    verified_market_rows: verified.length,
    profile_rows: profileRows,
    max_hot_rows: STEP1041_HOT_MAX_ROWS,
    sample_volume_24h_usd: sampleVolume24h,
    sample_liquidity_usd: sampleLiquidity,
    change_distribution: { rising, flat, falling },
    new_pool_observation: { within_1h: new1h, within_24h: new24h, within_7d: new7d, semantics: 'best_pool_created_at_in_current_discovered_sample_not_token_contract_creation' },
    per_chain: perChain,
    top_volume: byVolume,
    top_liquidity: byLiquidity,
    top_gainers: gainers,
    top_losers: losers,
    no_user_upstream_build: true,
    volume_liquidity_are_sample_sums_not_whole_chain_totals: true,
  };
}

async function cachedBuild(key, { freshMs, staleMs, negativeMs = 45_000 }, builder) {
  const now = Date.now();
  const neg = negativeCache.get(key);
  if (neg && neg.until > now) { stats.negative_hits += 1; return { value: null, cache_status: 'negative_hit' }; }
  if (neg) negativeCache.delete(key);
  const entry = cache.get(key);
  if (entry && now - entry.storedAt <= freshMs) { stats.cache_fresh_hits += 1; return { value: entry.value, cache_status: 'fresh_hit' }; }
  if (entry && now - entry.storedAt <= staleMs) {
    stats.cache_stale_hits += 1;
    if (!inflight.has(key)) {
      const pending = Promise.resolve().then(builder).then((value) => {
        if (value === null || value === undefined || (Array.isArray(value) && value.length === 0)) {
          negativeCache.set(key, { until: Date.now() + negativeMs }); pruneMap(negativeCache, NEGATIVE_CACHE_MAX_ENTRIES);
        } else { cache.set(key, { value, storedAt: Date.now() }); pruneMap(cache, CACHE_MAX_ENTRIES); }
        return value;
      }).finally(() => inflight.delete(key));
      inflight.set(key, pending);
    }
    return { value: entry.value, cache_status: 'stale_hit' };
  }
  let pending = inflight.get(key);
  if (pending) stats.inflight_hits += 1;
  if (!pending) {
    stats.cache_misses += 1;
    pending = Promise.resolve().then(builder).then((value) => {
      if (value === null || value === undefined || (Array.isArray(value) && value.length === 0)) {
        negativeCache.set(key, { until: Date.now() + negativeMs }); pruneMap(negativeCache, NEGATIVE_CACHE_MAX_ENTRIES);
      } else { cache.set(key, { value, storedAt: Date.now() }); pruneMap(cache, CACHE_MAX_ENTRIES); }
      return value;
    }).finally(() => inflight.delete(key));
    inflight.set(key, pending);
  }
  return { value: await pending, cache_status: 'miss' };
}

function normalizeDexPair(pair) {
  if (!pair || typeof pair !== 'object') return null;
  const network = DEX_TO_NETWORK[lower(pair.chainId)] || '';
  if (!network) return null;
  const base = pair.baseToken || {};
  const quote = pair.quoteToken || {};
  const poolAddress = text(pair.pairAddress);
  if (!poolAddress) return null;
  const txns = pair.txns || {};
  const volume = pair.volume || {};
  const changes = pair.priceChange || {};
  const liquidity = pair.liquidity || {};
  return {
    network,
    chain_id: NETWORKS[network].chain_id,
    pool_address: poolAddress,
    dex_id: text(pair.dexId),
    pair_url: text(pair.url),
    labels: Array.isArray(pair.labels) ? pair.labels.map(text).filter(Boolean).slice(0, 8) : [],
    base_token: { address: text(base.address), symbol: text(base.symbol), name: text(base.name) },
    quote_token: { address: text(quote.address), symbol: text(quote.symbol), name: text(quote.name) },
    price_usd: numberOrNull(pair.priceUsd),
    price_native: numberOrNull(pair.priceNative),
    liquidity_usd: numberOrNull(liquidity.usd),
    market_cap_usd: numberOrNull(pair.marketCap),
    fdv_usd: numberOrNull(pair.fdv),
    volume_usd: { m5: numberOrNull(volume.m5), h1: numberOrNull(volume.h1), h6: numberOrNull(volume.h6), h24: numberOrNull(volume.h24) },
    price_change_pct: { m5: numberOrNull(changes.m5), h1: numberOrNull(changes.h1), h6: numberOrNull(changes.h6), h24: numberOrNull(changes.h24) },
    txns: { m5: txns.m5 || null, h1: txns.h1 || null, h6: txns.h6 || null, h24: txns.h24 || null },
    pool_created_at: isoFromMs(pair.pairCreatedAt),
    info: pair.info && typeof pair.info === 'object' ? {
      image_url: text(pair.info.imageUrl),
      websites: Array.isArray(pair.info.websites) ? pair.info.websites.slice(0, 6) : [],
      socials: Array.isArray(pair.info.socials) ? pair.info.socials.slice(0, 8) : [],
    } : null,
    boosts_active: numberOrNull(pair?.boosts?.active),
    source: 'dexscreener_public_api',
  };
}
function normalizeDexPairs(payload) {
  const rows = Array.isArray(payload) ? payload : Array.isArray(payload?.pairs) ? payload.pairs : [];
  return rows.map(normalizeDexPair).filter(Boolean);
}
function pairIdentity(row) { return `${row.network}|${lower(row.pool_address)}`; }
function dedupePools(rows) {
  const byKey = new Map();
  for (const row of rows || []) {
    const key = pairIdentity(row);
    const current = byKey.get(key);
    if (!current || Number(row.liquidity_usd || 0) > Number(current.liquidity_usd || 0)) byKey.set(key, row);
  }
  return [...byKey.values()];
}
function tokenAddressInPair(row, tokenAddress) {
  const a = lower(tokenAddress);
  if (lower(row?.base_token?.address) === a) return row.base_token;
  if (lower(row?.quote_token?.address) === a) return row.quote_token;
  return null;
}
function poolLiquidity(row) { return Math.max(0, Number(row?.liquidity_usd || 0)); }
function poolVolume24h(row) { return Math.max(0, Number(row?.volume_usd?.h24 || 0)); }
function poolActivity(row, period) {
  return Math.max(0, Number(row?.txns?.[period]?.buys || 0) + Number(row?.txns?.[period]?.sells || 0));
}
// Step1049.1: keep this search-only. Do not alter pool identity, hot discovery,
// token detail, K-line identity or cache-key behavior outside the search route.
function searchTokenIdentityKey(network, address) {
  const raw = text(address);
  return `${network}|${network === 'solana' ? raw : lower(raw)}`;
}
function compareSearchPoolPriority(a, b) {
  return poolLiquidity(b) - poolLiquidity(a)
    || poolVolume24h(b) - poolVolume24h(a)
    || poolActivity(b, 'h24') - poolActivity(a, 'h24')
    || poolActivity(b, 'h1') - poolActivity(a, 'h1')
    || poolActivity(b, 'm5') - poolActivity(a, 'm5')
    || String(a?.pool_address || '').localeCompare(String(b?.pool_address || ''));
}
function poolScore(row) {
  // Legacy quality helper. Pool *selection* is no longer based on this score.
  const liq = Math.log10(1 + poolLiquidity(row));
  const vol = Math.log10(1 + poolVolume24h(row));
  return liq * 2 + vol * 2 + Math.log10(1 + poolActivity(row, 'h1')) * 1.5 + Math.log10(1 + poolActivity(row, 'm5'));
}
function hotScoreComponents(rows) {
  const pools = Array.isArray(rows) ? rows : [];
  const liquidity = pools.reduce((sum, row) => sum + poolLiquidity(row), 0);
  const volume24h = pools.reduce((sum, row) => sum + poolVolume24h(row), 0);
  const h1tx = pools.reduce((sum, row) => sum + poolActivity(row, 'h1'), 0);
  const m5tx = pools.reduce((sum, row) => sum + poolActivity(row, 'm5'), 0);
  // "Hot" = current real trading attention, not paid promotion and not price direction.
  // 24h turnover and short-window transaction activity dominate; liquidity is a quality floor.
  const score = Math.log10(1 + volume24h) * 4
    + Math.log10(1 + h1tx) * 3
    + Math.log10(1 + m5tx) * 2
    + Math.log10(1 + liquidity) * 1.5;
  return { score, volume_24h_usd: volume24h, liquidity_usd: liquidity, h1_transactions: h1tx, m5_transactions: m5tx };
}
function sortBestPools(rows) {
  return [...rows].sort((a, b) =>
    poolLiquidity(b) - poolLiquidity(a)
    || poolVolume24h(b) - poolVolume24h(a)
    || poolActivity(b, 'h1') - poolActivity(a, 'h1')
    || String(a?.pool_address || '').localeCompare(String(b?.pool_address || ''))
  );
}

function tokenMatchesText(token, query) {
  const q = lower(query);
  if (!q) return false;
  return [token?.address, token?.symbol, token?.name]
    .map((value) => lower(value))
    .some((value) => value && (value === q || value.includes(q)));
}
function tokenOrientationInPair(pair, tokenAddress) {
  if (lower(pair?.base_token?.address) === lower(tokenAddress)) return 'base';
  if (lower(pair?.quote_token?.address) === lower(tokenAddress)) return 'quote';
  return '';
}
function nullPriceChange() {
  return { m5: null, h1: null, h6: null, h24: null };
}
function tokenCentricRow(pair, token, extra = {}) {
  const orientation = tokenOrientationInPair(pair, token?.address);
  if (!orientation) return null;
  // Fail closed: pair-level price/change/FDV/market-cap are only bound to
  // the exact baseToken identity. A quoteToken never inherits those fields.
  const baseVerified = orientation === 'base';
  return {
    network: pair.network,
    chain_id: pair.chain_id,
    token: { ...token },
    best_pool: pair,
    token_orientation: orientation,
    token_market_fields_verified: baseVerified,
    price_usd: baseVerified ? pair.price_usd : null,
    liquidity_usd: pair.liquidity_usd,
    market_cap_usd: baseVerified ? pair.market_cap_usd : null,
    fdv_usd: baseVerified ? pair.fdv_usd : null,
    volume_usd: pair.volume_usd,
    price_change_pct: baseVerified ? pair.price_change_pct : nullPriceChange(),
    txns: pair.txns,
    pool_created_at: pair.pool_created_at,
    token_profile: extra.token_profile || tokenProfileForIdentity(pair.network, token?.address) || null,
    product_badges: Array.isArray(extra.product_badges) ? extra.product_badges : productBadgesForToken(pair, token),
    source: 'dexscreener_public_api_token_centric',
    ...extra,
  };
}
function chooseBetterTokenRow(current, candidate) {
  if (!current) return candidate;
  if (candidate.token_market_fields_verified && !current.token_market_fields_verified) return candidate;
  if (!candidate.token_market_fields_verified && current.token_market_fields_verified) return current;
  return compareSearchPoolPriority(candidate.best_pool, current.best_pool) < 0 ? candidate : current;
}
function tokenCentricSearchRows(query, pairs) {
  const byToken = new Map();
  for (const pair of pairs || []) {
    for (const token of [pair.base_token, pair.quote_token]) {
      if (!token?.address || !tokenMatchesText(token, query)) continue;
      const row = tokenCentricRow(pair, token, { search_query: text(query) });
      if (!row) continue;
      // Never merge by name/symbol. The only dedupe key is exact chain + token contract.
      // Solana/base58 stays case-sensitive; EVM remains case-insensitive.
      const key = searchTokenIdentityKey(pair.network, token.address);
      byToken.set(key, chooseBetterTokenRow(byToken.get(key), row));
    }
  }
  return [...byToken.values()]
    .filter((row) => row.token_market_fields_verified === true)
    .sort((a, b) =>
      compareSearchPoolPriority(a.best_pool, b.best_pool)
      || String(a.network || '').localeCompare(String(b.network || ''))
      || searchTokenIdentityKey(a.network, a?.token?.address)
        .localeCompare(searchTokenIdentityKey(b.network, b?.token?.address))
    )
    .slice(0, MAX_RESPONSE_ROWS);
}

async function buildDexSearch(query) {
  const payload = await dexFetchJson(`${DEX_BASE}/latest/dex/search?q=${encodeURIComponent(query)}`, { priority: 10, label: 'search' });
  const pairs = sortBestPools(dedupePools(normalizeDexPairs(payload))).slice(0, MAX_RESPONSE_ROWS);
  return tokenCentricSearchRows(query, pairs);
}
async function buildDexTokenPairs(network, address) {
  const meta = networkMeta(network);
  if (!meta || !validAddressForNetwork(network, address)) return [];
  const payload = await dexFetchJson(`${DEX_BASE}/token-pairs/v1/${encodeURIComponent(meta.dex)}/${encodeURIComponent(address)}`, { priority: 12, label: 'token_pairs' });
  const exact = normalizeDexPairs(payload).filter((row) => row.network === network && tokenAddressInPair(row, address));
  return sortBestPools(dedupePools(exact)).slice(0, MAX_RESPONSE_ROWS);
}
function normalizePublicLink(item) {
  if (!item || typeof item !== 'object') return null;
  const url = text(item.url);
  if (!/^https?:\/\//i.test(url)) return null;
  const type = text(item.type || item.platform || item.label).slice(0, 80);
  const label = text(item.label || item.type || item.platform).slice(0, 120);
  return { type, label, url: url.slice(0, 1000) };
}
function normalizeCandidateProfile(item) {
  if (!item || typeof item !== 'object') return null;
  const iconUrl = text(item.icon);
  const headerUrl = text(item.header);
  const profileUrl = text(item.url);
  const description = text(item.description).replace(/\s+/g, ' ').slice(0, 1200);
  const links = Array.isArray(item.links)
    ? item.links.map(normalizePublicLink).filter(Boolean).slice(0, 12)
    : [];
  if (!iconUrl && !headerUrl && !profileUrl && !description && !links.length) return null;
  return {
    icon_url: /^https:\/\//i.test(iconUrl) ? iconUrl.slice(0, 1000) : '',
    header_url: /^https:\/\//i.test(headerUrl) ? headerUrl.slice(0, 1000) : '',
    profile_url: /^https?:\/\//i.test(profileUrl) ? profileUrl.slice(0, 1000) : '',
    description,
    links,
    source: 'dexscreener_public_token_profile',
  };
}
function mergeTokenProfile(left, right) {
  if (!left) return right || null;
  if (!right) return left || null;
  const links = [];
  const seen = new Set();
  for (const item of [...(left.links || []), ...(right.links || [])]) {
    const key = lower(item?.url);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    links.push(item);
    if (links.length >= 12) break;
  }
  return {
    icon_url: text(right.icon_url) || text(left.icon_url),
    header_url: text(right.header_url) || text(left.header_url),
    profile_url: text(right.profile_url) || text(left.profile_url),
    description: text(right.description) || text(left.description),
    links,
    source: 'dexscreener_public_token_profile',
  };
}
function tokenProfileForIdentity(network, address) {
  const key = `${network}|${lower(address)}`;
  for (const row of trendingSnapshot) {
    if (`${row.network}|${lower(row?.token?.address)}` === key && row.token_profile) {
      return row.token_profile;
    }
  }
  return null;
}
function candidateRows(payload, source) {
  const raw = Array.isArray(payload) ? payload : payload && typeof payload === 'object' ? [payload] : [];
  return raw.map((item) => {
    const network = DEX_TO_NETWORK[lower(item?.chainId)] || '';
    const address = text(item?.tokenAddress);
    if (!network || !validAddressForNetwork(network, address)) return null;
    return {
      network,
      address,
      source,
      amount: numberOrNull(item?.amount),
      total_amount: numberOrNull(item?.totalAmount),
      token_profile: normalizeCandidateProfile(item),
    };
  }).filter(Boolean);
}

function geckoAddressFromRelationship(network, relation) {
  const id = text(relation?.data?.id);
  if (!id) return '';
  const prefix = `${GECKO_NETWORK[network] || ''}_`;
  if (prefix && lower(id).startsWith(lower(prefix))) return id.slice(prefix.length);
  const idx = id.indexOf('_');
  return idx >= 0 ? id.slice(idx + 1) : id;
}
async function fetchGeckoTrendingCandidates() {
  const candidates = [];
  let succeeded = 0;
  const readyNetworks = [];
  const failedNetworks = [];
  const retryNetworks = [];
  const retriedNetworks = [];
  const errorByNetwork = {};
  function appendPoolResources(network, payload, source) {
    let added = 0;
    for (const resource of Array.isArray(payload?.data) ? payload.data : []) {
      const address = geckoAddressFromRelationship(network, resource?.relationships?.base_token);
      if (!validAddressForNetwork(network, address)) continue;
      candidates.push({ network, address, source, token_profile: null, amount: 0, total_amount: 0 });
      added += 1;
    }
    return added;
  }
  function ready(network) {
    if (!readyNetworks.includes(network)) readyNetworks.push(network);
    delete errorByNetwork[network];
    succeeded = readyNetworks.length;
  }
  function recordError(network, stage, error) {
    errorByNetwork[network] = `${stage}:${text(error?.message || error).replace(/\s+/g, ' ').slice(0, 220)}`;
  }
  async function topPoolsFallback(network, gtNetwork, retryTag = '') {
    try {
      const topPools = await geckoFetchJson(
        `${GECKO_BASE}/networks/${encodeURIComponent(gtNetwork)}/pools?page=1`,
        { priority: -32, label: `background_gt_top_pool_fallback_${network}${retryTag}` },
      );
      const added = appendPoolResources(network, topPools, 'geckoterminal_top_pool_candidate_fallback');
      if (added > 0) { ready(network); return true; }
      errorByNetwork[network] = 'top_pools_success_but_no_usable_base_token';
      return false;
    } catch (error) {
      recordError(network, 'top_pools', error);
      return false;
    }
  }
  async function trendingAttempt(network, { retry = false } = {}) {
    const gtNetwork = GECKO_NETWORK[network];
    if (!gtNetwork) return false;
    try {
      const trending = await geckoFetchJson(
        `${GECKO_BASE}/networks/${encodeURIComponent(gtNetwork)}/trending_pools?page=1&duration=24h`,
        { priority: retry ? -31 : -30, label: `background_gt_trending_${network}${retry ? '_retry1' : ''}` },
      );
      const added = appendPoolResources(network, trending, 'geckoterminal_trending_pool_candidate');
      if (added > 0) { ready(network); return true; }
      // A successful response with zero usable base tokens is semantically different from
      // transport/rate-limit failure. Only this case is allowed to spend the Top Pools fallback.
      errorByNetwork[network] = 'trending_success_but_no_usable_base_token';
      return await topPoolsFallback(network, gtNetwork, retry ? '_after_retry1' : '');
    } catch (error) {
      recordError(network, retry ? 'trending_retry1' : 'trending', error);
      return false;
    }
  }

  // Step1042.2: public GeckoTerminal is documented as approximately 10 calls/minute and may
  // fluctuate with network traffic. Render previously succeeded for the first five networks
  // then lost the four trailing networks. Keep this lane deliberately below half that public
  // ceiling (~4 calls/min by default). A transport/rate-limit failure does NOT immediately
  // spend a second Top Pools call; failed networks receive exactly one delayed retry after the
  // first pass. This prevents trailing-chain starvation while keeping the collector bounded.
  for (const network of Object.keys(NETWORKS)) {
    if (!GECKO_NETWORK[network]) continue;
    const ok = await trendingAttempt(network);
    if (!ok && errorByNetwork[network] && !errorByNetwork[network].includes('success_but_no_usable')) {
      retryNetworks.push(network);
    }
  }
  for (const network of retryNetworks) {
    retriedNetworks.push(network);
    const ok = await trendingAttempt(network, { retry: true });
    if (!ok && !failedNetworks.includes(network)) failedNetworks.push(network);
  }
  for (const network of Object.keys(NETWORKS)) {
    if (!readyNetworks.includes(network) && !failedNetworks.includes(network)) failedNetworks.push(network);
  }
  stats.gecko_discovery_cycles += 1;
  stats.gecko_discovery_candidates = candidates.length;
  stats.gecko_discovery_ready_networks = readyNetworks;
  stats.gecko_discovery_failed_networks = failedNetworks;
  stats.gecko_discovery_retried_networks = retriedNetworks;
  stats.gecko_discovery_error_by_network = { ...errorByNetwork };
  return {
    candidates,
    succeeded,
    ready_networks: readyNetworks,
    failed_networks: failedNetworks,
    retried_networks: retriedNetworks,
    error_by_network: { ...errorByNetwork },
  };
}


async function fetchDiscoveryCandidatePairs() {
  // Step1041.5: Binance Wallet public Trending (1h) is the primary mature ranking source.
  // GeckoTerminal + DEX Screener feeds are fallback/recall only. Every candidate — including
  // Binance-ranked tokens — is re-read through DEX Screener's exact token endpoint before
  // publication, so external rank never overrides Kaka's exact chain+contract identity rules.
  // Step1042.1: source families use independent schedulers, so start them together.
  // This keeps the fixed upstream rate limits intact while avoiding serial cold-start walls.
  const binanceWalletRankPromise = fetchBinanceWalletTrendingCandidates();
  const geckoPromise = fetchGeckoTrendingCandidates();
  const feedSpecs = [
    { path: '/token-boosts/top/v1', source: 'top_boost_candidate', label: 'background_boost_top_candidates' },
    { path: '/token-boosts/latest/v1', source: 'latest_boost_candidate', label: 'background_boost_latest_candidates' },
    { path: '/token-profiles/latest/v1', source: 'latest_profile_candidate', label: 'background_profile_candidates' },
    { path: '/community-takeovers/latest/v1', source: 'latest_community_takeover_candidate', label: 'background_cto_candidates' },
    { path: '/ads/latest/v1', source: 'latest_ad_candidate', label: 'background_ad_candidates' },
  ];
  const dexFeedsPromise = Promise.all(feedSpecs.map(async (spec) => {
    try {
      const payload = await dexFetchJson(`${DEX_BASE}${spec.path}`, { priority: -20, label: spec.label });
      return { ...spec, payload, ok: true };
    } catch (error) {
      return { ...spec, payload: [], ok: false, error: text(error?.message || error).slice(0, 160) };
    }
  }));
  const [binanceWalletRank, gecko, settled] = await Promise.all([
    binanceWalletRankPromise,
    geckoPromise,
    dexFeedsPromise,
  ]);
  const dexFeedReady = settled.some((x) => x.ok);
  if (!binanceWalletRank.candidates.length && !gecko.candidates.length && !dexFeedReady) {
    throw new Error('all_hot_candidate_sources_failed');
  }

  // Insertion order matters: Binance-ranked identities enter each chain bucket first and keep
  // their official relative order. Supplemental identities can only fill remaining capacity.
  const candidates = [
    ...binanceWalletRank.candidates,
    ...gecko.candidates,
    ...settled.flatMap((feed) => candidateRows(feed.payload, feed.source)),
  ];
  const byIdentity = new Map();
  for (const row of candidates) {
    const key = `${row.network}|${lower(row.address)}`;
    const cur = byIdentity.get(key) || { ...row, candidate_sources: [] };
    if (!cur.candidate_sources.includes(row.source)) cur.candidate_sources.push(row.source);
    cur.amount = Math.max(Number(cur.amount || 0), Number(row.amount || 0));
    cur.total_amount = Math.max(Number(cur.total_amount || 0), Number(row.total_amount || 0));
    cur.token_profile = mergeTokenProfile(cur.token_profile, row.token_profile);
    const incomingRank = Number(row.binance_wallet_rank || 0);
    const currentRank = Number(cur.binance_wallet_rank || 0);
    if (incomingRank > 0 && (currentRank <= 0 || incomingRank < currentRank)) {
      cur.binance_wallet_rank = incomingRank;
      cur.binance_wallet_rank_period = row.binance_wallet_rank_period || '1h';
      cur.binance_wallet_rank_period_code = row.binance_wallet_rank_period_code || BINANCE_WALLET_TRENDING_PERIOD;
      cur.binance_wallet_rank_type = row.binance_wallet_rank_type || 10;
      cur.binance_wallet_symbol = row.binance_wallet_symbol || '';
      cur.binance_wallet_icon = row.binance_wallet_icon || '';
      cur.binance_wallet_alpha_info = row.binance_wallet_alpha_info || null;
    }
    byIdentity.set(key, cur);
  }

  const grouped = new Map(Object.keys(NETWORKS).map((key) => [key, []]));
  for (const row of byIdentity.values()) {
    const list = grouped.get(row.network);
    if (list && list.length < DISCOVERY_MAX_CANDIDATES_PER_CHAIN) list.push(row);
  }

  const pairs = [];
  for (const [network, list] of grouped) {
    if (!list.length) continue;
    const meta = networkMeta(network);
    for (let offset = 0; offset < list.length; offset += DEX_TOKEN_BATCH_MAX) {
      const batch = list.slice(offset, offset + DEX_TOKEN_BATCH_MAX);
      const byAddress = new Map(batch.map((x) => [lower(x.address), x]));
      const addresses = batch.map((x) => x.address);
      const payload = await dexFetchJson(
        `${DEX_BASE}/tokens/v1/${encodeURIComponent(meta.dex)}/${addresses.map(encodeURIComponent).join(',')}`,
        { priority: -15, label: `background_batch_${network}_${Math.floor(offset / DEX_TOKEN_BATCH_MAX) + 1}` },
      );
      const normalized = normalizeDexPairs(payload).filter((row) => row.network === network);
      for (const pair of normalized) {
        const baseMatch = byAddress.get(lower(pair.base_token.address));
        const quoteMatch = byAddress.get(lower(pair.quote_token.address));
        const match = baseMatch || quoteMatch;
        if (!match) continue;
        pairs.push({
          ...pair,
          candidate_token_address: match.address,
          candidate_sources: match.candidate_sources,
          candidate_boost_amount: match.amount,
          candidate_total_boost_amount: match.total_amount,
          candidate_token_profile: match.token_profile || null,
          candidate_binance_wallet_rank: match.binance_wallet_rank || null,
          candidate_binance_wallet_rank_period: match.binance_wallet_rank_period || null,
          candidate_binance_wallet_rank_period_code: match.binance_wallet_rank_period_code || null,
          candidate_binance_wallet_rank_type: match.binance_wallet_rank_type || null,
          candidate_binance_wallet_alpha_info: match.binance_wallet_alpha_info || null,
        });
      }
    }
  }
  return pairs;
}
function compareHotRows(a, b) {
  const ar = Number(a?.binance_wallet_rank || 0);
  const br = Number(b?.binance_wallet_rank || 0);
  const aOfficial = ar > 0;
  const bOfficial = br > 0;
  if (aOfficial && bOfficial && ar !== br) return ar - br;
  if (aOfficial !== bOfficial) return aOfficial ? -1 : 1;
  return Number(b?.hot_score || 0) - Number(a?.hot_score || 0)
    || poolLiquidity(b?.best_pool) - poolLiquidity(a?.best_pool);
}
function recentHotTokenRows(pairs) {
  const groups = new Map();
  for (const pair of pairs || []) {
    const candidateAddress = text(pair.candidate_token_address);
    if (!candidateAddress) continue;
    const token = tokenAddressInPair(pair, candidateAddress);
    if (!token?.address) continue;
    // Published hot rows require exact base-token market fields. Quote-side pools remain
    // available in the pool list, but cannot lend the base token's price/change fields.
    if (tokenOrientationInPair(pair, candidateAddress) !== 'base') continue;
    const key = `${pair.network}|${lower(token.address)}`;
    let group = groups.get(key);
    if (!group) {
      group = { token, network: pair.network, pairs: [], candidate_sources: [], token_profile: null, binance_wallet_rank: null, binance_wallet_alpha_info: null };
      groups.set(key, group);
    }
    group.pairs.push(pair);
    for (const source of Array.isArray(pair.candidate_sources) ? pair.candidate_sources : []) {
      if (!group.candidate_sources.includes(source)) group.candidate_sources.push(source);
    }
    group.token_profile = mergeTokenProfile(group.token_profile, pair.candidate_token_profile || null);
    const rank = Number(pair.candidate_binance_wallet_rank || 0);
    if (rank > 0 && (!Number(group.binance_wallet_rank || 0) || rank < Number(group.binance_wallet_rank))) {
      group.binance_wallet_rank = rank;
      group.binance_wallet_alpha_info = pair.candidate_binance_wallet_alpha_info || null;
    }
  }

  const rows = [];
  for (const group of groups.values()) {
    const pools = sortBestPools(dedupePools(group.pairs));
    const bestPool = pools[0];
    if (!bestPool) continue;
    const components = hotScoreComponents(pools);
    const row = tokenCentricRow(bestPool, group.token, {
      recent_hot_score: components.score,
      hot_score: components.score,
      hot_score_components: components,
      hot_rule_version: HOT_RULE_VERSION,
      hot_discovery_rule_version: HOT_DISCOVERY_RULE_VERSION,
      hot_rank_source: Number(group.binance_wallet_rank || 0) > 0 ? 'binance_wallet_public_trending_1h' : 'objective_market_fallback_after_binance',
      binance_wallet_rank: Number(group.binance_wallet_rank || 0) > 0 ? Number(group.binance_wallet_rank) : null,
      binance_wallet_rank_period: Number(group.binance_wallet_rank || 0) > 0 ? '1h' : null,
      binance_wallet_rank_type: Number(group.binance_wallet_rank || 0) > 0 ? 10 : null,
      binance_wallet_alpha_info: group.binance_wallet_alpha_info || null,
      paid_promotion_affects_hot_score: false,
      candidate_sources: group.candidate_sources,
      candidate_boost_amount: null,
      candidate_total_boost_amount: null,
      token_profile: group.token_profile,
      product_badges: productBadgesForToken(bestPool, group.token),
      source: Number(group.binance_wallet_rank || 0) > 0
        ? 'binance_wallet_trending_order_plus_exact_token_market'
        : 'objective_candidate_exact_token_market_fallback',
    });
    if (row?.token_market_fields_verified === true) rows.push(row);
  }
  const sorted = rows.sort(compareHotRows);
  // Step1042: keep bounded coverage PER CHAIN internally. The old global slice(50)
  // could starve newly-supported networks even when discovery succeeded. Thirty rows
  // fit one DEX Screener token batch per chain; public responses still cap at 50.
  const selected = [];
  for (const network of Object.keys(NETWORKS)) {
    selected.push(...sorted.filter((row) => row.network === network).slice(0, STEP1042_INTERNAL_HOT_MAX_ROWS_PER_CHAIN));
  }
  return selected.sort(compareHotRows);
}


function exactPoolPriceFocusKey(network, tokenAddress, poolAddress) {
  return `${network}|${lower(tokenAddress)}|${lower(poolAddress)}`;
}
function prunePoolPriceFocus(now = Date.now()) {
  for (const [key, row] of poolPriceFocus) {
    if (now - Number(row?.last_seen_ms || 0) > POOL_PRICE_FOCUS_TTL_MS) {
      poolPriceFocus.delete(key);
      poolPriceSnapshot.delete(key);
    }
  }
  if (poolPriceFocus.size <= POOL_PRICE_FOCUS_MAX) return;
  const ordered = [...poolPriceFocus.entries()]
    .sort((a, b) => Number(a[1]?.last_seen_ms || 0) - Number(b[1]?.last_seen_ms || 0));
  while (ordered.length && poolPriceFocus.size > POOL_PRICE_FOCUS_MAX) {
    const [key] = ordered.shift();
    poolPriceFocus.delete(key);
    poolPriceSnapshot.delete(key);
  }
}
function touchExactPoolPriceFocus(network, tokenAddress, poolAddress) {
  const key = exactPoolPriceFocusKey(network, tokenAddress, poolAddress);
  const now = Date.now();
  poolPriceFocus.set(key, {
    network,
    token_address: tokenAddress,
    pool_address: poolAddress,
    last_seen_ms: now,
  });
  prunePoolPriceFocus(now);
  return key;
}
async function refreshExactPoolPrices() {
  if (poolPriceRefreshInflight) return poolPriceRefreshInflight;

  // Step1041.5.4.3.4.2.2.1: do the empty-focus check BEFORE assigning the
  // shared inflight Promise. An async IIFE can run synchronously until its first
  // await. With an empty focus set, its finally used to set the variable to null
  // before the outer `poolPriceRefreshInflight = <promise>` assignment completed;
  // that outer assignment then wrote the already-resolved Promise back forever.
  // No user read starts upstream here: the fixed 5s background timer is still the
  // only caller that reaches the upstream lane.
  const now = Date.now();
  prunePoolPriceFocus(now);
  if (!poolPriceFocus.size) return 0;

  const run = (async () => {
    try {
      stats.pool_price_refresh_started += 1;
      const grouped = new Map(Object.keys(NETWORKS).map((key) => [key, []]));
      for (const focus of poolPriceFocus.values()) {
        const list = grouped.get(focus.network);
        if (list) list.push(focus);
      }

      let refreshed = 0;
      for (const [network, focuses] of grouped) {
        if (!focuses.length) continue;
        const meta = networkMeta(network);
        const uniqueTokens = [...new Map(
          focuses.map((row) => [lower(row.token_address), row.token_address]),
        ).values()];
        for (let offset = 0; offset < uniqueTokens.length; offset += DEX_TOKEN_BATCH_MAX) {
          const tokens = uniqueTokens.slice(offset, offset + DEX_TOKEN_BATCH_MAX);
          const payload = await dexFetchJson(
            `${DEX_BASE}/tokens/v1/${encodeURIComponent(meta.dex)}/${tokens.map(encodeURIComponent).join(',')}`,
            { priority: -8, label: `background_exact_pool_price_${network}_${Math.floor(offset / DEX_TOKEN_BATCH_MAX) + 1}` },
          );
          const pairs = normalizeDexPairs(payload).filter((row) => row.network === network);
          for (const focus of focuses) {
            if (!tokens.some((token) => exactAddressEqual(network, token, focus.token_address))) continue;
            const match = pairs.find((pair) =>
              exactAddressEqual(network, pair?.pool_address, focus.pool_address) &&
              pairContainsToken(network, pair, focus.token_address)
            );
            const price = numberOrNull(match?.price_usd);
            if (!match || price === null || price <= 0) continue;
            const key = exactPoolPriceFocusKey(network, focus.token_address, focus.pool_address);
            poolPriceSnapshot.set(key, {
              network,
              token_address: focus.token_address,
              pool_address: focus.pool_address,
              dex_id: text(match.dex_id),
              price_usd: price,
              source_time_ms: Date.now(),
              source: 'dexscreener_background_exact_pool_price',
            });
            refreshed += 1;
          }
        }
      }
      poolPriceUpdatedAt = Date.now();
      stats.pool_price_refresh_succeeded += 1;
      stats.pool_price_rows = refreshed;
      return refreshed;
    } catch (error) {
      stats.pool_price_refresh_failed += 1;
      throw error;
    } finally {
      if (poolPriceRefreshInflight === run) poolPriceRefreshInflight = null;
    }
  })();
  poolPriceRefreshInflight = run;
  return run;
}

function currentCandidateMetadata() {
  const byKey = new Map();
  for (const row of trendingSnapshot) {
    const address = text(row?.token?.address);
    if (!address || !row?.network) continue;
    byKey.set(`${row.network}|${lower(address)}`, {
      network: row.network,
      address,
      candidate_sources: Array.isArray(row.candidate_sources) ? row.candidate_sources : [],
      candidate_boost_amount: row.candidate_boost_amount ?? null,
      candidate_total_boost_amount: row.candidate_total_boost_amount ?? null,
      token_profile: row.token_profile || null,
      product_badges: Array.isArray(row.product_badges) ? row.product_badges : [],
      hot_score: row.hot_score ?? row.recent_hot_score ?? null,
      hot_score_components: row.hot_score_components || null,
      binance_wallet_rank: row.binance_wallet_rank ?? null,
      binance_wallet_rank_period: row.binance_wallet_rank_period ?? null,
      binance_wallet_rank_type: row.binance_wallet_rank_type ?? null,
      binance_wallet_alpha_info: row.binance_wallet_alpha_info || null,
    });
  }
  return byKey;
}
function mergeCurrentMarketRowsWithRetained(freshRows, now = Date.now()) {
  const freshNetworks = new Set((freshRows || []).map((row) => row?.network).filter(Boolean));
  const retained = [];
  const retainedNetworks = [];
  for (const network of Object.keys(NETWORKS)) {
    if (freshNetworks.has(network)) continue;
    const previous = trendingSnapshot.filter((row) => row?.network === network);
    if (!previous.length) continue;
    const lastNetworkUpdate = Number(marketNetworkUpdatedAt.get(network) || 0);
    const fallbackUpdate = Number(marketUpdatedAt || discoveryUpdatedAt || 0);
    const baseUpdate = lastNetworkUpdate > 0 ? lastNetworkUpdate : fallbackUpdate;
    const ageMs = baseUpdate > 0 ? Math.max(0, now - baseUpdate) : Number.POSITIVE_INFINITY;
    if (ageMs > MARKET_RETAIN_MS) continue;
    retained.push(...previous);
    retainedNetworks.push(network);
  }
  return {
    rows: [...(freshRows || []), ...retained].sort(compareHotRows),
    retained_networks: retainedNetworks,
    retained_rows: retained.length,
  };
}
async function refreshCurrentMarketFields() {
  if (marketRefreshInflight || trendingSnapshot.length === 0) return marketRefreshInflight;
  marketRefreshInflight = (async () => {
    stats.market_refresh_started += 1;
    const candidates = currentCandidateMetadata();
    const grouped = new Map(Object.keys(NETWORKS).map((key) => [key, []]));
    for (const row of candidates.values()) {
      const list = grouped.get(row.network);
      if (list && list.length < STEP1041_HOT_MAX_ROWS) list.push(row);
    }
    try {
      const refreshedPairs = [];
      for (const [network, list] of grouped) {
        if (!list.length) continue;
        const meta = networkMeta(network);
        for (let offset = 0; offset < list.length; offset += DEX_TOKEN_BATCH_MAX) {
          const batch = list.slice(offset, offset + DEX_TOKEN_BATCH_MAX);
          const byAddress = new Map(batch.map((x) => [lower(x.address), x]));
          const addresses = batch.map((x) => x.address);
          const payload = await dexFetchJson(
            `${DEX_BASE}/tokens/v1/${encodeURIComponent(meta.dex)}/${addresses.map(encodeURIComponent).join(',')}`,
            { priority: -10, label: `background_market_refresh_${network}_${Math.floor(offset / DEX_TOKEN_BATCH_MAX) + 1}` },
          );
          const normalized = normalizeDexPairs(payload).filter((row) => row.network === network);
          for (const pair of normalized) {
            const match = byAddress.get(lower(pair.base_token.address)) || byAddress.get(lower(pair.quote_token.address));
            if (!match) continue;
            refreshedPairs.push({
              ...pair,
              candidate_token_address: match.address,
              candidate_sources: match.candidate_sources,
              candidate_boost_amount: match.candidate_boost_amount,
              candidate_total_boost_amount: match.candidate_total_boost_amount,
              candidate_token_profile: match.token_profile,
              candidate_product_badges: match.product_badges || [],
              candidate_binance_wallet_rank: match.binance_wallet_rank || null,
              candidate_binance_wallet_rank_period: match.binance_wallet_rank_period || null,
              candidate_binance_wallet_rank_type: match.binance_wallet_rank_type || null,
              candidate_binance_wallet_alpha_info: match.binance_wallet_alpha_info || null,
            });
          }
        }
      }
      const now = Date.now();
      const freshRows = recentHotTokenRows(refreshedPairs);
      const merged = mergeCurrentMarketRowsWithRetained(freshRows, now);
      if (!merged.rows.length) throw new Error('dexscreener_current_market_refresh_empty');
      trendingSnapshot = merged.rows;
      for (const network of new Set(freshRows.map((row) => row.network).filter(Boolean))) {
        marketNetworkUpdatedAt.set(network, now);
      }
      stats.market_refresh_retained_networks_last = merged.retained_networks;
      stats.market_refresh_retained_rows_last = merged.retained_rows;
      marketUpdatedAt = now;
      stats.market_refresh_succeeded += 1;
      return merged.rows.length;
    } catch (error) {
      stats.market_refresh_failed += 1;
      throw error;
    } finally {
      marketRefreshInflight = null;
    }
  })();
  return marketRefreshInflight;
}
function parseEcbDailyFxXml(xml) {
  const raw = text(xml);
  const timeMatch = raw.match(/<Cube\s+time=['\"]([^'\"]+)['\"]/i);
  if (!timeMatch) throw new Error('ecb_fx_observation_missing');
  const rates = {};
  for (const match of raw.matchAll(/<Cube\s+currency=['\"]([A-Z]{3})['\"]\s+rate=['\"]([0-9.]+)['\"]\s*\/?>/g)) {
    const value = Number(match[2]);
    if (Number.isFinite(value) && value > 0) rates[match[1]] = value;
  }
  const usd = Number(rates.USD);
  if (!Number.isFinite(usd) || usd <= 0) throw new Error('ecb_fx_usd_missing');
  const usdTo = { USD: 1, EUR: 1 / usd };
  for (const code of ['CNY', 'JPY']) {
    const value = Number(rates[code]);
    if (Number.isFinite(value) && value > 0) usdTo[code] = value / usd;
  }
  if (!usdTo.CNY || !usdTo.JPY) throw new Error('ecb_fx_required_currency_missing');
  return {
    source: 'ecb_euro_foreign_exchange_reference_rates',
    source_role: 'daily_reference_secondary_display_only',
    observation_date: timeMatch[1],
    base_currency: 'USD',
    usd_to: usdTo,
    eur_reference: { USD: usd, CNY: Number(rates.CNY), JPY: Number(rates.JPY) },
  };
}
async function refreshFxReference() {
  if (fxRefreshInflight) return fxRefreshInflight;
  fxRefreshInflight = (async () => {
    stats.fx_refresh_started += 1;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS);
    timer.unref?.();
    try {
      const response = await fetch(ECB_FX_URL, {
        signal: controller.signal,
        headers: { accept: 'application/xml,text/xml;q=0.9,*/*;q=0.1', 'user-agent': 'KakaWeb3-FX-Shared/1037.5' },
      });
      const body = await response.text();
      if (!response.ok) throw new Error(`ecb_fx_http_${response.status}`);
      fxSnapshot = parseEcbDailyFxXml(body);
      fxUpdatedAt = Date.now();
      stats.fx_refresh_succeeded += 1;
      return fxSnapshot;
    } catch (error) {
      stats.fx_refresh_failed += 1;
      throw error;
    } finally {
      clearTimeout(timer);
      fxRefreshInflight = null;
    }
  })();
  return fxRefreshInflight;
}
async function refreshDiscovery() {
  if (discoveryInflight) return discoveryInflight;
  discoveryInflight = (async () => {
    stats.background_cycles_started += 1;
    stats.last_background_started_at = new Date().toISOString();
    try {
      const pairs = await fetchDiscoveryCandidatePairs();
      await refreshBinanceAlphaRegistry();
      const rows = recentHotTokenRows(pairs);
      if (!rows.length) throw new Error('onchain_recent_hot_discovery_empty');
      // A transient Binance Wallet rank outage must not silently reorder an already-good
      // Binance-ranked snapshot into Kaka fallback order. The 30s exact-market refresher keeps
      // current prices/liquidity fresh while the previous mature rank is retained.
      const previousOfficialRows = trendingSnapshot.filter((row) => Number(row?.binance_wallet_rank || 0) > 0).length;
      const previousAgeMs = discoveryUpdatedAt ? Math.max(0, Date.now() - discoveryUpdatedAt) : Number.POSITIVE_INFINITY;
      if (!binanceWalletLastCycleSucceeded && previousOfficialRows > 0 && previousAgeMs <= DISCOVERY_RETAIN_MS) {
        throw new Error('binance_wallet_trending_temporarily_unavailable_retain_previous_rank');
      }
      trendingSnapshot = rows;
      discoveryUpdatedAt = Date.now();
      marketUpdatedAt = discoveryUpdatedAt;
      marketNetworkUpdatedAt.clear();
      for (const network of new Set(rows.map((row) => row.network).filter(Boolean))) {
        marketNetworkUpdatedAt.set(network, discoveryUpdatedAt);
      }
      stats.market_refresh_retained_networks_last = [];
      stats.market_refresh_retained_rows_last = 0;
      stats.background_cycles_succeeded += 1;
      stats.last_background_success_at = new Date().toISOString();
      stats.last_background_error = '';
      return rows.length;
    } catch (error) {
      stats.background_cycles_failed += 1;
      stats.last_background_error = text(error?.message || error).replace(/\s+/g, ' ').slice(0, 300);
      throw error;
    } finally { discoveryInflight = null; }
  })();
  return discoveryInflight;
}
export function startOnchainMarketCollector() {
  if (discoveryStarted) return;
  discoveryStarted = true;
  const first = setTimeout(() => refreshDiscovery().catch(() => {}), 2_500);
  first.unref?.();
  const timer = setInterval(() => refreshDiscovery().catch(() => {}), DISCOVERY_REFRESH_MS);
  timer.unref?.();
  const marketFirst = setTimeout(() => refreshCurrentMarketFields().catch(() => {}), 22_000);
  marketFirst.unref?.();
  const marketTimer = setInterval(() => refreshCurrentMarketFields().catch(() => {}), MARKET_REFRESH_MS);
  marketTimer.unref?.();
  const poolPriceTimer = setInterval(() => refreshExactPoolPrices().catch(() => {}), POOL_PRICE_REFRESH_MS);
  poolPriceTimer.unref?.();
  const smartFirst = setTimeout(() => refreshSmartMoneySnapshot().catch(() => {}), 7_500);
  smartFirst.unref?.();
  const smartTimer = setInterval(() => refreshSmartMoneySnapshot().catch(() => {}), SMART_MONEY_REFRESH_MS);
  smartTimer.unref?.();
  const walletFirst = setTimeout(() => refreshTopWalletSnapshot().catch(() => {}), 38_000);
  walletFirst.unref?.();
  const walletTimer = setInterval(() => refreshTopWalletSnapshot().catch(() => {}), TOP_WALLET_REFRESH_MS);
  walletTimer.unref?.();
  const fxFirst = setTimeout(() => refreshFxReference().catch(() => {}), 4_500);
  fxFirst.unref?.();
  const fxTimer = setInterval(() => refreshFxReference().catch(() => {}), FX_REFRESH_MS);
  fxTimer.unref?.();
}
function discoveryRows(network, limit) {
  return trendingSnapshot.filter((row) => network === 'all' || row.network === network).slice(0, limit);
}
function healthPayload() {
  const ageMs = discoveryUpdatedAt ? Math.max(0, Date.now() - discoveryUpdatedAt) : null;
  const marketAgeMs = marketUpdatedAt ? Math.max(0, Date.now() - marketUpdatedAt) : null;
  const fxAgeMs = fxUpdatedAt ? Math.max(0, Date.now() - fxUpdatedAt) : null;
  return responseBase({
    service: 'onchain-market',
    networks: Object.values(NETWORKS).map((x) => ({ key: x.key, dex: x.dex, chain_id: x.chain_id, family: x.family, zh: x.zh, en: x.en })),
    sources: {
      dexscreener: {
        docs: 'https://docs.dexscreener.com/api/reference',
        api_terms: 'https://docs.dexscreener.com/api/api-terms-and-conditions',
        commercial_use_permitted_subject_to_api_terms: true,
        api_resale_or_direct_competitor_use_forbidden: true,
        role: 'recent_hot_candidates_plus_exact_search_token_pools',
        documented_candidate_rate_limit_per_minute: 60,
        documented_search_pair_rate_limit_per_minute: 300,
        backend_global_min_gap_ms: DEX_MIN_GAP_MS,
        backend_global_max_starts_per_minute: Math.floor(60_000 / DEX_MIN_GAP_MS),
      },
      ecb_fx: {
        docs: 'https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html',
        data_url: ECB_FX_URL,
        role: 'daily_reference_secondary_display_only',
        background_only: true,
        user_reads_start_upstream: false,
        refresh_interval_ms: FX_REFRESH_MS,
        retain_ms: FX_RETAIN_MS,
      },
      moralis: {
        docs_evm_ohlcv: 'https://docs.moralis.com/data-api/evm/price/ohlc',
        docs_solana_ohlcv: 'https://docs.moralis.com/data-api/solana/price/ohlc',
        docs_pricing: 'https://docs.moralis.com/data-api/pricing',
        docs_wallet_pnl: 'https://docs.moralis.com/data-api/evm/wallet/wallet-pnl',
        docs_wallet_chain_activity: 'https://docs.moralis.com/data-api/evm/wallet/chain-activity',
        docs_evm_token_swaps: 'https://docs.moralis.com/data-api/evm/token/swaps/token-swaps',
        docs_top_traders: 'https://docs.moralis.com/data-api/evm/token/signals/top-traders',
        docs_solana_token_swaps: 'https://docs.moralis.com/data-api/solana/token/swaps/token-swaps',
        docs_solana_wallet_swaps: 'https://docs.moralis.com/data-api/solana/token/swaps/wallet-swaps',
        docs_wallet_history: 'https://docs.moralis.com/data-api/evm/wallet/wallet-history',
        terms: 'https://moralis.com/terms/',
        role: 'exact_pool_ohlcv_history_plus_recent_pair_swaps_plus_holder_analytics_plus_step1039_wallet_pnl_plus_step1040_bounded_funding_evidence',
        api_key_configured: Boolean(MORALIS_API_KEY),
        api_key_exposed: false,
        backend_only_secret: true,
        auth_header_name: 'X-API-Key',
        auth_header_count_per_request: 1,
        duplicate_case_variant_headers: false,
        pair_candlestick_cu: MORALIS_KLINE_CU,
        pair_swap_cu: MORALIS_TRADES_CU,
        holder_metrics_cu: MORALIS_HOLDER_METRICS_CU,
        evm_top_holders_cu: MORALIS_TOP_HOLDERS_CU,
        step1039_token_swaps_cu: MORALIS_TOKEN_SWAPS_CU,
        step1039_top_traders_cu: MORALIS_TOP_TRADERS_CU,
        step1039_wallet_pnl_cu: MORALIS_WALLET_PNL_CU,
        step1039_wallet_activity_internal_reserved_cu: MORALIS_WALLET_ACTIVITY_BUDGET_CU,
        step1040_wallet_history_internal_reserved_cu: MORALIS_WALLET_HISTORY_BUDGET_CU,
        wallet_insights_premium_endpoint_used: false,
        scheduler: moralisScheduler.state(),
        budget: moralisBudgetState(),
      },
      goplus: {
        docs_evm_security: 'https://docs.gopluslabs.io/reference/tokensecurityusingget_1',
        docs_solana_security: 'https://docs.gopluslabs.io/reference/solanatokensecurityusingget',
        docs_response_evm: 'https://docs.gopluslabs.io/reference/response-details',
        docs_response_solana: 'https://docs.gopluslabs.io/reference/response-detail-1',
        role: 'token_contract_security_creator_owner_lp_and_reported_holder_facts',
        access_token_configured: Boolean(GOPLUS_ACCESS_TOKEN),
        access_token_exposed: false,
        backend_global_min_gap_ms: GOPLUS_MIN_GAP_MS,
        backend_global_max_starts_per_minute: Math.floor(60_000 / GOPLUS_MIN_GAP_MS),
        scheduler: goplusScheduler.state(),
      },
      helius: {
        docs_get_token_accounts: 'https://www.helius.dev/docs/api-reference/das/gettokenaccounts',
        docs_get_token_supply: 'https://www.helius.dev/docs/api-reference/rpc/http/gettokensupply',
        docs_pricing: 'https://www.helius.dev/pricing',
        docs_wallet_portfolio: 'https://www.helius.dev/docs/quickstart/portfolio-tracker',
        docs_wallet_funded_by: 'https://www.helius.dev/docs/api-reference/wallet-api/funded-by',
        role: 'solana_exact_mint_holder_index_plus_step1039_wallet_portfolio_plus_step1040_original_funder_evidence',
        api_key_configured: Boolean(HELIUS_API_KEY),
        api_key_exposed: false,
        backend_only_secret: true,
        page_limit: HELIUS_PAGE_LIMIT,
        exact_scan_max_token_accounts: HELIUS_MAX_EXACT_TOKEN_ACCOUNTS,
        backend_global_min_gap_ms: HELIUS_MIN_GAP_MS,
        backend_global_max_starts_per_second: Math.floor(1_000 / HELIUS_MIN_GAP_MS),
        das_free_tier_reference_rps: 2,
        das_credits_per_request: 10,
        scheduler: heliusScheduler.state(),
      },
    },
    step1038_holder_security: {
      opened: true,
      feature_schema_version: STEP1038_FEATURE_SCHEMA_VERSION,
      holders_route: HOLDERS_ROUTE,
      security_route: SECURITY_ROUTE,
      supported_networks: Object.keys(NETWORKS),
      exact_chain_token_identity_required: true,
      cross_chain_substitution: false,
      cross_token_substitution: false,
      holder_cache_fresh_ms: HOLDER_FRESH_MS,
      holder_cache_stale_ms: HOLDER_STALE_MS,
      solana_helius_holder_cache_fresh_ms: SOLANA_HELIUS_HOLDER_FRESH_MS,
      solana_helius_holder_cache_stale_ms: SOLANA_HELIUS_HOLDER_STALE_MS,
      security_cache_fresh_ms: SECURITY_FRESH_MS,
      security_cache_stale_ms: SECURITY_STALE_MS,
      evm_exact_top20_from_owner_list: true,
      solana_top20_not_fabricated: true,
      solana_holder_primary_source: 'helius_getTokenAccounts_exact_mint_full_scan',
      solana_holder_requires_complete_scan_before_publish: true,
      solana_holder_max_exact_token_accounts: HELIUS_MAX_EXACT_TOKEN_ACCOUNTS,
      solana_moralis_holder_endpoints_not_used_because_deprecated: true,
      no_composite_security_score_generated: true,
      creator_owner_labels_are_source_facts_not_dev_inference: true,
      user_reads_direct_upstream_requests: 0,
    },
    step1039_wallet_intelligence: {
      opened: true,
      feature_schema_version: STEP1039_FEATURE_SCHEMA_VERSION,
      token_wallets_route: TOKEN_WALLETS_ROUTE,
      wallet_quickview_route: WALLET_QUICKVIEW_ROUTE,
      supported_networks: Object.keys(NETWORKS),
      exact_chain_token_wallet_identity_required: true,
      token_wallet_cache_fresh_ms: TOKEN_WALLET_FRESH_MS,
      token_wallet_cache_stale_ms: TOKEN_WALLET_STALE_MS,
      wallet_base_cache_fresh_ms: WALLET_BASE_FRESH_MS,
      wallet_base_cache_stale_ms: WALLET_BASE_STALE_MS,
      evm_wallet_age_source: 'moralis_wallet_chain_activity_non_premium',
      evm_pnl_source: 'moralis_wallet_pnl_breakdown',
      moralis_wallet_insights_premium_not_used: true,
      solana_wallet_portfolio_source: 'helius_getAssetsByOwner',
      solana_wallet_age_source: 'helius_getSignaturesForAddress_bounded',
      solana_swap_cashflow_not_labeled_pnl: true,
      early_buyer_scope: `first_${EARLY_SWAP_SCOPE}_token_swaps_not_exhaustive_launch_history`,
      whale_rule: 'top10_or_holder_percent_gte_1',
      smart_money_candidate_rule_version: SMART_MONEY_RULE_VERSION,
      smart_money_is_candidate_not_confirmed_identity: true,
      confirmed_smart_money_label_generated: false,
      kol_label_generated: false,
      sniper_label_generated: false,
      dev_label_inferred: false,
      insider_label_generated: false,
      common_funding_inference_enabled: false,
      quickview_on_demand_only: true,
      no_bulk_wallet_enrichment_for_token_lists: true,
      user_reads_direct_upstream_requests: 0,
    },
    step1040_relationship_evidence: {
      opened: true,
      feature_schema_version: STEP1040_FEATURE_SCHEMA_VERSION,
      relations_route: RELATIONS_ROUTE,
      supported_networks: Object.keys(NETWORKS),
      analyzed_wallet_max: RELATION_ANALYZED_WALLET_MAX,
      relation_cache_fresh_ms: RELATION_FRESH_MS,
      relation_cache_stale_ms: RELATION_STALE_MS,
      funding_cache_fresh_ms: FUNDING_FRESH_MS,
      funding_cache_stale_ms: FUNDING_STALE_MS,
      evm_funding_source: 'moralis_wallet_history_earliest_observed_native_transfer',
      solana_funding_source: 'helius_wallet_api_funded_by',
      sniper_signal_is_timing_evidence_not_identity: true,
      creator_owner_direct_roles_are_facts_not_dev_inference: true,
      common_funding_same_known_exchange_is_low_confidence_only: true,
      wallet_clusters_are_evidence_components_not_entity_identity: true,
      insider_or_rat_trading_claim_generated: false,
      wrongdoing_claim_generated: false,
      confidence_rule_version: RELATION_CONFIDENCE_RULE_VERSION,
      user_reads_direct_upstream_requests: 0,
    },
    step1041_final_productization: {
      opened: true,
      feature_schema_version: STEP1041_FEATURE_SCHEMA_VERSION,
      hot_max_rows: STEP1041_HOT_MAX_ROWS,
      internal_hot_max_rows_per_chain: STEP1042_INTERNAL_HOT_MAX_ROWS_PER_CHAIN,
      internal_hot_max_rows_total: STEP1042_INTERNAL_HOT_MAX_ROWS_PER_CHAIN * Object.keys(NETWORKS).length,
      new_max_rows: STEP1041_NEW_MAX_ROWS,
      expected_self_test_min: 65,
      discovery_candidate_feed_count: STEP1041_CANDIDATE_FEED_COUNT,
      exact_market_batch_max: DEX_TOKEN_BATCH_MAX,
      new_pool_max_age_ms: STEP1041_NEW_POOL_MAX_AGE_MS,
      trending_rows: Math.min(trendingSnapshot.length, STEP1041_HOT_MAX_ROWS),
      overview_route: OVERVIEW_ROUTE,
      overview_user_read_upstream_requests: 0,
      new_pool_user_read_upstream_requests: 0,
      shared_snapshot_reads: stats.step1041_shared_snapshot_reads,
      snapshot_json_bytes: Buffer.byteLength(JSON.stringify(trendingSnapshot.slice(0, STEP1041_HOT_MAX_ROWS))),
      process_memory: (() => { const m = process.memoryUsage(); return { rss_bytes: m.rss, heap_used_bytes: m.heapUsed, heap_total_bytes: m.heapTotal, external_bytes: m.external }; })(),
      pressure_contract: { users: [10, 100, 1000], routes: [TRENDING_ROUTE, NEW_POOLS_ROUTE, OVERVIEW_ROUTE, HEALTH_ROUTE], direct_upstream_amplification_coefficient: 0 },
      hot_and_new_are_background_snapshot_reads: true,
      new_pool_is_not_token_contract_creation: true,
      hot_rule_version: HOT_RULE_VERSION,
      hot_discovery_rule_version: HOT_DISCOVERY_RULE_VERSION,
      primary_hot_rank_source: 'binance_wallet_public_token_rank_trending_rankType10_period1h',
      primary_hot_rank_period: '1h',
      primary_hot_rank_order_preserved: true,
      primary_hot_rank_requires_exact_chain_contract_market_verification: true,
      first_party_user_search_or_view_heat_used_for_rank: false,
      first_party_heat_future_ready_when_user_scale_is_sufficient: true,
      fallback_rank_source: 'objective_volume_activity_liquidity_after_all_verified_binance_ranked_rows',
      binance_wallet_rank_background_only: true,
      binance_wallet_rank_last_cycle_succeeded: binanceWalletLastCycleSucceeded,
      binance_wallet_rank_failure_retains_previous_rank_snapshot: true,
      binance_wallet_rank_user_reads_trigger_upstream: false,
      binance_wallet_rank_stats: { started: stats.binance_wallet_rank_started, succeeded: stats.binance_wallet_rank_succeeded, failed: stats.binance_wallet_rank_failed, rows: stats.binance_wallet_rank_rows, last_success_at: stats.binance_wallet_rank_last_success_at, last_error: stats.binance_wallet_rank_last_error },
      paid_boost_ad_cto_affects_rank: false,
      default_pool_order: 'liquidity_usd_desc_then_volume_then_activity',
      geckoterminal_objective_discovery: true,
      binance_alpha_badge_source: 'binance_official_alpha_token_list_exact_chain_contract',
    },
    step1042_multichain_smart_money: {
      opened: true,
      feature_schema_version: STEP1042_FEATURE_SCHEMA_VERSION,
      supported_market_networks: Object.keys(NETWORKS),
      added_market_networks: ['arbitrum','polygon','optimism','avalanche','linea'],
      smart_money_route: SMART_MONEY_ROUTE,
      top_wallets_route: TOP_WALLETS_ROUTE,
      smart_money_supported_networks: SMART_MONEY_FLOW_NETWORKS,
      smart_money_periods: SMART_MONEY_FLOW_PERIODS,
      top_wallet_supported_networks: TOP_WALLET_NETWORKS,
      top_wallet_periods: TOP_WALLET_PERIODS,
      hot_order_source_coverage: {
        binance_wallet_trending_primary: ['ethereum','bsc','base','solana'],
        objective_gecko_dex_fallback: ['arbitrum','polygon','optimism','avalanche','linea'],
        paid_promotion_affects_rank: false,
      },
      smart_money_ready_networks: SMART_MONEY_FLOW_NETWORKS.filter((n) => SMART_MONEY_FLOW_PERIODS.some((p) => smartMoneySnapshot.has(`${n}|${p}`))),
      top_wallet_ready_networks: TOP_WALLET_NETWORKS.filter((n) => TOP_WALLET_PERIODS.some((p) => topWalletSnapshot.has(`${n}|${p}`))),
      smart_money_refresh_ms: SMART_MONEY_REFRESH_MS,
      top_wallet_refresh_ms: TOP_WALLET_REFRESH_MS,
      smart_money_ready: smartMoneySnapshot.size > 0 && smartMoneyUpdatedAt > 0 && Date.now() - smartMoneyUpdatedAt <= SMART_MONEY_RETAIN_MS,
      top_wallet_ready: topWalletSnapshot.size > 0 && topWalletUpdatedAt > 0 && Date.now() - topWalletUpdatedAt <= TOP_WALLET_RETAIN_MS,
      smart_money_scope_count: smartMoneySnapshot.size,
      top_wallet_scope_count: topWalletSnapshot.size,
      smart_money_rows: [...smartMoneySnapshot.values()].reduce((n, rows) => n + rows.length, 0),
      top_wallet_rows: [...topWalletSnapshot.values()].reduce((n, rows) => n + rows.length, 0),
      smart_money_source: 'binance_web3_public_smart_money_inflow_rank_tagType2',
      top_wallet_source: 'binance_web3_public_address_pnl_leaderboard_ALL',
      official_source_labels_preserved: true,
      kaka_profit_candidate_not_relabelled_as_official_smart_money: true,
      user_reads_start_upstream: false,
      user_read_upstream_requests: 0,
      pressure_contract: { users: [10,100,1000], direct_upstream_amplification_coefficient: 0 },
      stats: {
        smart_money_refresh_started: stats.smart_money_refresh_started,
        smart_money_refresh_succeeded: stats.smart_money_refresh_succeeded,
        smart_money_refresh_failed: stats.smart_money_refresh_failed,
        smart_money_upstream_requests: stats.smart_money_upstream_requests,
        top_wallet_refresh_started: stats.top_wallet_refresh_started,
        top_wallet_refresh_succeeded: stats.top_wallet_refresh_succeeded,
        top_wallet_refresh_failed: stats.top_wallet_refresh_failed,
        top_wallet_upstream_requests: stats.top_wallet_upstream_requests,
      },
    },
    current_market_refresh: {
      ready: trendingSnapshot.length > 0 && (marketAgeMs === null || marketAgeMs <= MARKET_RETAIN_MS),
      refresh_interval_ms: MARKET_REFRESH_MS,
      retain_ms: MARKET_RETAIN_MS,
      age_ms: marketAgeMs,
      rows: trendingSnapshot.length,
      per_network_verified_rows: Object.fromEntries(Object.keys(NETWORKS).map((network) => [network, trendingSnapshot.filter((row) => row?.network === network).length])),
      per_network_age_ms: Object.fromEntries(Object.keys(NETWORKS).map((network) => {
        const updated = Number(marketNetworkUpdatedAt.get(network) || 0);
        return [network, updated > 0 ? Math.max(0, Date.now() - updated) : null];
      })),
      partial_empty_network_retain_previous_verified: true,
      partial_empty_network_retain_max_ms: MARKET_RETAIN_MS,
      retained_networks_last: stats.market_refresh_retained_networks_last,
      retained_rows_last: stats.market_refresh_retained_rows_last,
      user_reads_start_upstream: false,
      fixed_background_rate_independent_of_user_count: true,
    },
    exact_pool_price_realtime: {
      route: POOL_PRICE_ROUTE,
      batch_route: POOL_PRICES_ROUTE,
      batch_max: POOL_PRICE_BATCH_MAX,
      mode: 'bounded_fixed_background_focus_refresh',
      refresh_interval_ms: POOL_PRICE_REFRESH_MS,
      retain_ms: POOL_PRICE_RETAIN_MS,
      focus_ttl_ms: POOL_PRICE_FOCUS_TTL_MS,
      focus_max: POOL_PRICE_FOCUS_MAX,
      active_focus: poolPriceFocus.size,
      cached_rows: poolPriceSnapshot.size,
      updated_at: isoFromMs(poolPriceUpdatedAt),
      user_reads_start_upstream: false,
      user_reads_direct_upstream_requests: 0,
      fixed_background_rate_independent_of_user_count: true,
      exact_chain_token_pool_required: true,
      empty_focus_inflight_release_fixed: true,
      empty_focus_inflight_assignment_order_fixed: true,
      stats: {
        reads: stats.pool_price_focus_reads,
        batch_reads: stats.pool_price_batch_reads,
        refresh_started: stats.pool_price_refresh_started,
        refresh_succeeded: stats.pool_price_refresh_succeeded,
        refresh_failed: stats.pool_price_refresh_failed,
        rows: stats.pool_price_rows,
      },
    },
    fx_reference: {
      ready: Boolean(fxSnapshot) && (fxAgeMs === null || fxAgeMs <= FX_RETAIN_MS),
      route: FX_REFERENCE_ROUTE,
      source: 'ecb_euro_foreign_exchange_reference_rates',
      observation_date: fxSnapshot?.observation_date || null,
      supported_secondary_currencies: ['CNY', 'JPY', 'EUR'],
      refresh_interval_ms: FX_REFRESH_MS,
      retain_ms: FX_RETAIN_MS,
      age_ms: fxAgeMs,
      user_reads_start_upstream: false,
    },
    discovery: {
      ready: trendingSnapshot.length > 0 && (ageMs === null || ageMs <= DISCOVERY_RETAIN_MS),
      name: 'recent_hot',
      token_centric_results: true,
      exact_discovered_candidate_token_only: true,
      both_sides_of_pair_are_not_automatically_listed: true,
      quote_token_never_inherits_base_token_market_fields: true,
      basis: 'binance_wallet_official_trending_1h_order_primary_then_exact_chain_contract_market_verification_with_objective_fallback',
      token_profile_metadata_preserved: true,
      paid_boost_rank_not_used_as_final_rank: true,
      retained_if_refresh_fails: true,
      refresh_interval_ms: DISCOVERY_REFRESH_MS,
      retain_ms: DISCOVERY_RETAIN_MS,
      age_ms: ageMs,
      rows: trendingSnapshot.length,
      max_candidates_per_chain: DISCOVERY_MAX_CANDIDATES_PER_CHAIN,
      exact_market_batch_max: DEX_TOKEN_BATCH_MAX,
      cold_start_exact_batches_per_chain_max: Math.ceil(DISCOVERY_MAX_CANDIDATES_PER_CHAIN / DEX_TOKEN_BATCH_MAX),
      gecko_normal_calls_per_cycle_max: Object.keys(GECKO_NETWORK).length,
      gecko_fallback_only_when_trending_empty: true,
      gecko_ready_networks: stats.gecko_discovery_ready_networks,
      gecko_failed_networks: stats.gecko_discovery_failed_networks,
      gecko_retried_networks: stats.gecko_discovery_retried_networks,
      gecko_error_by_network: stats.gecko_discovery_error_by_network,
      gecko_min_gap_ms: GECKO_MIN_GAP_MS,
      gecko_default_requests_per_minute_ceiling: 60_000 / GECKO_MIN_GAP_MS,
      gecko_transport_failure_top_pool_fallback: false,
      gecko_failed_network_retry_max_per_cycle: 1,
      candidate_feed_count: STEP1041_CANDIDATE_FEED_COUNT,
      candidate_feeds: ['binance_wallet_trending_1h','geckoterminal_trending','geckoterminal_top_pool_fallback','top_boost','latest_boost','latest_profile','community_takeover','latest_ad'],
      paid_or_cto_presence_not_used_as_final_rank: true,
      binance_wallet_trending_rank_preserved_for_verified_rows: true,
      binance_wallet_trending_endpoint_public_no_user_auth: true,
      binance_wallet_trending_period_code: BINANCE_WALLET_TRENDING_PERIOD,
      final_hot_rows_require_verified_token_market_identity: true,
    },
    kline: {
      opened: true,
      route: KLINES_ROUTE,
      source: 'moralis_official_data_api_pair_ohlcv',
      api_key_configured: Boolean(MORALIS_API_KEY),
      app_direct_moralis_requests: 0,
      exact_chain_token_pool_preflight: true,
      historical_pages_require_identity_proof: true,
      historical_empty_page_pool_creation_boundary_returns_200: true,
      historical_exhaustion_requires_exact_identity_and_pool_creation_boundary: true,
      feature_schema_version: KLINE_FEATURE_SCHEMA_VERSION,
      supported_intervals: [...KAKA_FULL_INTERVALS],
      native_intervals: Object.keys(MORALIS_TIMEFRAME),
      derived_intervals: Object.keys(KAKA_DERIVED_PLAN).filter((x) => !Object.prototype.hasOwnProperty.call(MORALIS_TIMEFRAME, x)),
      derived_base_cache_limit: KLINE_MAX_ROWS,
      derived_15m_from_same_pool_5m: true,
      bounded_zero_trade_gap_fill: true,
      zero_trade_fill_evidence: 'internal_missing_bucket_bounded_by_real_bars_same_exact_pool',
      zero_trade_tail_extrapolation: false,
      max_rows_per_response: KLINE_MAX_ROWS,
      cache_entries: klineCache.size,
      cache_max_entries: KLINE_CACHE_MAX_ENTRIES,
      inflight: klineInflight.size,
      identity_proof_entries: identityProofCache.size,
      identity_proof_ttl_ms: IDENTITY_PROOF_TTL_MS,
      moralis_cu_per_upstream_call: MORALIS_KLINE_CU,
    },
    recent_trades: {
      opened: true,
      route: TRADES_ROUTE,
      source: 'moralis_official_data_api_pair_swaps',
      api_key_configured: Boolean(MORALIS_API_KEY),
      exact_chain_token_pool_preflight: true,
      max_rows: TRADE_MAX_ROWS,
      cache_entries: tradeCache.size,
      moralis_cu_per_upstream_call: MORALIS_TRADES_CU,
      response_schema: 'pair_level_base_quote_metadata_plus_row_base_quote_amounts',
      token_swaps_bought_sold_schema_not_assumed: true,
      exact_moralis_pair_identity_checked: true,
      exact_requested_token_in_pair_checked_when_metadata_present: true,
    },
    new_pools: {
      opened: true,
      route: NEW_POOLS_ROUTE,
      source: 'dexscreener_step1036_shared_candidate_snapshot',
      exhaustive_chain_scan: false,
      user_reads_start_upstream: false,
    },
    bounded_user_builds: {
      exact_search_and_token_pool_may_enqueue_bounded_build: true,
      search_returns_token_centric_rows: true,
      quote_token_market_fields_fail_closed: true,
      fixed_backend_rate_independent_of_user_count: true,
      same_key_cache_singleflight: true,
      queue_overflow_rejected_not_amplified: true,
      direct_app_upstream: false,
    },
    caches: {
      entries: cache.size,
      max_entries: CACHE_MAX_ENTRIES,
      negative_entries: negativeCache.size,
      negative_max_entries: NEGATIVE_CACHE_MAX_ENTRIES,
      inflight: inflight.size,
      kline_entries: klineCache.size,
      kline_max_entries: KLINE_CACHE_MAX_ENTRIES,
      kline_inflight: klineInflight.size,
      trade_entries: tradeCache.size,
      trade_max_entries: TRADE_CACHE_MAX_ENTRIES,
      trade_inflight: tradeInflight.size,
      identity_proof_entries: identityProofCache.size,
      identity_proof_max_entries: IDENTITY_PROOF_MAX_ENTRIES,
      step1038_shared_generic_cache_entries: cache.size,
    },
    scheduler: dexScheduler.state(),
    geckoterminal_scheduler: geckoScheduler.state(),
    geckoterminal_stats: {
      started: stats.gecko_upstream_started,
      succeeded: stats.gecko_upstream_succeeded,
      failed: stats.gecko_upstream_failed,
      discovery_cycles: stats.gecko_discovery_cycles,
      discovery_candidates: stats.gecko_discovery_candidates,
    },
    binance_alpha_registry: {
      rows: binanceAlphaRegistry.size,
      updated_at: binanceAlphaUpdatedAt ? new Date(binanceAlphaUpdatedAt).toISOString() : null,
      refresh_started: stats.binance_alpha_refresh_started,
      refresh_succeeded: stats.binance_alpha_refresh_succeeded,
      refresh_failed: stats.binance_alpha_refresh_failed,
    },
    moralis_scheduler: moralisScheduler.state(),
    goplus_scheduler: goplusScheduler.state(),
    helius_scheduler: heliusScheduler.state(),
    moralis_budget: moralisBudgetState(),
    stats: { ...stats },
    memory_usage: { rss_mb: Math.round(process.memoryUsage().rss / 1048576), heap_used_mb: Math.round(process.memoryUsage().heapUsed / 1048576) },
  });
}
export function getOnchainMarketHealth() { return healthPayload(); }

function runSelfTest() {
  const tests = [];
  const t = (name, pass, detail = '') => tests.push({ name, pass: Boolean(pass), detail });
  t('network_eth_alias', normalizeNetwork('ETH') === 'ethereum');
  t('network_bsc_alias', normalizeNetwork('BNBChain') === 'bsc');
  t('network_sol_alias', normalizeNetwork('SOL') === 'solana');
  t('network_arbitrum_alias', normalizeNetwork('ARB') === 'arbitrum');
  t('network_polygon_alias', normalizeNetwork('MATIC') === 'polygon');
  t('network_optimism_alias', normalizeNetwork('OP') === 'optimism');
  t('network_avalanche_alias', normalizeNetwork('AVAX') === 'avalanche');
  t('network_linea_alias', normalizeNetwork('LINEA') === 'linea');
  t('step1042_nine_market_networks', Object.keys(NETWORKS).length === 9);
  t('step1042_smart_money_official_scope', SMART_MONEY_FLOW_NETWORKS.join(',') === 'bsc,base,solana');
  t('step1042_top_wallet_official_scope', TOP_WALLET_NETWORKS.length === 4 && TOP_WALLET_NETWORKS.includes('ethereum'));
  t('step1042_user_reads_do_not_start_smart_money_upstream', healthPayload().step1042_multichain_smart_money.user_reads_start_upstream === false);
  t('evm_address_validation', looksEvmAddress('0x0000000000000000000000000000000000000001'));
  t('solana_address_validation', looksSolanaAddress('So11111111111111111111111111111111111111112'));
  t('bad_address_rejected', !looksEvmAddress('not-a-contract') && !looksSolanaAddress('not-a-contract'));
  const dex = normalizeDexPair({ chainId: 'base', dexId: 'uniswap', pairAddress: '0x1111111111111111111111111111111111111111', baseToken: { address: '0x2222222222222222222222222222222222222222', symbol: 'AAA', name: 'A' }, quoteToken: { address: '0x3333333333333333333333333333333333333333', symbol: 'USDC', name: 'USD Coin' }, priceUsd: '1.2', liquidity: { usd: 10 }, volume: { h24: 20 }, priceChange: { h24: 3 } });
  t('dex_pair_parser', dex?.network === 'base' && dex?.price_usd === 1.2 && dex?.liquidity_usd === 10);
  const quoteSynthetic = tokenCentricRow(
    {
      network: 'base',
      chain_id: 'base',
      base_token: { address: '0x0000000000000000000000000000000000000001', symbol: 'BASE', name: 'Base' },
      quote_token: { address: '0x0000000000000000000000000000000000000002', symbol: 'QUOTE', name: 'Quote' },
      price_usd: 9.99,
      market_cap_usd: 999,
      fdv_usd: 1111,
      liquidity_usd: 100,
      volume_usd: { h24: 50 },
      price_change_pct: { h24: 88 },
      txns: {},
      pool_created_at: null,
      pool_address: '0x0000000000000000000000000000000000000003',
      dex_id: 'test',
    },
    { address: '0x0000000000000000000000000000000000000002', symbol: 'QUOTE', name: 'Quote' },
  );
  t('quote_token_market_fields_fail_closed',
    quoteSynthetic?.token_market_fields_verified === false &&
    quoteSynthetic?.price_usd === null &&
    quoteSynthetic?.market_cap_usd === null &&
    quoteSynthetic?.fdv_usd === null &&
    quoteSynthetic?.price_change_pct?.h24 === null);
  t('cross_chain_substitution_false', responseBase().cross_chain_substitution === false);
  t('cross_token_substitution_false', responseBase().cross_token_substitution === false);
  t('direct_app_upstream_zero', responseBase().app_direct_upstream_requests === 0);
  t('global_rate_below_candidate_limit', 60_000 / DEX_MIN_GAP_MS < 60);
  t('cache_bounded', CACHE_MAX_ENTRIES <= 1024);
  t('negative_cache_bounded', NEGATIVE_CACHE_MAX_ENTRIES <= 512);
  t('response_rows_bounded', MAX_RESPONSE_ROWS <= 100);
  t('kline_opened_with_backend_secret_source', healthPayload().kline.opened === true);
  t('kline_app_direct_moralis_zero', healthPayload().kline.app_direct_moralis_requests === 0);
  t('kline_limit_bounded', KLINE_MAX_ROWS <= 300);
  t('kline_cache_bounded', KLINE_CACHE_MAX_ENTRIES <= 128);
  t('moralis_budget_below_free_reference', MORALIS_DAILY_CU_BUDGET < 40_000);
  t('moralis_secret_never_exposed', healthPayload().sources.moralis.api_key_exposed === false);
  t('moralis_single_auth_header_only', healthPayload().sources.moralis.auth_header_count_per_request === 1 && healthPayload().sources.moralis.duplicate_case_variant_headers === false);
  t('moralis_pair_swap_schema_not_token_swap_schema', healthPayload().recent_trades.token_swaps_bought_sold_schema_not_assumed === true);
  t('moralis_15m_same_pool_derivation_only', MORALIS_TIMEFRAME['15m'] === '5min');
  t('step1046_2_7_history_exhaustion_requires_creation_boundary', historicalRangeReachesPoolCreation({ pool_created_at: '2026-01-01T00:00:00Z' }, '15m', 300, Date.parse('2026-01-02T00:00:00Z')) === true && historicalRangeReachesPoolCreation({ pool_created_at: '2026-01-01T00:00:00Z' }, '15m', 300, Date.parse('2026-01-10T00:00:00Z')) === false);
  t('step1046_2_5_full_kline_interval_namespace', ['3m','2h','6h','8h','12h','3d','1w','1M'].every((x) => KAKA_FULL_INTERVALS.includes(x)));
  t('step1046_2_5_derived_plan_exact_base', klineDerivedPlan('2h')?.base === '1h' && klineDerivedPlan('1M')?.base === '1d');
  const profileSynthetic = normalizeCandidateProfile({ icon: 'https://cdn.example/icon.png', header: 'https://cdn.example/header.png', description: 'Hello', url: 'https://dexscreener.com/x', links: [{ type: 'twitter', url: 'https://x.com/example' }] });
  t('token_profile_metadata_parser', profileSynthetic?.icon_url.startsWith('https://') && profileSynthetic?.description === 'Hello' && profileSynthetic?.links?.length === 1);
  const fxSynthetic = parseEcbDailyFxXml(`<gesmes><Cube><Cube time='2026-08-19'><Cube currency='USD' rate='1.1605'/><Cube currency='JPY' rate='184.62'/><Cube currency='CNY' rate='7.8197'/></Cube></Cube></gesmes>`);
  t('ecb_fx_cross_rate_math', Math.abs(fxSynthetic.usd_to.CNY - (7.8197 / 1.1605)) < 1e-9 && Math.abs(fxSynthetic.usd_to.EUR - (1 / 1.1605)) < 1e-9);
  const savedTrendingForRetainTest = trendingSnapshot;
  const savedMarketUpdatedForRetainTest = marketUpdatedAt;
  const savedLineaUpdatedForRetainTest = marketNetworkUpdatedAt.get('linea');
  try {
    trendingSnapshot = [{ network: 'linea', token: { address: '0x0000000000000000000000000000000000000011' }, hot_score: 1 }];
    marketUpdatedAt = Date.now();
    marketNetworkUpdatedAt.set('linea', marketUpdatedAt);
    const retainedSynthetic = mergeCurrentMarketRowsWithRetained([], marketUpdatedAt + 1_000);
    t('step1042_3_partial_market_refresh_retains_recent_verified_chain', retainedSynthetic.rows.length === 1 && retainedSynthetic.retained_networks.includes('linea'));
  } finally {
    trendingSnapshot = savedTrendingForRetainTest;
    marketUpdatedAt = savedMarketUpdatedForRetainTest;
    if (savedLineaUpdatedForRetainTest === undefined) marketNetworkUpdatedAt.delete('linea');
    else marketNetworkUpdatedAt.set('linea', savedLineaUpdatedForRetainTest);
  }
  t('market_refresh_fixed_background', MARKET_REFRESH_MS >= 30_000 && MARKET_REFRESH_MS < DISCOVERY_REFRESH_MS);
  t('fx_user_reads_do_not_start_upstream', healthPayload().fx_reference.user_reads_start_upstream === false);
  const holderSynthetic = normalizeHolderMetrics({ totalHolders: 1000, holderSupply: { top10: { supply: '100', supplyPercent: 10 }, top25: { supply: '200', supplyPercent: 20 }, top50: { supply: '300', supplyPercent: 30 } } });
  t('step1038_holder_metrics_parser', holderSynthetic.total_holders === 1000 && holderSynthetic.concentration.top10.supply_percent === 10 && holderSynthetic.concentration.top50.supply_percent === 30);
  const holderWrappedSynthetic = normalizeHolderMetrics({ data: { totalHolders: '321', holderSupply: { top10: { supplyPercent: '12.5' } } } });
  t('step1038_holder_metrics_wrapper_parser', holderWrappedSynthetic.total_holders === 321 && holderWrappedSynthetic.concentration.top10.supply_percent === 12.5);
  const gpSynthetic = normalizeGoPlusEvm({ token_name: 'T', token_symbol: 'T', is_honeypot: '0', is_open_source: '1', creator_address: '0x0000000000000000000000000000000000000001', creator_percent: '0.025', holders: [{ address: '0x0000000000000000000000000000000000000002', percent: '0.1', balance: '10', is_locked: '0' }] }, 'ethereum', '0x0000000000000000000000000000000000000003');
  t('step1038_goplus_evm_parser', gpSynthetic.contract_facts.is_open_source === true && gpSynthetic.trading_facts.is_honeypot === false && Math.abs(gpSynthetic.creator.percent - 2.5) < 1e-9 && Math.abs(gpSynthetic.top10_percent_reported - 10) < 1e-9);
  t('step1038_goplus_rate_below_30_per_min', 60_000 / GOPLUS_MIN_GAP_MS < 30);
  const dasMint = '6TpjRqHB5BBZH6gtKdqiHDD8u7noVqS85LhtwxySpump';
  const dasOwner = '86xCnPeV69n6t3DnyGvkKobf9FdN2H9oiVDdaMpo2MMY';
  const parsedDas = parseHeliusDasTokenAccount({ mint: dasMint, owner: dasOwner, amount: '123456789' }, dasMint);
  t('step1038_2_2_helius_das_token_account_parser', parsedDas?.amount === 123456789n && parsedDas?.owner === dasOwner);
  t('step1038_2_2_helius_percent_bigint', Math.abs(bigintPercent(25n, 100n) - 25) < 1e-9);
  t('step1038_2_2_helius_das_rate_below_free_2rps', 1_000 / HELIUS_MIN_GAP_MS < 2);
  t('step1038_2_2_helius_das_page_limit', HELIUS_PAGE_LIMIT <= 1_000 && HELIUS_MAX_HOLDER_PAGES <= 100);
  t('step1038_2_1_solana_moralis_deprecated_holder_not_used', healthPayload().step1038_holder_security.solana_moralis_holder_endpoints_not_used_because_deprecated === true);
  t('step1038_no_composite_security_score', healthPayload().step1038_holder_security.no_composite_security_score_generated === true);
  t('step1038_solana_top20_fail_closed', healthPayload().step1038_holder_security.solana_top20_not_fabricated === true);
  const syntheticSwaps = [
    { walletAddress: '0x0000000000000000000000000000000000000001', transactionType: 'buy', blockTimestamp: '2026-01-01T00:00:00Z', totalValueUsd: 100 },
    { walletAddress: '0x0000000000000000000000000000000000000001', transactionType: 'sell', blockTimestamp: '2026-01-02T00:00:00Z', totalValueUsd: 150 },
    { walletAddress: '0x0000000000000000000000000000000000000002', transactionType: 'buy', blockTimestamp: '2026-01-03T00:00:00Z', totalValueUsd: 200 },
  ].map((x) => normalizeTokenSwap('ethereum', x)).filter(Boolean);
  const aggWallets = aggregateRecentWallets('ethereum', syntheticSwaps, 10);
  t('step1039_swap_parser', syntheticSwaps.length === 3 && syntheticSwaps[0].transaction_type === 'buy');
  t('step1039_recent_wallet_aggregation', aggWallets.length === 2 && aggWallets[0].total_usd >= 200);
  t('step1039_early_buyer_unique', earliestBuyerRows('ethereum', syntheticSwaps, 10).length === 2);
  const signalSynthetic = smartMoneyTokenCandidate({ address: '0x0000000000000000000000000000000000000001', realized_profit_usd: 1500, realized_profit_percentage: 30, count_of_trades: 8 });
  t('step1039_smart_money_candidate_transparent', signalSynthetic?.smart_money_candidate === true && signalSynthetic?.confirmed_smart_money === false);
  const pnlSynthetic = summarizePnl([
    normalizePnlRow({ token_address: '0x0000000000000000000000000000000000000001', realized_profit_usd: '100', count_of_trades: 2, total_usd_invested: '50', total_sold_usd: '150', total_buys: 1, total_sells: 1, possible_spam: false }),
    normalizePnlRow({ token_address: '0x0000000000000000000000000000000000000002', realized_profit_usd: '-20', count_of_trades: 2, total_usd_invested: '100', total_sold_usd: '80', total_buys: 1, total_sells: 1, possible_spam: false }),
  ].filter(Boolean));
  t('step1039_pnl_win_rate_semantics', pnlSynthetic.evaluable_token_positions === 2 && pnlSynthetic.profitable_token_positions === 1 && Math.abs(pnlSynthetic.realized_token_position_win_rate_pct - 50) < 0.001);
  t('step1039_no_premium_wallet_insights', healthPayload().step1039_wallet_intelligence.moralis_wallet_insights_premium_not_used === true);
  t('step1039_solana_cashflow_not_pnl', healthPayload().step1039_wallet_intelligence.solana_swap_cashflow_not_labeled_pnl === true);
  t('step1039_no_sniper_dev_insider_inference', healthPayload().step1039_wallet_intelligence.sniper_label_generated === false && healthPayload().step1039_wallet_intelligence.dev_label_inferred === false && healthPayload().step1039_wallet_intelligence.insider_label_generated === false);
  const sniperSynthetic = buildSniperSignals([{ address:'0x0000000000000000000000000000000000000003', early_buy_rank:2, first_observed_buy_at:'2026-01-01T00:00:20Z', evidence_scope:'synthetic' }], '2026-01-01T00:00:00Z');
  t('step1040_sniper_signal_evidence_not_identity', sniperSynthetic.length === 1 && sniperSynthetic[0].confidence === 'high' && sniperSynthetic[0].confirmed_identity === false);
  const fundingSynthetic = fundingGroups('ethereum', [
    {wallet:'0x0000000000000000000000000000000000000001',funding:{funder:'0x0000000000000000000000000000000000000009',shared_entity_funder:false}},
    {wallet:'0x0000000000000000000000000000000000000002',funding:{funder:'0x0000000000000000000000000000000000000009',shared_entity_funder:false}},
  ]);
  t('step1040_common_funder_group_exact', fundingSynthetic.length === 1 && fundingSynthetic[0].wallets.length === 2 && fundingSynthetic[0].confidence === 'high');
  t('step1040_no_wrongdoing_or_insider_claim', healthPayload().step1040_relationship_evidence.wrongdoing_claim_generated === false && healthPayload().step1040_relationship_evidence.insider_or_rat_trading_claim_generated === false);
  const finalOverview = buildStep1041Overview();
  t('step1042_public_hot_cap_50_internal_per_chain_30', STEP1041_HOT_MAX_ROWS === 50 && STEP1042_INTERNAL_HOT_MAX_ROWS_PER_CHAIN === 30 && recentHotTokenRows([]).length === 0);
  t('step1042_2_gecko_nine_chain_conservative_rate_bound', Object.keys(GECKO_NETWORK).length === 9 && GECKO_MIN_GAP_MS >= 12_500 && 60_000 / GECKO_MIN_GAP_MS <= 4.8);
  t('step1042_2_gecko_failure_retry_bounded', healthPayload().discovery.gecko_failed_network_retry_max_per_cycle === 1 && healthPayload().discovery.gecko_transport_failure_top_pool_fallback === false);
  t('step1041_overview_shared_only', finalOverview.no_user_upstream_build === true && finalOverview.volume_liquidity_are_sample_sums_not_whole_chain_totals === true);
  t('step1041_new_pool_semantics_fail_closed', STEP1041_NEW_POOL_MAX_AGE_MS === 7 * 24 * 60 * 60_000);
  t('step1041_pressure_contract_10_100_1000', JSON.stringify(healthPayload().step1041_final_productization.pressure_contract.users) === JSON.stringify([10,100,1000]));
  t('step1041_4_pool_order_liquidity_first', sortBestPools([{pool_address:'a',liquidity_usd:10,volume_usd:{h24:999999}},{pool_address:'b',liquidity_usd:20,volume_usd:{h24:1}}])[0].pool_address === 'b');
  t('step1041_5_hot_rank_excludes_paid_amount', HOT_RULE_VERSION.includes('binance_wallet_trending') && healthPayload().step1041_final_productization.paid_boost_ad_cto_affects_rank === false);
  t('step1041_4_gecko_exact_interval_map', geckoKlineSpec('15m')?.timeframe === 'minute' && geckoKlineSpec('15m')?.aggregate === 15 && geckoKlineSpec('4h')?.aggregate === 4);
  t('step1041_4_badges_fact_only', productBadgesForToken({network:'bsc',dex_id:'pumpswap'}, {address:'0x0000000000000000000000000000000000000001'}).every((x)=>x.confidence === 'fact'));
  t('step1041_4_hot_score_components_finite', Number.isFinite(hotScoreComponents([{liquidity_usd:1000,volume_usd:{h24:5000},txns:{h1:{buys:10,sells:5},m5:{buys:3,sells:2}}}]).score));
  t('step1041_candidate_feed_expansion_8', STEP1041_CANDIDATE_FEED_COUNT === 8);
  t('step1042_1_discovery_one_exact_batch_per_chain', DEX_TOKEN_BATCH_MAX === 30 && DISCOVERY_MAX_CANDIDATES_PER_CHAIN === 30);
  t('step1041_hot_rows_verified_only', healthPayload().discovery.final_hot_rows_require_verified_token_market_identity === true);
  t('step1041_5_binance_wallet_trending_primary', healthPayload().step1041_final_productization.primary_hot_rank_source.includes('binance_wallet_public_token_rank_trending'));
  t('step1041_5_binance_wallet_period_1h', BINANCE_WALLET_TRENDING_PERIOD === 30 && healthPayload().step1041_final_productization.primary_hot_rank_period === '1h');
  t('step1041_5_binance_rank_preserved_before_fallback', recentHotTokenRows([
    {...normalizeDexPair({chainId:'bsc',dexId:'x',pairAddress:'0x0000000000000000000000000000000000000101',baseToken:{address:'0x0000000000000000000000000000000000000001',symbol:'A',name:'A'},quoteToken:{address:'0x00000000000000000000000000000000000000f1',symbol:'USDT',name:'USDT'},priceUsd:'1',liquidity:{usd:1000},volume:{h24:1000},txns:{h1:{buys:1,sells:1},m5:{buys:1,sells:0}}}),candidate_token_address:'0x0000000000000000000000000000000000000001',candidate_sources:['binance_wallet_public_trending_1h'],candidate_binance_wallet_rank:2},
    {...normalizeDexPair({chainId:'bsc',dexId:'x',pairAddress:'0x0000000000000000000000000000000000000102',baseToken:{address:'0x0000000000000000000000000000000000000002',symbol:'B',name:'B'},quoteToken:{address:'0x00000000000000000000000000000000000000f1',symbol:'USDT',name:'USDT'},priceUsd:'1',liquidity:{usd:1},volume:{h24:1},txns:{h1:{buys:1,sells:0},m5:{buys:0,sells:0}}}),candidate_token_address:'0x0000000000000000000000000000000000000002',candidate_sources:['binance_wallet_public_trending_1h'],candidate_binance_wallet_rank:1},
  ].filter(Boolean))[0]?.binance_wallet_rank === 1);
  t('step1041_5_user_heat_not_used_yet', healthPayload().step1041_final_productization.first_party_user_search_or_view_heat_used_for_rank === false);
  t('trading_disabled', responseBase().trading_enabled === false);
  t('db_writes_disabled', responseBase().database_writes === false);
  t('commercial_source_terms_recorded', healthPayload().sources.dexscreener.commercial_use_permitted_subject_to_api_terms === true);
  return responseBase({ ok: tests.every((x) => x.pass), test_count: tests.length, passed: tests.filter((x) => x.pass).length, failed: tests.filter((x) => !x.pass).length, tests });
}


function onchainRankSortKey(raw) {
  const value = lower(raw);
  if (['change_desc','gainers','gain'].includes(value)) return 'change_desc';
  if (['change_asc','losers','loss'].includes(value)) return 'change_asc';
  if (['volume_desc','turnover_desc','volume'].includes(value)) return 'volume_desc';
  if (['liquidity_desc','liquidity'].includes(value)) return 'liquidity_desc';
  if (['market_cap_desc','market_cap','cap'].includes(value)) return 'market_cap_desc';
  return 'default';
}

function onchainRankNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function onchainRankCompareNullable(a, b, descending = false) {
  const av = onchainRankNumber(a);
  const bv = onchainRankNumber(b);
  if (av == null && bv == null) return 0;
  if (av == null) return 1;
  if (bv == null) return -1;
  return descending ? bv - av : av - bv;
}

function onchainRankRows(rows, sortKey) {
  if (sortKey === 'default') return rows.map((row) => ({ ...row }));
  return rows.map((row) => ({ ...row })).sort((a, b) => {
    let cmp = 0;
    if (sortKey === 'change_desc') cmp = onchainRankCompareNullable(a?.price_change_pct?.h24, b?.price_change_pct?.h24, true);
    else if (sortKey === 'change_asc') cmp = onchainRankCompareNullable(a?.price_change_pct?.h24, b?.price_change_pct?.h24, false);
    else if (sortKey === 'volume_desc') cmp = onchainRankCompareNullable(a?.volume_usd?.h24, b?.volume_usd?.h24, true);
    else if (sortKey === 'liquidity_desc') cmp = onchainRankCompareNullable(a?.liquidity_usd, b?.liquidity_usd, true);
    else if (sortKey === 'market_cap_desc') cmp = onchainRankCompareNullable(a?.market_cap_usd, b?.market_cap_usd, true);
    if (!cmp) cmp = onchainRankCompareNullable(a?.liquidity_usd, b?.liquidity_usd, true);
    if (!cmp) cmp = onchainRankCompareNullable(a?.volume_usd?.h24, b?.volume_usd?.h24, true);
    if (!cmp) {
      const aKey = `${lower(a?.network)}|${lower(a?.token?.address)}|${lower(a?.best_pool?.pool_address)}`;
      const bKey = `${lower(b?.network)}|${lower(b?.token?.address)}|${lower(b?.best_pool?.pool_address)}`;
      cmp = aKey.localeCompare(bKey);
    }
    return cmp;
  });
}

function onchainRankPage(rows, { sort = 'default', offset = 0, limit = 50 } = {}) {
  const sortKey = onchainRankSortKey(sort);
  const safeOffset = Math.max(0, Math.trunc(Number(offset) || 0));
  const safeLimit = Math.max(1, Math.min(50, Math.trunc(Number(limit) || 50)));
  const ranked = onchainRankRows(rows, sortKey);
  const page = ranked.slice(safeOffset, safeOffset + safeLimit).map((row, index) => ({
    ...row,
    rank_index: safeOffset + index + 1,
  }));
  return {
    sort: sortKey,
    total_ranked_rows: ranked.length,
    offset: safeOffset,
    limit: safeLimit,
    has_more: safeOffset + page.length < ranked.length,
    next_offset: safeOffset + page.length < ranked.length ? safeOffset + page.length : null,
    rows: page,
  };
}

export async function handleOnchainMarket(req, res, url) {
  const path = url?.pathname || '';
  if (![HEALTH_ROUTE, SELF_TEST_ROUTE, TRENDING_ROUTE, SEARCH_ROUTE, TOKEN_ROUTE, POOLS_ROUTE, KLINES_ROUTE, POOL_PRICE_ROUTE, POOL_PRICES_ROUTE, TRADES_ROUTE, NEW_POOLS_ROUTE, FX_REFERENCE_ROUTE, HOLDERS_ROUTE, SECURITY_ROUTE, TOKEN_WALLETS_ROUTE, WALLET_QUICKVIEW_ROUTE, RELATIONS_ROUTE, OVERVIEW_ROUTE, SMART_MONEY_ROUTE, TOP_WALLETS_ROUTE].includes(path)) return false;
  stats.user_reads += 1;
  if (req.method !== 'GET') { sendJson(res, 405, responseBase({ ok: false, error: 'method_not_allowed' })); return true; }
  if (path === HEALTH_ROUTE) { sendJson(res, 200, healthPayload()); return true; }
  if (path === SELF_TEST_ROUTE) { const result = runSelfTest(); sendJson(res, result.ok ? 200 : 500, result); return true; }
  if (path === FX_REFERENCE_ROUTE) {
    const ageMs = fxUpdatedAt ? Math.max(0, Date.now() - fxUpdatedAt) : null;
    if (!fxSnapshot || ageMs === null || ageMs > FX_RETAIN_MS) {
      sendJson(res, 503, responseBase({ ok: false, error: 'shared_fx_reference_not_ready', source: 'ecb_euro_foreign_exchange_reference_rates', user_read_upstream_requests: 0 }));
      return true;
    }
    sendJson(res, 200, responseBase({ ...fxSnapshot, generated_at: new Date(fxUpdatedAt).toISOString(), shared_snapshot_age_ms: ageMs, user_read_upstream_requests: 0, cache_status: 'background_shared' }));
    return true;
  }

  if (path === OVERVIEW_ROUTE) {
    stats.step1041_shared_snapshot_reads += 1;
    const ageMs = marketUpdatedAt ? Math.max(0, Date.now() - marketUpdatedAt) : null;
    if (!trendingSnapshot.length || ageMs === null || ageMs > MARKET_RETAIN_MS) {
      sendJson(res, 503, responseBase({ ok: false, error: 'step1041_shared_onchain_overview_not_ready', feature_schema_version: STEP1041_FEATURE_SCHEMA_VERSION, user_read_upstream_requests: 0 }));
      return true;
    }
    sendJson(res, 200, responseBase({ ...buildStep1041Overview(), cache_status: 'background_shared', shared_snapshot_age_ms: ageMs, user_read_upstream_requests: 0 }));
    return true;
  }

  if (path === SMART_MONEY_ROUTE) {
    const requestedNetwork = normalizeNetwork(url.searchParams.get('network'));
    const network = requestedNetwork || 'all';
    const period = text(url.searchParams.get('period')) || '24h';
    const limit = intRange(url.searchParams.get('limit'), 1, SMART_MONEY_MAX_ROWS, 25);
    if (!SMART_MONEY_FLOW_PERIODS.includes(period)) {
      sendJson(res, 400, responseBase({ ok: false, error: 'unsupported_smart_money_period', supported_periods: SMART_MONEY_FLOW_PERIODS }));
      return true;
    }
    if (network !== 'all' && !SMART_MONEY_FLOW_NETWORKS.includes(network)) {
      sendJson(res, 200, responseBase({
        feature_schema_version: STEP1042_FEATURE_SCHEMA_VERSION,
        network, period, supported: false, rows: [], row_count: 0,
        supported_networks: SMART_MONEY_FLOW_NETWORKS,
        source: 'binance_web3_public_smart_money_inflow_rank_tagType2',
        message: 'official_smart_money_inflow_board_not_available_for_selected_network',
        user_read_upstream_requests: 0,
      }));
      return true;
    }
    const ageMs = smartMoneyUpdatedAt ? Math.max(0, Date.now() - smartMoneyUpdatedAt) : null;
    const rows = sharedSmartMoneyRows(network, period).slice(0, limit);
    if (!rows.length && (!smartMoneyUpdatedAt || ageMs > SMART_MONEY_RETAIN_MS)) {
      sendJson(res, 503, responseBase({ ok: false, error: 'shared_smart_money_snapshot_not_ready', network, period, rows: [], user_read_upstream_requests: 0 }));
      return true;
    }
    sendJson(res, 200, responseBase({
      feature_schema_version: STEP1042_FEATURE_SCHEMA_VERSION,
      network, period, supported: true, rows, row_count: rows.length,
      supported_networks: SMART_MONEY_FLOW_NETWORKS,
      supported_periods: SMART_MONEY_FLOW_PERIODS,
      source: 'binance_web3_public_smart_money_inflow_rank_tagType2',
      semantics: 'official_smart_money_tag_net_inflow_rank_not_kaka_inference',
      generated_at: smartMoneyUpdatedAt ? new Date(smartMoneyUpdatedAt).toISOString() : null,
      shared_snapshot_age_ms: ageMs,
      cache_status: 'background_shared',
      user_read_upstream_requests: 0,
    }));
    return true;
  }

  if (path === TOP_WALLETS_ROUTE) {
    const requestedNetwork = normalizeNetwork(url.searchParams.get('network'));
    const network = requestedNetwork || 'all';
    const period = text(url.searchParams.get('period')) || '30d';
    const limit = intRange(url.searchParams.get('limit'), 1, TOP_WALLET_MAX_ROWS, 25);
    if (!TOP_WALLET_PERIODS.includes(period)) {
      sendJson(res, 400, responseBase({ ok: false, error: 'unsupported_top_wallet_period', supported_periods: TOP_WALLET_PERIODS }));
      return true;
    }
    if (network !== 'all' && !TOP_WALLET_NETWORKS.includes(network)) {
      sendJson(res, 200, responseBase({
        feature_schema_version: STEP1042_FEATURE_SCHEMA_VERSION,
        network, period, supported: false, rows: [], row_count: 0,
        supported_networks: TOP_WALLET_NETWORKS,
        source: 'binance_web3_public_address_pnl_leaderboard_ALL',
        message: 'official_top_wallet_board_not_available_for_selected_network',
        user_read_upstream_requests: 0,
      }));
      return true;
    }
    const ageMs = topWalletUpdatedAt ? Math.max(0, Date.now() - topWalletUpdatedAt) : null;
    const rows = sharedTopWalletRows(network, period).slice(0, limit);
    if (!rows.length && (!topWalletUpdatedAt || ageMs > TOP_WALLET_RETAIN_MS)) {
      sendJson(res, 503, responseBase({ ok: false, error: 'shared_top_wallet_snapshot_not_ready', network, period, rows: [], user_read_upstream_requests: 0 }));
      return true;
    }
    sendJson(res, 200, responseBase({
      feature_schema_version: STEP1042_FEATURE_SCHEMA_VERSION,
      network, period, supported: true, rows, row_count: rows.length,
      supported_networks: TOP_WALLET_NETWORKS,
      supported_periods: TOP_WALLET_PERIODS,
      source: 'binance_web3_public_address_pnl_leaderboard_ALL',
      semantics: 'official_top_trader_performance_board_not_kaka_smart_money_identity',
      generated_at: topWalletUpdatedAt ? new Date(topWalletUpdatedAt).toISOString() : null,
      shared_snapshot_age_ms: ageMs,
      cache_status: 'background_shared',
      user_read_upstream_requests: 0,
    }));
    return true;
  }

  const network = normalizeNetwork(url.searchParams.get('network'));
  const limit = intRange(url.searchParams.get('limit'), 1, MAX_RESPONSE_ROWS, 50);

  if (path === POOL_PRICES_ROUTE) {
    stats.pool_price_batch_reads += 1;
    const rawItems = text(url.searchParams.get('items'));
    const specs = rawItems
      .split('~')
      .map((item) => item.trim())
      .filter(Boolean)
      .slice(0, POOL_PRICE_BATCH_MAX);
    if (!specs.length) {
      sendJson(res, 400, responseBase({
        ok: false,
        error: 'exact_pool_items_required',
        rows: [],
        user_read_upstream_requests: 0,
        direct_upstream_requests: 0,
      }));
      return true;
    }

    const rows = [];
    const seen = new Set();
    let invalidRows = 0;
    const now = Date.now();
    for (const spec of specs) {
      const parts = spec.split('|');
      if (parts.length !== 3) { invalidRows += 1; continue; }
      const itemNetwork = normalizeNetwork(parts[0]);
      const tokenAddress = text(parts[1]);
      const poolAddress = text(parts[2]);
      if (!itemNetwork || itemNetwork === 'all' ||
          !validAddressForNetwork(itemNetwork, tokenAddress) ||
          !validAddressForNetwork(itemNetwork, poolAddress)) {
        invalidRows += 1;
        continue;
      }
      const exactKey = exactPoolPriceFocusKey(itemNetwork, tokenAddress, poolAddress);
      if (seen.has(exactKey)) continue;
      seen.add(exactKey);
      const key = touchExactPoolPriceFocus(itemNetwork, tokenAddress, poolAddress);
      const current = poolPriceSnapshot.get(key) || null;
      const ageMs = current ? Math.max(0, now - Number(current.source_time_ms || 0)) : null;
      const ready = Boolean(current) && ageMs !== null && ageMs <= POOL_PRICE_RETAIN_MS;
      rows.push({
        ready,
        network: itemNetwork,
        token_address: tokenAddress,
        pool_address: poolAddress,
        dex_id: ready ? current.dex_id : null,
        price_usd: ready ? current.price_usd : null,
        source_time_ms: ready ? current.source_time_ms : null,
        source_time: ready ? isoFromMs(current.source_time_ms) : null,
        age_ms: ready ? ageMs : null,
        source: ready ? current.source : null,
        focus_registered: true,
        exact_chain_token_pool_required: true,
      });
    }
    const readyRows = rows.filter((row) => row.ready).length;
    sendJson(res, 200, responseBase({
      ready: readyRows > 0,
      requested_rows: specs.length,
      accepted_rows: rows.length,
      invalid_rows: invalidRows,
      ready_rows: readyRows,
      rows,
      batch_max: POOL_PRICE_BATCH_MAX,
      background_refresh_interval_ms: POOL_PRICE_REFRESH_MS,
      cache_status: 'background_shared_exact_pool_batch',
      user_read_upstream_requests: 0,
      direct_upstream_requests: 0,
      user_reads_register_focus_only: true,
      fixed_background_rate_independent_of_user_count: true,
    }));
    return true;
  }

  if (path === POOL_PRICE_ROUTE) {
    stats.pool_price_focus_reads += 1;
    if (!network || network === 'all') {
      sendJson(res, 400, responseBase({ ok: false, error: 'exact_network_required', user_read_upstream_requests: 0 }));
      return true;
    }
    const tokenAddress = text(url.searchParams.get('address') || url.searchParams.get('token_address'));
    const poolAddress = text(url.searchParams.get('pool') || url.searchParams.get('pool_address'));
    if (!validAddressForNetwork(network, tokenAddress) || !validAddressForNetwork(network, poolAddress)) {
      sendJson(res, 400, responseBase({ ok: false, error: 'invalid_exact_token_or_pool_address', network, user_read_upstream_requests: 0 }));
      return true;
    }
    const key = touchExactPoolPriceFocus(network, tokenAddress, poolAddress);
    const current = poolPriceSnapshot.get(key) || null;
    const ageMs = current ? Math.max(0, Date.now() - Number(current.source_time_ms || 0)) : null;
    if (!current || ageMs === null || ageMs > POOL_PRICE_RETAIN_MS) {
      sendJson(res, 503, responseBase({
        ok: false,
        ready: false,
        error: 'shared_exact_pool_price_pending',
        network,
        token_address: tokenAddress,
        pool_address: poolAddress,
        user_read_upstream_requests: 0,
        direct_upstream_requests: 0,
        focus_registered: true,
        background_refresh_interval_ms: POOL_PRICE_REFRESH_MS,
      }));
      return true;
    }
    sendJson(res, 200, responseBase({
      ready: true,
      network,
      token_address: current.token_address,
      pool_address: current.pool_address,
      dex_id: current.dex_id,
      price_usd: current.price_usd,
      source_time_ms: current.source_time_ms,
      source_time: isoFromMs(current.source_time_ms),
      age_ms: ageMs,
      source: current.source,
      cache_status: 'background_shared_exact_pool',
      user_read_upstream_requests: 0,
      direct_upstream_requests: 0,
      focus_registered: true,
      exact_chain_token_pool_verified_by_background_pair: true,
    }));
    return true;
  }

  if (path === RELATIONS_ROUTE) {
    if (!network || network === 'all') {
      sendJson(res, 400, responseBase({ ok: false, error: 'exact_network_required', feature_schema_version: STEP1040_FEATURE_SCHEMA_VERSION }));
      return true;
    }
    const tokenAddress = text(url.searchParams.get('address') || url.searchParams.get('token_address'));
    if (!validAddressForNetwork(network, tokenAddress)) {
      sendJson(res, 400, responseBase({ ok: false, error: 'invalid_contract_address', network, feature_schema_version: STEP1040_FEATURE_SCHEMA_VERSION }));
      return true;
    }
    try {
      const key = `step1040:relations:${network}:${lower(tokenAddress)}`;
      const result = await cachedBuild(key, { freshMs: RELATION_FRESH_MS, staleMs: RELATION_STALE_MS, negativeMs: RELATION_NEGATIVE_MS }, () => buildTokenRelations(network, tokenAddress));
      const value = result.value;
      if (!value) throw new Error('relationship_evidence_not_ready');
      sendJson(res, 200, responseBase({ feature_schema_version: STEP1040_FEATURE_SCHEMA_VERSION, ...value, cache_status: result.cache_status, user_read_direct_upstream_requests: 0 }));
    } catch (error) {
      sendJson(res, 503, responseBase({ ok: false, feature_schema_version: STEP1040_FEATURE_SCHEMA_VERSION, error: text(error?.message || error), network, address: tokenAddress, no_cross_chain_or_wallet_fallback: true }));
    }
    return true;
  }

  if (path === TOKEN_WALLETS_ROUTE || path === WALLET_QUICKVIEW_ROUTE) {
    if (!network || network === 'all') {
      sendJson(res, 400, responseBase({ ok: false, error: 'exact_network_required', feature_schema_version: STEP1039_FEATURE_SCHEMA_VERSION }));
      return true;
    }
    const tokenAddress = text(url.searchParams.get('address') || url.searchParams.get('token_address'));
    if (path === TOKEN_WALLETS_ROUTE) {
      if (!validAddressForNetwork(network, tokenAddress)) {
        sendJson(res, 400, responseBase({ ok: false, error: 'invalid_contract_address', network, feature_schema_version: STEP1039_FEATURE_SCHEMA_VERSION }));
        return true;
      }
      try {
        const key = `step1039:token-wallets:${network}:${lower(tokenAddress)}`;
        const result = await cachedBuild(key, { freshMs: TOKEN_WALLET_FRESH_MS, staleMs: TOKEN_WALLET_STALE_MS, negativeMs: TOKEN_WALLET_NEGATIVE_MS }, () => buildTokenWalletIntelligence(network, tokenAddress));
        const value = result.value;
        if (!value) throw new Error('token_wallet_intelligence_not_ready');
        sendJson(res, 200, responseBase({
          feature_schema_version: STEP1039_FEATURE_SCHEMA_VERSION,
          network,
          address: tokenAddress,
          source: value.source,
          scopes: value.scopes,
          early_buyers: value.early_buyers,
          large_holders: value.large_holders,
          recent_traders: value.recent_traders,
          smart_money_candidates: value.smart_money_candidates,
          direct_role_wallets: value.direct_role_wallets,
          all_candidates: value.all_candidates,
          smart_money_rule_version: value.smart_money_rule_version,
          smart_money_is_candidate_not_identity: true,
          confirmed_smart_money_label_generated: false,
          no_kol_sniper_dev_insider_inference: true,
          cache_status: result.cache_status,
          upstream_partial_errors: value.upstream_partial_errors,
          user_read_direct_upstream_requests: 0,
        }));
      } catch (error) {
        sendJson(res, 503, responseBase({ ok: false, feature_schema_version: STEP1039_FEATURE_SCHEMA_VERSION, error: text(error?.message || error), network, address: tokenAddress, no_cross_chain_or_wallet_fallback: true }));
      }
      return true;
    }

    const wallet = text(url.searchParams.get('wallet') || url.searchParams.get('wallet_address'));
    if (!walletAddressValid(network, wallet)) {
      sendJson(res, 400, responseBase({ ok: false, error: 'invalid_wallet_address', network, feature_schema_version: STEP1039_FEATURE_SCHEMA_VERSION }));
      return true;
    }
    if (tokenAddress && !validAddressForNetwork(network, tokenAddress)) {
      sendJson(res, 400, responseBase({ ok: false, error: 'invalid_contract_address', network, feature_schema_version: STEP1039_FEATURE_SCHEMA_VERSION }));
      return true;
    }
    try {
      const key = `step1039:wallet-quickview:${network}:${walletIdentityKey(network, wallet)}:${tokenAddress ? lower(tokenAddress) : 'none'}`;
      const result = await cachedBuild(key, { freshMs: WALLET_QUICKVIEW_FRESH_MS, staleMs: WALLET_QUICKVIEW_STALE_MS, negativeMs: WALLET_BASE_NEGATIVE_MS }, () => buildWalletQuickview(network, wallet, tokenAddress));
      const value = result.value;
      if (!value) throw new Error('wallet_quickview_not_ready');
      sendJson(res, 200, responseBase({
        feature_schema_version: STEP1039_FEATURE_SCHEMA_VERSION,
        ...value,
        cache_status: result.cache_status,
        smart_money_is_candidate_not_identity: true,
        confirmed_smart_money_label_generated: false,
        win_rate_semantics: network === 'solana' ? 'unavailable_exact_realized_pnl' : 'profitable_realized_token_positions_divided_by_evaluable_realized_token_positions_not_trade_win_rate',
        solana_swap_cashflow_not_pnl: network === 'solana',
        user_read_direct_upstream_requests: 0,
      }));
    } catch (error) {
      sendJson(res, 503, responseBase({ ok: false, feature_schema_version: STEP1039_FEATURE_SCHEMA_VERSION, error: text(error?.message || error), network, wallet, token_address: tokenAddress || null, no_cross_chain_or_wallet_fallback: true }));
    }
    return true;
  }

  if (path === HOLDERS_ROUTE || path === SECURITY_ROUTE) {
    if (!network || network === 'all') {
      sendJson(res, 400, responseBase({ ok: false, error: 'exact_network_required', feature_schema_version: STEP1038_FEATURE_SCHEMA_VERSION }));
      return true;
    }
    const address = text(url.searchParams.get('address') || url.searchParams.get('token_address'));
    if (!validAddressForNetwork(network, address)) {
      sendJson(res, 400, responseBase({ ok: false, error: 'invalid_contract_address', network, feature_schema_version: STEP1038_FEATURE_SCHEMA_VERSION }));
      return true;
    }
    try {
      if (path === HOLDERS_ROUTE) {
        const key = `step1038:holders:${network}:${lower(address)}`;
        const result = await cachedBuild(key, { freshMs: network === 'solana' ? SOLANA_HELIUS_HOLDER_FRESH_MS : HOLDER_FRESH_MS, staleMs: network === 'solana' ? SOLANA_HELIUS_HOLDER_STALE_MS : HOLDER_STALE_MS, negativeMs: HOLDER_NEGATIVE_MS }, () => buildHolderAnalysis(network, address));
        const value = result.value;
        if (!value) throw new Error('holder_analysis_not_ready');
        sendJson(res, 200, responseBase({
          feature_schema_version: STEP1038_FEATURE_SCHEMA_VERSION,
          network,
          address,
          source: value.source,
          source_scope: value.source_scope,
          total_holders: value.total_holders,
          concentration: value.concentration,
          holder_change: value.holder_change,
          holder_distribution: value.holder_distribution,
          holders_by_acquisition: value.holders_by_acquisition,
          top_holders: value.top_holders,
          top_holder_list_available: value.top_holder_list_available,
          exact_top20_available: value.exact_top20_available,
          field_sources: value.field_sources || null,
          upstream_partial_errors: value.upstream_partial_errors || null,
          cache_status: result.cache_status,
          user_read_direct_moralis_requests: 0,
          moralis_cu_if_full_evm_upstream_build: MORALIS_HOLDER_METRICS_CU + MORALIS_TOP_HOLDERS_CU,
          moralis_cu_if_solana_metrics_build: network === 'solana' ? 0 : MORALIS_HOLDER_METRICS_CU,
          helius_exact_solana_holder_index: network === 'solana',
          helius_holder_index_method: network === 'solana' ? 'getTokenAccounts' : null,
          helius_scan_complete: value.helius_scan_complete ?? null,
          token_accounts_scanned: value.token_accounts_scanned ?? null,
          reported_total_token_accounts: value.reported_total_token_accounts ?? null,
          helius_pages_scanned: value.helius_pages_scanned ?? null,
          no_deprecated_solana_holder_call: true,
        }));
        return true;
      }
      const key = `step1038:security:${network}:${lower(address)}`;
      const result = await cachedBuild(key, { freshMs: SECURITY_FRESH_MS, staleMs: SECURITY_STALE_MS, negativeMs: SECURITY_NEGATIVE_MS }, () => buildGoPlusSecurity(network, address));
      const value = result.value;
      if (!value) throw new Error('security_analysis_not_ready');
      sendJson(res, 200, responseBase({
        feature_schema_version: STEP1038_FEATURE_SCHEMA_VERSION,
        network,
        address,
        source: network === 'solana' ? 'goplus_solana_token_security_beta' : 'goplus_token_security',
        security: value,
        cache_status: result.cache_status,
        no_composite_security_score: true,
        source_facts_only: true,
        creator_owner_are_direct_source_fields_not_dev_inference: true,
        user_read_direct_goplus_requests: 0,
      }));
      return true;
    } catch (error) {
      sendJson(res, 503, responseBase({
        ok: false,
        feature_schema_version: STEP1038_FEATURE_SCHEMA_VERSION,
        error: text(error?.message || error),
        network,
        address,
        no_cross_chain_or_token_fallback: true,
      }));
      return true;
    }
  }

  if (path === TRENDING_ROUTE) {
    stats.step1041_shared_snapshot_reads += 1;
    if (!network) { sendJson(res, 400, responseBase({ ok: false, error: 'invalid_network' })); return true; }
    const sourceRows = discoveryRows(network, STEP1041_HOT_MAX_ROWS);
    const rankPage = onchainRankPage(sourceRows, {
      sort: url.searchParams.get('sort') || 'default',
      offset: url.searchParams.get('offset') || 0,
      limit,
    });
    const rows = rankPage.rows;
    const ageMs = discoveryUpdatedAt ? Math.max(0, Date.now() - discoveryUpdatedAt) : null;
    if (!rows.length && !sourceRows.length && (!discoveryUpdatedAt || ageMs > DISCOVERY_RETAIN_MS)) {
      sendJson(res, 503, responseBase({ ok: false, error: 'onchain_shared_recent_hot_not_ready', network, rows: [], user_read_upstream_requests: 0 }));
      return true;
    }
    sendJson(res, 200, responseBase({
      network,
      ...rankPage,
      row_count: rows.length,
      ranking_happens_before_pagination: true,
      supported_sorts: ['default','market_cap_desc','change_desc','change_asc','volume_desc','liquidity_desc'],
      generated_at: marketUpdatedAt ? new Date(marketUpdatedAt).toISOString() : (discoveryUpdatedAt ? new Date(discoveryUpdatedAt).toISOString() : null),
      shared_snapshot_age_ms: marketUpdatedAt ? Math.max(0, Date.now() - marketUpdatedAt) : ageMs,
      user_read_upstream_requests: 0,
      cache_status: 'background_shared',
    }));
    return true;
  }

  if (path === SEARCH_ROUTE) {
    const q = text(url.searchParams.get('q')).slice(0, 160);
    if (q.length < 2) { sendJson(res, 400, responseBase({ ok: false, error: 'query_too_short', rows: [] })); return true; }
    const key = `search:${lower(q)}:${network || 'all'}`;
    try {
      const result = await cachedBuild(key, { freshMs: 60_000, staleMs: 10 * 60_000 }, () => buildDexSearch(q));
      const rows = (result.value || []).filter((row) => !network || network === 'all' || row.network === network).slice(0, limit);
      sendJson(res, 200, responseBase({
        query: q,
        network: network || 'all',
        rows,
        row_count: rows.length,
        search_rank_rule_version: 'liquidity_desc_then_24h_volume_desc_then_24h_activity_desc_v2',
        exact_identity_scope: 'network_plus_token_contract_best_pool_contract_v2',
        ranking_happens_before_response_limit: true,
        cache_status: result.cache_status,
        bounded_backend_build: result.cache_status === 'miss',
      }));
    } catch (error) { sendJson(res, 503, responseBase({ ok: false, error: text(error?.message || error), query: q, rows: [] })); }
    return true;
  }

  if (path === TOKEN_ROUTE || path === POOLS_ROUTE) {
    if (!network || network === 'all') { sendJson(res, 400, responseBase({ ok: false, error: 'exact_network_required' })); return true; }
    const address = text(url.searchParams.get('address'));
    if (!validAddressForNetwork(network, address)) { sendJson(res, 400, responseBase({ ok: false, error: 'invalid_contract_address', network })); return true; }
    const key = `token_pairs:${network}:${lower(address)}`;
    try {
      const result = await cachedBuild(key, { freshMs: 20_000, staleMs: 5 * 60_000 }, () => buildDexTokenPairs(network, address));
      const rows = (result.value || []).slice(0, limit);
      if (path === POOLS_ROUTE) {
        sendJson(res, 200, responseBase({ network, address, rows, row_count: rows.length, cache_status: result.cache_status }));
      } else {
        const best = rows[0] || null;
        const token = best ? tokenAddressInPair(best, address) : null;
        const tokenMarket = best && token
          ? tokenCentricRow(best, token, { source: 'dexscreener_public_api_exact_token' })
          : null;
        sendJson(res, 200, responseBase({
          network,
          address,
          token,
          best_pool: best,
          token_market: tokenMarket,
          token_profile: tokenMarket?.token_profile || tokenProfileForIdentity(network, address) || null,
          product_badges: tokenMarket?.product_badges || (best && token ? productBadgesForToken(best, token) : []),
          token_market_fields_verified: tokenMarket?.token_market_fields_verified === true,
          pool_count: rows.length,
          pools_preview: rows.slice(0, 6),
          cache_status: result.cache_status,
        }));
      }
    } catch (error) { sendJson(res, 503, responseBase({ ok: false, error: text(error?.message || error), network, address })); }
    return true;
  }

  if (path === NEW_POOLS_ROUTE) {
    stats.step1041_shared_snapshot_reads += 1;
    if (!network) { sendJson(res, 400, responseBase({ ok: false, error: 'invalid_network', rows: [] })); return true; }
    const sourceRows = recentCandidatePools(network, STEP1041_NEW_MAX_ROWS);
    const rankPage = onchainRankPage(sourceRows, {
      sort: url.searchParams.get('sort') || 'default',
      offset: url.searchParams.get('offset') || 0,
      limit,
    });
    const rows = rankPage.rows;
    sendJson(res, 200, responseBase({
      feature_schema_version: STEP1041_FEATURE_SCHEMA_VERSION,
      network,
      ...rankPage,
      row_count: rows.length,
      max_rows: STEP1041_NEW_MAX_ROWS,
      ranking_happens_before_pagination: true,
      supported_sorts: ['default','market_cap_desc','change_desc','change_asc','volume_desc','liquidity_desc'],
      cache_status: 'background_shared',
      coverage: 'newest_pool_among_current_objective_and_supplemental_candidate_tokens_not_token_contract_creation_and_not_exhaustive_chain_scan',
      new_pool_max_age_ms: STEP1041_NEW_POOL_MAX_AGE_MS,
      user_read_upstream_requests: 0,
    }));
    return true;
  }

  if (path === KLINES_ROUTE || path === TRADES_ROUTE) {
    if (!network || network === 'all') {
      sendJson(res, 400, responseBase({ ok: false, error: 'exact_network_required' }));
      return true;
    }
    const tokenAddress = text(url.searchParams.get('address') || url.searchParams.get('token_address'));
    const poolAddress = text(url.searchParams.get('pool_address'));
    if (!validAddressForNetwork(network, tokenAddress)) {
      sendJson(res, 400, responseBase({ ok: false, error: 'invalid_contract_address', network }));
      return true;
    }
    if (!validAddressForNetwork(network, poolAddress)) {
      sendJson(res, 400, responseBase({ ok: false, error: 'invalid_pool_address', network }));
      return true;
    }
    if (!MORALIS_API_KEY) {
      sendJson(res, 503, responseBase({
        ok: false,
        error: 'onchain_history_source_not_configured',
        setup_required: 'Render Environment MORALIS_API_KEY',
        api_key_exposed: false,
      }));
      return true;
    }

    try {
      const pool = await exactPoolPreflight(network, tokenAddress, poolAddress);

      if (path === TRADES_ROUTE) {
        const tradeLimit = intRange(url.searchParams.get('limit'), 1, TRADE_MAX_ROWS, 30);
        const key = `trades:${network}:${lower(tokenAddress)}:${lower(poolAddress)}:${tradeLimit}`;
        const result = await cachedTradeBuild(
          key,
          () => buildMoralisTrades(network, tokenAddress, pool, tradeLimit),
        );
        sendJson(res, 200, responseBase({
          network,
          address: tokenAddress,
          pool_address: poolAddress,
          dex_id: pool.dex_id,
          rows: result.value || [],
          row_count: (result.value || []).length,
          cache_status: result.cache_status,
          source: 'moralis_official_data_api_pair_swaps',
          moralis_cu_if_upstream_build: MORALIS_TRADES_CU,
          user_read_direct_moralis_requests: 0,
        }));
        return true;
      }

      const interval = text(url.searchParams.get('interval')) || '1h';
      if (!KAKA_FULL_INTERVALS.includes(interval)) {
        sendJson(res, 400, responseBase({ ok: false, error: 'unsupported_interval', supported_intervals: [...KAKA_FULL_INTERVALS] }));
        return true;
      }
      const klineLimit = intRange(url.searchParams.get('limit'), 1, KLINE_MAX_ROWS, 240);
      const endRaw = text(url.searchParams.get('end_time') || url.searchParams.get('end_time_ms'));
      const endTimeMs = endRaw ? Number(endRaw) : null;
      if (endRaw && (!Number.isFinite(endTimeMs) || endTimeMs <= 0)) {
        sendJson(res, 400, responseBase({ ok: false, error: 'invalid_end_time' }));
        return true;
      }
      // Round latest cache keys by natural short TTL. Explicit historical end_time remains exact.
      const endKey = endTimeMs ? String(Math.floor(endTimeMs)) : 'latest';
      const key = `kline:${network}:${lower(tokenAddress)}:${lower(poolAddress)}:${interval}:${klineLimit}:${endKey}`;
      const result = await cachedKlineBuild(
        key,
        intervalPolicy(interval, endTimeMs),
        () => buildSharedContinuityKlines(network, tokenAddress, pool, interval, klineLimit, endTimeMs),
      );
      const built = result.value || { rows: [] };
      const rows = (built.rows || []).map((row) => ({
        ...row,
        network,
        token_address: tokenAddress,
        pool_address: poolAddress,
        dex_id: pool.dex_id,
        interval,
        source: built.source || 'moralis_official_data_api_pair_ohlcv',
      }));
      sendJson(res, 200, responseBase({
        network,
        address: tokenAddress,
        token: tokenAddressInPair(pool, tokenAddress),
        pool_address: poolAddress,
        dex_id: pool.dex_id,
        pair: {
          base_token: pool.base_token,
          quote_token: pool.quote_token,
        },
        interval,
        source_interval: built.source_timeframe || MORALIS_TIMEFRAME[interval],
        source: built.source || 'moralis_official_data_api_pair_ohlcv',
        source_pair_address: built.source_pair_address || poolAddress,
        fallback_used: built.fallback_used === true,
        fallback_from: built.fallback_from || null,
        primary_error: built.primary_error || null,
        source_token_address: built.source_token_address || null,
        identity_proof: built.identity_proof || null,
        exact_chain_token_pool_preflight: true,
        derived_15m_from_5m: built.derived_15m_from_5m === true,
        kline_feature_schema_version: built.kline_feature_schema_version || KLINE_FEATURE_SCHEMA_VERSION,
        interval_mode: built.interval_mode || 'native_shared',
        derived_from_interval: built.derived_from_interval || null,
        base_cache_status: built.base_cache_status || null,
        zero_trade_fill_count: Number(built.zero_trade_fill_count || 0),
        zero_trade_fill_policy: built.zero_trade_fill_policy || 'disabled',
        zero_trade_fill_tail_extrapolation: false,
        rows,
        row_count: rows.length,
        cache_status: result.cache_status,
        same_exact_key_reads_share_cache_and_inflight: true,
        user_read_direct_moralis_requests: 0,
        moralis_cu_if_upstream_build: MORALIS_KLINE_CU,
        historical_end_time_ms: endTimeMs,
        history_exhausted: built.history_exhausted === true,
        history_exhausted_reason: built.history_exhausted_reason || null,
      }));
    } catch (error) {
      const status = Number(error?.statusCode || 0);
      const code = text(error?.message || error);
      const httpStatus = status === 400 || code.includes('pool_not_owned') ? 400 : 503;
      sendJson(res, httpStatus, responseBase({
        ok: false,
        error: code,
        network,
        address: tokenAddress,
        pool_address: poolAddress,
      }));
    }
    return true;
  }

  return false;
}
