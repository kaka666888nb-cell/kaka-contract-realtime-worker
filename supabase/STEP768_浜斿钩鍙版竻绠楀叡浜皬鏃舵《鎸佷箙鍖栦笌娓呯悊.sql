-- 卡卡Web3 Step768
-- 五平台清算共享小时桶持久化与自动清理
-- 只保存 provider + symbol + 1h 的有界聚合桶，不保存原始清算事件。
-- 原始事件继续仅保留在 Render 进程短窗口和 App 本机失败回退中。

begin;

create table if not exists public.app_contract_liquidation_1h_cache (
  provider text not null,
  market_type text not null default 'contract',
  symbol text not null,
  quote_asset text not null default 'USDT',
  bucket_start timestamptz not null,
  bucket_end timestamptz not null,
  long_notional numeric not null default 0,
  short_notional numeric not null default 0,
  total_notional numeric not null default 0,
  long_count bigint not null default 0,
  short_count bigint not null default 0,
  event_count bigint not null default 0,
  largest_event_id text,
  largest_event_side text,
  largest_event_notional numeric,
  largest_event_price numeric,
  largest_event_time timestamptz,
  latest_event_time timestamptz,
  bucket_closed boolean not null default false,
  provisional boolean not null default true,
  coverage_complete boolean not null default false,
  observed_since timestamptz,
  last_gap_at timestamptz,
  source text not null default 'render_public_liquidation_ws_hour_bucket_v1',
  cached_at timestamptz not null default now(),
  primary key (provider, market_type, symbol, bucket_start),
  constraint app_contract_liquidation_1h_cache_bucket_order_chk
    check (bucket_end > bucket_start),
  constraint app_contract_liquidation_1h_cache_notional_chk
    check (
      long_notional >= 0 and
      short_notional >= 0 and
      total_notional >= 0
    ),
  constraint app_contract_liquidation_1h_cache_count_chk
    check (
      long_count >= 0 and
      short_count >= 0 and
      event_count >= 0
    ),
  constraint app_contract_liquidation_1h_cache_side_chk
    check (largest_event_side is null or largest_event_side in ('long', 'short'))
);

create index if not exists app_contract_liquidation_1h_cache_bucket_desc_idx
  on public.app_contract_liquidation_1h_cache (bucket_start desc);

create index if not exists app_contract_liquidation_1h_cache_provider_symbol_bucket_idx
  on public.app_contract_liquidation_1h_cache
  (provider, symbol, bucket_start desc);

create index if not exists app_contract_liquidation_1h_cache_cached_at_idx
  on public.app_contract_liquidation_1h_cache (cached_at desc);

create or replace function public.kaka_normalize_contract_liquidation_1h_row()
returns trigger
language plpgsql
security invoker
set search_path = public
as $$
begin
  new.provider := lower(trim(coalesce(new.provider, '')));
  if new.provider = 'okex' then new.provider := 'okx'; end if;
  if new.provider in ('gate.io', 'gateio') then new.provider := 'gate'; end if;
  if new.provider not in ('binance', 'okx', 'bybit', 'bitget', 'gate') then
    raise exception 'unsupported liquidation provider: %', new.provider;
  end if;

  new.market_type := 'contract';
  new.symbol := upper(regexp_replace(trim(coalesce(new.symbol, '')), '[^A-Za-z0-9]', '', 'g'));
  if new.symbol = '' then raise exception 'liquidation symbol is required'; end if;

  new.quote_asset := upper(trim(coalesce(new.quote_asset, 'USDT')));
  if new.quote_asset not in ('USDT', 'USDC', 'USD') then
    raise exception 'unsupported liquidation quote asset: %', new.quote_asset;
  end if;

  new.bucket_start := date_trunc('hour', new.bucket_start);
  new.bucket_end := new.bucket_start + interval '1 hour';

  new.long_notional := greatest(coalesce(new.long_notional, 0), 0);
  new.short_notional := greatest(coalesce(new.short_notional, 0), 0);
  new.total_notional := new.long_notional + new.short_notional;
  new.long_count := greatest(coalesce(new.long_count, 0), 0);
  new.short_count := greatest(coalesce(new.short_count, 0), 0);
  new.event_count := greatest(coalesce(new.event_count, 0), new.long_count + new.short_count);

  new.largest_event_side := lower(nullif(trim(coalesce(new.largest_event_side, '')), ''));
  if new.largest_event_side not in ('long', 'short') then new.largest_event_side := null; end if;
  if coalesce(new.largest_event_notional, 0) <= 0 then new.largest_event_notional := null; end if;
  if coalesce(new.largest_event_price, 0) <= 0 then new.largest_event_price := null; end if;

  new.bucket_closed := coalesce(new.bucket_closed, false);
  new.provisional := not new.bucket_closed;
  new.coverage_complete := coalesce(new.coverage_complete, false) and new.bucket_closed;
  new.source := coalesce(nullif(trim(new.source), ''), 'render_public_liquidation_ws_hour_bucket_v1');
  new.cached_at := now();
  return new;
end;
$$;

drop trigger if exists kaka_normalize_contract_liquidation_1h_row_trg
  on public.app_contract_liquidation_1h_cache;

create trigger kaka_normalize_contract_liquidation_1h_row_trg
before insert or update on public.app_contract_liquidation_1h_cache
for each row execute function public.kaka_normalize_contract_liquidation_1h_row();

alter table public.app_contract_liquidation_1h_cache enable row level security;
revoke all on table public.app_contract_liquidation_1h_cache from anon, authenticated;
grant select, insert, update, delete on table public.app_contract_liquidation_1h_cache to service_role;

create or replace function public.kaka_cleanup_contract_liquidation_cache()
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  deleted_rows bigint := 0;
begin
  delete from public.app_contract_liquidation_1h_cache
  where bucket_start < now() - interval '15 days';
  get diagnostics deleted_rows = row_count;

  return jsonb_build_object(
    'ok', true,
    'cleaned_at', now(),
    'liquidation_1h_deleted', deleted_rows,
    'aggregate_retention_days', 15,
    'raw_events_persisted', false
  );
end;
$$;

revoke all on function public.kaka_cleanup_contract_liquidation_cache() from public, anon, authenticated;
grant execute on function public.kaka_cleanup_contract_liquidation_cache() to service_role;

commit;

select public.kaka_cleanup_contract_liquidation_cache() as step768_cleanup_test;

select jsonb_build_object(
  'ok', true,
  'step', 'STEP768_FIVE_PROVIDER_LIQUIDATION_SHARED_HOUR_BUCKETS',
  'storage_table', 'app_contract_liquidation_1h_cache',
  'aggregate_period', '1h',
  'aggregate_retention_days', 15,
  'raw_events_persisted', false,
  'existing_hour_rows', (select count(*) from public.app_contract_liquidation_1h_cache),
  'provider_count', (select count(distinct provider) from public.app_contract_liquidation_1h_cache),
  'logical_duplicate_groups', (
    select count(*) from (
      select provider, market_type, symbol, bucket_start
      from public.app_contract_liquidation_1h_cache
      group by provider, market_type, symbol, bucket_start
      having count(*) > 1
    ) duplicates
  )
) as step768_storage_ready;
