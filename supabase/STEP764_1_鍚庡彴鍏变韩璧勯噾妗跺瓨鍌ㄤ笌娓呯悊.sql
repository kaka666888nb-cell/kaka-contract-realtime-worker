-- Step764.1 / Render 650.8.15.38
-- 后台共享资金桶持久化、低带宽读取与自动保留期清理。
-- 执行位置：Supabase Dashboard -> SQL Editor。
-- 只需执行一次。App 不直接访问本表，Render 使用 service_role 读写。

begin;

create table if not exists public.app_contract_flow_15m_cache (
  bucket_start timestamptz primary key,
  bucket_end timestamptz not null,
  close_after timestamptz not null,
  quote_asset text not null default 'USDT',
  taker_buy_quote_volume numeric not null default 0,
  taker_sell_quote_volume numeric not null default 0,
  taker_net_quote_volume numeric not null default 0,
  cvd_delta_quote numeric,
  cvd_sample_count integer not null default 0,
  trade_count bigint not null default 0,
  sample_count integer not null default 0,
  pair_count integer not null default 0,
  provider_count integer not null default 0,
  providers jsonb not null default '[]'::jsonb,
  observed_5m_bucket_count integer not null default 0,
  complete_5m_bucket_count integer not null default 0,
  expected_5m_bucket_count integer not null default 3,
  bucket_closed boolean not null default false,
  provisional boolean not null default true,
  latest_sample_at timestamptz,
  source text not null default 'render_shared_persisted_15m_cache',
  updated_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  constraint app_contract_flow_15m_quote_check check (quote_asset = 'USDT'),
  constraint app_contract_flow_15m_count_check check (
    observed_5m_bucket_count >= 0 and
    complete_5m_bucket_count >= 0 and
    expected_5m_bucket_count = 3
  )
);

create index if not exists app_contract_flow_15m_cache_updated_at_idx
  on public.app_contract_flow_15m_cache (updated_at desc);

create index if not exists app_contract_flow_5m_cache_bucket_time_idx
  on public.app_contract_flow_5m_cache (bucket_time desc);

alter table public.app_contract_flow_15m_cache enable row level security;
revoke all on table public.app_contract_flow_15m_cache from anon, authenticated;
grant all on table public.app_contract_flow_15m_cache to service_role;

create or replace function public.kaka_refresh_contract_flow_15m_cache(
  p_hours integer default 2
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_hours integer := greatest(1, least(coalesce(p_hours, 2), 168));
  v_affected bigint := 0;
begin
  insert into public.app_contract_flow_15m_cache (
    bucket_start,
    bucket_end,
    close_after,
    quote_asset,
    taker_buy_quote_volume,
    taker_sell_quote_volume,
    taker_net_quote_volume,
    cvd_delta_quote,
    cvd_sample_count,
    trade_count,
    sample_count,
    pair_count,
    provider_count,
    providers,
    observed_5m_bucket_count,
    complete_5m_bucket_count,
    expected_5m_bucket_count,
    bucket_closed,
    provisional,
    latest_sample_at,
    source,
    updated_at
  )
  with latest_identity as (
    select distinct on (provider, symbol, bucket_time)
      lower(provider) as provider,
      upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g')) as symbol,
      bucket_time,
      bucket_end_time,
      greatest(coalesce(buy_quote, 0), 0)::numeric as buy_quote,
      greatest(coalesce(sell_quote, 0), 0)::numeric as sell_quote,
      greatest(coalesce(trade_count, 0), 0)::bigint as trade_count,
      coalesce(updated_at, bucket_end_time, bucket_time) as source_updated_at
    from public.app_contract_flow_5m_cache
    where bucket_time >= now() - make_interval(hours => v_hours)
      and lower(provider) in ('binance', 'okx', 'bybit', 'bitget', 'gate')
      and upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g')) like '%USDT'
    order by provider, symbol, bucket_time, updated_at desc nulls last
  ), normalized as (
    select
      to_timestamp(floor(extract(epoch from bucket_time) / 900) * 900) as bucket_start,
      provider,
      symbol,
      bucket_time,
      buy_quote,
      sell_quote,
      trade_count,
      source_updated_at
    from latest_identity
    where buy_quote >= 0 and sell_quote >= 0 and trade_count > 0
  ), grouped as (
    select
      bucket_start,
      sum(buy_quote)::numeric as buy_quote,
      sum(sell_quote)::numeric as sell_quote,
      sum(trade_count)::bigint as trade_count,
      count(*)::integer as sample_count,
      count(distinct provider || ':' || symbol)::integer as pair_count,
      count(distinct provider)::integer as provider_count,
      to_jsonb(array_agg(distinct provider order by provider)) as providers,
      count(distinct to_timestamp(floor(extract(epoch from bucket_time) / 300) * 300))::integer
        as observed_5m_bucket_count,
      count(distinct to_timestamp(floor(extract(epoch from bucket_time) / 300) * 300))::integer
        as complete_5m_bucket_count,
      max(source_updated_at) as latest_sample_at
    from normalized
    group by bucket_start
  )
  select
    bucket_start,
    bucket_start + interval '15 minutes',
    bucket_start + interval '17 minutes',
    'USDT',
    buy_quote,
    sell_quote,
    buy_quote - sell_quote,
    buy_quote - sell_quote,
    sample_count,
    trade_count,
    sample_count,
    pair_count,
    provider_count,
    providers,
    observed_5m_bucket_count,
    complete_5m_bucket_count,
    3,
    now() >= bucket_start + interval '17 minutes',
    not (
      now() >= bucket_start + interval '17 minutes'
      and complete_5m_bucket_count >= 3
    ),
    latest_sample_at,
    'render_shared_supabase_5m_to_persisted_15m_v2',
    now()
  from grouped
  on conflict (bucket_start) do update set
    bucket_end = excluded.bucket_end,
    close_after = excluded.close_after,
    quote_asset = excluded.quote_asset,
    taker_buy_quote_volume = excluded.taker_buy_quote_volume,
    taker_sell_quote_volume = excluded.taker_sell_quote_volume,
    taker_net_quote_volume = excluded.taker_net_quote_volume,
    cvd_delta_quote = excluded.cvd_delta_quote,
    cvd_sample_count = excluded.cvd_sample_count,
    trade_count = excluded.trade_count,
    sample_count = excluded.sample_count,
    pair_count = excluded.pair_count,
    provider_count = excluded.provider_count,
    providers = excluded.providers,
    observed_5m_bucket_count = excluded.observed_5m_bucket_count,
    complete_5m_bucket_count = excluded.complete_5m_bucket_count,
    expected_5m_bucket_count = excluded.expected_5m_bucket_count,
    bucket_closed = excluded.bucket_closed,
    provisional = excluded.provisional,
    latest_sample_at = excluded.latest_sample_at,
    source = excluded.source,
    updated_at = excluded.updated_at;

  get diagnostics v_affected = row_count;

  return jsonb_build_object(
    'ok', true,
    'hours', v_hours,
    'affected_rows', v_affected,
    'table', 'app_contract_flow_15m_cache',
    'generated_at', now()
  );
end;
$$;

revoke all on function public.kaka_refresh_contract_flow_15m_cache(integer)
  from public, anon, authenticated;
grant execute on function public.kaka_refresh_contract_flow_15m_cache(integer)
  to service_role;

create or replace function public.kaka_cleanup_contract_flow_cache()
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_flow_5m_deleted bigint := 0;
  v_position_5m_deleted bigint := 0;
  v_flow_15m_deleted bigint := 0;
begin
  delete from public.app_contract_flow_5m_cache
   where bucket_time < now() - interval '8 days';
  get diagnostics v_flow_5m_deleted = row_count;

  if to_regclass('public.app_contract_position_5m_cache') is not null then
    execute $cleanup$
      delete from public.app_contract_position_5m_cache
       where bucket_time < now() - interval '8 days'
    $cleanup$;
    get diagnostics v_position_5m_deleted = row_count;
  end if;

  delete from public.app_contract_flow_15m_cache
   where bucket_start < now() - interval '31 days';
  get diagnostics v_flow_15m_deleted = row_count;

  return jsonb_build_object(
    'ok', true,
    'flow_5m_deleted', v_flow_5m_deleted,
    'position_5m_deleted', v_position_5m_deleted,
    'flow_15m_deleted', v_flow_15m_deleted,
    'raw_retention_days', 8,
    'aggregate_retention_days', 31,
    'cleaned_at', now()
  );
end;
$$;

revoke all on function public.kaka_cleanup_contract_flow_cache()
  from public, anon, authenticated;
grant execute on function public.kaka_cleanup_contract_flow_cache()
  to service_role;

-- 一次性回填现有最近7天原始5分钟桶。以后由Render每5分钟增量刷新最近2小时。
select public.kaka_refresh_contract_flow_15m_cache(168);
select public.kaka_cleanup_contract_flow_cache();

commit;
