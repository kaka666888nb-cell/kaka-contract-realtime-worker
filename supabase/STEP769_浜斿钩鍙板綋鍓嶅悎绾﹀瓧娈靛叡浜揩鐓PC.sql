-- 卡卡Web3 Step769 / Render 650.8.15.43
-- 五平台当前合约字段共享快照：复用既有OI/多空比5分钟表与当前资金费率表，
-- 所有用户读取同一份后台结果；不新建业务表、不新增Cron、不请求交易所。

begin;

-- 精确读取索引。只优化现有有界表，不改变保留期。
create index if not exists app_contract_position_5m_cache_exact_latest_idx
  on public.app_contract_position_5m_cache (provider, symbol, bucket_time desc);

create index if not exists app_funding_rate_current_cache_exact_latest_idx
  on public.app_funding_rate_current_cache (provider, market_type, symbol, cached_at desc);

create or replace function public.kaka_get_contract_current_metric_snapshot(
  p_max_age_minutes integer default 30,
  p_limit integer default 5000
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_minutes integer := greatest(5, least(coalesce(p_max_age_minutes, 30), 180));
  v_limit integer := greatest(1, least(coalesce(p_limit, 5000), 10000));
  v_rows jsonb := '[]'::jsonb;
  v_provider_coverage jsonb := '{}'::jsonb;
  v_position_rows integer := 0;
  v_funding_rows integer := 0;
  v_merged_rows integer := 0;
begin
  with latest_position as (
    select distinct on (lower(trim(provider)), upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g')))
      lower(trim(provider)) as provider,
      upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g')) as symbol,
      bucket_time,
      case when open_interest > 0 then open_interest else null end as open_interest,
      case when open_interest_value > 0 then open_interest_value else null end as open_interest_value,
      case when global_long_short_ratio > 0 then global_long_short_ratio else null end as global_long_short_ratio,
      case when global_long_account > 0 then global_long_account else null end as global_long_account,
      case when global_short_account > 0 then global_short_account else null end as global_short_account,
      case when top_account_long_short_ratio > 0 then top_account_long_short_ratio else null end as top_account_long_short_ratio,
      case when top_account_long > 0 then top_account_long else null end as top_account_long,
      case when top_account_short > 0 then top_account_short else null end as top_account_short,
      case when top_position_long_short_ratio > 0 then top_position_long_short_ratio else null end as top_position_long_short_ratio,
      case when top_position_long > 0 then top_position_long else null end as top_position_long,
      case when top_position_short > 0 then top_position_short else null end as top_position_short,
      source as metric_source,
      coalesce(updated_at, bucket_time) as metric_updated_at
    from public.app_contract_position_5m_cache
    where bucket_time >= now() - make_interval(mins => v_minutes)
      and lower(trim(provider)) in ('binance','okx','bybit','bitget','gate')
      and upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g')) like '%USDT'
    order by lower(trim(provider)),
             upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g')),
             bucket_time desc,
             updated_at desc nulls last
  ), latest_funding as (
    select distinct on (lower(trim(provider)), upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g')))
      lower(trim(provider)) as provider,
      upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g')) as symbol,
      coalesce(nullif(lower(trim(market_type)), ''), 'contract') as market_type,
      last_funding_rate,
      funding_rate,
      last_funding_rate_percent,
      funding_rate_percent,
      next_funding_time,
      case when mark_price > 0 then mark_price else null end as mark_price,
      case when index_price > 0 then index_price else null end as index_price,
      funding_interval_hours,
      source_time as funding_source_time,
      cached_at as funding_cached_at
    from public.app_funding_rate_current_cache
    where cached_at >= now() - make_interval(mins => v_minutes)
      and lower(trim(provider)) in ('binance','okx','bybit','bitget','gate')
      and coalesce(nullif(lower(trim(market_type)), ''), 'contract') = 'contract'
      and upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g')) like '%USDT'
    order by lower(trim(provider)),
             upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g')),
             cached_at desc
  ), identities as (
    select provider, symbol from latest_position
    union
    select provider, symbol from latest_funding
  ), merged as (
    select
      i.provider,
      'contract'::text as market_type,
      i.symbol,
      'USDT'::text as quote_asset,
      p.open_interest,
      p.open_interest_value,
      p.global_long_short_ratio,
      p.global_long_account,
      p.global_short_account,
      p.top_account_long_short_ratio,
      p.top_account_long,
      p.top_account_short,
      p.top_position_long_short_ratio,
      p.top_position_long,
      p.top_position_short,
      p.bucket_time as oi_source_time,
      p.bucket_time as ratio_source_time,
      p.metric_source,
      p.metric_updated_at,
      f.last_funding_rate,
      f.funding_rate,
      f.last_funding_rate_percent,
      f.funding_rate_percent,
      f.next_funding_time,
      f.mark_price,
      f.index_price,
      f.funding_interval_hours,
      coalesce(f.funding_source_time, f.funding_cached_at) as funding_source_time,
      coalesce(f.funding_source_time, f.funding_cached_at) as mark_index_source_time,
      f.funding_cached_at,
      greatest(
        coalesce(p.metric_updated_at, '-infinity'::timestamptz),
        coalesce(f.funding_cached_at, '-infinity'::timestamptz)
      ) as updated_at
    from identities i
    left join latest_position p using (provider, symbol)
    left join latest_funding f using (provider, symbol)
    order by updated_at desc, i.provider, i.symbol
    limit v_limit
  ), coverage as (
    select
      provider,
      count(*)::integer as row_count,
      count(*) filter (where open_interest is not null or open_interest_value is not null)::integer as oi_count,
      count(*) filter (where global_long_short_ratio is not null or top_account_long_short_ratio is not null or top_position_long_short_ratio is not null)::integer as ratio_count,
      count(*) filter (where funding_rate is not null or last_funding_rate is not null or funding_rate_percent is not null or last_funding_rate_percent is not null)::integer as funding_count,
      count(*) filter (where mark_price is not null and index_price is not null)::integer as basis_count
    from merged
    group by provider
  )
  select
    coalesce((select jsonb_agg(to_jsonb(m) order by m.updated_at desc, m.provider, m.symbol) from merged m), '[]'::jsonb),
    coalesce((select jsonb_object_agg(provider, jsonb_build_object(
      'rows', row_count,
      'oi', oi_count,
      'ratios', ratio_count,
      'funding', funding_count,
      'basis', basis_count
    )) from coverage), '{}'::jsonb),
    (select count(*) from latest_position),
    (select count(*) from latest_funding),
    (select count(*) from merged)
  into v_rows, v_provider_coverage, v_position_rows, v_funding_rows, v_merged_rows;

  return jsonb_build_object(
    'ok', true,
    'rows', v_rows,
    'row_count', v_merged_rows,
    'position_rows', v_position_rows,
    'funding_rows', v_funding_rows,
    'provider_coverage', v_provider_coverage,
    'max_age_minutes', v_minutes,
    'source', 'supabase_shared_current_contract_metric_snapshot_v1',
    'position_table', 'app_contract_position_5m_cache',
    'funding_table', 'app_funding_rate_current_cache',
    'exchange_requests_started', 0,
    'generated_at', now()
  );
end;
$$;

revoke all on function public.kaka_get_contract_current_metric_snapshot(integer, integer)
  from public, anon, authenticated;
grant execute on function public.kaka_get_contract_current_metric_snapshot(integer, integer)
  to service_role;

commit;

select jsonb_build_object(
  'ok', true,
  'step', 'STEP769_SHARED_CURRENT_CONTRACT_FIELDS',
  'rpc', 'kaka_get_contract_current_metric_snapshot',
  'position_rows_last_30m', (
    select count(*) from public.app_contract_position_5m_cache
    where bucket_time >= now() - interval '30 minutes'
  ),
  'funding_rows_last_30m', (
    select count(*) from public.app_funding_rate_current_cache
    where cached_at >= now() - interval '30 minutes'
  ),
  'changed_business_rows', 0,
  'exchange_requests_started', 0
) as step769_shared_snapshot_ready;
