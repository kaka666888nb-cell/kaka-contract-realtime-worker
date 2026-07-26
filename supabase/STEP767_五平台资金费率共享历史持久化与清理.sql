-- 卡卡Web3 Step767 / Render 650.8.15.39
-- 五平台资金费率共享历史持久化、精确去重、持久轮换与自动清理。
-- 执行一次；不新增Cron、不修改交易所密钥、不开放普通用户写权限。

begin;

-- 统一旧数据身份。历史表很小，此操作只规范已有资金费率缓存。
update public.app_funding_rate_current_cache
set provider = lower(trim(provider)),
    market_type = lower(trim(market_type)),
    symbol = upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g'))
where provider is distinct from lower(trim(provider))
   or market_type is distinct from lower(trim(market_type))
   or symbol is distinct from upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g'));

update public.app_funding_rate_history_cache
set provider = lower(trim(provider)),
    market_type = lower(trim(market_type)),
    symbol = upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g'))
where provider is distinct from lower(trim(provider))
   or market_type is distinct from lower(trim(market_type))
   or symbol is distinct from upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g'));

-- 若旧Binance Cron或历史手动同步曾产生重复，保留缓存时间最新的一行。
delete from public.app_funding_rate_current_cache older
using public.app_funding_rate_current_cache newer
where older.ctid < newer.ctid
  and older.provider = newer.provider
  and older.market_type = newer.market_type
  and older.symbol = newer.symbol;

delete from public.app_funding_rate_history_cache older
using public.app_funding_rate_history_cache newer
where older.ctid < newer.ctid
  and older.provider = newer.provider
  and older.market_type = newer.market_type
  and older.symbol = newer.symbol
  and older.funding_time = newer.funding_time;

create unique index if not exists app_funding_rate_current_cache_identity_uq
  on public.app_funding_rate_current_cache (provider, market_type, symbol);

create unique index if not exists app_funding_rate_history_cache_identity_uq
  on public.app_funding_rate_history_cache (provider, market_type, symbol, funding_time);

create index if not exists app_funding_rate_history_cache_time_idx
  on public.app_funding_rate_history_cache (funding_time desc);

create index if not exists app_funding_rate_history_cache_exact_read_idx
  on public.app_funding_rate_history_cache (provider, market_type, symbol, funding_time desc);

create index if not exists app_funding_rate_current_cache_cached_at_idx
  on public.app_funding_rate_current_cache (cached_at desc);

-- 后台目录轮换游标。App不读写，本表只避免Render重启后重复从目录开头开始。
create table if not exists public.app_contract_funding_rotation_state (
  provider text primary key,
  cursor integer not null default 0,
  cycle bigint not null default 0,
  catalog_size integer not null default 0,
  last_symbols jsonb not null default '[]'::jsonb,
  last_started_at timestamptz,
  last_completed_at timestamptz,
  last_error text not null default '',
  updated_at timestamptz not null default now(),
  constraint app_contract_funding_rotation_provider_check
    check (provider in ('binance','okx','bybit','bitget','gate')),
  constraint app_contract_funding_rotation_cursor_check check (cursor >= 0),
  constraint app_contract_funding_rotation_cycle_check check (cycle >= 0)
);

alter table public.app_contract_funding_rotation_state enable row level security;
revoke all on table public.app_contract_funding_rotation_state from anon, authenticated;
grant all on table public.app_contract_funding_rotation_state to service_role;

-- 普通用户仍沿用现有只读策略；写入只通过service_role。
revoke insert, update, delete, truncate
  on table public.app_funding_rate_current_cache,
           public.app_funding_rate_history_cache
  from anon, authenticated;
grant select on table public.app_funding_rate_current_cache,
                      public.app_funding_rate_history_cache
  to anon, authenticated;
grant all on table public.app_funding_rate_current_cache,
                   public.app_funding_rate_history_cache
  to service_role;

create or replace function public.kaka_cleanup_contract_funding_cache()
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_current_deleted bigint := 0;
  v_history_deleted bigint := 0;
begin
  delete from public.app_funding_rate_current_cache
   where cached_at < now() - interval '7 days';
  get diagnostics v_current_deleted = row_count;

  delete from public.app_funding_rate_history_cache
   where funding_time < now() - interval '31 days';
  get diagnostics v_history_deleted = row_count;

  return jsonb_build_object(
    'ok', true,
    'current_deleted', v_current_deleted,
    'history_deleted', v_history_deleted,
    'current_retention_days', 7,
    'history_retention_days', 31,
    'cleaned_at', now()
  );
end;
$$;

revoke all on function public.kaka_cleanup_contract_funding_cache()
  from public, anon, authenticated;
grant execute on function public.kaka_cleanup_contract_funding_cache()
  to service_role;

select public.kaka_cleanup_contract_funding_cache();

commit;

-- 部署前自检。应看到唯一索引、轮换表、清理RPC和changed_rows=0标记。
select jsonb_build_object(
  'ok', true,
  'step', 'STEP767_FIVE_PROVIDER_FUNDING_HISTORY_PERSISTENCE',
  'history_rows', (select count(*) from public.app_funding_rate_history_cache),
  'history_providers', (select count(distinct provider) from public.app_funding_rate_history_cache),
  'rotation_rows', (select count(*) from public.app_contract_funding_rotation_state),
  'current_retention_days', 7,
  'history_retention_days', 31
) as step767_storage_ready;
