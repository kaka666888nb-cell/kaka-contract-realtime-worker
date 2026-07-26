-- 卡卡Web3 Step767.1 / Render 650.8.15.40
-- 修复旧资金费率历史 market_type 身份重复，并让旧Binance Cron后续写入自动归一为 contract。
-- 执行一次；不新增Cron、不请求交易所、不删除不同结算时间的真实历史。

begin;

-- 先按“规范后的provider + symbol + funding_time”跨旧market_type去重。
-- 优先保留cached_at更新的一行；同时间优先保留已经是contract的行。
with ranked as (
  select
    ctid,
    row_number() over (
      partition by
        lower(trim(provider)),
        upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g')),
        funding_time
      order by
        cached_at desc nulls last,
        case when lower(trim(market_type)) = 'contract' then 0 else 1 end,
        ctid desc
    ) as rn
  from public.app_funding_rate_history_cache
)
delete from public.app_funding_rate_history_cache target
using ranked
where target.ctid = ranked.ctid
  and ranked.rn > 1;

-- 当前费率同样跨旧market_type去重。
with ranked as (
  select
    ctid,
    row_number() over (
      partition by
        lower(trim(provider)),
        upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g'))
      order by
        cached_at desc nulls last,
        source_time desc nulls last,
        case when lower(trim(market_type)) = 'contract' then 0 else 1 end,
        ctid desc
    ) as rn
  from public.app_funding_rate_current_cache
)
delete from public.app_funding_rate_current_cache target
using ranked
where target.ctid = ranked.ctid
  and ranked.rn > 1;

-- 资金费率业务只属于合约，统一旧身份。
update public.app_funding_rate_history_cache
set provider = lower(trim(provider)),
    market_type = 'contract',
    symbol = upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g'))
where provider is distinct from lower(trim(provider))
   or market_type is distinct from 'contract'
   or symbol is distinct from upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g'));

update public.app_funding_rate_current_cache
set provider = lower(trim(provider)),
    market_type = 'contract',
    symbol = upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g'))
where provider is distinct from lower(trim(provider))
   or market_type is distinct from 'contract'
   or symbol is distinct from upper(regexp_replace(symbol, '[^A-Za-z0-9]', '', 'g'));

-- 保留Step767精确唯一键。
create unique index if not exists app_funding_rate_current_cache_identity_uq
  on public.app_funding_rate_current_cache (provider, market_type, symbol);

create unique index if not exists app_funding_rate_history_cache_identity_uq
  on public.app_funding_rate_history_cache (provider, market_type, symbol, funding_time);

-- 所有后续写入（包括暂时保留的旧Binance Cron）在进入表前自动归一，
-- 防止usdt_perpetual与contract再次形成两套逻辑重复历史。
create or replace function public.kaka_normalize_contract_funding_cache_identity()
returns trigger
language plpgsql
set search_path = public
as $$
begin
  new.provider := lower(trim(new.provider));
  new.market_type := 'contract';
  new.symbol := upper(regexp_replace(new.symbol, '[^A-Za-z0-9]', '', 'g'));
  return new;
end;
$$;

drop trigger if exists trg_kaka_normalize_funding_current_identity
  on public.app_funding_rate_current_cache;
create trigger trg_kaka_normalize_funding_current_identity
before insert or update of provider, market_type, symbol
on public.app_funding_rate_current_cache
for each row
execute function public.kaka_normalize_contract_funding_cache_identity();

drop trigger if exists trg_kaka_normalize_funding_history_identity
  on public.app_funding_rate_history_cache;
create trigger trg_kaka_normalize_funding_history_identity
before insert or update of provider, market_type, symbol
on public.app_funding_rate_history_cache
for each row
execute function public.kaka_normalize_contract_funding_cache_identity();

commit;

-- 自检：legacy_market_type_rows和logical_duplicate_groups都应为0。
select jsonb_build_object(
  'ok', true,
  'step', 'STEP767_1_FUNDING_HISTORY_IDENTITY_NORMALIZATION',
  'history_rows', (select count(*) from public.app_funding_rate_history_cache),
  'history_providers', (select count(distinct provider) from public.app_funding_rate_history_cache),
  'legacy_market_type_rows', (
    select count(*)
    from public.app_funding_rate_history_cache
    where market_type is distinct from 'contract'
  ),
  'logical_duplicate_groups', (
    select count(*)
    from (
      select provider, symbol, funding_time
      from public.app_funding_rate_history_cache
      group by provider, symbol, funding_time
      having count(*) > 1
    ) duplicates
  ),
  'binance_btcusdt_rows', (
    select count(*)
    from public.app_funding_rate_history_cache
    where provider = 'binance'
      and market_type = 'contract'
      and symbol = 'BTCUSDT'
  ),
  'changed_rows', 0
) as step767_1_identity_ready;
