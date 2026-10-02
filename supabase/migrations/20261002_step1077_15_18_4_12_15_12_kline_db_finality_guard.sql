-- KakaWeb3 Step1077.15.18.4.12.15.12
-- Persistent Kline DB boundary finality defense.
-- Production was applied with exact preflight/audit before this migration ledger
-- was committed. No historical data deletion is encoded here.

create or replace function public.app_market_kline_finality_guard()
returns trigger
language plpgsql
set search_path to 'public', 'pg_temp'
as $$
begin
  if new.close_time is null
     or new.close_time > now() - interval '2 seconds' then
    return null;
  end if;

  if lower(coalesce(new.provider, '')) = 'okx'
     and coalesce(new.raw_payload->>'confirm', '') = '0' then
    return null;
  end if;

  return new;
end;
$$;

drop trigger if exists app_market_klines_00_finality_guard
  on public.app_market_klines_cache;

create trigger app_market_klines_00_finality_guard
before insert or update on public.app_market_klines_cache
for each row
execute function public.app_market_kline_finality_guard();

create or replace function public.app_contract_kline_finality_guard()
returns trigger
language plpgsql
set search_path to 'public', 'pg_temp'
as $$
begin
  if new.close_time is null
     or new.close_time > now() - interval '2 seconds' then
    return null;
  end if;
  return new;
end;
$$;

drop trigger if exists app_contract_klines_00_finality_guard
  on public.app_contract_klines_cache;

create trigger app_contract_klines_00_finality_guard
before insert or update on public.app_contract_klines_cache
for each row
execute function public.app_contract_kline_finality_guard();
