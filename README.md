# Step781.2.5 / Render 650.8.15.56

Root-cause repair for the two remaining Step781.2.4 all-platform 1-second history gate failures.

- Explicit `end_time` now defines a 1-second history request. The backend no longer guesses history mode from `Date.now() - 8s`, which failed when a latest page contained only a few recent seconds.
- Bitget contract history follows the official parameter precedence: the first request uses `startTime` + `endTime`; later requests use only `idLessThan`. Keeping the time range on later pages caused Bitget to ignore the cursor.
- Binance contract 1-second aggregate-trade reads now enter the existing high-priority Kline relay lane instead of the auxiliary lane. Ordinary successful relay calls no longer hold the single relay slot during non-safety telemetry persistence. Restriction and validation state writes remain strict and synchronous.

Existing protections remain unchanged: no synthetic empty seconds in Render, exact provider/market/symbol identity, bounded pagination, empty results never overwrite verified data, and Binance contract direct Render REST remains permanently disabled.
