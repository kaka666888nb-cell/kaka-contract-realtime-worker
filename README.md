# Kaka Contract Realtime Worker 650.8.15.46 / Step771

Step771 moves the Data-page current liquidation 5×3 observation from per-user exact reads to one backend-shared bounded snapshot.

- `GET /api/contract-liquidation/current-snapshot` returns 15-minute current observations for five venues and up to three shared core targets per venue.
- The snapshot reuses the existing persistent official liquidation WebSocket feeds and never opens a new exchange connection while serving a read.
- One high-activity target plus two backend-rotated core targets are selected per venue every 30 minutes.
- Current rows preserve provider, symbol, connection state, coverage completeness, long/short notional, event counts and recent verified events.
- `exchange_connections_started=0` and `exchange_requests_started=0` on shared reads; exchange connections do not scale with App users.
- Existing single-provider exact endpoint and shared persisted 1-hour history remain available for detail/fallback workflows.
- No new SQL, Cron, Edge function, environment variable or pubspec change is required.
