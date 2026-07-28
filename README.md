# Step784 / Render 650.8.15.62

Binance Edge relay queue scheduling fix based on the Step783.1 production audit:

- fixes the delayed-dispatch race where a waiter was removed before its timer fired and could be stranded when another higher-priority request acquired the slot first;
- re-evaluates the full priority queue at dispatch time and keeps FIFO order within the same priority;
- reserves two of six pending positions for visible Kline and critical first-paint metrics;
- defers background funding/history and full position-metric rotation while the relay is busy instead of letting them age into queue timeouts;
- records queue waits/timeouts by lane and bounded source labels for production verification;
- keeps Render-direct Binance REST permanently disabled and does not increase queue size or upstream request rate.
