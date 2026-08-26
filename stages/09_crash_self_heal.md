# Stage 9 — Crash mid-fan-out

## The scenario
An AAPL tick fans out to 50,000 client tasks. The worker pool processes
30,000, then the service crashes (OOM, pod restart). The other 20,000
tasks are lost.

## The realization
This doesn't need explicit recovery. Because PnL is **recomputed from
scratch on every tick** (`(current_price − avg_buy_price) × quantity`), not
incrementally updated, the very next tick produces a complete, correct
result for all 50,000 clients — including the ones who missed the previous
one. There's no state to reconcile and nothing to replay.

**Implication:** no at-least-once/exactly-once delivery guarantee is needed
on the PnL push itself. Staleness for one tick is invisible because the
next tick is seconds (or less) away and self-corrects — a meaningfully
different reliability posture than something like a payment event.

## What you're building
This stage doesn't add new business logic — it adds **observability** to
prove the property, by extending the `FanoutDispatcher` you already built:
- **`pnl/fanout/dispatcher.py`** — add `tasks_started` / `tasks_completed`
  / `tasks_failed` counters and an `_in_flight_tasks` set (see the Stage 9
  docstring addition).
- **`GET /metrics`** (provided in `pnl/main.py`) exposes those counters.
- `scripts/chaos_kill_fanout.py` (provided, manual/optional) — watches
  `/metrics` while you `docker kill -s SIGKILL pnl-app` mid-burst and
  restart it, to see the property hold against the real running service.

## Run it
```bash
docker compose up -d --build
```

## Verify
```bash
pytest tests/stage09
```
- `test_self_heal.py` — cancels in-flight fan-out tasks mid-tick
  (simulating a crash), confirms none of the cancelled clients received an
  update and `tasks_failed` reflects it, then sends one ordinary next tick
  with **no** special recovery code and asserts every client — including
  the ones that were cancelled — now has the correct, current PnL.
