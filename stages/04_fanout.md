# Stage 4 — Fan-out: per-client tasks, not a loop-per-tick

## The trap
One AAPL tick with 50,000 affected clients, processed as a single task
looping over all 50,000 inside it: one slow operation (a hung WebSocket
push, a slow Redis call) blocks all 50,000 behind it. Unbounded blast
radius from a single misbehaving client.

## The fix
Fan out to individual tasks, one per affected client, on a bounded worker
pool. A slow client only holds up itself. **asyncio mapping:** the
"worker pool + buffered channel" pattern becomes `asyncio.create_task()`
per client, bounded by a shared `asyncio.Semaphore` — this workload is
I/O-bound (Redis, eventually WebSocket pushes), so cooperative concurrency
is the right tool; there's no GIL contention to fight.

This stage also finishes wiring the Bloom filter gate from Stage 3: it must
be checked **once per tick, before** the `symbol_index` lookup — not per
client afterward. There's no reason to look up who holds a symbol you
already know nobody holds.

## What you're building
- **`pnl/fanout/dispatcher.py::FanoutDispatcher`** — implement
  `handle_tick()` (bloom gate → index lookup → fire-and-forget per-client
  tasks) and `_process_client()` (semaphore-bounded: read-through position
  lookup → `compute_pnl` → push callback).
- `pnl/ws/server.py` (provided) exposes `POST /ticks` wired to
  `dispatcher.handle_tick`. The real WebSocket push replaces the current
  log-only `push_callback` in Stage 10 — for now it just logs.
- `pnl/ticks/generator.py` (provided, manual dev tool) — posts a random
  walk of ticks so you can watch the pipeline react in real time.

## Run it
```bash
docker compose up -d --build
python -m pnl.ticks.generator --symbol AAPL   # manual, separate terminal
```

## Verify
```bash
pytest tests/stage04
```
- `test_fanout_isolation.py` — one client whose push callback sleeps 3s
  among 199 fast ones for the same symbol: the fast ones must all complete
  in well under a second, unblocked by the slow one.
- `test_fanout_concurrency_bound.py` — in-flight tasks never exceed the
  configured `max_concurrency`; a symbol that fails the Bloom filter check
  never reaches the index lookup or the push callback at all, even if
  `symbol_index` happens to have stale members for it.
