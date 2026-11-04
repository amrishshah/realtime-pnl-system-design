# Stage 5 — Backpressure: coalesce, don't queue

## The trap
If AAPL ticks arrive every 100ms and each spawns thousands of fan-out
tasks, an unbounded queue backs up faster than the worker pool can drain
it — memory blowup, growing latency, eventual collapse.

## The fix
Coalesce ticks per symbol — keep only the *latest* price, overwriting stale
intermediate values instead of queueing every tick. The consumer always
works with the freshest price. **asyncio mapping:** the "buffered channel
of size 1" becomes a `dict[symbol -> latest_tick]` plus one `asyncio.Event`
per symbol, drained by one long-lived consumer task per symbol — a new
`submit()` overwrites the dict entry and wakes the consumer, which always
re-reads whatever is *currently* there rather than a queued value.

A subtlety: `FanoutDispatcher.handle_tick` returns as soon as it has
spawned the per-client tasks. Coalescing ticks in front of it would shrink
the *tick* count but not the *work* — every tick would still add a fresh
round of waiting per-client tasks. So the consumer also waits (bounded) for
the previous tick's fan-out to drain via `dispatcher.wait_idle(symbol)`, and
each per-client task has a timeout so one hung client can't stall its symbol.

## What you're building
- **`pnl/ticks/coalescer.py::SymbolCoalescer`** — implement `submit()` and
  `_consume()` per the docstring.
- `pnl/ws/server.py` and `pnl/main.py` are updated (provided) so ingestion
  now calls `coalescer.submit(tick)` instead of `dispatcher.handle_tick(tick)`
  directly — the coalescer sits between ingestion and the Stage 4 dispatcher.

## Run it
```bash
docker compose up -d --build
python -m pnl.ticks.generator --symbol AAPL --interval 0.01   # fast burst
```

## Verify
```bash
pytest tests/stage05
```
- `test_coalescing.py` — a burst of 100 ticks for one symbol, faster than a
  (deliberately slow, fake) dispatcher can drain, must produce far fewer
  than 100 processed values, with the very last one always reflecting the
  most recently submitted price. A second test confirms two symbols'
  consumer loops are independent — one being slow never starves the other.
- `test_backlog_bounded.py` — with a real dispatcher and slow pushes, a
  burst of 50 ticks never has more than one tick's worth of per-client
  tasks in flight, and every client still converges on the latest price.
