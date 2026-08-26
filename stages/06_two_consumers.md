# Stage 6 — Two consumers, one stream

## The trap
Coalescing is correct for the live PnL *number* on a dashboard — it only
ever needs the latest value. But a PnL **chart/history** view needs every
meaningful price movement, or the line looks wrong/jumpy. Applying
coalescing to both consumers silently drops data the chart needs.

## The fix
Branch the pipeline at **ingestion**, before coalescing:
- **Raw ticks** → written to **ClickHouse** on ingestion, in parallel,
  for the chart/historical path (append-only, high-volume — exactly the
  shape ClickHouse is built for).
- **Coalesced latest-per-symbol** → feeds the fan-out dispatcher for the
  live dashboard number (unchanged from Stage 5).

Same source event, two consumers, two different freshness/completeness
requirements — solved by choosing the right branch point in the pipeline,
not by picking one strategy for everything.

## What you're building
- **ClickHouse** added to the stack; `docker/clickhouse/init/tables.sql`
  (provided) creates the `ticks` MergeTree table, ordered by
  `(symbol, event_time)`.
- **`pnl/history/clickhouse_sink.py::RawTickSink`** — implement `write()`.
- `pnl/main.py` (provided) now branches ingestion with `asyncio.gather(
  raw_sink.write(tick), coalescer.submit(tick))` — both paths run
  unconditionally and independently for every tick.

## Run it
```bash
docker compose up -d --build
python -m pnl.ticks.generator --symbol AAPL --interval 0.01
```

## Verify
```bash
pytest tests/stage06
```
- `test_two_paths.py` — burst 200 ticks for one symbol: ClickHouse must
  contain all 200 rows, while the (deliberately slow, fake) coalesced
  dispatcher processes far fewer — proving the branch happens before
  coalescing, not after.
