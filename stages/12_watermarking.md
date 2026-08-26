# Stage 12 — Out-of-order events / watermarking

## The trap
Network jitter or multi-source feeds can mean tick A (generated upstream at
`10:00:00.500`, price $180) arrives *before* tick B (generated upstream at
`10:00:00.300`, price $179) — a naive coalesce ("overwrite with whatever I
see last") would let B clobber A, regressing the live price to stale data
even though A is objectively newer.

## The fix — compare event-time, not arrival order
Every tick carries an **event-time** (when the price was generated
upstream), distinct from **processing-time** (when your service received
it). Coalesce logic becomes: `if incoming.event_time > stored.event_time:
update`. This alone fixes simple reordering.

## The harder case — genuinely late stragglers
A tick delayed 30 seconds by a network partition arrives long after you've
already computed and pushed PnL off newer prices. Waiting indefinitely for
every possible straggler isn't viable — a **watermark** bounds this:
"accept late data up to N seconds behind the newest event-time seen, then
stop waiting."

## The two downstream branches have different lateness tolerance
- **Live dashboard path (coalesced)**: apply the watermark strictly — a
  late tick is dropped either way (it can't be newer than what's held), but
  we distinguish *nearby, superseded* reordering from a *genuine straggler*
  for observability. Dropping costs nothing here — the next fresh tick
  self-heals the view (§9).
- **ClickHouse history path (raw ticks, pre-coalesce)**: never drop for
  lateness. A late-arriving tick still belongs in the correct position on
  the historical timeline, keyed by **event-time**, so charts and audit
  trails render it correctly regardless of when it physically arrived.

## What you're building
Revisit **`pnl/ticks/coalescer.py::SymbolCoalescer`** (see the Stage 12
docstring addition):
- `submit()` compares `tick.event_time` against the stored value's
  `event_time` — reject anything not strictly newer.
- Classify a rejected tick's lateness relative to `watermark_lateness_seconds`
  into `self.dropped_out_of_order` (nearby) vs `self.dropped_late`
  (genuine straggler) — both still get dropped; the split is for metrics.

`pnl/history/clickhouse_sink.py` (Stage 6) is **not** touched — it must
keep accepting every tick unconditionally.

## Run it
```bash
docker compose up -d --build
```

## Verify
```bash
pytest tests/stage12
```
- `test_watermark.py` — feeds event-times `[5, 3, 8, 1, 7]` (seconds) at a
  2-second watermark: the coalesced value ends up reflecting event_time=8;
  event_time=3 and 7 are counted as nearby/superseded drops; event_time=1
  is counted as a genuine straggler drop.
- `test_clickhouse_never_drops.py` — the identical scrambled sequence into
  `RawTickSink` lands all 5 rows, correctly orderable by event_time.
