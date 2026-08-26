# Stage 13 — EOD reconciliation

## The core problem
Everything built so far treats the live, tick-driven path as correct
because it's *fast* — but nothing verifies it. If the Bloom filter silently
stops flagging a symbol, a CDC event fails to update someone's
`avg_buy_price`, or a bug creeps into the coalescing logic, the dashboard
keeps confidently displaying a wrong number all day, with no alarm anywhere
in the pipeline. The live path is **fast but unverified by construction**.

## Why re-running the same pipeline doesn't help
If you recompute EOD PnL by walking the *same* derived chain
(MySQL → CDC → Redis → coalesce), a bug anywhere in that chain reproduces
itself in the "check" too. The reconciliation path must be **structurally
independent**.

## The independent path
1. **Source of truth for positions** — MySQL position rows, the same table
   the trade service writes under `SELECT ... FOR UPDATE` (Stage 8).
2. **Source of truth for prices** — official closing prices (`
   eod_closing_prices`, populated independently — never by the live tick
   pipeline).
3. An EOD job computes `PnL = (closing_price − avg_buy_price) × qty`
   directly from these two, per client per symbol.

## What to diff it against
ClickHouse's raw ticks alone aren't enough — they hold *prices*, not
*computed PnL*. The live path needs its own durable audit trail: every
fan-out task, at the moment it computes and pushes PnL, also writes that
value to `pnl_audit`.

## The reconciliation loop
1. Compute authoritative PnL from MySQL positions × closing prices.
2. Diff against the latest audited value for the same client/symbol.
3. Drift beyond tolerance → flag it — the only mechanism in this whole
   design that can catch a silent Bloom filter bug, a missed CDC event, or
   a coalescing bug.
4. EOD is also a natural checkpoint to **rebuild the Bloom filter from
   scratch** off MySQL positions (Stage 11's `rebuild_from_positions`) — a
   good scheduled safety net against deletion/collision edge cases, rather
   than relying on incremental counter decrement forever.

## What you're building
- **`pnl/audit/audit_writer.py::AuditWriter`** — implement `write()`.
  Wired into `pnl/main.py`'s push path (provided) so every live PnL push
  also lands in `pnl_audit`.
- `docker/clickhouse/init/tables.sql` (provided) adds the `pnl_audit`
  table; `docker/mysql/init/01_schema.sql` (provided) adds
  `eod_closing_prices`; `scripts/seed_eod_prices.py` (provided) simulates
  an independent EOD price feed.
- **`pnl/eod/reconciliation.py::EODReconciler`** — implement `run()` per
  the docstring: compute official PnL, diff against the latest audit row,
  flag drift beyond tolerance, rebuild the Bloom filter.

## Run it
```bash
docker compose up -d --build
python scripts/seed_eod_prices.py --date 2024-01-15 AAPL=185.50
python -m pnl.eod.reconciliation --date 2024-01-15
```

## Verify
```bash
pytest tests/stage13
```
- `test_audit_writer.py` — `write()` persists a queryable row.
- `test_reconciliation.py` — seeds a correct position, a deliberately
  drifted one, and one with no audit row at all; asserts the reconciler
  flags exactly the drifted and missing-audit pairs (and not the correct
  one), and that the rebuilt Bloom filter matches exactly the symbols with
  a nonzero held quantity.

## This is the end of the course
Every trap in the design doc — the dual-write race, idempotent indexing,
the Bloom-filter gate, bounded fan-out, coalescing vs. backpressure, the
two-consumer branch, weighted-average cost basis, the lost-update race,
crash self-healing, reconnect semantics, safe Bloom deletion, watermarking,
and independent EOD verification — now has a stage, a working
implementation, and a test that proves it.
