# Stage 7 — Stale buy price on repeat trades (weighted average cost basis)

## The trap
If a client buys more of a stock they already hold, `buy_price` isn't just
"the latest trade price" — it's a recalculated **weighted average cost
basis** across all holdings. The question is *where* that averaging math
happens and *where* it's propagated from.

## Two options, and the one we chose
1. **Trade service** reads the existing position (old qty + old avg price)
   at trade time, computes the new weighted average, writes the finished
   number to the MySQL position row. CDC just propagates the already-computed
   value to Redis.
2. The CDC consumer receives the raw trade event and does the averaging itself.

**Chosen: option 1.** Keeps the CDC consumer simple, stateless, and purely
propagating — it shouldn't own business logic like cost-basis math. MySQL
holds a stored, updated-in-place position row per `(client_id, symbol)` —
not just an append-only trade log.

This is explicitly **not** "write-through" (that term means the app writes
to cache and DB synchronously in the same call) — this is DB-first,
CDC-propagated cache update, decoupled from the write path entirely.

## What you're building
Go back to **`pnl/trade_service/position_writer.py::record_trade`** and
replace Stage 1's naive upsert with real math (see the updated docstring):
- **BUY**: `new_avg = (old_qty*old_avg + trade_qty*trade_price) / new_qty`.
- **SELL**: quantity decreases, `avg_buy_price` is unchanged; reject a sell
  that would take quantity negative.

`pnl/cdc/position_cache_projector.py` (Stage 3) does **not** change — it
must stay math-free forever, mirroring whatever MySQL already decided.

## Run it
```bash
docker compose up -d --build
```

## Verify
```bash
pytest tests/stage07
```
- `test_weighted_avg.py` — two sequential buys (10@100, 10@120) produce
  `avg_buy_price == 110` in MySQL, propagated correctly to Redis via CDC;
  a sell reduces quantity without touching the average; overselling raises.
