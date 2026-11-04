# Stage 8 — Concurrent trades on the same position

## The trap
Two buy orders for the same client+symbol arrive milliseconds apart, hit
two different app server instances. Both do a **read-modify-write** on the
same position row — both read the same old `(qty, avg_price)` before
either writes back, both compute their new average off stale data, and one
write clobbers the other. Classic **lost update**.

## Rejected fix
Route all trades for a given symbol to the same app instance
(application-level partitioning). Works, but adds routing/rebalancing/
hot-key complexity to solve a problem the database already has a tool for.

## The fix
`SELECT ... FOR UPDATE` — takes a row-level exclusive lock at read time.
The second transaction blocks until the first commits, then reads the
*updated* row instead of a stale one. Mutual exclusion handled at the
source of truth, no app-level coordination needed.

## The subtlety the design doc doesn't mention
A client's *first* trade on a symbol has no row to lock. Under MySQL's
default REPEATABLE READ, `FOR UPDATE` on a missing row takes a **gap lock**;
two concurrent first trades both get it, both `INSERT`, and InnoDB
deadlocks one of them (error 1213). The standard answer is to roll back and
retry the whole transaction (also for lock-wait timeouts, 1205, and the
duplicate-key form of the same race under READ COMMITTED, 1062), bounded and
with jittered backoff. The Stage 8 test hits exactly this: 20 concurrent
first trades on a brand-new position.

## What you're building
Go back to **`pnl/trade_service/position_writer.py::record_trade`** one
more time (see the updated docstring) and wrap the read-modify-write in a
transaction using `SELECT ... FOR UPDATE`. This is the final version of
this function — Stages 9 onward don't touch it again.

## Run it
```bash
docker compose up -d --build
```

## Verify
```bash
pytest tests/stage08
```
- `test_concurrent_trades.py` — fires 20 concurrent buys for the same
  position, five times over, and asserts the final quantity is exactly the
  sum of all trade quantities every round. Run this against your
  pre-Stage-8 implementation first if you want to see the trap reproduce —
  it should occasionally (or often, depending on timing) undercount.
