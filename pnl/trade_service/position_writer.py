"""
`record_trade` has evolved across three stages — this is the final version
to implement:
  - Stage 1: append to `trades`, upsert `positions`, MySQL-only (no Redis,
    no Kafka producer — everything else is derived via CDC).
  - Stage 7: real weighted-average cost-basis math (see below), computed
    here rather than in the CDC consumer, which stays math-free.
  - Stage 8 (this stage): make the read-modify-write safe under concurrent
    trades on the same position.

STAGE 8 EXERCISE — add `SELECT ... FOR UPDATE`.

Design doc §8: two buy orders for the same client+symbol arrive
milliseconds apart, hit two different app server instances. Both do a
read-modify-write on the same position row — both read the same old
`(qty, avg_price)` before either writes back, both compute their new
average off stale data, and one write clobbers the other. Classic **lost
update**.

**Rejected alternative**: route all trades for a given symbol to the same
app instance (application-level partitioning). Works, but adds
routing/rebalancing/hot-key complexity to solve a problem the database
already has a tool for.

**Fix**: wrap the read + compute + write in one transaction, and take a
row-level exclusive lock at read time with `SELECT ... FOR UPDATE`. A
second concurrent transaction touching the same `(client_id, symbol)` row
blocks until the first commits, then reads the row it just wrote — not a
stale value. Mutual exclusion handled at the source of truth, no
application-level coordination needed.

What to implement:
1. Open a transaction (`conn.begin()` / disable autocommit for the
   connection, matching your `asyncmy` pool's transaction API).
2. `INSERT INTO trades (...) VALUES (...)`.
3. `SELECT quantity, avg_buy_price FROM positions WHERE client_id=%s AND
   symbol=%s FOR UPDATE` — if no row, treat as `(0, 0)` (you'll still need
   an `INSERT` for the first trade on a position; `INSERT ... ON DUPLICATE
   KEY UPDATE` after computing the new values works, or a separate
   INSERT/UPDATE branch).
4. Compute `new_quantity` / `new_avg_buy_price` per the Stage 7 rules
   (weighted average on BUY, unchanged average + decreased quantity on
   SELL, reject a SELL that would go negative).
5. Write the position row.
6. Commit. On any error, roll back.

Verify this actually fixes the race (not just "looks right") by running the
Stage 8 concurrency test — it fires many concurrent buys for the same
position and checks the final quantity is exactly the sum of all trade
quantities, every time.
"""
from __future__ import annotations

from pnl.common.events import Trade
from pnl.db.mysql import get_pool


async def record_trade(trade: Trade) -> None:
    raise NotImplementedError(
        "Stage 8: implement record_trade with SELECT ... FOR UPDATE for concurrency safety"
    )
