"""
STAGE 13 EXERCISE — implement EODReconciler.

Design doc §13: the live path is best-effort; EOD batch recompute against
the system of record is the authoritative check. The reconciliation path
must be **structurally independent** of the live derived chain:

1. **Source of truth for positions** — MySQL `positions` rows
   (`client_id, symbol, quantity, avg_buy_price`) — the same table Stage 8
   writes under `SELECT ... FOR UPDATE`, authoritative regardless of
   anything that happened downstream in Redis, the Bloom filter, or the
   coalescer.
2. **Source of truth for prices** — `eod_closing_prices`, populated by a
   separate process (`scripts/seed_eod_prices.py` simulates this), never
   touched by the live tick pipeline.
3. Compute `official_pnl = (close_price - avg_buy_price) * quantity` per
   client/symbol directly from these two independent sources.
4. Diff against the latest row in `pnl_audit` (Stage 13's other half) for
   the same client_id/symbol. Drift beyond `tolerance` -> flag it — this is
   the only mechanism in the whole design that can catch a silent Bloom
   filter bug, a missed CDC event, or a coalescing bug; the live path has
   no way to detect these about itself.
5. Also rebuild the Bloom filter from MySQL positions as a scheduled safety
   net (reusing `CountingBloomFilter.rebuild_from_positions` from Stage 11)
   — a separate concern from PnL reconciliation, bundled here because EOD
   is a natural checkpoint for both.

What to implement:
- `__init__(mysql_pool, clickhouse_client, bloom)`: store all three.
- `run(close_date, tolerance=Decimal("0.01"))`:
  1. `SELECT client_id, symbol, quantity, avg_buy_price FROM positions
     WHERE quantity > 0`.
  2. `SELECT symbol, close_price FROM eod_closing_prices WHERE
     close_date = %s` — build a `{symbol: close_price}` map. Skip a
     position if there's no closing price for its symbol (nothing to
     reconcile against).
  3. For each position, compute `official_pnl`, then look up the most
     recent `pnl_audit` row for that `(client_id, symbol)` (`ORDER BY
     computed_at DESC LIMIT 1`). Treat "no audit row found" as a drift
     worth flagging too (something should have been pushed by now).
  4. If `abs(official_pnl - audited_pnl) > tolerance` (or no audit row),
     append a dict describing the drift to a results list.
  5. Call `await self.bloom.rebuild_from_positions(self.mysql_pool)`.
  6. Return the list of flagged drifts (empty list = everything reconciled
     cleanly).
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import date
from decimal import Decimal


class EODReconciler:
    def __init__(self, mysql_pool, clickhouse_client, bloom) -> None:
        raise NotImplementedError("Stage 13: implement EODReconciler.__init__")

    async def run(self, close_date: date, tolerance: Decimal = Decimal("0.01")) -> list[dict]:
        raise NotImplementedError("Stage 13: implement EODReconciler.run")


async def _main() -> None:
    import clickhouse_connect
    import asyncmy

    from pnl.cache.bloom import CountingBloomFilter
    from pnl.cache.redis_client import get_redis
    from pnl.config import settings

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--tolerance", type=str, default="0.01")
    args = parser.parse_args()

    pool = await asyncmy.create_pool(
        host=settings.mysql_host, port=settings.mysql_port,
        user=settings.mysql_user, password=settings.mysql_password, db=settings.mysql_db,
    )
    ch_client = await clickhouse_connect.get_async_client(
        host=settings.clickhouse_host, port=settings.clickhouse_port
    )
    bloom = CountingBloomFilter(get_redis(), "bloom:symbols", 1_000_000, 7)

    reconciler = EODReconciler(pool, ch_client, bloom)
    drifts = await reconciler.run(date.fromisoformat(args.date), Decimal(args.tolerance))

    if drifts:
        print(f"DRIFT DETECTED — {len(drifts)} client/symbol pair(s) out of tolerance:")
        for d in drifts:
            print(f"  {d}")
    else:
        print("Reconciled cleanly — no drift beyond tolerance.")

    pool.close()
    await pool.wait_closed()
    await ch_client.close()


if __name__ == "__main__":
    asyncio.run(_main())
