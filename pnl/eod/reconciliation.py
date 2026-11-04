"""
EOD reconciliation (design doc §13).

The live tick path is fast but structurally unverified: a silent Bloom
filter bug, a missed CDC event or a coalescing bug would leave a wrong
number on the dashboard all day with no alarm. Re-walking the same derived
chain (MySQL -> CDC -> Redis -> coalesce) would reproduce any such bug, so
this check is built from two *independent* sources of truth:

1. positions — MySQL `positions`, written by the trade service under
   `SELECT ... FOR UPDATE`, regardless of anything downstream;
2. prices    — official closing prices in `eod_closing_prices`, populated
   separately from the live tick feed.

`official_pnl = (close_price - avg_buy_price) * quantity` per client/symbol
is diffed against the last value the live path actually pushed, from the
`pnl_audit` trail. Drift beyond tolerance (or no audit value at all) is
reported and alerted on. EOD is also when the symbol index and Bloom filter
are rebuilt from MySQL, as a safety net against accumulated drift.

Run after the close with trading halted and CDC projection paused: positions
are read as they stand now, and the rebuild must not race live updates.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections.abc import Callable
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from asyncmy.cursors import SSCursor

from pnl.cdc.symbol_index_projector import rebuild_index_and_bloom

logger = logging.getLogger(__name__)

Alert = Callable[[dict], None]


def log_alert(drift: dict) -> None:
    logger.warning(
        "EOD DRIFT %s/%s: official=%s audited=%s (%s)",
        drift["client_id"], drift["symbol"], drift["official_pnl"],
        drift["audited_pnl"], drift["reason"],
    )


class EODReconciler:
    def __init__(
        self,
        mysql_pool,
        clickhouse_client,
        bloom,
        redis_client=None,
        alert: Alert = log_alert,
        audit_lookback_days: int = 7,
    ) -> None:
        self.mysql_pool = mysql_pool
        self.clickhouse_client = clickhouse_client
        self.bloom = bloom
        self.redis = redis_client if redis_client is not None else bloom.redis
        self.alert = alert
        self.audit_lookback_days = audit_lookback_days

    async def run(
        self,
        close_date: date,
        tolerance: Decimal = Decimal("0.01"),
        rebuild_filters: bool = True,
    ) -> list[dict]:
        """Returns the drifted client/symbol pairs (empty = clean)."""
        positions = await self._load_positions()
        closes = await self._load_closing_prices(close_date)
        audited = await self._load_latest_audit(close_date)

        drifts: list[dict] = []
        checked = 0
        for client_id, symbol, quantity, avg_buy_price in positions:
            close_price = closes.get(symbol)
            if close_price is None:
                continue  # no official close for this symbol: nothing to check against
            checked += 1
            official = (close_price - avg_buy_price) * quantity
            audit_pnl = audited.get((client_id, symbol))

            if audit_pnl is None:
                reason, drift = "missing_audit", None
            else:
                drift = official - audit_pnl
                if abs(drift) <= tolerance:
                    continue
                reason = "drift"

            record = {
                "client_id": client_id,
                "symbol": symbol,
                "quantity": quantity,
                "avg_buy_price": avg_buy_price,
                "close_price": close_price,
                "official_pnl": official,
                "audited_pnl": audit_pnl,
                "drift": drift,
                "reason": reason,
            }
            drifts.append(record)
            self.alert(record)

        logger.info(
            "EOD %s: checked %d positions, %d out of tolerance", close_date, checked, len(drifts)
        )
        if rebuild_filters:
            counts = await rebuild_index_and_bloom(self.redis, self.mysql_pool, self.bloom)
            logger.info("EOD rebuilt symbol index and Bloom filter for %d symbols", len(counts))
        return drifts

    async def _load_positions(self) -> list[tuple[str, str, Decimal, Decimal]]:
        out = []
        async with self.mysql_pool.acquire() as conn:
            try:
                async with conn.cursor(SSCursor) as cur:
                    await cur.execute(
                        "SELECT client_id, symbol, quantity, avg_buy_price "
                        "FROM positions WHERE quantity > 0"
                    )
                    while rows := await cur.fetchmany(10_000):
                        out.extend(
                            (cid, sym, Decimal(qty), Decimal(avg)) for cid, sym, qty, avg in rows
                        )
            finally:
                await conn.rollback()
        return out

    async def _load_closing_prices(self, close_date: date) -> dict[str, Decimal]:
        async with self.mysql_pool.acquire() as conn:
            try:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT symbol, close_price FROM eod_closing_prices WHERE close_date = %s",
                        (close_date,),
                    )
                    rows = await cur.fetchall()
            finally:
                await conn.rollback()
        return {symbol: Decimal(price) for symbol, price in rows}

    async def _load_latest_audit(self, close_date: date) -> dict[tuple[str, str], Decimal]:
        """Last pushed PnL per (client, symbol) as of the end of close_date,
        in one aggregate query rather than one lookup per position."""
        end = datetime.combine(close_date + timedelta(days=1), time.min)
        start = end - timedelta(days=self.audit_lookback_days)
        result = await self.clickhouse_client.query(
            "SELECT client_id, symbol, argMax(pnl, computed_at) FROM pnl_audit "
            "WHERE computed_at >= {start:DateTime64(3)} AND computed_at < {end:DateTime64(3)} "
            "GROUP BY client_id, symbol",
            parameters={"start": start, "end": end},
        )
        return {(cid, sym): Decimal(str(pnl)) for cid, sym, pnl in result.result_rows}


async def _main() -> int:
    import clickhouse_connect

    from pnl.cache.bloom import symbols_bloom
    from pnl.cache.redis_client import get_redis
    from pnl.config import settings
    from pnl.db.mysql import close_pool, get_pool

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="EOD PnL reconciliation")
    parser.add_argument("--date", required=True, help="close date, YYYY-MM-DD")
    parser.add_argument("--tolerance", default="0.01")
    parser.add_argument("--no-rebuild", action="store_true", help="skip the index/Bloom rebuild")
    args = parser.parse_args()

    pool = await get_pool()
    ch_client = await clickhouse_connect.get_async_client(
        host=settings.clickhouse_host, port=settings.clickhouse_port
    )
    redis_client = get_redis()
    try:
        reconciler = EODReconciler(pool, ch_client, symbols_bloom(redis_client), redis_client)
        drifts = await reconciler.run(
            date.fromisoformat(args.date), Decimal(args.tolerance), rebuild_filters=not args.no_rebuild
        )
    finally:
        await ch_client.close()
        await close_pool()
        await redis_client.aclose()

    if drifts:
        print(f"DRIFT DETECTED: {len(drifts)} client/symbol pair(s) out of tolerance")
        for d in drifts:
            print(f"  {d['client_id']}/{d['symbol']}: official={d['official_pnl']} "
                  f"audited={d['audited_pnl']} ({d['reason']})")
        return 1  # non-zero so a scheduler/monitor can alert on it
    print("Reconciled cleanly: no drift beyond tolerance.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
