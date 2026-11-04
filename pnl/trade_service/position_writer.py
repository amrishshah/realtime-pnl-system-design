"""
Trade-write path (design doc §1).

A trade write is MySQL's problem alone: this module appends to `trades` and
upserts `positions` in one transaction, and touches nothing else — no
Redis, no Kafka producer, no symbol -> clients index. Everything downstream
is *derived* from this one write via CDC off the binlog, so there's no
second write that can race or fail independently (the dual-write bug).

Stage 1 version: the `positions` upsert is deliberately naive — it adds the
signed quantity and overwrites `avg_buy_price` with the latest trade price.
Stage 7 replaces this with real weighted-average cost-basis math, and Stage
8 makes the read-modify-write safe under concurrent trades.
"""
from __future__ import annotations

from datetime import datetime, timezone

from pnl.common.events import Side, Trade
from pnl.db.mysql import get_pool

_INSERT_TRADE = (
    "INSERT INTO trades (client_id, symbol, side, quantity, price, event_time) "
    "VALUES (%s, %s, %s, %s, %s, %s)"
)

_UPSERT_POSITION = (
    "INSERT INTO positions (client_id, symbol, quantity, avg_buy_price) "
    "VALUES (%s, %s, %s, %s) "
    "ON DUPLICATE KEY UPDATE quantity = quantity + VALUES(quantity), "
    "avg_buy_price = VALUES(avg_buy_price)"
)


def _to_mysql_datetime(value: datetime) -> datetime:
    """DATETIME columns carry no zone; store everything as naive UTC."""
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


async def record_trade(trade: Trade) -> None:
    signed_quantity = trade.quantity if trade.side is Side.BUY else -trade.quantity

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.begin()
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    _INSERT_TRADE,
                    (
                        trade.client_id,
                        trade.symbol,
                        trade.side.value,
                        trade.quantity,
                        trade.price,
                        _to_mysql_datetime(trade.event_time),
                    ),
                )
                await cur.execute(
                    _UPSERT_POSITION,
                    (trade.client_id, trade.symbol, signed_quantity, trade.price),
                )
            await conn.commit()
        except BaseException:
            await conn.rollback()
            raise
