"""
Trade-write path (design doc §1, §7).

A trade write is MySQL's problem alone: this module appends to `trades` and
updates `positions` in one transaction, and touches nothing else — no
Redis, no Kafka producer, no symbol -> clients index. Everything downstream
is *derived* from this one write via CDC off the binlog, so there's no
second write that can race or fail independently (the dual-write bug).

Cost basis (§7) is computed here, at write time, and stored on the
`positions` row. The CDC projector stays a math-free mirror of whatever
this module decided. This is DB-first, CDC-propagated cache update — not
write-through; Redis never appears in this file.

Stage 7 version: a plain read-then-write. Under concurrent trades on the
same position this loses updates — Stage 8 fixes that with a row lock.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import ROUND_HALF_EVEN, Decimal

from pnl.common.events import Side, Trade
from pnl.db.mysql import get_pool

# positions.quantity / avg_buy_price are DECIMAL(20, 8).
_SCALE = Decimal("0.00000001")


class InsufficientPositionError(ValueError):
    """A sell for more than the client currently holds."""


def apply_trade(
    old_quantity: Decimal, old_avg_buy_price: Decimal, trade: Trade
) -> tuple[Decimal, Decimal]:
    """New (quantity, avg_buy_price) after applying `trade` to a position.

    BUY:  new_avg = (old_qty * old_avg + trade_qty * trade_price) / new_qty
    SELL: quantity decreases; the cost basis of the remaining shares is
          unchanged. Selling more than is held is rejected.
    """
    if trade.side is Side.BUY:
        new_quantity = old_quantity + trade.quantity
        if old_quantity == 0:
            new_avg = trade.price
        else:
            new_avg = (
                old_quantity * old_avg_buy_price + trade.quantity * trade.price
            ) / new_quantity
        return new_quantity, new_avg.quantize(_SCALE, rounding=ROUND_HALF_EVEN)

    if trade.quantity > old_quantity:
        raise InsufficientPositionError(
            f"cannot sell {trade.quantity} {trade.symbol}: client {trade.client_id} "
            f"holds {old_quantity}"
        )
    return old_quantity - trade.quantity, old_avg_buy_price


def _to_mysql_datetime(value: datetime) -> datetime:
    """DATETIME columns carry no zone; store everything as naive UTC."""
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


_INSERT_TRADE = (
    "INSERT INTO trades (client_id, symbol, side, quantity, price, event_time) "
    "VALUES (%s, %s, %s, %s, %s, %s)"
)
_SELECT_POSITION = (
    "SELECT quantity, avg_buy_price FROM positions WHERE client_id = %s AND symbol = %s"
)
_INSERT_POSITION = (
    "INSERT INTO positions (client_id, symbol, quantity, avg_buy_price) VALUES (%s, %s, %s, %s)"
)
_UPDATE_POSITION = (
    "UPDATE positions SET quantity = %s, avg_buy_price = %s WHERE client_id = %s AND symbol = %s"
)


async def record_trade(trade: Trade) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.begin()
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    _INSERT_TRADE,
                    (
                        trade.client_id, trade.symbol, trade.side.value,
                        trade.quantity, trade.price, _to_mysql_datetime(trade.event_time),
                    ),
                )
                await cur.execute(_SELECT_POSITION, (trade.client_id, trade.symbol))
                row = await cur.fetchone()
                old_quantity = Decimal(row[0]) if row else Decimal(0)
                old_avg = Decimal(row[1]) if row else Decimal(0)

                new_quantity, new_avg = apply_trade(old_quantity, old_avg, trade)

                if row is None:
                    await cur.execute(
                        _INSERT_POSITION, (trade.client_id, trade.symbol, new_quantity, new_avg)
                    )
                else:
                    await cur.execute(
                        _UPDATE_POSITION, (new_quantity, new_avg, trade.client_id, trade.symbol)
                    )
            await conn.commit()
        except BaseException:
            await conn.rollback()
            raise
