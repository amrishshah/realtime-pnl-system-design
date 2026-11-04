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

Concurrent trades on the same position (§8): two buys arriving together
would both read the same old (qty, avg) and one write would clobber the
other — a lost update. The read takes a row-level exclusive lock with
`SELECT ... FOR UPDATE`, so a second transaction on the same
(client_id, symbol) blocks until the first commits, then reads the updated
row. Mutual exclusion lives at the source of truth; no app-level routing.

A client's *first* trade on a symbol has no row to lock yet. Under
REPEATABLE READ, `FOR UPDATE` on a missing row takes a gap lock; two
concurrent first trades both get it, both INSERT, and InnoDB deadlocks one
of them. That (and lock-wait timeouts, or a duplicate-key race under READ
COMMITTED) is handled the standard way: roll back and retry the whole
transaction, a bounded number of times with jittered backoff. Once the
row exists, later trades simply queue on its lock.
"""
from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from decimal import ROUND_HALF_EVEN, Decimal

from asyncmy.errors import IntegrityError, OperationalError

from pnl.common.events import Side, Trade
from pnl.db.mysql import get_pool

logger = logging.getLogger(__name__)

# positions.quantity / avg_buy_price are DECIMAL(20, 8).
_SCALE = Decimal("0.00000001")

# MySQL error codes worth retrying the whole transaction for.
_ER_LOCK_DEADLOCK = 1213
_ER_LOCK_WAIT_TIMEOUT = 1205
_ER_DUP_ENTRY = 1062
_RETRYABLE = {_ER_LOCK_DEADLOCK, _ER_LOCK_WAIT_TIMEOUT, _ER_DUP_ENTRY}
_MAX_ATTEMPTS = 8


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
_SELECT_POSITION_FOR_UPDATE = (
    "SELECT quantity, avg_buy_price FROM positions "
    "WHERE client_id = %s AND symbol = %s FOR UPDATE"
)
_INSERT_POSITION = (
    "INSERT INTO positions (client_id, symbol, quantity, avg_buy_price) VALUES (%s, %s, %s, %s)"
)
_UPDATE_POSITION = (
    "UPDATE positions SET quantity = %s, avg_buy_price = %s WHERE client_id = %s AND symbol = %s"
)


async def record_trade(trade: Trade) -> None:
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            await _record_trade_once(trade)
            return
        except (OperationalError, IntegrityError) as exc:
            code = exc.args[0] if exc.args else None
            if code not in _RETRYABLE or attempt == _MAX_ATTEMPTS:
                raise
            logger.info(
                "retrying trade for %s/%s after MySQL error %s (attempt %d)",
                trade.client_id, trade.symbol, code, attempt,
            )
            await asyncio.sleep(random.uniform(0, 0.01 * 2**attempt))


async def _record_trade_once(trade: Trade) -> None:
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
                await cur.execute(_SELECT_POSITION_FOR_UPDATE, (trade.client_id, trade.symbol))
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
