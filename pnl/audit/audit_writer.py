"""
Durable audit trail of live PnL (design doc §13).

The live path is fast but unverified. EOD reconciliation independently
recomputes PnL from MySQL positions and official closing prices — but it
needs something to diff against, and the raw `ticks` table holds prices,
not computed PnL. So every fan-out push also records the value it pushed
here, in `pnl_audit`.

This is the highest-volume write in the system (one row per client per
tick), so rows go through the same batched inserter as raw ticks rather
than one ClickHouse INSERT each.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import ROUND_HALF_EVEN, Decimal

from pnl.history.batching import BatchedInserter

# pnl_audit.pnl / price are Decimal64(4).
_SCALE = Decimal("0.0001")


class AuditWriter:
    def __init__(
        self,
        client,
        batch_size: int = 5000,
        flush_interval_seconds: float = 0.5,
        max_buffered_rows: int = 500_000,
    ) -> None:
        self._inserter = BatchedInserter(
            client,
            table="pnl_audit",
            column_names=["client_id", "symbol", "pnl", "price", "computed_at"],
            batch_size=batch_size,
            flush_interval_seconds=flush_interval_seconds,
            max_buffered_rows=max_buffered_rows,
        )

    async def write(self, client_id: str, symbol: str, pnl: Decimal, price: Decimal) -> None:
        await self._inserter.add([
            client_id,
            symbol,
            pnl.quantize(_SCALE, rounding=ROUND_HALF_EVEN),
            price.quantize(_SCALE, rounding=ROUND_HALF_EVEN),
            datetime.now(timezone.utc),
        ])

    async def flush(self) -> None:
        await self._inserter.flush()

    async def close(self) -> None:
        await self._inserter.close()
