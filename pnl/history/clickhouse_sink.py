"""
Raw-tick history sink (design doc §6, §12).

Coalescing is right for the live number but wrong for a PnL chart, which
needs every price movement. So ingestion branches *before* coalescing: every
tick is written here, unconditionally, in parallel with the coalesced live
path (the branch itself is wired in `pnl/main.py`).

This path never drops a tick for lateness or reordering (§12): the `ticks`
table is `ORDER BY (symbol, event_time)`, so a late tick still lands in the
right place on the timeline.
"""
from __future__ import annotations

from pnl.common.events import Tick
from pnl.history.batching import BatchedInserter


class RawTickSink:
    def __init__(
        self,
        client,
        batch_size: int = 1000,
        flush_interval_seconds: float = 0.25,
        max_buffered_rows: int = 100_000,
    ) -> None:
        self._inserter = BatchedInserter(
            client,
            table="ticks",
            column_names=["symbol", "price", "event_time"],
            batch_size=batch_size,
            flush_interval_seconds=flush_interval_seconds,
            max_buffered_rows=max_buffered_rows,
        )

    async def write(self, tick: Tick) -> None:
        await self._inserter.add([tick.symbol, tick.price, tick.event_time])

    async def flush(self) -> None:
        """Force buffered ticks into ClickHouse now."""
        await self._inserter.flush()

    async def close(self) -> None:
        await self._inserter.close()
