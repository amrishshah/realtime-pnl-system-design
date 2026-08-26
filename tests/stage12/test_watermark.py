"""Feeds ticks with event-times [5, 3, 8, 1, 7] (seconds) at
watermark_lateness=2: the coalesced value must end up reflecting
event_time=8 (the true max), with event_time=3 and 7 dropped as
nearby/superseded reordering and event_time=1 dropped as a genuine
straggler (more than the watermark behind the max seen)."""
import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from pnl.common.events import Tick
from pnl.ticks.coalescer import SymbolCoalescer


class RecordingDispatcher:
    def __init__(self) -> None:
        self.calls: list[Tick] = []

    async def handle_tick(self, tick: Tick) -> None:
        self.calls.append(tick)


@pytest.mark.asyncio
async def test_out_of_order_and_late_ticks_are_dropped_correctly():
    dispatcher = RecordingDispatcher()
    coalescer = SymbolCoalescer(dispatcher, watermark_lateness_seconds=2.0)
    symbol = "WMTEST"
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    def tick_at(seconds: int) -> Tick:
        return Tick(
            symbol=symbol, price=Decimal(str(100 + seconds)),
            event_time=base + timedelta(seconds=seconds),
        )

    for s in [5, 3, 8, 1, 7]:
        await coalescer.submit(tick_at(s))

    assert coalescer.dropped_out_of_order == 2, (
        f"expected 2 nearby/superseded drops (event_time 3 and 7 relative "
        f"to a max of 5 then 8), got {coalescer.dropped_out_of_order}"
    )
    assert coalescer.dropped_late == 1, (
        f"expected 1 genuine-straggler drop (event_time 1, 7s behind the "
        f"max of 8, past the 2s watermark), got {coalescer.dropped_late}"
    )

    await asyncio.sleep(0.3)  # let the background consumer task drain
    assert dispatcher.calls, "dispatcher was never invoked"
    assert dispatcher.calls[-1].event_time == base + timedelta(seconds=8), (
        "the coalesced value must reflect the tick with the highest "
        "event_time (8), not whichever arrived last"
    )
