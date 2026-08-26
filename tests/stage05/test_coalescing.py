"""A burst of ticks for one symbol, faster than the downstream dispatcher
can drain them, must coalesce to far fewer processed values — with the
latest one always winning — instead of queueing every tick."""
import asyncio
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from pnl.common.events import Tick
from pnl.ticks.coalescer import SymbolCoalescer


class FakeDispatcher:
    def __init__(self, delay: float) -> None:
        self.delay = delay
        self.calls: list[Tick] = []

    async def handle_tick(self, tick: Tick) -> None:
        await asyncio.sleep(self.delay)
        self.calls.append(tick)


@pytest.mark.asyncio
async def test_burst_of_ticks_coalesces_to_far_fewer_calls():
    fake = FakeDispatcher(delay=0.05)
    coalescer = SymbolCoalescer(fake)
    symbol = f"SYM{uuid.uuid4().hex[:6].upper()}"

    num_ticks = 100
    for i in range(num_ticks):
        tick = Tick(
            symbol=symbol, price=Decimal(str(100 + i)),
            event_time=datetime.now(timezone.utc),
        )
        await coalescer.submit(tick)

    await asyncio.sleep(1.0)  # let the one consumer task drain what's left

    assert len(fake.calls) < num_ticks, (
        "every tick was processed individually — ticks are being queued, "
        "not coalesced to latest-only"
    )
    assert fake.calls[-1].price == Decimal(str(100 + num_ticks - 1)), (
        "the last processed value must be the most recently submitted "
        "price, not a stale intermediate one"
    )


@pytest.mark.asyncio
async def test_independent_symbols_do_not_block_each_other():
    fake = FakeDispatcher(delay=0.02)
    coalescer = SymbolCoalescer(fake)
    symbol_a = f"A{uuid.uuid4().hex[:6]}"
    symbol_b = f"B{uuid.uuid4().hex[:6]}"

    await coalescer.submit(
        Tick(symbol=symbol_a, price=Decimal("1"), event_time=datetime.now(timezone.utc))
    )
    await coalescer.submit(
        Tick(symbol=symbol_b, price=Decimal("2"), event_time=datetime.now(timezone.utc))
    )
    await asyncio.sleep(0.5)

    processed_symbols = {c.symbol for c in fake.calls}
    assert symbol_a in processed_symbols
    assert symbol_b in processed_symbols
