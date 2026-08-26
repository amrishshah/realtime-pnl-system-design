"""Trade-write HTTP surface. Provided as-is: this route is intentionally thin
and delegates to `position_writer.record_trade`, which is the actual
exercise in Stage 1 (and gets revised in Stages 7 and 8)."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter
from pydantic import BaseModel

from pnl.common.events import Side, Trade
from pnl.trade_service import position_writer

router = APIRouter()


class TradeIn(BaseModel):
    client_id: str
    symbol: str
    side: Side
    quantity: Decimal
    price: Decimal
    event_time: datetime

    def to_domain(self) -> Trade:
        return Trade(
            client_id=self.client_id,
            symbol=self.symbol,
            side=self.side,
            quantity=self.quantity,
            price=self.price,
            event_time=self.event_time,
        )


@router.post("/trades", status_code=201)
async def create_trade(trade_in: TradeIn) -> dict[str, str]:
    await position_writer.record_trade(trade_in.to_domain())
    return {"status": "accepted"}
