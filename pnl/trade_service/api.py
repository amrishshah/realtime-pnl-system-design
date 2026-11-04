"""Trade-write HTTP surface. Thin by design: validation here, then
`position_writer.record_trade` does the MySQL write."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from pnl.common.events import Side, Trade
from pnl.trade_service import position_writer

router = APIRouter()


class TradeIn(BaseModel):
    # Bounds mirror the MySQL columns (VARCHAR(64), VARCHAR(16), DECIMAL(20, 8)).
    client_id: str = Field(min_length=1, max_length=64)
    symbol: str = Field(min_length=1, max_length=16)
    side: Side
    quantity: Decimal = Field(gt=0, max_digits=20, decimal_places=8)
    price: Decimal = Field(gt=0, max_digits=20, decimal_places=8)
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
    try:
        await position_writer.record_trade(trade_in.to_domain())
    except position_writer.InsufficientPositionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"status": "accepted"}
