"""Tick ingestion and the client-facing WebSocket push surface. This route
module is thin scaffolding — the pipeline stage it hands ticks to
(FanoutDispatcher, then SymbolCoalescer, then the ClickHouse+coalesce
branch) and the reconnect cache it reads from are where the exercises
live."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from pnl.common.events import Tick

if TYPE_CHECKING:
    from pnl.cache.reconnect_cache import ReconnectCache
    from pnl.ws.connection_manager import ConnectionManager

router = APIRouter()

TickHandler = Callable[[Tick], Awaitable[None]]
_tick_handler: TickHandler | None = None
_reconnect_cache: "ReconnectCache | None" = None
_connection_manager: "ConnectionManager | None" = None


def set_tick_handler(handler: TickHandler) -> None:
    global _tick_handler
    _tick_handler = handler


def init_reconnect(cache: "ReconnectCache", manager: "ConnectionManager") -> None:
    global _reconnect_cache, _connection_manager
    _reconnect_cache = cache
    _connection_manager = manager


class TickIn(BaseModel):
    symbol: str = Field(min_length=1, max_length=16)
    price: Decimal = Field(gt=0)
    event_time: datetime

    def to_domain(self) -> Tick:
        # Ordering compares event-times, and Python can't compare naive with
        # aware datetimes: treat a zone-less timestamp as UTC.
        event_time = self.event_time
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)
        return Tick(
            symbol=self.symbol,
            price=self.price,
            event_time=event_time,
            received_at=datetime.now(timezone.utc),
        )


@router.post("/ticks", status_code=202)
async def ingest_tick(tick_in: TickIn) -> dict[str, str]:
    if _tick_handler is None:
        raise HTTPException(status_code=503, detail="tick pipeline not wired yet")
    await _tick_handler(tick_in.to_domain())
    return {"status": "accepted"}


@router.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str) -> None:
    if _reconnect_cache is None or _connection_manager is None:
        await websocket.close(code=1013, reason="reconnect cache not wired yet")
        return
    await websocket.accept()

    # Stage 10: reconnect is a cheap read, not a wait for the next tick.
    snapshot = await _reconnect_cache.snapshot(client_id)
    await websocket.send_json({"type": "snapshot", "data": snapshot})

    _connection_manager.connect(client_id, websocket)
    try:
        while True:
            await websocket.receive_text()  # keep the connection open
    except WebSocketDisconnect:
        pass
    finally:
        _connection_manager.disconnect(client_id, websocket)
