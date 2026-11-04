from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from pnl.db.mysql import close_pool, get_pool
from pnl.trade_service.api import router as trade_router
from pnl.ws.server import router as ws_router

logger = logging.getLogger("pnl")

_dispatcher = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    await get_pool()
    yield
    await close_pool()


app = FastAPI(title="Real-Time PnL Service", lifespan=lifespan)
app.include_router(trade_router)
app.include_router(ws_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> dict[str, int]:
    """Fan-out dispatcher task counters (Stage 9)."""
    if _dispatcher is None:
        raise HTTPException(status_code=503, detail="fan-out dispatcher not wired yet")
    return {
        "tasks_started": _dispatcher.tasks_started,
        "tasks_completed": _dispatcher.tasks_completed,
        "tasks_failed": _dispatcher.tasks_failed,
    }
