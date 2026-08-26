"""MySQL connection pool. Provided as-is — infra glue, not a stage exercise."""
from __future__ import annotations

import asyncmy

from pnl.config import settings

_pool: asyncmy.Pool | None = None


async def get_pool() -> asyncmy.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncmy.create_pool(
            host=settings.mysql_host,
            port=settings.mysql_port,
            user=settings.mysql_user,
            password=settings.mysql_password,
            db=settings.mysql_db,
            autocommit=False,
            minsize=1,
            maxsize=10,
        )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        await _pool.wait_closed()
        _pool = None
