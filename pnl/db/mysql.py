"""MySQL connection pool."""
from __future__ import annotations

import asyncio

import asyncmy

from pnl.config import settings

_pool: asyncmy.Pool | None = None
_pool_loop: asyncio.AbstractEventLoop | None = None


async def get_pool() -> asyncmy.Pool:
    """The process-wide pool, created on first use.

    A pool's connections belong to the event loop that opened them, so if
    the running loop has changed (a new `asyncio.run`, or per-test loops),
    build a fresh pool instead of handing out connections bound to a loop
    that may already be closed.
    """
    global _pool, _pool_loop
    loop = asyncio.get_running_loop()
    if _pool is None or _pool_loop is not loop:
        _pool = await asyncmy.create_pool(
            host=settings.mysql_host,
            port=settings.mysql_port,
            user=settings.mysql_user,
            password=settings.mysql_password,
            db=settings.mysql_db,
            autocommit=False,
            minsize=1,
            maxsize=settings.mysql_pool_max_size,
        )
        _pool_loop = loop
    return _pool


async def close_pool() -> None:
    global _pool, _pool_loop
    if _pool is not None:
        _pool.close()
        await _pool.wait_closed()
        _pool = None
        _pool_loop = None
