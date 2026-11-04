"""Shared Redis client."""
from __future__ import annotations

import redis.asyncio as redis

from pnl.config import settings

_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _client
    if _client is None:
        # Blocking pool: under a fan-out burst, wait for a free connection
        # instead of failing with MaxConnectionsError (redis-py's default pool
        # is capped at 100 and raises immediately when exhausted).
        pool = redis.BlockingConnectionPool(
            host=settings.redis_host,
            port=settings.redis_port,
            max_connections=settings.redis_max_connections,
            timeout=settings.redis_pool_timeout_seconds,
            decode_responses=True,
        )
        _client = redis.Redis(connection_pool=pool)
    return _client
