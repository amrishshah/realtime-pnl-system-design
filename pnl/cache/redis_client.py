"""Redis client factory. Provided as-is — infra glue, not a stage exercise."""
from __future__ import annotations

import redis.asyncio as redis

from pnl.config import settings

_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.Redis(
            host=settings.redis_host, port=settings.redis_port, decode_responses=True
        )
    return _client
