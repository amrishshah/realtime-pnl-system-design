"""
Generic, at-least-once CDC consumer around aiokafka (design doc §1/§2).

Debezium publishes each row change as a JSON envelope shaped like:
    {"before": {...} | None, "after": {...} | None, "op": "c"|"u"|"d"|"r",
     "source": {...}, "ts_ms": ...}
`op` is "c" (create), "u" (update), "d" (delete), or "r" (snapshot read).

Delivery contract:
- Auto-commit is off. An offset is committed only after the handler has
  finished successfully for that message.
- If the handler raises, the offset is NOT committed and the message is
  retried in place (with capped exponential backoff) — the consumer never
  skips ahead. Dropping a CDC event would silently
  corrupt everything derived from it, so blocking on it is the safer
  failure mode; the error is logged loudly on every attempt.
- Cancellation (`asyncio.CancelledError`) propagates immediately, without
  committing the in-flight message, so a restarted consumer redelivers it.

Redelivery means handlers may see the same event more than once — that's
what the idempotent Redis SET in Stage 2 is built to tolerate. Dedup does
not belong at this layer.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.structs import OffsetAndMetadata

logger = logging.getLogger(__name__)

Handler = Callable[[dict], Awaitable[None]]

_INITIAL_BACKOFF_SECONDS = 0.1
_MAX_BACKOFF_SECONDS = 5.0


class CDCConsumer:
    def __init__(
        self,
        topic: str,
        group_id: str,
        handler: Handler,
        bootstrap_servers: str,
    ) -> None:
        self.topic = topic
        self.group_id = group_id
        self.handler = handler
        self._consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            # A new projector group must build its view from the full
            # history, not just changes that happen after it first starts.
            auto_offset_reset="earliest",
        )

    async def start(self) -> None:
        await self._consumer.start()

    async def stop(self) -> None:
        await self._consumer.stop()

    async def run_forever(self) -> None:
        async for msg in self._consumer:
            tp = TopicPartition(msg.topic, msg.partition)
            if msg.value is not None:
                await self._handle_with_retry(msg)
            # Tombstones (value=None) carry nothing to project; just move past them.
            await self._consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, "")})

    async def _handle_with_retry(self, msg) -> None:
        payload = json.loads(msg.value)
        backoff = _INITIAL_BACKOFF_SECONDS
        while True:
            try:
                await self.handler(payload)
                return
            except Exception:
                logger.exception(
                    "CDC handler failed for %s[%d]@%d (group %s); not committing, retrying in %.1fs",
                    msg.topic, msg.partition, msg.offset, self.group_id, backoff,
                )
                # Retry in place: nothing after this message is processed or
                # committed until it succeeds.
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _MAX_BACKOFF_SECONDS)
