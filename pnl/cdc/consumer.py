"""
STAGE 1 EXERCISE — implement a generic, at-least-once CDC consumer wrapper
around aiokafka.

Debezium publishes each row change as a JSON envelope shaped roughly like:
    {"before": {...} | None, "after": {...} | None, "op": "c"|"u"|"d"|"r",
     "source": {...}, "ts_ms": ...}
`op` is "c" (create), "u" (update), "d" (delete), or "r" (initial snapshot
read). `after` holds the row's new state (None on delete).

What to implement:
- `__init__` stores topic/group_id/handler/bootstrap_servers and constructs
  (but does not yet start) an `aiokafka.AIOKafkaConsumer` with
  `enable_auto_commit=False` — you must commit offsets yourself, only after
  the handler succeeds. That's the whole point: if the handler raises, the
  message is NOT committed, so a restart redelivers it. This is exactly the
  at-least-once semantic Stage 2's idempotent Redis SET is built to tolerate
  — don't try to dedupe here, that's the wrong layer for it.
- `start()` starts the underlying consumer.
- `stop()` stops it cleanly.
- `run_forever()` loops `async for msg in consumer`, decodes `msg.value`
  (Debezium's default is JSON; `json.loads(msg.value)`), calls
  `await self.handler(decoded)`, then `await consumer.commit()` only on
  success. Let exceptions from the handler propagate (don't swallow them)
  after ensuring the offset is not committed.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable

Handler = Callable[[dict], Awaitable[None]]


class CDCConsumer:
    def __init__(
        self,
        topic: str,
        group_id: str,
        handler: Handler,
        bootstrap_servers: str,
    ) -> None:
        raise NotImplementedError("Stage 1: implement CDCConsumer.__init__")

    async def start(self) -> None:
        raise NotImplementedError("Stage 1: implement CDCConsumer.start")

    async def stop(self) -> None:
        raise NotImplementedError("Stage 1: implement CDCConsumer.stop")

    async def run_forever(self) -> None:
        raise NotImplementedError("Stage 1: implement CDCConsumer.run_forever")
