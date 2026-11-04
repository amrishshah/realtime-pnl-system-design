"""
Batched ClickHouse inserts.

Inserting one row per tick is an anti-pattern for ClickHouse: every INSERT
creates a new data part, and at tick rate the server can't merge parts as
fast as they arrive ("too many parts"). This buffers rows and inserts them
in batches — when a batch fills up, or every `flush_interval_seconds`,
whichever comes first.

Durability semantics:
- Rows leave the buffer only after ClickHouse accepts them. A failed insert
  keeps them buffered and retries on the next flush.
- The buffer is bounded. Once `max_buffered_rows` is reached (e.g.
  ClickHouse is down), `add()` flushes inline, so a failure surfaces to the
  caller as backpressure instead of growing memory without limit.
- `close()` flushes whatever is left, so a clean shutdown loses nothing.
"""
from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)


class BatchedInserter:
    def __init__(
        self,
        client,
        table: str,
        column_names: list[str],
        batch_size: int = 1000,
        flush_interval_seconds: float = 0.25,
        max_buffered_rows: int = 100_000,
    ) -> None:
        self.client = client
        self.table = table
        self.column_names = column_names
        self.batch_size = batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self.max_buffered_rows = max_buffered_rows
        self._buffer: list[list] = []
        self._lock = asyncio.Lock()
        self._batch_ready = asyncio.Event()
        self._flusher: asyncio.Task | None = None

    async def add(self, row: list) -> None:
        self._ensure_flusher()
        self._buffer.append(row)
        if len(self._buffer) >= self.max_buffered_rows:
            await self.flush()
        elif len(self._buffer) >= self.batch_size:
            self._batch_ready.set()

    async def flush(self) -> None:
        async with self._lock:
            while self._buffer:
                batch = self._buffer[: self.batch_size]
                await self.client.insert(self.table, batch, column_names=self.column_names)
                # Rows added during the await were appended after this batch,
                # so removing from the front never touches them.
                del self._buffer[: len(batch)]

    async def close(self) -> None:
        if self._flusher is not None:
            self._flusher.cancel()
            await asyncio.gather(self._flusher, return_exceptions=True)
            self._flusher = None
        await self.flush()

    def _ensure_flusher(self) -> None:
        if self._flusher is None or self._flusher.done():
            self._flusher = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._batch_ready.wait(), self.flush_interval_seconds)
            except TimeoutError:
                pass
            self._batch_ready.clear()
            try:
                await self.flush()
            except Exception:
                logger.exception(
                    "insert into %s failed; %d rows kept buffered for retry",
                    self.table, len(self._buffer),
                )
