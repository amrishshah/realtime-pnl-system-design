"""The raw-tick sink batches ClickHouse inserts (one INSERT per tick would
swamp ClickHouse with tiny parts). Batching must not cost durability: rows
leave the buffer only after an insert succeeds, and close() drains it."""
import pytest

from pnl.history.batching import BatchedInserter


class FlakyClient:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.inserted: list[list] = []
        self.insert_calls = 0

    async def insert(self, table, rows, column_names=None):
        self.insert_calls += 1
        if self.failures > 0:
            self.failures -= 1
            raise ConnectionError("clickhouse unavailable")
        self.inserted.extend(rows)


@pytest.mark.asyncio
async def test_rows_are_batched_not_inserted_one_by_one():
    client = FlakyClient(failures=0)
    inserter = BatchedInserter(client, "t", ["a"], batch_size=100, flush_interval_seconds=60)
    for i in range(250):
        await inserter.add([i])
    await inserter.close()
    assert [r[0] for r in client.inserted] == list(range(250))
    assert client.insert_calls <= 3, f"{client.insert_calls} inserts for 250 rows — not batching"


@pytest.mark.asyncio
async def test_failed_insert_keeps_rows_for_retry():
    client = FlakyClient(failures=1)
    inserter = BatchedInserter(client, "t", ["a"], batch_size=10, flush_interval_seconds=60)
    for i in range(5):
        await inserter.add([i])

    with pytest.raises(ConnectionError):
        await inserter.flush()
    assert client.inserted == []

    await inserter.close()  # retries what's still buffered
    assert [r[0] for r in client.inserted] == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_full_buffer_applies_backpressure_instead_of_growing():
    client = FlakyClient(failures=10**6)  # ClickHouse is down for good
    inserter = BatchedInserter(
        client, "t", ["a"], batch_size=10, flush_interval_seconds=60, max_buffered_rows=20
    )
    for i in range(19):
        await inserter.add([i])
    with pytest.raises(ConnectionError):
        await inserter.add([19])  # hits the cap: flushes inline, failure surfaces
    assert len(inserter._buffer) == 20
    inserter._flusher.cancel()
