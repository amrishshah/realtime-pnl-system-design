"""Simulates the consumer process dying partway through a batch (before
offsets for the remaining messages are committed) and confirms a restart
with the same group_id ends up with exactly correct SET membership — no
loss, and duplicates from redelivery are harmless because SADD is
idempotent."""
import asyncio
import json
import uuid

import pytest
import redis.asyncio as redis
from aiokafka import AIOKafkaProducer

from pnl.cdc.consumer import CDCConsumer
from pnl.cdc.symbol_index_projector import SymbolIndexProjector

KAFKA_BOOTSTRAP = "localhost:29092"


def make_event(client_id: str, symbol: str) -> dict:
    return {
        "before": None,
        "after": {"client_id": client_id, "symbol": symbol, "side": "buy",
                   "quantity": "1", "price": "1"},
        "op": "c",
        "source": {"table": "trades"},
        "ts_ms": 0,
    }


@pytest.mark.asyncio
async def test_kill_mid_batch_then_restart_is_correct():
    topic = f"test-crash-{uuid.uuid4().hex[:8]}"
    group_id = f"test-crash-group-{uuid.uuid4().hex[:8]}"
    symbol = f"SYM{uuid.uuid4().hex[:6].upper()}"
    key = f"symbol_index:{symbol}"

    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    await r.delete(key)

    client_ids = [f"client-{i}-{uuid.uuid4().hex[:6]}" for i in range(20)]

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    try:
        for cid in client_ids:
            await producer.send_and_wait(topic, json.dumps(make_event(cid, symbol)).encode())
    finally:
        await producer.stop()

    processed_before_crash = []

    async def crashy_handler(event: dict) -> None:
        projector = SymbolIndexProjector(r)
        await projector.handle(event)
        processed_before_crash.append(event["after"]["client_id"])
        if len(processed_before_crash) == 10:
            raise asyncio.CancelledError("simulated crash")

    consumer1 = CDCConsumer(topic=topic, group_id=group_id, handler=crashy_handler,
                             bootstrap_servers=KAFKA_BOOTSTRAP)
    await consumer1.start()
    try:
        with pytest.raises(asyncio.CancelledError):
            await consumer1.run_forever()
    finally:
        await consumer1.stop()

    assert len(processed_before_crash) == 10

    # Restart: a fresh consumer, same group_id, resumes from the last
    # committed offset and picks up everything not yet successfully handled.
    projector2 = SymbolIndexProjector(r)
    consumer2 = CDCConsumer(topic=topic, group_id=group_id, handler=projector2.handle,
                             bootstrap_servers=KAFKA_BOOTSTRAP)
    await consumer2.start()
    try:
        run_task = asyncio.create_task(consumer2.run_forever())
        deadline = asyncio.get_event_loop().time() + 15
        while await r.scard(key) < len(client_ids) and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.2)
        run_task.cancel()
        try:
            await run_task
        except asyncio.CancelledError:
            pass
    finally:
        await consumer2.stop()

    members = await r.smembers(key)
    assert members == set(client_ids), (
        "final SET membership after crash+restart is not exactly correct — "
        "either messages were lost, or the consumer isn't resuming from the "
        "committed offset correctly"
    )

    await r.delete(key)
    await r.aclose()
