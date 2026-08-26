"""Verifies CDCConsumer's at-least-once contract directly (no Debezium
needed here — just a plain Kafka topic): offsets commit only after the
handler succeeds, so a failing handler causes redelivery on restart."""
import asyncio
import json
import uuid

import pytest
from aiokafka import AIOKafkaProducer

from pnl.cdc.consumer import CDCConsumer

KAFKA_BOOTSTRAP = "localhost:29092"


@pytest.mark.asyncio
async def test_handler_failure_is_not_committed_and_is_redelivered():
    topic = f"test-cdc-{uuid.uuid4().hex[:8]}"
    group_id = f"test-group-{uuid.uuid4().hex[:8]}"

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    try:
        await producer.send_and_wait(topic, json.dumps({"seq": 1}).encode())
    finally:
        await producer.stop()

    attempts = []

    async def flaky_handler(payload: dict) -> None:
        attempts.append(payload)
        if len(attempts) == 1:
            raise RuntimeError("simulated transient failure")

    consumer = CDCConsumer(
        topic=topic, group_id=group_id, handler=flaky_handler,
        bootstrap_servers=KAFKA_BOOTSTRAP,
    )
    await consumer.start()
    try:
        run_task = asyncio.create_task(consumer.run_forever())
        deadline = asyncio.get_event_loop().time() + 15
        while len(attempts) < 2 and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.2)
        run_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run_task
    finally:
        await consumer.stop()

    assert len(attempts) >= 2, (
        "handler was not retried after raising — offsets must only commit "
        "on success, so the message should be redelivered"
    )
    assert all(a == {"seq": 1} for a in attempts)


@pytest.mark.asyncio
async def test_successful_handler_commits_and_is_not_redelivered():
    topic = f"test-cdc-{uuid.uuid4().hex[:8]}"
    group_id = f"test-group-{uuid.uuid4().hex[:8]}"

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    try:
        await producer.send_and_wait(topic, json.dumps({"seq": 1}).encode())
    finally:
        await producer.stop()

    seen = []

    async def handler(payload: dict) -> None:
        seen.append(payload)

    consumer = CDCConsumer(
        topic=topic, group_id=group_id, handler=handler,
        bootstrap_servers=KAFKA_BOOTSTRAP,
    )
    await consumer.start()
    try:
        run_task = asyncio.create_task(consumer.run_forever())
        await asyncio.sleep(3)
        run_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run_task
    finally:
        await consumer.stop()

    # Restart with a fresh consumer instance, same group_id: nothing new
    # should be redelivered because the first run committed its offset.
    seen_after_restart = []

    async def handler2(payload: dict) -> None:
        seen_after_restart.append(payload)

    consumer2 = CDCConsumer(
        topic=topic, group_id=group_id, handler=handler2,
        bootstrap_servers=KAFKA_BOOTSTRAP,
    )
    await consumer2.start()
    try:
        run_task2 = asyncio.create_task(consumer2.run_forever())
        await asyncio.sleep(3)
        run_task2.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run_task2
    finally:
        await consumer2.stop()

    assert len(seen) == 1
    assert len(seen_after_restart) == 0, (
        "message was redelivered even though the handler succeeded and "
        "should have committed its offset"
    )
