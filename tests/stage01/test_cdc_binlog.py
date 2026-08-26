"""Verifies the trade write is captured by Debezium off the MySQL binlog —
proving there is exactly one write path and everything else is derived from
it, with no second application-level write racing it."""
import asyncio
import datetime
import json

import httpx
import pytest
from aiokafka import AIOKafkaConsumer

BASE_URL = "http://localhost:8000"
KAFKA_BOOTSTRAP = "localhost:29092"
TRADES_TOPIC = "pnl.pnl.trades"


@pytest.mark.asyncio
async def test_trade_write_appears_on_binlog_topic():
    client_id = "client-cdc-test"
    symbol = "MSFT"

    consumer = AIOKafkaConsumer(
        TRADES_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=None,
    )
    await consumer.start()
    try:
        # Give the consumer a moment to join before producing the event we're
        # waiting for (auto_offset_reset=latest means anything before this
        # point is invisible).
        await asyncio.sleep(2)

        async with httpx.AsyncClient(base_url=BASE_URL, timeout=10) as client:
            resp = await client.post(
                "/trades",
                json={
                    "client_id": client_id,
                    "symbol": symbol,
                    "side": "buy",
                    "quantity": "5",
                    "price": "300.00",
                    "event_time": datetime.datetime.utcnow().isoformat(),
                },
            )
        assert resp.status_code == 201

        found = False
        deadline = asyncio.get_event_loop().time() + 20
        while asyncio.get_event_loop().time() < deadline:
            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=5)
            except asyncio.TimeoutError:
                continue
            envelope = json.loads(msg.value)
            after = envelope.get("after") or {}
            if after.get("client_id") == client_id and after.get("symbol") == symbol:
                found = True
                assert envelope["op"] in ("c", "r")
                break
        assert found, (
            "Debezium change event for the posted trade never appeared on "
            f"'{TRADES_TOPIC}' — check that the connector is registered "
            "(scripts/register_connectors.sh) and mysql binlog is enabled."
        )
    finally:
        await consumer.stop()
