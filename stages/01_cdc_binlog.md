# Stage 1 — The core race: trade write vs. index update

## The trap
To know which client portfolios a market-data tick affects, you need a fast
`symbol -> {client_ids}` lookup. If a trade write to MySQL and a *separate*
write to that index (Redis/DynamoDB) are two independent operations, that's
a **dual-write problem**: if the second write is delayed or fails, a tick can
arrive for a stock a client just bought and miss them entirely.

## The fix
Don't do a second application-level write at all. The trade commits to MySQL
as the single source of truth; the binlog captures the change; everything
else is a reliable, replayable derivation of *that one write*, via CDC
(Debezium). No race window, because there's only one write.

## What you're building
1. **MySQL schema** (already provided in `docker/mysql/init/01_schema.sql`):
   an append-only `trades` table and a `positions` table (naive upsert target
   for now — real weighted-average math is Stage 7, concurrency-safety is
   Stage 8).
2. **`pnl/trade_service/position_writer.py::record_trade`** — implement this.
   It must INSERT into `trades` and upsert `positions`, in one transaction,
   and touch *nothing else*. No Redis. No Kafka producer. Nothing.
3. **`pnl/cdc/consumer.py::CDCConsumer`** — implement a generic, at-least-once
   Kafka consumer wrapper: subscribe to a topic, decode Debezium's JSON
   envelope, call a handler, and commit the offset **only after the handler
   succeeds**. This is the mechanism that makes CDC reliable without a
   second write path.
4. **Debezium wiring** (already provided): `docker/debezium/connector-trades.json`
   configures a Debezium MySQL connector to capture `pnl.trades`;
   `scripts/register_connectors.sh` registers it against Kafka Connect's
   REST API.

## Run it
```bash
docker compose up -d --build
# wait ~20-30s for kafka-connect to come up, then:
./scripts/register_connectors.sh
```

## Verify
```bash
pytest tests/stage01
```
- `test_trade_write.py` — your `record_trade` persists correctly to MySQL,
  and doesn't import redis/kafka (static guard against reintroducing the
  dual-write bug).
- `test_cdc_binlog.py` — posting a trade produces a Debezium change event on
  the `pnl.pnl.trades` Kafka topic, proving the binlog capture works
  end-to-end.
- `test_cdc_consumer_semantics.py` — exercises your `CDCConsumer` directly
  against a plain topic: a handler that raises must NOT have its offset
  committed (redelivered on restart); a handler that succeeds must commit
  (not redelivered).
