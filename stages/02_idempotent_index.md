# Stage 2 — Idempotency on the index

## The trap
CDC pipelines guarantee **at-least-once** delivery, not exactly-once. If the
same trade change event is delivered twice to the index-updater, naive
"append client_id to a list" logic creates duplicates.

## The fix
Back `symbol -> {client_ids}` with a Redis **SET**, not a list. `SADD` is
idempotent by construction — adding the same client_id twice is a no-op.
The consumer is safe against duplicate delivery with zero dedup logic.

## What you're building
- **Redis** added to the stack (already wired in `docker-compose.yml`).
- **`pnl/cdc/symbol_index_projector.py::SymbolIndexProjector`** — implement
  `handle(event)`: pull `client_id`/`symbol` out of the Debezium envelope's
  `after` and `SADD symbol_index:{symbol} {client_id}`.
- **`pnl/cdc/main.py`** (already wired) runs this projector as a consumer
  on the `pnl.pnl.trades` topic from Stage 1, under a dedicated consumer
  group.

## Run it
```bash
docker compose up -d --build
./scripts/register_connectors.sh   # if not already done
```

## Verify
```bash
pytest tests/stage02
```
- `test_idempotent_set.py` — feeding the same event 5x leaves `SCARD == 1`;
  distinct clients still accumulate correctly.
- `test_crash_recovery.py` — a handler that raises partway through a batch
  (simulating a crash before offset commit) still ends up with **exactly**
  correct SET membership after a fresh consumer with the same group_id
  resumes — no loss, and any redelivered duplicates are harmless.
