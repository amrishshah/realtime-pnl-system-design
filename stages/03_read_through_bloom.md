# Stage 3 — Where current price and buy price come from

## Where each value comes from
- **Current price** — free, already in the incoming tick payload.
- **Quantity / buy price** — live in the client's position record, read
  through Redis with a MySQL fallback.

`PnL = (current_price - avg_buy_price) * quantity`

## The trap
Doing read-through on *every* tick for *every* symbol — including symbols
nobody holds — hammers MySQL with wasted lookups at tick-rate volume.

## The fix
A **Bloom filter** answers "is this symbol held by anyone at all?" in O(1):
no false negatives (it may say "maybe held" for something nobody holds —
a false positive costing one wasted lookup — but never "not held" for
something that *is* held). Only symbols that pass proceed to the index
lookup and Redis read-through. (The actual gating — checking the Bloom
filter *before* touching Redis/MySQL — is wired into the fan-out dispatcher
in Stage 4, since that's where per-tick lookups happen; this stage builds
the two pieces the gate protects.)

## What you're building
1. **Second Debezium connector** (already provided:
   `docker/debezium/connector-positions.json`) capturing `pnl.positions`.
2. **`pnl/cdc/position_cache_projector.py::PositionCacheProjector`** —
   implement `handle()`: a pure, math-free mirror from the `positions`
   binlog into Redis (`HSET position:{client_id}:{symbol}`).
3. **`pnl/cache/position_cache.py::PositionCache`** — implement `get()`:
   read-through — Redis `HGETALL`, MySQL fallback on miss, backfill Redis.
4. **`pnl/cache/bloom.py::BloomFilter`** — implement `add()`/`might_contain()`
   using `mmh3` hashing over a Redis-backed bit array (`SETBIT`/`GETBIT`).
5. Go back to **`pnl/cdc/symbol_index_projector.py`** and wire it to also
   call `bloom.add(symbol)` (see the updated docstring there) — this is
   what makes a newly-bought symbol pass the filter.
6. `pnl/fanout/pnl_calculator.py::compute_pnl` is provided as-is.

## Run it
```bash
docker compose up -d --build
./scripts/register_connectors.sh
```

## Verify
```bash
pytest tests/stage03
```
- `test_bloom_filter.py` — no false negatives for anything added; the
  false-positive rate stays in the expected ballpark for the chosen size/k.
- `test_position_cache.py` — a cache hit never touches MySQL; a miss falls
  back correctly and backfills Redis; an unheld symbol returns `None`.
- `test_position_cache_projector.py` — the projector writes exactly the
  fields it was handed with zero arithmetic (static AST guard) — it must
  never grow into doing cost-basis math itself.
