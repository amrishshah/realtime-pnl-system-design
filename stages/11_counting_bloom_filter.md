# Stage 11 — Bloom filter deletion on full sell

## The trap
A standard Bloom filter has no deletion — it's just a bit array, set to 1
on insert. If a client fully exits a position and you "delete" by clearing
bits, any *other* symbol that happens to hash to the same bit positions (a
collision) gets silently unset too — its ticks would stop triggering
fan-out with no visible failure. PnL updates would just quietly stop for
an unrelated, still-held symbol.

## The fix
A **Counting Bloom Filter** — each slot is a small counter instead of a
single bit. Insert increments the relevant counters; delete decrements
them. A collision means a counter goes from 2 to 1 rather than being
cleared to 0, so it correctly stays "positive" for whichever symbol still
needs it. Only when a counter reaches 0 does that slot genuinely mean
"nothing hashes here anymore."

## Three subtleties this stage surfaces
1. **Entries and exits must come from one ordered stream.** Up to Stage 10
   the index was fed from `trades`; a full exit is only visible on
   `positions`. Kafka orders events within a partition, not across topics,
   so buy → sell-all → buy-again could see the exit land *after* the
   re-entry and silently drop a client who still holds the position. The
   symbol index projector now reads `positions`: `quantity > 0` joins,
   `quantity == 0` (or a deleted row) leaves. Debezium keys those events by
   primary key, so one (client, symbol)'s events stay in order.
2. **Counters must equal index membership, exactly.** Count one per
   (client, symbol) holding — not one per trade, and not one per distinct
   symbol. `join`/`leave` change the index SET and the counters in one Lua
   script, and only when membership actually changes, so a crash between
   the two writes can't desync them and a redelivered event is a no-op.
3. **Rebuilds count holders and swap atomically.** `rebuild_from_positions`
   counts holders per symbol (a `SELECT DISTINCT` rebuild would give a
   3-holder symbol a count of 1, and the first exit would zero it). It
   builds into a temp key and `RENAME`s it into place, so readers never see
   an empty filter mid-rebuild. Run it while CDC projection is paused.

## What you're building
- **`pnl/cache/bloom.py::CountingBloomFilter`** — counter-per-slot `add`,
  `might_contain`, `remove` (never below zero; underflow is logged), atomic
  `join`/`leave`, and `rebuild`/`rebuild_from_positions`.
- **`pnl/cdc/symbol_index_projector.py`** — consumes `positions`, joins or
  leaves by quantity, and drops a closed position from the client's
  reconnect snapshot.
- `pnl/cdc/position_cache_projector.py` stays a pure mirror.
- `pnl/cdc/main.py` seeds the counting filter from the index on first start
  (new key, since the Stage 3 filter was a bit array); both entrypoints
  share it via `symbols_bloom()`.

## Run it
```bash
docker compose up -d --build
```

## Verify
```bash
pytest tests/stage11
```
- `test_bloom_collision.py` — reproduces the trap on a plain `BloomFilter`
  (naive bit-clearing breaks a colliding symbol), then shows
  `CountingBloomFilter` survives the identical collision via decrement.
- `test_bloom_rebuild.py` — rebuilding from MySQL matches exactly the
  symbols with a nonzero held quantity, counting each holder.
- `test_projector_cleanup.py` — sole-holder exit clears the symbol; an exit
  while another client holds it doesn't; redelivered joins/exits don't skew
  counts; exit-then-re-entry stays indexed; a deleted row is an exit; a
  closed position leaves the reconnect snapshot.
