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

## A subtlety this stage surfaces
Correctness now depends on `add`/`remove` being called **exactly once per
logical membership** joining/leaving — not once per trade. `pnl/cdc/
symbol_index_projector.py` is revisited to guard `bloom.add()` behind
Redis `SADD`'s return value (it tells you whether the client was actually
new to that symbol's set), and `pnl/cdc/position_cache_projector.py` calls
`bloom.remove()` once per full-exit event. Get this balance right and the
counters self-correct: a symbol held by N clients has a slot count that
stays positive until the Nth (last) exit.

## What you're building
- **`pnl/cache/bloom.py::CountingBloomFilter`** (subclasses `BloomFilter`)
  — implement `add`, `might_contain` (overriding the bit-array versions
  with counter-based ones), `remove`, and `rebuild_from_positions`.
- Revisit **`pnl/cdc/symbol_index_projector.py`** — guard `bloom.add()` on
  `SADD`'s return value.
- Revisit **`pnl/cdc/position_cache_projector.py`** — on a full-exit event
  (`quantity == 0`), `SREM` the symbol index and `bloom.remove(symbol)`.
- `pnl/cdc/main.py` and `pnl/main.py` (provided) now construct a
  `CountingBloomFilter` instead of a plain `BloomFilter` everywhere.

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
  symbols with a nonzero held quantity.
- `test_projector_cleanup.py` — a sole holder's full exit removes the
  symbol from the filter; a full exit while another client still holds it
  leaves the filter correctly positive.
