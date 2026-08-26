# Stage 10 — Client reconnect

## The trap
Pure push means a client whose WebSocket was disconnected for 10 minutes
missed every update in that window. On reconnect, "recompute everything
from scratch" is expensive and unnecessary — you don't need to replay
market history, you need whatever the *current* correct value already is.

## The fix
Persist last-known PnL so a reconnect is a cheap read.
- **Key shape**: one Redis **hash per `client_id`** (not per
  `client_id + symbol`) — `client:{id}` with fields per symbol, e.g.
  `HSET client:{id} AAPL '{"pnl":..., "price":..., "qty":...}'`. A full
  portfolio renders in a single `HGETALL`.
- **Per-symbol writes** — plain `HSET` is safe with no extra coordination.
  Different fields (different symbols) in the same hash don't collide.
- **Aggregate total PnL** — *that* field needs atomic read-sum-write, since
  two symbols updating concurrently could both read a stale total and
  clobber each other's write. This is the one place a **Lua script
  (`EVAL`)** is actually needed in this design.

| Hazard | Tool |
|---|---|
| Concurrent trades on the same position | MySQL `SELECT ... FOR UPDATE` (Stage 8) |
| Per-symbol PnL/price writes to the reconnect cache | Redis plain `HSET` |
| Portfolio-wide total PnL field | Redis Lua/`EVAL` |

## What you're building
- **`pnl/cache/lua/total_pnl.lua`** — implement the atomic read-sum-write
  (see the script's comments).
- **`pnl/cache/reconnect_cache.py::ReconnectCache`** — implement
  `record()` (HSET the per-symbol JSON blob, invoke the Lua script for
  `total`) and `snapshot()` (HGETALL).
- `pnl/fanout/dispatcher.py`'s `push_callback` signature widens to include
  `price` and `quantity` (see the Stage 10 docstring addition) — the
  reconnect cache needs both, and they're already on hand in
  `_process_client`.
- `pnl/ws/server.py` (provided) now has a real `WebSocket /ws/{client_id}`
  route: sends an immediate snapshot on connect, then streams live pushes
  via `pnl/ws/connection_manager.py::ConnectionManager` (provided).

## Run it
```bash
docker compose up -d --build
python -m pnl.ticks.generator --symbol AAPL
# in another terminal: websocat ws://localhost:8000/ws/some-client-id
```

## Verify
```bash
pytest tests/stage10
```
- `test_reconnect_cache.py` — `record()` writes the correct per-symbol JSON;
  after many concurrent updates across 20 symbols for one client, `total`
  matches the true sum of every per-symbol pnl value.
- `test_websocket_reconnect.py` — connecting to `/ws/{client_id}` yields an
  immediate snapshot of pre-seeded Redis state, with no wait for a tick.
