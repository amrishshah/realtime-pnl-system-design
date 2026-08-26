"""
STAGE 13 EXERCISE — implement AuditWriter.

Design doc §13: the live tick-driven path is fast but structurally
unverified — nothing checks it against ground truth. Reconciling by
re-walking the same derived chain (MySQL -> CDC -> Redis -> coalesce)
would just reproduce any bug already in that chain, so it doesn't count as
verification. An independent EOD job needs something to diff its own
independently-computed PnL against — but `pnl/history/clickhouse_sink.py`
only holds raw *prices*, not computed *PnL*. This writer is what makes a
durable audit trail exist at all: every fan-out task, at the moment it
computes and pushes PnL, also records that value here.

What to implement:
- `__init__(client)`: store a `clickhouse_connect` async client.
- `write(client_id, symbol, pnl, price)`: insert one row into `pnl_audit`
  (`client_id`, `symbol`, `pnl`, `price` — `computed_at` defaults to now in
  the table schema, but passing it explicitly is fine too if you prefer).
"""
from __future__ import annotations

from decimal import Decimal


class AuditWriter:
    def __init__(self, client) -> None:
        raise NotImplementedError("Stage 13: implement AuditWriter.__init__")

    async def write(self, client_id: str, symbol: str, pnl: Decimal, price: Decimal) -> None:
        raise NotImplementedError("Stage 13: implement AuditWriter.write")
