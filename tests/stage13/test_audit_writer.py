"""AuditWriter must durably record each computed PnL value — this is the
audit trail EOD reconciliation diffs against."""
import uuid
from decimal import Decimal

import clickhouse_connect
import pytest

from pnl.audit.audit_writer import AuditWriter


@pytest.mark.asyncio
async def test_write_persists_a_queryable_row():
    client = await clickhouse_connect.get_async_client(host="localhost", port=8123)
    writer = AuditWriter(client)
    client_id = f"client-{uuid.uuid4().hex[:8]}"
    symbol = f"SYM{uuid.uuid4().hex[:6].upper()}"

    try:
        await writer.write(client_id, symbol, Decimal("123.45"), Decimal("99.99"))

        result = await client.query(
            "SELECT pnl, price FROM pnl_audit WHERE client_id = {cid:String} "
            "AND symbol = {sym:String}",
            parameters={"cid": client_id, "sym": symbol},
        )
        assert len(result.result_rows) == 1
        pnl, price = result.result_rows[0]
        assert Decimal(str(pnl)) == Decimal("123.45")
        assert Decimal(str(price)) == Decimal("99.99")
    finally:
        await client.command(f"ALTER TABLE pnl_audit DELETE WHERE client_id = '{client_id}'")
        await client.close()
