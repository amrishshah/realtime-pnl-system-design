"""PositionCacheProjector must be a pure, math-free mirror — it writes
exactly the fields it was handed, doing no reads and no computation."""
import ast
import uuid
from pathlib import Path

import pytest
import redis.asyncio as redis

from pnl.cdc.position_cache_projector import PositionCacheProjector

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_projector_has_no_arithmetic_operators():
    """Static guard: catches a common shortcut of recomputing an average
    here instead of in the trade service (Stage 7's job, not this one)."""
    source = (REPO_ROOT / "pnl" / "cdc" / "position_cache_projector.py").read_text()
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.BinOp) and isinstance(
            node.op, (ast.Add, ast.Sub, ast.Mult, ast.Div)
        ):
            pytest.fail(
                "position_cache_projector.py contains arithmetic — this "
                "projector must stay a pure propagator; cost-basis math "
                "belongs in the trade service (Stage 7)"
            )


@pytest.mark.asyncio
async def test_handle_mirrors_fields_verbatim():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    client_id, symbol = f"client-{uuid.uuid4().hex[:8]}", "TSLA"
    key = f"position:{client_id}:{symbol}"
    await r.delete(key)

    projector = PositionCacheProjector(r)
    event = {
        "before": None,
        "after": {
            "client_id": client_id,
            "symbol": symbol,
            "quantity": "42.00000000",
            "avg_buy_price": "199.99000000",
        },
        "op": "u",
        "source": {"table": "positions"},
        "ts_ms": 0,
    }
    await projector.handle(event)

    cached = await r.hgetall(key)
    assert cached["quantity"] == "42.00000000"
    assert cached["avg_buy_price"] == "199.99000000"

    await r.delete(key)
    await r.aclose()
