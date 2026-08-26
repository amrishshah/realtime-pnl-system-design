"""PnL formula. Provided as-is — the interesting decisions in this design
are about *when* and *how often* this gets called (Stages 4/5/9), not the
arithmetic itself."""
from __future__ import annotations

from decimal import Decimal


def compute_pnl(current_price: Decimal, avg_buy_price: Decimal, quantity: Decimal) -> Decimal:
    return (current_price - avg_buy_price) * quantity
