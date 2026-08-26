"""Shared data shapes used across the pipeline. These are provided as-is —
the exercise in each stage is the logic that produces/consumes them, not
these definitions."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass(frozen=True, slots=True)
class Trade:
    client_id: str
    symbol: str
    side: Side
    quantity: Decimal
    price: Decimal
    event_time: datetime


@dataclass(frozen=True, slots=True)
class Tick:
    symbol: str
    price: Decimal
    event_time: datetime
    received_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    client_id: str
    symbol: str
    quantity: Decimal
    avg_buy_price: Decimal
