"""Dev tool: posts a stream of random-walk ticks to the ingestion endpoint.
Not part of the automated test suite — run manually to watch the pipeline
work end-to-end, e.g.:

    python -m pnl.ticks.generator --symbol AAPL --interval 0.1
"""
from __future__ import annotations

import argparse
import asyncio
import random
from datetime import datetime, timezone
from decimal import Decimal

import httpx


async def run(symbol: str, base_url: str, interval: float, start_price: float) -> None:
    price = start_price
    async with httpx.AsyncClient(base_url=base_url, timeout=5) as client:
        while True:
            price = max(0.01, price + random.uniform(-1, 1))
            await client.post(
                "/ticks",
                json={
                    "symbol": symbol,
                    "price": f"{price:.2f}",
                    "event_time": datetime.now(timezone.utc).isoformat(),
                },
            )
            print(f"tick {symbol} @ {price:.2f}")
            await asyncio.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--interval", type=float, default=0.1)
    parser.add_argument("--start-price", type=float, default=150.0)
    args = parser.parse_args()
    asyncio.run(run(args.symbol, args.base_url, args.interval, args.start_price))
