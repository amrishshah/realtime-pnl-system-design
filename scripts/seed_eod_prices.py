"""Dev tool: simulates an official EOD closing-price feed, independent of
the live tick pipeline. Run before pnl/eod/reconciliation.py.

Usage:
    python scripts/seed_eod_prices.py --date 2024-01-15 AAPL=185.50 MSFT=402.10
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import date

import asyncmy


async def seed(close_date: str, prices: dict[str, str]) -> None:
    conn = await asyncmy.connect(
        host="localhost", port=3306, user="pnl", password="pnl", db="pnl"
    )
    try:
        async with conn.cursor() as cur:
            for symbol, price in prices.items():
                await cur.execute(
                    "INSERT INTO eod_closing_prices (symbol, close_date, close_price) "
                    "VALUES (%s, %s, %s) "
                    "ON DUPLICATE KEY UPDATE close_price = VALUES(close_price)",
                    (symbol, close_date, price),
                )
        await conn.commit()
        print(f"Seeded {len(prices)} closing price(s) for {close_date}.")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=str(date.today()), help="YYYY-MM-DD")
    parser.add_argument("prices", nargs="+", help="SYMBOL=PRICE pairs, e.g. AAPL=185.50")
    args = parser.parse_args()

    price_map = dict(p.split("=", 1) for p in args.prices)
    asyncio.run(seed(args.date, price_map))
