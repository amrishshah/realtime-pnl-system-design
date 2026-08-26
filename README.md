# Real-Time PnL Calculation Service

A staged, codecrafters-style build of a real-time PnL (profit & loss)
calculation service: trade events + market data ticks → sub-second
unrealized PnL updates per client, per position.

This repo is **not** a finished implementation — it's a course. The
infrastructure, wiring, and data shapes are provided; the core logic in
each stage is a `NotImplementedError` stub you fill in, verified by a
pytest suite that acts as the "tester." Stages build on each other in one
evolving codebase, mirroring the trap → fix reasoning of the original
design walkthrough.

## Prerequisites

- Docker + Docker Compose
- Python 3.11+
- `pip install -e ".[dev]"` from the repo root

## Stack

MySQL (binlog CDC) → Debezium → Kafka → Redis / ClickHouse, fronted by a
FastAPI service, all wired together via `docker-compose.yml`. Everything
runs as real containers — no mocked infrastructure.

## How to work through this course

Each stage lives in `stages/NN_name.md` and has:
- **Goal** — what you're building and why (tied to a specific trap/fix from
  the design).
- **What you're building** — the exact files/classes to implement.
- **Run it** / **Verify** — bring up the stack, run that stage's
  `tests/stageNN/` directory, confirm green, move to the next stage.

Start here:
```bash
docker compose up -d --build
pytest tests/stage00
```

Then work through `stages/01_cdc_binlog.md` onward in order — later stages
assume earlier ones are implemented and re-run their tests as regressions.

## Stage index

| Stage | Design doc trap | What it fixes |
|---|---|---|
| [00](stages/00_bootstrap.md) | — | Repo/infra bootstrap |
| [01](stages/01_cdc_binlog.md) | Dual-write race between trade write and index update | CDC off the MySQL binlog — one write, everything else derived |
| [02](stages/02_idempotent_index.md) | At-least-once CDC delivery | Redis SET for idempotent indexing |
| [03](stages/03_read_through_bloom.md) | Read-through hammering MySQL for unheld symbols | Bloom filter gate + read-through position cache |
| [04](stages/04_fanout.md) | One slow client blocking a whole tick's fan-out | Per-client asyncio tasks + bounded semaphore |
| [05](stages/05_coalescing.md) | Unbounded queueing under tick bursts | Coalesce to latest-per-symbol |
| [06](stages/06_two_consumers.md) | Coalescing silently dropping chart-needed data | Branch raw ticks to ClickHouse before coalescing |
| [07](stages/07_weighted_avg_cost_basis.md) | Stale buy price on repeat trades | Weighted-average cost basis computed in the trade service |
| [08](stages/08_concurrent_trades.md) | Lost update on concurrent trades | `SELECT ... FOR UPDATE` |
| [09](stages/09_crash_self_heal.md) | Crash mid-fan-out | Self-healing via full recompute every tick |
| [10](stages/10_reconnect.md) | Client reconnect after disconnection | Redis hash snapshot + Lua atomic total |
| [11](stages/11_counting_bloom_filter.md) | Bloom filter can't delete safely | Counting Bloom Filter |
| [12](stages/12_watermarking.md) | Out-of-order/late ticks | Event-time comparison + watermarking |
| [13](stages/13_eod_reconciliation.md) | Live path is fast but unverified | Independent EOD reconciliation |

## Repo layout

```
pnl/            the service package — exercises live here
docker/         MySQL/Debezium/ClickHouse config
scripts/        dev tools (tick generator, connector registration, EOD seed, chaos)
stages/         course content, one file per stage
tests/          the tester — one directory per stage
```
