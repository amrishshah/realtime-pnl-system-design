# Real-Time PnL Calculation Service

A staged, codecrafters-style build of a real-time PnL (profit & loss)
calculation service: trade events + market data ticks → sub-second
unrealized PnL updates per client, per position.

The repo is both a course and its reference implementation. Each stage has
a write-up in `stages/` and a pytest suite in `tests/stageNN/` that acts as
the "tester". The `main` code is fully implemented, and the git history has
**one commit per stage**, so you can check out any stage to see exactly what
it adds:

```bash
git log --oneline              # "Stage N: ..." commits, in order
git show <stage-commit>        # just that stage's change
```

To work through it yourself, check out the scaffold commit ("Add staged,
codecrafters-style course ...") where the core logic is still
`NotImplementedError` stubs, and implement stage by stage against the tests.

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

## Running the tests

The suites run against the real stack, so bring it up and register the
Debezium connector first:

```bash
docker compose up -d --build
./scripts/register_connectors.sh   # once kafka-connect is up (~20-30s)
pytest tests
```

The `app` and `cdc-worker` containers are the two processes: the FastAPI
service (trades, ticks, WebSocket, `/metrics`) and the CDC projectors. The
EOD job runs on demand: `python -m pnl.eod.reconciliation --date YYYY-MM-DD`
(exits non-zero on drift).

## Production notes beyond the design doc

Building this against real MySQL, Kafka/Debezium and Redis surfaced several
issues the design doc doesn't cover; each is explained in its stage write-up
and commit:

- One Debezium connector for both tables: two connectors sharing a
  `topic.prefix` collide on JMX metrics and the second one stalls (Stage 1).
- Index membership follows the `positions` stream, so entries and exits stay
  ordered; feeding them from two topics can drop a re-entering client
  (Stage 11).
- The coalescer waits for each tick's fan-out to drain; otherwise every tick
  adds another round of per-client tasks (Stage 5).
- First-trade gap-lock deadlocks under `SELECT ... FOR UPDATE` are retried
  (Stage 8).
- Counting-Bloom counters and index membership change in one atomic Lua
  script; rebuilds count holders and swap in atomically (Stage 11).
- ClickHouse writes are batched; money totals in Lua use exact fixed-point
  arithmetic, not doubles (Stages 6, 10).
- redis-py's default pool caps at 100 connections and fails fast; the app
  uses a blocking pool and caps fan-out concurrency to it (Stage 4).
