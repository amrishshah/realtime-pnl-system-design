# Stage 0 — Bootstrap

## Goal
Get a runnable skeleton: MySQL in Docker (with binary logging enabled, since every later
stage depends on CDC reading that binlog), a Python package structure, and a FastAPI app
that responds on `/health`.

## Why this stage exists
Every subsequent stage in this course depends on two things being true from day one:
1. MySQL's binlog is enabled in `ROW` format with GTIDs — Debezium (Stage 1) cannot capture
   row-level changes without this, and turning it on later means throwing away MySQL data
   or restarting from a fresh binlog position. Better to bake it into the compose file now.
2. A single running service process exists to hang every later route/consumer off of.

## What was built
- `docker-compose.yml` — `mysql` (with `docker/mysql/my.cnf` mounted for
  `binlog_format=ROW`, `gtid_mode=ON`), `adminer` (DB browser on :8081), and `app`
  (the FastAPI service, hot-reloading via a mounted volume).
- `pnl/config.py` — `pydantic-settings`-based config, env-prefixed `PNL_*`.
- `pnl/main.py` — `GET /health` → `{"status": "ok"}`.

## Verify
```bash
docker compose up -d --build
curl localhost:8000/health   # {"status":"ok"}
pytest tests/stage00
```
