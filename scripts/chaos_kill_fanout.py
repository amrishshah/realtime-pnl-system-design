"""Dev tool for manually observing Stage 9's self-healing property against
the live docker-compose stack (the automated proof is
tests/stage09/test_self_heal.py, which cancels tasks in-process for
determinism — this script is for watching it happen for real).

Usage:
    python -m pnl.ticks.generator --symbol AAPL --interval 0.05 &   # start load
    python scripts/chaos_kill_fanout.py                              # then, separately:
    docker kill -s SIGKILL pnl-app                                   # simulate a crash
    docker compose start app                                         # let it come back

Watch GET /metrics before and after: tasks_failed will jump when the
container is killed mid-fan-out (in-flight tasks never get to increment
tasks_completed), but once the app restarts and the tick generator's next
tick arrives, every client's PnL is correct again with no replay, no queue
to drain, and no reconciliation step — because PnL is recomputed from
scratch on every tick.
"""
import time

import httpx

BASE_URL = "http://localhost:8000"

if __name__ == "__main__":
    print("Polling /metrics every 2s — Ctrl+C to stop.")
    print("In another terminal: docker kill -s SIGKILL pnl-app, then docker compose start app")
    while True:
        try:
            resp = httpx.get(f"{BASE_URL}/metrics", timeout=2)
            print(resp.json())
        except httpx.HTTPError as exc:
            print(f"app unreachable ({exc}) — this is expected right after a kill")
        time.sleep(2)
