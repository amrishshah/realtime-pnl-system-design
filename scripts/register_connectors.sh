#!/usr/bin/env bash
# Registers the Debezium connector against the Kafka Connect REST API.
# Run after `docker compose up -d` once mysql + kafka-connect are healthy
# (kafka-connect can take 20-30s to come up -- retry if you get connection refused).
#
# One connector captures both `pnl.trades` and `pnl.positions` (topics
# pnl.pnl.trades / pnl.pnl.positions). Two connectors sharing a topic.prefix
# collide on Debezium's JMX metrics name and stall, so keep it to one.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONFIG_FILE="docker/debezium/connector-pnl.json"

name=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['name'])" "$CONFIG_FILE")
echo "Registering connector '$name' from $CONFIG_FILE ..."
curl -sS -X POST -H "Content-Type: application/json" \
    --data "@$CONFIG_FILE" \
    "$CONNECT_URL/connectors" | python3 -m json.tool
