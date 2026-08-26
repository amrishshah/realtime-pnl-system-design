#!/usr/bin/env bash
# Registers Debezium connectors against the Kafka Connect REST API.
# Run after `docker compose up -d` once mysql + kafka-connect are healthy
# (kafka-connect can take 20-30s to come up -- retry if you get connection refused).
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

register() {
    local config_file="$1"
    local name
    name=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['name'])" "$config_file")
    echo "Registering connector '$name' from $config_file ..."
    curl -sS -X POST -H "Content-Type: application/json" \
        --data "@$config_file" \
        "$CONNECT_URL/connectors" | python3 -m json.tool
    echo
}

register docker/debezium/connector-trades.json

if [[ -f docker/debezium/connector-positions.json ]]; then
    register docker/debezium/connector-positions.json
fi
