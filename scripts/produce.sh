#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/produce.sh [events] [unique] [topic]
# Defaults: events=10000 unique=200 topic=search.events

EVENTS="${1:-10000}"
UNIQUE="${2:-200}"
TOPIC="${3:-search.events}"

echo "Producing $EVENTS events to topic '$TOPIC' (unique=$UNIQUE)..."

docker compose exec kafka bash -lc "for i in \$(seq 1 $EVENTS); do ts=\$(date -u +%Y-%m-%dT%H:%M:%SZ); printf '%s\n' \"{\\\"query\\\":\\\"query-\\\$((i % $UNIQUE))\\\",\\\"request_id\\\":\\\"r\\\$i\\\",\\\"user_id\\\":\\\"u\\\$((i%100))\\\",\\\"timestamp\\\":\\\"\\\$ts\\\"}\"; done | kafka-console-producer.sh --bootstrap-server kafka:9092 --topic $TOPIC"

echo "Done."
