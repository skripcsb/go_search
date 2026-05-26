#!/usr/bin/env bash
set -euo pipefail

# Simple load test helper.
# Configure via ENV or defaults below.

EVENTS="${EVENTS:-10000}"
UNIQUE="${UNIQUE:-200}"
DURATION="${DURATION:-30s}"
CONCURRENCY="${CONCURRENCY:-100}"
TOPIC="${TOPIC:-search.events}"
URL="${URL:-http://localhost:8080/api/v1/top?n=10}"

echo "Produce $EVENTS events (unique=$UNIQUE) to Kafka topic $TOPIC"
./scripts/produce.sh "$EVENTS" "$UNIQUE" "$TOPIC"

if ! command -v hey >/dev/null 2>&1; then
  echo "'hey' not found. Install: 'go install github.com/rakyll/hey@latest' or use your preferred load tester"
  exit 1
fi

echo "Running load test: duration=$DURATION concurrency=$CONCURRENCY -> $URL"
hey -z "$DURATION" -c "$CONCURRENCY" "$URL"
