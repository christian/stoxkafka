#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
CONTAINER_NAME="${CONTAINER_NAME:-stox-kafka}"

topics=(
  "market.trades.raw:24:Raw exchange trades. Key: exchange:symbol"
  "market.candles.1s:24:One-second live candles. Key: exchange:symbol:1s"
  "market.candles.1m:24:One-minute live candles. Key: exchange:symbol:1m"
  "market.candles.5m:12:Five-minute live candles. Key: exchange:symbol:5m"
  "market.candles.15m:12:Fifteen-minute live candles. Key: exchange:symbol:15m"
  "market.candles.1h:6:One-hour live candles. Key: exchange:symbol:1h"
  "market.candles.1d:3:Daily live candles. Key: exchange:symbol:1d"
)

topic_exists() {
  local topic="$1"

  docker exec "$CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --describe \
    --topic "$topic" >/dev/null 2>&1
}

current_partitions() {
  local topic="$1"

  docker exec "$CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --describe \
    --topic "$topic" |
    awk -F'PartitionCount: ' '/PartitionCount:/ { split($2, parts, "\t"); print parts[1]; exit }' || true
}

for spec in "${topics[@]}"; do
  IFS=":" read -r topic partitions description <<< "$spec"

  if topic_exists "$topic"; then
    existing_partitions="$(current_partitions "$topic")"

    if (( existing_partitions < partitions )); then
      echo "Increasing $topic partitions from $existing_partitions to $partitions"
      docker exec "$CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --alter \
        --topic "$topic" \
        --partitions "$partitions"
    else
      echo "Exists: $topic partitions=$existing_partitions"
    fi
  else
    echo "Creating $topic partitions=$partitions - $description"
    docker exec "$CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "$BOOTSTRAP_SERVER" \
      --create \
      --topic "$topic" \
      --partitions "$partitions" \
      --replication-factor 1
  fi
done

echo
docker exec "$CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --list
