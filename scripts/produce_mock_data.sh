#!/bin/bash
set -e

# Produce mock market data to Redpanda

TOPIC="market_data_raw"
BROKER="localhost:19092"

echo "Creating topic: $TOPIC"
docker exec redpanda rpk topic create $TOPIC --brokers $BROKER 2>/dev/null || echo "Topic already exists"

echo "Producing mock data..."

# Generate 10 sample trade events
for i in {1..10}; do
  TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")
  PRICE=$(echo "50000 + $RANDOM % 1000" | bc)
  QUANTITY=$(echo "scale=4; $RANDOM / 32767" | bc)
  
  JSON_DATA=$(cat <<EOF
{
  "symbol_id": "BTC-USD",
  "exchange": "COINBASE",
  "event_type": "trade",
  "price": $PRICE,
  "quantity": $QUANTITY,
  "tags": ["live", "spot"],
  "time_exchange": "$TIMESTAMP",
  "time_ingest": "$TIMESTAMP",
  "is_snapshot": false,
  "sequence": $i
}
EOF
)
  
  echo "$JSON_DATA" | docker exec -i redpanda rpk topic produce $TOPIC --brokers $BROKER
  echo "Produced trade $i: BTC-USD @ \$$PRICE, qty: $QUANTITY"
  sleep 0.1
done

echo "Mock data production complete!"
echo "Total messages produced: 10"
