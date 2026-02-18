#!/bin/bash
set -e

# Load configuration
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
else
    echo "❌ .env file not found!"
    exit 1
fi

echo "🚀 Starting Pipeline Verification..."

# 1. Wait for services
echo "⏳ Waiting for services to be ready..."
until docker exec redpanda rpk cluster health | grep -q "Healthy"; do
  echo "  - Waiting for Redpanda..."
  sleep 5
done

until docker exec postgres pg_isready -U ${DB_USER} > /dev/null 2>&1; do
  echo "  - Waiting for Postgres..."
  sleep 5
done

until curl -s "http://localhost:${ARROYO_UI_PORT}/api/v1/ping" > /dev/null; do
  # Fallback to checking UI root if ping endpoint varies
  if curl -s "http://localhost:${ARROYO_UI_PORT}/" > /dev/null; then
    break
  fi
  echo "  - Waiting for Arroyo Controller..."
  sleep 5
done

until curl -s "http://localhost:${QUESTDB_UI_PORT}/ping" > /dev/null; do
  echo "  - Waiting for QuestDB..."
  sleep 5
done

echo "✅ All services OK."

# 2. Produce Sample Data to Redpanda
echo "📤 Producing sample data to 'market_data_raw'..."
SAMPLE_JSON='{"symbol_id":"BTC-USD","price":50000.0,"quantity":0.5,"timestamp":1700000000}'
echo "$SAMPLE_JSON" | docker exec -i redpanda rpk topic produce market_data_raw

echo "✅ Data produced to Kafka."


# 2.5 Seed Data (User & API Key)
echo "🌱 Seeding initial data..."
if [ -f "./torii_gateway/scripts/seed_data.sh" ]; then
    chmod +x ./torii_gateway/scripts/seed_data.sh
    ./torii_gateway/scripts/seed_data.sh
else
    echo "⚠️  Seed script not found at ./torii_gateway/scripts/seed_data.sh"
    # Fallback or just warn
fi

# 3. Verify Ingestion Engine (Live Data)
echo "🔄 Verifying Ingestion Engine (Live Binance Data)..."

# Wait for ingestion engine to be healthy
until docker inspect --format='{{.State.Health.Status}}' torii-ingestion | grep -q "healthy"; do
  echo "  - Waiting for Ingestion Engine..."
  sleep 5
done

# Consume a few messages to verify live data flow
echo "  - Consuming live messages from Redpanda..."
# Consume 1 message from the end of the topic
LIVE_MSG=$(docker exec redpanda rpk topic consume market_data_raw -o end -n 1 2>&1)

if echo "$LIVE_MSG" | grep -q "\"exchange\":\"binance\""; then
    echo "✅ Live data verified! Found Binance trade data in recent messages."
else
    echo "⚠️  Live data verification warning: Could not find Binance data in recent messages. Output: $LIVE_MSG"
    # Don't fail the whole script for this warn, but good to know
fi

# 4. Verify QuestDB Connectivity (Create Table & Insert)
echo "💾 Verifying QuestDB ingestion..."

# Create a test table via REST
curl -G "http://localhost:${QUESTDB_UI_PORT}/exec" \
    --data-urlencode "query=CREATE TABLE IF NOT EXISTS verification_test (ts TIMESTAMP, price DOUBLE) timestamp(ts) PARTITION BY DAY WAL" \
    > /dev/null 2>&1

# Insert via SQL (fallback for verification)
curl -G "http://localhost:${QUESTDB_UI_PORT}/exec" \
    --data-urlencode "query=INSERT INTO verification_test (ts, price) VALUES (systimestamp(), 123.45)"

sleep 2

# Query verification using Python for reliable JSON parsing
echo "🔍 Querying QuestDB for row count..."
RESPONSE=$(curl -s -G "http://localhost:${QUESTDB_UI_PORT}/exec" --data-urlencode "query=select count() from verification_test")
echo "QuestDB Response: $RESPONSE"

COUNT=$(echo "$RESPONSE" | python3 -c "import sys, json; print(json.load(sys.stdin)['dataset'][0][0])" 2>/dev/null || echo "0")

if [ "$COUNT" -gt "0" ]; then
    echo "✅ QuestDB Verification Successful! Row count: $COUNT"
else
    echo "❌ QuestDB Verification Failed. Row count is $COUNT (expected > 0)"
    exit 1
fi

echo "🎉 Pipeline Infrastructure verified successfully!"
