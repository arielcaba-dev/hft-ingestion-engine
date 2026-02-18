# Wait for Postgres to be ready
echo "Waiting for Postgres..."
until docker exec postgres pg_isready -U arroyo; do
  sleep 2
done

# Insert Test User
# Check if user with this key already exists to prevent duplicates
KEY_HASH=$(echo -n "test_key_123" | sha256sum | cut -d ' ' -f 1)
EXISTING_USER_ID=$(docker exec -i postgres psql -U arroyo -d arroyo -t -q -c "SELECT user_id FROM api_keys WHERE key_hash='$KEY_HASH';" | head -n 1 | tr -d '[:space:]')

if [ ! -z "$EXISTING_USER_ID" ]; then
    echo "Test user already exists. ID: $EXISTING_USER_ID"
    USER_ID=$EXISTING_USER_ID
else
    # Insert Test User (Schema: id, tier, balance)
    echo "Seeding Test User..."
    USER_ID=$(docker exec -i postgres psql -U arroyo -d arroyo -t -q -c "INSERT INTO users (tier, balance) VALUES ('pro', 1000) RETURNING id;" | head -n 1 | tr -d '[:space:]')
    echo "Created User ID: $USER_ID"
fi

# Generate API Key (simple hash for demo)
# In real app, use the endpoint. Here we insert directly to bypass auth for bootstrapping.
# Key: "test_key_123"
# Hash: SHA256("test_key_123")
# KEY_HASH is calculated above

# Insert API Key if it doesn't exist
if [ -z "$EXISTING_USER_ID" ]; then
    echo "Seeding API Key..."
    docker exec -i postgres psql -U arroyo -d arroyo -c "INSERT INTO api_keys (user_id, key_hash, key_prefix, scopes) VALUES ('$USER_ID', '$KEY_HASH', 'test_key', '{\"market_data:read\", \"trade:execute\"}');"
else
    echo "API Key already exists."
fi

# Subscribe User to Pro Tier (ID 2 usually, assuming migration created 1=Free, 2=Pro, 3=Enterprise)
echo "Subscribing User to Pro Tier..."
docker exec -i postgres psql -U arroyo -d arroyo -c "INSERT INTO user_subscriptions (user_id, tier_id, credits_remaining) VALUES ('$USER_ID', 2, 1000);"

# Seed Redis with Credits (since billing checks Redis first)
echo "Seeding Redis Credits..."
docker exec -i redis redis-cli SET "credits:$USER_ID" 1000

echo "Seeding Complete."
echo "Use API Key: 'test_key_123' for testing."
