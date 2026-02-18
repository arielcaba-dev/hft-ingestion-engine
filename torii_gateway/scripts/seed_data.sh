# Wait for Postgres to be ready
echo "Waiting for Postgres..."
until docker exec postgres pg_isready -U arroyo; do
  sleep 2
done

# Insert Test User
echo "Seeding Test User..."
USER_ID=$(docker exec -i postgres psql -U arroyo -d arroyo -t -q -c "INSERT INTO users (email) VALUES ('test_user@example.com') ON CONFLICT (email) DO UPDATE SET email=EXCLUDED.email RETURNING id;" | head -n 1 | tr -d '[:space:]')
if [ -z "$USER_ID" ]; then
    # Fallback to select if update didn't return (though it should)
    USER_ID=$(docker exec -i postgres psql -U arroyo -d arroyo -t -q -c "SELECT id FROM users WHERE email='test_user@example.com';" | head -n 1 | tr -d '[:space:]')
fi
echo "Created User ID: $USER_ID"

# Generate API Key (simple hash for demo)
# In real app, use the endpoint. Here we insert directly to bypass auth for bootstrapping.
# Key: "test_key_123"
# Hash: SHA256("test_key_123")
KEY_HASH=$(echo -n "test_key_123" | sha256sum | cut -d ' ' -f 1)

# Insert API Key
echo "Seeding API Key..."
docker exec -i postgres psql -U arroyo -d arroyo -c "INSERT INTO api_keys (user_id, key_hash, key_prefix, scopes) VALUES ('$USER_ID', '$KEY_HASH', 'test_key', '{\"market_data:read\", \"trade:execute\"}');"

# Subscribe User to Pro Tier (ID 2 usually, assuming migration created 1=Free, 2=Pro, 3=Enterprise)
echo "Subscribing User to Pro Tier..."
docker exec -i postgres psql -U arroyo -d arroyo -c "INSERT INTO user_subscriptions (user_id, tier_id, credits_remaining) VALUES ('$USER_ID', 2, 1000);"

# Seed Redis with Credits (since billing checks Redis first)
echo "Seeding Redis Credits..."
docker exec -i redis redis-cli SET "credits:$USER_ID" 1000

echo "Seeding Complete."
echo "Use API Key: 'test_key_123' for testing."
