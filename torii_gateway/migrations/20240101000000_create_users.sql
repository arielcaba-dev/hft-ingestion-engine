-- Create Users Table
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255), -- Optional if using external auth, but good to have
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create Subscription Tiers
CREATE TABLE IF NOT EXISTS subscription_tiers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL, -- 'free', 'pro', 'enterprise'
    rate_limit_per_second INT NOT NULL,
    websocket_enabled BOOLEAN DEFAULT FALSE,
    l3_data_enabled BOOLEAN DEFAULT FALSE,
    ds_mode_enabled BOOLEAN DEFAULT FALSE,
    monthly_credits INT DEFAULT 0,
    price_cents INT DEFAULT 0
);

-- Create User Subscriptions
CREATE TABLE IF NOT EXISTS user_subscriptions (
    user_id UUID PRIMARY KEY REFERENCES users(id),
    tier_id INT REFERENCES subscription_tiers(id),
    credits_remaining INT DEFAULT 0,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create API Keys
CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(id),
    key_hash VARCHAR(64) UNIQUE NOT NULL, -- SHA256 of the key
    key_prefix VARCHAR(8) NOT NULL, -- First 8 chars for user identification
    scopes TEXT[] DEFAULT '{}', -- Array of strings e.g., ['market_data:read', 'trade:execute']
    is_active BOOLEAN DEFAULT TRUE,
    last_used_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys(user_id);

-- Seeding Default Tiers
INSERT INTO subscription_tiers (name, rate_limit_per_second, websocket_enabled, l3_data_enabled, ds_mode_enabled, monthly_credits, price_cents)
VALUES 
    ('free', 10, FALSE, FALSE, FALSE, 10000, 0),
    ('pro', 500, TRUE, FALSE, FALSE, 1000000, 9900),
    ('enterprise', 10000, TRUE, TRUE, TRUE, 10000000, 99900)
ON CONFLICT (name) DO NOTHING;
