-- Create Credit Transactions Table
CREATE TABLE IF NOT EXISTS credit_transactions (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    amount INT NOT NULL, -- Negative for deductions, positive for top-ups
    balance_after INT NOT NULL,
    reason VARCHAR(255), -- 'query_execution', 'subscription_renewal', 'manual_topup'
    metadata JSONB, -- Store query details, endpoint accessed, etc.
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_credit_transactions_user ON credit_transactions(user_id, created_at DESC);
