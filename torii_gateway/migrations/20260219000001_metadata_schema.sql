-- Up Migration
CREATE TABLE exchanges (
    id VARCHAR(32) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE assets (
    id VARCHAR(32) PRIMARY KEY, -- e.g., BTC, USD
    symbol VARCHAR(32) NOT NULL,
    name VARCHAR(255),
    decimals INTEGER NOT NULL DEFAULT 8,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE symbols (
    id VARCHAR(64) PRIMARY KEY, -- e.g., BINANCE:BTC-USD
    exchange_id VARCHAR(32) NOT NULL REFERENCES exchanges(id),
    base_asset_id VARCHAR(32) NOT NULL REFERENCES assets(id),
    quote_asset_id VARCHAR(32) NOT NULL REFERENCES assets(id),
    symbol VARCHAR(64) NOT NULL, -- e.g., BTCUSDT
    normalized_symbol VARCHAR(64) NOT NULL, -- e.g., BTC-USD
    price_precision DOUBLE PRECISION NOT NULL, -- Tick size (e.g., 0.01)
    size_precision DOUBLE PRECISION NOT NULL, -- Lot size (e.g., 0.0001)
    min_order_size DOUBLE PRECISION DEFAULT 0.0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(exchange_id, symbol)
);

-- Seed Initial Data
INSERT INTO exchanges (id, name) VALUES ('binance', 'Binance');

INSERT INTO assets (id, symbol, name, decimals) VALUES 
('BTC', 'BTC', 'Bitcoin', 8),
('ETH', 'ETH', 'Ethereum', 18),
('USD', 'USD', 'US Dollar', 2),
('USDT', 'USDT', 'Tether', 6);

INSERT INTO symbols (id, exchange_id, base_asset_id, quote_asset_id, symbol, normalized_symbol, price_precision, size_precision) VALUES
('binance:BTC-USD', 'binance', 'BTC', 'USD', 'BTCUSDT', 'BTC-USD', 0.01, 0.00001),
('binance:ETH-USD', 'binance', 'ETH', 'USD', 'ETHUSDT', 'ETH-USD', 0.01, 0.0001);

-- Down Migration
-- DROP TABLE symbols;
-- DROP TABLE assets;
-- DROP TABLE exchanges;
