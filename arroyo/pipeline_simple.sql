-- Arroyo Pipeline for Crypto Market Data Processing
-- Corrected syntax for Arroyo SQL with proper window struct handling

-- Source Table connected to Redpanda
CREATE TABLE market_data_raw (
    symbol_id TEXT,
    exchange TEXT,
    event_type TEXT,
    price DOUBLE,
    quantity DOUBLE,
    time_exchange TIMESTAMP,
    time_ingest TIMESTAMP,
    is_snapshot BOOLEAN,
    sequence BIGINT
) WITH (
    connector = 'kafka',
    type = 'source',
    bootstrap_servers = 'redpanda:9092',
    topic = 'market_data_raw',
    format = 'json',
    'source.offset' = 'earliest'
);

-- OHLCV 1-second bars
CREATE TABLE market_data_1s_bar (
    symbol_id TEXT,
    window_start TIMESTAMP,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    volume DOUBLE
) WITH (
    connector = 'kafka',
    type = 'sink',
    bootstrap_servers = 'redpanda:9092',
    topic = 'market_data_1s_bar',
    format = 'json'
);

INSERT INTO market_data_1s_bar
SELECT
    symbol_id,
    tumble(interval '1 second')['start'] as window_start,
    (array_agg(price ORDER BY time_exchange))[1] as open_price,
    max(price) as high_price,
    min(price) as low_price,
    (array_agg(price ORDER BY time_exchange DESC))[1] as close_price,
    sum(quantity) as volume
FROM market_data_raw
GROUP BY symbol_id, tumble(interval '1 second');

-- OHLCV 1-minute bars  
CREATE TABLE market_data_1m_bar (
    symbol_id TEXT,
    window_start TIMESTAMP,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    volume DOUBLE
) WITH (
    connector = 'kafka',
    type = 'sink',
    bootstrap_servers = 'redpanda:9092',
    topic = 'market_data_1m_bar',
    format = 'json'
);

INSERT INTO market_data_1m_bar
SELECT
    symbol_id,
    tumble(interval '1 minute')['start'] as window_start,
    (array_agg(price ORDER BY time_exchange))[1] as open_price,
    max(price) as high_price,
    min(price) as low_price,
    (array_agg(price ORDER BY time_exchange DESC))[1] as close_price,
    sum(quantity) as volume
FROM market_data_raw
GROUP BY symbol_id, tumble(interval '1 minute');

-- VWAP Calculation
CREATE TABLE market_data_vwap (
    symbol_id TEXT,
    window_start TIMESTAMP,
    vwap DOUBLE,
    accumulated_volume DOUBLE
) WITH (
    connector = 'kafka',
    type = 'sink',
    bootstrap_servers = 'redpanda:9092',
    topic = 'market_data_vwap',
    format = 'json'
);

INSERT INTO market_data_vwap
SELECT
    symbol_id,
    tumble(interval '1 minute')['start'] as window_start,
    sum(price * quantity) / NULLIF(sum(quantity), 0) as vwap,
    sum(quantity) as accumulated_volume
FROM market_data_raw
WHERE quantity > 0
GROUP BY symbol_id, tumble(interval '1 minute');
