-- Define the Source Table connected to Redpanda
CREATE TABLE market_data_raw (
    symbol_id STRING,
    exchange STRING,
    event_type STRING,
    price DOUBLE,
    quantity DOUBLE,
    tags ARRAY<STRING>, -- Assuming tags is an array of strings
    time_exchange TIMESTAMP,
    time_ingest TIMESTAMP,
    is_snapshot BOOLEAN,
    sequence BIGINT,
    WATERMARK FOR time_exchange AS time_exchange - INTERVAL '5' SECOND
) WITH (
    connector = 'kafka',
    topic = 'market_data_raw',
    bootstrap_servers = 'redpanda:9092',
    format = 'json'
);

-- Register UDF
CREATE FUNCTION is_wash_trade(json_data STRING) RETURNS BOOLEAN LANGUAGE RUST AS '
// (Rust logic implementation would be referenced here or inline for some platforms, but assuming external UDF linking mechanism or simplified inline for this example context)
// For Arroyo Cloud / Enterprise, UDFs are often uploaded separately. 
// However, standard SQL UDF definition might look like this:
fn is_wash_trade(json_data: &str) -> bool {
    // ... logic from src/lib.rs ...
    // For this SQL file, we assume the UDF is available as `is_wash_trade` taking the raw JSON row or extracted fields.
    // If we pass the whole row as JSON string, we need to serialize it first or use scalar UDF on `tags` directly.
    // Let's assume we pass the `tags` array directly for simplicity if Supported, 
    // OR we pass the construct JSON.
    // Given the previous UDF implementation expected `json_data` string, let's stick to that pattern 
    // by casting the row to JSON string if possible, or adjusting the UDF to take ARRAY<STRING>.
    true 
}
';
-- NOTE: In actual deployment, UDFs are often compiled into a stored artifact. 
-- For this file, we assume `is_wash_trade` is available.

-- Filtered Stream (View)
CREATE VIEW cleanliness_verified_data AS
SELECT *
FROM market_data_raw
WHERE NOT is_wash_trade(to_json_string(tags)); -- Assuming a helper to convert array to json string for the Rust UDF, or UDF accepts Array.

-- 1. OHLCV Aggregation (1 second)
CREATE TABLE market_data_1s_bar (
    symbol_id STRING,
    window_start TIMESTAMP,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    volume DOUBLE
) WITH (
    connector = 'kafka',
    topic = 'market_data_1s_bar',
    bootstrap_servers = 'redpanda:9092',
    format = 'json'
);

INSERT INTO market_data_1s_bar
SELECT
    symbol_id,
    tumble_start(time_exchange, INTERVAL '1' SECOND) as window_start,
    earliest(price) as open_price,
    max(price) as high_price,
    min(price) as low_price,
    latest(price) as close_price,
    sum(quantity) as volume
FROM cleanliness_verified_data
GROUP BY
    symbol_id,
    tumble(time_exchange, INTERVAL '1' SECOND);

-- 2. OHLCV Aggregation (1 minute)
CREATE TABLE market_data_1m_bar (
    symbol_id STRING,
    window_start TIMESTAMP,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    volume DOUBLE
) WITH (
    connector = 'kafka',
    topic = 'market_data_1m_bar',
    bootstrap_servers = 'redpanda:9092',
    format = 'json'
);

INSERT INTO market_data_1m_bar
SELECT
    symbol_id,
    tumble_start(time_exchange, INTERVAL '1' MINUTE) as window_start,
    earliest(price) as open_price,
    max(price) as high_price,
    min(price) as low_price,
    latest(price) as close_price,
    sum(quantity) as volume
FROM cleanliness_verified_data
GROUP BY
    symbol_id,
    tumble(time_exchange, INTERVAL '1' MINUTE);

-- 3. VWAP Calculation (Continuous)
-- VWAP = Sum(Price * Quantity) / Sum(Quantity)
-- We can maintain this state. Using a sliding window or continuous accumulation depending on "Continuous Query" requirement.
-- Usually VWAP is strictly intraday, resetting at market open. 
-- For this pipeline, assuming a simple continuous accumulation or a large window (e.g., 24h) for simplicity in demo.
-- Using a 24-hour tumbling window to simulate daily VWAP reset.

CREATE TABLE vwap_realtime (
    symbol_id STRING,
    window_start TIMESTAMP,
    vwap DOUBLE
) WITH (
    connector = 'redis', -- Hypothetical Redis Sink
    address = 'redis:6379',
    table = 'vwap'
);

-- Note: Arroyo might need a specific Redis connector definition.
-- If Redis is not standard, we use Kafka and have a separate consumer push to Redis. 
-- The prompt asked for "Sink Connectivity... push ... to Redpanda ... AND ... to Delta Lake".
-- Redis was mentioned as "Downstream... Hot store". 
-- Let's output VWAP to a Kafka topic `market_data_vwap` which can be consumed by the "Hot Store" writer.

CREATE TABLE market_data_vwap (
    symbol_id STRING,
    window_start TIMESTAMP,
    vwap DOUBLE,
    accumulated_volume DOUBLE
) WITH (
    connector = 'kafka',
    topic = 'market_data_vwap',
    bootstrap_servers = 'redpanda:9092',
    format = 'json'
);

INSERT INTO market_data_vwap
SELECT
    symbol_id,
    tumble_start(time_exchange, INTERVAL '1' DAY) as window_start, -- Daily VWAP
    sum(price * quantity) / sum(quantity) as vwap,
    sum(quantity) as accumulated_volume
FROM cleanliness_verified_data
GROUP BY
    symbol_id,
    tumble(time_exchange, INTERVAL '1' DAY);

-- 4. Archival to S3 (Delta Lake / Parquet)
CREATE TABLE archival_sink (
    symbol_id STRING,
    exchange STRING,
    event_type STRING,
    price DOUBLE,
    quantity DOUBLE,
    time_exchange TIMESTAMP,
    time_ingest TIMESTAMP
) WITH (
    connector = 'delta', -- or parquet/s3
    path = 's3://hft-datalake/market_data_raw/',
    parquet_compression = 'snappy'
);

INSERT INTO archival_sink
SELECT 
    symbol_id,
    exchange,
    event_type,
    price,
    quantity,
    time_exchange,
    time_ingest
FROM cleanliness_verified_data;
