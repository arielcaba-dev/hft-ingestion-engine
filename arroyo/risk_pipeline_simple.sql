-- Simplified Risk Pipeline (Phase 1: OHLCV & Windowing)

-- 1. SOURCE: Redpanda
CREATE TABLE market_data_raw (
    symbol_id STRING,
    price DOUBLE,
    quantity DOUBLE,
    time_exchange TIMESTAMP NOT NULL,
    WATERMARK FOR time_exchange AS time_exchange - INTERVAL '5' SECOND
) WITH (
    connector = 'kafka',
    topic = 'market_data_v2',
    bootstrap_servers = 'redpanda:9092',
    format = 'json',
    type = 'source'
);

-- 2. OHLCV AGGREGATION (1 Minute)
CREATE VIEW ohlcv_1m AS
SELECT
    symbol_id,
    max(time_exchange) as window_end,
    min(price) as open,
    max(price) as high,
    min(price) as low,
    max(price) as close,
    sum(quantity) as volume
FROM market_data_raw
GROUP BY
    symbol_id,
    tumble(INTERVAL '1' MINUTE);

-- 3. SINK: Redpanda (ohlcv_1m)
-- The existing bridge.py or a new consumer will pick this up and write to QuestDB
CREATE TABLE ohlcv_sink (
    symbol_id STRING,
    window_end TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE
) WITH (
    connector = 'kafka',
    topic = 'ohlcv_1m',
    bootstrap_servers = 'redpanda:9092',
    format = 'json',
    type = 'sink'
);

INSERT INTO ohlcv_sink
SELECT
    symbol_id,
    window_end,
    open,
    high,
    low,
    close,
    volume
FROM ohlcv_1m;
