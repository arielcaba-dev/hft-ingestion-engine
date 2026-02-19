-- source: ohlcv_1m (JSON format from Metadata/Risk pipeline)
CREATE TABLE ohlcv_source (
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
    type = 'source',
    format = 'json',
    event_time_field = 'window_end'
);

-- sink: risk_impermanent_loss
CREATE TABLE risk_impermanent_loss (
    symbol_id STRING,
    window_end TIMESTAMP,
    il_score DOUBLE,
    entry_price DOUBLE,
    current_price DOUBLE
) WITH (
    connector = 'kafka',
    topic = 'risk_impermanent_loss',
    bootstrap_servers = 'redpanda:9092',
    type = 'sink',
    format = 'json'
);

-- Calculation using TUMBLE window (1 Hour updates)
-- 1. Aggregation View
CREATE VIEW il_calculation AS
SELECT
    symbol_id,
    max(window_end) as window_end,
    -- IL Formula
    (2 * sqrt(last_value(close) / avg(close)) / (1 + (last_value(close) / avg(close))) - 1) as il_score,
    avg(close) as entry_price,
    last_value(close) as current_price
FROM ohlcv_source
GROUP BY
    symbol_id,
    tumble(interval '1' hour);

-- 2. Sink
INSERT INTO risk_impermanent_loss
SELECT
    symbol_id,
    window_end,
    il_score,
    entry_price,
    current_price
FROM il_calculation;
