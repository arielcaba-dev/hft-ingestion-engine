-- ========================================================================
-- PIPELINE: Stable Path (Webhook / HTTP Sink)
-- Description: Ingests market data and pushes to QuestDB via HTTP ILP.
-- Pros: No external dependencies, works in restricted networks.
-- Cons: Higher per-request overhead than TCP.
-- ========================================================================

-- 1. Source: Raw Market Data from Redpanda
CREATE TABLE market_data_raw (
    symbol_id STRING,
    price DOUBLE,
    quantity DOUBLE,
    time_exchange TIMESTAMP
) WITH (
    connector = 'kafka',
    topic = 'market_data_raw',
    bootstrap_servers = 'redpanda:9092',
    format = 'json',
    type = 'source'
);

-- 2. Processing: Format as InfluxDB Line Protocol (ILP)
-- Format: table_name,symbol=VAL price=VAL,quantity=VAL TIMESTAMP_NANOS
-- Example: trades,symbol=BTC-USD price=65000.0,quantity=0.5 1678900000000000
CREATE VIEW trades_ilp AS
SELECT
    concat(
        'trades,symbol=', symbol_id, ' ',
        'price=', CAST(price AS STRING), ',',
        'quantity=', CAST(quantity AS STRING), ' ',
        -- Convert timestamp to nanoseconds (Epoch)
        CAST(CAST(EXTRACT(EPOCH FROM time_exchange) * 1000000000 AS BIGINT) AS STRING)
    ) as message
FROM market_data_raw;

-- 3. Sink: Webhook to QuestDB HTTP Endpoint
CREATE TABLE questdb_http_sink (
    message STRING
) WITH (
    connector = 'webhook',
    endpoint = 'http://questdb:9000/write',
    headers = 'Content-Type: text/plain',
    format = 'json'
);

INSERT INTO questdb_http_sink
SELECT message FROM trades_ilp;
