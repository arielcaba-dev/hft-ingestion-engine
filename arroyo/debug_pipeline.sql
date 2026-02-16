
CREATE TABLE market_data_v2 (
    symbol_id STRING,
    price DOUBLE,
    quantity DOUBLE,
    time_exchange TIMESTAMP
) WITH (
    connector = 'kafka',
    topic = 'market_data_v2',
    bootstrap_servers = 'redpanda:9092',
    format = 'json',
    type = 'source'
);

CREATE TABLE debug_sink (
    symbol_id STRING,
    price DOUBLE,
    quantity DOUBLE,
    time_exchange TIMESTAMP
) WITH (
    connector = 'kafka',
    topic = 'debug_output',
    bootstrap_servers = 'redpanda:9092',
    format = 'json',
    type = 'sink'
);

INSERT INTO debug_sink
SELECT symbol_id, price, quantity, time_exchange
FROM market_data_v2;
