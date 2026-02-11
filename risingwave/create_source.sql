DROP SOURCE IF EXISTS market_data_raw CASCADE;
CREATE SOURCE market_data_raw (
    symbol_id VARCHAR,
    exchange VARCHAR,
    event_type VARCHAR,
    price DOUBLE,
    quantity DOUBLE,
    tags VARCHAR[],
    time_exchange TIMESTAMPTZ,
    time_ingest TIMESTAMPTZ,
    is_snapshot BOOLEAN,
    sequence BIGINT,
    WATERMARK FOR time_exchange AS time_exchange - INTERVAL '5' SECOND
) WITH (
    connector = 'kafka',
    topic = 'market_data_raw',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;
