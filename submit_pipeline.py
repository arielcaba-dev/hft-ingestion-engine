
import json
import requests
import sys

import os

ARROYO_URL = os.getenv('ARROYO_URL', "http://localhost:5115")

import os
import re

ARROYO_URL = os.getenv('ARROYO_URL', "http://localhost:5115")

# Read SQL
try:
    with open("arroyo/risk_pipeline.sql", "r") as f:
        sql_content = f.read()
except FileNotFoundError:
    print("SQL file not found")
    sys.exit(1)

# Standardize SQL
compatible_sql = """
-- 1. SOURCE: Redpanda
CREATE TABLE market_data_raw (
    symbol_id STRING,
    price DOUBLE,
    quantity DOUBLE,
    time_exchange TIMESTAMP NOT NULL,
    WATERMARK FOR time_exchange AS time_exchange - INTERVAL '5' SECOND
) WITH (
    connector = 'kafka',
    topic = 'market_data_raw',
    bootstrap_servers = 'redpanda:9092',
    format = 'json',
    type = 'source'
);

-- 2. OHLCV & HISTORY AGGREGATION
CREATE VIEW ohlcv_1m AS
SELECT
    symbol_id,
    max(time_exchange) as window_end,
    min(price) as open,
    max(price) as high,
    min(price) as low,
    max(price) as close,
    sum(quantity) as volume,
    ARRAY_AGG(price) as price_history
FROM market_data_raw
GROUP BY
    symbol_id,
    tumble(INTERVAL '1' MINUTE);

-- 3. RISK METRICS
CREATE VIEW risk_metrics AS
SELECT
    symbol_id,
    window_end,
    calculate_cvar(CAST(price_history AS STRING)) as cvar_95
FROM ohlcv_1m;

-- 4. SINK: Redpanda (metrics_derived)
CREATE TABLE metrics_sink (
    symbol_id STRING,
    window_end TIMESTAMP,
    cvar_95 DOUBLE
) WITH (
    connector = 'kafka',
    topic = 'metrics_derived',
    bootstrap_servers = 'redpanda:9092',
    format = 'json',
    type = 'sink'
);

-- 5. SINK: MinIO (Cold Storage)
CREATE TABLE ohlcv_archive (
    symbol_id STRING,
    window_end TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE
) WITH (
    connector = 'filesystem',
    path = 's3://archive/ohlcv/',
    format = 'json',
    type = 'sink',
    'storage.endpoint' = '{}',
    'storage.region' = 'us-east-1'
);

-- 6. SINK: Redpanda (ohlcv_1m)
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

INSERT INTO metrics_sink
SELECT
    symbol_id,
    window_end,
    cvar_95
FROM risk_metrics;

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

INSERT INTO ohlcv_archive
SELECT
    symbol_id,
    window_end,
    open,
    high,
    low,
    close,
    volume
FROM ohlcv_1m;
"""

# Define UDFs
udfs = [
    {
        "language": "rust",
        "definition": """
            use arroyo_udf_plugin::udf;
            #[udf]
            fn calculate_cvar(json_prices: &str) -> f64 {
                // Manual parsing of JSON array [1.0, 2.0, ...]
                let prices: Vec<f64> = json_prices
                    .trim_matches(|c| c == '[' || c == ']')
                    .split(',')
                    .filter_map(|s| s.trim().parse::<f64>().ok())
                    .collect();
                
                let confidence = 0.95;
                if prices.len() < 2 {
                    return 0.0;
                }
                let mut returns = Vec::with_capacity(prices.len() - 1);
                for i in 1..prices.len() {
                    if prices[i-1] != 0.0 {
                        returns.push((prices[i] - prices[i-1]) / prices[i-1]);
                    }
                }
                
                if returns.is_empty() { return 0.0; }
                
                let mut sorted_returns = returns.clone();
                sorted_returns.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                
                let tail_count = ((1.0 - confidence) * (sorted_returns.len() as f64)).ceil() as usize;
                if tail_count == 0 {
                    return *sorted_returns.first().unwrap_or(&0.0);
                }
                
                let tail = &sorted_returns[0..tail_count];
                if tail.is_empty() { return 0.0; }
                
                tail.iter().sum::<f64>() / (tail.len() as f64)
            }
        """
    }
]

# Construct payload for pipeline creation
payload = {
    "name": "risk_engine_json_v1",
    "parallelism": 1,
    "udfs": udfs,
    "query": compatible_sql
}

try:
    print(f"Submitting to Arroyo API at {ARROYO_URL}...")
    resp = requests.post(f"{ARROYO_URL}/api/v1/pipelines", json=payload)
    print(f"Status: {resp.status_code}")
    print(f"Response: {resp.text}")
    
    if resp.status_code != 200:
        # Try preview endpoint debugging info
        print("\nTrying preview/compile endpoint...")
        preview_payload = {"query": compatible_sql, "udfs": udfs}
        resp = requests.post(f"{ARROYO_URL}/api/v1/pipelines/preview", json=preview_payload)
        print(f"Preview Status: {resp.status_code}") 
        print(f"Preview Response: {resp.text}")
        resp = requests.post(f"{ARROYO_URL}/api/v1/pipelines/preview", json=preview_payload)
        print(f"Preview Status: {resp.status_code}") 
        print(f"Preview Response: {resp.text}")

except Exception as e:
    print(f"Error: {e}")
