-- Configures the pipeline to use the QuestDB UDF and Risk Indicators
/*
[dependencies]
questdb-rs = "3.0"
statrs = "0.16"
once_cell = "1.18"
tokio = { version = "1", features = ["full"] }
*/

-- Define the Source Table connected to Redpanda
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

-- Register UDFs from external file (conceptually) or inline here
-- 1. QuestDB Sink UDF
CREATE FUNCTION send_to_questdb(
    symbol: STRING,
    table_name: STRING,
    columns: STRING, -- JSON string or specific args? Let's use specific args for type safety or structured approach
    -- For simplicity in this SQL, we might need separate UDFs for different table schemas 
    -- OR a generic one that takes JSON. Let's start with specific ones for safety.
    val1: DOUBLE,
    val2: DOUBLE,
    timestamp: TIMESTAMP
) RETURNS BOOLEAN LANGUAGE RUST AS $$
    // ... (Implementation similar to pipeline_questdb.sql but generic or adapted)
    // For brevity, assuming we use the existing pattern or a specialized sink connector if available.
    // Re-using the pattern from pipeline_questdb.sql for OHLCV
    
    use questdb::{
        ingress::{Sender, Buffer, TimestampNanos},
        Result as QResult,
    };
    use std::cell::RefCell;
    use std::time::SystemTime;

    thread_local! {
        static SENDER: RefCell<Option<Sender>> = RefCell::new(None);
    }

    pub async fn send_to_questdb(
        symbol: String,
        table_name: String,
        val1: f64,
        val2: f64,
        timestamp: SystemTime
    ) -> bool {
        let result = SENDER.with(|sender_cell| {
            let mut borrowed_sender = sender_cell.borrow_mut();

            if borrowed_sender.is_none() {
                let sender = Sender::from_conf("tcp::addr=questdb:9009;");
                if let Ok(s) = sender {
                    *borrowed_sender = Some(s);
                } else {
                    return false;
                }
            }

            if let Some(sender) = borrowed_sender.as_mut() {
                let mut buffer = Buffer::new();
                let ts_nanos = timestamp.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_nanos() as i64;

                // Simple schema mapping for now: 
                // If table is ohlcv_*, val1=close, val2=volume (simplified) -> ACTUALLY we need all OHLCV columns.
                // This generic UDF approach is tricky in SQL without struct support in UDF args easily.
                // Let's rely on specific UDFs or the HTTP sink for production if Arroyo supports it perfectly.
                // Attempting a specific OHLCV sink UDF.
                
                let mut row = buffer.table(table_name.as_str()).unwrap().symbol("symbol", &symbol).unwrap();
                
                // Hack: Using val1/val2 for specific metrics. Better to define specific UDFs.
                // Let's define `send_metric` for risk and `send_ohlcv` 
                
                row.column_f64("value", val1).unwrap().at(TimestampNanos::new(ts_nanos)).unwrap();
                
                if let Err(_) = sender.flush(&mut buffer) {
                     *borrowed_sender = None;
                     return false;
                }
                true
            } else {
                false
            }
        });
        result
    }
$$;

-- 2. Risk Indicator UDFs (Inline from udf_indicators.rs logic)
CREATE FUNCTION calculate_rsi(prices: ARRAY<DOUBLE>) RETURNS DOUBLE LANGUAGE RUST AS $$
    // Logic from udf_indicators.rs
    // ...
    if prices.len() < 15 { return 0.0; }
    // ... RSI calc ...
    50.0 // Stub for syntax check
$$;

CREATE FUNCTION calculate_volatility(prices: ARRAY<DOUBLE>) RETURNS DOUBLE LANGUAGE RUST AS $$
    // Logic from udf_indicators.rs
    // Annualized Volatility
    0.02 // Stub
$$;

CREATE FUNCTION calculate_liquidity(volumes: ARRAY<DOUBLE>, prices: ARRAY<DOUBLE>) RETURNS DOUBLE LANGUAGE RUST AS $$
    // Liquidity Score
    1000.0 // Stub
$$;

CREATE FUNCTION calculate_cvar(prices: ARRAY<DOUBLE>, confidence: DOUBLE) RETURNS DOUBLE LANGUAGE RUST AS $$
    if prices.len() < 2 || confidence <= 0.0 || confidence >= 1.0 {
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
    
    let tail_count = ((1.0 - confidence) * sorted_returns.len() as f64).ceil() as usize;
    if tail_count == 0 {
        return sorted_returns.first().copied().unwrap_or(0.0);
    }
    
    let tail = &sorted_returns[0..tail_count];
    if tail.is_empty() { return 0.0; }
    
    tail.iter().sum::<f64>() / tail.len() as f64
$$;


-- =================================================================================
-- PIPELINE LOGIC
-- =================================================================================

-- 1. CLEAN & WINDOW (1 Minute)
-- Calculate OHLCV
CREATE VIEW ohlcv_1m AS
SELECT
    symbol_id,
    tumble_end(time_exchange, INTERVAL '1' MINUTE) as window_end,
    earliest(price) as open,
    max(price) as high,
    min(price) as low,
    latest(price) as close,
    sum(quantity) as volume,
    ARRAY_AGG(price) as price_history, -- For Volatility (needs optimization for large windows)
    ARRAY_AGG(quantity) as vol_history   -- For Liquidity
FROM market_data_raw
GROUP BY
    symbol_id,
    tumble(time_exchange, INTERVAL '1' MINUTE);

-- 2. CALCULATE METRICS
CREATE VIEW risk_metrics AS
SELECT
    symbol_id,
    window_end,
    calculate_volatility(price_history) as realized_volatility,
    calculate_liquidity(vol_history, price_history) as liquidity_score,
    calculate_rsi(price_history) as rsi_14,
    calculate_cvar(price_history, 0.95) as cvar_95
FROM ohlcv_1m;

-- 3. SINK TO QUESTDB (METRICS)
-- Using a specialized sink definition (Kafka -> Python Bridge -> QuestDB) is often standard in this project 
-- given the `questdb-bridge` service.
-- OR use the UDF sink method.
-- Let's output to Redpanda topics `metrics_risk` and `ohlcv_1m` which the Python bridge or a new consumer can ingest.
-- This decouples Arroyo (Stateless/Windowed) from DB Insert Logic.

CREATE TABLE metrics_sink (
    symbol_id STRING,
    window_end TIMESTAMP,
    volatility DOUBLE,
    liquidity DOUBLE,
    rsi DOUBLE,
    cvar_95 DOUBLE
) WITH (
    connector = 'kafka',
    topic = 'metrics_derived',
    bootstrap_servers = 'redpanda:9092',
    format = 'json'
);

INSERT INTO metrics_sink
SELECT
    symbol_id,
    window_end,
    realized_volatility,
    liquidity_score,
    rsi_14,
    cvar_95
FROM risk_metrics;

-- 4. OHLCV SINK
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
    format = 'json'
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

-- 5. COLD STORAGE SINK (Parquet/S3)
-- Using Delta/Parquet connector
CREATE TABLE archival_sink (
    symbol_id STRING,
    price DOUBLE,
    quantity DOUBLE,
    time_exchange TIMESTAMP
) WITH (
   connector = 'delta',
   path = 's3://torii-lake/raw/',
   parquet_compression = 'snappy'
);

INSERT INTO archival_sink
SELECT symbol_id, price, quantity, time_exchange
FROM market_data_raw;
