-- 1. OHLCV Aggregation (1 minute)
CREATE MATERIALIZED VIEW ohlcv_1m AS
SELECT
    symbol_id,
    window_start,
    first_value(price ORDER BY time_exchange) as open_price,
    max(price) as high_price,
    min(price) as low_price,
    last_value(price ORDER BY time_exchange) as close_price,
    sum(quantity) as volume
FROM tumble(market_data_raw, time_exchange, INTERVAL '1' MINUTE)
GROUP BY
    symbol_id,
    window_start;

-- 2. VWAP Calculation (Rolling / Sliding Window)
-- Calculating VWAP over the last 1 hour, sliding every 1 minute
-- Or if "Rolling" implies cumulative for the day, we need a different approach.
-- User asked for "vwap_rolling: Calculating VWAP over a sliding window."
-- Let's assume a 1-hour sliding window for this example.

CREATE MATERIALIZED VIEW vwap_rolling AS
SELECT
    symbol_id,
    window_start,
    window_end,
    sum(price * quantity) / sum(quantity) as vwap
FROM hop(market_data_raw, time_exchange, INTERVAL '1' MINUTE, INTERVAL '1' HOUR)
GROUP BY
    symbol_id,
    window_start,
    window_end;
