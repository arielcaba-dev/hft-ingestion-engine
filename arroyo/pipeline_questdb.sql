-- Configures the pipeline to use the QuestDB UDF
/*
[dependencies]
questdb-rs = "3.0"
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

-- Register UDF for QuestDB Ingestion
CREATE FUNCTION send_to_questdb(
    symbol: STRING,
    price: DOUBLE,
    volume: DOUBLE,
    timestamp: TIMESTAMP
) RETURNS BOOLEAN LANGUAGE RUST AS $$
    use questdb::{
        ingress::{Sender, Buffer, TimestampNanos},
        Result as QResult,
    };
    use std::cell::RefCell;
    use std::time::SystemTime;

    // Use thread_local to maintain a connection per worker thread
    // This avoids re-establishing the TCP connection for every row
    thread_local! {
        static SENDER: RefCell<Option<Sender>> = RefCell::new(None);
    }

    pub async fn send_to_questdb(
        symbol: String,
        price: f64,
        volume: f64,
        timestamp: SystemTime
    ) -> bool {
        let result = SENDER.with(|sender_cell| {
            let mut borrowed_sender = sender_cell.borrow_mut();

            // Initialize connection if not already present
            if borrowed_sender.is_none() {
                // Connect to QuestDB (using Docker service name 'questdb')
                // Note: In production you might want retry logic here
                let sender = Sender::from_conf("tcp::addr=questdb:9009;");
                match sender {
                    Ok(s) => *borrowed_sender = Some(s),
                    Err(e) => {
                        eprintln!("Failed to connect to QuestDB: {}", e);
                        return false;
                    }
                }
            }

            if let Some(sender) = borrowed_sender.as_mut() {
                let mut buffer = Buffer::new();
                
                // Convert SystemTime to nanoseconds since epoch
                let ts_nanos = timestamp
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as i64;

                // Format ILP row
                // Table: trades, Symbol: symbol, Columns: price, volume
                let res = buffer
                    .table("trades")
                    .unwrap()
                    .symbol("symbol", &symbol)
                    .unwrap()
                    .column_f64("price", price)
                    .unwrap()
                    .column_f64("volume", volume)
                    .unwrap()
                    .at(TimestampNanos::new(ts_nanos))
                    .unwrap();
                
                // Flush to QuestDB
                if let Err(e) = sender.flush(&mut buffer) {
                     eprintln!("Failed to flush to QuestDB: {}", e);
                     // If flush fails, invalidate the sender to force reconnect next time
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

-- Main Pipeline Logic
-- Select data from source and apply the UDF side-effect
SELECT 
    symbol_id,
    price,
    quantity,
    time_exchange,
    send_to_questdb(symbol_id, price, quantity, time_exchange) as inserted
FROM market_data_raw;
