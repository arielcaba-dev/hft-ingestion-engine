use log::{debug, info, warn};
use std::sync::Arc;
use torii_ingestion_engine::config::{ExchangeConfig, Settings};
use torii_ingestion_engine::connectors::{BinanceConnector, ExchangeConnector};
use torii_ingestion_engine::core::ring_buffer::RingBuffer;
use torii_ingestion_engine::model::NormalizedMarketData;
use torii_ingestion_engine::producers::{MessageProducer, RedpandaProducer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    info!("Starting HFT Ingestion Engine...");

    let settings = Settings::new()?;
    info!("Loaded configuration: {:?}", settings);

    // Initialize components
    let (tx, mut rx) = tokio::sync::mpsc::channel::<NormalizedMarketData>(1024);

    // 1. Start Connector (Producer of raw data)
    let binance_config = ExchangeConfig {
        name: "Binance".to_string(),
        type_code: "binance".to_string(),
        websocket_url: "wss://stream.binance.com:9443/ws".to_string(),
        fix_url: None,
        api_key: None,
        api_secret: None,
    };

    // Pass configured symbols to connector
    let mut connector = BinanceConnector::new(binance_config, settings.symbols.clone());

    tokio::spawn(async move {
        connector.run(tx).await;
    });

    // 2. Setup RingBuffer (The "Core" of the engine)
    let ring_buffer = Arc::new(RingBuffer::new(1024));
    let rb_producer = ring_buffer.clone();
    let rb_consumer = ring_buffer.clone();

    // 3. Start Ingestion Thread (Channel -> RingBuffer)
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            while let Some(data) = rx.recv().await {
                loop {
                    match rb_producer.push(data.clone()) {
                        Ok(_) => break,
                        Err(_) => std::thread::yield_now(),
                    }
                }
            }
        });
    });

    // 4. Start Processing Thread (RingBuffer -> Normalizer/Producer)
    let redpanda_config = settings.redpanda.clone();
    let topic_prefix = settings.redpanda.topic_prefix.clone(); // Clone for closure
    let mut producer = RedpandaProducer::new(redpanda_config);

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            if let Err(e) = producer.initialize().await {
                warn!("WARNING: Failed to initialize Redpanda Producer: {}. Continuing in ingestion-only mode (messages will be dropped).", e);
            }
            loop {
                if let Some(data) = rb_consumer.pop() {
                    if let Err(e) = producer.send(&topic_prefix, &data).await {
                        debug!("Failed to send to Redpanda: {}", e);
                    }
                } else {
                    std::thread::yield_now();
                }
            }
        });
    });

    // Keep main alive
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
