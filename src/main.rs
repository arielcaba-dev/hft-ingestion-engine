mod config;
mod connectors;
mod core;
mod model;
mod normalizers;
mod producers;

use crate::config::Settings;
use crate::connectors::ExchangeConnector;
use crate::model::NormalizedMarketData;
use crate::producers::MessageProducer;
use log::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    info!("Starting HFT Ingestion Engine...");

    let settings = Settings::new()?;
    info!("Loaded configuration: {:?}", settings);

    // Initialize components
    let (tx, mut rx) = tokio::sync::mpsc::channel::<NormalizedMarketData>(1024);

    // 1. Start Connector (Producer of raw data)
    // In a real app, this would spawn multiple connectors based on config
    let binance_config = crate::config::ExchangeConfig {
        name: "Binance".to_string(),
        type_code: "binance".to_string(),
        websocket_url: "wss://stream.binance.com:9443/ws".to_string(),
        fix_url: None,
        api_key: None,
        api_secret: None,
    };
    let mut connector =
        crate::connectors::BinanceConnector::new(binance_config, settings.symbols.clone());

    tokio::spawn(async move {
        connector.run(tx).await;
    });

    // 2. Setup RingBuffer (The "Core" of the engine)
    // We use a simplified SPSC RingBuffer here.
    // In a full implementation, we might have an MPSC wrapper or a busy-wait strategy.
    // For this demo, we'll wrap it in an Arc and use a dedicated thread for processing.
    // NOTE: This RingBuffer is SPSC. We are single-threading the consumer of the channel (producer to RingBuffer).
    use std::sync::Arc;
    let ring_buffer = Arc::new(crate::core::ring_buffer::RingBuffer::new(1024));
    let rb_producer = ring_buffer.clone();
    let rb_consumer = ring_buffer.clone();

    // 3. Start Ingestion Thread (Channel -> RingBuffer)
    // This thread ensures we drain the network channel as fast as possible and push to our lock-free buffer.
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            while let Some(data) = rx.recv().await {
                // Busy-wait or backoff if buffer is full
                // Real HFT might overwrite old data or expand buffer
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
    // This is the "Consumer" of the RingBuffer.
    let redpanda_config = settings.redpanda.clone();
    let mut producer = crate::producers::RedpandaProducer::new(redpanda_config);

    // We spin-loop efficiently on the RingBuffer
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            if let Err(e) = producer.initialize().await {
                log::warn!("WARNING: Failed to initialize Redpanda Producer: {}. Continuing in ingestion-only mode (messages will be dropped).", e);
            }
            loop {
                if let Some(data) = rb_consumer.pop() {
                    // Normalize (already mostly done in connector in this simple example, but typically here)
                    // Publish
                    if let Err(e) = producer.send(&settings.redpanda.topic_prefix, &data).await {
                        log::debug!("Failed to send to Redpanda: {}", e);
                    }
                } else {
                    // Backoff strategy (e.g., spin for a bit, then sleep)
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
