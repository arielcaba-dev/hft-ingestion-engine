use crate::config::RedpandaConfig;
use crate::model::NormalizedMarketData;
use async_trait::async_trait;
use kafka::producer::{Producer, Record, RequiredAcks};
use log::info;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[async_trait]
pub trait MessageProducer {
    async fn send(&self, topic: &str, data: &NormalizedMarketData) -> Result<(), String>;
}

pub struct RedpandaProducer {
    // kafka crate producer is not thread safe for sharing across threads easily without mutex?
    // actually it might be, but usually we wrap in Mutex or use one per thread.
    // simpler to wrap in Arc<Mutex<>> for async context.
    producer: Option<Arc<Mutex<Producer>>>,
    config: RedpandaConfig,
}

impl RedpandaProducer {
    pub fn new(config: RedpandaConfig) -> Self {
        Self {
            producer: None,
            config,
        }
    }

    pub async fn initialize(&mut self) -> Result<(), String> {
        let brokers: Vec<String> = self
            .config
            .brokers
            .split(',')
            .map(|s| s.to_string())
            .collect();
        // Blocking initialization
        let brokers_clone = brokers.clone();

        let producer = tokio::task::spawn_blocking(move || {
            Producer::from_hosts(brokers_clone)
                .with_ack_timeout(Duration::from_secs(1))
                .with_required_acks(RequiredAcks::One)
                .create()
        })
        .await
        .map_err(|e| format!("Task join error: {}", e))?
        .map_err(|e| format!("Failed to create Kafka producer: {}", e))?;

        info!(
            "Created Redpanda Producer for brokers: {}",
            self.config.brokers
        );
        self.producer = Some(Arc::new(Mutex::new(producer)));
        Ok(())
    }
}

#[async_trait]
impl MessageProducer for RedpandaProducer {
    async fn send(&self, topic: &str, data: &NormalizedMarketData) -> Result<(), String> {
        let payload = serde_json::to_vec(data).map_err(|e| e.to_string())?;

        if let Some(producer_arc) = &self.producer {
            let producer = producer_arc.clone();
            let topic = topic.to_string();
            let key = data.symbol_id.clone(); // Use symbol_id as key

            // Perform blocking send in a separate thread
            // Note: In high perf HFT, we wouldn't spawn a thread per message.
            // We would use a dedicated thread or existing blocking thread.
            // But RedpandaProducer trait is async here.
            // For now, spawn_blocking is "okay" for MVP validation.

            tokio::task::spawn_blocking(move || {
                let mut locked_producer = producer.lock().unwrap();
                let record =
                    Record::from_key_value(topic.as_str(), key.as_str(), payload.as_slice());
                locked_producer.send(&record)
            })
            .await
            .map_err(|e| format!("Task join error: {}", e))?
            .map_err(|e| format!("Failed to produce: {}", e))?;

            Ok(())
        } else {
            Err("Producer not initialized".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_producer_initialization_failure() {
        let config = RedpandaConfig {
            brokers: "invalid_host:9092".to_string(),
            topic_prefix: "test".to_string(),
        };
        let mut producer = RedpandaProducer::new(config);

        // Should fail to connect to invalid host or at least return error
        // Note: kafka crate might not fail immediately on creation if it doesn't connect,
        // but our initialize() calls .create() which usually checks metadata.
        // If it doesn't fail, we might need to adjust expectation, but let's try.
        let result = producer.initialize().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_send_before_initialization() {
        let config = RedpandaConfig {
            brokers: "localhost:9092".to_string(),
            topic_prefix: "test".to_string(),
        };
        let producer = RedpandaProducer::new(config);

        // Create dummy data
        let data = NormalizedMarketData {
            symbol_id: "BTC-USD".to_string(),
            exchange: "binance".to_string(),
            event_type: crate::model::MarketEventType::Trade,
            price: 100000.0,
            quantity: 0.1,
            time_exchange: chrono::Utc::now(),
            time_ingest: chrono::Utc::now(),
            sequence: 0,
            is_snapshot: false,
        };

        // Should fail because initialize() was never called
        let result = producer.send("test_topic", &data).await;
        assert_eq!(result.err(), Some("Producer not initialized".to_string()));
    }
}
