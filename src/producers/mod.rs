use async_trait::async_trait;
use crate::model::NormalizedMarketData;
use crate::config::RedpandaConfig;

#[async_trait]
pub trait MessageProducer {
    async fn send(&self, topic: &str, data: &NormalizedMarketData) -> Result<(), String>;
}

pub struct RedpandaProducer {
    config: RedpandaConfig,
    // In reality, this would hold a rdkafka::producer::FutureProducer
}

impl RedpandaProducer {
    pub fn new(config: RedpandaConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl MessageProducer for RedpandaProducer {
    async fn send(&self, topic: &str, data: &NormalizedMarketData) -> Result<(), String> {
        // Serialize data (e.g., JSON or Bincode)
        let payload = serde_json::to_vec(data).map_err(|e| e.to_string())?;
        
        // Mock sending to Redpanda
        println!("Producing to topic {}: {} bytes", topic, payload.len());
        Ok(())
    }
}
