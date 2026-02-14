use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct NormalizedMarketData {
    pub symbol_id: String,
    pub exchange: String,
    pub event_type: MarketEventType,
    pub price: f64,
    pub quantity: f64,
    pub time_exchange: DateTime<Utc>,
    pub time_ingest: DateTime<Utc>,
    // Optional fields for order book updates
    pub is_snapshot: bool,
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MarketEventType {
    Trade,
    Quote, // Best Bid/Ask
    L2Update,
}
