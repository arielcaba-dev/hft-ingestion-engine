use config::Config;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Settings {
    pub exchanges: HashMap<String, ExchangeConfig>,
    pub redpanda: RedpandaConfig,
    pub symbols: Vec<SymbolConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ExchangeConfig {
    pub name: String,
    pub type_code: String, // "binance", "coinbase", etc.
    pub websocket_url: String,
    pub fix_url: Option<String>,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RedpandaConfig {
    pub brokers: String,
    pub topic_prefix: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SymbolConfig {
    pub internal_id: String,                        // e.g., "BTC-USD"
    pub exchange_mappings: HashMap<String, String>, // "binance" -> "BTCUSDT", "kraken" -> "XXBTZUSD"
}

impl Settings {
    pub fn new() -> Result<Self, config::ConfigError> {
        let _run_mode = std::env::var("RUN_MODE").unwrap_or_else(|_| "development".into());

        // Use TOML string for defaults to handle nested structures correctly
        let default_config = r#"
            [exchanges.binance]
            name = "Binance"
            type_code = "binance"
            websocket_url = "wss://stream.binance.com:9443/ws"

            [redpanda]
            brokers = "localhost:19092"
            topic_prefix = "market_data_raw"

            [[symbols]]
            internal_id = "BTC-USD"
            [symbols.exchange_mappings]
            binance = "BTCUSDT"

            [[symbols]]
            internal_id = "ETH-USD"
            [symbols.exchange_mappings]
            binance = "ETHUSDT"
        "#;

        let s = Config::builder()
            .add_source(config::File::from_str(
                default_config,
                config::FileFormat::Toml,
            ))
            // Add in settings from the environment (with a prefix of APP)
            // E.g. `APP_REDPANDA__BROKERS=redpanda:9092` overrides `redpanda.brokers`
            .add_source(config::Environment::with_prefix("APP").separator("__"))
            // Manual overrides because config-rs 0.13 has case-sensitivity issues with env vars
            .set_override(
                "redpanda.brokers",
                std::env::var("APP_REDPANDA__BROKERS")
                    .unwrap_or_else(|_| "localhost:19092".to_string()),
            )?
            .set_override(
                "redpanda.topic_prefix",
                std::env::var("APP_REDPANDA__TOPIC_PREFIX")
                    .unwrap_or_else(|_| "market_data_raw".to_string()),
            )?
            .build()?;

        // Deserialize configuration
        s.try_deserialize()
    }
}
