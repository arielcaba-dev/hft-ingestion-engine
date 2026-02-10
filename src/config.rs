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
        let run_mode = std::env::var("RUN_MODE").unwrap_or_else(|_| "development".into());

        let s = Config::builder()
            // Start with a default configuration (could also be a file)
            // .add_source(File::with_name("config/default"))
            // Add in settings from the environment (with a prefix of APP)
            // E.g. `APP_DEBUG=1` would set the `debug` key
            .add_source(config::Environment::with_prefix("APP"))
            .build()?;

        // For now, since we don't have a config file in the environment, we might fail here if we rely solely on file.
        // I will return a mock config or expect a file if one is provided.
        // But to make it runnable without external config file for now, I'll use a hardcoded default or builder pattern if needed.
        // However, standard pattern is to load from file. let's assume a config.toml exists or use defaults.

        // Constructing a default config for demonstration purposes
        let default_settings = Settings {
            exchanges: HashMap::new(),
            redpanda: RedpandaConfig {
                brokers: "localhost:9092".to_string(),
                topic_prefix: "market_data".to_string(),
            },
            symbols: vec![],
        };

        Ok(default_settings)
    }
}
