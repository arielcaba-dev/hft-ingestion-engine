use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize, Clone)]
pub struct GatewayConfig {
    pub server_port: u16,
    pub database_url: String,
    pub redis_url: String,
    pub s3_endpoint: String,
    pub s3_bucket: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub questdb_url: String,
    pub questdb_pg_url: String,
    pub redpanda_brokers: String,
    pub jwt_secret: String,
}

impl GatewayConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let run_mode = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());

        let s = Config::builder()
            // Start with default values
            .set_default("server_port", 8080)?
            .set_default("jwt_secret", "secret")?
            // Load from environment variables
            .add_source(Environment::default())
            .build()?;

        s.try_deserialize()
    }
}
