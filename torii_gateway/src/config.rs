use config::{Config, ConfigError, Environment};
use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize, Clone)]
pub struct GatewayConfig {
    pub server_port: u16,
    pub database_url: String,
    pub redis_url: String,
    #[allow(dead_code)]
    pub s3_endpoint: String,
    pub public_s3_endpoint: String,
    #[allow(dead_code)]
    pub s3_bucket: String,
    #[allow(dead_code)]
    pub s3_access_key: String,
    #[allow(dead_code)]
    pub s3_secret_key: String,
    #[allow(dead_code)]
    pub questdb_url: String,
    pub questdb_pg_url: String,
    pub redpanda_brokers: String,
    #[allow(dead_code)]
    pub jwt_secret: String,
}

impl GatewayConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let _run_mode = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());

        let s = Config::builder()
            // Start with default values
            .set_default("server_port", 8080)?
            .set_default("public_s3_endpoint", "http://localhost:9090")?
            .set_default("jwt_secret", "secret")?
            // Load from environment variables
            .add_source(Environment::default())
            .build()?;

        s.try_deserialize()
    }
}
