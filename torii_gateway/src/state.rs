use crate::config::GatewayConfig;
use redis::Client as RedisClient;
use sqlx::PgPool;

#[derive(Clone)]
pub struct AppState {
    pub config: GatewayConfig,
    pub pool: sqlx::PgPool,    // Main Postgres (Users, Keys)
    pub questdb: sqlx::PgPool, // QuestDB (Market Data)
    pub redis: redis::Client,  // Cache & Rate Limit
    pub s3_client: aws_sdk_s3::Client,
}
