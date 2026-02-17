use crate::config::GatewayConfig;
use redis::Client as RedisClient;
use sqlx::PgPool;

#[derive(Clone)]
pub struct AppState {
    pub config: GatewayConfig,
    pub pool: PgPool,       // Main Postgres (Users, Keys)
    pub questdb: PgPool,    // QuestDB (Market Data)
    pub redis: RedisClient, // Cache & Rate Limit
}
