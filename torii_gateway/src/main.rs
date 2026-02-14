mod billing;
mod config;
mod error;
mod handlers;
mod middleware;
mod model;
mod ws;

use axum::{
    middleware::from_fn_with_state,
    routing::{get, post},
    Router,
};
use config::GatewayConfig;
use log::info;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tokio::net::TcpListener;

pub struct AppState {
    pub config: GatewayConfig,
    pub db: sqlx::PgPool,
    pub questdb: sqlx::PgPool,
    pub redis: redis::Client,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Load configuration
    let config = GatewayConfig::new()?;
    info!("Starting Gateway Service on port {}", config.server_port);

    // Connect to Postgres
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await
        .expect("Failed to connect to Postgres");

    // Run migrations
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    // Connect to QuestDB (PG Wire)
    let questdb_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.questdb_pg_url)
        .await
        .expect("Failed to connect to QuestDB");

    // Connect to Redis
    let redis_client = redis::Client::open(config.redis_url.clone())?;

    let state = Arc::new(AppState {
        config: config.clone(),
        db: pool,
        questdb: questdb_pool,
        redis: redis_client,
    });

    // Build Router
    let app = Router::new()
        .route("/health", get(handlers::health::health_check))
        .route("/v1/mcp", post(handlers::mcp::mcp_handler))
        .route("/v1/ws", get(ws::handler::ws_handler))
        .route("/v1/ws/ds", get(ws::ds_mode::ds_handler))
        .route(
            "/v1/trades/historical",
            get(handlers::historical::historical_handler),
        )
        .layer(from_fn_with_state(
            state.clone(),
            middleware::rate_limit::rate_limit_middleware,
        ))
        .with_state(state);

    // Start Server
    let listener = TcpListener::bind(format!("0.0.0.0:{}", config.server_port)).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
