mod billing;
mod config;
mod error;
mod handlers;
mod middleware;
mod model;
mod state; // New state module
mod ws;

use crate::state::AppState;
use axum::{
    middleware::from_fn_with_state,
    routing::{delete, get, post},
    Router,
};
use config::GatewayConfig;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

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
    // Using connect_lazy to avoid hanging if QuestDB is not ready immediately
    let questdb_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_lazy(&config.questdb_pg_url)
        .expect("Failed to create QuestDB pool");

    // Connect to Redis
    info!("Connecting to Redis at {}", config.redis_url);
    let redis_client = redis::Client::open(config.redis_url.clone()).map_err(|e| {
        error!("Invalid Redis URL: {}", e);
        e
    })?;

    // Allow connection to verify
    let _conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| {
            error!("Failed to connect to Redis: {}", e);
            e
        })?;
    info!("Redis connected successfully");

    // Initialize S3 Client
    info!("Initializing S3 client for {}", config.s3_endpoint);
    let s3_config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(config.s3_endpoint.clone())
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            &config.s3_access_key,
            &config.s3_secret_key,
            None,
            None,
            "static",
        ))
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&s3_config_loader)
        .force_path_style(true)
        .build();

    let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

    let state = Arc::new(AppState {
        config: config.clone(),
        pool: pool,
        questdb: questdb_pool,
        redis: redis_client,
        s3_client: s3_client,
    });

    // Start Billing Task
    let billing_state = state.clone();
    tokio::spawn(async move {
        billing::start_billing_sync(billing_state).await;
    });

    // Build Router
    info!("Building router...");
    let app = Router::new()
        .route("/health", get(handlers::health::health_check))
        .route("/v1/mcp", post(handlers::mcp::mcp_handler))
        .route("/v1/ws", get(ws::handler::ws_handler))
        .route("/v1/ws/ds", get(ws::ds_mode::ds_handler))
        .route(
            "/v1/trades/historical",
            get(handlers::historical::historical_handler),
        )
        .route(
            "/v1/market/health",
            get(handlers::market::get_market_health),
        )
        .route(
            "/v1/market/recent-trades",
            get(handlers::market::get_recent_trades),
        )
        .route("/v1/keys", post(handlers::keys::create_api_key))
        .route("/v1/keys/:id", delete(handlers::keys::revoke_api_key))
        .layer(from_fn_with_state(
            state.clone(),
            middleware::rate_limit_middleware,
        ))
        .layer(from_fn_with_state(
            state.clone(),
            middleware::auth_middleware,
        ))
        .with_state(state);

    // Start Server
    let addr = format!("0.0.0.0:{}", config.server_port);
    info!("Binding to address: {}", addr);
    let listener = TcpListener::bind(&addr).await.map_err(|e| {
        error!("Failed to bind to address {}: {}", addr, e);
        e
    })?;

    info!("Axum server starting...");
    axum::serve(listener, app).await.map_err(|e| {
        error!("Server error: {}", e);
        e
    })?;

    Ok(())
}
