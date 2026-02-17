use crate::error::AppError;
use crate::state::AppState;
use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
pub struct MarketHealth {
    pub volume_1h: i64,
    pub latency_ms: f64,
    pub active_symbols: i64,
    pub total_trades: i64,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct Trade {
    pub timestamp: chrono::NaiveDateTime,
    pub symbol: String,
    pub price: f64,
    pub quantity: f64,
}

pub async fn get_market_health(
    State(state): State<Arc<AppState>>,
) -> Result<Json<MarketHealth>, AppError> {
    // 1. Volume 1h
    let volume_row = sqlx::query(
        "SELECT count() as count FROM trades WHERE timestamp > dateadd('h', -1, now())",
    )
    .fetch_one(&state.questdb)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch volume: {}", e);
        AppError::Database(e)
    })?;
    let volume_1h: i64 = volume_row.try_get("count").unwrap_or(0);

    // 2. Latency
    let latency_row = sqlx::query("SELECT (now() - max(timestamp)) / 1000.0 as lag_us FROM trades")
        .fetch_one(&state.questdb)
        .await
        .map_err(|e| {
            tracing::error!("Failed to fetch latency: {}", e);
            AppError::Database(e)
        })?;
    // Result is in microseconds (QuestDB timestamp diff), convert to ms
    let latency_ms: f64 = latency_row.try_get::<f64, _>("lag_us").unwrap_or(0.0) / 1000.0;

    // 3. Active Symbols
    let symbols_row = sqlx::query("SELECT count(distinct symbol) as count FROM trades WHERE timestamp > dateadd('h', -1, now())")
        .fetch_one(&state.questdb)
        .await
        .map_err(|e| {
            tracing::error!("Failed to fetch symbols: {}", e);
            AppError::Database(e)
        })?;
    let active_symbols: i64 = symbols_row.try_get("count").unwrap_or(0);

    // 4. Total Trades
    let total_row = sqlx::query("SELECT count() as count FROM trades")
        .fetch_one(&state.questdb)
        .await
        .map_err(|e| {
            tracing::error!("Failed to fetch total trades: {}", e);
            AppError::Database(e)
        })?;
    let total_trades: i64 = total_row.try_get("count").unwrap_or(0);

    Ok(Json(MarketHealth {
        volume_1h,
        latency_ms,
        active_symbols,
        total_trades,
    }))
}

pub async fn get_recent_trades(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<Trade>>, AppError> {
    let trades = sqlx::query_as::<_, Trade>(
        "SELECT timestamp, symbol, price, quantity FROM trades ORDER BY timestamp DESC LIMIT 50",
    )
    .fetch_all(&state.questdb)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch recent trades: {}", e);
        AppError::Database(e)
    })?;

    Ok(Json(trades))
}
