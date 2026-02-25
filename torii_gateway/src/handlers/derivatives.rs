use crate::error::AppError;
use crate::model::AuthContext;
use crate::state::AppState;
use axum::{extract::{Path, State}, response::IntoResponse, Json};
use serde_json::json;
use std::sync::Arc;
use sqlx::Row;

/// GET /v1/derivatives/liquidations/:symbol
pub async fn get_liquidations(
    State(state): State<Arc<AppState>>,
    _auth: AuthContext,
    Path(symbol): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    if !symbol.chars().all(|c| c.is_alphanumeric() || c == '-') {
        return Err(AppError::Internal("Invalid symbol format".into()));
    }

    let query = format!(
        "SELECT CAST(timestamp AS BIGINT) as timestamp, symbol, exchange, side, price, quantity \
         FROM liquidations WHERE symbol = '{}' ORDER BY timestamp DESC LIMIT 100",
        symbol
    );

    let rows = sqlx::query(&query)
        .fetch_all(&state.questdb)
        .await
        .map_err(|e| AppError::Internal(format!("QuestDB error: {}", e)))?;

    let data: Vec<serde_json::Value> = rows.into_iter().map(|row| {
        let ts: i64 = row.get("timestamp");
        let dt = chrono::DateTime::from_timestamp_micros(ts).unwrap_or_default();
        json!({
            "timestamp": dt.to_rfc3339(),
            "symbol": row.get::<String, _>("symbol"),
            "exchange": row.get::<String, _>("exchange"),
            "side": row.get::<String, _>("side"),
            "price": row.get::<f64, _>("price"),
            "quantity": row.get::<f64, _>("quantity"),
            "notional": row.get::<f64, _>("price") * row.get::<f64, _>("quantity")
        })
    }).collect();

    Ok(Json(json!({
        "symbol": symbol,
        "count": data.len(),
        "liquidations": data
    })))
}

/// GET /v1/derivatives/oi/:symbol
pub async fn get_open_interest(
    State(state): State<Arc<AppState>>,
    _auth: AuthContext,
    Path(symbol): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    if !symbol.chars().all(|c| c.is_alphanumeric() || c == '-') {
        return Err(AppError::Internal("Invalid symbol format".into()));
    }

    let query = format!(
        "SELECT CAST(timestamp AS BIGINT) as timestamp, symbol, exchange, oi_value, notional_value \
         FROM open_interest WHERE symbol = '{}' ORDER BY timestamp DESC LIMIT 100",
        symbol
    );

    let rows = sqlx::query(&query)
        .fetch_all(&state.questdb)
        .await
        .map_err(|e| AppError::Internal(format!("QuestDB error: {}", e)))?;

    let data: Vec<serde_json::Value> = rows.into_iter().map(|row| {
        let ts: i64 = row.get("timestamp");
        let dt = chrono::DateTime::from_timestamp_micros(ts).unwrap_or_default();
        json!({
            "timestamp": dt.to_rfc3339(),
            "symbol": row.get::<String, _>("symbol"),
            "exchange": row.get::<String, _>("exchange"),
            "oi_value": row.get::<f64, _>("oi_value"),
            "notional_value": row.get::<f64, _>("notional_value")
        })
    }).collect();

    Ok(Json(json!({
        "symbol": symbol,
        "count": data.len(),
        "open_interest": data
    })))
}

/// GET /v1/derivatives/funding/:symbol
pub async fn get_funding_rates(
    State(state): State<Arc<AppState>>,
    _auth: AuthContext,
    Path(symbol): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    if !symbol.chars().all(|c| c.is_alphanumeric() || c == '-') {
        return Err(AppError::Internal("Invalid symbol format".into()));
    }

    let query = format!(
        "SELECT CAST(timestamp AS BIGINT) as timestamp, symbol, exchange, funding_rate, mark_price \
         FROM funding_rates WHERE symbol = '{}' ORDER BY timestamp DESC LIMIT 100",
        symbol
    );

    let rows = sqlx::query(&query)
        .fetch_all(&state.questdb)
        .await
        .map_err(|e| AppError::Internal(format!("QuestDB error: {}", e)))?;

    let data: Vec<serde_json::Value> = rows.into_iter().map(|row| {
        let ts: i64 = row.get("timestamp");
        let dt = chrono::DateTime::from_timestamp_micros(ts).unwrap_or_default();
        json!({
            "timestamp": dt.to_rfc3339(),
            "symbol": row.get::<String, _>("symbol"),
            "exchange": row.get::<String, _>("exchange"),
            "funding_rate": row.get::<f64, _>("funding_rate"),
            "mark_price": row.get::<f64, _>("mark_price")
        })
    }).collect();

    Ok(Json(json!({
        "symbol": symbol,
        "count": data.len(),
        "funding_rates": data
    })))
}
