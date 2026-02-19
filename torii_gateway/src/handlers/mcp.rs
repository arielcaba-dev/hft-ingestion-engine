use crate::billing::deduct_credits;
use crate::error::AppError;
use crate::model::AuthContext;
use crate::state::AppState;
use axum::{extract::State, response::IntoResponse, Json};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use sqlx::Row;

#[derive(Deserialize)]
pub struct McpQuery {
    pub query: String,
    pub context: Option<serde_json::Value>,
}

pub async fn mcp_handler(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
    Json(payload): Json<McpQuery>,
) -> Result<impl IntoResponse, AppError> {
    // 1. Calculate Cost (Hardcoded for now)
    let cost = 10;

    // 2. Deduct Credits
    deduct_credits(&state, auth.user_id, cost)
        .await
        .map_err(|e| match e {
            axum::http::StatusCode::PAYMENT_REQUIRED => AppError::PaymentRequired(format!(
                "Processing this query requires {} credits.",
                cost
            )),
            _ => AppError::Internal("Billing error".into()),
        })?;

    // 3. Process Query
    // Simple Keyword-based Intent Parser
    // Supported: "RSI", "VWAP", "MACD", "Bollinger", "CVaR"
    // Extract Symbol from payload or context

    let query_lower = payload.query.to_lowercase();
    let symbol_context = payload
        .context
        .as_ref()
        .and_then(|c| c.get("symbol"))
        .and_then(|v| v.as_str())
        .unwrap_or("BTC-USD"); // Default

    // Validate Symbol (Should be cleaner in prod)
    if !symbol_context
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-')
    {
        return Err(AppError::Internal("Invalid symbol format".into()));
    }

    let (sql_query, query_type) = if query_lower.contains("risk") || query_lower.contains("volatility") {
        (
            format!(
                "SELECT CAST(timestamp AS BIGINT) as timestamp, symbol, price, quantity FROM trades WHERE symbol = '{}' ORDER BY timestamp DESC LIMIT 100",
                symbol_context
            ),
            "trades"
        )
    } else {
         (
            format!(
                "SELECT CAST(timestamp AS BIGINT) as timestamp, symbol, price, quantity FROM trades WHERE symbol = '{}' ORDER BY timestamp DESC LIMIT 20",
                symbol_context
            ),
            "trades"
        )
    };

    // Execute query
    let rows = sqlx::query(&sql_query)
        .fetch_all(&state.questdb)
        .await
        .map_err(|e| AppError::Internal(format!("QuestDB query error: {}", e)))?;

    // Map rows to JSON based on query type
    let data = if query_type == "trades" {
         rows.into_iter().map(|row| {
             let ts: i64 = row.get("timestamp");
             let dt = chrono::DateTime::from_timestamp_micros(ts).unwrap_or_default();
             json!({
                 "timestamp": dt.to_rfc3339(),
                 "symbol": row.get::<String, _>("symbol"),
                 "price": row.get::<f64, _>("price"),
                 "quantity": row.get::<f64, _>("quantity")
             })
         }).collect::<Vec<_>>()
    } else {
        vec![]
    };

    let response = json!({
        "summary": format!("Executed query: {}", sql_query),
        "data": data,
        "count": data.len(),
        "credits_used": cost
    });

    Ok(Json(response))
}
