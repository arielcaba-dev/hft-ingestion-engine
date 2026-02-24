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

    let response = if query_lower.contains("squeeze") || query_lower.contains("leverage") || (query_lower.contains("liquidat") && query_lower.contains("open interest")) {
        // --- Squeeze Analysis (Derivatives) ---
        let liq_query = format!(
            "SELECT CAST(timestamp AS BIGINT) as timestamp, side, price, quantity FROM liquidations WHERE symbol = '{}' ORDER BY timestamp DESC LIMIT 100",
            symbol_context
        );
        let oi_query = format!(
            "SELECT CAST(timestamp AS BIGINT) as timestamp, oi_value, notional_value FROM open_interest WHERE symbol = '{}' ORDER BY timestamp DESC LIMIT 50",
            symbol_context
        );
        let price_query = format!(
            "SELECT CAST(timestamp AS BIGINT) as timestamp, price FROM trades WHERE symbol = '{}' ORDER BY timestamp DESC LIMIT 50",
            symbol_context
        );

        let liqs = sqlx::query(&liq_query).fetch_all(&state.questdb).await.map_err(|e| AppError::Internal(e.to_string()))?;
        let ois = sqlx::query(&oi_query).fetch_all(&state.questdb).await.map_err(|e| AppError::Internal(e.to_string()))?;
        let prices = sqlx::query(&price_query).fetch_all(&state.questdb).await.map_err(|e| AppError::Internal(e.to_string()))?;

        // Aggregate liquidation stats
        let mut long_liq_total = 0.0_f64;
        let mut short_liq_total = 0.0_f64;
        let liq_data: Vec<serde_json::Value> = liqs.into_iter().map(|row| {
            let ts: i64 = row.get("timestamp");
            let dt = chrono::DateTime::from_timestamp_micros(ts).unwrap_or_default();
            let side: String = row.get("side");
            let price: f64 = row.get("price");
            let qty: f64 = row.get("quantity");
            let notional = price * qty;
            if side == "Long" { long_liq_total += notional; } else { short_liq_total += notional; }
            json!({ "timestamp": dt.to_rfc3339(), "side": side, "price": price, "quantity": qty, "notional": notional })
        }).collect();

        let oi_data: Vec<serde_json::Value> = ois.into_iter().map(|row| {
            let ts: i64 = row.get("timestamp");
            let dt = chrono::DateTime::from_timestamp_micros(ts).unwrap_or_default();
            json!({ "timestamp": dt.to_rfc3339(), "oi_value": row.get::<f64, _>("oi_value"), "notional": row.get::<f64, _>("notional_value") })
        }).collect();

        let price_data: Vec<serde_json::Value> = prices.into_iter().map(|row| {
            let ts: i64 = row.get("timestamp");
            let dt = chrono::DateTime::from_timestamp_micros(ts).unwrap_or_default();
            json!({ "timestamp": dt.to_rfc3339(), "price": row.get::<f64, _>("price") })
        }).collect();

        // Simple squeeze signal
        let squeeze_signal = if short_liq_total > long_liq_total * 2.0 {
            "HIGH: Aggressive short liquidations detected. Possible short squeeze forming."
        } else if long_liq_total > short_liq_total * 2.0 {
            "HIGH: Aggressive long liquidations detected. Possible long squeeze / capitulation."
        } else {
            "LOW: Liquidation balance is neutral. No squeeze signal."
        };

        json!({
            "type": "squeeze_analysis",
            "symbol": symbol_context,
            "squeeze_signal": squeeze_signal,
            "long_liquidation_notional": long_liq_total,
            "short_liquidation_notional": short_liq_total,
            "liquidations": liq_data,
            "open_interest": oi_data,
            "price_data": price_data,
            "credits_used": cost
        })
    } else if query_lower.contains("liquidat") {
        // --- Liquidation-only query ---
        let liq_query = format!(
            "SELECT CAST(timestamp AS BIGINT) as timestamp, side, price, quantity FROM liquidations WHERE symbol = '{}' ORDER BY timestamp DESC LIMIT 50",
            symbol_context
        );
        let rows = sqlx::query(&liq_query).fetch_all(&state.questdb).await.map_err(|e| AppError::Internal(e.to_string()))?;
        let data: Vec<serde_json::Value> = rows.into_iter().map(|row| {
            let ts: i64 = row.get("timestamp");
            let dt = chrono::DateTime::from_timestamp_micros(ts).unwrap_or_default();
            json!({ "timestamp": dt.to_rfc3339(), "side": row.get::<String, _>("side"), "price": row.get::<f64, _>("price"), "quantity": row.get::<f64, _>("quantity") })
        }).collect();
        json!({ "type": "liquidations", "symbol": symbol_context, "data": data, "credits_used": cost })
    } else if query_lower.contains("open interest") || query_lower.contains(" oi") || query_lower.contains("oi ") {
        // --- OI-only query ---
        let oi_query = format!(
            "SELECT CAST(timestamp AS BIGINT) as timestamp, oi_value, notional_value FROM open_interest WHERE symbol = '{}' ORDER BY timestamp DESC LIMIT 50",
            symbol_context
        );
        let rows = sqlx::query(&oi_query).fetch_all(&state.questdb).await.map_err(|e| AppError::Internal(e.to_string()))?;
        let data: Vec<serde_json::Value> = rows.into_iter().map(|row| {
            let ts: i64 = row.get("timestamp");
            let dt = chrono::DateTime::from_timestamp_micros(ts).unwrap_or_default();
            json!({ "timestamp": dt.to_rfc3339(), "oi_value": row.get::<f64, _>("oi_value"), "notional": row.get::<f64, _>("notional_value") })
        }).collect();
        json!({ "type": "open_interest", "symbol": symbol_context, "data": data, "credits_used": cost })
    } else if query_lower.contains("correlat") || query_lower.contains("impact") || (query_lower.contains("risk") && query_lower.contains("sentiment")) {
        // --- Correlation Logic ---
        let trades_query = format!(
            "SELECT CAST(timestamp AS BIGINT) as timestamp, symbol, price FROM trades WHERE symbol = '{}' ORDER BY timestamp DESC LIMIT 50",
            symbol_context
        );
        let sentiment_query = format!(
            "SELECT CAST(timestamp AS BIGINT) as timestamp, symbol, sentiment_score, impact_score FROM sentiment WHERE symbol = '{}' ORDER BY timestamp DESC LIMIT 50",
            symbol_context
        );

        let trades = sqlx::query(&trades_query).fetch_all(&state.questdb).await.map_err(|e| AppError::Internal(e.to_string()))?;
        let sentiment = sqlx::query(&sentiment_query).fetch_all(&state.questdb).await.map_err(|e| AppError::Internal(e.to_string()))?;

        let trades_data = trades.into_iter().map(|row| {
             let ts: i64 = row.get("timestamp");
             let dt = chrono::DateTime::from_timestamp_micros(ts).unwrap_or_default();
             json!({
                 "timestamp": dt.to_rfc3339(),
                 "price": row.get::<f64, _>("price")
             })
         }).collect::<Vec<_>>();

        let sentiment_data = sentiment.into_iter().map(|row| {
             let ts: i64 = row.get("timestamp");
             let dt = chrono::DateTime::from_timestamp_micros(ts).unwrap_or_default();
             json!({
                 "timestamp": dt.to_rfc3339(),
                 "score": row.get::<f64, _>("sentiment_score"),
                 "impact": row.get::<f64, _>("impact_score")
             })
         }).collect::<Vec<_>>();

        json!({
            "type": "correlation",
            "symbol": symbol_context,
            "market_data": trades_data,
            "sentiment_data": sentiment_data,
            "analysis": "Align timestamps to visualize impact of sentiment spikes on price volatility.",
            "credits_used": cost
        })
    } else {
        // --- Standard Logic ---
        let (sql_query, query_type) = if query_lower.contains("risk") || query_lower.contains("volatility") {
            (
                format!(
                    "SELECT CAST(timestamp AS BIGINT) as timestamp, symbol, price, quantity FROM trades WHERE symbol = '{}' ORDER BY timestamp DESC LIMIT 100",
                    symbol_context
                ),
                "trades"
            )
        } else if query_lower.contains("sentiment") || query_lower.contains("news") || query_lower.contains("social") {
            (
                format!(
                    "SELECT CAST(timestamp AS BIGINT) as timestamp, symbol, source, sentiment_score, impact_score FROM sentiment WHERE symbol = '{}' ORDER BY timestamp DESC LIMIT 50",
                    symbol_context
                ),
                "sentiment"
            )
        } else {
            // Default to Trades/Risk
             (
                format!(
                    "SELECT CAST(timestamp AS BIGINT) as timestamp, symbol, price, quantity FROM trades WHERE symbol = '{}' ORDER BY timestamp DESC LIMIT 20",
                    symbol_context
                ),
                "trades"
            )
        };

        let rows = sqlx::query(&sql_query)
            .fetch_all(&state.questdb)
            .await
            .map_err(|e| AppError::Internal(format!("QuestDB query error: {}", e)))?;

        let data = if query_type == "sentiment" {
            rows.into_iter().map(|row| {
                 let ts: i64 = row.get("timestamp");
                 let dt = chrono::DateTime::from_timestamp_micros(ts).unwrap_or_default();
                 json!({
                     "timestamp": dt.to_rfc3339(),
                     "symbol": row.get::<String, _>("symbol"),
                     "source": row.get::<String, _>("source"),
                     "sentiment_score": row.get::<f64, _>("sentiment_score"),
                     "impact_score": row.get::<f64, _>("impact_score")
                 })
             }).collect::<Vec<_>>()
        } else {
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
        };

        json!({
            "summary": format!("Executed query: {}", sql_query),
            "data": data,
            "count": data.len(),
            "credits_used": cost
        })
    };

    Ok(Json(response))
}
