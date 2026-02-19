use crate::error::AppError;
use crate::state::AppState;
use axum::{extract::State, response::IntoResponse, Json};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// --- Data Models ---

#[derive(Serialize, Deserialize, Debug, sqlx::FromRow)]
pub struct Exchange {
    pub id: String,
    pub name: String,
    pub is_active: bool,
}

#[derive(Serialize, Deserialize, Debug, sqlx::FromRow)]
pub struct Asset {
    pub id: String,
    pub symbol: String,
    pub name: Option<String>,
    pub decimals: i32,
}

#[derive(Serialize, Deserialize, Debug, sqlx::FromRow)]
pub struct Symbol {
    pub id: String,
    pub exchange_id: String,
    pub base_asset_id: String,
    pub quote_asset_id: String,
    pub symbol: String,
    pub normalized_symbol: String,
    pub price_precision: f64,
    pub size_precision: f64,
    pub min_order_size: f64,
}

// --- Handlers ---

pub async fn get_exchanges(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    let cache_key = "metadata:exchanges";
    
    // 1. Try Cache
    let mut conn = state.redis.get_multiplexed_async_connection().await
        .map_err(|_| AppError::Internal("Redis connection error".into()))?;
        
    let cached: Option<String> = conn.get(cache_key).await.unwrap_or(None);
    if let Some(json_str) = cached {
        let exchanges: Vec<Exchange> = serde_json::from_str(&json_str).unwrap_or_default();
        return Ok(Json(exchanges));
    }

    // 2. Query DB
    let exchanges = sqlx::query_as::<_, Exchange>("SELECT id, name, is_active FROM exchanges WHERE is_active = true")
        .fetch_all(&state.pool)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    // 3. Cache Result (1 Hour)
    let json_str = serde_json::to_string(&exchanges).unwrap_or_default();
    let _: () = conn.set_ex(cache_key, json_str, 3600).await.unwrap_or(());

    Ok(Json(exchanges))
}

pub async fn get_assets(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    let cache_key = "metadata:assets";
    
    // 1. Try Cache
    let mut conn = state.redis.get_multiplexed_async_connection().await
        .map_err(|_| AppError::Internal("Redis connection error".into()))?;
        
    let cached: Option<String> = conn.get(cache_key).await.unwrap_or(None);
    if let Some(json_str) = cached {
        let assets: Vec<Asset> = serde_json::from_str(&json_str).unwrap_or_default();
        return Ok(Json(assets));
    }

    // 2. Query DB
    let assets = sqlx::query_as::<_, Asset>("SELECT id, symbol, name, decimals FROM assets WHERE is_active = true")
        .fetch_all(&state.pool)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    // 3. Cache Result (1 Hour)
    let json_str = serde_json::to_string(&assets).unwrap_or_default();
    let _: () = conn.set_ex(cache_key, json_str, 3600).await.unwrap_or(());

    Ok(Json(assets))
}

pub async fn get_symbols(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    let cache_key = "metadata:symbols";
    
    // 1. Try Cache
    let mut conn = state.redis.get_multiplexed_async_connection().await
        .map_err(|_| AppError::Internal("Redis connection error".into()))?;
        
    let cached: Option<String> = conn.get(cache_key).await.unwrap_or(None);
    if let Some(json_str) = cached {
        let symbols: Vec<Symbol> = serde_json::from_str(&json_str).unwrap_or_default();
        return Ok(Json(symbols));
    }

    // 2. Query DB
    let symbols = sqlx::query_as::<_, Symbol>(
        r#"
        SELECT 
            id, exchange_id, base_asset_id, quote_asset_id, 
            symbol, normalized_symbol, price_precision, size_precision, min_order_size 
        FROM symbols 
        WHERE is_active = true
        "#
    )
    .fetch_all(&state.pool)
    .await
    .map_err(|e| AppError::Internal(e.to_string()))?;

    // 3. Cache Result (1 Hour)
    let json_str = serde_json::to_string(&symbols).unwrap_or_default();
    let _: () = conn.set_ex(cache_key, json_str, 3600).await.unwrap_or(());

    Ok(Json(symbols))
}
