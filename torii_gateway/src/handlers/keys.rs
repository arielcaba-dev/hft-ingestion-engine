use crate::state::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Serialize)]
pub struct ApiKeyResponse {
    key: String, // The plain key, show only once!
    id: Uuid,
}

#[derive(Deserialize)]
pub struct CreateKeyRequest {
    pub user_id: Uuid,
    pub scopes: Vec<String>,
}

pub async fn create_api_key(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateKeyRequest>,
) -> Result<Json<ApiKeyResponse>, StatusCode> {
    // 1. Generate random key
    let plain_key =
        Uuid::new_v4().to_string().replace("-", "") + &Uuid::new_v4().to_string().replace("-", "");

    // 2. Hash it (SHA256 for fast lookup)
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(plain_key.as_bytes());
    let key_hash = format!("{:x}", hasher.finalize());

    // 3. Store in DB
    let key_prefix = &plain_key[..8];
    let row = sqlx::query(
        "INSERT INTO api_keys (user_id, key_hash, key_prefix, scopes) VALUES ($1, $2, $3, $4) RETURNING id",
    )
    .bind(payload.user_id)
    .bind(key_hash)
    .bind(key_prefix)
    .bind(&payload.scopes)
    .fetch_one(&state.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to create key: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let key_id: Uuid = row
        .try_get("id")
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(ApiKeyResponse {
        key: plain_key,
        id: key_id,
    }))
}

pub async fn revoke_api_key(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    sqlx::query("DELETE FROM api_keys WHERE id = $1")
        .bind(id)
        .execute(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::NO_CONTENT)
}
