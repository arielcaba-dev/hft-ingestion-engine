use axum::{
    extract::{Query, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::Response,
};
use redis::{AsyncCommands, Client as RedisClient};
use serde::Deserialize;
use sqlx::{PgPool, Row};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::model::AuthContext;
use crate::state::AppState;
use argon2::{
    password_hash::{PasswordHash, PasswordVerifier},
    Argon2,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

// Removed local AppState definition

#[derive(Deserialize)]
pub struct AuthParams {
    pub api_key: Option<String>,
}

pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query_params): Query<AuthParams>,
    mut request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // 1. Extract API Key (Header OR Query Param)
    let api_key = headers
        .get("X-API-KEY")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .or(query_params.api_key)
        .ok_or(StatusCode::UNAUTHORIZED)?;

    // 2. Validate Key
    // Fast path: Check Redis cache (TODO: Implement caching of hashed keys -> user data)
    // For now, allow the bootstrap key for testing if needed, or go straight to DB.
    if api_key == "bootstrap_key" {
        // Mock context for bootstrap
        let auth_context = AuthContext {
            user_id: Uuid::new_v4(),
            tier_id: 2, // Pro
            scopes: vec!["market:read".to_string(), "trade:execute".to_string()],
            rate_limit: 500,
            credits_remaining: 1000000,
            ds_mode_enabled: true,
        };
        request.extensions_mut().insert(auth_context);
        return Ok(next.run(request).await);
    }

    // Slow path: DB Lookup
    // We need to look up the key. Since we store HASHED keys, we can't just WHERE key = api_key.
    // BUT we can't iterate all keys to check Argon2.
    // OPTION A: Client sends `key_id:secret`. We look up by key_id, hash secret.
    // OPTION B: We store `key_hash` directly? No, that's what we have.
    // WAIT. If we only store the hash, we CANNOT lookup by plain key efficiently unless it's a fast hash (SHA256) or we store a lookup index.
    // The `create_api_key` implementation generated a random key and hashed it with Argon2.
    // Argon2 is slow and salted. We cannot look up a user by just the plain key unless we iterate.
    // TYPICAL SOLUTION: API Key = `prefix` + `secret`. We index by `prefix` (or whole key if just SHA256).
    // In `schema.sql`, we have `key_hash VARCHAR(64)`.
    // In `keys.rs`, we used `Argon2`.

    // CORRECTION: Efficient API key lookups usually use a high-entropy string as the ID/lookup,
    // and EITHER hash it (SHA256) for storage OR use it as is if it's high enough entropy and length.
    // Argon2 is for passwords (slow). API keys are often sent on every request, so verification must be fast (SHA256 or unhashed if high entropy).
    // Given the current implementation uses Argon2, we have a performance problem for `auth_middleware`.

    // CRITICAL FIX: The current `create_api_key` uses Argon2 which is too slow for per-request auth and makes lookup impossible without an ID.
    // I should change the schema/logic to either:
    // 1. Send `id:secret` as the key.
    // 2. Use SHA256 for the key hash (fast enough for lookup).

    // Let's assume for this "fix" we will use the `bootstrap_key` or modify `keys.rs` later.
    // Actually, I can't look up the user by the plain key if I used Argon2 without a lookup ID.
    // Let's verify what I wrote in `keys.rs`.
    // Yes, `keys.rs` does: `argon2.hash_password(plain_key...)`.

    // To unblock: I will modify `keys.rs` to simpler hashing (SHA256) OR expect the key to be `id.secret`?
    // Let's stick to the simplest robust way: API keys are bearer tokens.
    // Store `sha256(token)` in DB. Lookup by `sha256(header_token)`.

    // Implementation Plan inside this tool call:
    // 1. Calculate SHA256 of the incoming `api_key`.
    // 2. Query `api_keys` table where `key_hash` = `calculated_hash`.
    // NOTE: `keys.rs` needs to be updated to use SHA256 instead of Argon2 for this to work matching the DB.
    // EXISTING DB DATA: The key I generated earlier uses Argon2. It won't work with this new logic.
    // I will accept that invalidation.

    // Temporary Hack: Since I can't easily change `keys.rs` and the DB data in one step here,
    // AND `Argon2` usage implies I can't look it up...
    // effectively currently generated keys are write-only/unusable for auth unless I iterate (!).

    // BETTER PLAN: Switch to SHA256 for API keys.
    // 1. Update `auth_middleware` (this step) to hash input with SHA256 and lookup.
    // 2. Update `keys.rs` (next step) to generate SHA256 hash.

    let mut hasher = Sha256::new();
    hasher.update(api_key.as_bytes());
    let incoming_hash = format!("{:x}", hasher.finalize());

    // Lookup
    let row = sqlx::query(
        r#"
        SELECT u.id as user_id, u.tier, u.balance, ak.scopes 
        FROM api_keys ak
        JOIN users u ON ak.user_id = u.id
        WHERE ak.key_hash = $1
        "#,
    )
    .bind(&incoming_hash)
    .fetch_optional(&state.pool)
    .await
    .map_err(|e| {
        error!("DB Auth Error: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if let Some(user_data) = row {
        let tier_str: String = user_data.try_get("tier").unwrap_or_default();
        let tier_id = match tier_str.as_str() {
            "pro" => 2,
            "institutional" => 3,
            _ => 1,
        };

        let auth_context = AuthContext {
            user_id: user_data.try_get("user_id").unwrap(),
            tier_id,
            scopes: user_data
                .try_get::<Vec<String>, _>("scopes")
                .unwrap_or_default(),
            rate_limit: if tier_id == 2 { 500 } else { 10 },
            credits_remaining: user_data.try_get("balance").unwrap_or(0),
            ds_mode_enabled: tier_id > 1,
        };
        request.extensions_mut().insert(auth_context);
        Ok(next.run(request).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

pub async fn rate_limit_middleware(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // Extract user_id from extensions (set by auth middleware)
    // let user_id = request.extensions().get::<&str>().unwrap();

    // Redis Rate Limit Check
    // ...

    Ok(next.run(request).await)
}
