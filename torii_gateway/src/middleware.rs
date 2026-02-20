use axum::{
    extract::{Query, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::Response,
};
use serde::Deserialize;
use sqlx::Row;
use std::sync::Arc;
use tracing::error;

use crate::model::AuthContext;
use crate::state::AppState;
use sha2::{Digest, Sha256};
use tracing::info;
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
            user_id: Uuid::nil(), // Use Nil UUID for bootstrap super-user
            tier_id: 3,           // Enterprise
            scopes: vec!["market:read".to_string(), "trade:execute".to_string()],
            rate_limit: 10000,
            credits_remaining: 99999999,
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
        SELECT 
            ak.user_id, 
            us.tier_id, 
            us.credits_remaining, 
            ak.scopes, 
            st.rate_limit_per_second as rate_limit,
            st.ds_mode_enabled
        FROM api_keys ak
        JOIN user_subscriptions us ON ak.user_id = us.user_id
        JOIN subscription_tiers st ON us.tier_id = st.id
        WHERE ak.key_hash = $1 AND ak.is_active = true
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
        let auth_context = AuthContext {
            user_id: user_data.try_get("user_id").unwrap(),
            tier_id: user_data.try_get("tier_id").unwrap(),
            scopes: user_data
                .try_get::<Vec<String>, _>("scopes")
                .unwrap_or_default(),
            rate_limit: user_data.try_get::<i32, _>("rate_limit").unwrap_or(10),
            credits_remaining: user_data
                .try_get::<i32, _>("credits_remaining")
                .unwrap_or(0),
            ds_mode_enabled: user_data.try_get("ds_mode_enabled").unwrap_or(false),
        };
        request.extensions_mut().insert(auth_context);
        Ok(next.run(request).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

use axum::Extension;

pub async fn rate_limit_middleware(
    State(state): State<Arc<AppState>>,
    Extension(auth_context): Extension<AuthContext>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let mut conn = state
        .redis
        .get_multiplexed_async_connection()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let user_id = auth_context.user_id;

    // Bypass for super-user (bootstrap)
    // Bypass for super-user (bootstrap)
    if user_id.is_nil() {
        let response = next.run(request).await;
        let status_code = response.status().as_u16().to_string();
        metrics::increment_counter!("http_requests_total", "status" => status_code);
        return Ok(response);
    }

    let limit = auth_context.rate_limit as u64;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as f64;
    let window_start = now - 1_000_000.0; // 1 second in microseconds

    let key = format!("rate_limit:{}", user_id);

    // 1. Sliding Window Logic
    use redis::AsyncCommands;

    // Remove old requests
    let _: () = conn
        .zrembyscore(&key, "-inf", window_start)
        .await
        .unwrap_or(());

    // Count current requests
    let current_count: u64 = conn.zcard(&key).await.unwrap_or(0);

    if current_count >= limit {
        metrics::increment_counter!("http_requests_total", "status" => "429");
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    // 2. Billing logic (Simplified: 1 credit per request for now)
    // We can make this more complex later if needed.
    if let Err(status) = crate::billing::deduct_credits(&state, user_id, 1).await {
        return Err(status);
    }

    // Add current request
    let _: () = conn.zadd(&key, now, now).await.unwrap_or(());

    // 3. Run next
    let mut response = next.run(request).await;

    // 4. Add Headers
    let headers = response.headers_mut();
    headers.insert("X-RateLimit-Limit", limit.into());
    headers.insert("X-RateLimit-Remaining", (limit - current_count - 1).into());

    // Metrics
    let status_code = response.status().as_u16().to_string();
    metrics::increment_counter!("http_requests_total", "status" => status_code);

    Ok(response)
}
