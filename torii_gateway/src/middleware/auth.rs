use crate::error::AppError;
use crate::model::AuthContext;
use crate::AppState;
use axum::{
    async_trait,
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
    RequestPartsExt,
};
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;

#[async_trait]
impl FromRequestParts<Arc<AppState>> for AuthContext {
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        // 1. Extract API Key (Header: X-API-KEY)
        let api_key = parts
            .headers
            .get("X-API-KEY")
            .ok_or_else(|| AppError::Unauthorized("Missing X-API-KEY header".into()))?
            .to_str()
            .map_err(|_| AppError::Unauthorized("Invalid API Key format".into()))?;

        // 2. Hash Key
        let mut hasher = Sha256::new();
        hasher.update(api_key.as_bytes());
        let key_hash = hex::encode(hasher.finalize());

        // 3. Check Redis Cache
        let redis_key = format!("apikey:{}", key_hash);
        let mut redis_conn = state.redis.get_multiplexed_async_connection().await?;

        if let Ok(cached) = redis_conn.get::<_, String>(&redis_key).await {
            if let Ok(ctx) = serde_json::from_str::<AuthContext>(&cached) {
                return Ok(ctx);
            }
        }

        // 4. DB Lookup (if cache miss)
        // using sqlx::query() instead of macro to avoid compile-time DB requirement without offline mode
        let row = sqlx::query(
            r#"
            SELECT 
                u.id as user_id, 
                ak.scopes, 
                us.tier_id, 
                st.rate_limit_per_second,
                us.credits_remaining,
                st.ds_mode_enabled
            FROM api_keys ak
            JOIN users u ON ak.user_id = u.id
            JOIN user_subscriptions us ON u.id = us.user_id
            JOIN subscription_tiers st ON us.tier_id = st.id
            WHERE ak.key_hash = $1 AND ak.is_active = TRUE
            "#,
        )
        .bind(key_hash)
        .fetch_optional(&state.db)
        .await
        .map_err(AppError::Database)?;

        let row =
            row.ok_or_else(|| AppError::Unauthorized("Invalid or inactive API Key".into()))?;

        // Manual mapping from Row
        use sqlx::Row;
        let user_id: Uuid = row.try_get("user_id").map_err(AppError::Database)?;
        let scopes: Option<Vec<String>> = row.try_get("scopes").map_err(AppError::Database)?;
        let tier_id: Option<i32> = row.try_get("tier_id").map_err(AppError::Database)?;
        let rate_limit: i32 = row
            .try_get("rate_limit_per_second")
            .map_err(AppError::Database)?;
        let credits_remaining: Option<i32> = row
            .try_get("credits_remaining")
            .map_err(AppError::Database)?;
        let ds_mode_enabled: Option<bool> =
            row.try_get("ds_mode_enabled").map_err(AppError::Database)?;

        let ctx = AuthContext {
            user_id,
            tier_id: tier_id.unwrap_or(1),
            scopes: scopes.unwrap_or_default(),
            rate_limit,
            credits_remaining: credits_remaining.unwrap_or(0),
            ds_mode_enabled: ds_mode_enabled.unwrap_or(false),
        };

        // 5. Cache result (TTL 5 mins)
        let serialized = serde_json::to_string(&ctx).unwrap();
        let _: () = redis_conn.set_ex(&redis_key, serialized, 300).await?;

        Ok(ctx)
    }
}
