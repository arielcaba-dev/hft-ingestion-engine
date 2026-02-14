use crate::error::AppError;
use crate::model::AuthContext;
use crate::AppState;
use axum::{
    body::Body,
    http::{Request, Response},
    middleware::Next,
    response::IntoResponse,
};
use redis::AsyncCommands;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub async fn rate_limit_middleware(
    state: axum::extract::State<Arc<AppState>>,
    auth: AuthContext,
    request: Request<Body>,
    next: Next,
) -> Result<Response, AppError> {
    let mut redis_conn = state.redis.get_multiplexed_async_connection().await?;
    let key = format!("ratelimit:{}:{}", auth.user_id, "global"); // Simple global limit for now
    let limit = auth.rate_limit as isize;
    let window_ms = 1000; // 1 second window

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    // Pipeline: Remove old -> Count -> Add new
    let (count,): (isize,) = redis::pipe()
        .zrembyscore(&key, "-inf", now_ms - window_ms)
        .zcount(&key, now_ms - window_ms, now_ms)
        .query_async(&mut redis_conn)
        .await?;

    if count >= limit {
        return Err(AppError::RateLimitExceeded(1000));
    }

    // Add current request
    let _: () = redis_conn.zadd(&key, now_ms.to_string(), now_ms).await?;

    // Set expiry to avoid stale keys
    let _: () = redis_conn.expire(&key, 5).await?;

    let mut response = next.run(request).await;

    // Add Rate Limit Headers
    response
        .headers_mut()
        .insert("X-RateLimit-Limit", limit.to_string().parse().unwrap());
    response.headers_mut().insert(
        "X-RateLimit-Remaining",
        (limit - count - 1).max(0).to_string().parse().unwrap(),
    );

    Ok(response)
}
