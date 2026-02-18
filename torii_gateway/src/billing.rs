use crate::state::AppState;
use axum::http::StatusCode;
use redis::AsyncCommands;
use std::sync::Arc;
use tokio::time::Duration;

pub async fn start_billing_sync(state: Arc<AppState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));

    loop {
        interval.tick().await;

        if let Err(e) = sync_credits(&state).await {
            tracing::error!("Billing sync failed: {:?}", e);
        }
    }
}

async fn sync_credits(state: &AppState) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Scan for all user credit keys in Redis `credits:*`
    // In a real high-scale app, we might use a set to track active users.
    // For now, let's assume we have a set `active_users` in Redis that we add to on every request.

    let mut conn = state.redis.get_multiplexed_async_connection().await?;

    let active_users: Vec<String> = conn.smembers("active_billing_users").await?;

    for user_id_str in active_users {
        let key = format!("credits:{}", user_id_str);
        let balance: i32 = conn.get(&key).await.unwrap_or(0);

        // 2. Update Postgres
        // We do an absolute set here because Redis is the source of truth for the session.
        // Alternatively, we could use decrement deltas if we wanted to support multi-region.
        let user_uuid = uuid::Uuid::parse_str(&user_id_str)?;

        sqlx::query("UPDATE user_subscriptions SET credits_remaining = $1 WHERE user_id = $2")
            .bind(balance)
            .bind(user_uuid)
            .execute(&state.pool)
            .await?;

        // 3. Remove from active set if they haven't been active?
        // For simplicity, we keep them there or use a TTL-based approach.
    }

    Ok(())
}

// Atomic credit deduction
pub async fn deduct_credits(
    state: &Arc<AppState>,
    user_id: uuid::Uuid,
    cost: i32,
) -> Result<(), StatusCode> {
    let mut conn = state
        .redis
        .get_multiplexed_async_connection()
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let key = format!("credits:{}", user_id);
    let balance: i32 = conn.get(&key).await.unwrap_or(0);

    if balance < cost {
        return Err(axum::http::StatusCode::PAYMENT_REQUIRED);
    }

    let _: i32 = conn.decr(&key, cost).await.unwrap_or(0);
    // Add to active set for background sync
    let _: () = conn
        .sadd("active_billing_users", user_id.to_string())
        .await
        .unwrap_or(());
    Ok(())
}
