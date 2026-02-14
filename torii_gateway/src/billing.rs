use crate::error::AppError;
use crate::AppState;
use redis::AsyncCommands;
use std::sync::Arc;
use uuid::Uuid;

pub struct Billing;

impl Billing {
    pub fn calculate_cost(path: &str) -> i32 {
        if path.starts_with("/v1/mcp") {
            100
        } else if path.starts_with("/v1/trades") {
            1 // Simplified for now, should parse limit
        } else if path.starts_with("/v1/ohlcv") {
            1
        } else {
            0
        }
    }

    pub async fn deduct_credits(
        state: &Arc<AppState>,
        user_id: Uuid,
        cost: i32,
    ) -> Result<(), AppError> {
        if cost <= 0 {
            return Ok(());
        }

        let key = format!("user:{}:credits", user_id);
        let mut conn = state.redis.get_multiplexed_async_connection().await?;

        // Atomic check and deduct
        // This is a simple implementation. For production, use Lua script to ensure non-negative.
        let current: Option<i32> = conn.get(&key).await?;
        let current = current.unwrap_or(0);

        if current < cost {
            return Err(AppError::PaymentRequired("Insufficient credits".into()));
        }

        let _: i32 = conn.decr(&key, cost).await?;

        // Async log to DB (fire and forget for latency, or spawn task)
        let state_clone = state.clone();
        tokio::spawn(async move {
            let _ = sqlx::query(
                "INSERT INTO credit_transactions (user_id, amount, balance_after, reason) VALUES ($1, $2, $3, $4)"
            )
            .bind(user_id)
            .bind(-cost)
            .bind(current - cost)
            .bind("api_access")
            .execute(&state_clone.db)
            .await;
        });

        Ok(())
    }
}
