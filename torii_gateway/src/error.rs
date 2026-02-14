use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;

#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum AppError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("Unauthorized: {0}")]
    Unauthorized(String),
    #[error("Forbidden: {0}")]
    Forbidden(String),
    #[error("Rate limit exceeded")]
    RateLimitExceeded(u64), // Returns retry_after (ms)
    #[error("Payment required: {0}")]
    PaymentRequired(String),
    #[error("Internal server error: {0}")]
    Internal(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::Database(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            ),
            AppError::Redis(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Redis error: {}", e),
            ),
            AppError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg),
            AppError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg),
            AppError::RateLimitExceeded(etry_after) => {
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    [("Retry-After", etry_after.to_string())],
                    Json(json!({"error": "Rate limit exceeded"})),
                )
                    .into_response();
            }
            AppError::PaymentRequired(msg) => (StatusCode::PAYMENT_REQUIRED, msg),
            AppError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        (status, Json(json!({ "error": message }))).into_response()
    }
}
