use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthContext {
    pub user_id: Uuid,
    pub tier_id: i32,
    pub scopes: Vec<String>,
    pub rate_limit: i32,
    pub credits_remaining: i32,
    pub ds_mode_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: Uuid,
    pub email: String,
}
