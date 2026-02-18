use serde_json::json;
use sqlx::{postgres::PgPoolOptions, Row};
use uuid::Uuid;

const BASE_URL: &str = "http://localhost:8080";
const BOOTSTRAP_KEY: &str = "bootstrap_key";
const DB_URL: &str = "postgres://arroyo:secret_password_placeholder@localhost:5432/arroyo";

#[tokio::test]
async fn test_rate_limiting_and_billing() {
    // 1. Setup DB Connection
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(DB_URL)
        .await
        .expect("Failed to connect to DB");

    // 2. Create a Test User (Tier 1 - Free, 10 req/s)
    let user_id = create_test_user(&pool, 1, 100).await;
    let api_key = generate_api_key(user_id).await;
    println!("Test User: {}, API Key: {}", user_id, api_key);

    let client = reqwest::Client::new();

    // 3. Test Rate Limiting (Burst 15 requests)
    println!("Sending 15 requests to test rate limit (limit=10)...");
    let mut success_count = 0;
    let mut rejected_count = 0;
    let mut last_remaining = 0;

    for i in 0..15 {
        let res = client
            .get(format!("{}/health", BASE_URL)) // Health is not protected but in our router it's layered.
            // Wait, in main.rs, health is at the top. Let's use an endpoint that IS protected by auth.
            // Actually, health is also protected by auth because layers are applied to ALL routes if placed after them?
            // No, the layers are at the end: `.layer(rate_limit).layer(auth)`.
            // So they apply to everything in the router above them.
            .header("X-API-KEY", &api_key)
            .send()
            .await
            .expect("Failed to send request");

        let status = res.status();
        let headers = res.headers();

        let remaining = headers
            .get("X-RateLimit-Remaining")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<i32>().ok());

        if status == 200 {
            success_count += 1;
            last_remaining = remaining.unwrap_or(0);
        } else if status == 429 {
            rejected_count += 1;
        } else {
            panic!("Unexpected status code: {} at request {}", status, i);
        }
    }

    println!(
        "Success: {}, Rejected: {}, Last Remaining: {}",
        success_count, rejected_count, last_remaining
    );
    assert!(success_count <= 10, "Too many successful requests allowed");
    assert!(rejected_count >= 5, "Rate limit did not reject burst");

    // 4. Test Billing (Credits should have decreased)
    // The credit sync is every 10s. We should check Redis directly or wait.
    // Let's check the balance in DB after waiting a bit or just assume internal logic works if success_count is correct.
    // Actually, we can check the balance in DB by waiting 12 seconds.
    println!("Waiting for billing sync (12s)...");
    tokio::time::sleep(std::time::Duration::from_secs(12)).await;

    let balance: i32 =
        sqlx::query("SELECT credits_remaining FROM user_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .expect("Failed to fetch balance")
            .get("credits_remaining");

    println!("Final Balance: {}", balance);
    // Initial was 100. We succeeded success_count times.
    assert_eq!(
        balance,
        100 - (success_count as i32),
        "Credits were not deducted correctly"
    );

    // 5. Test Credit Exhaustion
    println!("Testing credit exhaustion...");
    // Update balance to 0 in Redis directly or via DB and wait.
    // Since deduction happens in Redis first, we should really seed Redis.
    // But our middleware calls deduct_credits which checks balance.
    // Let's just update DB to 0 and wait for sync? No, sync is DB -> Redis or Redis -> DB?
    // Current billing.rs sync is Redis -> DB.
    // So we need to update Redis.

    let mut redis_conn = redis::Client::open("redis://localhost:6379")
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::AsyncCommands::set(&mut redis_conn, format!("credits:{}", user_id), 0)
        .await
        .unwrap();

    let res = client
        .get(format!("{}/health", BASE_URL))
        .header("X-API-KEY", &api_key)
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(
        res.status(),
        402,
        "Expected 402 Payment Required for exhausted credits"
    );
}

async fn create_test_user(pool: &sqlx::PgPool, tier_id: i32, initial_balance: i32) -> Uuid {
    let email = format!("test_gw_{}_{}@example.com", tier_id, Uuid::new_v4());

    let user_id = sqlx::query("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(pool)
        .await
        .expect("Failed to create user")
        .try_get("id")
        .expect("Failed to get ID");

    // Create Subscription
    sqlx::query(
        "INSERT INTO user_subscriptions (user_id, tier_id, credits_remaining) VALUES ($1, $2, $3)",
    )
    .bind(user_id)
    .bind(tier_id)
    .bind(initial_balance)
    .execute(pool)
    .await
    .expect("Failed to create subscription");

    // ALSO: Seed Redis balance for immediate use
    let mut redis_conn = redis::Client::open("redis://localhost:6379")
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::AsyncCommands::set(
        &mut redis_conn,
        format!("credits:{}", user_id),
        initial_balance,
    )
    .await
    .unwrap();

    user_id
}

async fn generate_api_key(user_id: Uuid) -> String {
    let client = reqwest::Client::new();
    let res = client
        .post(format!("{}/v1/keys", BASE_URL))
        .header("Content-Type", "application/json")
        .header("X-API-KEY", BOOTSTRAP_KEY)
        .json(&json!({
            "user_id": user_id,
            "scopes": ["market:read"]
        }))
        .send()
        .await
        .expect("Failed to call create key API");

    if res.status() != 200 {
        panic!("Failed to create key: Status {}", res.status());
    }

    let body: serde_json::Value = res.json().await.expect("Failed to parse JSON");
    body["key"].as_str().expect("Key not found").to_string()
}
