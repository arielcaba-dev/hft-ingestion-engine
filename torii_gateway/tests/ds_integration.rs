use serde_json::json;
use sqlx::{postgres::PgPoolOptions, Row};
use tokio_tungstenite::connect_async;
use url::Url;
use uuid::Uuid;

const BASE_URL: &str = "http://localhost:8080";
const WS_DS_URL: &str = "ws://localhost:8080/v1/ws/ds";
const BOOTSTRAP_KEY: &str = "bootstrap_key";
const DB_URL: &str = "postgres://arroyo:secret_password_placeholder@localhost:5432/arroyo";

#[tokio::test]
async fn test_ds_auth_tiers() {
    // 1. Setup DB Connection
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(DB_URL)
        .await
        .expect("Failed to connect to DB");

    // 2. Create Users (Tier 2 & Tier 3)
    let tier2_user_id = create_test_user(&pool, 2).await;
    let tier3_user_id = create_test_user(&pool, 3).await;

    // 3. Generate API Keys
    let tier2_key = generate_api_key(tier2_user_id).await;
    let tier3_key = generate_api_key(tier3_user_id).await;

    println!("Tier 2 Key: {}", tier2_key);
    println!("Tier 3 Key: {}", tier3_key);

    // 4. Test Tier 2 Connection (Should Fail)
    println!("Testing Tier 2 Connection (Expect Failure)...");
    let tier2_url = Url::parse(&format!("{}?api_key={}", WS_DS_URL, tier2_key)).unwrap();
    let res = connect_async(tier2_url).await;
    match res {
        Ok(_) => panic!("Tier 2 user should NOT be able to connect to DS mode"),
        Err(e) => println!("Tier 2 connection failed as expected: {:?}", e),
    }

    // 5. Test Tier 3 Connection (Should Success)
    println!("Testing Tier 3 Connection (Expect Success)...");
    let tier3_url = Url::parse(&format!("{}?api_key={}", WS_DS_URL, tier3_key)).unwrap();
    let (mut ws_stream, _) = connect_async(tier3_url)
        .await
        .expect("Tier 3 user failed to connect to DS mode");

    // Verify we can receive messages (optional, or just auth success)
    // We heavily rely on connection upgrade success here.
    // Ideally we would wait for a message, but DS mode effectively streams Kafka data.
    // If no market data is flowing, we might just hang.
    // For this security test, connection success is the primary signal.
    // We can try to send a ping or close to ensure it's alive.

    ws_stream.close(None).await.expect("Failed to close socket");

    // Cleanup (Optional)
    // database cleanup logic here?
}

async fn create_test_user(pool: &sqlx::PgPool, tier_id: i32) -> Uuid {
    let email = format!("test_ds_{}_{}@example.com", tier_id, Uuid::new_v4());

    let user_id = sqlx::query("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(pool)
        .await
        .expect("Failed to create user")
        .try_get("id")
        .expect("Failed to get ID");

    // Create Subscription
    sqlx::query("INSERT INTO user_subscriptions (user_id, tier_id, credits_remaining) VALUES ($1, $2, 1000000)")
        .bind(user_id)
        .bind(tier_id)
        .execute(pool)
        .await
        .expect("Failed to create subscription");

    // ALSO: Seed Redis balance for immediate use
    let mut redis_conn = redis::Client::open("redis://localhost:6379")
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::AsyncCommands::set(&mut redis_conn, format!("credits:{}", user_id), 1000000)
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
