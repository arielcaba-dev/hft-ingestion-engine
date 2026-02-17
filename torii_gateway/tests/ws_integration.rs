use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

const BASE_URL: &str = "http://localhost:8085";
const WS_URL: &str = "ws://localhost:8085/v1/ws";
const BOOTSTRAP_KEY: &str = "bootstrap_key";

#[tokio::test]
async fn test_ws_authentication_flow() {
    // 1. Create a new API key
    let client = reqwest::Client::new();
    let create_key_res = client
        .post(format!("{}/v1/keys", BASE_URL))
        .header("Content-Type", "application/json")
        .header("X-API-KEY", BOOTSTRAP_KEY)
        .json(&json!({
            "user_id": "a13a091f-6932-42e7-bf8d-880893e5578e",
            "scopes": ["market:read"]
        }))
        .send()
        .await
        .expect("Failed to execute create key request");

    assert_eq!(create_key_res.status(), 200);

    let key_data: serde_json::Value = create_key_res.json().await.expect("Failed to parse JSON");
    let api_key = key_data["key"].as_str().expect("Key not found");
    println!("Generated API Key: {}", api_key);

    // 2. Test WebSocket connection with INVALID key
    let invalid_ws_url = Url::parse(&format!("{}?api_key=invalid_key", WS_URL)).unwrap();
    let result = connect_async(invalid_ws_url).await;
    assert!(result.is_err(), "Should fail with invalid key");

    // 3. Test WebSocket connection with VALID key
    let valid_ws_url = Url::parse(&format!("{}?api_key={}", WS_URL, api_key)).unwrap();
    let (ws_stream, _) = connect_async(valid_ws_url)
        .await
        .expect("Failed to connect with valid key");

    let (mut write, mut read) = ws_stream.split();

    // 4. Test Subscription
    let subscribe_msg = json!({
        "action": "Subscribe",
        "symbols": ["BTC-USD"]
    });

    write
        .send(Message::Text(subscribe_msg.to_string()))
        .await
        .expect("Failed to send subscribe");

    // Expecting status message
    if let Some(msg) = read.next().await {
        let msg = msg.expect("Error reading message");
        if let Message::Text(text) = msg {
            println!("Received: {}", text);
            let response: serde_json::Value = serde_json::from_str(&text).expect("Invalid JSON");
            assert_eq!(response["status"], "subscribed");
        } else {
            panic!("Expected text message");
        }
    } else {
        panic!("Connection closed unexpectedly");
    }
}
