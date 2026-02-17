use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

const BASE_URL: &str = "http://localhost:8080";
const WS_URL: &str = "ws://localhost:8080/v1/ws";
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
            "user_id": "8498d5ad-674b-40a7-8d31-ab09d2eeb7e8", // Known existing user ID
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

    // 5. Test End-to-End Data Flow (Redpanda -> WebSocket)
    // Produce a message to Redpanda
    use rdkafka::config::ClientConfig;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use std::time::Duration;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:19092")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let payload = json!({
        "symbol_id": "BTC-USD",
        "price": 50100.0,
        "quantity": 0.5,
        "timestamp": 1234567891
    })
    .to_string();

    // Give the Gateway's consumer some time to initialize/rebalance
    tokio::time::sleep(Duration::from_secs(3)).await;

    println!("Producing message: {}", payload);
    let delivery_status = producer
        .send(
            FutureRecord::to("market_data_raw")
                .payload(&payload)
                .key("BTC-USD"),
            Duration::from_secs(5),
        )
        .await;

    assert!(
        delivery_status.is_ok(),
        "Failed to produce message to Redpanda"
    );

    // Expect the message on WebSocket
    // We might need to wait a bit or loop, but since we just subscribed, it should come.
    // Give it a few seconds timeout
    let timeout = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(timeout);

    // 5. Test End-to-End Data Flow (Redpanda -> WebSocket)
    // ... (Redpanda/Producer setup skipped for brevity in tool call, ensuring context) ...
    // (We will keep the existing test flow and ADD the ping check at the end or in the loop)

    // Let's modify the loop to also look for Pings.
    // We already have a loop waiting for data.
    // If we receive a Ping, that's good!

    let mut received_data = false;
    let mut received_ping = false;

    loop {
        tokio::select! {
             Some(msg) = read.next() => {
                let msg = msg.expect("Error reading message");
                match msg {
                    Message::Text(text) => {
                        println!("Received Data: {}", text);
                        if text.contains("50100.0") {
                            received_data = true;
                        }
                    }
                    Message::Ping(_) => {
                        println!("Received Ping!");
                        received_ping = true;
                    }
                    _ => {}
                }

                if received_data && received_ping {
                    break;
                }
            }
            _ = &mut timeout => {
                // If we timed out but got data, standard test passed.
                // But we WANT to see a ping.
                // The Ping interval is 5s. Timeout is 5s. Race condition.
                // Let's rely on the immediate first tick or extend timeout.
                if received_data {
                     println!("Got data but valid Ping might be delayed or swallowed by client lib.");
                     // For this specific test, if we got data, we are good on the "E2E" part.
                     // But to verify Ping, we might need to wait longer.
                     // Let's extend timeout to 7s if data received but no ping.
                }
                break; // Break the select loop
            }
        }
    }

    // Check results
    if !received_data {
        panic!("Timed out waiting for Redpanda data");
    }
    // We strictly want to verify Ping now.
    // If we haven't received it yet, wait a bit more.
    if !received_ping {
        println!("Waiting for Ping...");
        let ping_timeout = tokio::time::sleep(Duration::from_secs(6));
        tokio::pin!(ping_timeout);
        loop {
            tokio::select! {
                Some(msg) = read.next() => {
                     if let Ok(Message::Ping(_)) = msg {
                         println!("Received Ping (delayed)!");
                         received_ping = true;
                         break;
                     }
                }
                _ = &mut ping_timeout => {
                    break;
                }
            }
        }
    }

    assert!(received_ping, "Did not receive Heartbeat Ping from server");
}
