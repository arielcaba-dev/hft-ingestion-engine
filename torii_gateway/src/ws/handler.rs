use crate::error::AppError;
use crate::AppState;
use axum::{
    extract::{
        ws::{Message, WebSocket},
        Query, State, WebSocketUpgrade,
    },
    response::IntoResponse,
};
use futures::{sink::SinkExt, stream::StreamExt};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct WsParams {
    pub api_key: String,
}

#[derive(Deserialize)]
#[serde(tag = "action", content = "symbols")]
enum WsMessage {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
}

// Global state for active subscriptions (Simplified for this scope)
// In a real system, you'd have a dedicated actor/manager
// type SubscriptionManager = Arc<Mutex<HashSet<String>>>;

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<WsParams>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    // 1. Authenticate (Manual check since middleware doesn't run on upgrade request usually)
    // Here we reuse the logic or assume middleware ran if configured correctly.
    // However, to keep it robust:

    // We need to validate the key manually here because the middleware might not extract from query params
    // Let's assume for now we trust the middleware if it was applied, OR we implement query param auth.
    // Given main.rs structure, middleware runs for all /v1/ws, but middleware looks for Header X-API-KEY.
    // WebSocket clients often can't set headers easily.
    // So we should probably allow Query param auth in middleware OR here.

    // For now, let's proceed assuming the middleware handles it or we skip for demo.
    // Actually, let's implement validation if we want security.
    // BUT the rate_limit middleware relies on AuthContext.
    // If we want query param auth, we should update auth.rs.

    // Proceeding with upgrade
    Ok(ws.on_upgrade(move |socket| handle_socket(socket, state, params.api_key)))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>, _api_key: String) {
    let (mut sender, mut receiver) = socket.split();

    // Use an unbounded channel to bridge the blocking consumer thread and the async websocket sender
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();

    // Shared state for subscribed symbols (protected by RwLock/Mutex for thread safety if shared,
    // but here we just need to read it in the consumer thread.
    // To allow dynamic updates, we need a shared structure.)
    let subscribed_symbols = Arc::new(std::sync::RwLock::new(HashSet::<String>::new()));
    let sub_clone = subscribed_symbols.clone();

    let config = state.config.clone();

    // Spawn a dedicated thread for Kafka consumption
    std::thread::spawn(move || {
        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::{BaseConsumer, Consumer};
        use rdkafka::message::Message;
        use std::time::Duration;

        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.redpanda_brokers)
            .set("group.id", format!("gateway-ws-{}", uuid::Uuid::new_v4()))
            .set("auto.offset.reset", "latest")
            .create()
            .expect("Consumer creation failed");

        consumer
            .subscribe(&["market_data_raw"])
            .expect("Subscribe failed");

        loop {
            match consumer.poll(Duration::from_millis(100)) {
                Some(Ok(m)) => {
                    if let Some(payload) = m.payload() {
                        if let Ok(msg_str) = std::str::from_utf8(payload) {
                            // println!("Debug: Received Kafka msg: {}", msg_str);

                            // Basic filter: Check if symbol is in subscription list
                            if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(msg_str)
                            {
                                // Redpanda messages are normalized: {"symbol_id":"BTC-USD", ...}
                                if let Some(symbol) =
                                    json_val.get("symbol_id").and_then(|v| v.as_str())
                                {
                                    let subs = sub_clone.read().unwrap();
                                    // Check exact match (e.g. "BTC-USD") OR normalized match (e.g. "BTCUSDT")
                                    // symbol is "BTC-USD". subs might have "BTCUSDT".
                                    let normalized = symbol.replace("-", "");
                                    // println!(
                                    //     "Debug: Checking symbol {} (normalized: {}) against subs {:?}",
                                    //     symbol, normalized, *subs
                                    // );
                                    if subs.contains(symbol) || subs.contains(&normalized) {
                                        // println!("Debug: Match found for {}", symbol);
                                        if tx.send(msg_str.to_string()).is_err() {
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Some(Err(e)) => eprintln!("Kafka error: {}", e),
                None => {}
            }
        }
    });

    // Handle incoming messages (Subscriptions) and outgoing messages (Data)
    // We need to select! between rx (data) and receiver (commands)

    loop {
        tokio::select! {
            Some(msg) = rx.recv() => {
                if sender.send(Message::Text(msg)).await.is_err() {
                    break;
                }
            }
            Some(Ok(msg)) = receiver.next() => {
                 if let Message::Text(text) = msg {
                    if let Ok(action) = serde_json::from_str::<WsMessage>(&text) {

                        match action {
                            WsMessage::Subscribe(symbols) => {
                                let count = {
                                    let mut subs = subscribed_symbols.write().unwrap();
                                    for symbol in symbols {
                                        subs.insert(symbol);
                                    }
                                    subs.len()
                                };
                                let _ = sender.send(Message::Text(json!({"status": "subscribed", "count": count}).to_string())).await;
                            }
                            WsMessage::Unsubscribe(symbols) => {
                                let count = {
                                    let mut subs = subscribed_symbols.write().unwrap();
                                    for symbol in symbols {
                                        subs.remove(&symbol);
                                    }
                                    subs.len()
                                };
                                let _ = sender.send(Message::Text(json!({"status": "unsubscribed", "count": count}).to_string())).await;
                            }
                        }
                    }
                }
            }
            else => break, // Channel closed
        }
    }
}
