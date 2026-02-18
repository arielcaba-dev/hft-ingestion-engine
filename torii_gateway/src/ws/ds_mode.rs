use crate::model::AuthContext;
use crate::AppState;
use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    http::StatusCode,
    response::IntoResponse,
    Extension,
};
use futures::{sink::SinkExt, stream::StreamExt};
use prost::Message as ProstMessage;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message as RdMessage;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid;

// Include generated protobuf
pub mod market_data {
    include!(concat!(env!("OUT_DIR"), "/market_data.rs"));
}

pub async fn ds_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    Extension(auth_context): Extension<AuthContext>,
) -> Result<impl IntoResponse, StatusCode> {
    if !auth_context.ds_mode_enabled {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(ws.on_upgrade(move |socket| handle_ds_socket(socket, state)))
}

async fn handle_ds_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, _receiver) = socket.split();

    // Create a channel to send protobuf bytes from the consumer thread to the websocket sender
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

    let config = state.config.clone();

    // Spawn a dedicated thread for Kafka consumption (blocking IO in rdkafka BaseConsumer)
    // Alternatively use StreamConsumer for async, but BaseConsumer is fine for a dedicated thread.
    // For high performance, we should reuse a global consumer and broadcast,
    // but for "DS Mode" (Direct Stream), a dedicated consumer per connection guarantees
    // strictly ordered, partition-specific delivery if configured.
    std::thread::spawn(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.redpanda_brokers)
            .set("group.id", format!("gateway-ds-{}", uuid::Uuid::new_v4())) // Unique group for DS mode (fan-out)
            .set("auto.offset.reset", "latest")
            .create()
            .expect("Consumer creation failed");

        consumer
            .subscribe(&["market_data_raw"])
            .expect("Subscribe failed");

        loop {
            // Poll for messages
            match consumer.poll(Duration::from_millis(100)) {
                Some(Ok(m)) => {
                    // Start Parsing
                    // Ingestion format: {"e":"trade","E":1771...,"s":"BTCUSDT","p":"70313.77",...}
                    if let Some(payload) = m.payload() {
                        if let Ok(json_val) = serde_json::from_slice::<serde_json::Value>(payload) {
                            // Extract fields safely
                            let symbol_raw = json_val
                                .get("s")
                                .and_then(|v| v.as_str())
                                .unwrap_or("UNKNOWN");
                            let price_str =
                                json_val.get("p").and_then(|v| v.as_str()).unwrap_or("0.0");
                            let qty_str =
                                json_val.get("q").and_then(|v| v.as_str()).unwrap_or("0.0");
                            let ts = json_val.get("E").and_then(|v| v.as_i64()).unwrap_or(0);

                            // Map Symbol
                            let symbol = match symbol_raw {
                                "BTCUSDT" => "BTC-USD",
                                "ETHUSDT" => "ETH-USD",
                                _ => symbol_raw,
                            };

                            let packet = market_data::MarketDataPacket {
                                symbol: symbol.to_string(),
                                price: price_str.parse().unwrap_or(0.0),
                                quantity: qty_str.parse().unwrap_or(0.0),
                                timestamp: ts,
                                is_snapshot: false,
                                sequence_id: 0,
                            };

                            let mut buf = Vec::new();
                            if let Ok(_) = packet.encode(&mut buf) {
                                if tx.send(buf).is_err() {
                                    return; // Channel closed, exit thread
                                }
                            }
                        }
                    }
                }
                Some(Err(e)) => {
                    eprintln!("Kafka error: {}", e);
                }
                None => {} // Timeout
            }
        }
    });

    // Forward messages from channel to WebSocket
    while let Some(data) = rx.recv().await {
        if sender.send(Message::Binary(data)).await.is_err() {
            break;
        }
    }
}
