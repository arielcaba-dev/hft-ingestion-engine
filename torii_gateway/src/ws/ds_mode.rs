use crate::AppState;
use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::IntoResponse,
};
use futures::{sink::SinkExt, stream::StreamExt};
use kafka::consumer::{Consumer, FetchOffset};
use prost::Message as ProstMessage;
use std::sync::Arc; // Rename to avoid conflict with axum Message

// Include generated protobuf
pub mod market_data {
    include!(concat!(env!("OUT_DIR"), "/market_data.rs"));
}

pub async fn ds_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ds_socket(socket, state))
}

async fn handle_ds_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, _receiver) = socket.split();

    // Create a channel to bridge blocking Kafka thread and Async WebSocket
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

    let brokers = vec![state.config.redpanda_brokers.clone()];
    let topic = "market_data_raw".to_string();

    // Spawn dedicated blocking thread for high-performance consumption
    std::thread::spawn(move || {
        let mut consumer = match Consumer::from_hosts(brokers)
            .with_topic(topic)
            .with_fallback_offset(FetchOffset::Latest)
            //.with_group("ds_consumer_group".to_string()) // DS mode might want unique group or no group?
            // Usually DS mode is unique per connection, so we might need random group or assign partition manually
            .create()
        {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to create kafka consumer: {}", e);
                return;
            }
        };

        loop {
            // Poll for messages
            let poll_result = consumer.poll();
            if let Err(e) = poll_result {
                eprintln!("Kafka poll failed: {}", e);
                std::thread::sleep(std::time::Duration::from_millis(100));
                continue;
            }

            for ms in poll_result.unwrap().iter() {
                for _m in ms.messages() {
                    // In a real scenario, 'm.value' is the source data.
                    // We need to parse it (if it's JSON/Bincode) and re-serialize to Protobuf.
                    // Or if source is already Protobuf, pass through.

                    // For demo, we construct a dummy packet
                    let packet = market_data::MarketDataPacket {
                        symbol: "BTC-USD".to_string(),
                        price: 67000.0,
                        quantity: 0.5,
                        timestamp: chrono::Utc::now().timestamp_millis(),
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
    });

    // Async loop to pipe data to WebSocket
    while let Some(data) = rx.recv().await {
        if sender.send(Message::Binary(data)).await.is_err() {
            break;
        }
    }
}
