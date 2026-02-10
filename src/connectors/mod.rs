use crate::config::ExchangeConfig;
use crate::model::{MarketEventType, NormalizedMarketData};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

#[async_trait]
pub trait ExchangeConnector {
    async fn run(&mut self, output: mpsc::Sender<NormalizedMarketData>);
}

pub struct BinanceConnector {
    config: ExchangeConfig,
}

impl BinanceConnector {
    pub fn new(config: ExchangeConfig) -> Self {
        Self { config }
    }

    async fn handle_connection(
        &self,
        tx: &mpsc::Sender<NormalizedMarketData>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = Url::parse(&self.config.websocket_url)?;
        info!("Connecting to Binance WebSocket: {}", url);

        let (mut ws_stream, _) = connect_async(url).await?;
        info!("Connected to Binance WebSocket");

        // Subscribe to trade channel (hardcoded for MVP demo)
        // In a real app, we would iterate over self.config.symbols or similar
        let subscribe_msg = json!({
            "method": "SUBSCRIBE",
            "params": [
                "btcusdt@trade"
            ],
            "id": 1
        });

        ws_stream
            .send(Message::Text(subscribe_msg.to_string()))
            .await?;
        info!("Sent subscription message: btcusdt@trade");

        while let Some(msg) = ws_stream.next().await {
            let msg = msg?;
            match msg {
                Message::Text(text) => {
                    // trace!("Received: {}", text);
                    if let Ok(event) = serde_json::from_str::<BinanceTradeEvent>(&text) {
                        if event.e == "trade" {
                            let data = NormalizedMarketData {
                                symbol_id: "BTC-USD".to_string(), // Simplified mapping
                                exchange: "binance".to_string(),
                                event_type: MarketEventType::Trade,
                                price: event.p.parse().unwrap_or(0.0),
                                quantity: event.q.parse().unwrap_or(0.0),
                                time_exchange: Utc.timestamp_millis_opt(event.trade_time).unwrap(),
                                time_ingest: Utc::now(),
                                is_snapshot: false,
                                sequence: event.t,
                            };

                            if tx.send(data).await.is_err() {
                                return Err("Receiver dropped".into());
                            }
                        }
                    } else {
                        // Handle other messages (ping/pong handled by tungstenite implicitly, or subscription responses)
                        // warn!("Failed to parse or unknown message: {}", text);
                    }
                }
                Message::Ping(_) | Message::Pong(_) => {}
                Message::Close(_) => return Ok(()),
                _ => {}
            }
        }

        Ok(())
    }
}

#[async_trait]
impl ExchangeConnector for BinanceConnector {
    async fn run(&mut self, tx: mpsc::Sender<NormalizedMarketData>) {
        loop {
            match self.handle_connection(&tx).await {
                Ok(_) => warn!("Binance connection closed gracefully. Reconnecting..."),
                Err(e) => error!("Binance connection error: {}. Reconnecting in 5s...", e),
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    }
}

#[derive(Deserialize, Debug)]
struct BinanceTradeEvent {
    e: String, // Event type
    #[serde(rename = "E")]
    _event_time: u64, // Event time
    #[serde(rename = "s")]
    _symbol: String, // Symbol
    t: u64,    // Trade ID
    p: String, // Price
    q: String, // Quantity
    #[serde(rename = "T")]
    trade_time: i64, // Trade time
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_binance_trade() {
        let json_data = r#"
        {
          "e": "trade",
          "E": 123456789,
          "s": "BNBBTC",
          "t": 12345,
          "p": "0.001",
          "q": "100",
          "b": 88,
          "a": 50,
          "T": 123456785,
          "m": true,
          "M": true
        }
        "#;

        let event: BinanceTradeEvent = serde_json::from_str(json_data).unwrap();
        assert_eq!(event.e, "trade");
        assert_eq!(event.p, "0.001");
        assert_eq!(event.q, "100");
        assert_eq!(event.trade_time, 123456785);
    }
}
