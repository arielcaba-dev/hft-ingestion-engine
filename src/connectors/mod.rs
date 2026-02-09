use async_trait::async_trait;
use tokio::sync::mpsc;
use crate::model::NormalizedMarketData;
use crate::config::ExchangeConfig;

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
}

#[async_trait]
impl ExchangeConnector for BinanceConnector {
    async fn run(&mut self, tx: mpsc::Sender<NormalizedMarketData>) {
        // Mock implementation of a WebSocket loop
        // In reality, this would use tokio-tungstenite to connect to self.config.websocket_url
        // and loop over incoming messages.
        
        println!("Starting Binance Connector for {}", self.config.name);
        
        // Mock loop
        loop {
            // simulating receiving a trade
            let trade = NormalizedMarketData {
                symbol_id: "BTC-USD".to_string(), // In reality we'd map this from "BTCUSDT"
                exchange: "binance".to_string(),
                event_type: crate::model::MarketEventType::Trade,
                price: 50000.0,
                quantity: 0.1,
                time_exchange: chrono::Utc::now(),
                time_ingest: chrono::Utc::now(),
                is_snapshot: false,
                sequence: 0,
            };

            if let Err(e) = tx.send(trade).await {
                eprintln!("Error sending data: {}", e);
                break;
            }
            
            // Artificial delay to simulate realistic tick rate
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
}
