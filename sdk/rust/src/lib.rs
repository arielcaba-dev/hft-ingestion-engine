use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::StreamExt;
use prost::Message as ProstMessage;
use std::error::Error;
use url::Url;

// Include generation
pub mod market_data {
    include!(concat!(env!("OUT_DIR"), "/market_data.rs"));
}

pub struct ToriiClient {
    url: String,
    api_key: String,
}

impl ToriiClient {
    pub fn new(url: &str, api_key: &str) -> Self {
        Self {
            url: url.to_string(),
            api_key: api_key.to_string(),
        }
    }

    pub async fn stream_market_data<F>(&self, symbols: Vec<&str>, mut callback: F) -> Result<(), Box<dyn Error>>
    where
        F: FnMut(market_data::MarketData) -> (),
    {
        // 1. Build URL with Auth
        let mut url = Url::parse(&self.url)?;
        url.set_query(Some(&format!("api_key={}", self.api_key)));

        // 2. Connect
        let (ws_stream, _) = connect_async(url).await?;
        println!("Connected to Torii Gateway DS Mode");

        let (mut write, mut read) = ws_stream.split();

        // 3. Subscribe (simplified: assume connected means subscribed to broadcast or send sub message)
        // In real impl, we'd send a JSON subscription frame here.
        println!("Subscribing to: {:?}", symbols);

        // 4. Processing Loop
        while let Some(msg) = read.next().await {
            let msg = msg?;
            if let Message::Binary(payload) = msg {
                // Zero-copy decoding? Prost handles Bytes/Vec efficiently.
                // For true zero-copy we might need 'bytes' crate integration, but strict object decoding copies fields.
                // However, prost is very fast.
                match market_data::MarketData::decode(&payload[..]) {
                    Ok(data) => callback(data),
                    Err(e) => eprintln!("Failed to decode Protobuf: {}", e),
                }
            }
        }

        Ok(())
    }
}
