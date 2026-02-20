use torii_client::ToriiClient;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let api_key = env::var("TORII_API_KEY").unwrap_or("bootstrap_key".to_string());
    let url = "ws://localhost:8080/ws";

    let client = ToriiClient::new(url, &api_key);

    println!("Starting Market Maker Bot..."); // :)

    client.stream_market_data(vec!["BTC-USD", "ETH-USD"], |data| {
        // High-frequency callback
        // In a real bot, we'd push to a ring buffer or update an order book struct
        println!(
            "Tick: {} @ {:.2} (vol: {:.4})",
            data.symbol, data.price, data.volume
        );
    }).await?;

    Ok(())
}
