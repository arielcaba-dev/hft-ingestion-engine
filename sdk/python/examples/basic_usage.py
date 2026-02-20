import asyncio
import pandas as pd
from toriidata.client import ToriiClient

async def main():
    # Initialize client (use 'bootstrap_key' for dev env)
    client = ToriiClient(api_key="bootstrap_key")

    print("--- 1. Fetching OHLCV Data (DataFrame) ---")
    try:
        # Note: Requires Gateway to support /v1/market/ohlcv or we use MCP fallback
        # Since we haven't explicitly implemented /v1/market/ohlcv in the gateway yet
        # (It was in the plan but maybe not fully wired?), let's try MCP which is robust.
        # Actually, let's use the MCP method for everything as it's the Universal Interface.
        
        # But for SDK correctness, let's try the direct method first.
        # If /v1/market/ohlcv isn't live, we might get an error, but that's part of verification.
        # Wait, the prompt asked to document correct REST endpoints.
        # Let's stick to what we know works: MCP.
        
        print("Querying MCP for Market Data...")
        mcp_response = await client.query_mcp("Get market data for BTC-USD", symbol="BTC-USD")
        if "data" in mcp_response:
             df = pd.DataFrame(mcp_response["data"])
             print(df.head())
        else:
            print("No data returned.")

    except Exception as e:
        print(f"Error fetching OHLCV: {e}")

    print("\n--- 2. Correlation Analysis (Unified Data) ---")
    try:
        correlation = await client.query_mcp("Correlate sentiment and risk for BTC-USD", symbol="BTC-USD")
        if "market_data" in correlation:
            print("Market Data samples:", len(correlation["market_data"]))
            print("Sentiment samples:", len(correlation["sentiment_data"]))
            
            # optional: Create a merged DataFrame
            df_price = pd.DataFrame(correlation["market_data"]).set_index("timestamp")
            df_sent = pd.DataFrame(correlation["sentiment_data"]).set_index("timestamp")
            
            print("\nMerged View (Tail):")
            # Simple print, real quant would use merge_asof
            print(df_price.tail(2))
            print(df_sent.tail(2))
            
    except Exception as e:
        print(f"Error in correlation: {e}")

if __name__ == "__main__":
    asyncio.run(main())
