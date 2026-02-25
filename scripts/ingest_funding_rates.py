import time
import requests
import os
import json
from datetime import datetime, timezone
from questdb.ingress import Sender, TimestampNanos

# Configuration
QUESTDB_HOST = os.getenv("QUESTDB_HOST", "localhost")
QUESTDB_CONF = f"tcp::addr={QUESTDB_HOST}:9009;"
POLL_INTERVAL = 60  # seconds

# Binance Futures Premium Index Endpoint
BINANCE_FUNDING_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"

# Symbols to track (normalized to our format)
SYMBOLS = {
    "BTCUSDT": "BTC-USD",
    "ETHUSDT": "ETH-USD",
    "SOLUSDT": "SOL-USD",
    "BNBUSDT": "BNB-USD",
    "XRPUSDT": "XRP-USD"
}

def fetch_funding_rates():
    """Fetch mark price and funding rate for all symbols from Binance Futures."""
    try:
        response = requests.get(BINANCE_FUNDING_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        results = []
        for item in data:
            if item["symbol"] in SYMBOLS:
                results.append({
                    "symbol": SYMBOLS[item["symbol"]],
                    "mark_price": float(item["markPrice"]),
                    "funding_rate": float(item["lastFundingRate"]),
                    # Use the nextFundingTime to represent the current funding window or just current time
                    "timestamp_ms": int(time.time() * 1000) 
                })
        return results
    except Exception as e:
        print(f"❌ Error fetching Funding Rates: {e}")
        return []

def write_to_questdb(data):
    """Write funding rate snapshots to QuestDB via ILP."""
    if not data:
        return

    try:
        with Sender.from_conf(QUESTDB_CONF) as sender:
            for item in data:
                # Convert ms to ns
                ts_nanos = item["timestamp_ms"] * 1000000
                
                sender.row(
                    "funding_rates",
                    symbols={
                        "symbol": item["symbol"],
                        "exchange": "binance_futures"
                    },
                    columns={
                        "funding_rate": item["funding_rate"],
                        "mark_price": item["mark_price"]
                    },
                    at=TimestampNanos(ts_nanos)
                )
            sender.flush()
            
            # Formatted log
            now_iso = datetime.now(timezone.utc).isoformat()
            print(f"✅ Funding Rates written at {now_iso}")
            for item in data:
                print(f"  {item['symbol']}: Rate={item['funding_rate']:.6f} Mark=${item['mark_price']:,.2f}")
                
    except Exception as e:
        print(f"❌ Sender error: {e}")

if __name__ == "__main__":
    print("📊 Torii Funding Rate Poller")
    print(f"Polling interval: {POLL_INTERVAL}s")
    print(f"QuestDB ILP target: {QUESTDB_CONF}")
    print(f"Tracking symbols: {list(SYMBOLS.values())}")
    
    while True:
        rates = fetch_funding_rates()
        write_to_questdb(rates)
        time.sleep(POLL_INTERVAL)
