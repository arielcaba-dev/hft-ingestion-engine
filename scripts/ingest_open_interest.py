"""
Open Interest Ingestion Worker
Polls Binance Futures REST API every 60 seconds for OI snapshots.
Writes to QuestDB via ILP.
"""
import json
import time
import signal
import sys
from datetime import datetime, timezone

try:
    import urllib.request
except:
    pass

from questdb.ingress import Sender, TimestampNanos

# Configuration
QUESTDB_CONF = "tcp::addr=localhost:9009;"
POLL_INTERVAL = 60  # seconds

# Symbols to track
SYMBOLS = {
    "BTCUSDT": "BTC-USD",
    "ETHUSDT": "ETH-USD",
    "SOLUSDT": "SOL-USD",
    "BNBUSDT": "BNB-USD",
    "XRPUSDT": "XRP-USD",
}

BINANCE_OI_URL = "https://fapi.binance.com/fapi/v1/openInterest"

running = True

def handle_signal(sig, frame):
    global running
    running = False
    print("\nStopping OI worker...")

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

def fetch_open_interest(binance_symbol: str) -> dict:
    """Fetch OI from Binance Futures REST API."""
    url = f"{BINANCE_OI_URL}?symbol={binance_symbol}"
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "ToriiData/1.0")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())

def run():
    """Main polling loop."""
    global running
    print(f"📊 Torii Open Interest Poller")
    print(f"Polling interval: {POLL_INTERVAL}s")
    print(f"QuestDB ILP target: {QUESTDB_CONF}")
    print(f"Tracking symbols: {list(SYMBOLS.values())}")

    while running:
        try:
            with Sender.from_conf(QUESTDB_CONF) as sender:
                now = datetime.now(tz=timezone.utc)
                ts_nanos = TimestampNanos(int(now.timestamp() * 1e9))

                for binance_sym, torii_sym in SYMBOLS.items():
                    try:
                        data = fetch_open_interest(binance_sym)
                        oi_value = float(data.get("openInterest", 0))

                        # Fetch mark price for notional calculation
                        mark_url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={binance_sym}"
                        mark_req = urllib.request.Request(mark_url)
                        mark_req.add_header("User-Agent", "ToriiData/1.0")
                        with urllib.request.urlopen(mark_req, timeout=10) as mark_resp:
                            mark_data = json.loads(mark_resp.read().decode())
                        mark_price = float(mark_data.get("markPrice", 0))
                        notional = oi_value * mark_price

                        sender.row(
                            "open_interest",
                            symbols={
                                "symbol": torii_sym,
                                "exchange": "binance_futures",
                            },
                            columns={
                                "oi_value": oi_value,
                                "notional_value": notional,
                            },
                            at=ts_nanos,
                        )

                        print(f"  {torii_sym}: OI={oi_value:,.2f} contracts, Notional=${notional:,.0f}")

                    except Exception as e:
                        print(f"  ⚠️  Error fetching {binance_sym}: {e}")

                sender.flush()
                print(f"✅ OI snapshot written at {now.isoformat()}")

        except Exception as e:
            print(f"❌ Sender error: {e}")

        # Wait for next poll
        for _ in range(POLL_INTERVAL):
            if not running:
                break
            time.sleep(1)

    print("OI worker stopped.")

if __name__ == "__main__":
    run()
