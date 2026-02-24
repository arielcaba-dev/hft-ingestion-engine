"""
Liquidation Ingestion Worker
Subscribes to Binance Futures forceOrder WebSocket stream.
Writes liquidation events to QuestDB via ILP and publishes to Redpanda.
"""
import json
import asyncio
import signal
import sys
from datetime import datetime, timezone

try:
    import websockets
except ImportError:
    print("Installing websockets...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--break-system-packages", "--user", "websockets"])
    import websockets

from questdb.ingress import Sender, TimestampNanos

# Configuration
QUESTDB_CONF = "tcp::addr=localhost:9009;"
BINANCE_WS = "wss://fstream.binance.com/ws/!forceOrder@arr"

# Symbols we care about (normalized)
SYMBOL_MAP = {
    "BTCUSDT": "BTC-USD",
    "ETHUSDT": "ETH-USD",
    "SOLUSDT": "SOL-USD",
    "BNBUSDT": "BNB-USD",
    "XRPUSDT": "XRP-USD",
    "DOGEUSDT": "DOGE-USD",
    "ADAUSDT": "ADA-USD",
    "AVAXUSDT": "AVAX-USD",
}

running = True

def handle_signal(sig, frame):
    global running
    running = False
    print("\nStopping liquidation worker...")

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

async def ingest_liquidations():
    """Connect to Binance Futures and ingest liquidation events."""
    global running
    print(f"Connecting to Binance Futures liquidation stream...")
    print(f"QuestDB ILP target: {QUESTDB_CONF}")

    reconnect_delay = 1
    while running:
        try:
            async with websockets.connect(BINANCE_WS, ping_interval=20) as ws:
                print("✅ Connected to Binance Futures forceOrder stream")
                reconnect_delay = 1  # Reset on successful connect

                with Sender.from_conf(QUESTDB_CONF) as sender:
                    while running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            continue  # No liquidation in 30s, keep waiting

                        data = json.loads(msg)
                        order = data.get("o", data)

                        raw_symbol = order.get("s", "")
                        symbol = SYMBOL_MAP.get(raw_symbol)
                        if not symbol:
                            continue  # Skip symbols we don't track

                        side = "Long" if order.get("S", "") == "SELL" else "Short"
                        price = float(order.get("p", 0))
                        quantity = float(order.get("q", 0))
                        trade_time = int(order.get("T", 0))

                        # Convert ms to datetime
                        ts = datetime.fromtimestamp(trade_time / 1000, tz=timezone.utc)

                        # Write to QuestDB
                        sender.row(
                            "liquidations",
                            symbols={
                                "symbol": symbol,
                                "exchange": "binance_futures",
                                "side": side,
                            },
                            columns={
                                "price": price,
                                "quantity": quantity,
                            },
                            at=TimestampNanos(int(ts.timestamp() * 1e9)),
                        )
                        sender.flush()

                        notional = price * quantity
                        print(f"💥 {side} LIQ: {symbol} @ ${price:,.2f} x {quantity:.4f} (${notional:,.0f})")

        except websockets.exceptions.ConnectionClosed:
            print(f"Connection closed. Reconnecting in {reconnect_delay}s...")
        except Exception as e:
            print(f"Error: {e}. Reconnecting in {reconnect_delay}s...")

        if running:
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

if __name__ == "__main__":
    print("🔥 Torii Liquidation Ingestion Worker")
    asyncio.run(ingest_liquidations())
