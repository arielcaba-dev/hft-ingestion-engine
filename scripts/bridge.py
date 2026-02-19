"""
QuestDB Bridge - Kafka to QuestDB ILP Writer
Consumes market data from Redpanda and writes to QuestDB using InfluxDB Line Protocol.
Supports multiple topics: market_data_raw (trades) and ohlcv_1m (candles)
"""
import json
import os
import threading
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from questdb.ingress import Sender, IngressError, TimestampNanos

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'redpanda:9092')
QUESTDB_HOST = os.getenv('QUESTDB_HOST', 'questdb')
QUESTDB_PORT = int(os.getenv('QUESTDB_PORT', 9009))

def parse_iso8601_to_nanos(timestamp_str):
    """Convert ISO 8601 timestamp to TimestampNanos"""
    try:
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        nanos = int(dt.timestamp() * 1_000_000_000)
        return TimestampNanos(nanos)
    except Exception as e:
        print(f"⚠️ Timestamp parse error: {e}, using now()")
        return TimestampNanos.now()

def consume_trades():
    """Consumer for raw trade data -> trades table"""
    c = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'questdb-trades-group',
        'auto.offset.reset': 'earliest'
    })
    c.subscribe(['market_data_raw', 'market_data_v2'])
    
    conf = f'tcp::addr={QUESTDB_HOST}:{QUESTDB_PORT};'
    print(f"🚀 Trades consumer started: {KAFKA_BROKER} -> {QUESTDB_HOST}:{QUESTDB_PORT}")
    
    with Sender.from_conf(conf) as sender:
        try:
            msg_count = 0
            while True:
                msg = c.poll(1.0)
                
                if msg is None:
                    sender.flush()
                    continue
                
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    
                    sender.row(
                        'trades',
                        symbols={
                            'symbol': data.get('symbol_id', 'UNKNOWN')
                        },
                        columns={
                            'price': float(data.get('price', 0.0)),
                            'quantity': float(data.get('quantity', 0.0))
                        },
                        at=TimestampNanos.now()
                    )
                    
                    msg_count += 1
                    if msg_count % 1000 == 0:
                       sender.flush()

                except Exception as e:
                    print(f"⚠️ Trades serialization error: {e}")

        except KeyboardInterrupt:
            print("\n🛑 Trades consumer stopped")
        finally:
            c.close()

# State for IL Calculation (Symbol -> List of closes)
# Simple in-memory state. In prod, use Redis or DB.
symbol_state = {}

def update_il_state(symbol, close_price):
    if symbol not in symbol_state:
        symbol_state[symbol] = []
    
    history = symbol_state[symbol]
    history.append(close_price)
    
    # Keep last 60 minutes (1 hour)
    if len(history) > 60:
        history.pop(0)
        
    # Calculate Entry Price (Avg of window)
    entry_price = sum(history) / len(history)
    return entry_price

def consume_ohlcv():
    """Consumer for OHLCV candles -> ohlcv_1m table"""
    c = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'questdb-ohlcv-group',
        'auto.offset.reset': 'earliest'
    })
    c.subscribe(['ohlcv_1m'])
    
    conf = f'tcp::addr={QUESTDB_HOST}:{QUESTDB_PORT};'
    print(f"📊 OHLCV consumer started: {KAFKA_BROKER} -> {QUESTDB_HOST}:{QUESTDB_PORT}")
    
    with Sender.from_conf(conf) as sender:
        try:
            msg_count = 0
            while True:
                msg = c.poll(1.0)
                
                if msg is None:
                    sender.flush()
                    continue
                
                if msg.error():
                    print(f"OHLCV consumer error: {msg.error()}")
                    continue

                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    
                    # Parse window_end timestamp
                    ts_nanos = parse_iso8601_to_nanos(data.get('window_end'))
                    symbol = data.get('symbol_id', 'UNKNOWN')
                    close_price = float(data.get('close', 0.0))
                    
                    sender.row(
                        'ohlcv_1m',
                        symbols={
                            'symbol': symbol
                        },
                        columns={
                            'open': float(data.get('open', 0.0)),
                            'high': float(data.get('high', 0.0)),
                            'low': float(data.get('low', 0.0)),
                            'close': close_price,
                            'volume': float(data.get('volume', 0.0))
                        },
                        at=ts_nanos
                    )
                    
                    # 2. Calculate IL
                    entry_price = update_il_state(symbol, close_price)
                    if entry_price > 0:
                        ratio = close_price / entry_price
                        # IL Formula: 2 * sqrt(ratio) / (1 + ratio) - 1
                        il_score = (2 * (ratio ** 0.5) / (1 + ratio)) - 1
                        
                        # 3. Ingest IL Risk
                        sender.row(
                            'defi_risk',
                            symbols={'symbol': symbol},
                            columns={
                                'il_score': il_score,
                                'entry_price': entry_price,
                                'current_price': close_price
                            },
                            at=ts_nanos
                        )
                    
                    msg_count += 1
                    if msg_count % 100 == 0:
                        print(f"✅ OHLCV & Risk: Processed {msg_count} records")
                        sender.flush()

                except Exception as e:
                    print(f"⚠️ OHLCV serialization error: {e}, data: {msg.value()[:200]}")

        except KeyboardInterrupt:
            print("\n🛑 OHLCV consumer stopped")
        finally:
            c.close()

def consume_metrics():
    """Consumer for derived risk metrics -> market_risk table"""
    c = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'questdb-metrics-group',
        'auto.offset.reset': 'earliest'
    })
    c.subscribe(['metrics_derived'])
    
    conf = f'tcp::addr={QUESTDB_HOST}:{QUESTDB_PORT};'
    print(f"📈 Metrics consumer started: {KAFKA_BROKER} -> {QUESTDB_HOST}:{QUESTDB_PORT}")
    
    with Sender.from_conf(conf) as sender:
        try:
            msg_count = 0
            while True:
                msg = c.poll(1.0)
                
                if msg is None:
                    sender.flush()
                    continue
                
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    ts_nanos = parse_iso8601_to_nanos(data.get('window_end', datetime.utcnow().isoformat()))
                    
                    sender.row(
                        'market_risk',
                        symbols={
                            'symbol': data.get('symbol_id', 'UNKNOWN')
                        },
                        columns={
                            'volatility': float(data.get('volatility', 0.0)),
                            'liquidity': float(data.get('liquidity', 0.0)),
                            'rsi': float(data.get('rsi', 0.0)),
                            'cvar_95': float(data.get('cvar_95', 0.0))
                        },
                        at=ts_nanos
                    )
                    
                    msg_count += 1
                    if msg_count % 100 == 0:
                        print(f"✅ Metrics: Processed {msg_count} records")
                        sender.flush()

                except Exception as e:
                    print(f"⚠️ Metrics serialization error: {e}, data: {msg.value()[:200]}")

        except KeyboardInterrupt:
            print("\n🛑 Metrics consumer stopped")
        finally:
            c.close()


def run():
    """Start all consumer threads"""
    print("=" * 60)
    print("QuestDB Bridge - Multi-Topic Consumer")
    print("=" * 60)
    
    # Start trades consumer in separate thread
    trades_thread = threading.Thread(target=consume_trades, daemon=True)
    trades_thread.start()
    
    # Start metrics consumer in separate thread
    metrics_thread = threading.Thread(target=consume_metrics, daemon=True)
    metrics_thread.start()

    
    # Start OHLCV consumer in main thread (blocks)
    consume_ohlcv()

if __name__ == '__main__':
    run()
