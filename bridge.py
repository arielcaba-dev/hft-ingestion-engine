"""
QuestDB Bridge - Kafka to QuestDB ILP Writer
Consumes market data from Redpanda and writes to QuestDB using InfluxDB Line Protocol.
"""
import json
import os
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from questdb.ingress import Sender, IngressError, TimestampNanos

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'redpanda:9092')
QUESTDB_HOST = os.getenv('QUESTDB_HOST', 'questdb')
QUESTDB_PORT = int(os.getenv('QUESTDB_PORT', 9009))
TOPIC = os.getenv('TOPIC', 'market_data_raw')

def run():
    # 1. Setup Kafka Consumer
    c = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'questdb-bridge-group',
        'auto.offset.reset': 'earliest'
    })
    c.subscribe([TOPIC])

    # 2. Setup QuestDB Sender
    # Using specific configuration for high-throughput
    conf = f'tcp::addr={QUESTDB_HOST}:{QUESTDB_PORT};'
    
    print(f"🚀 Bridge started: {KAFKA_BROKER} -> {QUESTDB_HOST}:{QUESTDB_PORT} (Topic: {TOPIC})")

    with Sender.from_conf(conf) as sender:
        try:
            while True:
                msg = c.poll(1.0)  # Poll with 1s timeout

                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                # 3. Parse & Transform
                # Assuming JSON: {"symbol_id": "BTC-USD", "price": 50000.0, "quantity": 1.5, "time_exchange": 1678900000}
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    
                    # Convert timestamp to nanoseconds for QuestDB
                    # Assuming time_exchange is in seconds (Unix epoch)
                    ts_nanos = int(data.get('time_exchange', 0) * 1_000_000_000)

                    # 4. Buffer Row (ILP)
                    sender.row(
                        'trades',  # Table Name
                        symbols={
                            'symbol': data.get('symbol_id', 'UNKNOWN')
                        },
                        columns={
                            'price': float(data.get('price', 0.0)),
                            'quantity': float(data.get('quantity', 0.0))
                        },
                        at=ts_nanos
                    )

                    # Flush regularly (QuestDB client auto-flushes based on buffer size, 
                    # but explicit flush ensures low latency for low-volume streams)
                    sender.flush()
                    
                    print(f"✅ Wrote: {data.get('symbol_id')} @ {data.get('price')}")

                except Exception as e:
                    print(f"⚠️ Serialization Error: {e}, Payload: {msg.value()}")

        except KeyboardInterrupt:
            print("\n🛑 Bridge stopped")
        finally:
            c.close()

if __name__ == '__main__':
    run()
