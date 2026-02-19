import json
import time
import requests
import random
from confluent_kafka import Producer
from datetime import datetime, timezone

import os

# Configuration
REDPANDA_BROKERS = os.getenv('REDPANDA_BROKERS', "localhost:19092")
QUESTDB_URL = os.getenv('QUESTDB_URL', "http://localhost:9000/exec")
TOPIC = "market_data_raw"

# Initialize Producer
conf = {'bootstrap.servers': REDPANDA_BROKERS}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")

def generate_market_data(symbol="BTC-USD", event_count=100):
    print(f"Generating {event_count} trades for {symbol}...")
    
    price = 50000.0
    
    start_time = datetime.now(timezone.utc).timestamp()
    
    for i in range(event_count):
        # Random walk
        change = random.uniform(-10.0, 10.0)
        price += change
        quantity = random.uniform(0.01, 2.0)
        
        # Advance time by 0.5s per event to span time
        current_ts = start_time + (i * 0.5)
        # ISOfromat
        timestamp = datetime.fromtimestamp(current_ts, timezone.utc).isoformat()
        
        event = {
            "symbol_id": symbol,
            "price": round(price, 2),
            "quantity": round(quantity, 4),
            "time_exchange": timestamp,
            "time_ingest": timestamp,
            "is_snapshot": False,
            "sequence": i
        }
        
        producer.produce(TOPIC, json.dumps(event).encode('utf-8'), callback=delivery_report)
        
        if i % 10 == 0:
            producer.poll(0)
            
    producer.flush()
    print("Data generation complete.")

def check_questdb_metrics(symbol="BTC-USD"):
    print(f"\nChecking QuestDB for metrics on {symbol}...")
    
    # Query OHLCV
    ohlcv_query = f"SELECT * FROM ohlcv_1m WHERE symbol='{symbol}' ORDER BY timestamp DESC LIMIT 5"
    resp = requests.get(QUESTDB_URL, params={"query": ohlcv_query})
    
    if resp.status_code == 200:
        data = resp.json()
        if data.get('count', 0) > 0:
            print("✅ OHLCV Data Found:")
            for row in data['dataset']:
                print(row)
        else:
            print("❌ No OHLCV data found yet.")
    else:
        print(f"❌ OHLCV Query Failed: {resp.text}")

    # Query Risk Metrics
    risk_query = f"SELECT * FROM market_risk WHERE symbol='{symbol}' ORDER BY timestamp DESC LIMIT 5"
    resp = requests.get(QUESTDB_URL, params={"query": risk_query})
    
    if resp.status_code == 200:
        data = resp.json()
        if data.get('count', 0) > 0:
            print("✅ Risk Metrics Found:")
            columns = [c['name'] for c in data['columns']]
            if 'cvar_95' in columns:
                print("   Found 'cvar_95' column.")
            else:
                print("   ⚠️ 'cvar_95' column NOT found.")
                
            for row in data['dataset']:
                print(row)
        else:
            print(f"❌ No Risk metrics found yet for {symbol}.")
    else:
        print(f"❌ Risk Query Failed: {resp.text}")

def check_kafka_ohlcv():
    print(f"\nChecking Kafka topic 'ohlcv_1m' for generated candles...")
    from confluent_kafka import Consumer, KafkaError
    
    c = Consumer({
        'bootstrap.servers': REDPANDA_BROKERS,
        'group.id': 'verifier-group',
        'auto.offset.reset': 'earliest'
    })
    
    c.subscribe(['metrics_derived'])
    
    start_time = time.time()
    msg_count = 0
    while time.time() - start_time < 30: # Wait up to 30s (window is 1m, might take time)
        msg = c.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
            
        print(f"✅ Received OHLCV Candle: {msg.value().decode('utf-8')}")
        msg_count += 1
        if msg_count >= 1: # Just need one to prove it works
            c.close()
            return

    c.close()
    print("❌ No OHLCV messages received in 30s.")

if __name__ == "__main__":
    # Generate data (enough to trigger a 1m window)
    # Arroyo triggers window at watermark pass.
    # We need timestamps spanning > 1 minute.
    print("Generating 1 minute of data...")
    generate_market_data(event_count=200) 
    
    print("\nWaiting for pipeline processing...")
    # Parallel check or just wait? Window is 1m.
    # We generated instantaneous data with "now".
    # We need to simulate time passing or just wait?
    # Actually generate_market_data uses "now". 
    # If we run it, it sends data for "current time".
    # The window will close when watermark advances past window end.
    # Watermark is `time - 5s`.
    # So we need to send data, wait 1m, or send data with future timestamps?
    # Better to just wait.
    
    check_kafka_ohlcv()
    
    # Check QuestDB
    print("\nWaiting for QuestDB ingestion...")
    import time
    time.sleep(5)
    check_questdb_metrics()
