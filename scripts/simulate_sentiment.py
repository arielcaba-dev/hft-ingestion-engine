import socket
import time
import random
import sys
from datetime import datetime

# QuestDB ILP Config
HOST = 'localhost'
PORT = 9009

def send_ilp(sock, line):
    try:
        sock.sendall((line + '\n').encode('utf-8'))
        # print(f"Sent: {line}")
    except Exception as e:
        print(f"Error sending ILP: {e}")

def generate_sentiment():
    symbols = ["BTC-USD", "ETH-USD", "SOL-USD"]
    sources = ["twitter", "reddit", "news", "telegram"]
    
    # Hype cycle simulation
    time_seed = time.time()
    
    data = []
    for symbol in symbols:
        # Base sentiment fluctuates
        base = 0.0
        if "BTC" in symbol:
            base = 0.5  # Bullish bias
        
        # Random noise
        noise = random.uniform(-0.5, 0.5)
        
        score = max(-1.0, min(1.0, base + noise))
        impact = random.uniform(0.0, 10.0)
        source = random.choice(sources)
        
        # ILP Format: table,symbol=... column=... timestamp
        # sentiment,symbol=BTC-USD,source=twitter sentiment_score=0.8,impact_score=9.5 timestamp_nanos
        
        timestamp = time.time_ns()
        line = f"sentiment,symbol={symbol},source={source} sentiment_score={score},impact_score={impact} {timestamp}"
        data.append(line)
        
    return data

def main():
    print(f"Connecting to QuestDB ILP at {HOST}:{PORT}...")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
    except Exception as e:
        print(f"Failed to connect: {e}")
        return

    print("Sending synthetic sentiment data (Ctrl+C to stop)...")
    try:
        while True:
            lines = generate_sentiment()
            for line in lines:
                send_ilp(sock, line)
            
            time.sleep(1.0) # 1 publish per second per symbol
            
            if random.random() < 0.1:
                print(f"Published {len(lines)} data points.")
                
    except KeyboardInterrupt:
        print("\nStopping simulation.")
    finally:
        sock.close()

if __name__ == "__main__":
    main()
