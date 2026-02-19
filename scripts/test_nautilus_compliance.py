import requests
import socket
import time
import psycopg2
import os
import sys

GATEWAY_URL = os.getenv('GATEWAY_URL', "http://localhost:8080")
QUESTDB_ILP_HOST = os.getenv('QUESTDB_HOST', "localhost")
QUESTDB_ILP_PORT = int(os.getenv('QUESTDB_ILP_PORT', 9009))
PG_HOST = os.getenv('PG_HOST', "localhost")
PG_PORT = int(os.getenv('PG_PORT', 5432))
PG_USER = "arroyo"
PG_PASS = "secret_password_placeholder"
PG_DB = "arroyo"
API_KEY = "bootstrap_key"

def insert_metadata():
    print("Inserting metadata for NAUTILUS-TEST...")
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, dbname=PG_DB
        )
        cur = conn.cursor()
        cur.execute("INSERT INTO symbols (id, exchange_id, base_asset_id, quote_asset_id, symbol, normalized_symbol, price_precision, size_precision) VALUES ('nautilus:TEST', 'binance', 'BTC', 'USD', 'NAUTILUS_TEST', 'NAUTILUS-TEST', 0.01, 0.0001) ON CONFLICT (exchange_id, symbol) DO NOTHING;")
        conn.commit()
        cur.close()
        conn.close()
        print("Metadata inserted.")
    except Exception as e:
        print(f"Postgres Error: {e}")
        sys.exit(1)

def insert_trades_ilp():
    print("Inserting 10,005 trades via ILP (with one bad price)...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((QUESTDB_ILP_HOST, QUESTDB_ILP_PORT))
    except Exception as e:
        print(f"Failed to connect to QuestDB ILP: {e}")
        sys.exit(1)

    # Insert 10000 valid trades
    for i in range(10000):
        # symbol,price=100.00,quantity=1.0 timestamp
        line = f"trades,symbol=NAUTILUS-TEST price=100.00,quantity=1.0 {time.time_ns()}\n"
        sock.sendall(line.encode())
    
    # Insert 1 BAD trade
    # Price 100.005 violates 0.01 precision
    line = f"trades,symbol=NAUTILUS-TEST price=100.005,quantity=1.0 {time.time_ns()}\n"
    sock.sendall(line.encode())
    
    sock.close()
    time.sleep(1) # Allow commit

def test_historical_request():
    print("Requesting historical data (Offload path)...")
    url = f"{GATEWAY_URL}/v1/trades/historical?symbol=NAUTILUS-TEST&limit=20000"
    headers = {"X-API-KEY": API_KEY}
    
    try:
        resp = requests.get(url, headers=headers)
        print(f"Response Status: {resp.status_code}")
        
        if resp.status_code == 500:
            print("SUCCESS: Received 500 Internal Server Error as expected.")
            # Ideally check error message if exposed, but 500 confirms failure in processing
        elif resp.status_code == 303:
            print("FAILURE: Received 303 Redirect. Precision guard failed to catch bad data.")
            sys.exit(1)
        else:
            print(f"FAILURE: Unexpected status {resp.status_code}")
            sys.exit(1)
            
    except Exception as e:
        print(f"Request failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    insert_metadata()
    # Wait for metadata cache update? Currently 1 hour TTL.
    # We might need to restart gateway to pick up new metadata if get_symbols cached empty list?
    # But get_symbol_metadata in historical.rs queries DB directly?
    # Yes, I implemented query_as using state.pool in historical.rs.
    # So no cache issue for precision check itself.
    
    insert_trades_ilp()
    test_historical_request()
