import psycopg2
import time
from datetime import datetime
import os

# Configuration
RW_HOST = os.getenv('RW_HOST', 'localhost')
RW_PORT = os.getenv('RW_PORT', '4566') # Default RisingWave Postgres port
RW_USER = os.getenv('RW_USER', 'root')
RW_DB = os.getenv('RW_DB', 'dev')

def get_connection():
    try:
        conn = psycopg2.connect(
            host=RW_HOST,
            port=RW_PORT,
            user=RW_USER,
            database=RW_DB
        )
        return conn
    except Exception as e:
        print(f"Error connecting to RisingWave: {e}")
        return None

def query_latest_ohlcv(symbol):
    conn = get_connection()
    if not conn: return
    
    query = """
    SELECT window_start, open_price, high_price, low_price, close_price, volume
    FROM ohlcv_1m
    WHERE symbol_id = %s
    ORDER BY window_start DESC
    LIMIT 1;
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (symbol,))
        row = cur.fetchone()
        if row:
            print(f"Latest OHLCV for {symbol}:")
            print(f"Time: {row[0]}, Open: {row[1]}, High: {row[2]}, Low: {row[3]}, Close: {row[4]}, Vol: {row[5]}")
        else:
            print(f"No OHLCV data found for {symbol}")
    conn.close()

def query_vwap(symbol):
    conn = get_connection()
    if not conn: return
    
    query = """
    SELECT window_start, window_end, vwap
    FROM vwap_rolling
    WHERE symbol_id = %s
    ORDER BY window_end DESC
    LIMIT 1;
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (symbol,))
        row = cur.fetchone()
        if row:
            print(f"Latest VWAP for {symbol}:")
            print(f"Window: {row[0]} - {row[1]}, VWAP: {row[2]}")
        else:
            print(f"No VWAP data found for {symbol}")
    conn.close()

if __name__ == "__main__":
    test_symbol = "BTC-USD"
    print(f"Querying metrics for {test_symbol}...")
    query_latest_ohlcv(test_symbol)
    query_vwap(test_symbol)
