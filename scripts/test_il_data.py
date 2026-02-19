import requests
import time
import json

import os

QUESTDB_QUERY_URL = os.getenv("QUESTDB_QUERY_URL", "http://localhost:9000/exec")

def query_questdb(sql):
    try:
        response = requests.get(QUESTDB_QUERY_URL, params={'query': sql})
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error querying QuestDB: {response.status_code} {response.text}")
            return None
    except Exception as e:
        print(f"Exception querying QuestDB: {e}")
        return None

def test_il_data():
    print("Testing DeFi Risk (IL) ingestion...")
    
    # 1. Check if table exists and has data
    sql = "SELECT * FROM defi_risk ORDER BY timestamp DESC LIMIT 5"
    result = query_questdb(sql)
    
    if result and result.get('dataset'):
        print("\n✅ Data found in 'defi_risk':")
        columns = [c['name'] for c in result['columns']]
        print(f"Columns: {columns}")
        for row in result['dataset']:
            print(row)
            
        # Verify values
        # Check if il_score is reasonable (usually near 0 for stable, negative for IL)
        # IL is always <= 0.
        latest_row = result['dataset'][0]
        il_score_idx = next((i for i, c in enumerate(columns) if c == 'il_score'), -1)
        if il_score_idx != -1:
            il_score = latest_row[il_score_idx]
            print(f"\nLatest IL Score: {il_score}")
        else:
            print("❌ il_score column not found")
    else:
        print("\n❌ No data found in 'defi_risk' table yet.")
        
    # Check ohlcv_1m functionality too
    sql_ohlcv = "SELECT * FROM ohlcv_1m ORDER BY timestamp DESC LIMIT 1"
    result_ohlcv = query_questdb(sql_ohlcv)
    if result_ohlcv and result_ohlcv.get('dataset'):
        print("\n✅ Data found in 'ohlcv_1m'")
    else:
        print("\n❌ No data found in 'ohlcv_1m'")

if __name__ == "__main__":
    # Wait a bit for bridge to process
    print("Waiting 10s for bridge to process data...")
    time.sleep(10)
    test_il_data()
