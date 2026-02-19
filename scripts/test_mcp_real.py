import requests
import os
import sys
import json

GATEWAY_URL = os.getenv('GATEWAY_URL', "http://localhost:8080")
API_KEY = "bootstrap_key"

def test_mcp_query():
    print("Testing MCP Query (Real Data)...")
    url = f"{GATEWAY_URL}/v1/mcp"
    headers = {"X-API-KEY": API_KEY, "Content-Type": "application/json"}
    
    # Query for price/trades
    payload = {
        "query": "Show me latest price for BTC-USD",
        "context": {"symbol": "BTC-USD"}
    }
    
    try:
        resp = requests.post(url, headers=headers, json=payload)
        print(f"Status: {resp.status_code}")
        
        if resp.status_code == 200:
            data = resp.json()
            print(json.dumps(data, indent=2))
            
            if data.get("count", 0) > 0:
                print("SUCCESS: Received real data from QuestDB.")
            else:
                print("WARNING: Received empty data (Table might be empty or query failed).")
                # We inserted trades in previous test, so it should not be empty.
                if "mock_data" in str(data):
                     print("FAILURE: Received mock data! Resolver not updated.")
                     sys.exit(1)
        else:
            print(f"FAILURE: Unexpected status {resp.status_code}")
            print(resp.text)
            sys.exit(1)
            
    except Exception as e:
        print(f"Request failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    test_mcp_query()
