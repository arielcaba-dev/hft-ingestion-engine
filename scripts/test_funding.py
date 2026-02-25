import requests
import json
import time

GATEWAY_URL = "http://localhost:8080"
API_KEY = "test_key_123"

def test_funding_rest():
    print("\n[1] Testing REST Endpoint: /v1/derivatives/funding/BTC-USD")
    try:
        headers = {"X-API-KEY": API_KEY}
        response = requests.get(f"{GATEWAY_URL}/v1/derivatives/funding/BTC-USD", headers=headers, timeout=5)
        response.raise_for_status()
        data = response.json()
        
        print(f"Status: {response.status_code}")
        print(f"Record Count: {data.get('count')}")
        
        if data.get('count', 0) > 0:
            latest = data['funding_rates'][0]
            print("Latest Record:")
            print(json.dumps(latest, indent=2))
        else:
            print("❌ No data returned.")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed: {e}")

def test_funding_mcp():
    print("\n[2] Testing MCP Intent: 'What is the current funding rate and cost of carry for BTC-USD?'")
    try:
        headers = {
            "Content-Type": "application/json",
            "X-API-KEY": API_KEY
        }
        payload = {
            "query": "funding rate and cost of carry",
            "context": {"symbol": "BTC-USD"}
        }
        response = requests.post(f"{GATEWAY_URL}/v1/mcp", headers=headers, json=payload, timeout=5)
        response.raise_for_status()
        data = response.json()
        
        print(f"Status: {response.status_code}")
        print(f"Analysis Node: {data.get('analysis')}")
        print(f"Type: {data.get('type')}")
        
        if data.get('data'):
            latest = data['data'][0]
            print("\nLatest Evaluated Record:")
            print(f"  Timestamp:         {latest.get('timestamp')}")
            print(f"  Mark Price:        ${latest.get('mark_price'):,.2f}")
            print(f"  Raw Funding Rate:  {latest.get('funding_rate'):.6%}")
            print(f"  Annualized Carry:  {latest.get('annualized_carry_pct'):.2f}%")
        else:
            print("❌ No data returned.")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed: {e}")

if __name__ == "__main__":
    print(f"🧪 Starting Funding Rate Tests (Target: {GATEWAY_URL})")
    test_funding_rest()
    test_funding_mcp()
    print("\n✅ Verification complete.")
