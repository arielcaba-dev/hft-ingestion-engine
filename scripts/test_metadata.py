import urllib.request
import json
import os
import sys
import time

GATEWAY_URL = os.getenv('GATEWAY_URL', "http://localhost:8080")
API_KEY = "bootstrap_key"

def get_json(endpoint):
    url = f"{GATEWAY_URL}{endpoint}"
    print(f"\n[TEST] GET {url}")
    req = urllib.request.Request(url)
    req.add_header("X-API-KEY", API_KEY)
    
    try:
        with urllib.request.urlopen(req) as response:
            if response.status == 200:
                data = json.loads(response.read().decode())
                print(f"SUCCESS: Received {len(data)} items.")
                return data
            else:
                print(f"FAILURE: Expected status 200, got {response.status}")
                sys.exit(1)
    except Exception as e:
        print(f"FAILURE: Error requesting {url}: {e}")
        sys.exit(1)

def test_metadata():
    # 1. Exchanges
    exchanges = get_json("/v1/exchanges")
    if not any(e['id'] == 'binance' for e in exchanges):
        print("FAILURE: Binance exchange not found.")
        sys.exit(1)
    
    # 2. Assets
    assets = get_json("/v1/assets")
    if not any(a['symbol'] == 'BTC' for a in assets):
        print("FAILURE: BTC asset not found.")
        sys.exit(1)

    # 3. Symbols
    symbols = get_json("/v1/symbols")
    btc_usd = next((s for s in symbols if s['normalized_symbol'] == 'BTC-USD'), None)
    if btc_usd:
        print(f"SUCCESS: Found BTC-USD. Price Precision: {btc_usd['price_precision']}, Size Precision: {btc_usd['size_precision']}")
        if btc_usd['price_precision'] != 0.01:
             print(f"FAILURE: Expected price precision 0.01, got {btc_usd['price_precision']}")
             sys.exit(1)
    else:
        print("FAILURE: BTC-USD symbol not found.")
        sys.exit(1)

if __name__ == "__main__":
    test_metadata()
    print("\n✅ ALL METADATA TESTS PASSED")
