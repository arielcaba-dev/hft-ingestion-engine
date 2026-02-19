import urllib.request
import json
import os
import sys

GATEWAY_URL = os.getenv('GATEWAY_URL', "http://localhost:8080")
API_KEY = "bootstrap_key"

def test_historical_small():
    url = f"{GATEWAY_URL}/v1/trades/historical?symbol=BTC-USD&limit=5"
    print(f"\n[TEST] Historical Small (JSON) Request: {url}")
    
    req = urllib.request.Request(url)
    req.add_header("X-API-KEY", API_KEY)
    
    try:
        with urllib.request.urlopen(req) as response:
            if response.status == 200:
                data = json.loads(response.read().decode())
                print(f"SUCCESS: Received {len(data)} records in JSON.")
                # Verify schema
                if len(data) > 0:
                    item = data[0]
                    required_keys = {"symbol", "price", "quantity", "timestamp"}
                    if all(k in item for k in required_keys):
                        print("SUCCESS: JSON Schema verified.")
                    else:
                        print(f"FAILURE: Missing keys in JSON schema. Got: {list(item.keys())}")
                        sys.exit(1)
            else:
                print(f"FAILURE: Expected status 200, got {response.status}")
                sys.exit(1)
    except Exception as e:
        print(f"FAILURE: Historical Small Error: {e}")
        sys.exit(1)

def test_historical_large():
    # No limit = large request (if total count > 10k)
    url = f"{GATEWAY_URL}/v1/trades/historical?symbol=BTC-USD"
    print(f"\n[TEST] Historical Large (S3 Redirect) Request: {url}")
    
    # We use a custom handler to NOT follow redirects automatically to verify 303
    class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
        def http_error_303(self, req, fp, code, msg, headers):
            return fp
    
    opener = urllib.request.build_opener(NoRedirectHandler)
    req = urllib.request.Request(url)
    req.add_header("X-API-KEY", API_KEY)
    
    try:
        with opener.open(req) as response:
            if response.status == 303:
                location = response.headers.get("Location")
                print(f"SUCCESS: Received 303 See Other.")
                print(f"Redirect Location: {location}")
                if "minio" in location and ".parquet" in location and "X-Amz-Signature" in location:
                    print("SUCCESS: Presigned Parquet URL verified.")
                    return location
                else:
                    print("FAILURE: Location header does not look like a presigned Parquet URL.")
                    sys.exit(1)
            else:
                print(f"FAILURE: Expected status 303, got {response.status}")
                sys.exit(1)
    except Exception as e:
        print(f"FAILURE: Historical Large Error: {e}")
        sys.exit(1)

def test_historical_cache(original_location):
    url = f"{GATEWAY_URL}/v1/trades/historical?symbol=BTC-USD"
    print(f"\n[TEST] Historical Cache Verification: {url}")
    
    class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
        def http_error_303(self, req, fp, code, msg, headers):
            return fp
    
    opener = urllib.request.build_opener(NoRedirectHandler)
    req = urllib.request.Request(url)
    req.add_header("X-API-KEY", API_KEY)
    
    try:
        with opener.open(req) as response:
            location = response.headers.get("Location")
            if location == original_location:
                print("SUCCESS: Cache hit! Location URL matches exactly.")
            else:
                print("FAILURE: Cache miss or URL mismatch.")
                print(f"Original: {original_location}")
                print(f"New: {location}")
                sys.exit(1)
    except Exception as e:
        print(f"FAILURE: Cache test error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    test_historical_small()
    location = test_historical_large()
    if location:
        test_historical_cache(location)
    print("\n✅ ALL HISTORICAL TESTS PASSED")
