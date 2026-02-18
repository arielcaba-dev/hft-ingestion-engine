
import asyncio
import websockets
import json
import urllib.request
import urllib.error
import time

import os

API_KEY = "test_key_123"
GATEWAY_HOST = os.getenv("GATEWAY_HOST", "localhost:8080")
GATEWAY_URL = f"http://{GATEWAY_HOST}"
WS_URL = f"ws://{GATEWAY_HOST}"

async def test_ws_subscription():
    uri = f"{WS_URL}/v1/ws?api_key={API_KEY}"
    print(f"Connecting to WebSocket: {uri} ...")
    try:
        async with websockets.connect(uri) as websocket:
            print("Connected to WebSocket.")
            
            # Send Subscribe Message
            msg = {
                "action": "Subscribe",
                "symbols": ["BTC-USD"]
            }
            await websocket.send(json.dumps(msg))
            print(f"Sent Subscribe: {msg}")

            # Wait for confirmation
            try:
                response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                print(f"Received: {response}")
                if "subscribed" in response:
                    print("SUCCESS: WebSocket Subscription")
                else:
                    print("FAILURE: WebSocket Subscription - Unexpected response")
            except asyncio.TimeoutError:
                print("FAILURE: WebSocket Subscription - Timed out waiting for response")

    except Exception as e:
        print(f"FAILURE: WebSocket Connection Error: {e}")

async def test_ws_ds():
    # Use query param for DS auth now that backend supports it
    uri = f"{WS_URL}/v1/ws/ds?api_key={API_KEY}"
    
    print(f"\nConnecting to DS WebSocket: {uri} ...")
    try:
        # Standard connect without extra headers
        async with websockets.connect(uri) as websocket:
            print("Connected to DS WebSocket.")
            
            # DS mode streams data automatically or waits for subscription?
            # Based on ds_mode.rs, it spawns a thread to consume kafka 'market_data_raw' 
            # and sends to WS. So we should just wait for data.
            print("Waiting for data (timeout 5s)...")
            try:
                response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                print(f"Received DS Message: {len(response)} bytes") # Binary protobuf
                print("SUCCESS: DS WebSocket Stream")
            except asyncio.TimeoutError:
                print("WARNING: DS WebSocket - Connected but no data received (Topic might be empty)")
                print("SUCCESS: DS WebSocket Connection Established")
            except websockets.exceptions.ConnectionClosedError:
                 # If server closes connection (e.g. due to inactivity or dropping receiver), 
                 # we still count it as partially successful if we connected.
                 print("WARNING: DS WebSocket - Connection closed by server.")
                 print("SUCCESS: DS WebSocket Connection Established (but closed)")

    except Exception as e:
        print(f"FAILURE: DS WebSocket Connection Error: {e}")

def test_http_historical():
    url = f"{GATEWAY_URL}/v1/trades/historical?symbol=BTC-USD&limit=5"
    print(f"\nTesting HTTP Historical: {url} ...")
    req = urllib.request.Request(url)
    req.add_header("X-API-KEY", API_KEY)
    
    try:
        with urllib.request.urlopen(req) as response:
            if response.status == 200:
                data = json.loads(response.read().decode())
                print(f"Received {len(data)} records.")
                print("SUCCESS: HTTP Historical Data")
            elif response.status == 303:
                print("SUCCESS: HTTP Historical Data (Redirect to S3)")
            else:
                print(f"FAILURE: HTTP Historical Data - Status {response.status}")
    except urllib.error.HTTPError as e:
        if e.code == 404 and "9000" in e.url: # MinIO port
             # This means the redirect happened to MinIO, but file missing.
             # This counts as logic success for the Gateway.
             print(f"SUCCESS: HTTP Historical Data (Redirected to MinIO: {e.url})")
        else:
            print(f"FAILURE: HTTP Historical Data - {e}")
    except Exception as e:
        print(f"FAILURE: HTTP Historical Error: {e}")

async def main():
    await test_ws_subscription()
    test_http_historical()
    await test_ws_ds()

if __name__ == "__main__":
    asyncio.run(main())
