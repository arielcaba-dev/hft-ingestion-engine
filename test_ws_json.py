import asyncio
import websockets
import json
import os

async def test_json_websocket():
    gateway_host = os.getenv("GATEWAY_HOST", "localhost:8080")
    uri = f"ws://{gateway_host}/v1/ws?api_key=test_key_123"
    async with websockets.connect(uri) as websocket:
        print(f"Connected to Standard WS: {uri}")
        
        # Subscribe to BTCUSDT and ETHUSDT
        sub_msg = {
            "action": "Subscribe",
            "symbols": ["BTC-USD", "ETH-USD"]
        }
        await websocket.send(json.dumps(sub_msg))
        print(f"Sent subscription: {sub_msg}")

        msg_count = 0
        while msg_count < 5:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                print(f"Received: {message}")
                
                # Check if it's data or subscription confirmation
                data = json.loads(message)
                if "s" in data: # Ingestion data has "s" (symbol)
                    msg_count += 1
                elif "status" in data:
                    print(f"Status update: {data}")
                    
            except asyncio.TimeoutError:
                print("Timeout waiting for message")
                break
        print(f"Test finished: Received {msg_count} data messages")

if __name__ == "__main__":
    asyncio.run(test_json_websocket())
