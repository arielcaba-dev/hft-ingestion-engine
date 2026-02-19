import asyncio
import websockets
import json
import os

async def test_websocket():
    gateway_host = os.getenv("GATEWAY_HOST", "localhost:8080")
    uri = f"ws://{gateway_host}/v1/ws/ds?api_key=test_key_123"
    async with websockets.connect(uri) as websocket:
        print(f"Connected to DS Mode: {uri}")
        
        # DS mode streams immediately, no subscription needed for this demo
        
        msg_count = 0
        while msg_count < 5:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                if isinstance(message, bytes):
                    print(f"Received Binary Message: {len(message)} bytes | Hex: {message.hex()[:20]}...")
                else:
                    print(f"Received Text: {message}")
                msg_count += 1
            except asyncio.TimeoutError:
                print("Timeout waiting for message")
                break
        print(f"Test finished: Received {msg_count} messages")

if __name__ == "__main__":
    asyncio.run(test_websocket())
