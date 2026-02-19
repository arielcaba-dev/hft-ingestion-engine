import asyncio
import websockets
import os
import sys

# Ensure protobuf generated code can be imported
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import market_data_pb2

async def test_ds_proto():
    gateway_host = os.getenv("GATEWAY_HOST", "gateway:8080")
    # Enterprise Tier required for DS Mode (Tier 3)
    # Ensure test_key used has correct tier or use bootstrap_key
    uri = f"ws://{gateway_host}/v1/ws/ds?api_key=bootstrap_key"
    
    print(f"Connecting to {uri}...")
    try:
        async with websockets.connect(uri) as websocket:
            print("Connected!")
            
            # Wait for message
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=15.0)
                if isinstance(message, bytes):
                    print(f"Received {len(message)} bytes.")
                    packet = market_data_pb2.MarketDataPacket()
                    packet.ParseFromString(message)
                    
                    print(f"Symbol: {packet.symbol}")
                    print(f"Price: {packet.price}")
                    print(f"Quantity: {packet.quantity}")
                    print(f"Timestamp (legacy): {packet.timestamp}")
                    print(f"Time Exchange: {packet.time_exchange} micros")
                    print(f"Time Ingest: {packet.time_ingest} micros")
                    
                    if packet.time_exchange > 0 and packet.time_ingest > 0:
                        print("SUCCESS: Dual timestamps present.")
                    else:
                         print("FAILURE: Timestamps missing or zero.")
                         sys.exit(1)
                else:
                    print(f"Received unexpected text message: {message}")
                    sys.exit(1)
            except asyncio.TimeoutError:
                print("Timeout waiting for message.")
                sys.exit(1)
                
    except Exception as e:
        print(f"Connection failed: {e}")
        # Check if gateway is upgrading/available
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(test_ds_proto())
