#!/usr/bin/env python3
"""
Latency benchmark for WebSocket endpoints (DS mode and Standard mode).
Measures end-to-end latency from exchange timestamp to client reception.
"""
import asyncio
import websockets
import json
import time
from datetime import datetime
import statistics
import sys

# Configuration
import os

GATEWAY_HOST = os.getenv("GATEWAY_HOST", "localhost:8080")
DS_URI = f"ws://{GATEWAY_HOST}/v1/ws/ds?api_key=test_key_123"
STANDARD_URI = f"ws://{GATEWAY_HOST}/v1/ws?api_key=test_key_123"
SAMPLE_SIZE = 100  # Number of messages to sample for latency calculation

async def benchmark_ds_mode():
    """Benchmark DS mode (Protobuf) WebSocket latency."""
    print("=" * 60)
    print("DS MODE (Protobuf) LATENCY BENCHMARK")
    print("=" * 60)
    
    latencies = []
    async with websockets.connect(DS_URI) as websocket:
        print(f"Connected to: {DS_URI}")
        
        msg_count = 0
        while msg_count < SAMPLE_SIZE:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                receive_time = time.time()
                
                # DS mode sends binary Protobuf - we can't parse timestamps without protobuf
                # So we'll just measure message reception rate
                msg_count += 1
                
                if msg_count % 10 == 0:
                    print(f"Received {msg_count}/{SAMPLE_SIZE} messages...")
                    
            except asyncio.TimeoutError:
                print("Timeout waiting for message")
                break
        
        print(f"\n✓ DS Mode: Received {msg_count} messages")
        print(f"  Note: DS mode uses Protobuf encoding, full latency calculation requires parsing")
        return None

async def benchmark_standard_mode():
    """Benchmark Standard mode (JSON) WebSocket latency."""
    print("\n" + "=" * 60)
    print("STANDARD MODE (JSON) LATENCY BENCHMARK")
    print("=" * 60)
    
    latencies = []
    async with websockets.connect(STANDARD_URI) as websocket:
        print(f"Connected to: {STANDARD_URI}")
        
        # Subscribe to BTC-USD and ETH-USD
        sub_msg = {"action": "Subscribe", "symbols": ["BTC-USD", "ETH-USD"]}
        await websocket.send(json.dumps(sub_msg))
        
        # Wait for subscription confirmation
        confirm = await websocket.recv()
        print(f"Subscription: {confirm}")
        
        msg_count = 0
        while msg_count < SAMPLE_SIZE:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                receive_time = time.time() * 1000  # Convert to milliseconds
                
                data = json.loads(message)
                
                # Skip non-data messages
                if "status" in data:
                    continue
                
                # Parse exchange timestamp
                if "time_exchange" in data:
                    exchange_time_str = data["time_exchange"]
                    exchange_time = datetime.fromisoformat(exchange_time_str.replace('Z', '+00:00'))
                    exchange_time_ms = exchange_time.timestamp() * 1000
                    
                    # Calculate latency
                    latency = receive_time - exchange_time_ms
                    latencies.append(latency)
                    msg_count += 1
                    
                    if msg_count % 10 == 0:
                        print(f"Received {msg_count}/{SAMPLE_SIZE} messages...")
                    
            except asyncio.TimeoutError:
                print("Timeout waiting for message")
                break
            except Exception as e:
                print(f"Error parsing message: {e}")
                continue
        
        if latencies:
            print(f"\n✓ Standard Mode: Analyzed {len(latencies)} messages")
            print(f"\n📊 LATENCY STATISTICS (ms):")
            print(f"  • Min:     {min(latencies):.2f} ms")
            print(f"  • Max:     {max(latencies):.2f} ms")
            print(f"  • Mean:    {statistics.mean(latencies):.2f} ms")
            print(f"  • Median:  {statistics.median(latencies):.2f} ms")
            print(f"  • P95:     {statistics.quantiles(latencies, n=20)[18]:.2f} ms")
            print(f"  • P99:     {statistics.quantiles(latencies, n=100)[98]:.2f} ms")
            print(f"  • StdDev:  {statistics.stdev(latencies):.2f} ms")
            
            return {
                "min": min(latencies),
                "max": max(latencies),
                "mean": statistics.mean(latencies),
                "median": statistics.median(latencies),
                "p95": statistics.quantiles(latencies, n=20)[18],
                "p99": statistics.quantiles(latencies, n=100)[98],
                "stddev": statistics.stdev(latencies)
            }
        else:
            print("\n✗ No latency data collected")
            return None

async def main():
    print("\n🚀 WebSocket Latency Benchmark")
    print(f"Sample size: {SAMPLE_SIZE} messages per endpoint\n")
    
    # Benchmark DS mode
    await benchmark_ds_mode()
    
    # Benchmark Standard mode
    stats = await benchmark_standard_mode()
    
    print("\n" + "=" * 60)
    print("BENCHMARK COMPLETE")
    print("=" * 60)
    
    if stats:
        return 0
    else:
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
