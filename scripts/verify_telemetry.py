import requests
import time
import sys

def check_endpoint(url, metric_name):
    print(f"Checking {url} for {metric_name}...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        if metric_name in response.text:
            print(f"✅ Found {metric_name}")
            return True
        else:
            print(f"❌ Metric {metric_name} NOT found")
            return False
    except Exception as e:
        print(f"❌ Failed to query {url}: {e}")
        return False

def main():
    endpoints = [
        ("http://localhost:9003/metrics", "ingestion_latency_seconds"),
        ("http://localhost:9002/metrics", "http_requests_total"),
        ("http://localhost:9091/-/healthy", "Prometheus Server is Healthy") # Healthy check
    ]

    failed = False
    for url, metric in endpoints:
        if not check_endpoint(url, metric):
            failed = True
    
    if failed:
        sys.exit(1)
    else:
        print("🚀 All telemetry endpoints verified!")
        sys.exit(0)

if __name__ == "__main__":
    main()
