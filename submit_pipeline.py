
import json
import requests
import sys

# Read SQL with proper error handling
try:
    with open("arroyo/risk_pipeline_simple.sql", "r") as f:
        sql = f.read()
except FileNotFoundError:
    print("SQL file not found")
    sys.exit(1)

# Debug: Print SQL length
print(f"SQL Length: {len(sql)}")

# Construct payload for pipeline creation
payload = {
    "name": "ingest_debug",
    "parallelism": 1,
    "udfs": [],
    "sql": sql  # (unused if query is set below)
}
payload["query"] = sql 

try:
    with open("arroyo/debug_pipeline.sql", "r") as f:
        sql = f.read()
    payload["query"] = sql
except FileNotFoundError:
    print("Debug SQL file not found")

try:
    print("Submitting to Arroyo API...")
    resp = requests.post("http://localhost:5115/api/v1/pipelines", json=payload)
    print(f"Status: {resp.status_code}")
    print(f"Response: {resp.text}")
    
    if resp.status_code != 200:
        # Try preview endpoint debugging info
        print("\nTrying preview/compile endpoint...")
        preview_payload = {"query": sql, "udfs": []}
        resp = requests.post("http://localhost:5115/api/v1/pipelines/preview", json=preview_payload)
        print(f"Preview Status: {resp.status_code}") 
        print(f"Preview Response: {resp.text}")

except Exception as e:
    print(f"Error: {e}")
