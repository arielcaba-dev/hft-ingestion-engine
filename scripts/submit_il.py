import requests
import os
import sys

ARROYO_URL = os.getenv('ARROYO_URL', "http://localhost:5115")

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sql_path = os.path.join(script_dir, "..", "arroyo/pipeline_il.sql")
    with open(sql_path, "r") as f:
        sql = f.read()
    
    payload = {
        "name": "defi_risk_il",
        "parallelism": 1,
        "udfs": [], 
        "query": sql
    }
    
    print(f"Submitting to {ARROYO_URL}...")
    resp = requests.post(f"{ARROYO_URL}/api/v1/pipelines", json=payload)
    print(f"Status: {resp.status_code}")
    print(f"Response: {resp.text}")
    
    if resp.status_code != 200:
        sys.exit(1)

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
