import requests
import os

ARROYO_URL = os.getenv('ARROYO_URL', "http://localhost:5115")

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sql_path = os.path.join(script_dir, "..", "arroyo/pipeline_questdb.sql")
    with open(sql_path, "r") as f:
        sql = f.read()
    
    payload = {
        "name": "baseline_questdb_test",
        "parallelism": 1,
        "udfs": [],
        "query": sql
    }
    
    print(f"Submitting to {ARROYO_URL}...")
    resp = requests.post(f"{ARROYO_URL}/api/v1/pipelines", json=payload)
    print(f"Status: {resp.status_code}")
    print(f"Response: {resp.text}")
except Exception as e:
    print(f"Error: {e}")
