import time
import requests
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

QUESTDB_HOST = os.getenv("QUESTDB_HOST", "localhost")
QUESTDB_PORT = os.getenv("QUESTDB_UI_PORT", "9000")
URL = f"http://{QUESTDB_HOST}:{QUESTDB_PORT}/exec"
CLEAN_INTERVAL = int(os.getenv("CLEAN_INTERVAL_SECONDS", 3600))  # Hourly by default
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", 1))

def get_partitioned_tables():
    query = "SELECT table_name, designatedTimestamp FROM tables() WHERE partitionBy != 'NONE' and designatedTimestamp is not null"
    try:
        resp = requests.get(URL, params={"query": query})
        resp.raise_for_status()
        data = resp.json()
        return data.get("dataset", [])
    except Exception as e:
        logging.error(f"Failed to fetch tables: {e}")
        return []

def clean_old_data():
    tables = get_partitioned_tables()
    for table_name, ts_col in tables:
        # Drop partitions older than RETENTION_DAYS
        drop_query = f"ALTER TABLE {table_name} DROP PARTITION WHERE {ts_col} < dateadd('d', -{RETENTION_DAYS}, now())"
        try:
            logging.info(f"Cleaning {table_name} partitions older than {RETENTION_DAYS} day(s)...")
            resp = requests.get(URL, params={"query": drop_query})
            if resp.status_code == 200:
                logging.info(f"✅ Successfully checked/cleaned {table_name}")
            else:
                logging.warning(f"⚠️ Failed to clean {table_name}: {resp.text}")
        except Exception as e:
            logging.error(f"Error cleaning {table_name}: {e}")

if __name__ == "__main__":
    logging.info(f"Starting Data Retention Worker. Interval: {CLEAN_INTERVAL}s, Target: QuestDB HTTP API ({URL})")
    while True:
        clean_old_data()
        logging.info(f"Sleeping for {CLEAN_INTERVAL} seconds...")
        time.sleep(CLEAN_INTERVAL)
