"""
Create QuestDB tables for Derivatives data (Open Interest & Liquidations).
Run once to initialize the schema.
"""
import urllib.request
import urllib.parse

QUESTDB_URL = "http://localhost:9000/exec"

TABLES = [
    """
    CREATE TABLE IF NOT EXISTS open_interest (
        timestamp TIMESTAMP,
        symbol SYMBOL,
        exchange SYMBOL,
        oi_value DOUBLE,
        notional_value DOUBLE
    ) TIMESTAMP(timestamp) PARTITION BY DAY WAL;
    """,
    """
    CREATE TABLE IF NOT EXISTS liquidations (
        timestamp TIMESTAMP,
        symbol SYMBOL,
        exchange SYMBOL,
        side SYMBOL,
        price DOUBLE,
        quantity DOUBLE
    ) TIMESTAMP(timestamp) PARTITION BY DAY WAL;
    """,
    """
    CREATE TABLE IF NOT EXISTS funding_rates (
        timestamp TIMESTAMP,
        symbol SYMBOL,
        exchange SYMBOL,
        funding_rate DOUBLE,
        mark_price DOUBLE
    ) TIMESTAMP(timestamp) PARTITION BY DAY WAL;
    """,
]

def create_tables():
    for sql in TABLES:
        sql_clean = " ".join(sql.split())
        url = f"{QUESTDB_URL}?query={urllib.parse.quote(sql_clean)}"
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req) as resp:
                print(f"✅ Executed: {sql_clean[:60]}...")
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    create_tables()
    print("🚀 Derivatives tables ready.")
