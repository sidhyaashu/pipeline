import os
import json
import time
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import app.api_main
import app.accord_client
from app.config import LOAD_ORDER

# 1. Load test environment
# Try loading from current dir, then from tests/ dir
if os.path.exists(".env.test"):
    load_dotenv(".env.test")
elif os.path.exists("tests/.env.test"):
    load_dotenv("tests/.env.test")

DB_URL = os.getenv("DATABASE_URL")
DATA_DIR = os.getenv("DATA_DIR", "./tests/data")

# 2. Mock Function
def mock_fetch_accord_feed(filename: str, date_ddmmyyyy: str, token: str):
    """
    Simulates the API by reading local .txt files from the data directory.
    """
    # Try different case variations if needed, but usually filename matches feed_name
    local_path = os.path.join(DATA_DIR, f"{filename}.txt")
    
    if not os.path.exists(local_path):
        # Optional: try lowercase if exact match fails
        local_path = os.path.join(DATA_DIR, f"{filename.lower()}.txt")
        if not os.path.exists(local_path):
            return 404, None
    
    try:
        with open(local_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return 200, data
    except Exception as e:
        print(f"   [SIMULATOR] Error reading {local_path}: {e}")
        return 500, None

# 3. Apply Monkeypatch
app.accord_client.fetch_accord_feed = mock_fetch_accord_feed
app.api_main.fetch_accord_feed = mock_fetch_accord_feed

def run_full_simulation():
    print("\n" + "="*60)
    print("🚀 STARTING FULL PIPELINE SIMULATION SERVICE")
    print("="*60)
    
    engine = create_engine(DB_URL)
    
    print(f"📂 Scanning data directory: {DATA_DIR}")
    
    # Identify which files we have
    available_files = [f.replace(".txt", "") for f in os.listdir(DATA_DIR) if f.endswith(".txt")]
    print(f"📦 Found data files for: {available_files}")

    # We follow LOAD_ORDER to ensure parent records (like Company_master) are processed first
    feeds_to_run = [f for f in LOAD_ORDER if f in available_files]
    
    # If a file exists but isn't in LOAD_ORDER, we add it to the end
    extra_feeds = [f for f in available_files if f not in LOAD_ORDER]
    all_simulation_feeds = feeds_to_run + extra_feeds

    print(f"📋 Execution Plan: {' -> '.join(all_simulation_feeds)}")

    for feed_name in all_simulation_feeds:
        print(f"\n▶️ SIMULATING FEED: {feed_name}")
        try:
            # 1. Preview the data
            local_path = os.path.join(DATA_DIR, f"{feed_name}.txt")
            if not os.path.exists(local_path):
                local_path = os.path.join(DATA_DIR, f"{feed_name.lower()}.txt")
            
            with open(local_path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
                rows = raw_data.get("Table", [])
                if rows:
                    cols = list(rows[0].keys())
                    print(f"   📊 Incoming Columns ({len(cols)}): {', '.join(cols[:10])}{'...' if len(cols) > 10 else ''}")
                    print(f"   📝 Data Preview (First 5 rows):")
                    for i, row in enumerate(rows[:5]):
                        # Print first 5 values of each row
                        vals = list(row.values())
                        print(f"      Row {i+1}: {vals[:5]}...")
                else:
                    print("   ⚠️  File exists but contains no data in 'Table' key.")

            # 2. Run the ingestion
            from app.api_main import run_incremental_for_feeds
            start_time = time.time()
            run_incremental_for_feeds([feed_name], execution_context="full_simulation")
            duration = time.time() - start_time
            
            # Simple verification query
            from app.utils import resolve_table_name
            table_name = resolve_table_name(engine, feed_name)
            if table_name:
                with engine.connect() as conn:
                    count = conn.execute(text(f'SELECT count(*) FROM "{table_name}"')).scalar()
                    print(f"✅ [{feed_name}] -> Table \"{table_name}\" now has {count} total rows. (Took {duration:.2f}s)")
            
        except Exception as e:
            print(f"❌ [{feed_name}] Failed: {e}")

    print("\n" + "="*60)
    print("🎊 FULL SIMULATION COMPLETED")
    print("="*60)

    # 4. Generate Diagnostics Report
    try:
        from app.diagnostics import get_troubleshooting_report
        print("\n" + get_troubleshooting_report())
    except Exception as e:
        print(f"\n⚠️  Could not generate diagnostics report: {e}")

if __name__ == "__main__":
    run_full_simulation()
