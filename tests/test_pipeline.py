import os
import subprocess
import time
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load test environment
if os.path.exists(".env.test"):
    load_dotenv(".env.test")
elif os.path.exists("tests/.env.test"):
    load_dotenv("tests/.env.test")

DB_URL = os.getenv("DATABASE_URL")

def run_command(command, description):
    print(f"\n🚀 {description}...")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"❌ Error during {description}:")
        print(result.stderr)
        return False
    print(f"✅ {description} completed.")
    # print(result.stdout)
    return True

def main():
    print("=== STARTING FULL PIPELINE TEST ===")

    # 1. Start Docker Database
    compose_path = "tests/docker-compose.test.yml" if os.path.exists("tests/docker-compose.test.yml") else "docker-compose.test.yml"
    if not run_command(f"docker-compose -f {compose_path} up -d", "Starting local Postgres"):
        return

    # 2. Wait for DB to be ready
    print("⏳ Waiting for database to initialize...")
    time.sleep(10) # Give it some time

    engine = create_engine(DB_URL)
    connected = False
    for i in range(10):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                connected = True
                print("✅ Database is ready.")
                break
        except Exception:
            print(f"⏳ Retry {i+1}/10...")
            time.sleep(2)
    
    if not connected:
        print("❌ Could not connect to test database.")
        return

    # 3. Initialize Schema and Load Company Master using etl.py
    # We only want to load Company_master for the test to be fast
    os.environ["DATA_DIR"] = "tests/data" if os.path.exists("tests/data") else "./data"
    os.environ["ETL_LOG_LEVEL"] = "INFO"
    # We can't easily restrict etl.py to one file without changing its LOAD_ORDER or deleting other files
    # but since Company_master.txt is the only one in /data, it should be fine.
    if not run_command("python etl.py", "Loading Company Master via etl.py"):
        return

    # 4. Run app migrations (tracking tables)
    if not run_command("python -m app.migrate_db", "Running application migrations"):
        return

    # 5. Run a sample ingestion from the API with SIMULATION
    print("\n🌐 Running simulated API ingestion for 'Company_master'...")
    try:
        import json
        import app.api_main
        import app.accord_client

        # --- SIMULATOR MOCK ---
        def mock_fetch_accord_feed(filename: str, date_ddmmyyyy: str, token: str):
            print(f"   [SIMULATOR] Mocking fetch for {filename}...")
            data_dir = "tests/data" if os.path.exists("tests/data") else "./data"
            local_path = os.path.join(data_dir, f"{filename}.txt")
            if not os.path.exists(local_path):
                return 404, None
            
            with open(local_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return 200, data

        # Monkeypatch BOTH the client and the main reference
        app.accord_client.fetch_accord_feed = mock_fetch_accord_feed
        app.api_main.fetch_accord_feed = mock_fetch_accord_feed
        # ----------------------

        from app.api_main import run_incremental_for_feeds
        run_incremental_for_feeds(["Company_master"], execution_context="simulated_test")
        print("✅ Simulated API ingestion completed.")
    except Exception as e:
        print(f"❌ API ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        return

    # 6. Verify Data
    print("\n📊 VERIFICATION 📊")
    with engine.connect() as conn:
        # Check company_master
        count = conn.execute(text("SELECT count(*) FROM company_master")).scalar()
        print(f"🔹 Rows in company_master: {count}")
        
        # Check ingestion_runs
        runs = conn.execute(text("SELECT feed_name, status, execution_context FROM ingestion_runs")).fetchall()
        print(f"🔹 Ingestion Runs recorded:")
        for run in runs:
            print(f"   - {run[0]}: {run[1]} ({run[2]})")

    print("\n=== TEST COMPLETED SUCCESSFULLY ===")
    compose_path = "tests/docker-compose.test.yml" if os.path.exists("tests/docker-compose.test.yml") else "docker-compose.test.yml"
    print(f"To clean up, run: docker-compose -f {compose_path} down")

if __name__ == "__main__":
    main()
