import os
import sys
import time
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Ensure the parent directory (project root) is in sys.path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from etl import create_tables
from app.ingestion_log import create_ingestion_tables
from app.config import SCHEMA_DIR

def init_db():
    print("\n" + "="*60)
    print("🛠️  INITIALIZING TEST DATABASE")
    print("="*60)
    
    if os.path.exists(".env.test"):
        load_dotenv(".env.test")
    elif os.path.exists("tests/.env.test"):
        load_dotenv("tests/.env.test")
        
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    
    # 1. Wait for DB to be really ready
    print("⏳ Waiting for database connection...")
    retries = 10
    while retries > 0:
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                break
        except Exception:
            retries -= 1
            time.sleep(2)
    
    # 2. Create Tracking Tables (ingestion_runs, etc.)
    print("📊 Creating ingestion tracking tables...")
    try:
        create_ingestion_tables(engine)
        print("   ✅ Tracking tables ready.")
    except Exception as e:
        print(f"   ⚠️  Tracking tables notice: {e}")

    # 3. Create Data Tables from Schemas
    print(f"🏗️  Creating data tables from {SCHEMA_DIR}...")
    try:
        tables_created, step_pass, step_fail = create_tables(engine, SCHEMA_DIR)
        if step_fail > 0:
            print(f"   ❌ Schema application finished with {step_fail} failures.")
            exit(1)
        print(f"   ✅ Data schema applied ({step_pass} tables).")
    except Exception as e:
        print(f"   ❌ Error applying schema: {e}")
        exit(1)

    print("\n" + "="*60)
    print("✨ DATABASE INITIALIZATION COMPLETE")
    print("="*60)

if __name__ == "__main__":
    init_db()
