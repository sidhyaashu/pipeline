import os
import time
import subprocess
import json
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 1. Configuration
TEST_DIR = "tests"
DATA_DIR = os.path.join(TEST_DIR, "data")
PEN_DIR = os.path.join(DATA_DIR, "penetration")
REPORT_FILE = os.path.join(TEST_DIR, "detailed_test_report.md")
CONSOLE_LOG = os.path.join(TEST_DIR, "output_console.txt")

# Initialize Console Log
with open(CONSOLE_LOG, "w", encoding="utf-8") as f:
    f.write(f"=== FULL TEST SUITE CONSOLE LOG ===\n")
    f.write(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

load_dotenv(os.path.join(TEST_DIR, ".env.test"))
DB_URL = os.getenv("DATABASE_URL")
engine = create_engine(DB_URL)

results = []

def log_to_console(text):
    print(text)
    with open(CONSOLE_LOG, "a", encoding="utf-8") as f:
        f.write(str(text) + "\n")

def run_step(name, command, description):
    log_to_console(f"\n" + "="*60)
    log_to_console(f"STEP: {name} | {description}")
    log_to_console(f"CMD: {command}")
    log_to_console("="*60)
    
    start = time.time()
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    duration = time.time() - start
    
    # Save raw logs
    with open(CONSOLE_LOG, "a", encoding="utf-8") as f:
        f.write("\n--- STDOUT ---\n")
        f.write(result.stdout)
        f.write("\n--- STDERR ---\n")
        f.write(result.stderr)
        f.write("\n" + "-"*60 + "\n")
    
    status = "SUCCESS" if result.returncode == 0 else "FAILED"
    results.append({
        "step": name,
        "description": description,
        "status": status,
        "duration": f"{duration:.2f}s",
        "error": result.stderr if status == "FAILED" else ""
    })
    
    if status == "FAILED":
        log_to_console(f"   FAIL: {name} failed (Code: {result.returncode})")
    else:
        log_to_console(f"   PASS: {name} completed in {duration:.2f}s")
    return result.returncode == 0

def generate_report():
    log_to_console(f"\n[REPORT] Generating consolidated report: {REPORT_FILE}")
    report = [
        "# Pipeline Robustness & Integrity Report",
        f"**Date**: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "\n## Execution Summary",
        "| Step | Description | Status | Duration |",
        "| :--- | :--- | :--- | :--- |"
    ]
    
    for r in results:
        report.append(f"| {r['step']} | {r['description']} | {r['status']} | {r['duration']} |")

    # DB Stats
    report.append("\n## Database Statistics")
    try:
        with engine.connect() as conn:
            # Table counts
            report.append("\n### Table Row Counts")
            report.append("| Table | Count |")
            report.append("| :--- | :--- |")
            tables = ["company_master", "ingestion_runs", "rejected_ingestion_rows", "daily_ingestion_summary"]
            for t in tables:
                try:
                    count = conn.execute(text(f'SELECT count(*) FROM "{t}"')).scalar()
                    report.append(f"| {t} | {count} |")
                except:
                    report.append(f"| {t} | MISSING |")

            # Ingestion Runs
            report.append("\n### Ingestion History (Last 10)")
            report.append("| Feed | Date | Status | Received | Rejected | Error |")
            report.append("| :--- | :--- | :--- | :--- | :--- | :--- |")
            history = conn.execute(text("""
                SELECT feed_name, requested_date, status, rows_received, rows_rejected, error_message 
                FROM ingestion_runs 
                ORDER BY finished_at DESC LIMIT 10
            """)).fetchall()
            for h in history:
                err = (h[5][:50] + "...") if h[5] else "-"
                report.append(f"| {h[0]} | {h[1]} | {h[2]} | {h[3]} | {h[4]} | {err} |")
    except Exception as e:
        report.append(f"\nFAIL: Could not fetch DB stats: {e}")

    report.append("\n## Data Penetration Results")
    report.append("Detailed analysis of how the system handled malformed inputs.")
    # (Optional: read specific ingestion_runs for Penetration_ feeds)
    try:
        with engine.connect() as conn:
            pen_runs = conn.execute(text("""
                SELECT feed_name, status, error_message 
                FROM ingestion_runs 
                WHERE feed_name LIKE 'Penetration_%'
                ORDER BY feed_name
            """)).fetchall()
            if pen_runs:
                report.append("| Test Case | Result | Observation |")
                report.append("| :--- | :--- | :--- |")
                for pr in pen_runs:
                    obs = pr[2] if pr[2] else "Handled Gracefully"
                    report.append(f"| {pr[0]} | {pr[1]} | {obs} |")
    except:
        pass

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(report))
    print("✨ Report generation complete.")

def main():
    # -2. Clean up previous runs to ensure fresh credentials
    run_step("CleanUp", "docker-compose -f tests/docker-compose.test.yml down -v", "Cleaning up old test containers and volumes")

    # -1. Start DB if not running
    run_step("StartDB", "docker-compose -f tests/docker-compose.test.yml up -d db_test", "Ensuring test database is running")
    time.sleep(5) # Give Postgres a moment to start initializing

    # 0. Generate penetration data
    run_step("DataGen", "python tests/generate_penetration_data.py", "Generating malformed test data")

    # 1. Wait for DB readiness on host
    log_to_console("\nWaiting for database to be ready on 127.0.0.1:5432...")
    db_ready = False
    for i in range(15):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                db_ready = True
                log_to_console("✅ Database is reachable.")
                break
        except Exception as e:
            log_to_console(f"   ({i+1}/15) Still waiting for DB... {str(e)[:50]}")
            time.sleep(3)
    
    if not db_ready:
        log_to_console("❌ CRITICAL: Database could not be reached on 127.0.0.1:5432. Skipping DB dependent tests.")
    else:
        # 1b. Reset & Init DB
        run_step("InitDB", "python tests/init_test_db.py", "Applying schemas and tracking tables")

        # 2. Fast ETL Test (loading from file)
        os.environ["DATA_DIR"] = DATA_DIR
        run_step("FastETL", "python etl.py", "Testing direct file loading (Fast ETL)")

    # 3. Penetration Test (Simulated API)
    # Monkeypatching logic is easier in a separate script or via Docker
    # We will use Docker simulation for this
    run_step("Penetration", "docker-compose -f tests/docker-compose.test.yml run --rm penetration_test", "Simulating API ingestion with malformed data")

    # 4. Manual API Test
    run_step("ManualAPI", "docker-compose -f tests/docker-compose.test.yml run --rm api_manual_test", "Testing manual single-feed ingestion")

    # 5. Backfill Test
    run_step("Backfill", "docker-compose -f tests/docker-compose.test.yml run --rm backfill_test", "Testing 7-day chronological recovery")

    # 6. Generate Report
    generate_report()
    log_to_console("\n" + "="*60)
    log_to_console("ALL TESTS COMPLETED. See tests/detailed_test_report.md and tests/output_console.txt")
    log_to_console("="*60)

if __name__ == "__main__":
    main()
