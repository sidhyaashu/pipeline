import os
from sqlalchemy import create_engine, text
from app.db import build_engine

def get_troubleshooting_report():
    """
    Generates a consolidated report of all ingestion failures and handling patterns.
    """
    engine = build_engine()
    
    report = []
    report.append("=" * 60)
    report.append("📋 CONSOLIDATED INGESTION DIAGNOSTICS REPORT")
    report.append("=" * 60)

    with engine.connect() as conn:
        # 1. Overall Status Breakdown
        report.append("\n📊 OVERALL RUN STATUS (Last 100 runs):")
        status_query = text("""
            SELECT status, count(*) 
            FROM ingestion_runs 
            GROUP BY status 
            ORDER BY count DESC
        """)
        for row in conn.execute(status_query):
            report.append(f"   - {row[0]:<20}: {row[1]}")

        # 2. Top Failure Reasons (The "False" cases)
        report.append("\n❌ TOP FAILURE REASONS (Most Recent First):")
        fail_query = text("""
            SELECT feed_name, status, error_message, requested_date
            FROM ingestion_runs 
            WHERE status NOT IN ('SUCCESS', 'NO_CONTENT', 'EMPTY', 'SKIPPED_SAME_DATA', 'STARTED')
            ORDER BY finished_at DESC
            LIMIT 10
        """)
        for row in conn.execute(fail_query):
            report.append(f"   🚩 [{row[0]}] {row[1]}: {row[2] or 'No error message'} (Date: {row[3]})")

        # 3. Data Integrity Issues (Rejected Rows / Mass Deletes)
        report.append("\n⚠️ DATA INTEGRITY WARNINGS:")
        warning_query = text("""
            SELECT feed_name, rows_received, rows_rejected, rows_deleted, requested_date
            FROM ingestion_runs
            WHERE (rows_rejected > 0 OR (rows_deleted > (rows_received * 0.4) AND rows_received > 50))
            ORDER BY finished_at DESC
            LIMIT 5
        """)
        for row in conn.execute(warning_query):
            msg = f"   - [{row[0]}] {row[4]}: Rejected={row[2]}, Deleted={row[3]} of {row[1]} total"
            report.append(msg)

        # 4. Identification of "Missing" things
        report.append("\n🕵️ POTENTIAL GAPS (Missing Feeds for today):")
        # Get list of feeds that haven't run for today
        gap_query = text("""
            WITH all_feeds AS (
                SELECT unnest(string_to_array(:load_order, ',')) as feed_name
            )
            SELECT a.feed_name
            FROM all_feeds a
            LEFT JOIN ingestion_runs i ON a.feed_name = i.feed_name AND i.requested_date = CURRENT_DATE
            WHERE i.feed_name IS NULL
        """)
        from app.config import LOAD_ORDER
        gaps = conn.execute(gap_query, {"load_order": ",".join(LOAD_ORDER)}).fetchall()
        if gaps:
            report.append(f"   - Missing {len(gaps)} feeds for today: {', '.join([g[0] for f in gaps[:5]])}...")
        else:
            report.append("   - ✅ All feeds have been attempted for today.")

    report.append("\n" + "=" * 60)
    return "\n".join(report)

if __name__ == "__main__":
    print(get_troubleshooting_report())
