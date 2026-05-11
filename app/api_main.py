import gc
from datetime import datetime
import time

from sqlalchemy import inspect

from app.accord_client import fetch_accord_feed
from app.config import (
    ACCORD_API_TOKEN, API_DATE, LOAD_ORDER, PRIMARY_KEYS, 
    ENABLE_IDEMPOTENCY, MAX_FEED_CONSECUTIVE_FAILURES, LOG_RAW_PAYLOAD
)
from app.db import build_engine, wait_for_db
from app.ingestion_log import (
    create_ingestion_tables, log_run, save_raw_payload, has_successful_run, 
    update_daily_summary, start_ingestion_run, finish_ingestion_run, get_last_successful_hash
)
from app.merge_service import process_dataframe, generate_payload_hash

from app.normalizer import apply_renames, payload_to_dataframe
from app.validation_service import validate_payload_df
from app.alert_service import send_alert
from app.utils import resolve_table_name

def run_incremental_for_feeds(
    feeds: list[str], 
    override_date: str = None, 
    force: bool = False,
    execution_context: str = 'manual',
    execution_window: str = 'daily'
) -> None:
    engine = build_engine()
    wait_for_db(engine)
    create_ingestion_tables(engine)

    date_ddmmyyyy = override_date or API_DATE.strip() or datetime.now().strftime("%d%m%Y")
    requested_date = datetime.strptime(date_ddmmyyyy, "%d%m%Y").date()

    print(f"\n🚀 Starting ingestion for date={date_ddmmyyyy} [context={execution_context}, window={execution_window}]")
    print(f"Feeds: {feeds}")

    feed_failures: dict[str, int] = {}

    for feed_name in feeds:
        # Atomic start check
        if ENABLE_IDEMPOTENCY and not force:
            allowed = start_ingestion_run(
                engine, feed_name, requested_date, execution_context, execution_window
            )
            if not allowed:
                print(f"\n⏭️ Skipping {feed_name} for {date_ddmmyyyy} (already successful in this context)")
                continue
        else:
            # Force or idempotency disabled: still register as STARTED to track attempt
            start_ingestion_run(engine, feed_name, requested_date, execution_context, execution_window)

        if feed_failures.get(feed_name, 0) >= MAX_FEED_CONSECUTIVE_FAILURES:
            print(f"\n🛑 Skipping {feed_name}: {MAX_FEED_CONSECUTIVE_FAILURES} consecutive failures reached.")
            finish_ingestion_run(
                engine, feed_name, requested_date, execution_context, execution_window,
                status="SKIPPED_CIRCUIT_BREAKER", error_message="Consecutive failures reached"
            )
            continue

        success = process_single_feed(
            engine=engine,
            feed_name=feed_name,
            date_ddmmyyyy=date_ddmmyyyy,
            requested_date=requested_date,
            execution_context=execution_context,
            execution_window=execution_window
        )
        if success:
            feed_failures[feed_name] = 0
        else:
            feed_failures[feed_name] = feed_failures.get(feed_name, 0) + 1

    update_daily_summary(engine, requested_date)

def process_single_feed(
    engine, 
    feed_name: str, 
    date_ddmmyyyy: str, 
    requested_date,
    execution_context: str,
    execution_window: str
) -> bool:
    print(f"\n🌐 Feed: {feed_name}")

    table_name = resolve_table_name(engine, feed_name)

    if not table_name:
        msg = f"No DB table found for feed={feed_name}"
        print(f"❌ {msg}")
        finish_ingestion_run(
            engine, feed_name, requested_date, execution_context, execution_window,
            status="TABLE_NOT_FOUND", error_message=msg
        )
        return False

    start_time = time.time()
    try:
        http_status, payload = fetch_feed(feed_name, date_ddmmyyyy)

        if http_status == 204:
            print("⏭️ No incremental data")
            duration = int(time.time() - start_time)
            finish_ingestion_run(
                engine, feed_name, requested_date, execution_context, execution_window,
                status="NO_CONTENT", http_status=http_status, duration_seconds=duration
            )
            return True

        if http_status in (403, 404):
            msg = f"API returned HTTP {http_status}"
            print(f"❌ {msg}")
            send_alert(f"API Error {http_status}", f"{feed_name} returned {http_status}")
            duration = int(time.time() - start_time)
            finish_ingestion_run(
                engine, feed_name, requested_date, execution_context, execution_window,
                status="API_ERROR", http_status=http_status, error_message=msg, duration_seconds=duration
            )
            return False

        # ======================================================
        # PAYLOAD HASH COMPARISON (Skip if data identical)
        # ======================================================
        current_hash = generate_payload_hash(payload)
        last_success_hash = get_last_successful_hash(engine, feed_name)
        
        if current_hash == last_success_hash:
            print("⏭️ Skipping merge: Payload identical to last successful run.")
            duration = int(time.time() - start_time)
            finish_ingestion_run(
                engine, feed_name, requested_date, execution_context, execution_window,
                status="SKIPPED_SAME_DATA", http_status=http_status, duration_seconds=duration,
                payload_hash=current_hash
            )
            return True

        if LOG_RAW_PAYLOAD:
            save_raw_payload(engine, feed_name, requested_date, payload)

        df = payload_to_dataframe(payload)
        del payload
        gc.collect()

        if df.empty:
            duration = int(time.time() - start_time)
            finish_ingestion_run(
                engine, feed_name, requested_date, execution_context, execution_window,
                status="EMPTY", http_status=http_status, duration_seconds=duration,
                payload_hash=current_hash
            )
            return True

        df = apply_renames(df, feed_name)

        pk_cols = PRIMARY_KEYS.get(table_name, [])
        val_result = validate_payload_df(df, table_name, pk_cols)

        for w in val_result["warnings"]:
            print(f"⚠️ Validation Warning: {w}")

        if not val_result["valid"]:
            error_str = "; ".join(val_result["errors"])
            print(f"❌ Validation Errors: {error_str}")
            send_alert("Validation Failed", f"{feed_name}: {error_str}")
            duration = int(time.time() - start_time)
            finish_ingestion_run(
                engine, feed_name, requested_date, execution_context, execution_window,
                status="FAILED", error_message=error_str, duration_seconds=duration,
                payload_hash=current_hash
            )
            return False

        upserted, deleted, rejected = process_dataframe(
            engine=engine,
            table_name=table_name,
            df=df,
            feed_name=feed_name,
            requested_date=requested_date,
        )

        del df
        gc.collect()

        duration = int(time.time() - start_time)
        finish_ingestion_run(
            engine=engine,
            feed_name=feed_name,
            requested_date=requested_date,
            execution_context=execution_context,
            execution_window=execution_window,
            status="SUCCESS",
            http_status=http_status,
            rows_received=len(df),
            rows_upserted=upserted,
            rows_deleted=deleted,
            rows_rejected=rejected,
            duration_seconds=duration,
            payload_hash=current_hash
        )

        print(f"✅ Success: received={len(df)}, upserted={upserted}, deleted={deleted}, rejected={rejected}")
        if rejected > 0:
            send_alert("Rows Rejected", f"{feed_name} had {rejected} rows rejected")

        return True

    except Exception as e:
        duration = int(time.time() - start_time)
        print(f"❌ Failed feed={feed_name}: {e}")
        send_alert("Feed Failure", f"{feed_name} failed: {str(e)}")
        finish_ingestion_run(
            engine, feed_name, requested_date, execution_context, execution_window,
            status="FAILED", error_message=str(e), duration_seconds=duration
        )
        return False

def fetch_feed(feed_name: str, date_ddmmyyyy: str):
    if not ACCORD_API_TOKEN:
        raise RuntimeError("ACCORD_API_TOKEN is required")

    return fetch_accord_feed(feed_name, date_ddmmyyyy, ACCORD_API_TOKEN)


def run_incremental() -> None:
    run_incremental_for_feeds(LOAD_ORDER)

def run_backfill_last_7_days() -> None:
    import datetime
    today = datetime.datetime.now().date()
    
    # fetch missing dates in chronological order (oldest first)
    for i in range(6, -1, -1):
        backfill_date = today - datetime.timedelta(days=i)
        date_ddmmyyyy = backfill_date.strftime("%d%m%Y")
        print(f"\n⏳ Running backfill for {date_ddmmyyyy}")
        try:
            run_incremental_for_feeds(
                LOAD_ORDER, 
                override_date=date_ddmmyyyy, 
                force=False,
                execution_context='backfill',
                execution_window='daily'
            )
        except Exception as e:
            send_alert("Backfill Failure", f"Failed for {date_ddmmyyyy}: {str(e)}")



if __name__ == "__main__":
    run_incremental()