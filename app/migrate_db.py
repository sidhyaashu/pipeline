import logging
from sqlalchemy import text
from app.db import build_engine, wait_for_db
from app.ingestion_log import create_ingestion_tables

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("Migrator")

def run_migrations():
    """
    Main entry point for database migrations.
    This script ensures that the ingestion tracking tables and other 
    necessary schema components are created and updated.
    """
    logger.info("🚀 Starting database migration process...")
    
    engine = build_engine()
    
    # 1. Wait for DB to be ready
    try:
        wait_for_db(engine, retries=10, delay=2)
    except RuntimeError as e:
        logger.error(f"❌ Database not ready: {e}")
        return False

    # 2. Apply Ingestion Tracking Tables (sql/ingestion_tables.sql)
    logger.info("📄 Applying ingestion tracking schema...")
    try:
        create_ingestion_tables(engine)
        logger.info("✅ Ingestion tracking tables created/updated successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to apply ingestion tracking schema: {e}")
        return False

    # 3. Verify connection and critical tables
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT count(*) FROM ingestion_runs"))
            count = result.scalar()
            logger.info(f"📊 Verified: 'ingestion_runs' table exists (Rows: {count})")
    except Exception as e:
        logger.error(f"⚠️ Verification check failed: {e}")

    logger.info("🎊 Migration process completed successfully.")
    return True

if __name__ == "__main__":
    run_migrations()
