import logging
import os
import json
from datetime import datetime, timezone
from extract import fetch_markets
from transform import transform_markets
from load import upsert_latest, insert_history
from db_mongo import get_db, ensure_indexes

# --- Setup Logging ---
LOGS_DIR = "logs"
os.makedirs(LOGS_DIR, exist_ok=True)
log_filename = os.path.join(LOGS_DIR, "etl_pipeline.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_etl(save_history: bool = True) -> dict:
    """
    Orchestrates the full ETL process: Extract -> Transform -> Load (Upsert + History).
    """
    start_time = datetime.now(timezone.utc)
    logger.info(">>> Starting ETL Pipeline Orchestration...")
    
    summary = {
        "fetched": 0,
        "transformed": 0,
        "upsert": {},
        "history_inserted": 0,
        "ran_at": start_time.isoformat(),
        "status": "partial"
    }
    
    try:
        # 1. Extract
        raw_data = fetch_markets(per_page=20)
        summary["fetched"] = len(raw_data) if raw_data else 0
        
        if not raw_data:
            logger.error("Extraction failed: No data retrieved.")
            summary["status"] = "failed"
            return summary
            
        # 2. Transform
        docs = transform_markets(raw_data)
        summary["transformed"] = len(docs)
        
        if not docs:
            logger.error("Transformation failed: No valid docs produced.")
            summary["status"] = "failed"
            return summary
            
        # 3. Load (Connect & Ensure Indexes)
        db = get_db()
        ensure_indexes(db)
        
        # 3.1 Upsert Latest Snapshot
        upsert_res = upsert_latest(db, docs)
        summary["upsert"] = upsert_res
        
        # 3.2 Insert History (Optional)
        if save_history:
            history_cnt = insert_history(db, docs)
            summary["history_inserted"] = history_cnt
            
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        summary["status"] = "success"
        summary["duration_seconds"] = duration
        
        logger.info(f"<<< ETL Pipeline completed successfully in {duration:.2f}s.")
        return summary
        
    except Exception as e:
        logger.error(f"ETL Pipeline crashed: {e}")
        summary["status"] = "error"
        summary["error_message"] = str(e)
        return summary

if __name__ == "__main__":
    # Internal runner for verification
    result = run_etl(save_history=True)
    print("\n--- ETL Pipeline Summary ---")
    print(json.dumps(result, indent=2))
