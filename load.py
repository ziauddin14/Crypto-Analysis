import logging
import os
from pymongo.errors import BulkWriteError

# --- Setup Logging ---
LOGS_DIR = "logs"
os.makedirs(LOGS_DIR, exist_ok=True)
log_filename = os.path.join(LOGS_DIR, "load.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def upsert_latest(db, docs):
    """
    Upserts transformed docs into 'crypto_market' collection (Latest Snapshot).
    """
    summary = {"matched": 0, "modified": 0, "upserted": 0, "total": len(docs)}
    
    logger.info(f"Upserting {len(docs)} docs into crypto_market...")
    
    for doc in docs:
        try:
            result = db.crypto_market.update_one(
                {"coin_id": doc["coin_id"]},
                {"$set": doc},
                upsert=True
            )
            
            summary["matched"] += result.matched_count
            summary["modified"] += result.modified_count
            if result.upserted_id:
                summary["upserted"] += 1
                
        except Exception as e:
            logger.error(f"Error upserting {doc.get('coin_id')}: {e}")
            
    logger.info(f"Upsert Summary: {summary}")
    return summary

def insert_history(db, docs):
    """
    Inserts transformed docs into 'crypto_market_history' for time-series analysis.
    """
    if not docs:
        return 0
        
    logger.info(f"Inserting {len(docs)} docs into crypto_market_history...")
    try:
        # unordered=False ensures we stop on error, ordered=False continues (we want false for resilience)
        result = db.crypto_market_history.insert_many(docs, ordered=False)
        inserted_count = len(result.inserted_ids)
        logger.info(f"Successfully inserted {inserted_count} history records.")
        return inserted_count
    except BulkWriteError as bwe:
        inserted_count = bwe.details.get("nInserted", 0)
        logger.warning(f"Bulk insert partially failed. Inserted: {inserted_count}. Errors: {len(bwe.details.get('writeErrors', []))}")
        return inserted_count
    except Exception as e:
        logger.error(f"Failed to insert history: {e}")
        return 0

if __name__ == "__main__":
    from db_mongo import get_db, ensure_indexes
    from extract import fetch_markets
    from transform import transform_markets
    
    try:
        db = get_db()
        ensure_indexes(db)
        
        # ETL sequence
        raw = fetch_markets(per_page=5)
        docs = transform_markets(raw)
        
        # Load sequence
        upsert_res = upsert_latest(db, docs)
        history_cnt = insert_history(db, docs)
        
        print("\n--- Load Phase Summary ---")
        print(f"Upsert Result: {upsert_res}")
        print(f"History Inserts: {history_cnt}")
        print(f"Current crypto_market total: {db.crypto_market.count_documents({})}")
        
    except Exception as e:
        print(f"Fatal error in load runner: {e}")
