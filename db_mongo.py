import os
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING, DESCENDING
import logging

import certifi

# Load environment variables from .env
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "crypto_analytics"

def get_db():
    """
    Initializes and returns the MongoDB database object.
    """
    try:
        # Using 10s timeout for Atlas. Letting it use system CA store on Windows.
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        db = client[DB_NAME]
        return db
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def ensure_indexes(db):
    """
    Ensures that the required indexes are created for performance and data integrity.
    """
    try:
        # Collection: crypto_market
        # Unique index on coin_id to prevent duplicate entries for the same coin
        db.crypto_market.create_index([("coin_id", ASCENDING)], unique=True)
        logger.info("Unique index created on crypto_market: coin_id")

        # Collection: crypto_market_history
        # Index on extracted_at for efficient time-series queries
        db.crypto_market_history.create_index([("extracted_at", DESCENDING)])
        logger.info("Index created on crypto_market_history: extracted_at")

        # Compound index on (coin_id, extracted_at) for efficient history lookups per coin
        db.crypto_market_history.create_index([("coin_id", ASCENDING), ("extracted_at", DESCENDING)])
        logger.info("Compound index created on crypto_market_history: (coin_id, extracted_at)")

    except Exception as e:
        logger.error(f"Error while creating indexes: {e}")
        raise
