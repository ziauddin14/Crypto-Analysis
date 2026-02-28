import requests
import json
import os
import time
import logging
from datetime import datetime

# --- Configuration & Constants ---
BASE_URL = "https://api.coingecko.com/api/v3"
DATA_RAW_DIR = "data_raw"
LOGS_DIR = "logs"

# Ensure directories exist
os.makedirs(DATA_RAW_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# --- Logging Setup ---
log_filename = os.path.join(LOGS_DIR, "extract.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def fetch_markets(vs_currency="usd", per_page=20, page=1):
    """
    Fetches market data from CoinGecko API with retry logic and rate limit handling.
    """
    endpoint = f"{BASE_URL}/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": page,
        "sparkline": "false",
        "price_change_percentage": "24h"
    }
    
    max_retries = 3
    backoff_factors = [1, 2, 4]
    
    logger.info(f"Starting fetch: {vs_currency}, page {page}, per_page {per_page}")
    
    for attempt in range(max_retries):
        try:
            response = requests.get(endpoint, params=params, timeout=10)
            
            # Handle rate limiting (HTTP 429)
            if response.status_code == 429:
                wait_time = (attempt + 1) * 30 # CoinGecko rate limits are strict, wait longer
                logger.warning(f"Rate limit hit (429). Attempt {attempt + 1}/{max_retries}. Sleeping {wait_time}s...")
                time.sleep(wait_time)
                continue
                
            response.raise_for_status()
            data = response.json()
            
            logger.info(f"Successfully fetched {len(data)} coins.")
            
            # Save raw data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"markets_{timestamp}.json"
            filepath = os.path.join(DATA_RAW_DIR, filename)
            
            with open(filepath, "w") as f:
                json.dump(data, f, indent=4)
            
            logger.info(f"Raw data saved to: {filepath}")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                sleep_time = backoff_factors[attempt]
                logger.info(f"Retrying in {sleep_time}s...")
                time.sleep(sleep_time)
            else:
                logger.critical("Max retries reached. Extraction failed.")
                raise

    return None

if __name__ == "__main__":
    try:
        markets = fetch_markets()
        if markets and len(markets) > 0:
            print(f"\n--- Sanity Check ---")
            print(f"Total Coins Fetched: {len(markets)}")
            print(f"Keys in first coin: {list(markets[0].keys())}")
            print(f"First Coin Name: {markets[0].get('name')} ({markets[0].get('symbol').upper()})")
            print(f"Current Price: ${markets[0].get('current_price')}")
    except Exception as e:
        print(f"Fatal execution error: {e}")
