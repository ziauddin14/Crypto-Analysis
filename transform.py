import logging
import os
from datetime import datetime, timezone

# --- Setup Logging ---
LOGS_DIR = "logs"
os.makedirs(LOGS_DIR, exist_ok=True)
log_filename = os.path.join(LOGS_DIR, "transform.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def transform_markets(raw_list):
    """
    Transforms raw CoinGecko market data into a clean, schema-aligned format.
    """
    transformed_docs = []
    extracted_at = datetime.now(timezone.utc)
    
    logger.info(f"Starting transformation for {len(raw_list)} items.")
    
    for item in raw_list:
        coin_id = item.get("id")
        
        # Rule: If coin_id missing, skip that row (log warning)
        if not coin_id:
            logger.warning(f"Skipping row with missing coin_id: {item}")
            continue
            
        try:
            # Data Cleaning & Safety Casting
            current_price = float(item.get("current_price") or 0.0)
            market_cap = float(item.get("market_cap") or 0.0)
            total_volume = float(item.get("total_volume") or 0.0)
            
            # price_change_24h = raw price_change_percentage_24h (as requested)
            # Rule: Missing price_change_percentage_24h -> 0
            price_change_24h = float(item.get("price_change_percentage_24h") or 0.0)
            
            market_cap_rank = item.get("market_cap_rank")
            if market_cap_rank is not None:
                market_cap_rank = int(market_cap_rank)
            
            # Rule: symbol upper-case
            symbol = str(item.get("symbol", "")).upper()
            
            # New Field: volatility_score = abs(price_change_24h) * total_volume
            volatility_score = abs(price_change_24h) * total_volume

            doc = {
                "coin_id": coin_id,
                "symbol": symbol,
                "name": item.get("name"),
                "current_price": current_price,
                "market_cap": market_cap,
                "total_volume": total_volume,
                "price_change_24h": price_change_24h,
                "market_cap_rank": market_cap_rank,
                "volatility_score": volatility_score,
                "extracted_at": extracted_at
            }
            
            transformed_docs.append(doc)
            
        except (ValueError, TypeError) as e:
            logger.error(f"Error transforming coin {coin_id}: {e}")
            continue

    logger.info(f"Transformation complete. Output: {len(transformed_docs)} docs.")
    return transformed_docs

if __name__ == "__main__":
    from extract import fetch_markets
    
    try:
        raw_data = fetch_markets(per_page=5) # Small batch for sanity
        docs = transform_markets(raw_data)
        
        if docs:
            print("\n--- Transformation Sanity Check ---")
            print(f"Total Docs: {len(docs)}")
            print(f"Keys: {list(docs[0].keys())}")
            print(f"Sample (BTC): {docs[0]['name']} | Volatility: {docs[0]['volatility_score']:.2f}")
            print(f"Extracted At: {docs[0]['extracted_at']}")
            
    except Exception as e:
        print(f"Fatal error in transform runner: {e}")
