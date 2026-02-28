from db_mongo import get_db, ensure_indexes
import sys

def main():
    try:
        # 1. Get database connection
        db = get_db()
        
        # 2. Test connection with ping
        client = db.client
        client.admin.command("ping")
        print("[SUCCESS] MongoDB Atlas ping successful")

        # 3. Ensure indexes
        ensure_indexes(db)
        print("[SUCCESS] Indexes ensured on Atlas")

    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
