import os
import certifi
import traceback
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()
uri = os.getenv("MONGO_URI")
print(f"Connecting to: {uri[:20]}...")

try:
    # Explicitly trying with/without certificates
    client = MongoClient(uri, serverSelectionTimeoutMS=10000, tlsCAFile=certifi.where())
    print("Pinging Atlas...")
    client.admin.command('ping')
    print("[SUCCESS] Ping OK")
except Exception:
    print("[CRITICAL] Failed with certifi, trying with tlsAllowInvalidCertificates=True...")
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=10000, tlsAllowInvalidCertificates=True)
        client.admin.command('ping')
        print("[SUCCESS] Ping OK (Certificates ignored)")
    except Exception as e:
        print(f"[FATAL] Final attempt failed: {e}")
        traceback.print_exc()
