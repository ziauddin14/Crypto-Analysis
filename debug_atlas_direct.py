from pymongo import MongoClient
import sys

# Testing standard (non-srv) connection string
direct_uri = "mongodb://zu37216_db_user:ziauddin1425@ac-2p30pzf-shard-00-00.t90xa2j.mongodb.net:27017,ac-2p30pzf-shard-00-01.t90xa2j.mongodb.net:27017,ac-2p30pzf-shard-00-02.t90xa2j.mongodb.net:27017/?replicaSet=atlas-t90xa2j-shard-0&authSource=admin&tls=true"

print("Trying direct (non-SRV) connection...")
try:
    client = MongoClient(direct_uri, serverSelectionTimeoutMS=5000)
    print("Pinging...")
    client.admin.command('ping')
    print("[SUCCESS] Direct ping OK")
except Exception as e:
    print(f"[ERROR] Direct ping failed: {e}")
