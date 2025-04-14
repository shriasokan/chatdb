from pymongo import MongoClient

def get_mongo_client():
    # Temporary hardcoded connection (replace later with .env)
    mongo_uri = "mongodb://localhost:27017/"
    client = MongoClient(mongo_uri)
    return client

def execute_mongo_query(db_name, collection_name, query_pipeline):
    client = get_mongo_client()
    db = client[db_name]
    collection = db[collection_name]

    results = list(collection.aggregate(query_pipeline))
    client.close()

    return results