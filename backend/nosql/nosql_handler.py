from pymongo import MongoClient
import pandas as pd

def get_mongo_client():
    mongo_uri = "mongodb://localhost:27017/"
    client = MongoClient(mongo_uri)
    return client

def execute_mongo_query(db_name, collection_name, query_pipeline):
    client = get_mongo_client()
    db = client[db_name]
    collection = db[collection_name]
    results = list(collection.aggregate(query_pipeline))
    client.close()
    return pd.DataFrame(results)

def list_collections(db_name="dsci351"):
    client = get_mongo_client()
    db = client[db_name]
    collections = db.list_collection_names()
    client.close()
    return pd.DataFrame({"Collections": collections})

# This limit only gets applied during schema exploration
def sample_documents(collection_name, db_name="dsci351", limit=3):
    client = get_mongo_client()
    db = client[db_name]
    collection = db[collection_name]
    documents = list(collection.find().limit(limit))
    client.close()
    return pd.DataFrame(documents)

