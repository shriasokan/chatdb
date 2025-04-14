from backend.nosql.nosql_handler import execute_mongo_query
from backend.sql.sql_handler import execute_sql_query

def handle_query(nl_query, db_type):
    # Later this comes from NLP parser
    if db_type == "sql":
        sql_query = "SELECT * FROM test_table LIMIT 5;"
        return execute_sql_query(sql_query)
    
    elif db_type == "nosql":
        mongo_pipeline = [
            {"$match": {}}, # No filter for now
            {"$limit": 5}
        ]
        return execute_mongo_query("dsci351", "test_collection", mongo_pipeline)