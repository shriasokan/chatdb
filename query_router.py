from backend.nosql.nosql_handler import (
    execute_mongo_query,
    list_collections,
    sample_documents
)
from backend.sql.sql_handler import (
    execute_sql_query, 
    execute_modification_sql
)
from backend.sql.schema_handler import (
    list_tables, 
    describe_table, 
    sample_rows
)
from backend.nlp.parser import parse_nl_query

def handle_query(nl_query: str, db_type: str, table=None):
    parsed = parse_nl_query(nl_query, db_type, table)
    if "error" in parsed:
        return f"Error parsing query: {parsed['error']}"
    
    # SQL schema exploration
    if db_type == "sql":
        if parsed.get("schema_explore") == "list_tables":
            return list_tables()

        if parsed.get("schema_explore") == "describe_table":
            return describe_table(table or parsed.get("table"))

        if parsed.get("schema_explore") == "sample_rows":
            return sample_rows(table or parsed.get("table"))
    
    # NoSQL schema exploration
    elif db_type == "nosql":
        if parsed.get("schema_explore") == "list_collections":
            return list_collections()
        elif parsed.get("schema_explore") == "sample_documents":
            return sample_documents(parsed.get("collection") or table)
        
        # Data modification
        mongo_op_map = {
            "insertOne": "insert_one",
            "insertMany": "insert_many",
            "updateOne": "update_one",
            "deleteOne": "delete_one"
        }
        for mod_op in mongo_op_map:
            if mod_op in parsed:
                from pymongo import MongoClient
                client = MongoClient("mongodb://localhost:27017")
                db = client[parsed["db"]]
                collection = db[parsed["collection"]]
                pymongo_method = mongo_op_map[mod_op]
                operation = getattr(collection, pymongo_method)
                result = operation(**parsed[mod_op])
                return f"{mod_op} result: {result}"
            
        # agg pipeline queries
        if "pipeline" in parsed:
            return execute_mongo_query(parsed["db"], parsed["collection"], parsed["pipeline"])


    # SQL query execution
    if db_type == "sql":
        if "sql" in parsed:
            if parsed.get("modification"):
                return execute_modification_sql(parsed["sql"])
            return execute_sql_query(parsed["sql"])

    return "Unsupported or unrecognized query type."