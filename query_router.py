from backend.nosql.nosql_handler import execute_mongo_query
from backend.sql.sql_handler import execute_sql_query
from backend.nlp.parser import parse_nl_query

def handle_query(nl_query: str, db_type: str):
    parsed = parse_nl_query(nl_query, db_type)

    if "error" in parsed:
        return f"Error parsing query: {parsed['error']}"
    
    if db_type == "sql":
        return execute_sql_query(parsed["sql"])
    
    elif db_type == "nosql":
        return execute_mongo_query(parsed["db"], parsed["collection"], parsed["pipeline"])
    
    return "Unsupported database type."