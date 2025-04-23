from backend.nosql.nosql_handler import execute_mongo_query
from backend.sql.sql_handler import execute_sql_query, execute_modification_sql
from backend.sql.schema_handler import list_tables, describe_table, sample_rows
from backend.nlp.parser import parse_nl_query

def handle_query(nl_query: str, db_type: str, table=None):
    parsed = parse_nl_query(nl_query, db_type, table)
    if "error" in parsed:
        return f"Error parsing query: {parsed['error']}"

    if parsed.get("schema_explore") == "list_tables":
        return list_tables()

    if parsed.get("schema_explore") == "describe_table":
        return describe_table(table)

    if parsed.get("schema_explore") == "sample_rows":
        return sample_rows(table)
    
    if parsed.get("modification") in ["insert", "delete", "update"]:
        return execute_modification_sql(parsed["sql"])

    if db_type == "sql":
        return execute_sql_query(parsed["sql"])

    elif db_type == "nosql":
        return execute_mongo_query(parsed["db"], parsed["collection"], parsed["pipeline"])

    return "Unsupported database type."