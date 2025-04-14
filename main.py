from query_router import handle_query

if __name__ == "__main__":
    # For testing
    print("SQL Result: ")
    print(handle_query("show me data", "sql"))

    print("Mongo Result: ")
    print(handle_query("show me data", "nosql"))