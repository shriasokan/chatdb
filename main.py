from query_router import handle_query

if __name__ == "__main__":
    # For testing
    print("SQL Query (Gemini): ")
    print(handle_query("List all movies directed by Christopher Nolan after 2010", "sql"))

    print("\nMongo Query (Gemini): ")
    print(handle_query("Show average electric range grouped by Make", "nosql"))