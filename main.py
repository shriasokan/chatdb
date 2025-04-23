from query_router import handle_query

if __name__ == "__main__":
    # For testing
    #print("SQL Query 1 (electric_vehicles):")
    #print(handle_query("Show the top 5 electric vehicles with the longest electric range.", "sql", table="electric_vehicles").to_string(index=False))

    #print("SQL Query 2 (imdb_top_1000): ")
    #print(handle_query("List all movies in the Crime genre released after 2005.", "sql", table="imdb_top_1000"))

    #print("SQL Query 3 (air_quality):")
    #print(handle_query("Find the average data value for each air quality category.", "sql", table="air_quality"))

    # print("Schema Query 1: List all tables")
    # print(handle_query("What tables do I have?", "sql"))

    # print("\nSchema Query 2: View columns of imdb_top_1000")
    # print(handle_query("What columns are in imdb_top_1000?", "sql"))

    # print("\nSchema Query 3: Show samples from air_quality")
    # print(handle_query("Show sample rows from air_quality", "sql", table="air_quality"))

    print("EV")
    print(handle_query("Add a new EV with VIN ABC123, made by FORD in 2023, model Mustang, and electric range of 300.", "sql", table="electric_vehicles"))
