import mysql.connector
import pandas as pd
import pprint

# Connect to the MySQL database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",  # <-- Put your actual password here
    database="dsci351"
)

cursor = conn.cursor()

# Step 1: Get list of all tables
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()
table_names = [table[0] for table in tables]

# Step 2: For each table, get column names and a few sample rows
schema_info = {}

for table in table_names:
    cursor.execute(f"DESCRIBE {table}")
    columns = cursor.fetchall()
    cursor.execute(f"SELECT * FROM {table} LIMIT 3")
    sample_data = cursor.fetchall()
    column_names = [col[0] for col in columns]
    schema_info[table] = {
        "columns": column_names,
        "samples": sample_data
    }

cursor.close()
conn.close()

# Print schema info
pprint.pprint(schema_info)
