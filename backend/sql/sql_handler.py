import mysql.connector
import pandas as pd

def get_mysql_connection():
    # Temporary hardcoded connection (replace later with .env)
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="dsci351"
    )
    return connection

def execute_sql_query(query):
    conn = get_mysql_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        result = cursor.fetchall()
        columns = cursor.column_names
        df = pd.DataFrame(result, columns=columns)
        return df
    finally:
        cursor.close()
        conn.close()