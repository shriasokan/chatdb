import mysql.connector
import pandas as pd

def execute_sql_query(query):
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="dsci351"
        )
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        return f"SQL query execution failed: {str(e)}"

def execute_modification_sql(query):
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="dsci351"
        )
        cursor = conn.cursor()
        print("RAW SQL:", repr(query))
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        return f"Successfully executed: {query}"
    except Exception as e:
        return f"SQL modification failed: {str(e)}"