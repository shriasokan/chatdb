import mysql.connector

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
    cursor = conn.cursor(dictionary=True)

    cursor.execute(query)
    result = cursor.fetchall()

    cursor.close()
    conn.close()

    return result