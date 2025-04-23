from sqlalchemy import create_engine
import pandas as pd

# Create SQLAlchemy engine
DB_URI = "mysql+mysqlconnector://root:@localhost/dsci351"
engine = create_engine(DB_URI)

def handle_schema_query(query):
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        return f"Schema exploration failed: {str(e)}"

def list_tables():
    return handle_schema_query("SHOW TABLES")

def describe_table(table_name):
    return handle_schema_query(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")

def sample_rows(table_name, limit=3):
    return handle_schema_query(f"SELECT * FROM {table_name} LIMIT {limit}")