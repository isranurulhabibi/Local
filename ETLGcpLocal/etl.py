import sys
import logging
import os
import pandas as pd
from sqlalchemy import create_engine, text
from psycopg2 import connect, sql
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)

# Helper to get environment variables for sensitive information
def get_env_var(var_name):
    value = os.getenv(var_name)
    if not value:
        logging.error(f"Environment variable {var_name} is not set.")
        sys.exit(1)
    return value

# Database connection parameters from environment variables
SOURCE_DB_PARAMS = {
    'host': get_env_var('SOURCE_DB_HOST'),
    'database': get_env_var('SOURCE_DB_NAME'),
    'user': get_env_var('SOURCE_DB_USER'),
    'password': get_env_var('SOURCE_DB_PASSWORD'),
    'port': get_env_var('SOURCE_DB_PORT')
}

DEST_DB_URL = get_env_var('DEST_DB_URL')

# Create connection to the source PostgreSQL database
def connect_to_postgres(params):
    try:
        return connect(**params)
    except Exception as e:
        logging.error(f"Error connecting to the source PostgreSQL database: {e}")
        sys.exit(1)

# Create SQLAlchemy engine for the destination PostgreSQL database
def create_engine_for_other_postgres():
    try:
        return create_engine(DEST_DB_URL)
    except Exception as e:
        logging.error(f"Error creating SQLAlchemy engine for the destination database: {e}")
        sys.exit(1)

# Get the latest timestamp from the destination table
def get_latest_timestamp(engine, table_name, timestamp_column):
    query = text(f"SELECT MAX({timestamp_column}) FROM {table_name}")
    try:
        with engine.connect() as conn:
            return conn.execute(query).scalar()
    except Exception as e:
        logging.error(f"Error fetching latest timestamp from {table_name}: {e}")
        sys.exit(1)

# Create a DataFrame with data newer than the latest timestamp
def fetch_incremental_data(conn, table_name, timestamp_column, last_timestamp):
    try:
        query = sql.SQL("SELECT * FROM {} WHERE {} > %s").format(
            sql.Identifier(table_name), sql.Identifier(timestamp_column)
        ) if last_timestamp else sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))

        with conn.cursor() as cur:
            cur.execute(query, (last_timestamp,) if last_timestamp else None)
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()
            return pd.DataFrame(data, columns=columns)
    except Exception as e:
        logging.error(f"Error fetching data from {table_name}: {e}")
        sys.exit(1)

# Copy data to the destination database
def copy_data_to_other_postgres(engine, df, table_name, timestamp_column):
    if df.empty:
        logging.info(f"No new data to copy for {table_name}.")
        return
    
    try:
        row_count = len(df)
        df.to_sql(table_name, engine, if_exists='append', index=False)
        logging.info(f"Copied {row_count} rows to {table_name}.")

        # Check max timestamp after copy
        max_timestamp = get_latest_timestamp(engine, table_name, timestamp_column)
        logging.info(f"Max {timestamp_column} in {table_name} after copy: {max_timestamp}")
    except Exception as e:
        logging.error(f"Error copying data to {table_name}: {e}")
        sys.exit(1)

# Main function to perform the data migration for multiple tables
def main(tables, timestamp_column):
    dest_engine = create_engine_for_other_postgres()

    with connect_to_postgres(SOURCE_DB_PARAMS) as source_conn:
        for table_name in tables:
            logging.info(f"Processing table: {table_name}")
            last_timestamp = get_latest_timestamp(dest_engine, table_name, timestamp_column)
            df = fetch_incremental_data(source_conn, table_name, timestamp_column, last_timestamp)
            copy_data_to_other_postgres(dest_engine, df, table_name, timestamp_column)

if __name__ == "__main__":
    tables = ['dht_mq_ldr', 'multi_sensor_weather_data']
    timestamp_column = 'timestamp'  # Replace with the actual timestamp column name
    main(tables, timestamp_column)
