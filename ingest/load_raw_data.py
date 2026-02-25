import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import time


def get_db_connection():
    """Create database connection with retry logic."""
    max_retries = 5
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                database=os.getenv('DB_NAME', 'weather_db'),
                user=os.getenv('DB_USER', 'weather_user'),
                password=os.getenv('DB_PASSWORD', 'weather_pass')
            )
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                print(f"Database connection attempt {attempt + 1} failed. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                raise e


def check_if_data_exists(conn):
    """Check if raw_weather table already has data."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw_weather")
        count = cur.fetchone()[0]
        return count > 0


def load_csv_to_db(csv_path, source_file_name, conn):
    """Load CSV file into raw_weather table."""
    print(f"Loading {csv_path}...")
    
    # Read CSV with semicolon separator
    df = pd.read_csv(csv_path, sep=';')
    
    # Drop the first unnamed column (index column from CSV)
    if df.columns[0] == '' or 'Unnamed' in df.columns[0]:
        df = df.iloc[:, 1:]
    
    # Add metadata columns
    df['source_file'] = source_file_name
    df['ingested_at'] = datetime.now()
    
    # Replace NaN with None for proper NULL handling
    df = df.where(pd.notna(df), None)
    
    # Prepare column names
    columns = ['clock', 'temp', 'weather', 'wind', 'humidity', 'barometer', 
               'visibility', 'year', 'month', 'day', 'source_file', 'ingested_at']
    
    # Handle files without all columns (like future2.csv missing barometer/visibility)
    available_columns = []
    for col in columns:
        if col in ['source_file', 'ingested_at']:
            available_columns.append(col)
        elif col in df.columns:
            available_columns.append(col)
    
    # Prepare data for insertion
    records = []
    for _, row in df.iterrows():
        record = []
        for col in columns:
            if col in ['source_file', 'ingested_at']:
                record.append(row[col])
            elif col in df.columns:
                record.append(row[col])
            else:
                record.append(None)  # Missing columns get NULL
        records.append(tuple(record))
    
    # Bulk insert
    with conn.cursor() as cur:
        insert_query = f"""
            INSERT INTO raw_weather 
            ({', '.join(columns)})
            VALUES %s
        """
        execute_values(cur, insert_query, records, page_size=1000)
    
    conn.commit()
    print(f"Successfully loaded {len(records)} records from {source_file_name}")


def main():
    """Main ingest function."""
    print("Starting data ingestion process...")
    
    # Connect to database
    conn = get_db_connection()
    print("Connected to database successfully")
    
    try:
        # Check if data already exists
        if check_if_data_exists(conn):
            print("Data already exists in raw_weather table. Skipping ingestion.")
            print("To re-ingest, truncate the table first.")
            return
        
        # Load both CSV files
        data_dir = '/app/data'
        csv_files = [
            ('weather_in_Antwerp.csv', 'weather_in_Antwerp.csv'),
            ('weather_in_Antwerp_future2.csv', 'weather_in_Antwerp_future2.csv')
        ]
        
        for csv_file, source_name in csv_files:
            csv_path = os.path.join(data_dir, csv_file)
            if os.path.exists(csv_path):
                load_csv_to_db(csv_path, source_name, conn)
            else:
                print(f"Warning: {csv_path} not found, skipping...")
        
        print("Data ingestion completed successfully!")
        
    except Exception as e:
        print(f"Error during ingestion: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
