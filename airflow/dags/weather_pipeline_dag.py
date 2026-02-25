"""
Weather Data Pipeline DAG

This DAG orchestrates the complete weather data pipeline:
1. Download weather data from Kaggle (if needed)
2. Validate CSV files
3. Load raw data to staging
4. Transform data (parse text to numeric values)
5. Load cleaned data to production table
6. Generate data quality report
"""

from datetime import datetime, timedelta
from pathlib import Path
import os
import sys
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import kaggle

# Import transformation functions
sys.path.insert(0, os.path.dirname(__file__))
from transformations.weather_transform import (
    transform_weather_dataframe,
    get_data_quality_report
)


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def get_db_connection():
    """Create database connection to weather database."""
    return psycopg2.connect(
        host=os.getenv('WEATHER_DB_HOST', 'postgres'),
        port=os.getenv('WEATHER_DB_PORT', '5432'),
        database=os.getenv('WEATHER_DB_NAME', 'weather_db'),
        user=os.getenv('WEATHER_DB_USER', 'weather_user'),
        password=os.getenv('WEATHER_DB_PASSWORD', 'weather_pass')
    )


def download_kaggle_data(**context):
    """
    Task 1: Download weather dataset from Kaggle.
    
    Returns:
        dict: Download statistics
    """
    dataset = "ramima/weather-dataset-in-antwerp-belgium"
    download_path = "/opt/airflow/data"
    
    print(f"Starting download of dataset: {dataset}")
    print(f"Download path: {download_path}")
    
    # Ensure download directory exists
    Path(download_path).mkdir(parents=True, exist_ok=True)
    
    # Check if files already exist
    csv_files = list(Path(download_path).glob("*.csv"))
    if csv_files:
        print(f"Found {len(csv_files)} CSV files already downloaded:")
        for file in csv_files:
            print(f"  - {file.name}")
        
        stats = {
            'status': 'skipped',
            'files_found': len(csv_files),
            'file_names': [f.name for f in csv_files]
        }
        return stats
    
    try:
        # Download dataset using Kaggle API
        print("Downloading dataset from Kaggle...")
        kaggle.api.dataset_download_files(
            dataset,
            path=download_path,
            unzip=True,
            quiet=False
        )
        
        # Verify download
        csv_files = list(Path(download_path).glob("*.csv"))
        if csv_files:
            print(f"\n✓ Successfully downloaded {len(csv_files)} file(s):")
            for file in csv_files:
                size_mb = file.stat().st_size / (1024 * 1024)
                print(f"  - {file.name} ({size_mb:.2f} MB)")
            
            stats = {
                'status': 'success',
                'files_downloaded': len(csv_files),
                'file_names': [f.name for f in csv_files]
            }
            return stats
        else:
            raise Exception("No CSV files found after download")
            
    except Exception as e:
        print(f"\n✗ Error downloading dataset: {e}")
        raise


def validate_csv_files(**context):
    """
    Task 2: Validate CSV files.
    
    Returns:
        dict: Validation results
    """
    data_dir = Path("/opt/airflow/data")
    required_files = [
        'weather_in_Antwerp.csv',
        'weather_in_Antwerp_future2.csv'
    ]
    
    print("Validating CSV files...")
    validation_results = {}
    
    for filename in required_files:
        filepath = data_dir / filename
        
        if not filepath.exists():
            print(f"✗ Missing file: {filename}")
            validation_results[filename] = {
                'exists': False,
                'error': 'File not found'
            }
            continue
        
        try:
            # Try to read CSV
            df = pd.read_csv(filepath, sep=';', nrows=5)
            
            # Check for required columns
            required_cols = ['clock', 'temp', 'weather', 'wind', 'humidity', 'year', 'month', 'day']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            # Get file stats
            file_size_mb = filepath.stat().st_size / (1024 * 1024)
            
            # Count total rows
            row_count = sum(1 for _ in open(filepath)) - 1  # -1 for header
            
            validation_results[filename] = {
                'exists': True,
                'valid': len(missing_cols) == 0,
                'file_size_mb': round(file_size_mb, 2),
                'row_count': row_count,
                'columns': list(df.columns),
                'missing_columns': missing_cols
            }
            
            if missing_cols:
                print(f"✗ {filename}: Missing columns: {missing_cols}")
            else:
                print(f"✓ {filename}: Valid ({row_count} rows, {file_size_mb:.2f} MB)")
                
        except Exception as e:
            print(f"✗ Error reading {filename}: {e}")
            validation_results[filename] = {
                'exists': True,
                'valid': False,
                'error': str(e)
            }
    
    # Check if all validations passed
    all_valid = all(v.get('valid', False) for v in validation_results.values())
    
    if not all_valid:
        raise Exception(f"CSV validation failed: {validation_results}")
    
    return validation_results


def load_to_staging(**context):
    """
    Task 3: Load raw CSV data to staging table.
    
    Returns:
        dict: Loading statistics
    """
    print("Loading data to staging table...")
    
    conn = get_db_connection()
    
    try:
        # Clear staging table
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE raw_weather_staging")
        conn.commit()
        print("Cleared staging table")
        
        # Load CSV files
        data_dir = '/opt/airflow/data'
        csv_files = [
            ('weather_in_Antwerp.csv', 'weather_in_Antwerp.csv'),
            ('weather_in_Antwerp_future2.csv', 'weather_in_Antwerp_future2.csv')
        ]
        
        total_records = 0
        
        for csv_file, source_name in csv_files:
            csv_path = os.path.join(data_dir, csv_file)
            
            if not os.path.exists(csv_path):
                print(f"Warning: {csv_path} not found, skipping...")
                continue
            
            print(f"Loading {csv_file}...")
            
            # Read CSV with semicolon separator
            df = pd.read_csv(csv_path, sep=';')
            
            # Drop the first unnamed column (index column from CSV)
            if df.columns[0] == '' or 'Unnamed' in df.columns[0]:
                df = df.iloc[:, 1:]
            
            # Add metadata columns
            df['source_file'] = source_name
            df['ingested_at'] = datetime.now()
            
            # Replace NaN with None for proper NULL handling
            df = df.where(pd.notna(df), None)
            
            # Prepare column names
            columns = ['clock', 'temp', 'weather', 'wind', 'humidity', 'barometer', 
                       'visibility', 'year', 'month', 'day', 'source_file', 'ingested_at']
            
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
                    INSERT INTO raw_weather_staging 
                    ({', '.join(columns)})
                    VALUES %s
                """
                execute_values(cur, insert_query, records, page_size=1000)
            
            conn.commit()
            total_records += len(records)
            print(f"Successfully loaded {len(records)} records from {source_name}")
        
        stats = {
            'total_records': total_records,
            'files_loaded': len(csv_files)
        }
        
        print(f"Total records loaded to staging: {total_records}")
        return stats
        
    finally:
        conn.close()


def transform_and_load(**context):
    """
    Task 4 & 5: Transform data and load to cleaned table.
    
    Returns:
        dict: Transformation statistics
    """
    print("Transforming and loading cleaned data...")
    
    conn = get_db_connection()
    
    try:
        # Clear cleaned table
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE weather_cleaned")
        conn.commit()
        print("Cleared weather_cleaned table")
        
        # Read from staging table in chunks
        chunk_size = 10000
        offset = 0
        total_transformed = 0
        
        while True:
            # Fetch chunk from staging
            query = f"""
                SELECT id, clock, temp, weather, wind, humidity, barometer, visibility,
                       year, month, day, source_file, ingested_at
                FROM raw_weather_staging
                ORDER BY id
                LIMIT {chunk_size} OFFSET {offset}
            """
            
            df = pd.read_sql(query, conn)
            
            if len(df) == 0:
                break
            
            print(f"Processing chunk: {offset} to {offset + len(df)}")
            
            # Transform data
            df_transformed = transform_weather_dataframe(df)
            
            # Prepare data for insertion into cleaned table
            columns = ['clock', 'weather', 'year', 'month', 'day',
                       'temp_c', 'humidity_pct', 'wind_kmh', 'barometer_mbar', 'visibility_km',
                       'source_file', 'ingested_at', 'transformed_at']
            
            df_transformed['transformed_at'] = datetime.now()
            
            records = []
            for _, row in df_transformed.iterrows():
                record = (
                    row.get('clock'),
                    row.get('weather'),
                    row.get('year'),
                    row.get('month'),
                    row.get('day'),
                    row.get('temp_c'),
                    row.get('humidity_pct'),
                    row.get('wind_kmh'),
                    row.get('barometer_mbar'),
                    row.get('visibility_km'),
                    row.get('source_file'),
                    row.get('ingested_at'),
                    row.get('transformed_at')
                )
                records.append(record)
            
            # Bulk insert to cleaned table
            with conn.cursor() as cur:
                insert_query = f"""
                    INSERT INTO weather_cleaned 
                    ({', '.join(columns)})
                    VALUES %s
                """
                execute_values(cur, insert_query, records, page_size=1000)
            
            conn.commit()
            total_transformed += len(records)
            
            offset += chunk_size
        
        stats = {
            'total_transformed': total_transformed
        }
        
        print(f"Total records transformed and loaded: {total_transformed}")
        return stats
        
    finally:
        conn.close()


def generate_quality_report(**context):
    """
    Task 6: Generate data quality report.
    
    Returns:
        dict: Quality report
    """
    print("Generating data quality report...")
    
    conn = get_db_connection()
    
    try:
        # Read sample of cleaned data
        query = "SELECT * FROM weather_cleaned"
        df = pd.read_sql(query, conn)
        
        # Generate quality report
        report = get_data_quality_report(df)
        
        # Add additional statistics
        with conn.cursor() as cur:
            # Count by source file
            cur.execute("""
                SELECT source_file, COUNT(*) as count
                FROM weather_cleaned
                GROUP BY source_file
            """)
            source_counts = {row[0]: row[1] for row in cur.fetchall()}
            
            # Date range
            cur.execute("""
                SELECT MIN(year) as min_year, MAX(year) as max_year,
                       MIN(month) as min_month, MAX(month) as max_month
                FROM weather_cleaned
            """)
            date_range = cur.fetchone()
        
        report['source_file_counts'] = source_counts
        report['date_range'] = {
            'min_year': date_range[0],
            'max_year': date_range[1],
            'min_month': date_range[2],
            'max_month': date_range[3]
        }
        
        print("\nData Quality Report:")
        print(json.dumps(report, indent=2))
        
        return report
        
    finally:
        conn.close()


def update_pipeline_metadata(**context):
    """
    Task 7: Update pipeline metadata table with run statistics.
    """
    print("Updating pipeline metadata...")
    
    # Get task instance to access XCom
    ti = context['ti']
    dag_run = context['dag_run']
    
    # Get statistics from previous tasks
    download_stats = ti.xcom_pull(task_ids='download_kaggle_data') or {}
    validation_stats = ti.xcom_pull(task_ids='validate_csv_files') or {}
    staging_stats = ti.xcom_pull(task_ids='load_to_staging') or {}
    transform_stats = ti.xcom_pull(task_ids='transform_and_load') or {}
    quality_report = ti.xcom_pull(task_ids='generate_quality_report') or {}
    
    conn = get_db_connection()
    
    try:
        run_id = dag_run.run_id
        dag_id = dag_run.dag_id
        execution_date = dag_run.execution_date
        
        # Insert or update pipeline metadata
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_metadata 
                (run_id, dag_id, execution_date, end_time, status, 
                 records_downloaded, records_staged, records_transformed, records_loaded,
                 quality_report)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dag_id, execution_date) 
                DO UPDATE SET
                    end_time = EXCLUDED.end_time,
                    status = EXCLUDED.status,
                    records_downloaded = EXCLUDED.records_downloaded,
                    records_staged = EXCLUDED.records_staged,
                    records_transformed = EXCLUDED.records_transformed,
                    records_loaded = EXCLUDED.records_loaded,
                    quality_report = EXCLUDED.quality_report
            """, (
                run_id,
                dag_id,
                execution_date,
                datetime.now(),
                'success',
                download_stats.get('files_downloaded', 0),
                staging_stats.get('total_records', 0),
                transform_stats.get('total_transformed', 0),
                transform_stats.get('total_transformed', 0),
                json.dumps(quality_report)
            ))
        
        conn.commit()
        print(f"Pipeline metadata updated for run_id: {run_id}")
        
    finally:
        conn.close()


# Define the DAG
with DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Complete weather data pipeline with transformation',
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=['weather', 'etl', 'kaggle'],
) as dag:
    
    # Task 1: Download data from Kaggle
    task_download = PythonOperator(
        task_id='download_kaggle_data',
        python_callable=download_kaggle_data,
        provide_context=True,
    )
    
    # Task 2: Validate CSV files
    task_validate = PythonOperator(
        task_id='validate_csv_files',
        python_callable=validate_csv_files,
        provide_context=True,
    )
    
    # Task 3: Load to staging table
    task_load_staging = PythonOperator(
        task_id='load_to_staging',
        python_callable=load_to_staging,
        provide_context=True,
    )
    
    # Task 4 & 5: Transform and load to cleaned table
    task_transform = PythonOperator(
        task_id='transform_and_load',
        python_callable=transform_and_load,
        provide_context=True,
    )
    
    # Task 6: Generate quality report
    task_quality = PythonOperator(
        task_id='generate_quality_report',
        python_callable=generate_quality_report,
        provide_context=True,
    )
    
    # Task 7: Update pipeline metadata
    task_metadata = PythonOperator(
        task_id='update_pipeline_metadata',
        python_callable=update_pipeline_metadata,
        provide_context=True,
    )
    
    # Define task dependencies
    task_download >> task_validate >> task_load_staging >> task_transform >> task_quality >> task_metadata
