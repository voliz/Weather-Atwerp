-- Raw weather data storage table
CREATE TABLE IF NOT EXISTS raw_weather (
    id SERIAL PRIMARY KEY,
    clock TEXT,
    temp TEXT,
    weather TEXT,
    wind TEXT,
    humidity TEXT,
    barometer TEXT,
    visibility TEXT,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    source_file TEXT NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_raw_weather_year ON raw_weather (year);

CREATE INDEX IF NOT EXISTS idx_raw_weather_month ON raw_weather (month);

CREATE INDEX IF NOT EXISTS idx_raw_weather_day ON raw_weather (day);

CREATE INDEX IF NOT EXISTS idx_raw_weather_date ON raw_weather (year, month, day);

CREATE INDEX IF NOT EXISTS idx_raw_weather_source ON raw_weather (source_file);

CREATE INDEX IF NOT EXISTS idx_raw_weather_ingested ON raw_weather (ingested_at);

-- Staging table for raw weather data (used during pipeline processing)
CREATE TABLE IF NOT EXISTS raw_weather_staging (
    id SERIAL PRIMARY KEY,
    clock TEXT,
    temp TEXT,
    weather TEXT,
    wind TEXT,
    humidity TEXT,
    barometer TEXT,
    visibility TEXT,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    source_file TEXT NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Cleaned weather data with numeric values
CREATE TABLE IF NOT EXISTS weather_cleaned (
    id SERIAL PRIMARY KEY,
    clock TEXT,
    weather TEXT,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    -- Numeric parsed values
    temp_c NUMERIC(5, 2),
    humidity_pct NUMERIC(5, 2),
    wind_kmh NUMERIC(6, 2),
    barometer_mbar NUMERIC(7, 2),
    visibility_km NUMERIC(6, 2),
    -- Metadata
    source_file TEXT NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    transformed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for cleaned weather data
CREATE INDEX IF NOT EXISTS idx_weather_cleaned_year ON weather_cleaned (year);

CREATE INDEX IF NOT EXISTS idx_weather_cleaned_month ON weather_cleaned (month);

CREATE INDEX IF NOT EXISTS idx_weather_cleaned_day ON weather_cleaned (day);

CREATE INDEX IF NOT EXISTS idx_weather_cleaned_date ON weather_cleaned (year, month, day);

CREATE INDEX IF NOT EXISTS idx_weather_cleaned_temp ON weather_cleaned (temp_c);

CREATE INDEX IF NOT EXISTS idx_weather_cleaned_source ON weather_cleaned (source_file);

CREATE INDEX IF NOT EXISTS idx_weather_cleaned_transformed ON weather_cleaned (transformed_at);

-- Pipeline metadata tracking table
CREATE TABLE IF NOT EXISTS pipeline_metadata (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE,
    dag_id TEXT NOT NULL,
    execution_date TIMESTAMP NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    status TEXT CHECK (
        status IN (
            'running',
            'success',
            'failed',
            'skipped'
        )
    ),
    records_downloaded INTEGER DEFAULT 0,
    records_staged INTEGER DEFAULT 0,
    records_transformed INTEGER DEFAULT 0,
    records_loaded INTEGER DEFAULT 0,
    quality_report JSONB,
    error_message TEXT,
    CONSTRAINT unique_dag_execution UNIQUE (dag_id, execution_date)
);

-- Index for pipeline metadata queries
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_run_id ON pipeline_metadata (run_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_dag_id ON pipeline_metadata (dag_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_status ON pipeline_metadata (status);

CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_execution_date ON pipeline_metadata (execution_date);