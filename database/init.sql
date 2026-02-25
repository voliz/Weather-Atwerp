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