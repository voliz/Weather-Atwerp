# Apache Airflow Implementation Overview

## Implementatie Samenvatting

Dit document beschrijft de implementatie van Apache Airflow voor het orchestreren en visualiseren van de weather data pipeline. Airflow vervangt de standalone download en ingest containers door een geïntegreerde workflow met visuele monitoring, data transformatie en pipeline metadata tracking.

## Geïmplementeerde Componenten

### 1. Docker Infrastructure

**Bestand:** `docker-compose.yml`

Toegevoegde services:
- **airflow-postgres**: Metadata database voor Airflow's interne state
- **airflow-init**: Eenmalige initialisatie service (database migratie, admin user creatie)
- **airflow-webserver**: Web UI voor DAG visualisatie en monitoring (port 8080)
- **airflow-scheduler**: Task scheduler voor DAG execution

**Custom Airflow Image:**
- **Bestand:** `airflow/Dockerfile`
- Extends `apache/airflow:2.8.1-python3.11`
- Installeert additional dependencies: kaggle, pandas, psycopg2-binary
- **Bestand:** `airflow/requirements.txt` - lijst van Python packages

**Shared volumes:**
- `./airflow/dags` - DAG definitie bestanden
- `./airflow/logs` - Task execution logs
- `./airflow/plugins` - Custom Airflow plugins
- `./data` - CSV data bestanden

**Environment variabelen:**
- `AIRFLOW__CORE__EXECUTOR`: LocalExecutor
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`: PostgreSQL connection string
- `WEATHER_DB_*`: Credentials voor weather database
- `KAGGLE_USERNAME` en `KAGGLE_KEY`: Kaggle API credentials

### 2. Database Schema Updates

**Bestand:** `database/init.sql`

**Nieuwe tabellen:**

**raw_weather_staging**
- Tijdelijke staging tabel tijdens pipeline processing
- Identieke structuur aan raw_weather
- Wordt getruncate bij elke pipeline run

**weather_cleaned**
- Productie tabel met getransformeerde data
- Numerieke kolommen: temp_c, humidity_pct, wind_kmh, barometer_mbar, visibility_km
- Bevat zowel originele text kolommen als geparseerde numerieke waarden
- Metadata: source_file, ingested_at, transformed_at

**pipeline_metadata**
- Tracking van DAG runs
- Status, record counts, quality reports
- JSONB kolom voor data quality metrics
- Unique constraint op (dag_id, execution_date)

### 3. Data Transformation Module

**Bestand:** `airflow/dags/transformations/weather_transform.py`

**Parsing functies:**

| Functie | Input | Output | Validatie |
|---------|-------|--------|-----------|
| parse_temperature() | "11 °C" | 11.0 | Ondersteunt negatieve waarden |
| parse_humidity() | "94%" | 94.0 | Range check: 0-100 |
| parse_wind_speed() | "17 km/h" | 17.0 | "Calm" wordt 0.0 |
| parse_barometer() | "1011 mbar" | 1011.0 | Range: 900-1100 mbar |
| parse_visibility() | "5 km" | 5.0 | Range: 0-100 km |

**Bulk processing:**
- `transform_weather_dataframe()`: Transformeert volledige pandas DataFrame
- `get_data_quality_report()`: Genereert statistieken en null percentages

**Error handling:**
- Ongeldige waarden worden None (NULL in database)
- Out-of-range waarden worden None
- Geen crashes bij ongeldige input

### 4. Airflow DAG Definition

**Bestand:** `airflow/dags/weather_pipeline_dag.py`

**DAG configuratie:**
- **Naam**: weather_data_pipeline
- **Schedule**: None (manual trigger only)
- **Retries**: 1 per task
- **Retry delay**: 5 minuten
- **Start date**: days_ago(1)
- **Catchup**: False

**Task flow:**

```
download_kaggle_data
       ↓
validate_csv_files
       ↓
load_to_staging
       ↓
transform_and_load
       ↓
generate_quality_report
       ↓
update_pipeline_metadata
```

**Task beschrijvingen:**

1. **download_kaggle_data**
   - Downloadt dataset van Kaggle API
   - Skipt download als CSV's al aanwezig
   - Output: Download statistics via XCom

2. **validate_csv_files**
   - Valideert CSV structuur en integriteit
   - Controleert vereiste kolommen
   - Faalt bij missing files of invalid schema

3. **load_to_staging**
   - Truncate raw_weather_staging
   - Laad CSV's met pandas (semicolon separator)
   - Bulk insert met psycopg2 (1000 records/batch)

4. **transform_and_load**
   - Truncate weather_cleaned
   - Leest data van staging in chunks (10,000 records)
   - Past transformatie functies toe
   - Insert naar weather_cleaned tabel

5. **generate_quality_report**
   - Genereert data quality metrics
   - Null percentages, min/max/mean/median per kolom
   - Record counts per source file

6. **update_pipeline_metadata**
   - Verzamelt statistics van alle tasks via XCom
   - Insert/update pipeline_metadata tabel
   - Slaat quality report op als JSONB

### 5. API Updates

**Nieuwe bestanden:**

**api/models/db_models.py**
- `WeatherCleaned` SQLAlchemy model met Numeric kolommen

**api/models/schemas.py**
- `CleanedWeatherResponse` - Response schema met floats
- `CleanedPaginatedResponse` - Paginated wrapper
- `PipelineMetadataResponse` - Pipeline run info schema

**api/repositories/weather_repository.py**
- `CleanedWeatherRepository` class
- Query methods voor weather_cleaned tabel
- `get_temperature_stats()` voor min/max/avg temperatuur

**api/services/cleaned_weather_service.py**
- `CleanedWeatherService` class
- Business logic voor cleaned data endpoints

**api/routers/cleaned_weather_router.py**
- GET `/weather` - Lijst van cleaned records
- GET `/weather/{id}` - Enkel cleaned record
- GET `/weather/stats/summary` - Statistieken met temperatuur metrics

**api/main.py**
- Geïntegreerde cleaned_weather_router
- Updated API title en description
- Beide endpoints beschikbaar: `/weather` (nieuw) en `/raw-weather` (legacy)

### 6. Directory Structure

```
airflow/
├── Dockerfile                    (custom Airflow image definitie)
├── requirements.txt              (Python dependencies)
├── dags/
│   ├── .airflowignore           (patterns voor files om te negeren)
│   ├── weather_pipeline_dag.py  (hoofdpipeline DAG)
│   └── transformations/
│       ├── __init__.py
│       └── weather_transform.py (data parsing functies)
├── logs/                         (gegenereerd bij runtime)
└── plugins/                      (leeg, reserved voor custom plugins)
```

### 7. Documentation Updates

**README.md**
- Airflow setup instructies
- Web UI login credentials
- Pipeline trigger instructies
- Visualisatie mogelijkheden uitleg
- Updated API endpoints met cleaned data voorbeelden
- Uitgebreide troubleshooting sectie voor Airflow

**ARCHITECTURE.md**
- Airflow orchestratie uitleg
- DAG structure en task details
- XCom communication beschrijving
- Visualisatie features overzicht

**.env.example**
- `AIRFLOW_UID` configuratie toegevoegd

## Configuratie

### Environment Variables

Vereiste variabelen in `.env`:
```
KAGGLE_USERNAME=your_username
KAGGLE_KEY=your_api_key
AIRFLOW_UID=50000
```

### Airflow Login Credentials

- **URL**: http://localhost:8080
- **Username**: admin
- **Password**: admin

Deze worden automatisch aangemaakt door de airflow-init service.

### Database Connections

Airflow heeft automatisch toegang tot:
- **Weather Database**: Via environment variabelen (WEATHER_DB_*)
- **Airflow Metadata Database**: Via SQL_ALCHEMY_CONN

## Gebruik

### Starten van Services

```powershell
docker compose up --build
```

Eerste keer opstarten duurt 2-3 minuten voor Airflow initialisatie.

### Pipeline Triggeren

1. Open browser naar http://localhost:8080
2. Login met admin/admin
3. Navigeer naar DAGs pagina
4. Zoek `weather_data_pipeline`
5. Klik op play button (▶) rechts
6. Bevestig "Trigger DAG"

### Monitoring

**Graph View:**
- Visuele weergave van task dependencies
- Real-time status updates
- Kleuren: grijs (niet gestart), geel (running), groen (success), rood (failed)

**Task Logs:**
1. Klik op task box in graph
2. Klik "Log" button
3. Bekijk gedetailleerde output

**Views beschikbaar:**
- Graph: Task flow diagram
- Tree: Historische runs
- Gantt: Task durations
- Task Duration: Performance metrics
- Code: DAG source code

## API Endpoints

### Cleaned Data (Primair)

**GET /weather**
- Retourneert records met numerieke waarden
- Query parameters: year, month, day, source_file, limit, offset

**GET /weather/{id}**
- Enkel record met numerieke waarden

**GET /weather/stats/summary**
- Dataset statistieken inclusief temperatuur min/max/avg

### Raw Data (Legacy)

**GET /raw-weather**
- Retourneert records met tekst waarden ("11 °C", "94%")

## Data Flow

### Pipeline Execution Flow

```
Kaggle API
    ↓ (download_kaggle_data)
CSV Files in data/
    ↓ (validate_csv_files)
Validation OK
    ↓ (load_to_staging)
raw_weather_staging
    ↓ (transform_and_load)
weather_cleaned
    ↓ (generate_quality_report)
Quality Metrics
    ↓ (update_pipeline_metadata)
pipeline_metadata
```

### API Query Flow

```
HTTP Request
    ↓
Router (/weather)
    ↓
Service (CleanedWeatherService)
    ↓
Repository (CleanedWeatherRepository)
    ↓
SQLAlchemy Query
    ↓
weather_cleaned table
    ↓
Response (JSON met numerieke waarden)
```

## Technical Specifications

### Software Versies

- Apache Airflow: 2.8.1
- Python: 3.11
- PostgreSQL: 16 (Alpine)
- FastAPI: Latest
- SQLAlchemy: 2.0
- Pandas: Latest

### Resource Requirements

- RAM: Minimaal 4GB beschikbaar voor Docker
- Disk: ~500MB voor Airflow images
- CPU: 2+ cores aanbevolen voor parallelle processing

### Performance Metrics

Geschatte task durations:
- download_kaggle_data: 30-60 seconden
- validate_csv_files: <5 seconden
- load_to_staging: 30-60 seconden
- transform_and_load: 2-5 minuten (133k+ records)
- generate_quality_report: 10-30 seconden
- update_pipeline_metadata: <5 seconden

Totale pipeline duration: ~5-10 minuten voor volledige run.

## Architectuur Beslissingen

### Manual Triggering

Schedule interval is None omdat:
- Kaggle dataset is historisch (2012-2020)
- Geen periodieke updates verwacht
- Pipeline wordt on-demand uitgevoerd voor demonstratie doeleinden

### LocalExecutor

LocalExecutor gekozen omdat:
- Eenvoudige setup voor ontwikkeling
- Voldoende voor sequentiële tasks
- Geen distributed computing nodig voor deze dataset size

### Separate Cleaned Table

Aparte weather_cleaned tabel omdat:
- Behoud originele raw data voor auditability
- Betere API performance (query op numeric kolommen)
- Type safety in applicatie laag
- Backward compatibility met bestaande raw_weather tabel

### Transformation in Airflow

Data transformatie in Airflow DAG in plaats van API laag omdat:
- Eenmalige processing vs. per-request processing
- Betere performance voor API endpoints
- Data quality validatie tijdens pipeline
- Vereenvoudigde API logic

## Troubleshooting

### Airflow Web UI Niet Toegankelijk

- Wacht 2-3 minuten na docker compose up
- Check logs: `docker compose logs airflow-webserver`
- Verify port 8080 is vrij
- Check container status: `docker compose ps`

### DAG Import Errors

- Check Airflow UI voor error banner
- Review scheduler logs: `docker compose logs airflow-scheduler`
- Validate Python syntax in DAG file
- Verify all imports zijn beschikbaar in Airflow container

**ModuleNotFoundError voor kaggle/pandas/psycopg2:**
- Verify custom Airflow image wordt gebouwd (niet base image)
- Check `airflow/Dockerfile` en `airflow/requirements.txt` aanwezig zijn
- Rebuild containers: `docker compose down && docker compose up --build`
- Verify packages geïnstalleerd: `docker exec airflow-scheduler pip list`

### Task Failures

- Click op gefaalde task in Graph View
- Bekijk logs voor error messages
- Check database connectivity
- Verify Kaggle credentials voor download task

### Performance Issues

- Monitor met: `docker stats`
- Check chunk size voor transform task (default: 10,000)
- Verify database indexes zijn aangemaakt
- Consider increasing Docker memory allocation

## Future Enhancements

Mogelijke uitbreidingen:
- Scheduling voor periodieke checks van dataset updates
- Email notificaties bij pipeline failures
- Data quality thresholds met automatic alerts
- Incremental loading in plaats van full refresh
- Materialized views voor veelgebruikte aggregaties
- Additional transformations (derived metrics, aggregations)
- Integration met externe monitoring tools (Prometheus, Grafana)
