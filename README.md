# Weather Data API met Apache Airflow
gebruikte dataset: https://www.kaggle.com/datasets/ramima/weather-dataset-in-antwerp-belgium?resource=download

Een Docker-gebaseerde data engineering oplossing voor het orchestreren, transformeren en serveren van weerdata uit Antwerpen. De data wordt via Apache Airflow gedownload van Kaggle, getransformeerd naar numerieke waarden, en via een FastAPI REST API toegankelijk gemaakt.

## Architectuur

![architectuur](/images/architectuur.png)

### Components

- **Apache Airflow**: Workflow orchestratie en data pipeline management
- **Airflow Webserver**: Web UI voor DAG visualisatie en monitoring (port 8080)
- **Airflow Scheduler**: Task scheduling en DAG execution
- **Airflow Metadata DB**: PostgreSQL database voor Airflow's interne state
- **Weather Database**: PostgreSQL database met `raw_weather`, `raw_weather_staging`, en `weather_cleaned` tabellen
- **Data Transformation**: Python modules voor het parsen van tekst naar numerieke waarden
- **FastAPI**: REST API met clean architecture (routers → services → repositories)

### Data Pipeline (Airflow DAG)

```
Download CSV → Validate → Stage → Transform → Load Cleaned → Quality Report → Metadata
```

1. **Download**: Haal data op van Kaggle (skip als al aanwezig)
2. **Validate**: Controleer CSV structuur en integriteit
3. **Stage**: Laad raw data naar staging tabel
4. **Transform**: Parse tekst waarden ("11 °C") naar numeriek (11.0)
5. **Load Cleaned**: Insert getransformeerde data naar productie tabel
6. **Quality Report**: Genereer statistieken en data quality metrics
7. **Metadata**: Log pipeline run informatie

## Clean Code Architectuur (API)

De API volgt een gelaagde architectuur:

1. **Routers** (`routers/`): HTTP endpoints, validatie, response formatting
2. **Services** (`services/`): Business logic, orchestratie
3. **Repositories** (`repositories/`): Database queries, data access
4. **Models** (`models/`): Pydantic schemas en SQLAlchemy models
5. **Database** (`database.py`): Database connectie en sessie management

## Gebruik

### Vereisten

- Docker Desktop geïnstalleerd en draaiend
- PowerShell of Command Prompt
- **Kaggle account en API credentials** (voor automatische data download)
- Minimaal 4GB RAM beschikbaar voor Docker

### Setup Kaggle API Credentials

De dataset wordt via Airflow gedownload van Kaggle. Je hebt hiervoor een API key nodig:

#### Optie 1: Via Environment Variables (Aanbevolen)

1. Ga naar https://www.kaggle.com/settings
2. Scroll naar "API" sectie
3. Klik "Create New API Token" → dit download `kaggle.json`
4. Kopieer `.env.example` naar `.env`:
   ```powershell
   Copy-Item .env.example .env
   ```
5. Open `.env` en vul je credentials in (uit `kaggle.json`):
   ```env
   KAGGLE_USERNAME=your_username
   KAGGLE_KEY=your_api_key_here
   AIRFLOW_UID=50000
   ```

#### Optie 2: Via kaggle.json bestand

1. Download je `kaggle.json` van https://www.kaggle.com/settings
2. Plaats het in de `kaggle/` folder:
   ```powershell
   mkdir kaggle
   # Kopieer kaggle.json naar kaggle/ folder
   ```
3. Airflow leest automatisch deze credentials

**Belangrijk:** Deel nooit je API key! Deze bestanden staan in `.gitignore`.

### Start de Stack

1. Open een terminal in de project root directory:

```powershell
cd "d:\II\mydocs\school\2026 toegepaste informatica jaar 4 (AI)\Data engineering\Project"
```

2. **Eerste keer:** Configureer Kaggle credentials (zie Setup sectie hierboven)

3. Start alle services:

```powershell
docker compose up --build
```

Dit zal automatisch:
- Airflow metadata database starten
- Airflow webserver starten op `http://localhost:8080`
- Airflow scheduler starten
- Weather database starten (Postgres)
- FastAPI starten op `http://localhost:8000`
- DAGs laden vanuit `airflow/dags/` folder

**Eerste keer opstarten duurt ~2-3 minuten** voor Airflow initialisatie.

### Apache Airflow Gebruiken

#### 1. Toegang tot Airflow Web UI

Bezoek `http://localhost:8080` in je browser.

**Login credentials:**
- Username: `admin`
- Password: `admin`

#### 2. Weather Pipeline Triggeren

1. In Airflow UI, navigeer naar "DAGs" pagina
2. Zoek de DAG genaamd `weather_data_pipeline`
3. Click op de "Play" knop (▶) rechts om de DAG te triggeren
4. Bevestig "Trigger DAG"

#### 3. Pipeline Monitoring

**Graph View:**
- Click op de DAG naam `weather_data_pipeline`
- Kies "Graph" tab
- Zie visuele weergave van alle tasks en hun status:
  - ⚪ Grijs = Niet gestart
  - 🟡 Geel = Running
  - 🟢 Groen = Success
  - 🔴 Rood = Failed

**Logs bekijken:**
1. Click op een specifieke task box in de graph
2. Click "Log" button
3. Zie gedetailleerde output van de task

**Task Details:**
- `download_kaggle_data`: Download CSV van Kaggle
- `validate_csv_files`: Valideer CSV structuur
- `load_to_staging`: Laad naar staging tabel
- `transform_and_load`: Parse tekst → numeriek, laad naar cleaned tabel
- `generate_quality_report`: Data quality statistieken
- `update_pipeline_metadata`: Log run informatie

#### 4. Pipeline Visualisatie

**Airflow Web UI biedt:**
- **Graph View**: Task dependencies en flow
- **Tree View**: Historische runs over tijd
- **Gantt View**: Task durations en parallellisme
- **Task Duration**: Performance metrics per task
- **Code**: Bekijk DAG source code
- **Logs**: Gedetailleerde task output

**Pipeline Metadata bekijken via API:**
```bash
# In toekomstige versie via /pipeline/metadata endpoint
```

### API Endpoints

Bezoek `http://localhost:8000/docs` voor interactieve API documentatie (Swagger UI).

#### Beschikbare Endpoints

**Health Check**
```bash
GET http://localhost:8000/health
```

**Get Cleaned Weather Records (NIEUW - Aanbevolen)**
```bash
GET http://localhost:8000/weather?year=2012&month=1&limit=10
```

Retourneert data met **numerieke waarden**:
```json
{
  "id": 1,
  "year": 2012,
  "month": 1,
  "day": 1,
  "clock": "00:20",
  "weather": "Mostly cloudy.",
  "temp_c": 11.0,
  "humidity_pct": 94.0,
  "wind_kmh": 17.0,
  "barometer_mbar": 1011.0,
  "visibility_km": 5.0
}
```

Query parameters:
- `year`: Filter op jaar (optioneel)
- `month`: Filter op maand 1-12 (optioneel)
- `day`: Filter op dag 1-31 (optioneel)
- `source_file`: Filter op bron bestand (optioneel)
- `limit`: Max aantal records (default: 100, max: 10000)
- `offset`: Paginatie offset (default: 0)

**Get Single Cleaned Record by ID**
```bash
GET http://localhost:8000/weather/1
```

**Get Cleaned Dataset Statistics**
```bash
GET http://localhost:8000/weather/stats/summary
```

Geeft terug:
- Totaal aantal records
- Datum range (min/max)
- Lijst van source files
- Temperatuur statistieken (min/max/avg)

**Get Raw Weather Records (Legacy)**
```bash
GET http://localhost:8000/raw-weather?year=2012&month=1&limit=10
```

Retourneert data met **tekst waarden** (origineel formaat):
```json
{
  "temp": "11 °C",
  "humidity": "94%",
  "wind": "17 km/h"
}
```

### Voorbeeld Queries

**Alle gecleande data van januari 2012:**
```bash
curl "http://localhost:8000/weather?year=2012&month=1&limit=100"
```

**Eerste 10 records met numerieke waarden:**
```bash
curl "http://localhost:8000/weather?limit=10"
```

**Cleaned record met ID 42:**
```bash
curl "http://localhost:8000/weather/42"
```

**Dataset statistieken met temperatuur info:**
```bash
curl "http://localhost:8000/weather/stats/summary"
```

## Database Schema

### Table: `raw_weather` (Legacy)

| Kolom        | Type      | Beschrijving                        |
|--------------|-----------|-------------------------------------|
| id           | SERIAL    | Primary key                         |
| clock        | TEXT      | Tijd van meting (raw string)        |
| temp         | TEXT      | Temperatuur met eenheid (bijv. "11 °C") |
| weather      | TEXT      | Weer beschrijving                   |
| wind         | TEXT      | Wind met eenheid (bijv. "17 km/h")  |
| humidity     | TEXT      | Luchtvochtigheid (bijv. "94%")      |
| barometer    | TEXT      | Luchtdruk (bijv. "1011 mbar")       |
| visibility   | TEXT      | Zicht (bijv. "5 km")                |
| year         | INTEGER   | Jaar                                |
| month        | INTEGER   | Maand (1-12)                        |
| day          | INTEGER   | Dag (1-31)                          |
| source_file  | VARCHAR   | Naam van CSV bron                   |
| ingested_at  | TIMESTAMP | Ingest timestamp                    |

**Indexes**: `year`, `month`, `day`, `(year, month, day)`, `source_file`, `ingested_at`

### Table: `weather_cleaned` (Primair - Cleaned Data)

| Kolom          | Type       | Beschrijving                        |
|----------------|------------|-------------------------------------|
| id             | SERIAL     | Primary key                         |
| clock          | TEXT       | Tijd van meting                     |
| weather        | TEXT       | Weer beschrijving                   |
| year           | INTEGER    | Jaar (NOT NULL)                     |
| month          | INTEGER    | Maand (NOT NULL, 1-12)              |
| day            | INTEGER    | Dag (NOT NULL, 1-31)                |
| **temp_c**     | NUMERIC    | **Temperatuur in Celsius (numeriek)** |
| **humidity_pct** | NUMERIC  | **Luchtvochtigheid percentage (numeriek)** |
| **wind_kmh**   | NUMERIC    | **Windsnelheid in km/h (numeriek)** |
| **barometer_mbar** | NUMERIC | **Luchtdruk in mbar (numeriek)**   |
| **visibility_km** | NUMERIC  | **Zicht in km (numeriek)**         |
| source_file    | VARCHAR    | Naam van CSV bron                   |
| ingested_at    | TIMESTAMP  | Originele ingest timestamp          |
| transformed_at | TIMESTAMP  | Transformatie timestamp             |

**Indexes**: `year`, `month`, `day`, `(year, month, day)`, `temp_c`, `source_file`, `transformed_at`

### Table: `raw_weather_staging` (Tijdelijk - Pipeline)

Identiek aan `raw_weather`, gebruikt voor tijdelijke opslag tijdens pipeline processing.

### Table: `pipeline_metadata` (Airflow Tracking)

| Kolom               | Type      | Beschrijving                        |
|---------------------|-----------|-------------------------------------|
| id                  | SERIAL    | Primary key                         |
| run_id              | TEXT      | Airflow run identifier (UNIQUE)     |
| dag_id              | TEXT      | DAG naam                            |
| execution_date      | TIMESTAMP | Pipeline execution tijd             |
| start_time          | TIMESTAMP | Start tijd                          |
| end_time            | TIMESTAMP | Eind tijd (NULL als running)        |
| status              | TEXT      | running/success/failed/skipped      |
| records_downloaded  | INTEGER   | Aantal gedownloade files            |
| records_staged      | INTEGER   | Aantal records in staging           |
| records_transformed | INTEGER   | Aantal getransformeerde records     |
| records_loaded      | INTEGER   | Aantal records in cleaned tabel     |
| quality_report      | JSONB     | Data quality metrics                |
| error_message       | TEXT      | Error details (NULL bij success)    |

**Indexes**: `run_id`, `dag_id`, `status`, `execution_date`

## Data Pipeline (Airflow)

### Airflow DAG: `weather_data_pipeline`

De volledige data pipeline wordt georchestreerd door Apache Airflow. De pipeline wordt **handmatig getriggerd** via de Airflow Web UI.

**DAG Configuratie:**
- **Schedule**: `None` (manual trigger only)
- **Retries**: 1 per task met 5 minuten delay
- **Executor**: LocalExecutor

### Pipeline Tasks

#### 1. download_kaggle_data
- **Doel**: Download dataset van Kaggle
- **Duur**: ~30-60 seconden (bij eerste keer)
- **Logica**: 
  - Check of CSV's in `data/` folder bestaan
  - Skip download als files aanwezig
  - Download + unzip van Kaggle API anders
- **Output**: XCom met download statistics

#### 2. validate_csv_files
- **Doel**: Valideer CSV bestand integriteit
- **Checks**:
  - Bestaan vereiste files?
  - Zijn required kolommen aanwezig?
  - Wat is file size en row count?
- **Faalt bij**: Missing files of invalid schema
- **Output**: XCom met validation results

#### 3. load_to_staging
- **Doel**: Laad raw CSV data naar `raw_weather_staging` tabel
- **Logica**:
  - Truncate staging table
  - Read CSV met pandas (`;` separator)
  - Handle missende kolommen (future2.csv)
  - Bulk insert met psycopg2 (1000 records/batch)
- **Output**: XCom met staging statistics

#### 4. transform_and_load
- **Doel**: Parse tekst waarden → numeriek en laad naar `weather_cleaned`
- **Transformaties**:
  - "11 °C" → `temp_c = 11.0`
  - "94%" → `humidity_pct = 94.0`
  - "17 km/h" → `wind_kmh = 17.0`
  - "1011 mbar" → `barometer_mbar = 1011.0`
  - "5 km" → `visibility_km = 5.0`
- **Processing**: In chunks van 10,000 records
- **Output**: XCom met transformation statistics

#### 5. generate_quality_report
- **Doel**: Genereer data quality metrics
- **Metrics**:
  - Null percentages per kolom
  - Min/Max/Mean/Median waarden
  - Record counts per source file
  - Date range coverage
- **Output**: XCom met quality report (JSON)

#### 6. update_pipeline_metadata
- **Doel**: Log complete run informatie naar database
- **Stored**:
  - Run ID, execution date, status
  - Record counts (downloaded, staged, transformed)
  - Quality report (JSONB)
  - Error messages (bij failure)
- **Table**: `pipeline_metadata`

### Task Dependencies

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

Alle tasks zijn **sequentieel** - elk wacht op success van vorige task.

### Transformatie Logica

**Module**: `airflow/dags/transformations/weather_transform.py`

**Functies:**
- `parse_temperature()`: "11 °C" → 11.0, "−5 °C" → -5.0
- `parse_humidity()`: "94%" → 94.0 (validatie: 0-100)
- `parse_wind_speed()`: "17 km/h" → 17.0, "Calm" → 0.0
- `parse_barometer()`: "1011 mbar" → 1011.0 (validatie: 900-1100)
- `parse_visibility()`: "5 km" → 5.0
- `transform_weather_dataframe()`: Bulk transformatie van DataFrame
- `get_data_quality_report()`: Genereer quality metrics

**Error Handling:**
- Invalid values → `None` (NULL in database)
- Missing values → `None`
- Out-of-range values → `None`

## Stop de Stack

```powershell
docker compose down
```

Om ook de database volumes te verwijderen:

```powershell
docker compose down -v
```

**Let op**: Dit verwijdert **alle data** inclusief Airflow metadata en weather data!

## Development

### Lokaal Draaien (zonder Docker)

**Optie 1: Alleen API lokaal**

1. Start infrastructuur:
```powershell
docker compose up postgres airflow-postgres -d
```

2. Installeer Python dependencies:
```powershell
cd api
pip install -r requirements.txt
```

3. Run API lokaal:
```powershell
$env:DB_HOST="localhost"
$env:DB_NAME="weather_db"
uvicorn main:app --reload
```

**Optie 2: Airflow lokaal (gevorderd)**

Zie [Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html)

### DAG Development

1. Edit files in `airflow/dags/`
2. Airflow detecteert wijzigingen automatisch (~30 seconden)
3. Refresh browser op `http://localhost:8080`
4. Check DAG import errors in Airflow UI

**DAG testen:**
```powershell
# In Airflow container
docker exec -it airflow-scheduler bash
airflow dags test weather_data_pipeline 2024-01-01
```

### Transformatie Tests

```powershell
cd airflow/dags/transformations
python -m doctest weather_transform.py -v
```

### Logs Bekijken

```powershell
# Airflow logs
docker compose logs -f airflow-webserver
docker compose logs -f airflow-scheduler

# API logs
docker compose logs -f api

# Database logs
docker compose logs -f postgres
docker compose logs -f airflow-postgres
```

## Troubleshooting

### Airflow Issues

**Airflow Web UI niet toegankelijk:**
- Wacht 2-3 minuten na `docker compose up` voor volledige initialisatie
- Check logs: `docker compose logs airflow-webserver`
- Check of port 8080 vrij is: `netstat -an | findstr 8080`
- Check container status: `docker compose ps`

**DAG niet zichtbaar in UI:**
- Check DAG import errors in Airflow UI (rode banner)
- Check scheduler logs: `docker compose logs airflow-scheduler`
- Verify DAG file syntax: `docker exec airflow-scheduler airflow dags list`
- Check file permissions in `airflow/dags/`

**Task faalt met database error:**
- Verify Airflow kan postgres bereiken
- Check environment variables in docker-compose.yml
- Test connectie: `docker exec airflow-scheduler ping postgres`

**"Permission denied" errors:**
- Op Windows: zet `AIRFLOW_UID=50000` in `.env`
- Run: `docker compose down -v && docker compose up --build`

### Data Pipeline Issues

**Download failed - Kaggle API error:**
- Check of `.env` bestaat en credentials correct zijn
- Alternatief: plaats `kaggle.json` in `kaggle/` folder
- Verifieer credentials op https://www.kaggle.com/settings
- Check task logs in Airflow UI

**Validation task faalt:**
- Check of CSV's in `data/` folder staan
- Verify CSV format (semicolon separated)
- Check logs voor specifieke missende kolommen

**Transform task traag:**
- Normaal voor 133k+ records (~2-5 minuten)
- Monitor progress in task logs
- Check database CPU/memory met `docker stats`

**API retourneert lege results:**
- Check of pipeline succesvol gerund heeft
- Verify `weather_cleaned` tabel heeft data:
  ```sql
  docker exec -it weather-postgres psql -U weather_user -d weather_db -c "SELECT COUNT(*) FROM weather_cleaned;"
  ```
- Check API logs voor errors

### Database Issues

**API start niet:**
- Check of Postgres healthy is: `docker compose ps`
- Wait for initialization: ~10 seconden
- Check database logs: `docker compose logs postgres`

**"relation does not exist" errors:**
- Database schema niet geïnitialiseerd
- Recreate database: `docker compose down -v && docker compose up --build`
- Check `init.sql` werd uitgevoerd

**Dataset opnieuw verwerken:**
```powershell
# Option 1: Via Airflow (clean)
# Trigger DAG opnieuw in Web UI - truncate tables automatisch

# Option 2: Manual reset
docker exec -it weather-postgres psql -U weather_user -d weather_db
# In psql:
TRUNCATE TABLE weather_cleaned, raw_weather_staging, pipeline_metadata CASCADE;
# Exit en trigger DAG opnieuw
```

## Tech Stack

- **Orchestration**: Apache Airflow 2.8.1 (LocalExecutor)
- **Data Source**: Kaggle API (automated download)
- **Databases**: 
  - PostgreSQL 16 (Alpine) - Weather data
  - PostgreSQL 16 (Alpine) - Airflow metadata
- **API**: Python 3.11 + FastAPI + SQLAlchemy 2.0
- **Data Processing**: Python 3.11 + Pandas + NumPy
- **Transformation**: Custom Python modules (regex, type conversion)
- **Containerization**: Docker & Docker Compose
- **Workflow Visualization**: Airflow Web UI (Graph, Tree, Gantt views)