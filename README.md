# Weather Raw Data API
gebruikte dataset: https://www.kaggle.com/datasets/ramima/weather-dataset-in-antwerp-belgium?resource=download

Een Docker-gebaseerde data engineering oplossing voor het opslaan en ophalen van ruwe weerdata uit Antwerpen. De data wordt automatisch van Kaggle gedownload, vervolgens ingeladen naar een PostgreSQL database en via een FastAPI REST API toegankelijk gemaakt.

## Architectuur

![architectuur](/images/architectuur.png)

### Components

- **Download Container**: Python script dat dataset automatisch downloadt van Kaggle
- **Postgres**: Raw storage database met `raw_weather` tabel
- **Ingest Container**: Python script dat CSV's parset en in database laadt
- **FastAPI**: REST API met clean architecture (routers → services → repositories)


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

### Setup Kaggle API Credentials

De dataset wordt automatisch gedownload van Kaggle. Je hebt hiervoor een API key nodig:

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
   ```

#### Optie 2: Via kaggle.json bestand

1. Download je `kaggle.json` van https://www.kaggle.com/settings
2. Plaats het in de `kaggle/` folder:
   ```powershell
   mkdir kaggle
   # Kopieer kaggle.json naar kaggle/ folder
   ```
3. De download container leest automatisch deze credentials

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

Dit zal **automatisch**:
- Dataset downloaden van Kaggle (weather-dataset-in-antwerp-belgium)
- Postgres database starten
- Database schema initialiseren (`init.sql`)
- CSV's inladen via ingest container
- FastAPI starten op `http://localhost:8000`

**Data Download:** Als de CSV's al in `data/` folder staan, wordt de download overgeslagen.

### API Endpoints

Bezoek `http://localhost:8000/docs` voor interactieve API documentatie (Swagger UI).

#### Beschikbare Endpoints

**Health Check**
```bash
GET http://localhost:8000/health
```

**Get All Weather Records (met filtering)**
```bash
GET http://localhost:8000/raw-weather?year=2025&month=1&limit=10
```

Query parameters:
- `year`: Filter op jaar (optioneel)
- `month`: Filter op maand 1-12 (optioneel)
- `day`: Filter op dag 1-31 (optioneel)
- `source_file`: Filter op bron bestand (optioneel)
- `limit`: Max aantal records (default: 100, max: 10000)
- `offset`: Paginatie offset (default: 0)

**Get Single Record by ID**
```bash
GET http://localhost:8000/raw-weather/1
```

**Get Dataset Statistics**
```bash
GET http://localhost:8000/raw-weather/stats/summary
```

Geeft terug:
- Totaal aantal records
- Datum range (min/max)
- Lijst van source files

### Voorbeeld Queries

**Alle data van januari 2025:**
```bash
curl "http://localhost:8000/raw-weather?year=2025&month=1&limit=100"
```

**Eerste 10 records van specific source file:**
```bash
curl "http://localhost:8000/raw-weather?source_file=weather_in_Antwerp.csv&limit=10"
```

**Record met ID 42:**
```bash
curl "http://localhost:8000/raw-weather/42"
```

**Dataset statistieken:**
```bash
curl "http://localhost:8000/raw-weather/stats/summary"
```

## Database Schema

**Table: `raw_weather`**

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

## Data Pipeline

### 1. Download Container
- Draait als eerste (voor ingest)
- Gebruikt Kaggle API om dataset te downloaden
- Dataset: `ramima/weather-dataset-in-antwerp-belgium`
- Download naar `data/` folder
- Skip als CSV's al bestaan
- Stopt automatisch na completion

### 2. Ingest Container
- Wacht tot download compleet is
- Leest beide CSV's met pandas (`;` separator)
- Verwijdert lege index kolom
- Voegt metadata toe (`source_file`, `ingested_at`)
- Handelt missende kolommen af (barometer/visibility in future2.csv)
- Bulk insert met `psycopg2` (1000 records per batch)
- Skip re-ingest als data al bestaat
- Stopt automatisch na completion

### 3. API Container
- Start nadat ingest compleet is
- Serveert data via REST endpoints
- Blijft draaien voor requests

## Stop de Stack

```powershell
docker compose down
```

Om ook de database volume te verwijderen:

```powershell
docker compose down -v
```

## Development

### Lokaal Draaien (zonder Docker)

1. Start Postgres:
```powershell
docker compose up postgres -d
```

2. Installeer Python dependencies:
```powershell
cd api
pip install -r requirements.txt
```

3. Run API lokaal:
```powershell
$env:DB_HOST="localhost"
uvicorn main:app --reload
```

### Logs Bekijken

```powershell
# Alle services
docker compose logs -f

# Specifieke service
docker compose logs -f api
docker compose logs -f ingest
docker compose logs -f download
docker compose logs -f postgres
```

## Troubleshooting

**Download failed - Kaggle API error:**
- Check of `.env` bestaat en credentials correct zijn
- Alternatief: plaats `kaggle.json` in `kaggle/` folder
- Verifieer credentials op https://www.kaggle.com/settings
- Check logs: `docker compose logs download`

**Ingest draait opnieuw bij restart:**
- Data wordt alleen ingevoerd als `raw_weather` tabel leeg is
- Bij herstart wordt de data behouden via Docker volume

**API start niet:**
- Check of Postgres healthy is: `docker compose ps`
- Check ingest logs: `docker compose logs ingest`
- Check download logs: `docker compose logs download`

**Database connectie errors:**
- Wacht ~5-10 seconden na `docker compose up` voor Postgres init
- Check environment variabelen in `docker-compose.yml`

**Dataset opnieuw downloaden:**
```powershell
# Verwijder CSV's
Remove-Item data/*.csv
# Herstart
docker compose up --build
```

## Tech Stack

- **Data Source**: Kaggle API (automated download)
- **Database**: PostgreSQL 16 (Alpine)
- **API**: Python 3.11 + FastAPI + SQLAlchemy 2.0
- **Download**: Python 3.11 + Kaggle API
- **Ingest**: Python 3.11 + Pandas + psycopg2
- **Containerization**: Docker & Docker Compose