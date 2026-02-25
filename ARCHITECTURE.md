# Clean Architecture Uitleg - Weather Raw Data API

## Inhoudsopgave
1. [Project Overview & Data Pipeline](#project-overview--data-pipeline)
2. [Wat is Clean Architecture?](#wat-is-clean-architecture)
3. [De 4 Lagen in dit Project](#de-4-lagen-in-dit-project)
4. [Data Flow: Van HTTP Request naar Database](#data-flow-van-http-request-naar-database)
5. [Laag voor Laag Uitleg](#laag-voor-laag-uitleg)
6. [Dependency Flow](#dependency-flow)
7. [Waarom deze Architectuur?](#waarom-deze-architectuur)
8. [Code Voorbeelden](#code-voorbeelden)

---

## Project Overview & Data Pipeline

### Volledige Architectuur

Dit project bestaat uit **4 Docker containers** die samen een complete data engineering pipeline vormen:

```
┌─────────────────────────────────────────────────────────────┐
│                     DOCKER COMPOSE                           │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────┐  │
│  │   DOWNLOAD   │─────▶│    INGEST    │─────▶│   API    │  │
│  │  Container   │      │  Container   │      │Container │  │
│  └──────┬───────┘      └──────┬───────┘      └────┬─────┘  │
│         │                     │                    │        │
│         ▼                     ▼                    ▼        │
│  ┌──────────────┐      ┌──────────────┐                    │
│  │ data/ folder │      │  POSTGRES DB │                    │
│  │  (CSV files) │      │  raw_weather │                    │
│  └──────────────┘      └──────────────┘                    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Container Flow & Verantwoordelijkheden

#### 1️⃣ **Download Container**
**Bestand:** `download/download_data.py`  
**Wanneer:** Start als eerste, voor ingest  
**Doel:** Automatisch dataset downloaden van Kaggle

**Proces:**
```
START
  ↓
Check: Bestaan CSV's al in data/?
  ├─ Ja  → Skip download, stop container
  └─ Nee → Download van Kaggle API
             ↓
      Unzip naar data/ folder
             ↓
      Verify: Check of CSV's aanwezig zijn
             ↓
      Stop container (completion)
```

**Environment vereisten:**
- `KAGGLE_USERNAME` - Kaggle account username
- `KAGGLE_KEY` - Kaggle API key
- Of: kaggle.json bestand gemount

**Output:** 
- `data/weather_in_Antwerp.csv` (~133k regels)
- `data/weather_in_Antwerp_future2.csv` (~146 regels)

---

#### 2️⃣ **Postgres Container**
**Image:** `postgres:16-alpine`  
**Wanneer:** Start parallel met download  
**Doel:** Raw data storage database

**Proces:**
```
START
  ↓
Initialiseer database "weather_db"
  ↓
Run init.sql script:
  - CREATE TABLE raw_weather
  - CREATE INDEXES (year, month, day, source_file)
  ↓
Healthcheck: pg_isready
  ↓
READY (andere containers kunnen connecten)
```

**Schema:**
```sql
CREATE TABLE raw_weather (
    id SERIAL PRIMARY KEY,
    clock TEXT,
    temp TEXT,               -- Raw: "11 °C"
    weather TEXT,            -- Raw: "Mostly cloudy."
    wind TEXT,               -- Raw: "17 km/h"
    humidity TEXT,           -- Raw: "94%"
    barometer TEXT,          -- Raw: "1011 mbar"
    visibility TEXT,         -- Raw: "5 km"
    year INTEGER,
    month INTEGER,
    day INTEGER,
    source_file VARCHAR(255),
    ingested_at TIMESTAMP
);
```

---

#### 3️⃣ **Ingest Container**
**Bestand:** `ingest/load_raw_data.py`  
**Wanneer:** Start nadat download compleet EN postgres healthy  
**Doel:** CSV's parsen en laden in database

**Dependencies:**
```yaml
depends_on:
  postgres:
    condition: service_healthy
  download:
    condition: service_completed_successfully
```

**Proces:**
```
START
  ↓
Wait for: Postgres healthy + Download complete
  ↓
Connect to database
  ↓
Check: Is raw_weather tabel leeg?
  ├─ Nee → Skip ingest (data bestaat al)
  └─ Ja  → Continue
             ↓
      Read CSV 1: weather_in_Antwerp.csv
        - Parse met pandas (sep=';')
        - Verwijder lege index kolom
        - Voeg metadata toe (source_file, ingested_at)
             ↓
      Bulk insert (1000 records/batch met psycopg2)
             ↓
      Read CSV 2: weather_in_Antwerp_future2.csv
        - Zelfde proces
        - Handle missende kolommen (barometer/visibility)
             ↓
      Commit transaction
             ↓
      Stop container (completion)
```

**Error Handling:**
- Rollback bij errors
- Retry logic voor database connectie (5 pogingen)
- Skip bij lege CSV's

---

#### 4️⃣ **API Container**
**Bestand:** `api/main.py` + clean architecture  
**Wanneer:** Start nadat ingest compleet  
**Doel:** REST API voor data toegang

**Dependencies:**
```yaml
depends_on:
  postgres:
    condition: service_healthy
  ingest:
    condition: service_completed_successfully
```

**Proces:**
```
START
  ↓
Wait for: Postgres healthy + Ingest complete
  ↓
FastAPI startup:
  - Load routers
  - Setup database connection pool
  - Initialize SQLAlchemy models
  ↓
Start uvicorn server (port 8000)
  ↓
READY - Accept HTTP requests
  ↓
BLIJFT DRAAIEN (totdat gestopt)
```

**Endpoints:**
- `GET /health` - Health check
- `GET /raw-weather` - Lijst met filters/paginatie
- `GET /raw-weather/{id}` - Enkel record
- `GET /raw-weather/stats/summary` - Statistieken
- `GET /docs` - Swagger UI

**Architecture:** Clean Architecture (zie volgende secties)

---

### Volledige Pipeline Flow

**Bij `docker compose up --build`:**

```
Time  │ Container      │ Status      │ Actie
──────┼────────────────┼─────────────┼──────────────────────────────
0s    │ Postgres       │ Starting    │ Database init
0s    │ Download       │ Starting    │ Check for CSV's
──────┼────────────────┼─────────────┼──────────────────────────────
2s    │ Postgres       │ Healthy     │ ✓ Ready for connections
3s    │ Download       │ Running     │ Downloading from Kaggle...
──────┼────────────────┼─────────────┼──────────────────────────────
15s   │ Download       │ Completed   │ ✓ CSV's downloaded to data/
15s   │ Ingest         │ Starting    │ Wait resolved (deps ready)
──────┼────────────────┼─────────────┼──────────────────────────────
16s   │ Ingest         │ Running     │ Loading CSV → Postgres
──────┼────────────────┼─────────────┼──────────────────────────────
45s   │ Ingest         │ Completed   │ ✓ 133k+ records ingested
45s   │ API            │ Starting    │ Wait resolved (deps ready)
──────┼────────────────┼─────────────┼──────────────────────────────
46s   │ API            │ Running     │ ✓ Listening on :8000
──────┼────────────────┼─────────────┼──────────────────────────────
```

**Final State:**
- Download: Exited (success)
- Ingest: Exited (success)
- Postgres: Running
- API: Running

---

### Bij Herstart (`docker compose restart`)

```
Time  │ Container      │ Status      │ Actie
──────┼────────────────┼─────────────┼──────────────────────────────
0s    │ Postgres       │ Starting    │ Volume gemount (data blijft)
0s    │ Download       │ Starting    │ Check CSV's...
──────┼────────────────┼─────────────┼──────────────────────────────
2s    │ Postgres       │ Healthy     │ ✓ Existing data loaded
2s    │ Download       │ Completed   │ SKIP (CSV's exist)
2s    │ Ingest         │ Starting    │ Dependencies ready
──────┼────────────────┼─────────────┼──────────────────────────────
3s    │ Ingest         │ Completed   │ SKIP (DB has data)
3s    │ API            │ Starting    │ Dependencies ready
──────┼────────────────┼─────────────┼──────────────────────────────
4s    │ API            │ Running     │ ✓ Ready
──────┼────────────────┼─────────────┼──────────────────────────────
```

**Voordelen:**
- Data persistent (Postgres volume)
- Snelle herstart (skips downloads/ingests)
- Idempotent (meerdere runs = zelfde resultaat)

---

### Wat is het verschil tussen Pipeline en Clean Architecture?

| Aspect | Pipeline | Clean Architecture |
|--------|----------|-------------------|
| **Scope** | Hele project (4 containers) | Alleen API container |
| **Focus** | Data flow & orchestratie | Code organisatie |
| **Componenten** | Download → Ingest → API → DB | Routers → Services → Repos → Models |
| **Niveau** | Infrastructure/DevOps | Software Design |
| **Tools** | Docker Compose | Python packages/modules |

**Pipeline = Hoe data het systeem instroomt**  
**Clean Architecture = Hoe de API code georganiseerd is**

De rest van dit document focust op de **Clean Architecture** binnen de **API container**.

---

## Wat is Clean Architecture?

Clean Architecture is een **software design patroon** waarbij je code opdeelt in **lagen** met **duidelijke verantwoordelijkheden**. Elke laag heeft één specifieke taak en weet niks van de implementatie details van andere lagen.

### Kernprincipes:
- **Separation of Concerns**: Elke laag heeft 1 doel
- **Dependency Rule**: Binnenste lagen weten niks van buitenste lagen
- **Testability**: Elke laag kan apart getest worden
- **Maintainability**: Wijzigingen in 1 laag breken andere lagen niet

### In dit project:
```
┌──────────────────────────────────────────────┐
│           ROUTERS (Presentation)             │  ← Buitenste laag
│  - HTTP endpoints                            │
│  - Request/Response handling                 │
│  - Input validatie                           │
└─────────────────┬────────────────────────────┘
                  │ depends on
                  ↓
┌──────────────────────────────────────────────┐
│           SERVICES (Business Logic)          │
│  - Orchestratie                              │
│  - Data transformatie                        │
│  - Business rules                            │
└─────────────────┬────────────────────────────┘
                  │ depends on
                  ↓
┌──────────────────────────────────────────────┐
│        REPOSITORIES (Data Access)            │
│  - Database queries                          │
│  - Data filtering                            │
│  - CRUD operaties                            │
└─────────────────┬────────────────────────────┘
                  │ depends on
                  ↓
┌──────────────────────────────────────────────┐
│      MODELS & DATABASE (Infrastructure)      │  ← Binnenste laag
│  - Database schema (SQLAlchemy)              │
│  - API schemas (Pydantic)                    │
│  - Database connectie                        │
└──────────────────────────────────────────────┘
```

---

## De 4 Lagen in dit Project

### Laag 1: **ROUTERS** (`api/routers/`)
**Wat:** HTTP endpoints die requests ontvangen en responses terugsturen  
**Waar:** `api/routers/weather_router.py`  
**Verantwoordelijkheid:**
- Definieert URL routes (`/raw-weather`, `/raw-weather/{id}`)
- Valideert HTTP input (query parameters, path parameters)
- Roept Services aan
- Formatteert response (JSON)

### Laag 2: **SERVICES** (`api/services/`)
**Wat:** Business logica en orchestratie  
**Waar:** `api/services/weather_service.py`  
**Verantwoordelijkheid:**
- Bevat de "regels" van de applicatie
- Roept Repository aan voor data
- Transformeert data naar gewenste format
- Combineert meerdere repository calls indien nodig

### Laag 3: **REPOSITORIES** (`api/repositories/`)
**Wat:** Data access laag  
**Waar:** `api/repositories/weather_repository.py`  
**Verantwoordelijkheid:**
- Alle database queries (SELECT, INSERT, UPDATE, DELETE)
- Filtering en paginatie logic
- Geen business logic, enkel data ophalen/opslaan

### Laag 4: **MODELS & DATABASE** (`api/models/`, `api/database.py`)
**Wat:** Data structuren en database setup  
**Waar:** 
- `api/models/db_models.py` - SQLAlchemy models (database schema)
- `api/models/schemas.py` - Pydantic models (API validatie)
- `api/database.py` - Database connectie

**Verantwoordelijkheid:**
- Definieert hoe data eruit ziet
- Database connectie management
- Type definities

---

## Data Flow: Van HTTP Request naar Database

Laten we een **compleet voorbeeld** doorlopen: gebruiker wil alle weather data van januari 2025 ophalen.

### Request:
```http
GET http://localhost:8000/raw-weather?year=2025&month=1&limit=50
```

### Stap-voor-stap flow:

```
┌─────────────────────────────────────────────────────────────┐
│ 1. HTTP REQUEST komt binnen bij FastAPI                     │
└────────────────────────┬────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. ROUTER LAAG (weather_router.py)                          │
│                                                              │
│ Functie: get_weather_records()                              │
│                                                              │
│ Wat gebeurt er:                                              │
│ - FastAPI parsed query params: year=2025, month=1, limit=50 │
│ - Valideert input (year tussen 1900-2100, month tussen 1-12)│
│ - Maakt WeatherQueryParams object aan                       │
│ - Injecteert database sessie via Depends(get_db)            │
│                                                              │
│ Code:                                                        │
│   params = WeatherQueryParams(                              │
│       year=2025,                                             │
│       month=1,                                               │
│       limit=50,                                              │
│       offset=0                                               │
│   )                                                          │
│                                                              │
│ - Maakt WeatherService instance aan                         │
│ - Roept service.get_weather_records(params) aan             │
└────────────────────────┬────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. SERVICE LAAG (weather_service.py)                        │
│                                                              │
│ Functie: get_weather_records(params)                        │
│                                                              │
│ Wat gebeurt er:                                              │
│ - Ontvangt WeatherQueryParams object                        │
│ - Maakt WeatherRepository instance aan (met db sessie)      │
│ - Roept repository.get_all() aan met filters                │
│                                                              │
│ Code:                                                        │
│   records, total = self.repository.get_all(                 │
│       year=params.year,        # 2025                       │
│       month=params.month,      # 1                          │
│       limit=params.limit,      # 50                         │
│       offset=params.offset     # 0                          │
│   )                                                          │
│                                                              │
│ - Wacht op resultaat van repository                         │
└────────────────────────┬────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. REPOSITORY LAAG (weather_repository.py)                  │
│                                                              │
│ Functie: get_all(year, month, limit, offset)                │
│                                                              │
│ Wat gebeurt er:                                              │
│ - Bouwt SQLAlchemy query op                                 │
│                                                              │
│ Code:                                                        │
│   query = self.db.query(RawWeather)                         │
│                                                              │
│ - Voegt filters toe als ze gegeven zijn:                    │
│   query = query.filter(RawWeather.year == 2025)             │
│   query = query.filter(RawWeather.month == 1)               │
│                                                              │
│ - Telt totaal aantal records (voor paginatie):              │
│   total = query.count()  # bijv. 2419                       │
│                                                              │
│ - Voegt ordering en paginatie toe:                          │
│   records = query                                            │
│       .order_by(RawWeather.year.desc(), ...)                │
│       .limit(50)                                             │
│       .offset(0)                                             │
│       .all()                                                 │
│                                                              │
│ - Voert DAADWERKELIJKE database query uit                   │
└────────────────────────┬────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│ 5. DATABASE LAAG (SQLAlchemy + Postgres)                    │
│                                                              │
│ Wat gebeurt er:                                              │
│ - SQLAlchemy zet de query om naar SQL:                      │
│                                                              │
│   SELECT * FROM raw_weather                                 │
│   WHERE year = 2025 AND month = 1                           │
│   ORDER BY year DESC, month DESC, day DESC, id DESC         │
│   LIMIT 50 OFFSET 0                                         │
│                                                              │
│ - Query wordt gestuurd naar Postgres container              │
│ - Postgres voert query uit op raw_weather tabel             │
│ - Postgres gebruikt indexes (idx_raw_weather_date)          │
│ - Returns 50 rows                                            │
└────────────────────────┬────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│ 6. DATA KOMT TERUG (Database → Repository)                  │
│                                                              │
│ Repository ontvangt:                                         │
│ - records: List[RawWeather] (50 SQLAlchemy objects)         │
│ - total: 2419 (totaal aantal matching records)              │
│                                                              │
│ Returns tuple: (records, total)                             │
└────────────────────────┬────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│ 7. SERVICE TRANSFORMEERT DATA                               │
│                                                              │
│ Service ontvangt (records, total) van repository             │
│                                                              │
│ Wat gebeurt er:                                              │
│ - Converteert SQLAlchemy objects naar Pydantic schemas:     │
│                                                              │
│   data = [                                                   │
│       RawWeatherResponse.model_validate(record)             │
│       for record in records                                  │
│   ]                                                          │
│                                                              │
│ - Maakt PaginatedResponse object:                           │
│                                                              │
│   return PaginatedResponse(                                 │
│       total=2419,                                            │
│       limit=50,                                              │
│       offset=0,                                              │
│       data=data  # List van 50 RawWeatherResponse objects   │
│   )                                                          │
└────────────────────────┬────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│ 8. ROUTER STUURT RESPONSE                                   │
│                                                              │
│ Router ontvangt PaginatedResponse van service                │
│                                                              │
│ Wat gebeurt er:                                              │
│ - FastAPI converteert PaginatedResponse naar JSON           │
│ - Zet HTTP status code (200 OK)                             │
│ - Zet Content-Type header (application/json)                │
│ - Stuurt response naar client                               │
└────────────────────────┬────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│ 9. CLIENT ONTVANGT JSON                                     │
│                                                              │
│ {                                                            │
│   "total": 2419,                                             │
│   "limit": 50,                                               │
│   "offset": 0,                                               │
│   "data": [                                                  │
│     {                                                        │
│       "id": 123,                                             │
│       "clock": "00:53",                                      │
│       "temp": "11 °C",                                       │
│       "weather": "Mostly cloudy.",                           │
│       "wind": "17 km/h",                                     │
│       "humidity": "94%",                                     │
│       "year": 2025,                                          │
│       "month": 1,                                            │
│       "day": 15,                                             │
│       "source_file": "weather_in_Antwerp.csv",               │
│       "ingested_at": "2026-02-04T15:17:30"                  │
│     },                                                       │
│     ... 49 meer records ...                                 │
│   ]                                                          │
│ }                                                            │
└─────────────────────────────────────────────────────────────┘
```

---

## Laag voor Laag Uitleg

### 📌 LAAG 1: ROUTERS (Presentation Layer)

**Bestand:** `api/routers/weather_router.py`

**Rol:** De "voordeur" van je API. Ontvangt HTTP requests en stuurt HTTP responses.

**Wat doet deze laag:**
1. Definieert URL endpoints (`@router.get("/raw-weather")`)
2. Ontvangt en valideert input (query params, path params, body)
3. Injecteert dependencies (database sessie)
4. Roept de juiste Service functie aan
5. Formatteert de response als JSON
6. Handelt errors af (HTTP 404, 500, etc.)

**Wat doet deze laag NIET:**
- Geen database queries
- Geen business logica
- Weet niks van hoe data wordt opgehaald

**Voorbeeld code:**
```python
@router.get("/", response_model=PaginatedResponse)
def get_weather_records(
    year: Optional[int] = Query(None, ge=1900, le=2100),
    month: Optional[int] = Query(None, ge=1, le=12),
    day: Optional[int] = Query(None, ge=1, le=31),
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)  # Database sessie dependency
):
    # 1. Maak service instance
    service = WeatherService(db)
    
    # 2. Maak params object
    params = WeatherQueryParams(
        year=year,
        month=month,
        day=day,
        limit=limit,
        offset=offset
    )
    
    # 3. Roep service aan
    result = service.get_weather_records(params)
    
    # 4. FastAPI converteert automatisch naar JSON
    return result
```

**Waarom apart?**
- Als je van HTTP naar GraphQL wilt, verander je alleen de router
- Router logica (URL parsing) is gescheiden van business logica
- Makkelijk te testen met mock services

---

### 📌 LAAG 2: SERVICES (Business Logic Layer)

**Bestand:** `api/services/weather_service.py`

**Rol:** De "hersenen" van je applicatie. Bevat alle business logica.

**Wat doet deze laag:**
1. Implementeert business rules
2. Orchestreert meerdere repository calls
3. Transformeert data tussen formats
4. Combineert data uit verschillende bronnen
5. Handelt complexe operaties af

**Wat doet deze laag NIET:**
- Geen HTTP details (weet niet van requests/responses)
- Geen directe database queries
- Geen SQL schrijven

**Voorbeeld code:**
```python
class WeatherService:
    def __init__(self, db: Session):
        # Maakt repository instance aan
        self.repository = WeatherRepository(db)
    
    def get_weather_records(self, params: WeatherQueryParams) -> PaginatedResponse:
        # 1. Roep repository aan voor data
        records, total = self.repository.get_all(
            year=params.year,
            month=params.month,
            day=params.day,
            source_file=params.source_file,
            limit=params.limit,
            offset=params.offset
        )
        
        # 2. Transformeer SQLAlchemy objects naar Pydantic schemas
        data = [
            RawWeatherResponse.model_validate(record) 
            for record in records
        ]
        
        # 3. Bouw response object
        return PaginatedResponse(
            total=total,
            limit=params.limit,
            offset=params.offset,
            data=data
        )
    
    def get_statistics(self) -> dict:
        # Voorbeeld van meerdere repository calls combineren
        total_count = self.repository.get_total_count()
        min_date, max_date = self.repository.get_date_range()
        source_files = self.repository.get_source_files()
        
        # Business logic: combineer alles in 1 response
        return {
            "total_records": total_count,
            "date_range": {
                "min": min_date,
                "max": max_date
            },
            "source_files": source_files
        }
```

**Waarom apart?**
- Business logica kan herbruikt worden door meerdere routers
- Makkelijk te testen zonder HTTP
- Kan dezelfde service gebruiken voor CLI, GraphQL, etc.

---

### 📌 LAAG 3: REPOSITORIES (Data Access Layer)

**Bestand:** `api/repositories/weather_repository.py`

**Rol:** De "vertaler" tussen je applicatie en database. Alle database operaties gebeuren hier.

**Wat doet deze laag:**
1. Schrijft alle database queries (SQLAlchemy)
2. Implementeert CRUD operaties (Create, Read, Update, Delete)
3. Handelt filtering en sorting af
4. Implementeert paginatie
5. Returns ruwe database objecten (SQLAlchemy models)

**Wat doet deze laag NIET:**
- Geen data transformatie naar API schemas
- Geen business logica
- Geen HTTP details

**Voorbeeld code:**
```python
class WeatherRepository:
    def __init__(self, db: Session):
        self.db = db
    
    def get_all(
        self,
        year: Optional[int] = None,
        month: Optional[int] = None,
        day: Optional[int] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Tuple[List[RawWeather], int]:
        # 1. Start met base query
        query = self.db.query(RawWeather)
        
        # 2. Voeg filters toe (dynamisch)
        if year is not None:
            query = query.filter(RawWeather.year == year)
        if month is not None:
            query = query.filter(RawWeather.month == month)
        if day is not None:
            query = query.filter(RawWeather.day == day)
        
        # 3. Tel totaal (voor paginatie meta data)
        total = query.count()
        
        # 4. Voeg ordering toe
        query = query.order_by(
            RawWeather.year.desc(),
            RawWeather.month.desc(),
            RawWeather.day.desc(),
            RawWeather.id.desc()
        )
        
        # 5. Voeg paginatie toe en voer uit
        records = query.limit(limit).offset(offset).all()
        
        # 6. Return data en totaal count
        return records, total
    
    def get_by_id(self, weather_id: int) -> Optional[RawWeather]:
        # Simpele query: 1 record ophalen
        return self.db.query(RawWeather)\
            .filter(RawWeather.id == weather_id)\
            .first()
```

**Gegenereerde SQL:**
```sql
-- Voor get_all(year=2025, month=1, limit=50)
SELECT * FROM raw_weather
WHERE year = 2025 AND month = 1
ORDER BY year DESC, month DESC, day DESC, id DESC
LIMIT 50 OFFSET 0;

-- Voor get_by_id(weather_id=123)
SELECT * FROM raw_weather
WHERE id = 123
LIMIT 1;
```

**Waarom apart?**
- Als je van Postgres naar MySQL wilt, wijzig je alleen repository
- Database logica is geïsoleerd
- Makkelijk te testen met mock database

---

### 📌 LAAG 4: MODELS & DATABASE (Infrastructure Layer)

**Bestanden:** 
- `api/models/db_models.py` - Database schema
- `api/models/schemas.py` - API validation
- `api/database.py` - Database connectie

**Rol:** Definieert hoe data eruit ziet en beheert database connectie.

#### A. Database Models (`db_models.py`)

**Wat:** SQLAlchemy models - representatie van database tabellen

```python
from sqlalchemy import Column, Integer, String, Text, DateTime
from database import Base

class RawWeather(Base):
    __tablename__ = "raw_weather"
    
    # Kolom definities (exact zoals in database)
    id = Column(Integer, primary_key=True, index=True)
    clock = Column(Text)
    temp = Column(Text)
    weather = Column(Text)
    wind = Column(Text)
    humidity = Column(Text)
    barometer = Column(Text, nullable=True)
    visibility = Column(Text, nullable=True)
    year = Column(Integer, index=True)
    month = Column(Integer, index=True)
    day = Column(Integer, index=True)
    source_file = Column(String(255), nullable=False)
    ingested_at = Column(DateTime, default=datetime.utcnow)
```

**Wat doet dit:**
- Mapt Python class naar database tabel
- Definieert kolom types
- Definieert constraints (nullable, primary key)
- SQLAlchemy gebruikt dit om queries te bouwen

#### B. API Schemas (`schemas.py`)

**Wat:** Pydantic models - validatie en serialization voor API

```python
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class RawWeatherResponse(BaseModel):
    """Schema voor API response"""
    id: int
    clock: Optional[str] = None
    temp: Optional[str] = None
    weather: Optional[str] = None
    wind: Optional[str] = None
    humidity: Optional[str] = None
    year: Optional[int] = None
    month: Optional[int] = None
    day: Optional[int] = None
    source_file: str
    ingested_at: datetime
    
    class Config:
        from_attributes = True  # Kan SQLAlchemy object converteren

class WeatherQueryParams(BaseModel):
    """Schema voor query parameters"""
    year: Optional[int] = Field(None, ge=1900, le=2100)
    month: Optional[int] = Field(None, ge=1, le=12)
    day: Optional[int] = Field(None, ge=1, le=31)
    limit: int = Field(100, ge=1, le=10000)
    offset: int = Field(0, ge=0)
```

**Wat doet dit:**
- Valideert input (year moet tussen 1900-2100)
- Converteert SQLAlchemy objects naar JSON
- Type safety voor de hele applicatie
- Auto-generates OpenAPI docs

#### C. Database Connectie (`database.py`)

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

# Build connection string from environment
DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependency voor FastAPI
def get_db():
    db = SessionLocal()
    try:
        yield db  # Geef sessie aan endpoint
    finally:
        db.close()  # Sluit altijd af
```

**Wat doet dit:**
- Maakt connectie pool naar Postgres
- Beheert database sessies
- Zorgt dat connecties altijd worden gesloten
- Injecteerbaar via FastAPI Depends()

---

## Dependency Flow

**Belangrijk principe:** Dependencies gaan van buiten naar binnen.

```
ROUTERS
  ↓ imports & depends on
SERVICES
  ↓ imports & depends on
REPOSITORIES
  ↓ imports & depends on
MODELS & DATABASE
```

**Dit betekent:**
- Routers weten van Services (roepen ze aan)
- Services weten van Repositories (roepen ze aan)
- Repositories weten van Models (gebruiken ze in queries)
- **MAAR:** Models weten NIKS van Repositories
- **MAAR:** Repositories weten NIKS van Services
- **MAAR:** Services weten NIKS van Routers

**Voordeel:**
Als je Models wijzigt, moeten misschien Repositories aangepast, maar Services/Routers blijven intact (zolang de repository interface hetzelfde blijft).

---

## Waarom deze Architectuur?

### ✅ Voordelen

#### 1. **Testbaarheid**
Elke laag kan apart getest worden:

```python
# Test repository zonder service/router
def test_repository_get_all():
    repo = WeatherRepository(mock_db)
    records, total = repo.get_all(year=2025)
    assert total > 0

# Test service zonder router
def test_service_statistics():
    service = WeatherService(mock_db)
    stats = service.get_statistics()
    assert "total_records" in stats

# Test router met mock service
def test_router_endpoint():
    response = client.get("/raw-weather?year=2025")
    assert response.status_code == 200
```

#### 2. **Onderhoudbaarheid**
Wijzigingen zijn geïsoleerd:

- **Wijzig database?** → Alleen Repository aanpassen
- **Wijzig business logic?** → Alleen Service aanpassen
- **Wijzig API format?** → Alleen Router + Schemas aanpassen

#### 3. **Herbruikbaarheid**
Service kan gebruikt worden door:
- REST API (FastAPI router)
- GraphQL endpoint
- CLI tool
- Background job
- Unit tests

Allemaal zonder duplicatie van business logica!

#### 4. **Duidelijkheid**
Nieuwe developers zien meteen:
- `routers/` → Hier zijn de endpoints
- `services/` → Hier is de logica
- `repositories/` → Hier zijn de queries
- `models/` → Hier zijn de data structuren

#### 5. **Schaalbaarheid**
Als project groeit:
- Voeg nieuwe routers toe zonder bestaande te wijzigen
- Voeg nieuwe services toe voor nieuwe features
- Repositories blijven klein en focused

---

## Code Voorbeelden

### Voorbeeld 1: Simpele GET by ID

**Request:** `GET /raw-weather/42`

**Router:**
```python
@router.get("/{weather_id}", response_model=RawWeatherResponse)
def get_weather_by_id(weather_id: int, db: Session = Depends(get_db)):
    service = WeatherService(db)
    record = service.get_weather_by_id(weather_id)
    
    if record is None:
        raise HTTPException(status_code=404, detail="Not found")
    
    return record
```

**Service:**
```python
def get_weather_by_id(self, weather_id: int) -> Optional[RawWeatherResponse]:
    record = self.repository.get_by_id(weather_id)
    if record is None:
        return None
    return RawWeatherResponse.model_validate(record)
```

**Repository:**
```python
def get_by_id(self, weather_id: int) -> Optional[RawWeather]:
    return self.db.query(RawWeather)\
        .filter(RawWeather.id == weather_id)\
        .first()
```

**Flow:**
```
Client
  → Router: ontvang weather_id=42
    → Service: get_weather_by_id(42)
      → Repository: get_by_id(42)
        → Database: SELECT * FROM raw_weather WHERE id = 42
        ← Database: returns 1 row
      ← Repository: returns RawWeather object
    ← Service: converts to RawWeatherResponse
  ← Router: converts to JSON, returns HTTP 200
← Client: receives JSON
```

---

### Voorbeeld 2: Complexe Query met Filtering

**Request:** `GET /raw-weather?year=2025&month=1&limit=10`

**Router:**
```python
@router.get("/", response_model=PaginatedResponse)
def get_weather_records(
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    limit: int = Query(100),
    offset: int = Query(0),
    db: Session = Depends(get_db)
):
    service = WeatherService(db)
    params = WeatherQueryParams(year=year, month=month, limit=limit, offset=offset)
    return service.get_weather_records(params)
```

**Service:**
```python
def get_weather_records(self, params: WeatherQueryParams) -> PaginatedResponse:
    records, total = self.repository.get_all(
        year=params.year,
        month=params.month,
        limit=params.limit,
        offset=params.offset
    )
    
    data = [RawWeatherResponse.model_validate(r) for r in records]
    
    return PaginatedResponse(
        total=total,
        limit=params.limit,
        offset=params.offset,
        data=data
    )
```

**Repository:**
```python
def get_all(self, year, month, limit, offset) -> Tuple[List[RawWeather], int]:
    query = self.db.query(RawWeather)
    
    if year:
        query = query.filter(RawWeather.year == year)
    if month:
        query = query.filter(RawWeather.month == month)
    
    total = query.count()
    records = query.order_by(RawWeather.id.desc())\
                   .limit(limit)\
                   .offset(offset)\
                   .all()
    
    return records, total
```

---

## Samenvatting

### De 4 Lagen:

| Laag | Verantwoordelijkheid | Weet van | Weet NIET van |
|------|---------------------|----------|---------------|
| **Router** | HTTP handling | Service | Database, SQL |
| **Service** | Business logic | Repository | HTTP, SQL |
| **Repository** | Database queries | Models | Business logic, HTTP |
| **Models** | Data structuren | - | Alles anders |

### Data Flow Samenvatting:

```
HTTP Request
  → Router (validates input)
    → Service (applies business rules)
      → Repository (executes query)
        → Database (returns data)
      ← Repository (returns model objects)
    ← Service (transforms to schemas)
  ← Router (serializes to JSON)
← HTTP Response
```

### Kernprincipes:

1. **Elke laag heeft 1 doel**
2. **Dependencies gaan van buiten naar binnen**
3. **Binnen lagen weten niks van buiten lagen**
4. **Alles is testbaar in isolatie**

---

Dit is de complete architectuur van je Weather Raw Data API! 🚀
