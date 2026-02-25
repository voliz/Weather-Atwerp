from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional
from database import get_db
from services import WeatherService
from models import RawWeatherResponse, WeatherQueryParams, PaginatedResponse

router = APIRouter(prefix="/raw-weather", tags=["Raw Weather Data"])


@router.get("/", response_model=PaginatedResponse)
def get_weather_records(
    year: Optional[int] = Query(None, ge=1900, le=2100, description="Filter by year"),
    month: Optional[int] = Query(None, ge=1, le=12, description="Filter by month"),
    day: Optional[int] = Query(None, ge=1, le=31, description="Filter by day"),
    source_file: Optional[str] = Query(None, description="Filter by source file name"),
    limit: int = Query(100, ge=1, le=10000, description="Maximum number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    db: Session = Depends(get_db)
):
    """
    Get raw weather records with optional filtering and pagination.
    
    - **year**: Filter by year
    - **month**: Filter by month (1-12)
    - **day**: Filter by day (1-31)
    - **source_file**: Filter by source file name
    - **limit**: Maximum records to return (default: 100, max: 10000)
    - **offset**: Number of records to skip for pagination
    """
    service = WeatherService(db)
    params = WeatherQueryParams(
        year=year,
        month=month,
        day=day,
        source_file=source_file,
        limit=limit,
        offset=offset
    )
    
    return service.get_weather_records(params)


@router.get("/{weather_id}", response_model=RawWeatherResponse)
def get_weather_by_id(
    weather_id: int,
    db: Session = Depends(get_db)
):
    """
    Get a single weather record by ID.
    
    - **weather_id**: The unique ID of the weather record
    """
    service = WeatherService(db)
    record = service.get_weather_by_id(weather_id)
    
    if record is None:
        raise HTTPException(status_code=404, detail=f"Weather record with ID {weather_id} not found")
    
    return record


@router.get("/stats/summary")
def get_statistics(db: Session = Depends(get_db)):
    """
    Get dataset statistics including total records, date range, and source files.
    """
    service = WeatherService(db)
    return service.get_statistics()
