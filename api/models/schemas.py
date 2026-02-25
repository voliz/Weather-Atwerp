from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class RawWeatherResponse(BaseModel):
    """Response schema for raw weather data."""
    
    id: int
    clock: Optional[str] = None
    temp: Optional[str] = None
    weather: Optional[str] = None
    wind: Optional[str] = None
    humidity: Optional[str] = None
    barometer: Optional[str] = None
    visibility: Optional[str] = None
    year: Optional[int] = None
    month: Optional[int] = None
    day: Optional[int] = None
    source_file: str
    ingested_at: datetime
    
    class Config:
        from_attributes = True


class CleanedWeatherResponse(BaseModel):
    """Response schema for cleaned weather data with numeric values."""
    
    id: int
    clock: Optional[str] = None
    weather: Optional[str] = None
    year: int
    month: int
    day: int
    temp_c: Optional[float] = None
    humidity_pct: Optional[float] = None
    wind_kmh: Optional[float] = None
    barometer_mbar: Optional[float] = None
    visibility_km: Optional[float] = None
    source_file: str
    ingested_at: datetime
    transformed_at: datetime
    
    class Config:
        from_attributes = True


class WeatherQueryParams(BaseModel):
    """Query parameters for filtering weather data."""
    
    year: Optional[int] = Field(None, ge=1900, le=2100)
    month: Optional[int] = Field(None, ge=1, le=12)
    day: Optional[int] = Field(None, ge=1, le=31)
    source_file: Optional[str] = None
    limit: int = Field(100, ge=1, le=10000)
    offset: int = Field(0, ge=0)


class PaginatedResponse(BaseModel):
    """Paginated response wrapper."""
    
    total: int
    limit: int
    offset: int
    data: list[RawWeatherResponse]


class CleanedPaginatedResponse(BaseModel):
    """Paginated response wrapper for cleaned data."""
    
    total: int
    limit: int
    offset: int
    data: list[CleanedWeatherResponse]


class PipelineMetadataResponse(BaseModel):
    """Response schema for pipeline metadata."""
    
    id: int
    run_id: str
    dag_id: str
    execution_date: datetime
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str
    records_downloaded: int
    records_staged: int
    records_transformed: int
    records_loaded: int
    quality_report: Optional[dict] = None
    error_message: Optional[str] = None
