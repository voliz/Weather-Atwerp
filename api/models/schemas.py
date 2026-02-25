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
