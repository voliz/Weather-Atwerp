from sqlalchemy.orm import Session
from typing import List, Optional
from repositories import WeatherRepository
from models import RawWeatherResponse, WeatherQueryParams, PaginatedResponse


class WeatherService:
    """Service layer for weather data business logic."""
    
    def __init__(self, db: Session):
        self.repository = WeatherRepository(db)
    
    def get_weather_by_id(self, weather_id: int) -> Optional[RawWeatherResponse]:
        """Get single weather record by ID."""
        record = self.repository.get_by_id(weather_id)
        if record is None:
            return None
        return RawWeatherResponse.model_validate(record)
    
    def get_weather_records(self, params: WeatherQueryParams) -> PaginatedResponse:
        """Get weather records with filtering and pagination."""
        records, total = self.repository.get_all(
            year=params.year,
            month=params.month,
            day=params.day,
            source_file=params.source_file,
            limit=params.limit,
            offset=params.offset
        )
        
        # Convert to response models
        data = [RawWeatherResponse.model_validate(record) for record in records]
        
        return PaginatedResponse(
            total=total,
            limit=params.limit,
            offset=params.offset,
            data=data
        )
    
    def get_statistics(self) -> dict:
        """Get dataset statistics."""
        total_count = self.repository.get_total_count()
        min_date, max_date = self.repository.get_date_range()
        source_files = self.repository.get_source_files()
        
        return {
            "total_records": total_count,
            "date_range": {
                "min": min_date,
                "max": max_date
            },
            "source_files": source_files
        }
