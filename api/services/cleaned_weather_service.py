from sqlalchemy.orm import Session
from typing import Optional
from repositories.weather_repository import CleanedWeatherRepository
from models.schemas import CleanedWeatherResponse, WeatherQueryParams, CleanedPaginatedResponse


class CleanedWeatherService:
    """Service layer for cleaned weather data business logic."""
    
    def __init__(self, db: Session):
        self.repository = CleanedWeatherRepository(db)
    
    def get_weather_by_id(self, weather_id: int) -> Optional[CleanedWeatherResponse]:
        """Get single cleaned weather record by ID."""
        record = self.repository.get_by_id(weather_id)
        if record is None:
            return None
        return CleanedWeatherResponse.model_validate(record)
    
    def get_weather_records(self, params: WeatherQueryParams) -> CleanedPaginatedResponse:
        """Get cleaned weather records with filtering and pagination."""
        records, total = self.repository.get_all(
            year=params.year,
            month=params.month,
            day=params.day,
            source_file=params.source_file,
            limit=params.limit,
            offset=params.offset
        )
        
        # Convert to response models
        data = [CleanedWeatherResponse.model_validate(record) for record in records]
        
        return CleanedPaginatedResponse(
            total=total,
            limit=params.limit,
            offset=params.offset,
            data=data
        )
    
    def get_statistics(self) -> dict:
        """Get cleaned dataset statistics."""
        total_count = self.repository.get_total_count()
        min_date, max_date = self.repository.get_date_range()
        source_files = self.repository.get_source_files()
        temp_stats = self.repository.get_temperature_stats()
        
        return {
            "total_records": total_count,
            "date_range": {
                "min": min_date,
                "max": max_date
            },
            "source_files": source_files,
            "temperature_stats": temp_stats
        }
