from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional, Tuple
from models.db_models import RawWeather, WeatherCleaned


class WeatherRepository:
    """Repository for raw weather data access."""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_by_id(self, weather_id: int) -> Optional[RawWeather]:
        """Get single weather record by ID."""
        return self.db.query(RawWeather).filter(RawWeather.id == weather_id).first()
    
    def get_all(
        self,
        year: Optional[int] = None,
        month: Optional[int] = None,
        day: Optional[int] = None,
        source_file: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Tuple[List[RawWeather], int]:
        """
        Get weather records with optional filtering and pagination.
        Returns tuple of (records, total_count).
        """
        # Build base query
        query = self.db.query(RawWeather)
        
        # Apply filters
        if year is not None:
            query = query.filter(RawWeather.year == year)
        if month is not None:
            query = query.filter(RawWeather.month == month)
        if day is not None:
            query = query.filter(RawWeather.day == day)
        if source_file is not None:
            query = query.filter(RawWeather.source_file == source_file)
        
        # Get total count before pagination
        total = query.count()
        
        # Apply pagination and ordering
        records = (
            query
            .order_by(RawWeather.year.desc(), RawWeather.month.desc(), 
                     RawWeather.day.desc(), RawWeather.id.desc())
            .limit(limit)
            .offset(offset)
            .all()
        )
        
        return records, total
    
    def get_date_range(self) -> Tuple[Optional[dict], Optional[dict]]:
        """Get the earliest and latest dates in the dataset."""
        min_date = (
            self.db.query(
                RawWeather.year,
                RawWeather.month,
                RawWeather.day
            )
            .order_by(RawWeather.year, RawWeather.month, RawWeather.day)
            .first()
        )
        
        max_date = (
            self.db.query(
                RawWeather.year,
                RawWeather.month,
                RawWeather.day
            )
            .order_by(RawWeather.year.desc(), RawWeather.month.desc(), RawWeather.day.desc())
            .first()
        )
        
        min_dict = {"year": min_date.year, "month": min_date.month, "day": min_date.day} if min_date else None
        max_dict = {"year": max_date.year, "month": max_date.month, "day": max_date.day} if max_date else None
        
        return min_dict, max_dict
    
    def get_total_count(self) -> int:
        """Get total number of records."""
        return self.db.query(func.count(RawWeather.id)).scalar()
    
    def get_source_files(self) -> List[str]:
        """Get list of unique source files."""
        return [
            row[0] for row in 
            self.db.query(RawWeather.source_file).distinct().all()
        ]


class CleanedWeatherRepository:
    """Repository for cleaned weather data access."""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_by_id(self, weather_id: int) -> Optional[WeatherCleaned]:
        """Get single cleaned weather record by ID."""
        return self.db.query(WeatherCleaned).filter(WeatherCleaned.id == weather_id).first()
    
    def get_all(
        self,
        year: Optional[int] = None,
        month: Optional[int] = None,
        day: Optional[int] = None,
        source_file: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Tuple[List[WeatherCleaned], int]:
        """
        Get cleaned weather records with optional filtering and pagination.
        Returns tuple of (records, total_count).
        """
        # Build base query
        query = self.db.query(WeatherCleaned)
        
        # Apply filters
        if year is not None:
            query = query.filter(WeatherCleaned.year == year)
        if month is not None:
            query = query.filter(WeatherCleaned.month == month)
        if day is not None:
            query = query.filter(WeatherCleaned.day == day)
        if source_file is not None:
            query = query.filter(WeatherCleaned.source_file == source_file)
        
        # Get total count before pagination
        total = query.count()
        
        # Apply pagination and ordering
        records = (
            query
            .order_by(WeatherCleaned.year.desc(), WeatherCleaned.month.desc(), 
                     WeatherCleaned.day.desc(), WeatherCleaned.id.desc())
            .limit(limit)
            .offset(offset)
            .all()
        )
        
        return records, total
    
    def get_date_range(self) -> Tuple[Optional[dict], Optional[dict]]:
        """Get the earliest and latest dates in the cleaned dataset."""
        min_date = (
            self.db.query(
                WeatherCleaned.year,
                WeatherCleaned.month,
                WeatherCleaned.day
            )
            .order_by(WeatherCleaned.year, WeatherCleaned.month, WeatherCleaned.day)
            .first()
        )
        
        max_date = (
            self.db.query(
                WeatherCleaned.year,
                WeatherCleaned.month,
                WeatherCleaned.day
            )
            .order_by(WeatherCleaned.year.desc(), WeatherCleaned.month.desc(), WeatherCleaned.day.desc())
            .first()
        )
        
        min_dict = {"year": min_date.year, "month": min_date.month, "day": min_date.day} if min_date else None
        max_dict = {"year": max_date.year, "month": max_date.month, "day": max_date.day} if max_date else None
        
        return min_dict, max_dict
    
    def get_total_count(self) -> int:
        """Get total number of cleaned records."""
        return self.db.query(func.count(WeatherCleaned.id)).scalar()
    
    def get_source_files(self) -> List[str]:
        """Get list of unique source files."""
        return [
            row[0] for row in 
            self.db.query(WeatherCleaned.source_file).distinct().all()
        ]
    
    def get_temperature_stats(self) -> dict:
        """Get temperature statistics."""
        stats = self.db.query(
            func.min(WeatherCleaned.temp_c).label('min_temp'),
            func.max(WeatherCleaned.temp_c).label('max_temp'),
            func.avg(WeatherCleaned.temp_c).label('avg_temp')
        ).first()
        
        return {
            "min_temp_c": float(stats.min_temp) if stats.min_temp else None,
            "max_temp_c": float(stats.max_temp) if stats.max_temp else None,
            "avg_temp_c": round(float(stats.avg_temp), 2) if stats.avg_temp else None
        }
