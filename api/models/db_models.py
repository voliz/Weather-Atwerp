from sqlalchemy import Column, Integer, String, Text, DateTime, Numeric
from database import Base
from datetime import datetime


class RawWeather(Base):
    """SQLAlchemy model for raw_weather table."""
    
    __tablename__ = "raw_weather"
    
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
    
    def __repr__(self):
        return f"<RawWeather(id={self.id}, date={self.year}-{self.month}-{self.day}, source={self.source_file})>"


class WeatherCleaned(Base):
    """SQLAlchemy model for weather_cleaned table with numeric values."""
    
    __tablename__ = "weather_cleaned"
    
    id = Column(Integer, primary_key=True, index=True)
    clock = Column(Text)
    weather = Column(Text)
    year = Column(Integer, index=True, nullable=False)
    month = Column(Integer, index=True, nullable=False)
    day = Column(Integer, index=True, nullable=False)
    # Numeric parsed values
    temp_c = Column(Numeric(5, 2), nullable=True)
    humidity_pct = Column(Numeric(5, 2), nullable=True)
    wind_kmh = Column(Numeric(6, 2), nullable=True)
    barometer_mbar = Column(Numeric(7, 2), nullable=True)
    visibility_km = Column(Numeric(6, 2), nullable=True)
    # Metadata
    source_file = Column(String(255), nullable=False)
    ingested_at = Column(DateTime)
    transformed_at = Column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<WeatherCleaned(id={self.id}, date={self.year}-{self.month}-{self.day}, temp={self.temp_c}°C)>"
