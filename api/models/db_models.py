from sqlalchemy import Column, Integer, String, Text, DateTime
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
