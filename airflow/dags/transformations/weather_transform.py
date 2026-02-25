"""
Weather data transformation functions.

This module provides functions to parse raw text weather data
into clean numeric values for analysis.
"""

import re
import pandas as pd
import numpy as np
from typing import Optional, Union


def parse_temperature(temp_str: str) -> Optional[float]:
    """
    Parse temperature string to float (in Celsius).
    
    Args:
        temp_str: Temperature string like "11 °C" or "−5 °C"
    
    Returns:
        Float temperature value or None if parsing fails
    
    Examples:
        >>> parse_temperature("11 °C")
        11.0
        >>> parse_temperature("−5 °C")
        -5.0
        >>> parse_temperature("")
        None
    """
    if pd.isna(temp_str) or str(temp_str).strip() == '':
        return None
    
    try:
        # Remove °C and any extra whitespace
        cleaned = str(temp_str).replace('°C', '').strip()
        # Replace minus sign (−) with hyphen (-)
        cleaned = cleaned.replace('−', '-')
        return float(cleaned)
    except (ValueError, AttributeError):
        return None


def parse_humidity(humidity_str: str) -> Optional[float]:
    """
    Parse humidity percentage string to float.
    
    Args:
        humidity_str: Humidity string like "94%" or "100%"
    
    Returns:
        Float percentage value or None if parsing fails
    
    Examples:
        >>> parse_humidity("94%")
        94.0
        >>> parse_humidity("100%")
        100.0
    """
    if pd.isna(humidity_str) or str(humidity_str).strip() == '':
        return None
    
    try:
        # Remove % and any extra whitespace
        cleaned = str(humidity_str).replace('%', '').strip()
        value = float(cleaned)
        # Validate range
        if 0 <= value <= 100:
            return value
        return None
    except (ValueError, AttributeError):
        return None


def parse_wind_speed(wind_str: str) -> Optional[float]:
    """
    Parse wind speed string to float (in km/h).
    
    Args:
        wind_str: Wind string like "17 km/h" or "Calm"
    
    Returns:
        Float wind speed value or None if parsing fails
    
    Examples:
        >>> parse_wind_speed("17 km/h")
        17.0
        >>> parse_wind_speed("Calm")
        0.0
    """
    if pd.isna(wind_str) or str(wind_str).strip() == '':
        return None
    
    wind_str = str(wind_str).strip()
    
    # Handle "Calm" as 0 km/h
    if wind_str.lower() == 'calm':
        return 0.0
    
    try:
        # Extract numeric value using regex
        match = re.search(r'(\d+(?:\.\d+)?)', wind_str)
        if match:
            return float(match.group(1))
        return None
    except (ValueError, AttributeError):
        return None


def parse_barometer(barometer_str: str) -> Optional[float]:
    """
    Parse barometric pressure string to float (in mbar/hPa).
    
    Args:
        barometer_str: Barometer string like "1011 mbar"
    
    Returns:
        Float pressure value or None if parsing fails
    
    Examples:
        >>> parse_barometer("1011 mbar")
        1011.0
        >>> parse_barometer("1013.25 mbar")
        1013.25
    """
    if pd.isna(barometer_str) or str(barometer_str).strip() == '':
        return None
    
    try:
        # Remove mbar/hPa and any extra whitespace
        cleaned = str(barometer_str).replace('mbar', '').replace('hPa', '').strip()
        value = float(cleaned)
        # Validate reasonable range (950-1050 mbar)
        if 900 <= value <= 1100:
            return value
        return None
    except (ValueError, AttributeError):
        return None


def parse_visibility(visibility_str: str) -> Optional[float]:
    """
    Parse visibility string to float (in km).
    
    Args:
        visibility_str: Visibility string like "5 km" or "10 km"
    
    Returns:
        Float visibility value or None if parsing fails
    
    Examples:
        >>> parse_visibility("5 km")
        5.0
        >>> parse_visibility("10 km")
        10.0
    """
    if pd.isna(visibility_str) or str(visibility_str).strip() == '':
        return None
    
    try:
        # Extract numeric value using regex
        cleaned = str(visibility_str).replace('km', '').strip()
        value = float(cleaned)
        # Validate reasonable range
        if 0 <= value <= 100:
            return value
        return None
    except (ValueError, AttributeError):
        return None


def transform_weather_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform a raw weather dataframe with text values to numeric values.
    
    Args:
        df: DataFrame with raw weather data (text columns)
    
    Returns:
        DataFrame with additional numeric columns
    
    Expected input columns:
        - temp (text)
        - humidity (text)
        - wind (text)
        - barometer (text)
        - visibility (text)
    
    New output columns:
        - temp_c (float)
        - humidity_pct (float)
        - wind_kmh (float)
        - barometer_mbar (float)
        - visibility_km (float)
    """
    df_copy = df.copy()
    
    # Apply transformations
    if 'temp' in df_copy.columns:
        df_copy['temp_c'] = df_copy['temp'].apply(parse_temperature)
    
    if 'humidity' in df_copy.columns:
        df_copy['humidity_pct'] = df_copy['humidity'].apply(parse_humidity)
    
    if 'wind' in df_copy.columns:
        df_copy['wind_kmh'] = df_copy['wind'].apply(parse_wind_speed)
    
    if 'barometer' in df_copy.columns:
        df_copy['barometer_mbar'] = df_copy['barometer'].apply(parse_barometer)
    
    if 'visibility' in df_copy.columns:
        df_copy['visibility_km'] = df_copy['visibility'].apply(parse_visibility)
    
    return df_copy


def get_data_quality_report(df: pd.DataFrame) -> dict:
    """
    Generate a data quality report for transformed weather data.
    
    Args:
        df: Transformed DataFrame with numeric weather columns
    
    Returns:
        Dictionary with quality metrics
    """
    numeric_cols = ['temp_c', 'humidity_pct', 'wind_kmh', 'barometer_mbar', 'visibility_km']
    existing_cols = [col for col in numeric_cols if col in df.columns]
    
    report = {
        'total_records': len(df),
        'columns_analyzed': len(existing_cols),
        'quality_metrics': {}
    }
    
    for col in existing_cols:
        null_count = df[col].isna().sum()
        null_pct = (null_count / len(df)) * 100 if len(df) > 0 else 0
        
        report['quality_metrics'][col] = {
            'null_count': int(null_count),
            'null_percentage': round(null_pct, 2),
            'valid_count': int(len(df) - null_count),
            'min': float(df[col].min()) if not df[col].isna().all() else None,
            'max': float(df[col].max()) if not df[col].isna().all() else None,
            'mean': float(df[col].mean()) if not df[col].isna().all() else None,
            'median': float(df[col].median()) if not df[col].isna().all() else None
        }
    
    return report


if __name__ == '__main__':
    # Test the parsing functions
    import doctest
    doctest.testmod()
