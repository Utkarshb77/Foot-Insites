"""
ETL Utilities for Football Data Pipeline
Provides data cleaning and transformation functions
"""

import pandas as pd
from datetime import datetime


def standardize_date(date_str):
    """Convert date to YYYY-MM-DD format"""
    try:
        if pd.isna(date_str):
            return None
        # Try parsing common date formats
        for fmt in ['%d/%m/%Y', '%d/%m/%y', '%Y-%m-%d', '%m/%d/%Y']:
            try:
                return pd.to_datetime(date_str, format=fmt).strftime('%Y-%m-%d')
            except:
                continue
        return pd.to_datetime(date_str).strftime('%Y-%m-%d')
    except:
        return None


def standardize_time(time_str):
    """Convert time to HH:MM:SS format"""
    try:
        if pd.isna(time_str):
            return None
        time_str = str(time_str).strip()
        if ':' in time_str:
            parts = time_str.split(':')
            if len(parts) == 2:
                return f"{parts[0].zfill(2)}:{parts[1].zfill(2)}:00"
            elif len(parts) == 3:
                return f"{parts[0].zfill(2)}:{parts[1].zfill(2)}:{parts[2].zfill(2)}"
        return None
    except:
        return None


def clean_team_name(team_name):
    """Standardize team names"""
    if pd.isna(team_name):
        return None
    return str(team_name).strip()


def safe_int(value):
    """Safely convert to integer"""
    try:
        if pd.isna(value):
            return None
        return int(float(value))
    except:
        return None


def safe_float(value):
    """Safely convert to float"""
    try:
        if pd.isna(value):
            return None
        return float(value)
    except:
        return None


def remove_nulls_and_duplicates(df, required_cols=None):
    """Remove rows with null values in required columns and duplicates"""
    if required_cols:
        df = df.dropna(subset=required_cols)
    df = df.drop_duplicates()
    return df
