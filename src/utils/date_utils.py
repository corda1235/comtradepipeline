# File: src/utils/date_utils.py

# -*- coding: utf-8 -*-

"""
Date utility functions for the Comtrade Data Pipeline.
"""

from datetime import datetime, timedelta
from typing import List, Tuple


def parse_date(date_str: str) -> datetime:
    """
    Parse date string in YYYY-MM format.
    
    Args:
        date_str: Date string in YYYY-MM format.
        
    Returns:
        datetime: Parsed date.
        
    Raises:
        ValueError: If the date string is invalid.
    """
    try:
        return datetime.strptime(date_str, '%Y-%m')
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Expected format: YYYY-MM")


def format_date(date: datetime) -> str:
    """
    Format date to YYYY-MM string.
    
    Args:
        date: Datetime object.
        
    Returns:
        str: Formatted date string.
    """
    return date.strftime('%Y-%m')


def add_months(date: datetime, months: int) -> datetime:
    """
    Add the specified number of months to a date.
    
    Args:
        date: Start date.
        months: Number of months to add.
        
    Returns:
        datetime: New date.
    """
    month = date.month - 1 + months
    year = date.year + month // 12
    month = month % 12 + 1
    return date.replace(year=year, month=month)


def generate_date_ranges(start_date_str: str, end_date_str: str, months_per_call: int = 3) -> List[Tuple[str, str]]:
    """
    Generate date ranges for API calls.
    
    Args:
        start_date_str: Start date in YYYY-MM format.
        end_date_str: End date in YYYY-MM format.
        months_per_call: Number of months to include in each call.
        
    Returns:
        list: List of (start_date, end_date) tuples.
    """
    start_date = parse_date(start_date_str)
    end_date = parse_date(end_date_str)
    
    if start_date > end_date:
        raise ValueError("Start date must be before end date")
    
    date_ranges = []
    current_start = start_date
    
    while current_start <= end_date:
        # Calculate end date for this range
        current_end = add_months(current_start, months_per_call - 1)
        
        # Ensure we don't go beyond the overall end date
        if current_end > end_date:
            current_end = end_date
        
        # Add range to list
        date_ranges.append((format_date(current_start), format_date(current_end)))
        
        # Move to next range
        current_start = add_months(current_end, 1)
    
    return date_ranges