# File: tests/test_date_utils.py
# -*- coding: utf-8 -*-

"""
Unit tests for date utilities.
"""

import pytest
from datetime import datetime

from src.utils.date_utils import (
    parse_date, format_date, add_months, generate_date_ranges
)


class TestDateUtils:
    """Tests for date utility functions."""
    
    def test_parse_date(self):
        """Test parsing date from string."""
        # Test valid date
        date = parse_date("2022-01")
        assert date.year == 2022
        assert date.month == 1
        assert date.day == 1
        
        # Test invalid date format
        with pytest.raises(ValueError):
            parse_date("01-2022")
        
        with pytest.raises(ValueError):
            parse_date("2022/01")
    
    def test_format_date(self):
        """Test formatting date to string."""
        date = datetime(2022, 1, 15)
        formatted = format_date(date)
        assert formatted == "2022-01"
    
    def test_add_months(self):
        """Test adding months to a date."""
        date = datetime(2022, 1, 15)
        
        # Add positive months
        new_date = add_months(date, 1)
        assert new_date.year == 2022
        assert new_date.month == 2
        assert new_date.day == 15
        
        # Add months crossing year boundary
        new_date = add_months(date, 12)
        assert new_date.year == 2023
        assert new_date.month == 1
        assert new_date.day == 15
        
        # Add many months
        new_date = add_months(date, 25)
        assert new_date.year == 2024
        assert new_date.month == 2
        assert new_date.day == 15
        
        # Add negative months (not supported, should not change the date)
        new_date = add_months(date, -1)
        assert new_date.year == 2021
        assert new_date.month == 12
        assert new_date.day == 15
    
    def test_generate_date_ranges(self):
        """Test generating date ranges."""
        # Test basic range
        ranges = generate_date_ranges("2022-01", "2022-03", months_per_call=1)
        assert len(ranges) == 3
        assert ranges[0] == ("2022-01", "2022-01")
        assert ranges[1] == ("2022-02", "2022-02")
        assert ranges[2] == ("2022-03", "2022-03")
        
        # Test with months_per_call > 1
        ranges = generate_date_ranges("2022-01", "2022-06", months_per_call=3)
        assert len(ranges) == 2
        assert ranges[0] == ("2022-01", "2022-03")
        assert ranges[1] == ("2022-04", "2022-06")
        
        # Test with uneven division
        ranges = generate_date_ranges("2022-01", "2022-04", months_per_call=3)
        assert len(ranges) == 2
        assert ranges[0] == ("2022-01", "2022-03")
        assert ranges[1] == ("2022-04", "2022-04")
        
        # Test invalid date range
        with pytest.raises(ValueError):
            generate_date_ranges("2022-03", "2022-01")