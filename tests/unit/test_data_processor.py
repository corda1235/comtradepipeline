# File: tests/test_data_processor.py
# -*- coding: utf-8 -*-

"""
Unit tests for the Data Processor.
"""

import pytest
from unittest.mock import patch, MagicMock

from src.processing.data_processor import DataProcessor


class TestDataProcessor:
    """Tests for DataProcessor class."""
    
    def test_initialization(self):
        """Test initialization of data processor."""
        db_manager = MagicMock()
        processor = DataProcessor(db_manager)
        
        assert processor.db_manager == db_manager
        assert processor.reporter_cache == {}
        assert processor.partner_cache == {}
        assert processor.commodity_cache == {}
        assert processor.flow_cache == {}
    
    def test_extract_metadata(self, mock_api_response):
        """Test extracting metadata from API response."""
        db_manager = MagicMock()
        processor = DataProcessor(db_manager)
        
        # Extract metadata
        reporters, partners, commodities, flows = processor.extract_metadata(mock_api_response)
        
        # Verify reporters
        assert len(reporters) == 1
        assert reporters[0]['reporter_code'] == 'DE'
        assert reporters[0]['reporter_name'] == 'Germany'
        
        # Verify partners
        assert len(partners) == 2
        assert partners[0]['partner_code'] == 'CN'
        assert partners[0]['partner_name'] == 'China'
        assert partners[1]['partner_code'] == 'US'
        assert partners[1]['partner_name'] == 'United States'
        
        # Verify commodities
        assert len(commodities) == 2
        assert commodities[0]['commodity_code'] == '010121'
        assert commodities[0]['commodity_description'] == 'Horses, live pure-bred breeding'
        
        # Verify flows
        assert len(flows) == 1
        assert flows[0]['flow_code'] == 'M'
        assert flows[0]['flow_desc'] == 'Import'
    
    def test_store_metadata(self, mock_api_response):
        """Test storing metadata from API response."""
        db_manager = MagicMock()
        # Set all upsert methods to return True
        db_manager.upsert_reporters.return_value = True
        db_manager.upsert_partners.return_value = True
        db_manager.upsert_commodities.return_value = True
        db_manager.upsert_flows.return_value = True
        
        processor = DataProcessor(db_manager)
        
        # Store metadata
        result = processor.store_metadata(mock_api_response)
        
        # Verify result
        assert result is True
        
        # Verify db_manager methods were called with correct data
        db_manager.upsert_reporters.assert_called_once()
        db_manager.upsert_partners.assert_called_once()
        db_manager.upsert_commodities.assert_called_once()
        db_manager.upsert_flows.assert_called_once()
    
    def test_store_metadata_failure(self, mock_api_response):
        """Test storing metadata with failure."""
        db_manager = MagicMock()
        # Set one upsert method to fail
        db_manager.upsert_reporters.return_value = True
        db_manager.upsert_partners.return_value = False
        db_manager.upsert_commodities.return_value = True
        db_manager.upsert_flows.return_value = True
        
        processor = DataProcessor(db_manager)
        
        # Store metadata
        result = processor.store_metadata(mock_api_response)
        
        # Verify result
        assert result is False
    
    def test_get_dimension_ids(self):
        """Test getting dimension IDs."""
        db_manager = MagicMock()
        # Mock ID lookup methods
        db_manager.get_reporter_id.return_value = 1
        db_manager.get_partner_id.return_value = 2
        db_manager.get_commodity_id.return_value = 3
        db_manager.get_flow_id.return_value = 4
        
        processor = DataProcessor(db_manager)
        
        # Test getting IDs
        reporter_id = processor.get_reporter_id('DE')
        partner_id = processor.get_partner_id('CN')
        commodity_id = processor.get_commodity_id('010121')
        flow_id = processor.get_flow_id('M')
        
        # Verify results
        assert reporter_id == 1
        assert partner_id == 2
        assert commodity_id == 3
        assert flow_id == 4
        
        # Verify cache was populated
        assert processor.reporter_cache['DE'] == 1
        assert processor.partner_cache['CN'] == 2
        assert processor.commodity_cache['010121'] == 3
        assert processor.flow_cache['M'] == 4
        
        # Test getting cached IDs (db_manager should not be called again)
        db_manager.get_reporter_id.reset_mock()
        reporter_id = processor.get_reporter_id('DE')
        assert reporter_id == 1
        db_manager.get_reporter_id.assert_not_called()
    
    def test_get_dimension_ids_not_found(self):
        """Test getting dimension IDs when not found in database."""
        db_manager = MagicMock()
        # Mock ID lookup methods to return None (not found)
        db_manager.get_reporter_id.return_value = None
        
        processor = DataProcessor(db_manager)
        
        # Test getting ID for nonexistent code
        reporter_id = processor.get_reporter_id('XX')
        
        # Verify result
        assert reporter_id is None
        
        # Verify cache was not populated
        assert 'XX' not in processor.reporter_cache
    
    def test_parse_period(self):
        """Test parsing period string."""
        processor = DataProcessor(MagicMock())
        
        # Test valid period
        year, month = processor.parse_period('202201')
        assert year == 2022
        assert month == 1
        
        # Test invalid format
        year, month = processor.parse_period('2022-01')
        assert year is None
        assert month is None
        
        # Test invalid values
        year, month = processor.parse_period('202213')  # Invalid month
        assert year is None
        assert month is None
    
    def test_safe_conversions(self):
        """Test safe value conversion methods."""
        processor = DataProcessor(MagicMock())
        
        # Test safe_float
        assert processor.safe_float('123.45') == 123.45
        assert processor.safe_float('abc') is None
        assert processor.safe_float(None) is None
        
        # Test safe_int
        assert processor.safe_int('123') == 123
        assert processor.safe_int('123.45') is None
        assert processor.safe_int('abc') is None
        assert processor.safe_int(None) is None
        
        # Test safe_bool
        assert processor.safe_bool('true') is True
        assert processor.safe_bool('1') is True
        assert processor.safe_bool('false') is False
        assert processor.safe_bool('0') is False
        assert processor.safe_bool('abc') is None
        assert processor.safe_bool(None) is None
    
    def test_process_tariffline_record(self):
        """Test processing a single tariffline record."""
        db_manager = MagicMock()
        # Mock ID lookup methods
        db_manager.get_reporter_id.return_value = 1
        db_manager.get_partner_id.return_value = 2
        db_manager.get_commodity_id.return_value = 3
        db_manager.get_flow_id.return_value = 4
        
        processor = DataProcessor(db_manager)
        
        # Test record
        record = {
            'reporterCode': 'DE',
            'partnerCode': 'CN',
            'cmdCode': '010121',
            'flowCode': 'M',
            'period': '202201',
            'netWgt': 1000.5,
            'qty': 10,
            'qtyUnit': 'Number of items',
            'primaryValue': 5000.25,
            'flag': 0,
            'isReporterEstimate': False
        }
        
        # Process record
        processed = processor.process_tariffline_record(record, 'test_source')
        
        # Verify processed record
        assert processed['reporter_id'] == 1
        assert processed['partner_id'] == 2
        assert processed['commodity_id'] == 3
        assert processed['flow_id'] == 4
        assert processed['period'] == '202201'
        assert processed['year'] == 2022
        assert processed['month'] == 1
        assert processed['net_weight'] == 1000.5
        assert processed['quantity'] == 10
        assert processed['quantity_unit'] == 'Number of items'
        assert processed['trade_value'] == 5000.25
        assert processed['flag'] == 0
        assert processed['is_reporter_estimate'] is False
        assert processed['source_file'] == 'test_source'
    
    def test_process_tariffline_record_missing_dimension(self):
        """Test processing a record with missing dimension ID."""
        db_manager = MagicMock()
        # Mock one ID lookup to return None
        db_manager.get_reporter_id.return_value = 1
        db_manager.get_partner_id.return_value = None  # Missing partner ID
        db_manager.get_commodity_id.return_value = 3
        db_manager.get_flow_id.return_value = 4
        
        processor = DataProcessor(db_manager)
        
        # Test record
        record = {
            'reporterCode': 'DE',
            'partnerCode': 'XX',  # Unknown partner
            'cmdCode': '010121',
            'flowCode': 'M',
            'period': '202201',
            'netWgt': 1000.5,
            'qty': 10,
            'qtyUnit': 'Number of items',
            'primaryValue': 5000.25
        }
        
        # Process record
        processed = processor.process_tariffline_record(record)
        
        # Verify result
        assert processed is None
    
    def test_process_tariffline_record_invalid_period(self):
        """Test processing a record with invalid period."""
        db_manager = MagicMock()
        # Mock ID lookup methods
        db_manager.get_reporter_id.return_value = 1
        db_manager.get_partner_id.return_value = 2
        db_manager.get_commodity_id.return_value = 3
        db_manager.get_flow_id.return_value = 4
        
        processor = DataProcessor(db_manager)
        
        # Test record with invalid period
        record = {
            'reporterCode': 'DE',
            'partnerCode': 'CN',
            'cmdCode': '010121',
            'flowCode': 'M',
            'period': '2022-01',  # Invalid format
            'netWgt': 1000.5,
            'qty': 10,
            'qtyUnit': 'Number of items',
            'primaryValue': 5000.25
        }
        
        # Process record
        processed = processor.process_tariffline_record(record)
        
        # Verify result
        assert processed is None
    
    def test_process_api_response(self, mock_api_response):
        """Test processing complete API response."""
        db_manager = MagicMock()
        # Mock IDs and metadata storage
        db_manager.get_reporter_id.return_value = 1
        db_manager.get_partner_id.return_value = 2
        db_manager.get_commodity_id.return_value = 3
        db_manager.get_flow_id.return_value = 4
        db_manager.upsert_reporters.return_value = True
        db_manager.upsert_partners.return_value = True
        db_manager.upsert_commodities.return_value = True
        db_manager.upsert_flows.return_value = True
        
        processor = DataProcessor(db_manager)
        
        # Process API response
        processed_records = processor.process_api_response(mock_api_response, 'test_source')
        
        # Verify number of processed records
        assert len(processed_records) == 2
        
        # Verify first record
        assert processed_records[0]['reporter_id'] == 1
        assert processed_records[0]['partner_id'] == 2
        assert processed_records[0]['commodity_id'] == 3
        assert processed_records[0]['flow_id'] == 4
        assert processed_records[0]['period'] == '202201'
        assert processed_records[0]['year'] == 2022
        assert processed_records[0]['month'] == 1
        assert processed_records[0]['trade_value'] == 5000.25
        assert processed_records[0]['source_file'] == 'test_source'
