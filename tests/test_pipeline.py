# File: tests/test_pipeline.py
# -*- coding: utf-8 -*-

"""
Unit tests for the pipeline module.
"""

import pytest
from unittest.mock import patch, MagicMock, call

from src.pipeline import ComtradePipeline


class TestPipeline:
    """Tests for ComtradePipeline class."""
    
    def test_initialization(self, test_config):
        """Test initialization of pipeline."""
        with patch('src.api.client.ComtradeAPIClient') as mock_api, \
             patch('src.cache.cache_manager.CacheManager') as mock_cache, \
             patch('src.database.db_manager.DatabaseManager') as mock_db, \
             patch('src.processing.data_processor.DataProcessor') as mock_processor:
            
            pipeline = ComtradePipeline(test_config)
            
            # Verify all components were initialized
            mock_api.assert_called_once_with(test_config)
            mock_cache.assert_called_once_with(test_config)
            mock_db.assert_called_once_with(test_config)
            mock_processor.assert_called_once()
            
            # Verify EU countries were loaded
            assert pipeline.eu_countries == test_config['eu_countries']
            
            # Verify statistics were initialized
            assert 'total_calls' in pipeline.stats
            assert 'cache_hits' in pipeline.stats
            assert 'api_calls' in pipeline.stats
            assert 'processed_records' in pipeline.stats
            assert 'stored_records' in pipeline.stats
            assert 'failed_calls' in pipeline.stats
            assert 'skipped_records' in pipeline.stats
    
    def test_initialize_database(self, test_config):
        """Test database initialization."""
        pipeline = ComtradePipeline(test_config)
        
        # Mock db_manager
        pipeline.db_manager = MagicMock()
        pipeline.db_manager.initialize_schema.return_value = True
        
        # Initialize database
        result = pipeline._initialize_database()
        
        # Verify results
        assert result is True
        pipeline.db_manager.initialize_schema.assert_called_once()
    
    def test_get_api_params(self, test_config):
        """Test API parameter generation."""
        pipeline = ComtradePipeline(test_config)
        
        # Generate parameters
        params = pipeline._get_api_params('DE', '2022-01', '2022-03')
        
        # Verify parameters
        assert params['reporterCode'] == 'DE'
        assert params['partnerCode'] == 'ALL'
        assert params['period'] == '202201:202203'
        assert params['flowCode'] == test_config['comtrade']['flow_code']
        assert params['typeCode'] == test_config['comtrade']['type_code']
        assert params['freqCode'] == test_config['comtrade']['frequency']
        assert params['classCode'] == test_config['comtrade']['classification']
        assert params['cmdCode'] == 'TOTAL'
        assert params['subscription-key'] is None  # Will be added by API client
    
    def test_fetch_data_cache_hit(self, test_config, mock_api_response):
        """Test fetching data with cache hit."""
        pipeline = ComtradePipeline(test_config)
        
        # Mock cache_manager
        pipeline.cache_manager = MagicMock()
        pipeline.cache_manager.get.return_value = (mock_api_response, True)
        
        # Fetch data
        data, success = pipeline._fetch_data('DE', '2022-01', '2022-03')
        
        # Verify results
        assert success is True
        assert data == mock_api_response
        assert pipeline.stats['cache_hits'] == 1
        assert pipeline.stats['total_calls'] == 1
        assert pipeline.stats['api_calls'] == 0
        
        # Verify cache_manager.get was called with correct parameters
        pipeline.cache_manager.get.assert_called_once()
        
        # Verify api_client was not called
        pipeline.api_client.get_tariffline_data.assert_not_called()
    
    def test_fetch_data_api_call(self, test_config, mock_api_response):
        """Test fetching data with API call."""
        pipeline = ComtradePipeline(test_config)
        
        # Mock cache_manager and api_client
        pipeline.cache_manager = MagicMock()
        pipeline.cache_manager.get.return_value = (None, False)
        pipeline.cache_manager.save.return_value = True
        
        pipeline.api_client = MagicMock()
        pipeline.api_client.get_tariffline_data.return_value = (mock_api_response, True)
        
        # Fetch data
        data, success = pipeline._fetch_data('DE', '2022-01', '2022-03')
        
        # Verify results
        assert success is True
        assert data == mock_api_response
        assert pipeline.stats['cache_hits'] == 0
        assert pipeline.stats['total_calls'] == 1
        assert pipeline.stats['api_calls'] == 1
        assert pipeline.stats['failed_calls'] == 0
        
        # Verify api_client was called with correct parameters
        pipeline.api_client.get_tariffline_data.assert_called_once()
        args, kwargs = pipeline.api_client.get_tariffline_data.call_args
        assert kwargs['reporter_code'] == 'DE'
        assert kwargs['period_start'] == '202201'
        assert kwargs['period_end'] == '202203'
        
        # Verify cache_manager.save was called
        pipeline.cache_manager.save.assert_called_once()
    
    def test_fetch_data_api_failure(self, test_config):
        """Test fetching data with API failure."""
        pipeline = ComtradePipeline(test_config)
        
        # Mock cache_manager and api_client
        pipeline.cache_manager = MagicMock()
        pipeline.cache_manager.get.return_value = (None, False)
        
        pipeline.api_client = MagicMock()
        pipeline.api_client.get_tariffline_data.return_value = (None, False)
        
        # Fetch data
        data, success = pipeline._fetch_data('DE', '2022-01', '2022-03')
        
        # Verify results
        assert success is False
        assert data is None
        assert pipeline.stats['cache_hits'] == 0
        assert pipeline.stats['total_calls'] == 1
        assert pipeline.stats['api_calls'] == 1
        assert pipeline.stats['failed_calls'] == 1
        
        # Verify cache_manager.save was not called
        pipeline.cache_manager.save.assert_not_called()
    
    def test_process_and_store(self, test_config, mock_api_response):
        """Test processing and storing data."""
        pipeline = ComtradePipeline(test_config)
        
        # Mock data_processor
        processed_records = [
            {'reporter_id': 1, 'partner_id': 2, 'commodity_id': 3, 'flow_id': 4, 'period': '202201'},
            {'reporter_id': 1, 'partner_id': 3, 'commodity_id': 3, 'flow_id': 4, 'period': '202201'}
        ]
        pipeline.data_processor = MagicMock()
        pipeline.data_processor.process_api_response.return_value = processed_records
        
        # Mock db_manager
        pipeline.db_manager = MagicMock()
        pipeline.db_manager.bulk_insert_tariffline_data.return_value = (2, 0)  # 2 inserted, 0 skipped
        pipeline.db_manager.log_import_operation.return_value = True
        
        # Process and store data
        result = pipeline._process_and_store(mock_api_response, 'DE', '2022-01', '2022-03')
        
        # Verify results
        assert result is True
        assert pipeline.stats['processed_records'] == 2
        assert pipeline.stats['stored_records'] == 2
        assert pipeline.stats['skipped_records'] == 0
        
        # Verify data_processor was called
        pipeline.data_processor.process_api_response.assert_called_once_with(
            mock_api_response, 'DE_202201_202203')
        
        # Verify db_manager was called
        pipeline.db_manager.bulk_insert_tariffline_data.assert_called_once_with(
            processed_records, 'DE_202201_202203')
        pipeline.db_manager.log_import_operation.assert_called_once()
    
    def test_process_and_store_no_data(self, test_config):
        """Test processing and storing with no data."""
        pipeline = ComtradePipeline(test_config)
        
        # Mock data_processor to return no records
        pipeline.data_processor = MagicMock()
        pipeline.data_processor.process_api_response.return_value = []
        
        # Mock db_manager
        pipeline.db_manager = MagicMock()
        
        # Process and store empty data
        result = pipeline._process_and_store({'data': []}, 'DE', '2022-01', '2022-03')
        
        # Verify results
        assert result is False
        
        # Verify db_manager was not called for bulk insert
        pipeline.db_manager.bulk_insert_tariffline_data.assert_not_called()
    
    def test_process_reporter(self, test_config, mock_api_response):
        """Test processing a single reporter country."""
        pipeline = ComtradePipeline(test_config)
        
        # Mock internal methods
        pipeline._fetch_data = MagicMock(return_value=(mock_api_response, True))
        pipeline._process_and_store = MagicMock(return_value=True)
        
        # Test with date_utils.generate_date_ranges
        with patch('src.pipeline.generate_date_ranges') as mock_generate_dates:
            # Mock to return 3 date ranges
            mock_generate_dates.return_value = [
                ('2022-01', '2022-01'),
                ('2022-02', '2022-02'),
                ('2022-03', '2022-03')
            ]
            
            # Process reporter
            result = pipeline._process_reporter('DE', '2022-01', '2022-03')
            
            # Verify results
            assert result is True
            
            # Verify generate_date_ranges was called
            mock_generate_dates.assert_called_once_with('2022-01', '2022-03', months_per_call=3)
            
            # Verify _fetch_data and _process_and_store were called for each range
            assert pipeline._fetch_data.call_count == 3
            assert pipeline._process_and_store.call_count == 3
    
    def test_process_reporter_partial_failure(self, test_config, mock_api_response):
        """Test processing a reporter with some failures."""
        pipeline = ComtradePipeline(test_config)
        
        # Mock internal methods
        pipeline._fetch_data = MagicMock(side_effect=[
            (mock_api_response, True),  # First call succeeds
            (None, False),              # Second call fails
            (mock_api_response, True)   # Third call succeeds
        ])
        pipeline._process_and_store = MagicMock(return_value=True)
        
        # Test with date_utils.generate_date_ranges
        with patch('src.pipeline.generate_date_ranges') as mock_generate_dates:
            # Mock to return 3 date ranges
            mock_generate_dates.return_value = [
                ('2022-01', '2022-01'),
                ('2022-02', '2022-02'),
                ('2022-03', '2022-03')
            ]
            
            # Process reporter
            result = pipeline._process_reporter('DE', '2022-01', '2022-03')
            
            # Verify results
            assert result is False  # Overall result is False due to one failure
            
            # Verify _fetch_data was called for each range
            assert pipeline._fetch_data.call_count == 3
            
            # Verify _process_and_store was called only for successful fetches
            assert pipeline._process_and_store.call_count == 2
    
    def test_run(self, test_config):
        """Test running the full pipeline."""
        pipeline = ComtradePipeline(test_config)
        
        # Mock internal methods
        pipeline._initialize_database = MagicMock(return_value=True)
        pipeline._process_reporter = MagicMock(return_value=True)
        
        # Run for specific countries
        result = pipeline.run(['DE', 'FR'], '2022-01', '2022-03')
        
        # Verify results
        assert result is True
        
        # Verify _initialize_database was called
        pipeline._initialize_database.assert_called_once()
        
        # Verify _process_reporter was called for each country
        assert pipeline._process_reporter.call_count == 2
        calls = [
            call('DE', '2022-01', '2022-03'),
            call('FR', '2022-01', '2022-03')
        ]
        pipeline._process_reporter.assert_has_calls(calls, any_order=True)
    
    def test_run_all_countries(self, test_config):
        """Test running the pipeline for all EU countries."""
        pipeline = ComtradePipeline(test_config)
        
        # Mock internal methods
        pipeline._initialize_database = MagicMock(return_value=True)
        pipeline._process_reporter = MagicMock(return_value=True)
        
        # Run for all countries
        result = pipeline.run('all', '2022-01', '2022-03')
        
        # Verify results
        assert result is True
        
        # Verify _process_reporter was called for each EU country
        assert pipeline._process_reporter.call_count == len(test_config['eu_countries'])
        
        # Check each country was processed
        for country in test_config['eu_countries']:
            pipeline._process_reporter.assert_any_call(country, '2022-01', '2022-03')
    
    def test_run_db_init_failure(self, test_config):
        """Test running the pipeline with database initialization failure."""
        pipeline = ComtradePipeline(test_config)
        
        # Mock internal methods
        pipeline._initialize_database = MagicMock(return_value=False)
        pipeline._process_reporter = MagicMock()
        
        # Run pipeline
        result = pipeline.run(['DE'], '2022-01', '2022-03')
        
        # Verify results
        assert result is False
        
        # Verify _process_reporter was not called
        pipeline._process_reporter.assert_not_called()
    
    def test_run_partial_success(self, test_config):
        """Test running the pipeline with some countries failing."""
        pipeline = ComtradePipeline(test_config)
        
        # Mock internal methods
        pipeline._initialize_database = MagicMock(return_value=True)
        
        # First country succeeds, second fails
        pipeline._process_reporter = MagicMock(side_effect=[True, False])
        
        # Run for two countries
        result = pipeline.run(['DE', 'FR'], '2022-01', '2022-03')
        
        # Verify results
        assert result is False  # Overall result is False due to one failure
        
        # Verify _process_reporter was called for each country
        assert pipeline._process_reporter.call_count == 2
    
    def test_run_invalid_countries(self, test_config):
        """Test running the pipeline with invalid country codes."""
        pipeline = ComtradePipeline(test_config)
        
        # Mock internal methods
        pipeline._initialize_database = MagicMock(return_value=True)
        pipeline._process_reporter = MagicMock(return_value=True)
        
        # Run for countries including some invalid codes
        result = pipeline.run(['DE', 'XX', 'FR'], '2022-01', '2022-03')
        
        # Verify results
        assert result is True
        
        # Verify _process_reporter was called only for valid countries
        assert pipeline._process_reporter.call_count == 2
        calls = [
            call('DE', '2022-01', '2022-03'),
            call('FR', '2022-01', '2022-03')
        ]
        pipeline._process_reporter.assert_has_calls(calls, any_order=True)
