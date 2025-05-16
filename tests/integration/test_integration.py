# File: tests/test_integration.py
# -*- coding: utf-8 -*-

"""
Integration tests for the Comtrade Data Pipeline.
"""

import os
import pytest
from unittest.mock import patch, MagicMock

from src.api.client import ComtradeAPIClient
from src.cache.cache_manager import CacheManager
from src.database.db_manager import DatabaseManager
from src.processing.data_processor import DataProcessor
from src.pipeline import ComtradePipeline


class TestIntegration:
    """Integration tests for the pipeline components."""
    
    @patch('src.api.client.ComtradeAPIClient.get_tariffline_data')
    @patch('src.database.db_manager.DatabaseManager.initialize_schema')
    @patch('src.database.db_manager.DatabaseManager.bulk_insert_tariffline_data')
    @patch('src.database.db_manager.DatabaseManager.log_import_operation')
    def test_pipeline_run(self, mock_log, mock_insert, mock_init_schema, mock_get_data, 
                         test_config, mock_api_response, setup_test_environment):
        """Test running the full pipeline with mocked components."""
        # Mock API response
        mock_get_data.return_value = (mock_api_response, True)
        
        # Mock database operations
        mock_init_schema.return_value = True
        mock_insert.return_value = (2, 0)  # 2 inserted, 0 skipped
        mock_log.return_value = True
        
        # Create pipeline
        pipeline = ComtradePipeline(test_config)
        
        # Patch internal methods that access the database
        with patch.object(pipeline.data_processor, 'get_reporter_id', return_value=1), \
             patch.object(pipeline.data_processor, 'get_partner_id', return_value=2), \
             patch.object(pipeline.data_processor, 'get_commodity_id', return_value=3), \
             patch.object(pipeline.data_processor, 'get_flow_id', return_value=4), \
             patch.object(pipeline.data_processor, 'store_metadata', return_value=True):
            
            # Run pipeline for a single country
            result = pipeline.run(['DE'], '2022-01', '2022-03')
            
            # Verify results
            assert result is True
            
            # Verify schema was initialized
            mock_init_schema.assert_called_once()
            
            # Verify API was called
            assert mock_get_data.call_count > 0
            
            # Verify data was inserted
            assert mock_insert.call_count > 0
            
            # Verify import was logged
            assert mock_log.call_count > 0
            
            # Verify statistics were tracked
            assert pipeline.stats['api_calls'] > 0
            assert pipeline.stats['processed_records'] > 0
            assert pipeline.stats['stored_records'] > 0
    
    @patch('src.api.client.ComtradeAPIClient.get_tariffline_data')
    @patch('src.database.db_manager.DatabaseManager.initialize_schema')
    def test_pipeline_error_handling(self, mock_init_schema, mock_get_data, 
                                    test_config, setup_test_environment):
        """Test pipeline error handling."""
        # Mock database schema initialization to fail
        mock_init_schema.return_value = False
        
        # Create pipeline
        pipeline = ComtradePipeline(test_config)
        
        # Run pipeline
        result = pipeline.run(['DE'], '2022-01', '2022-03')
        
        # Verify results
        assert result is False
        
        # Verify API was not called
        mock_get_data.assert_not_called()
    
    def test_cache_api_integration(self, test_config, mock_api_response, setup_test_environment):
        """Test integration between cache manager and API client."""
        # Create real instances
        cache_manager = CacheManager(test_config)
        
        # Clear cache directory
        cache_dir = test_config['cache']['cache_dir']
        for file in os.listdir(cache_dir):
            if file.endswith('.json'):
                os.remove(os.path.join(cache_dir, file))
        
        # Create API parameters
        params = {
            'reporterCode': 'DE',
            'partnerCode': 'ALL',
            'period': '202201:202203',
            'flowCode': 'M',
            'subscription-key': 'test_key'
        }
        
        # Test initial cache miss
        data, is_cache_hit = cache_manager.get(params)
        assert is_cache_hit is False
        assert data is None
        
        # Save data to cache
        cache_manager.save(params, mock_api_response)
        
        # Test cache hit
        data, is_cache_hit = cache_manager.get(params)
        assert is_cache_hit is True
        assert data == mock_api_response
        
        # Verify parameter normalization (different key, same data)
        params2 = params.copy()
        params2['subscription-key'] = 'different_key'
        data, is_cache_hit = cache_manager.get(params2)
        assert is_cache_hit is True
        assert data == mock_api_response
    
    @patch('src.database.db_manager.DatabaseManager.connect')
    @patch('src.database.db_manager.DatabaseManager.get_reporter_id')
    @patch('src.database.db_manager.DatabaseManager.get_partner_id')
    @patch('src.database.db_manager.DatabaseManager.get_commodity_id')
    @patch('src.database.db_manager.DatabaseManager.get_flow_id')
    def test_processor_db_integration(self, mock_flow_id, mock_commodity_id, 
                                     mock_partner_id, mock_reporter_id, mock_connect,
                                     test_config, mock_api_response):
        """Test integration between data processor and database manager."""
        # Mock database connection
        mock_connect.return_value = True
        
        # Mock ID lookups
        mock_reporter_id.return_value = 1
        mock_partner_id.return_value = 2
        mock_commodity_id.return_value = 3
        mock_flow_id.return_value = 4
        
        # Create real instances
        db_manager = DatabaseManager(test_config)
        processor = DataProcessor(db_manager)
        
        # Mock store_metadata to avoid actual database calls
        with patch.object(processor, 'store_metadata', return_value=True):
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
            assert processed_records[0]['source_file'] == 'test_source'
            
            # Verify ID lookups were called
            mock_reporter_id.assert_called()
            mock_partner_id.assert_called()
            mock_commodity_id.assert_called()
            mock_flow_id.assert_called()