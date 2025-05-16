# File: tests/test_api_client.py
# -*- coding: utf-8 -*-

"""
Unit tests for the Comtrade API client.
"""

import pytest
from unittest.mock import patch, MagicMock
import requests

from src.api.client import ComtradeAPIClient


class TestComtradeAPIClient:
    """Tests for ComtradeAPIClient class."""
    
    def test_initialization(self, test_config):
        """Test initialization of API client."""
        client = ComtradeAPIClient(test_config)
        assert client.primary_key == test_config['api']['primary_key']
        assert client.secondary_key == test_config['api']['secondary_key']
        assert client.daily_limit == test_config['api']['daily_limit']
        assert client.record_limit == test_config['api']['record_limit']
        assert client.current_key == client.primary_key
        assert client.use_primary is True
        assert client.call_count == 0
    
    def test_key_switching(self, test_config):
        """Test API key switching."""
        client = ComtradeAPIClient(test_config)
        
        # Default is primary key
        assert client.use_primary is True
        assert client.current_key == client.primary_key
        
        # Manually switch to secondary
        client._switch_key()
        assert client.use_primary is False
        assert client.current_key == client.secondary_key
        
        # Switch back to primary
        client._switch_key()
        assert client.use_primary is True
        assert client.current_key == client.primary_key
    
    def test_automatic_key_switching(self, test_config):
        """Test automatic key switching based on call count."""
        client = ComtradeAPIClient(test_config)
        
        # Set call count to trigger switch
        client.call_count = client.daily_limit // 2
        
        # Should switch to secondary on next increment
        client._increment_call_count()
        assert client.use_primary is False
        assert client.current_key == client.secondary_key
    
    def test_call_count_reset(self, test_config):
        """Test call count reset logic."""
        client = ComtradeAPIClient(test_config)
        
        # Set initial count
        client.call_count = 5
        
        # Reset date is in the past, should reset
        from datetime import datetime, timedelta
        client.reset_date = datetime.now() - timedelta(days=1)
        client._reset_call_count()
        
        assert client.call_count == 0
        assert client.reset_date > datetime.now()
    
    @patch('comtradeapicall.getTarifflineData')
    def test_get_tariffline_data_success(self, mock_get_data, test_config, mock_api_response):
        """Test successful API call for tariffline data."""
        client = ComtradeAPIClient(test_config)
        
        # Mock successful API response
        mock_get_data.return_value = mock_api_response
        
        # Call the API
        data, success = client.get_tariffline_data(
            reporter_code='DE',
            period_start='202201',
            period_end='202203'
        )
        
        # Verify results
        assert success is True
        assert data == mock_api_response
        assert client.call_count == 1
        
        # Verify mock was called with correct parameters
        mock_get_data.assert_called_once()
        call_args = mock_get_data.call_args[0][0]
        assert call_args['reporterCode'] == 'DE'
        assert call_args['period'] == '202201:202203'
        assert call_args['subscription-key'] == client.primary_key
    
    @patch('comtradeapicall.getTarifflineData')
    def test_get_tariffline_data_retry(self, mock_get_data, test_config):
        """Test API call retry mechanism."""
        client = ComtradeAPIClient(test_config)
        client.base_retry_delay = 0.01  # Make tests faster
        
        # Mock API call to fail once then succeed
        error_response = {'error': {'message': 'Test error'}}
        mock_get_data.side_effect = [
            error_response,  # First call fails
            mock_api_response  # Second call succeeds
        ]
        
        # Set a short retry delay for testing
        client.base_retry_delay = 0.01
        
        # Call the API
        data, success = client.get_tariffline_data(
            reporter_code='DE',
            period_start='202201',
            period_end='202203'
        )
        
        # Verify results
        assert success is True
        assert data == mock_api_response
        assert mock_get_data.call_count == 2
    
    @patch('comtradeapicall.getTarifflineData')
    def test_get_tariffline_data_failure(self, mock_get_data, test_config):
        """Test API call failure."""
        client = ComtradeAPIClient(test_config)
        client.base_retry_delay = 0.01  # Make tests faster
        client.max_retries = 2  # Reduce retries for faster tests
        
        # Mock API call to always fail
        error_response = {'error': {'message': 'Test error'}}
        mock_get_data.return_value = error_response
        
        # Call the API
        data, success = client.get_tariffline_data(
            reporter_code='DE',
            period_start='202201',
            period_end='202203'
        )
        
        # Verify results
        assert success is False
        assert data is None
        assert mock_get_data.call_count == client.max_retries
    
    @patch('comtradeapicall.getTarifflineData')
    def test_request_exception_handling(self, mock_get_data, test_config):
        """Test handling of request exceptions."""
        client = ComtradeAPIClient(test_config)
        client.base_retry_delay = 0.01  # Make tests faster
        client.max_retries = 2  # Reduce retries for faster tests
        
        # Mock API call to raise an exception
        mock_get_data.side_effect = requests.exceptions.RequestException("Test exception")
        
        # Call the API
        data, success = client.get_tariffline_data(
            reporter_code='DE',
            period_start='202201',
            period_end='202203'
        )
        
        # Verify results
        assert success is False
        assert data is None
        assert mock_get_data.call_count == client.max_retries
    
    @patch('comtradeapicall.getTarifflineData')
    def test_daily_limit_reached(self, mock_get_data, test_config):
        """Test behavior when daily API limit is reached."""
        client = ComtradeAPIClient(test_config)
        
        # Set call count to limit
        client.call_count = client.daily_limit
        
        # Call the API
        data, success = client.get_tariffline_data(
            reporter_code='DE',
            period_start='202201',
            period_end='202203'
        )
        
        # Verify API was not called
        assert success is False
        assert data is None
        mock_get_data.assert_not_called()