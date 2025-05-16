# File: tests/conftest.py
# -*- coding: utf-8 -*-

"""
Configuration and fixtures for pytest for the Comtrade Data Pipeline.
"""

import os
import sys
import pytest
from pathlib import Path

# Add the src directory to the path so we can import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import project modules
from src.utils.config_loader import load_config
from src.utils.config_defaults import (
    DEFAULT_API_CONFIG,
    DEFAULT_CACHE_CONFIG,
    DEFAULT_DB_CONFIG,
    DEFAULT_LOGGING_CONFIG,
    DEFAULT_COMTRADE_CONFIG
)


@pytest.fixture
def test_config():
    """Fixture for test configuration."""
    # Create a test configuration that doesn't rely on external resources
    config = {
        'api': DEFAULT_API_CONFIG.copy(),
        'db': DEFAULT_DB_CONFIG.copy(),
        'eu_countries': ['DE', 'FR', 'IT', 'ES', 'NL'],  # Subset for testing
        'cache': DEFAULT_CACHE_CONFIG.copy(),
        'comtrade': DEFAULT_COMTRADE_CONFIG.copy(),
        'logging': DEFAULT_LOGGING_CONFIG.copy(),
    }
    
    # Override with test-specific values
    config['api'].update({
        'primary_key': 'test_primary_key',
        'secondary_key': 'test_secondary_key',
        'daily_limit': 10,  # Lower for testing
        'record_limit': 100,  # Lower for testing
    })
    
    config['cache'].update({
        'cache_dir': 'tests/data/cache',
        'enabled': True,
        'ttl_days': 1,  # Short TTL for testing
    })
    
    config['db'].update({
        'host': 'localhost',
        'port': 5432,
        'dbname': 'comtrade_test',  # Test database
        'user': 'postgres',
        'password': 'test',
        'max_retries': 1,  # Faster failure for tests
    })
    
    config['logging'].update({
        'log_dir': 'tests/logs',
        'default_level': 'DEBUG',
        'console': True,
    })
    
    return config


@pytest.fixture
def mock_api_response():
    """Fixture for a mock API response."""
    return {
        'data': [
            {
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
            },
            {
                'reporterCode': 'DE',
                'partnerCode': 'US',
                'cmdCode': '010129',
                'flowCode': 'M',
                'period': '202201',
                'netWgt': 2500.75,
                'qty': 25,
                'qtyUnit': 'Number of items',
                'primaryValue': 12500.50,
                'flag': 0,
                'isReporterEstimate': False
            }
        ],
        'reporterAreas': [
            {'id': 'DE', 'text': 'Germany'}
        ],
        'partnerAreas': [
            {'id': 'CN', 'text': 'China'},
            {'id': 'US', 'text': 'United States'}
        ],
        'cmdCodes': [
            {'id': '010121', 'text': 'Horses, live pure-bred breeding'},
            {'id': '010129', 'text': 'Horses, live except pure-bred breeding'}
        ],
        'flowCodes': [
            {'id': 'M', 'text': 'Import'}
        ]
    }


@pytest.fixture
def setup_test_environment(test_config):
    """Setup test environment with directories."""
    # Create test directories
    os.makedirs(test_config['cache']['cache_dir'], exist_ok=True)
    os.makedirs(test_config['logging']['log_dir'], exist_ok=True)
    os.makedirs('tests/data/output', exist_ok=True)
    
    yield
    
    # Cleanup is optional, but can be implemented here
    # Uncomment if you want to clean up after tests
    # import shutil
    # shutil.rmtree(test_config['cache']['cache_dir'], ignore_errors=True)
    # shutil.rmtree(test_config['logging']['log_dir'], ignore_errors=True)
    # shutil.rmtree('tests/data/output', ignore_errors=True)